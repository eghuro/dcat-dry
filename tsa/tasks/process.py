"""Celery tasks for running analyses."""
import logging
import os
import sys
from typing import Generator, List, Optional, Sequence, Tuple

import marisa_trie
import rdflib
import redis
import requests
import SPARQLWrapper
from celery import states
from celery.app.task import Task
from celery.exceptions import Ignore
from gevent.timeout import Timeout
from rdflib.exceptions import ParserError
from rdflib.plugins.stores.sparqlstore import SPARQLStore, _node_to_sparql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.celery import celery
from tsa.db import db_session
from tsa.extensions import redis_pool
from tsa.model import DatasetDistribution, DatasetEndpoint, SubjectObject
from tsa.monitor import TimedBlock, monitor
from tsa.net import (
    NoContent,
    RobotsBlock,
    RobotsRetry,
    Skip,
    fetch,
    get_content,
    guess_format,
)
from tsa.redis import KeyRoot, root_name
from tsa.robots import USER_AGENT, session
from tsa.settings import Config

try:
    from tsa.compression import decompress_7z, decompress_gzip
except ImportError:
    Config.COMPRESSED = False
from tsa.tasks.analyze import do_analyze_and_index, load_graph
from tsa.tasks.common import TrackableTask
from tsa.util import check_iri

trie = marisa_trie.Trie()
try:
    with open(Config.EXCLUDE_PREFIX_LIST, "r") as f:
        trie = marisa_trie.Trie([x.strip() for x in f.readlines()])
except FileNotFoundError:
    pass
# Following 2 tasks are doing the same thing but with different priorities
# This is to speed up known RDF distributions
# Time limit on process priority is to ensure we will do postprocessing after a while
@celery.task(
    bind=True,
    base=TrackableTask,
    ignore_result=True,
    autoretry_for=(redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError),
)
def process_priority(self, iri, force):
    do_process(iri, self, True, force)


@celery.task(
    bind=True,
    time_limit=300,
    base=TrackableTask,
    ignore_result=True,
    autoretry_for=(redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError),
)
def process(self, iri, force):
    do_process(iri, self, False, force)


def filter_iri(iri: str) -> bool:
    """
    Filter out IRIs that are not valid.
    Returns False if the IRI is valid, True if it should be skipped.

    We skip IRIs that starts with a prefix in the exclude list or
    ends with an unsupported filename extension.

    :param iri: IRI to check
    :return: True if the IRI should be skipped
    """
    iri = iri.strip()
    if iri.startswith("http"):
        iri = iri[8:]
    elif iri.startswith("https"):
        iri = iri[9:]
    if len(trie.prefixes(iri)) > 0:
        return True
    return (
        iri.endswith("csv.zip")
        or iri.endswith("csv")
        or iri.endswith("csv.gz")
        or iri.endswith("xls")
        or iri.endswith("docx")
        or iri.endswith("xlsx")
        or iri.endswith("pdf")
    )


def get_iris_to_dereference(
    graph: rdflib.Graph, iri: str
) -> Generator[str, None, None]:
    """
    Get subject, predicate and object IRIs to dereference from a graph.
    This is used to dereference all IRIs in a graph.

    :param graph: Graph to get IRIs from
    :param iri: distribution IRI (used for logging)
    :return: Generator of IRIs to dereference
    """
    log = logging.getLogger(__name__)
    if graph is None:
        log.debug("Graph is None when dereferencing %s", iri)
        return
    log.debug("Get iris to dereference from distribution: %s", iri)
    for subject, predicate, object_iri in graph:
        pred = str(predicate)
        obj = str(object_iri)
        sub = str(subject)

        if check_iri(pred) and not filter_iri(pred):
            yield pred
        if check_iri(obj) and not filter_iri(obj):
            yield obj
        if check_iri(sub) and not filter_iri(sub):
            yield sub


def dereference_from_endpoint(iri: str, endpoint_iri: str) -> rdflib.Graph:
    """
    Construct a three-level graph from an IRI in the endpoint.

    :param iri: IRI to dereference
    :param endpoint_iri: Endpoint IRI
    :return: Graph with the resource and its neighbors (in the endpoint)
    """
    log = logging.getLogger(__name__)
    log.info("Dereference %s from endpoint %s", iri, endpoint_iri)
    with RobotsBlock(endpoint_iri):
        store = SPARQLStore(
            endpoint_iri,
            True,
            True,
            _node_to_sparql,
            "application/rdf+xml",
            session=session,
            headers={"User-Agent": USER_AGENT},
        )
        endpoint_graph = rdflib.Graph(store=store)
        endpoint_graph.open(endpoint_iri)
        # for cube and ruian we need 3 levels (nested optionals are a must, otherwise the query will not finish)
        query = f"CONSTRUCT {{<{iri}> ?p1 ?o1. ?o1 ?p2 ?o2. ?o2 ?p3 ?o3.}} WHERE {{ <{iri}> ?p1 ?o1. OPTIONAL {{?o1 ?p2 ?o2. OPTIONAL {{?o2 ?p3 ?o3.}} }} }}"

        try:
            with TimedBlock("dereference_from_endpoints.construct"):
                return endpoint_graph.query(query).graph
        except SPARQLWrapper.SPARQLExceptions.QueryBadFormed:
            log.error(
                "Dereference %s from endpoint %s failed. Query:\n%s\n\n",
                iri,
                endpoint_iri,
                query,
            )
        except (rdflib.query.ResultException, requests.exceptions.HTTPError):
            log.warning(
                "Failed to dereference %s from endpoint %s: ResultException or HTTP Error",
                iri,
                endpoint_iri,
            )
        except ValueError as err:  # usually an empty graph
            log.debug(
                "Failed to dereference %s from endpoint %s: %s, query: %s",
                iri,
                endpoint_iri,
                str(err),
                query,
            )
        return rdflib.ConjunctiveGraph()


def sanitize_list(list_in: Optional[Sequence[str]]) -> Generator[str, None, None]:
    if list_in is not None:
        for item in list_in:
            if item is not None:
                yield item


def dereference_from_endpoints(iri: str, iri_distr: str) -> rdflib.ConjunctiveGraph:
    if not check_iri(iri):
        return rdflib.ConjunctiveGraph()
    monitor.log_dereference_processed()
    graph = rdflib.ConjunctiveGraph()
    log = logging.getLogger(__name__)

    endpoints = set()
    if "ENDPOINT" in os.environ:
        endpoints.add(os.environ["ENDPOINT"])
    endpoints_cfg = list(sanitize_list(Config.LOOKUP_ENDPOINTS))
    for endpoint in endpoints_cfg[1:]:
        # filter only relevant endpoint(s)
        prefix = endpoint[0:-7]  # remove /sparql
        if iri.startswith(prefix):
            endpoints.add(endpoint)
    for dataset_distribution in db_session.query(DatasetDistribution).filter_by(
        distr=iri_distr
    ):
        ds_iri = dataset_distribution.ds
        log.debug("For %s we have the dataset %s", iri_distr, ds_iri)

        for dataset_endpoint in db_session.query(DatasetEndpoint).filter_by(ds=ds_iri):
            endpoints.add(dataset_endpoint.endpoint)
    for endpoint_iri in endpoints:
        if check_iri(endpoint_iri):
            try:
                graph += dereference_from_endpoint(iri, endpoint_iri)
            except Skip:
                pass
    return graph


class FailedDereference(ValueError):
    pass


def dereference_one_impl(
    iri_to_dereference: Optional[str], iri_distr: str
) -> rdflib.ConjunctiveGraph:  # can raise RobotsRetry
    log = logging.getLogger(__name__)
    log.debug("Dereference: %s", iri_to_dereference)
    if not check_iri(iri_to_dereference):
        raise FailedDereference()
    if iri_to_dereference is None:
        raise FailedDereference()
    monitor.log_dereference_request()
    try:
        try:
            response = fetch(iri_to_dereference)
        except RobotsRetry:
            return dereference_from_endpoints(iri_to_dereference, iri_distr)
        # test_content_length(iri_to_dereference, response, log)
        guess, _ = guess_format(iri_to_dereference, response, log)
        content = get_content(iri_to_dereference, response)
        monitor.log_dereference_processed()
        graph = load_graph(iri_to_dereference, content, guess, False)
        if graph is not None and len(graph) > 0:
            return graph
        log.debug(
            "Loaded empty graph or none, will lookup in endpoint: %s",
            iri_to_dereference,
        )
        return dereference_from_endpoints(iri_to_dereference, iri_distr)
    except (requests.exceptions.HTTPError, UnicodeError):
        # log.debug("HTTP Error dereferencing, will lookup in endpoints: %s", iri_to_dereference)
        return dereference_from_endpoints(iri_to_dereference, iri_distr)
    except requests.exceptions.RequestException:
        log.debug(
            "Failed to dereference (RequestException fetching): %s", iri_to_dereference
        )
        return dereference_from_endpoints(iri_to_dereference, iri_distr)
    except (Skip, NoContent):
        return dereference_from_endpoints(iri_to_dereference, iri_distr)
    except:
        log.exception(
            "Unknown error dereferencing, will lookup in endpoints: %s",
            iri_to_dereference,
        )
        return dereference_from_endpoints(iri_to_dereference, iri_distr)


def has_same_as(graph: rdflib.Graph) -> bool:
    owl = rdflib.Namespace("http://www.w3.org/2002/07/owl#")
    for _ in graph.subject_objects(owl.sameAs):
        return True
    return False


def dereference_one(
    iri_to_dereference: Optional[str], iri_distr: str
) -> Tuple[rdflib.ConjunctiveGraph, bool]:
    try:
        graph = dereference_one_impl(iri_to_dereference, iri_distr)
        return graph, has_same_as(graph)
    except FailedDereference:
        logging.getLogger(__name__).exception(
            "All attempts to dereference failed: %s", iri_to_dereference
        )
        raise
    except RobotsRetry as e:
        raise FailedDereference() from e


def expand_graph_with_dereferences(
    graph: rdflib.ConjunctiveGraph, iri_distr: str
) -> rdflib.ConjunctiveGraph:
    log = logging.getLogger(__name__)
    if Config.MAX_RECURSION_LEVEL == 0:
        return graph
    dereferenced = set()
    queue = [(iri_distr, 0)]  # type: List[Tuple[str, int]]
    while len(queue) > 0:
        (iri, level) = queue.pop(0)
        for iri_to_dereference in frozenset(get_iris_to_dereference(graph, iri)):
            if iri_to_dereference in dereferenced:
                continue
            try:
                sub_graph, should_continue = dereference_one(
                    iri_to_dereference, iri_distr
                )
                dereferenced.add(iri_to_dereference)
                if sub_graph is not None:
                    graph += sub_graph

                if should_continue and level < Config.MAX_RECURSION_LEVEL:
                    log.info(
                        "Continue dereferencing: now at %s, dereferenced %s",
                        iri_distr,
                        iri_to_dereference,
                    )
                    new_level = level + 1
                    queue.append((iri_to_dereference, new_level))

            except UnicodeDecodeError:
                log.exception(
                    "Failed to dereference %s (UnicodeDecodeError)", iri_to_dereference
                )
            except FailedDereference:
                pass
    return graph


def store_pure_subjects(iri, graph):
    if iri is None or len(iri) == 0:
        return
    insert_stmt = (
        insert(SubjectObject)
        .values(
            [
                {"distribution_iri": iri, "iri": str(sub), "pureSubject": True}
                for sub, _, _ in graph
                if ((sub is not None) and len(str(sub)) > 0)
            ]
        )
        .on_conflict_do_nothing()
    )
    try:
        db_session.execute(insert_stmt)
        db_session.commit()
    except SQLAlchemyError:
        logging.getLogger(__name__).error("Failed to store pure objects")
        db_session.rollback()


def process_content(content: str, iri: str, guess: str, log: logging.Logger) -> None:
    log.info("Analyze and index %s", iri)
    with TimedBlock("process.load"):
        graph = load_graph(iri, content, guess, True)

    if graph is None:
        log.warning("Graph is none: %s", iri)
        return
    if len(graph) == 0:
        log.debug("Graph is empty: %s", iri)
        return

    store_pure_subjects(iri, graph)

    with TimedBlock("process.dereference"):
        try:
            graph = expand_graph_with_dereferences(graph, iri)
        except ValueError:
            log.exception("Failed to expand dereferenes: %s", iri)
    with TimedBlock("process.analyze_and_index"):
        do_analyze_and_index(graph, iri)
    log.debug("Done analyze and index %s (immediate)", iri)
    monitor.log_processed()


def _filter(
    iri: str, is_prio: bool, force: bool, log: logging.Logger, red: redis.Redis
) -> None:
    if filter_iri(iri):
        log.debug("Skipping distribution as it will not be supported: %s", iri)
        raise Skip()

    if not is_prio and (iri.endswith("xml") or iri.endswith("xml.zip")):
        log.debug(
            "Skipping distribution as it will not be supported: %s (xml in the non-priority channel)",
            iri,
        )
        raise Skip()

    key = root_name[KeyRoot.DISTRIBUTIONS]
    if not force and red.pfadd(key, iri) == 0:
        log.debug("Skipping distribution as it was recently processed: %s", iri)
        raise Skip()


def do_fetch(
    iri: str,
    task: Task,
    is_prio: bool,
    force: bool,
    log: logging.Logger,
    red: redis.Redis,
) -> Tuple[str, requests.Response]:
    if iri.endswith("trig") and not is_prio:
        log.error("RDF TriG not in priority")
    try:
        _filter(iri, is_prio, force, log, red)
        log.info("Processing %s", iri)
        response = fetch(iri)
        # test_content_length(iri, response, log)
        guess, priority = guess_format(iri, response, log)
        if not is_prio and priority:
            log.warn("Distribution is not in a priority channel: %s", iri)
        return guess, response
    except RobotsRetry as err:
        task.retry(countdown=err.delay)
    except requests.exceptions.HTTPError as err:
        log.warning(
            "HTTP Error processing %s: %s", iri, str(err)
        )  # this is a 404 or similar, not worth retrying
    except requests.exceptions.RequestException as err:
        task.retry(exc=err)
    except OverflowError:
        log.warning("Overflow error fetching %s", iri)
    raise Skip()


def do_process(iri: str, task: Task, is_prio: bool, force: bool) -> None:
    """Analyze an RDF distribution under given IRI."""
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)

    try:
        guess, response = do_fetch(iri, task, is_prio, force, log, red)

        if guess in [
            "application/x-7z-compressed",
            "application/x-zip-compressed",
            "application/zip",
        ]:
            do_decompress(red, iri, "zip", response)
        elif guess in ["application/gzip", "application/x-gzip"]:
            do_decompress(red, iri, "gzip", response)
        else:
            try:
                log.debug("Get content of %s", iri)
                content = get_content(iri, response)
                process_content(content, iri, guess, log)
            except requests.exceptions.ChunkedEncodingError as err:
                task.retry(exc=err)
            except NoContent:
                log.warning("No content for %s", iri)
    except Skip:
        monitor.log_processed()  # any logging is handled already
    except ParserError as err:
        log.warning("Failed to parse %s - likely not an RDF: %s", iri, str(err))
        monitor.log_processed()
    except RobotsRetry as err:
        task.retry(countdown=err.delay)
    except Timeout:
        log.error("Failed to get %s: timeout", iri)
        monitor.log_processed()
        task.update_state(state=states.FAILURE, meta="Timeout")
        raise Ignore()
    except:
        if sys.exc_info()[0] == Ignore:
            raise
        exc = sys.exc_info()[1]
        log.exception("Failed to get %s: %s", iri, str(exc))
        monitor.log_processed()


def do_decompress(red, iri, archive_type, request):
    if not Config.COMPRESSED:
        return

    log = logging.getLogger(__name__)

    key = root_name[KeyRoot.DISTRIBUTIONS]
    log.debug("Decompress %s", iri)

    with TimedBlock("process.decompress"):
        try:
            deco = {"zip": decompress_7z, "gzip": decompress_gzip}
            for sub_iri, data in deco[archive_type](iri, request):
                if red.pfadd(key, sub_iri) == 0:
                    log.debug(
                        "Skipping distribution as it was recently analyzed: %s", sub_iri
                    )
                    continue

                if sub_iri.endswith("/data"):  # extracted a file without a filename
                    process_content(
                        data, sub_iri, "text/plain", log
                    )  # this will allow for analysis to happen
                    continue

                try:
                    guess, _ = guess_format(sub_iri, request, log)
                except Skip:
                    continue
                if guess is None:
                    log.warning("Unknown format after decompression: %s", sub_iri)
                else:
                    process_content(data, sub_iri, guess, log)
        except (TypeError, ValueError):
            log.exception(
                "Failed to decompress. iri: %s, archive_type: %s", iri, archive_type
            )
