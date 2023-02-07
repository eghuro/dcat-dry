"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging
import itertools
import rdflib
from celery import group
from rdflib import Namespace
from rdflib.namespace import RDF
from requests.exceptions import HTTPError
from sqlalchemy import insert

from tsa.analyzer import GenericAnalyzer
from tsa.celery import celery
from tsa.db import db_session
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.model import DatasetDistribution, DatasetEndpoint
from tsa.monitor import TimedBlock, monitor
from tsa.net import RobotsRetry
from tsa.settings import Config
from tsa.tasks.common import TrackableTask
from tsa.tasks.process import filter_iri, process, process_priority
from tsa.util import check_iri


def query_parent(ds, g, log):
    query = f"SELECT ?parent WHERE {{ <{ds!s}> <http://purl.org/dc/terms/isPartOf> ?parent }}"
    try:
        for parent in g.query(query):
            parent_iri = str(parent["parent"])
            yield str(parent_iri)
    except ValueError:
        log.warning(f"Failed to query parent. Query was: {query}")


def test_allowed(url: str) -> bool:  #WTF?
    for prefix in [
        "https://data.cssz.cz",
        "https://rpp-opendata.egon.gov.cz",
        "https://data.mpsv.cz",
        "https://data.mvcr.gov.cz",
        "https://cedropendata.mfcr.cz",
        "https://opendata.praha.eu",
        "https://data.ctu.cz",
        "https://www.isvavai.cz",
    ]:
        if url.startswith(prefix):
            return True
    return False


def _dcat_extractor(g, log, force):
    if g is None:
        return
    distributions, distributions_priority = [], []
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    dcterms = Namespace("http://purl.org/dc/terms/")
    nkod = Namespace("https://data.gov.cz/slovník/nkod/mediaTyp")
    media_priority = set(
        [
            "https://www.iana.org/assignments/media-types/application/rdf+xml",
            "https://www.iana.org/assignments/media-types/application/trig",
            "https://www.iana.org/assignments/media-types/text/n3",
            "https://www.iana.org/assignments/media-types/application/ld+json",
            "https://www.iana.org/assignments/media-types/application/n-triples",
            "https://www.iana.org/assignments/media-types/application/n-quads",
            "https://www.iana.org/assignments/media-types/text/turtle",
            "http://www.iana.org/assignments/media-types/application/rdf+xml",
            "http://www.iana.org/assignments/media-types/application/trig",
            "http://www.iana.org/assignments/media-types/text/n3",
            "http://www.iana.org/assignments/media-types/application/ld+json",
            "http://www.iana.org/assignments/media-types/application/n-triples",
            "http://www.iana.org/assignments/media-types/application/n-quads",
            "http://www.iana.org/assignments/media-types/text/turtle",
        ]
    )  # IANA
    format_priority = set(
        [
            "http://publications.europa.eu/resource/authority/file-type/RDF",
            "http://publications.europa.eu/resource/authority/file-type/RDFA",
            "http://publications.europa.eu/resource/authority/file-type/RDF_N_QUADS",
            "http://publications.europa.eu/resource/authority/file-type/RDF_N_TRIPLES",
            "http://publications.europa.eu/resource/authority/file-type/RDF_TRIG",
            "http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE",
            "http://publications.europa.eu/resource/authority/file-type/RDF_XML",
            "http://publications.europa.eu/resource/authority/file-type/JSON_LD",
            "http://publications.europa.eu/resource/authority/file-type/N3",
            "https://publications.europa.eu/resource/authority/file-type/RDF",
            "https://publications.europa.eu/resource/authority/file-type/RDFA",
            "https://publications.europa.eu/resource/authority/file-type/RDF_N_QUADS",
            "https://publications.europa.eu/resource/authority/file-type/RDF_N_TRIPLES",
            "https://publications.europa.eu/resource/authority/file-type/RDF_TRIG",
            "https://publications.europa.eu/resource/authority/file-type/RDF_TURTLE",
            "https://publications.europa.eu/resource/authority/file-type/RDF_XML",
            "https://publications.europa.eu/resource/authority/file-type/JSON_LD",
            "https://publications.europa.eu/resource/authority/file-type/N3",
        ]
    )  # EU
    queue = distributions

    db_endpoints = []
    db_distributions = []

    #log.debug(f"Extracting distributions from {graph_iri}")
    # DCAT dataset
    with TimedBlock("dcat_extractor"):
        for ds in g.subjects(RDF.type, dcat.Dataset):
            #log.debug(f"DS: {ds!s}")
            effective_ds = ds

            for parent in query_parent(ds, g, log):
                effective_ds = parent
            #log.debug(f"effective DS: {effective_ds!s}")

            if effective_ds is None:
                log.error('Effective DS is NONE')
                continue

            # DCAT Distribution
            for d in g.objects(ds, dcat.distribution):
                # log.debug(f"Distr: {d!s}")
                # put RDF distributions into a priority queue
                for media in g.objects(d, dcat.mediaType):
                    if str(media) in media_priority:
                        queue = distributions_priority

                for distribution_format in g.objects(d, dcterms.format):
                    if str(distribution_format) in format_priority:
                        queue = distributions_priority

                # data.gov.cz specific
                for distribution_format in g.objects(d, nkod.mediaType):
                    if "rdf" in str(distribution_format):
                        queue = distributions_priority

                # download URL to files
                downloads = []
                for download_url in g.objects(d, dcat.downloadURL):
                    # log.debug(f'Down: {download_url!s}')
                    url = str(download_url)
                    if check_iri(url) and not filter_iri(url):
                        if url.endswith("/sparql"):
                            log.info(
                                f"Guessing {url} is a SPARQL endpoint, will use for dereferences from DCAT dataset {ds!s} (effective: {effective_ds!s})"
                            )
                            db_endpoints.append({
                                'ds': effective_ds,
                                'endpoint': str(url)
                            })
                        elif test_allowed(url) or not Config.LIMITED:
                            downloads.append(url)
                            log.debug(
                                f"Distribution {url!s} from DCAT dataset {ds!s} (effective: {effective_ds!s})"
                            )
                            if url.endswith("trig"):
                                distributions_priority.append(url)
                            else:
                                queue.append(url)
                            db_distributions.append({
                                'ds': str(effective_ds),
                                'distr': str(url)
                            })
                        else:
                            #log.debug(f"Skipping {url} due to filter")
                            pass
                    else:
                        # log.debug(f"{url} is not a valid download URL")
                        pass

                # scan for DCAT2 data services here as well
                for access in itertools.chain(g.objects(d, dcat.accessService), g.subjects(dcat.servesDataset, d)):
                    log.debug(f"Service: {access!s}")
                    for endpoint in g.objects(access, dcat.endpointURL):
                        if check_iri(str(endpoint)):
                            log.debug(
                                f"Endpoint {endpoint!s} from DCAT dataset {ds!s}"
                            )
                            if str(effective_ds) is not None:
                                db_endpoints.append({
                                    'ds': str(effective_ds),
                                    'endpoint': str(endpoint)
                                })
                            else:
                                log.error('Effective ds NONE in services loop')
        try:
            if len(db_endpoints) > 0:
                db_session.execute(insert(DatasetEndpoint).values(db_endpoints))
            if len(db_distributions) > 0:
                db_session.execute(insert(DatasetDistribution).values(db_distributions))
            if len(db_endpoints) + len(db_distributions) > 0:
                db_session.commit()
                GenericAnalyzer().get_details(g)  # extrakce labelu
        except:
            log.exception("Failed to commit in DCAT extractor")
            db_session.rollback()

        # TODO: possibly scan for service description as well
    tasks = [process_priority.si(a, force) for a in distributions_priority]
    tasks.extend(process.si(a, force) for a in distributions)
    monitor.log_tasks(len(tasks))
    group(tasks).apply_async()


# @celery.task(base=TrackableTask)
# def inspect_catalog(key):
#    """Analyze DCAT datasets listed in the catalog."""
#   log = logging.getLogger(__name__)
#    red = inspect_catalog.redis
#
#    log.debug('Parsing graph')
#    try:
#        g = rdflib.ConjunctiveGraph()
#       g.parse(data=red.get(key), format='n3')
#        red.delete(key)
#    except rdflib.plugin.PluginException:
#        log.debug('Failed to parse graph')
#        return None
#
#    return _dcat_extractor(g, red, log, False, key, None)


@celery.task(base=TrackableTask, bind=True, max_retries=5)
def inspect_graph(self, endpoint_iri, graph_iri, force):
    try:
        return do_inspect_graph(graph_iri, force, endpoint_iri)
    except RobotsRetry as exc:
        self.retry(timeout=exc.delay)


def do_inspect_graph(graph_iri, force, endpoint_iri):
    log = logging.getLogger(__name__)
    result = None
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        result = _dcat_extractor(
            inspector.process_graph(graph_iri), log, force
        )
    except (rdflib.query.ResultException, HTTPError):
        log.error(f"Failed to inspect graph {graph_iri}: ResultException or HTTP Error")
    monitor.log_inspected()
    return result


@celery.task(base=TrackableTask)
def inspect_graphs(graphs, endpoint_iri, force):
    do_inspect_graphs(graphs, endpoint_iri, force)


def do_inspect_graphs(graphs, endpoint_iri, force):
    return [do_inspect_graph(g, force, endpoint_iri) for g in graphs]


def multiply(item, times):
    for _ in range(times):
        yield item


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


@celery.task(base=TrackableTask)
def batch_inspect(endpoint_iri, graphs, chunks):
    items = len(graphs)
    monitor.log_graph_count(items)
    log = logging.getLogger(__name__)
    log.info(f"Batch of {items} graphs in {endpoint_iri}")
    return inspect_graph.chunks(
        zip(multiply(endpoint_iri, items), graphs, multiply(True, items)), chunks
    ).apply_async()
