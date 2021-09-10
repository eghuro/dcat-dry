"""Celery tasks for running analyses."""
import logging
import sys

import rdflib
import redis
import requests
import SPARQLWrapper
from rdflib.plugins.stores.sparqlstore import SPARQLStore, _node_to_sparql
from requests import RequestException

from tsa.celery import celery
from tsa.compression import decompress_7z, decompress_gzip
from tsa.extensions import redis_pool
from tsa.monitor import TimedBlock, monitor
from tsa.net import RobotsRetry, Skip, fetch, get_content, guess_format, test_content_length
from tsa.redis import KeyRoot
from tsa.redis import data as data_key
from tsa.redis import dataset_endpoint
from tsa.redis import dereference as dereference_key
from tsa.redis import ds_distr, pure_subject, root_name
from tsa.robots import USER_AGENT
from tsa.settings import Config
from tsa.tasks.analyze import do_analyze_and_index, load_graph
from tsa.tasks.common import TrackableTask
from tsa.util import test_iri


# Following 2 tasks are doing the same thing but with different priorities
# This is to speed up known RDF distributions
# Time limit on process priority is to ensure we will do postprocessing after a while
@celery.task(bind=True, time_limit=3600, base=TrackableTask)
def process_priority(self, iri, force):
    do_process(iri, self, True, force)


@celery.task(bind=True, time_limit=600, base=TrackableTask)
def process(self, iri, force):
    do_process(iri, self, False, force)


def filter_iri(iri):
    return iri.endswith('csv.zip') or iri.endswith('csv') or iri.endswith('csv.gz') or iri.endswith('xls') or \
        iri.endswith('docx') or iri.endswith('xlsx') or iri.endswith('pdf') or \
        ((iri.startswith('http://vdp.cuzk.cz') or iri.startswith('https://vdp.cuzk.cz')) and (iri.endswith('xml.zip') or iri.endswith('xml'))) or \
        ((iri.startswith('http://dataor.justice.cz') or iri.startswith('https://dataor.justice.cz')) and (iri.endswith('xml') or iri.endswith('xml.gz'))) or \
        iri.startswith('https://apl.czso.cz/iSMS/cisexp.jsp') or iri.startswith('https://eagri.cz') or \
        iri.startswith('https://volby.cz/pls/ps2017/vysledky_okres') or \
        iri.startswith('http://services.cuzk.cz/') or iri.startswith('https://services.cuzk.cz/')


def get_iris_to_dereference(graph, iri):
    log = logging.getLogger(__name__)
    if graph is None:
        log.debug(f'Graph is None when dereferencing {iri}')
        return
    log.debug(f'Get iris to dereference from distribution: {iri}')
    for s, p, o in graph:
        pred = str(p)
        obj = str(o)
        sub = str(s)

        if test_iri(pred):
            yield pred
        if test_iri(obj):
            yield obj
        if test_iri(sub):
            yield sub


def dereference_from_endpoint(iri, endpoint_iri):
    log = logging.getLogger(__name__)
    log.info(f'Dereference {iri} from endpoint {endpoint_iri}')
    store = SPARQLStore(endpoint_iri, True, True, _node_to_sparql,
                        'application/rdf+xml',
                        headers={'User-Agent': USER_AGENT})
    endpoint_graph = rdflib.Graph(store=store)
    endpoint_graph.open(endpoint_iri)
    # for cube and ruian we need 3 levels (nested optionals are a must, otherwise the query will not finish)
    query = f'CONSTRUCT {{<{iri}> ?p1 ?o1. ?o1 ?p2 ?o2. ?o2 ?p3 ?o3.}} WHERE {{ <{iri}> ?p1 ?o1. OPTIONAL {{?o1 ?p2 ?o2. OPTIONAL {{?o2 ?p3 ?o3.}} }} }}'

    graph = rdflib.ConjunctiveGraph()
    try:
        with TimedBlock('dereference_from_endpoints.construct'):
            graph = endpoint_graph.query(query).graph
    except SPARQLWrapper.SPARQLExceptions.QueryBadFormed:
        log.error(f'Dereference {iri} from endpoint failed. Query:\n{query}\n\n')
    except (rdflib.query.ResultException, requests.exceptions.HTTPError):
        log.error(f'Failed to dereference {iri}: ResultException or HTTP Error')
    except ValueError as err:
        log.error(f'Failed to dereference {iri}: {err!s}, query: {query}')
    return graph


def dereference_from_endpoints(iri, iri_distr, red):
    if not test_iri(iri):
        return None
    monitor.log_dereference_processed()
    graph = rdflib.ConjunctiveGraph()
    log = logging.getLogger(__name__)

    _, distrds = ds_distr()
    for ds_iri in red.smembers(f'{distrds}:{str(iri_distr)}'):
        log.debug(f'For {iri_distr} we have the dataset {ds_iri}')
        local_endpoints = red.smembers(dataset_endpoint(str(ds_iri)))
        endpoints = Config.LOOKUP_ENDPOINTS + local_endpoints
        for endpoint_iri in endpoints:
            graph += dereference_from_endpoint(iri, endpoint_iri)
    return graph


class FailedDereference(ValueError):
    pass


def dereference_one_impl(iri_to_dereference, iri_distr):
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    log.debug(f'Dereference: {iri_to_dereference}')
    if not test_iri(iri_to_dereference):
        raise FailedDereference()
    monitor.log_dereference_request()
    try:
        try:
            response = fetch(iri_to_dereference, log, red)
        except RobotsRetry as err:
            log.warning(f'Should retry with delay of {err.delay}, will lookup in endpoint: {iri_to_dereference}')
            return dereference_from_endpoints(iri_to_dereference, iri_distr, red)
        except requests.exceptions.HTTPError:
            log.debug(f'HTTP Error dereferencing, will lookup in endpoint: {iri_to_dereference}')
            return dereference_from_endpoints(iri_to_dereference, iri_distr, red)
        except requests.exceptions.RequestException:
            log.debug(f'Failed to dereference (RequestException fetching): {iri_to_dereference}')
            return dereference_from_endpoints(iri_to_dereference, iri_distr, red)
        except Skip:
            return dereference_from_endpoints(iri_to_dereference, iri_distr, red)

        try:
            test_content_length(iri_to_dereference, response, log)
            guess, _ = guess_format(iri_to_dereference, response, log)
        except Skip:
            log.debug(f'Attempt to lookup {iri_to_dereference} in endpoint')
            return dereference_from_endpoints(iri_to_dereference, iri_distr, red)

        try:
            content = get_content(iri_to_dereference, response, red)
            if content is None:
                log.debug(f'No content: {iri_to_dereference}')
                return dereference_from_endpoints(iri_to_dereference, iri_distr, red)
            content.encode('utf-8')
            monitor.log_dereference_processed()
            return load_graph(iri_to_dereference, content, guess)
        except requests.exceptions.ChunkedEncodingError:
            log.warning(f'Failed to dereference (ChunkedEncodingError getting content): {iri_to_dereference}')
            return dereference_from_endpoints(iri_to_dereference, iri_distr, red)
    except RequestException:
        log.exception(f'Failed to dereference: {iri_to_dereference}')
        return dereference_from_endpoints(iri_to_dereference, iri_distr, red)


def dereference_one(iri_to_dereference, iri_distr):
    try:
        red = redis.Redis(connection_pool=redis_pool)
        key = dereference_key(iri_to_dereference)
        if red.exists(key):
            data = red.get(key)
            if data is not None:
                graph = load_graph(iri_to_dereference, data, 'n3')
            else:
                return None, False
        else:
            graph = dereference_one_impl(iri_to_dereference, iri_distr)
            if graph is not None:
                red.set(key, graph.serialize(format='n3'))
            else:
                red.set(key, '')
        owl = rdflib.URIRef('http://www.w3.org/2002/07/owl#')
        has_same_as = (None, owl['sameAs'], None) in graph
        return graph, has_same_as
    except:
        logging.getLogger(__name__).exception(f'All attempts to dereference failed: {iri_to_dereference}')
        raise FailedDereference() from sys.exc_info()[1]


def expand_graph_with_dereferences(graph, iri_distr, recursion=0):
    log = logging.getLogger(__name__)
    if recursion == Config.MAX_RECURSION_LEVEL:
        log.warning(f'Reached max recursion level {recursion} when dereferencing {iri_distr}')
        return graph
    for iri_to_dereference in frozenset(get_iris_to_dereference(graph, iri_distr)):
        try:
            sub_graph, should_continue = dereference_one(iri_to_dereference, iri_distr)
            if should_continue:
                log.info(f'Continue dereferencing: now at {iri_distr}, dereferenced {iri_to_dereference}')
                sub_graph += expand_graph_with_dereferences(sub_graph, iri_to_dereference, recursion + 1)
            if sub_graph is not None:
                graph += sub_graph
        except UnicodeDecodeError:
            log.exception(f'Failed to dereference {iri_to_dereference} (UnicodeDecodeError)')
        except FailedDereference:
            pass
    return graph


def store_pure_subjects(iri, graph, red):
    subjects_pure = set()
    for s, _, _ in graph:
        subjects_pure.add(str(s))
    red.lpush(pure_subject(iri), *list(subjects_pure))


def process_content(content, iri, guess, red, log):
    if content is None:
        log.warning(f'No content for {iri}')
        return

    log.debug(f'Analyze and index {iri}')
    content.encode('utf-8')
    with TimedBlock('process.load'):
        graph = load_graph(iri, content, guess)

    if graph is None:
        log.warning('Graph is none')
        return

    store_pure_subjects(iri, graph, red)

    with TimedBlock('process.dereference'):
        try:
            graph = expand_graph_with_dereferences(graph, iri)
        except ValueError:
            log.exception(f'Failed to expand dereferenes: {iri}')
    with TimedBlock('process.analyze_and_index'):
        do_analyze_and_index(graph, iri, red)
    log.debug(f'Done analyze and index {iri} (immediate)')
    monitor.log_processed()


def do_process(iri, task, is_prio, force):
    """Analyze an RDF distribution under given IRI."""
    log = logging.getLogger(__name__)

    if filter_iri(iri):
        log.debug(f'Skipping distribution as it will not be supported: {iri!s}')
        monitor.log_processed()
        return

    if not is_prio and (iri.endswith('xml') or iri.endswith('xml.zip')):
        log.debug(f'Skipping distribution as it will not be supported: {iri!s} (xml in the non-priority channel)')
        monitor.log_processed()
        return

    red = task.redis
    key = root_name[KeyRoot.DISTRIBUTIONS]
    if not force and red.pfadd(key, iri) == 0:
        log.debug(f'Skipping distribution as it was recently processed: {iri!s}')
        monitor.log_processed()
        return

    log.info(f'Processing {iri!s}')

    try:
        try:
            response = fetch(iri, log, red)
        except RobotsRetry as err:
            task.retry(countdown=err.delay)
        except requests.exceptions.HTTPError as err:
            log.warning(f'HTTP Error processsing {iri}: {err!s}')  # this is a 404 or similar, not worth retrying
            monitor.log_processed()
            return
        except requests.exceptions.RequestException as err:
            task.retry(exc=err)
        except Skip:
            monitor.log_processed()
            return

        try:
            test_content_length(iri, response, log)
            guess, priority = guess_format(iri, response, log)
            is_prio = is_prio | priority
        except Skip:
            monitor.log_processed()
            return

        if guess in ['application/x-7z-compressed', 'application/x-zip-compressed', 'application/zip']:
            with TimedBlock('process.decompress'):
                do_decompress(red, iri, 'zip', response)
        elif guess in ['application/gzip', 'application/x-gzip']:
            with TimedBlock('process.decompress'):
                do_decompress(red, iri, 'gzip', response)
        else:
            try:
                log.debug(f'Get content of {iri}')
                content = get_content(iri, response, red)
                process_content(content, iri, guess, red, log)
            except requests.exceptions.ChunkedEncodingError as err:
                task.retry(exc=err)
    except rdflib.exceptions.ParserError as err:
        log.warning(f'Failed to parse {iri!s} - likely not an RDF: {err!s}')
        monitor.log_processed()
    except:
        exc = sys.exc_info()[1]
        log.exception(f'Failed to get {iri!s}: {exc!s}')
        monitor.log_processed()


def do_decompress(red, iri, archive_type, request):
    log = logging.getLogger(__name__)

    key = root_name[KeyRoot.DISTRIBUTIONS]
    log.debug(f'Decompress {iri}')

    try:
        deco = {
            'zip': decompress_7z,
            'gzip': decompress_gzip
        }
        for sub_iri, data in deco[archive_type](iri, request):
            if red.pfadd(key, sub_iri) == 0:
                log.debug(f'Skipping distribution as it was recently analyzed: {sub_iri!s}')
                continue

            if sub_iri.endswith('/data'):  # extracted a file without a filename
                process_content(data, sub_iri, 'text/plain', red, log)  # this will allow for analysis to happen
                continue

            try:
                guess, _ = guess_format(sub_iri, request, log)
            except Skip:
                continue
            if guess is None:
                log.warning(f'Unknown format after decompression: {sub_iri}')
                red.expire(data_key(sub_iri), 1)
            else:
                process_content(data, sub_iri, guess, red, log)
    except (TypeError, ValueError):
        log.error(f'Failed to decompress. iri: {iri!s}, archive_type: {archive_type!s}')
