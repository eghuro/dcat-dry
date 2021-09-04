"""Celery tasks for running analyses."""
import logging

import gevent
import rdflib
import redis
import requests
import rfc3987
import SPARQLWrapper
from celery import group
from rdflib.plugins.stores.sparqlstore import SPARQLStore, _node_to_sparql

from tsa.celery import celery
from tsa.compression import SizeException, decompress_7z, decompress_gzip
from tsa.extensions import redis_pool
from tsa.monitor import TimedBlock, monitor
from tsa.net import RobotsRetry, Skip, fetch, get_content, guess_format, test_content_length
from tsa.redis import KeyRoot
from tsa.redis import data as data_key
from tsa.redis import expiration, graph, root_name
from tsa.robots import user_agent
from tsa.settings import Config
from tsa.tasks.analyze import do_analyze_and_index, load_graph
from tsa.tasks.common import TrackableTask


# Following 2 tasks are doing the same thing but with different priorities
# This is to speed up known RDF distributions
@celery.task(bind=True, base=TrackableTask)
def process_priority(self, iri, force):
    do_process(iri, self, True, force)

@celery.task(bind=True, time_limit=600, base=TrackableTask)
def process(self, iri, force):
    do_process(iri, self, False, force)


def filter(iri):
    return iri.endswith('csv.zip') or iri.endswith('csv') or  iri.endswith('csv.gz') or iri.endswith('xls') or \
            iri.endswith('docx') or iri.endswith('xlsx') or iri.endswith('pdf') or \
            ((iri.startswith('http://vdp.cuzk.cz') or iri.startswith('https://vdp.cuzk.cz')) and (iri.endswith('xml.zip') or iri.endswith('xml'))) or \
            ((iri.startswith('http://dataor.justice.cz') or iri.startswith('https://dataor.justice.cz')) and (iri.endswith('xml') or iri.endswith('xml.gz'))) or \
            iri.startswith('https://apl.czso.cz/iSMS/cisexp.jsp') or iri.startswith('https://eagri.cz') or \
            iri.startswith('https://volby.cz/pls/ps2017/vysledky_okres') or \
            iri.startswith('http://services.cuzk.cz/')  or iri.startswith('https://services.cuzk.cz/')

def get_iris_to_dereference(g, iri):
    log = logging.getLogger(__name__)
    if g is None:
        log.debug(f'Graph is None when dereferencing {iri}')
        return
    log.debug(f'Get iris to dereference from distribution: {iri}')
    for s, p, o in g:
        pred = str(p)
        obj = str(o)
        sub = str(s)

        if rfc3987.match(pred) and (pred.startswith('http://') or pred.startswith('https://')):
            yield pred
        if rfc3987.match(obj) and (obj.startswith('http://') or obj.startswith('https://')):
            yield obj
        if rfc3987.match(sub) and (sub.startswith('http://') or sub.startswith('https://')):
            yield sub

def dereference_from_endpoint(iri):
    log = logging.getLogger(__name__)
    endpoint_iri = Config.LOOKUP_ENDPOINT
    if not (rfc3987.match(iri) and (iri.startswith('http://') or iri.startswith('https://'))):
        return None

    log.info(f'Dereference {iri} from endpoint {endpoint_iri}')
    store = SPARQLStore(endpoint_iri, True, True, _node_to_sparql,
                        'application/rdf+xml',
                        headers={'User-Agent': user_agent})
    f = rdflib.Graph(store=store)
    f.open(endpoint_iri)
    # for cube and ruian we need 3 levels (nested optionals are a must, otherwise the query will not finish)
    query = f'CONSTRUCT {{<{iri}> ?p1 ?o1. ?o1 ?p2 ?o2. ?o2 ?p3 ?o3.}} WHERE {{ <{iri}> ?p1 ?o1. OPTIONAL {{?o1 ?p2 ?o2. OPTIONAL {{?o2 ?p3 ?o3.}} }} }}'

    g = rdflib.ConjunctiveGraph()
    try:
        with TimedBlock('dereference_from_endpoint.construct'):
            g = f.query(query).graph
    except SPARQLWrapper.SPARQLExceptions.QueryBadFormed:
        log.error(f'Dereference {iri} from endpoint failed. Query:\n{query}\n\n')
    except (rdflib.query.ResultException, requests.exceptions.HTTPError):
        log.error(f'Failed to dereference {iri}: ResultException or HTTP Error')
    except ValueError as e:
        log.error(f'Failed to dereference {iri}: {e!s}, query: {query}')
    return g

class FailedDereference(ValueError):
    pass

def dereference_one(iri_to_dereference):
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    log.debug(f'Dereference: {iri_to_dereference}')
    if not (rfc3987.match(iri_to_dereference) and iri_to_dereference.startswith("http")):
        raise FailedDereference()
    monitor.log_dereference_request()
    try:
        try:
            r = fetch(iri_to_dereference, log, red)
        except RobotsRetry as e:
            # self.retry(countdown=e.delay)
            log.warning(f'Should retry with delay of {e.delay}, will lookup in endpoint: {iri_to_dereference}')
            return dereference_from_endpoint(iri_to_dereference)
        except requests.exceptions.HTTPError:
            log.debug(f'HTTP Error dereferencing, will lookup in endpoint: {iri_to_dereference}')  # this is a 404 or similar, not worth retrying
            return dereference_from_endpoint(iri_to_dereference)
        except requests.exceptions.RequestException:
            # self.retry(exc=e)
            log.debug(f'Failed to dereference (RequestException fetching): {iri_to_dereference}')
            return dereference_from_endpoint(iri_to_dereference)
        except requests.exceptions.ChunkedEncodingError:
            # self.retry(exc=e)
            log.debug(f'Failed to dereference (ChunkedEncodingError fetching): {iri_to_dereference}')
            return dereference_from_endpoint(iri_to_dereference)
        except Skip:
            monitor.log_dereference_processed()
            return dereference_from_endpoint(iri_to_dereference)

        try:
            test_content_length(iri_to_dereference, r, log)
            guess, _ = guess_format(iri_to_dereference, r, log, red)
        except Skip:
            log.debug(f'Attempt to lookup {iri_to_dereference} in endpoint')
            return dereference_from_endpoint(iri_to_dereference)

        try:
            content = get_content(iri_to_dereference, r, red)
            if content is None:
                log.debug(f'No content: {iri_to_dereference}')
                return dereference_from_endpoint(iri_to_dereference)
            else:
                content.encode('utf-8')
            return load_graph(iri_to_dereference, content, guess)
        except requests.exceptions.ChunkedEncodingError:
            # task.retry(exc=e)
            log.warning(f'Failed to dereference (ChunkedEncodingError getting content): {iri_to_dereference}')
            return dereference_from_endpoint(iri_to_dereference)

        monitor.log_dereference_processed()
    except:
        log.exception(f'Failed to dereference: {iri_to_dereference}')
        return dereference_from_endpoint(iri_to_dereference)

def expand_graph_with_dereferences(graph, iri_distr):
    log = logging.getLogger(__name__)
    for iri_to_dereference in frozenset(get_iris_to_dereference(graph, iri_distr)):
        try:
            sub_graph = dereference_one(iri_to_dereference)
            if sub_graph is not None:
                graph += sub_graph
        except UnicodeDecodeError:
            log.exception(f'Failed to dereference {iri_to_dereference} (UnicodeDecodeError)')
        except FailedDereference:
            pass
    return graph

def do_process(iri, task, is_prio, force):
    """Analyze an RDF distribution under given IRI."""
    log = logging.getLogger(__name__)

    if filter(iri):
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

    if iri.endswith('sparql'):
        log.info(f'Guessing it is a SPARQL endpoint: {iri!s}')
        monitor.log_processed()
        return
        #return process_endpoint.si(iri).apply_async(queue='low_priority')

    log.info(f'Processing {iri!s}')

    try:
        try:
            r = fetch(iri, log, red)
        except RobotsRetry as e:
            task.retry(countdown=e.delay)
        except requests.exceptions.HTTPError:
            log.exception('HTTP Error') # this is a 404 or similar, not worth retrying
            monitor.log_processed()
            raise
        except requests.exceptions.RequestException as e:
            task.retry(exc=e)
        except requests.exceptions.ChunkedEncodingError as e:
            task.retry(exc=e)
        except Skip:
            monitor.log_processed()
            return
        except gevent.timeout.Timeout:
            monitor.log_processed()
            return

        try:
            test_content_length(iri, r, log)
            guess, priority = guess_format(iri, r, log, red)
            is_prio = is_prio | priority
        except Skip:
            monitor.log_processed()
            return

        decompress_task = decompress
        if is_prio:
            decompress_task = decompress_prio
        if guess in ['application/x-7z-compressed', 'application/x-zip-compressed', 'application/zip']:
            #delegate this into low_priority task
            #return decompress_task.si(iri, 'zip').apply_async(queue='low_priority')
            return
        elif guess in ['application/gzip', 'application/x-gzip']:
            #return decompress_task.si(iri, 'gzip').apply_async(queue='low_priority')
            return

        try:
            log.debug(f'Analyze and index {iri} immediately')
            content = get_content(iri, r, red)
            if content is None:
                log.warn(f'No content for {iri}')
                return
            else:
                content.encode('utf-8')
            graph = None
            with TimedBlock("process.load"):
                graph = load_graph(iri, content, guess)
            with TimedBlock("process.dereference"):
                try:
                    graph = expand_graph_with_dereferences(graph, iri)
                except ValueError:
                    log.exception(f'Failed to expand dereferenes: {iri}')
            with TimedBlock("process.analyze_and_index"):
                do_analyze_and_index(graph, iri, red)
            log.debug(f'Done analyze and index {iri} (immediate)')
            monitor.log_processed()
        except SizeException:
            log.warn(f'File is too large: {iri}')
            monitor.log_processed()
            raise
        except requests.exceptions.ChunkedEncodingError as e:
            task.retry(exc=e)
    except rdflib.exceptions.ParserError as e:
        log.warning(f'Failed to parse {iri!s} - likely not an RDF: {e!s}')
        monitor.log_processed()
    except:
        log.exception(f'Failed to get {iri!s}')
        #red.sadd('stat:failed', str(iri))
        monitor.log_processed()

# these 2 tasks do the same in low priority queue, however, non-priority one is time constrained AND the processing
# tasks after decompression will be scheduled into priority queue
@celery.task(bind=True, time_limit=60, base=TrackableTask)
def decompress(self, iri, type):
    with TimedBlock("process.decompress"):
        do_decompress(self, iri, type)

@celery.task(bind=True, base=TrackableTask)
def decompress_prio(self, iri, type):
    with TimedBlock("process.decompress"):
        do_decompress(self, iri, type, True)

def do_decompress(task, iri, type, is_prio=False):
    #we cannot pass request here, so we have to make a new one
    log = logging.getLogger(__name__)
    red = task.redis

    key = root_name[KeyRoot.DISTRIBUTIONS]
    try:
        r = fetch(iri, log, red)
    except RobotsRetry as e:
        task.retry(countdown=e.delay)
    except requests.exceptions.RequestException as e:
        task.retry(exc=e)

    def gen_iri_guess(iri, r):
        deco = {
            'zip': decompress_7z,
            'gzip': decompress_gzip
        }
        for sub_iri in deco[type](iri, r, red):
            if red.pfadd(key, sub_iri) == 0:
                log.debug(f'Skipping distribution as it was recently analyzed: {sub_iri!s}')
                continue

            sub_key = data_key(sub_iri)
            red.expire(sub_key, expiration[KeyRoot.DATA])

            if sub_iri.endswith('/data'):  # extracted a file without a filename
                yield sub_iri, 'text/plain'  # this will allow for analysis to happen
                continue

            try:
                guess, _ = guess_format(sub_iri, r, log, red)
            except Skip:
                continue
            if guess is None:
                log.warn(f'Unknown format after decompression: {sub_iri}')
                red.expire(data_key(sub_iri), 1)
            else:
                yield sub_iri, guess

    def gen_tasks(iri, r):
        lst = []
        try:
            for x in gen_iri_guess(iri, r): #this does the decompression
                lst.append(x)
        except SizeException as e:
            log.warn(f'One of the files in archive {iri} is too large ({e.name})')
            for sub_iri, _ in lst:
                log.debug(f'Expire {sub_iri}')
                red.expire(data_key(sub_iri), 1)
        except TypeError:
            log.exception(f'iri: {iri!s}, type: {type!s}')
        else:
            for sub_iri, guess in lst:
                yield analyze_and_index.si(sub_iri, guess)

    g = group([t for t in gen_tasks(iri, r)])
    if is_prio:
        return g.apply_async(queue='high_priority')
    else:
        return g.apply_async()
