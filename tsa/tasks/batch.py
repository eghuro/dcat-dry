"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging

import redis
import rdflib
import rfc3987
from celery import group
from celery.result import AsyncResult
from rdflib import Namespace
from rdflib.namespace import RDF

from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.tasks.common import TrackableTask, on_complete
from tsa.tasks.process import process, process_endpoint
from tsa.extensions import redis_pool
from tsa.redis import ds_title, ds_distr, KeyRoot, root_name


def _dcat_extractor(g, red, log, force):
    distributions, distributions_priority = [], []
    endpoints = []
    dcat = Namespace('http://www.w3.org/ns/dcat#')
    dcterms = Namespace('http://purl.org/dc/terms/')
    nkod = Namespace('https://data.gov.cz/slovník/nkod/mediaTyp')
    media_priority = set([
        'https://www.iana.org/assignments/media-types/application/rdf+xml',
        'https://www.iana.org/assignments/media-types/application/trig',
        'https://www.iana.org/assignments/media-types/text/n3',
        'https://www.iana.org/assignments/media-types/application/ld+json',
        'https://www.iana.org/assignments/media-types/application/n-triples',
        'https://www.iana.org/assignments/media-types/application/n-quads',
        'https://www.iana.org/assignments/media-types/text/turtle'
    ]) #IANA
    format_priority = set([
        'http://publications.europa.eu/resource/authority/file-type/RDF',
        'http://publications.europa.eu/resource/authority/file-type/RDFA',
        'http://publications.europa.eu/resource/authority/file-type/RDF_N_QUADS',
        'http://publications.europa.eu/resource/authority/file-type/RDF_N_TRIPLES',
        'http://publications.europa.eu/resource/authority/file-type/RDF_TRIG',
        'http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE',
        'http://publications.europa.eu/resource/authority/file-type/RDF_XML',
        'http://publications.europa.eu/resource/authority/file-type/JSON_LD',
        'http://publications.europa.eu/resource/authority/file-type/N3'
    ]) #EU
    queue = distributions

    log.info("Extracting distributions")
    #DCAT dataset
    dsdistr, distrds = ds_distr()
    with red.pipeline() as pipe:
        for ds in g.subjects(RDF.type, dcat.Dataset):
            log.debug(f'DS: {ds!s}')
            #dataset titles (possibly multilang)
            for t in g.objects(ds, dcterms.title):
                key = ds_title(ds, t.language)
                red.set(key, t.value)

            #NKOD specific
            #dataset keywords, possibly multilang, strip language tag, pick 'Číselník' only, create a set of those
            for keyword in g.objects(ds, dcat.keyword):
                if keyword.value.lower() == 'číselník':
                    red.hset(root_name[KeyRoot.CODELISTS], ds, t.value)

            #DCAT Distribution
            for d in g.objects(ds, dcat.distribution):
                log.debug(f'Distr: {d!s}')
                # put RDF distributions into a priority queue
                for media in g.objects(d, dcat.mediaType):
                    if str(media) in media_priority:
                        queue = distributions_priority

                for format in g.objects(d, dcterms.format):
                    if str(format) in format_priority:
                        queue = distributions_priority

                # data.gov.cz specific
                for format in g.objects(d, nkod.mediaType):
                    if 'rdf' in str(format):
                        queue = distributions_priority

                # download URL to files
                for downloadURL in g.objects(d, dcat.downloadURL):
                    log.debug(f'Down: {downloadURL!s}')
                    if rfc3987.match(str(downloadURL)):
                        log.debug(f'Distribution {downloadURL!s} from DCAT dataset {ds!s}')
                        queue.append(downloadURL)
                        pipe.hset(dsdistr, str(ds), str(downloadURL))
                        pipe.hset(distrds, str(downloadURL), str(ds))
                    else:
                        log.warn(f'{access!s} is not a valid download URL')

                # scan for DCAT2 data services here as well
                for access in g.objects(d, dcat.accessURL):
                    log.debug(f'Access: {access!s}')
                    for endpoint in g.objects(access, dcat.endpointURL):
                        if rfc3987.match(str(endpoint)):
                            log.debug(f'Endpoint {endpoint!s} from DCAT dataset {ds!s}')
                            endpoints.append(endpoint)

                            pipe.hset(dsdistr, str(ds), str(endpoint))
                            pipe.hset(distrds, str(endpoint), str(ds))
                        else:
                            log.warn(f'{endpoint!s} is not a valid endpoint URL')

        pipe.sadd('purgeable', dsdistr, distrds)
        # TODO: expire
        pipe.execute()
    # TODO: possibly scan for service description as well

    tasks = [process_priority.si(a, force) for a in distributions_priority]
    tasks.extend(process_endpoint.si(e, force) for e in endpoints)
    tasks.extend(process.si(a, force) for a in distributions)
    return group(tasks).apply_async()


@celery.task(base=TrackableTask)
def inspect_catalog(key):
    """Analyze DCAT datasets listed in the catalog."""
    log = logging.getLogger(__name__)
    red = inspect_catalog.redis

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=red.get(key), format='n3')
        red.delete(key)
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return None

    return _dcat_extractor(g, red, log)


@celery.task(base=TrackableTask)
def inspect_graph(endpoint_iri, graph_iri, force):
    log = logging.getLogger(__name__)
    inspector = SparqlEndpointAnalyzer()
    red = inspect_graph.redis
    return _dcat_extractor(inspector.process_graph(endpoint_iri, graph_iri), red, log, force)


@celery.task()
def cleanup_batches():
    #client = RwlockClient()
    red = redis.Redis(connection_pool=redis_pool)
    root = 'batch:'
    log = logging.getLogger(__name__)
    #rwlock = client.lock('batchLock', Rwlock.WRITE, timeout=Rwlock.FOREVER)
    #if rwlock.status == Rwlock.OK:
    for key in red.keys(f'{root}*'):
        batch_id = key[len(root):]
        for task_id in red.smembers(key):
            task = AsyncResult(task_id)
            if task.state in ['SUCCESS', 'FAILURE']:
                log.warning(f'Removing {task_id} as it is {task.state}')
                red.srem(key, task_id)
        log.warn(f'on_complete from cleanup: {batch_id}')
        on_complete(batch_id, red)

        #client.unlock(rwlock)
    #elif rwlock.status == Rwlock.DEADLOCK:
    #    logging.getLogger(__name__).exception('Deadlock, retrying')
    #    self.retry()
