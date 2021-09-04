"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging

import rdflib
import redis
import rfc3987
from celery import group
from celery.result import AsyncResult
from rdflib import Namespace
from rdflib.namespace import RDF
from requests.exceptions import HTTPError

from tsa.analyzer import GenericAnalyzer
from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.extensions import redis_pool
from tsa.monitor import TimedBlock, monitor
from tsa.redis import ds_distr
from tsa.tasks.common import TrackableTask
from tsa.tasks.process import filter, process, process_priority


def on_error(x):
    pass

def _dcat_extractor(g, red, log, force, graph_iri):
    distributions, distributions_priority = [], []
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

    log.debug(f'Extracting distributions from {graph_iri}')
    #DCAT dataset
    with TimedBlock("dcat_extractor"):
        dsdistr, distrds = ds_distr()
        distribution = False
        with red.pipeline() as pipe:
            for ds in g.subjects(RDF.type, dcat.Dataset):
                log.debug(f'DS: {ds!s}')
                #dataset titles (possibly multilang)
                #for t in g.objects(ds, dcterms.title):
                #    key = ds_title(ds, t.language)
                #    red.set(key, t.value)
                # done in GenericAnalyzer().get_details(g) ?

                #NKOD specific
                #dataset keywords, possibly multilang, strip language tag, pick 'Číselník' only, create a set of those
                #for keyword in g.objects(ds, dcat.keyword):
                #    if keyword.value.lower() == 'číselník':
                #        red.hset(root_name[KeyRoot.CODELISTS], ds, t.value)
                #TODO: extract data commented out above in postprocessing for interesting DS

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
                    for download_url in g.objects(d, dcat.downloadURL):
                        #log.debug(f'Down: {download_url!s}')
                        if rfc3987.match(str(download_url)) and not filter(str(download_url)):
                            distribution = True
                            log.debug(f'Distribution {download_url!s} from DCAT dataset {ds!s}')
                            queue.append(download_url)
                            pipe.sadd(f'{dsdistr}:{str(ds)}', str(download_url))
                            pipe.sadd(f'{distrds}:{str(download_url)}', str(ds))
                        else:
                            log.debug(f'{download_url!s} is not a valid download URL')

                    # scan for DCAT2 data services here as well
                    #for access in g.objects(d, dcat.accessURL):
                    #    log.debug(f'Access: {access!s}')
                    #    for endpoint in g.objects(access, dcat.endpointURL):
                    #        if rfc3987.match(str(endpoint)):
                    #            log.debug(f'Endpoint {endpoint!s} from DCAT dataset {ds!s}')
                    #            endpoints.append(endpoint)

                    #            pipe.hset(dsdistr, str(ds), str(endpoint))
                    #            pipe.hset(distrds, str(endpoint), str(ds))
                    #        else:
                    #            log.warn(f'{endpoint!s} is not a valid endpoint URL')

            #pipe.sadd('purgeable', dsdistr, distrds)
            # TODO: expire
            pipe.execute()
    # TODO: possibly scan for service description as well
        if distribution:
            GenericAnalyzer().get_details(g)  # extrakce labelu - heavy!
    tasks = [process_priority.si(a, force) for a in distributions_priority]
    #tasks.extend(process_endpoint.si(e, force) for e in endpoints)
    tasks.extend(process.si(a, force) for a in distributions)
    monitor.log_tasks(len(tasks))
    group(tasks).apply_async()


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

    return _dcat_extractor(g, red, log, False, key)


@celery.task(base=TrackableTask)
def inspect_graph(endpoint_iri, graph_iri, force, batch_id):
    inspector = SparqlEndpointAnalyzer(endpoint_iri)
    red = inspect_graph.redis
    return do_inspect_graph(graph_iri, force, batch_id, red, inspector)

def do_inspect_graph(graph_iri, force, batch_id, red, inspector):
    log = logging.getLogger(__name__)
    result = None
    try:
        result = _dcat_extractor(inspector.process_graph(graph_iri), red, log, force, graph_iri)
    except (rdflib.query.ResultException, HTTPError):
        log.error(f'Failed to inspect graph {graph_iri}: ResultException or HTTP Error')
    monitor.log_inspected()
    return result

@celery.task
def inspect_graphs(graphs, endpoint_iri, force, batch_id):
    red = inspect_graphs.redis
    do_inspect_graphs(graphs, endpoint_iri, force, batch_id, red)

def do_inspect_graphs(graphs, endpoint_iri, force, batch_id, red):
    inspector = SparqlEndpointAnalyzer(endpoint_iri)
    return [do_inspect_graph(g, force, batch_id, red, inspector) for g in graphs]


def multiply(item, times):
    for _ in range(times):
        yield item

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


@celery.task(base=TrackableTask)
def batch_inspect(endpoint_iri, graphs, force, batch_id, chunks):
    items = len(graphs)
    monitor.log_graph_count(items)
    logging.getLogger(__name__).info(f'Batch of {items} graphs in {endpoint_iri}')
    return inspect_graph.chunks(zip(multiply(endpoint_iri, items), graphs, multiply(force, items), multiply(batch_id, items)), chunks).apply_async()
