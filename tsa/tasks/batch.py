"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging

import rdflib
from celery import group
from rdflib import Graph, Namespace
from rdflib.namespace import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from requests.exceptions import HTTPError

from tsa.analyzer import GenericAnalyzer
from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.monitor import TimedBlock, monitor
from tsa.redis import dataset_endpoint, ds_distr
from tsa.robots import USER_AGENT
from tsa.tasks.common import TrackableTask
from tsa.tasks.process import filter_iri, process, process_priority
from tsa.util import test_iri


def query_parent(dataset_iri, endpoint):
    opts = [f'<{dataset_iri!s}> <http://purl.org/dc/terms/isPartOf> ?parent',
            f'?parent <http://purl.org/dc/terms/hasPart> <{dataset_iri!s}> ',
            f'<{dataset_iri!s}> <http://www.w3.org/ns/dcat#inSeries> ?parent',
            ]
    graph = Graph(SPARQLStore(endpoint, headers={'User-Agent': USER_AGENT}))
    for opt in opts:
        query = f'SELECT ?parent WHERE {{ {opt} }}'
        for parent in graph.query(query):
            parent_iri = str(parent['parent'])
            yield str(parent_iri)


def _dcat_extractor(graph, red, log, force, graph_iri, lookup_endpoint):
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
    ])  # IANA
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
    ])  # EU
    queue = distributions

    log.debug(f'Extracting distributions from {graph_iri}')
    # DCAT dataset
    with TimedBlock('dcat_extractor'):
        dsdistr, distrds = ds_distr()
        distribution = False
        with red.pipeline() as pipe:
            for dataset in graph.subjects(RDF.type, dcat.Dataset):
                log.debug(f'DS: {dataset!s}')
                effective_dataset = dataset

                for parent in query_parent(dataset, lookup_endpoint):
                    log.debug(f'{parent!s} is a series containing {dataset!s}')
                    effective_dataset = parent

                # DCAT Distribution
                for distribution in graph.objects(dataset, dcat.distribution):
                    log.debug(f'Distr: {distribution!s}')
                    # put RDF distributions into a priority queue
                    for media in graph.objects(distribution, dcat.mediaType):
                        if str(media) in media_priority:
                            queue = distributions_priority

                    for distribution_format in graph.objects(distribution, dcterms.format):
                        if str(distribution_format) in format_priority:
                            queue = distributions_priority

                    # data.gov.cz specific
                    for distribution_format in graph.objects(distribution, nkod.mediaType):
                        if 'rdf' in str(distribution_format):
                            queue = distributions_priority

                    # download URL to files
                    downloads = []
                    endpoints = set()
                    for download_url in graph.objects(distribution, dcat.downloadURL):
                        # log.debug(f'Down: {download_url!s}')
                        if test_iri(str(download_url)) and not filter_iri(str(download_url)):
                            if download_url.endswith('/sparql'):
                                log.info(f'Guessing {download_url} is a SPARQL endpoint, will use for dereferences from DCAT dataset {dataset!s} (effective: {effective_dataset!s})')
                                endpoints.add(download_url)
                            else:
                                downloads.append(download_url)
                                distribution = True
                                log.debug(f'Distribution {download_url!s} from DCAT dataset {dataset!s} (effective: {effective_dataset!s})')
                                queue.append(download_url)
                                pipe.sadd(f'{dsdistr}:{str(effective_dataset)}', str(download_url))
                                pipe.sadd(f'{distrds}:{str(download_url)}', str(effective_dataset))
                        else:
                            log.debug(f'{download_url!s} is not a valid download URL')

                    # scan for DCAT2 data services here as well
                    for access in graph.objects(distribution, dcat.accessService):
                        log.debug(f'Service: {access!s}')
                        for endpoint in graph.objects(access, dcat.endpointURL):
                            if test_iri(str(endpoint)):
                                log.debug(f'Endpoint {endpoint!s} from DCAT dataset {dataset!s}')
                                endpoints.add(endpoint)

                    for endpoint in endpoints:
                        pipe.sadd(dataset_endpoint(str(effective_dataset)), endpoint)

                    if not downloads and endpoints:
                        log.warning(f'Only endpoint without distribution for {dataset!s}')

            pipe.execute()
    # TODO: possibly scan for service description as well
        if distribution:
            GenericAnalyzer().get_details(graph)  # extrakce labelu - heavy!
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
#        graph = rdflib.ConjunctiveGraph()
#       graph.parse(data=red.get(key), format='n3')
#        red.delete(key)
#    except rdflib.plugin.PluginException:
#        log.debug('Failed to parse graph')
#        return None
#
#    return _dcat_extractor(graph, red, log, False, key, None)


@celery.task(base=TrackableTask)
def inspect_graph(endpoint_iri, graph_iri, force):
    red = inspect_graph.redis
    return do_inspect_graph(graph_iri, force, red, endpoint_iri)


def do_inspect_graph(graph_iri, force, red, endpoint_iri):
    log = logging.getLogger(__name__)
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        _dcat_extractor(inspector.process_graph(graph_iri), red, log, force, graph_iri, endpoint_iri)
    except (rdflib.query.ResultException, HTTPError):
        log.error(f'Failed to inspect graph {graph_iri}: ResultException or HTTP Error')
    monitor.log_inspected()


@celery.task
def inspect_graphs(graphs, endpoint_iri, force):
    red = inspect_graphs.redis
    for graph in graphs:
        do_inspect_graph(graph, force, red, endpoint_iri)


def multiply(item, times):
    for _ in range(times):
        yield item


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


@celery.task(base=TrackableTask)
def batch_inspect(endpoint_iri, graphs, force, chunks):
    items = len(graphs)
    monitor.log_graph_count(items)
    logging.getLogger(__name__).info(f'Batch of {items} graphs in {endpoint_iri}')
    return inspect_graph.chunks(zip(multiply(endpoint_iri, items), graphs, multiply(force, items)), chunks).apply_async()
