"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging
from enum import IntEnum
from typing import Any, Collection, Generator, List

import rdflib
from celery import group
from celery.result import AsyncResult
from rdflib import Graph
from rdflib.plugins.sparql.processor import prepareQuery
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from requests.exceptions import HTTPError

from tsa.analyzer import GenericAnalyzer
from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.redis import dataset_endpoint, ds_distr
from tsa.robots import USER_AGENT, session
from tsa.tasks.common import TrackableTask
from tsa.tasks.process import filter_iri, process, process_priority
from tsa.util import check_iri


class Query(IntEnum):
    PARENT_A = 0
    PARENT_B = 9
    PARENT_C = 10
    MEDIA_TYPE = 1
    FORMAT = 2
    NKOD_MEDIA_TYPE = 3
    DOWNLOAD_URL = 4
    ACCESS_SERVICE = 5
    ENDPOINT_URL = 6
    DISTRIBUTION = 7
    DATASET = 8


prepared_queries = {
    Query.PARENT_A: prepareQuery('SELECT ?parent WHERE { ?dataset  <http://purl.org/dc/terms/isPartOf> ?parent }'),
    Query.PARENT_B: prepareQuery('SELECT ?parent WHERE { ?parent <http://purl.org/dc/terms/hasPart> ?dataset }'),
    Query.PARENT_C: prepareQuery('SELECT ?parent WHERE { ?dataset <http://www.w3.org/ns/dcat#inSeries> ?parent }'),
    Query.MEDIA_TYPE: prepareQuery('SELECT ?media WHERE { ?distribution  <http://www.w3.org/ns/dcat#mediaType> ?media }'),
    Query.FORMAT: prepareQuery('SELECT ?format WHERE {  ?distribution <http://purl.org/dc/terms/format> ?format }'),
    Query.NKOD_MEDIA_TYPE: prepareQuery('SELECT ?format WHERE { ?distribution <https://data.gov.cz/slovník/nkod/mediaTyp> ?format }'),
    Query.DOWNLOAD_URL: prepareQuery('SELECT ?download WHERE { ?distribution <http://www.w3.org/ns/dcat#downloadURL> ?download }'),
    Query.ACCESS_SERVICE: prepareQuery('SELECT ?access WHERE { ?distribution <http://www.w3.org/ns/dcat#accessService> ?access }'),
    Query.ENDPOINT_URL: prepareQuery('SELECT ?endpoint WHERE { ?distribution <http://www.w3.org/ns/dcat#endpointURL> ?endpoint }'),
    Query.DISTRIBUTION: prepareQuery('SELECT ?distribution WHERE { ?dataset <http://www.w3.org/ns/dcat#distribution> ?distribution }'),
    Query.DATASET: prepareQuery('SELECT ?dataset WHERE { ?dataset a <http://www.w3.org/ns/dcat#Dataset> }')
}


class Context:
    distributions: List[str] = []
    distributions_priority: List[str] = []
    has_distribution = False
    endpoint_iri: str = ''
    graph_iri: str = ''
    red = None
    log = None
    downloads = []
    endpoints = set()

    def __init__(self, red, log):
        self.red = red
        self.log = log


def _query_parent(dataset_iri: str, endpoint: str, log: logging.Logger) -> Generator[str, None, None]:
    graph = Graph(SPARQLStore(endpoint, headers={'User-Agent': USER_AGENT}, session=session))
    for query in [Query.PARENT_A, Query.PARENT_B, Query.PARENT_C]:
        try:
            for parent in graph.query(prepared_queries[query], initBindings={'dataset': dataset_iri}):
                yield str(parent['parent'])
        except ValueError:
            log.debug('Failed to query parent. Query was: %d, dataset: %s', int(query), dataset_iri)  # empty result - no parent


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
dsdistr, distrds = ds_distr()


def _get_queue(distribution: str, graph: rdflib.Graph, context: Context) -> List:
    # put RDF distributions into a priority queue
    for row in graph.query(prepared_queries[Query.MEDIA_TYPE], initBindings={'distribution': distribution}):  # .format(distribution)):
        media = str(row['media'])
        if media in media_priority:
            return context.distributions_priority

    for row in graph.query(prepared_queries[Query.FORMAT], initBindings={'distribution': distribution}):
        distribution_format = str(row['format'])
        if distribution_format in format_priority:
            return context.distributions_priority

    # data.gov.cz specific
    for row in graph.query(prepared_queries[Query.NKOD_MEDIA_TYPE], initBindings={'distribution': distribution}):
        distribution_format = str(row['format'])
        if 'rdf' in str(distribution_format):
            return context.distributions_priority

    return context.distributions


def _distribution_extractor(distribution: str, dataset: str, effective_dataset: str, graph: rdflib.Graph, context: Context) -> None:
    context.log.debug('Distr: %s', str(distribution))
    queue = _get_queue(distribution, graph, context)

    with context.red.pipeline() as pipe:
        for row in graph.query(prepared_queries[Query.DOWNLOAD_URL], initBindings={'distribution': distribution}):
            download_url = str(row['download'])
            # log.debug(f'Down: {download_url!s}')
            if check_iri(str(download_url)) and not filter_iri(str(download_url)):
                if download_url.endswith('/sparql'):
                    context.log.info('Guessing %s is a SPARQL endpoint, will use for dereferences from DCAT dataset %s (effective: %s)', str(download_url), str(dataset), str(effective_dataset))
                    context.endpoints.add(download_url)
                else:
                    context.downloads.append(download_url)
                    context.log.debug('Distribution %s from DCAT dataset %s (effective: %s)', str(download_url), str(dataset), str(effective_dataset))
                    queue.append(download_url)
                    pipe.sadd(f'{dsdistr}:{str(effective_dataset)}', str(download_url))
                    pipe.sadd(f'{distrds}:{str(download_url)}', str(effective_dataset))
            else:
                context.log.debug('%s is not a valid download URL', str(download_url))
        pipe.execute()

    # scan for DCAT2 data services here as well
    for row in graph.query(prepared_queries[Query.ACCESS_SERVICE], initBindings={'distribution': distribution}):
        access = str(row['access'])
        context.log.debug('Service: %s', str(access))
        for row in graph.query(prepared_queries[Query.ENDPOINT_URL], initBindings={'distribution': access}):  #  .format(access)):
            endpoint = str(row['endpoint'])
            if check_iri(str(endpoint)):
                context.log.debug('Endpoint %s from DCAT dataset %s', str(endpoint), str(dataset))
                context.endpoints.add(endpoint)


def _dataset_extractor(dataset: str, graph: rdflib.Graph, context: Context) -> None:
    context.log.debug('DS: %s', str(dataset))
    effective_dataset = dataset

    for parent in _query_parent(dataset, context.endpoint_iri, context.log):
        context.log.debug('%s is a series containing %s', parent, str(dataset))
        effective_dataset = parent

    # DCAT Distribution
    for row in graph.query(prepared_queries[Query.DISTRIBUTION], initBindings={'dataset': dataset}):
        distribution = str(row['distribution'])
        _distribution_extractor(distribution, dataset, effective_dataset, graph, context)

    if len(context.downloads) == 0 and len(context.endpoints) == 0:
        context.log.warning('Only endpoint without distribution for %s', str(dataset))

    with context.red.pipeline() as pipe:
        for endpoint in context.endpoints:
            pipe.sadd(dataset_endpoint(str(effective_dataset)), endpoint)
        pipe.execute()


def _dcat_extractor(graph: rdflib.Graph, context: Context) -> None:
    context.log.debug('Extracting distributions from %s', context.graph_iri)
    # DCAT dataset
    for row in graph.query(prepared_queries[Query.DATASET]):
        dataset = str(row['dataset'])
        _dataset_extractor(dataset, graph, context)

# TODO: possibly scan for service description as well
    GenericAnalyzer().get_details(graph)  # extrakce labelu - heavy!
    tasks = [process_priority.si(a, False) for a in context.distributions_priority]
    tasks.extend(process.si(a, False) for a in context.distributions)
    group(tasks).apply_async()


@celery.task(base=TrackableTask, ignore_result=True)
def inspect_graph(endpoint_iri: str, graph_iri: str, force: bool) -> None:
    red = inspect_graph.redis
    log = logging.getLogger(__name__)
    context = Context(red, log)
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        _dcat_extractor(inspector.process_graph(graph_iri), context)
    except (rdflib.query.ResultException, HTTPError):
        log.error('Failed to inspect graph %s: ResultException or HTTP Error', graph_iri)


def _multiply(item: Any, times: int):
    for _ in range(times):
        yield item


@celery.task(base=TrackableTask, ignore_result=True)
def batch_inspect(endpoint_iri: str, graphs: Collection[str], force: bool, chunks: int) -> AsyncResult:
    items = len(graphs)
    logging.getLogger(__name__).info('Batch of %d graphs in %s', items, endpoint_iri)
    return inspect_graph.chunks(zip(_multiply(endpoint_iri, items), graphs, _multiply(force, items)), chunks).apply_async()
    # 1000 graphs into 10 chunks of 100
