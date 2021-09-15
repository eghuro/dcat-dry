"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging
from enum import IntEnum
from typing import Dict, Generator, List, Set

import rdflib
import redis
from celery import group
from rdflib import Graph
from rdflib.plugins.sparql.processor import prepareQuery
from requests.exceptions import HTTPError

from tsa.analyzer import GenericAnalyzer
from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.redis import dataset_endpoint, ds_distr
from tsa.tasks.common import TrackableTask
from tsa.tasks.process import filter_iri, process, process_priority
from tsa.util import check_iri


class Query(IntEnum):
    PARENT_A = 0
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
    # Query.PARENT_B: prepareQuery('SELECT ?parent WHERE { ?parent <http://purl.org/dc/terms/hasPart> ?dataset }'),
    # Query.PARENT_C: prepareQuery('SELECT ?parent WHERE { ?dataset <http://www.w3.org/ns/dcat#inSeries> ?parent }'),
    Query.MEDIA_TYPE: prepareQuery('SELECT ?media WHERE { ?distribution  <http://www.w3.org/ns/dcat#mediaType> ?media }'),
    Query.FORMAT: prepareQuery('SELECT ?format WHERE {  ?distribution <http://purl.org/dc/terms/format> ?format }'),
    Query.NKOD_MEDIA_TYPE: prepareQuery('SELECT ?format WHERE { ?distribution <https://data.gov.cz/slovník/nkod/mediaTyp> ?format }'),
    Query.DOWNLOAD_URL: prepareQuery('SELECT ?download WHERE { ?distribution <http://www.w3.org/ns/dcat#downloadURL> ?download }'),
    Query.ACCESS_SERVICE: prepareQuery('SELECT ?access WHERE { ?distribution <http://www.w3.org/ns/dcat#accessService> ?access }'),
    Query.ENDPOINT_URL: prepareQuery('SELECT ?endpoint WHERE { ?distribution <http://www.w3.org/ns/dcat#endpointURL> ?endpoint }'),
    Query.DISTRIBUTION: prepareQuery('SELECT ?distribution WHERE { ?dataset <http://www.w3.org/ns/dcat#distribution> ?distribution }'),
    Query.DATASET: prepareQuery('SELECT ?dataset WHERE { ?dataset a <http://www.w3.org/ns/dcat#Dataset> }')
}


class QueueType(IntEnum):
    DISTRIBUTIONS = 0
    PRIORITY = 1


class Context:
    queues: Dict[QueueType, List[str]] = {
        QueueType.DISTRIBUTIONS: [],
        QueueType.PRIORITY: []
    }
    graph_iri: str = ''
    endpoints: Set[str] = set()

    def __init__(self, red: redis.Redis, log: logging.Logger):
        self.red = red
        self.log = log


def _query_parent(dataset_iri: str, graph: Graph, log: logging.Logger) -> Generator[str, None, None]:
    for query in [Query.PARENT_A]:
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


def _get_queue(distribution: str, graph: rdflib.Graph, context: Context) -> QueueType:
    # put RDF distributions into a priority queue
    for row in graph.query(prepared_queries[Query.MEDIA_TYPE], initBindings={'distribution': distribution}):  # .format(distribution)):
        media = str(row['media'])
        if media in media_priority:
            return QueueType.PRIORITY

    for row in graph.query(prepared_queries[Query.FORMAT], initBindings={'distribution': distribution}):
        distribution_format = str(row['format'])
        if distribution_format in format_priority:
            return QueueType.PRIORITY

    # data.gov.cz specific
    for row in graph.query(prepared_queries[Query.NKOD_MEDIA_TYPE], initBindings={'distribution': distribution}):
        distribution_format = str(row['format'])
        if 'rdf' in str(distribution_format):
            return QueueType.PRIORITY

    return QueueType.DISTRIBUTIONS


def _distribution_extractor(distribution: str, dataset: str, effective_dataset: str, graph: rdflib.Graph, context: Context) -> None:
    context.log.debug('Distr: %s', str(distribution))
    queue = _get_queue(distribution, graph, context)

    with context.red.pipeline() as pipe:
        for row in graph.query(prepared_queries[Query.DOWNLOAD_URL], initBindings={'distribution': distribution}):
            download_url = str(row['download']).strip()
            # log.debug(f'Down: {download_url!s}')
            if check_iri(str(download_url)) and not filter_iri(str(download_url)):
                if download_url.endswith('/sparql'):
                    context.log.info('Guessing %s is a SPARQL endpoint, will use for dereferences from DCAT dataset %s (effective: %s)', str(download_url), str(dataset), str(effective_dataset))
                    context.endpoints.add(download_url)
                else:
                    context.log.info('Distribution %s from DCAT dataset %s (effective: %s)', str(download_url), str(dataset), str(effective_dataset))
                    context.queues[queue].append(download_url)
                    pipe.sadd(f'{dsdistr}:{str(effective_dataset)}', str(download_url))
                    pipe.sadd(f'{distrds}:{str(download_url)}', str(effective_dataset))
            else:
                context.log.debug('%s is not a valid download URL', str(download_url))
        pipe.execute()

    # scan for DCAT2 data services here as well
    for row in graph.query(prepared_queries[Query.ACCESS_SERVICE], initBindings={'distribution': distribution}):
        access = str(row['access'])
        context.log.info('Service: %s', str(access))
        for row in graph.query(prepared_queries[Query.ENDPOINT_URL], initBindings={'distribution': access}):  # .format(access)):
            endpoint = str(row['endpoint'])
            if check_iri(str(endpoint)):
                context.log.debug('Endpoint %s from DCAT dataset %s', str(endpoint), str(dataset))
                context.endpoints.add(endpoint)


def _dataset_extractor(dataset: str, graph: rdflib.Graph, context: Context) -> None:
    context.log.debug('DS: %s', str(dataset))
    effective_dataset = dataset

    for parent in _query_parent(dataset, graph, context.log):
        context.log.debug('%s is a series containing %s', parent, str(dataset))
        effective_dataset = parent

    # DCAT Distribution
    for row in graph.query(prepared_queries[Query.DISTRIBUTION], initBindings={'dataset': dataset}):
        distribution = str(row['distribution'])
        _distribution_extractor(distribution, dataset, effective_dataset, graph, context)

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
    if len(context.queues[QueueType.PRIORITY]) + len(context.queues[QueueType.DISTRIBUTIONS]) > 0:
        GenericAnalyzer().get_details(graph)  # extrakce labelu - heavy!
        tasks = [process_priority.si(a, False) for a in context.queues[QueueType.PRIORITY]]
        tasks.extend(process.si(a, False) for a in context.queues[QueueType.DISTRIBUTIONS])
        group(tasks).apply_async()


@celery.task(base=TrackableTask, ignore_result=True)
def inspect_graph(endpoint_iri: str, graph_iri: str, force: bool) -> None:
    log = logging.getLogger(__name__)
    context = Context(inspect_graph.redis, log)
    context.graph_iri = graph_iri
    endpoint_iri = endpoint_iri.strip()
    if not check_iri(endpoint_iri):
        return
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        _dcat_extractor(inspector.process_graph(graph_iri), context)
    except (rdflib.query.ResultException, HTTPError):
        log.error('Failed to inspect graph %s: ResultException or HTTP Error', graph_iri)
