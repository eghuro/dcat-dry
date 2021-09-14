"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging
from enum import IntEnum
from typing import Any, Collection, Generator, List, Set, Tuple

import rdflib
import redis
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
distributions: List[str] = []
distributions_priority: List[str] = []


def _is_priority(distribution: str, graph: rdflib.Graph) -> bool:
    # put RDF distributions into a priority queue
    for row in graph.query(prepared_queries[Query.MEDIA_TYPE], initBindings={'distribution': distribution}):  # .format(distribution)):
        media = str(row['media'])
        if media in media_priority:
            return True

    for row in graph.query(prepared_queries[Query.FORMAT], initBindings={'distribution': distribution}):
        distribution_format = str(row['format'])
        if distribution_format in format_priority:
            return True

    # data.gov.cz specific
    for row in graph.query(prepared_queries[Query.NKOD_MEDIA_TYPE], initBindings={'distribution': distribution}):
        distribution_format = str(row['format'])
        if 'rdf' in str(distribution_format):
            return True

    return False


def _distribution_extractor(distribution: str, dataset: str, effective_dataset: str, graph: rdflib.Graph, red: redis.Redis, log: logging.Logger) -> Tuple[Set[str], List[str], bool]:
    log.debug('Distr: %s', str(distribution))
    if _is_priority(distribution, graph):
        queue = distributions_priority
    else:
        queue = distributions

    # download URL to files
    downloads = []
    endpoints = set()
    has_distribution = False
    with red.pipeline() as pipe:
        for row in graph.query(prepared_queries[Query.DOWNLOAD_URL], initBindings={'distribution': distribution}):
            download_url = str(row['download'])
            # log.debug(f'Down: {download_url!s}')
            if check_iri(str(download_url)) and not filter_iri(str(download_url)):
                has_distribution = True
                if download_url.endswith('/sparql'):
                    log.info('Guessing %s is a SPARQL endpoint, will use for dereferences from DCAT dataset %s (effective: %s)', str(download_url), str(dataset), str(effective_dataset))
                    endpoints.add(download_url)
                else:
                    downloads.append(download_url)
                    log.debug('Distribution %s from DCAT dataset %s (effective: %s)', str(download_url), str(dataset), str(effective_dataset))
                    queue.append(download_url)
                    pipe.sadd(f'{dsdistr}:{str(effective_dataset)}', str(download_url))
                    pipe.sadd(f'{distrds}:{str(download_url)}', str(effective_dataset))
            else:
                log.debug('%s is not a valid download URL', str(download_url))
        pipe.execute()

    # scan for DCAT2 data services here as well
    for row in graph.query(prepared_queries[Query.ACCESS_SERVICE], initBindings={'distribution': distribution}):
        access = str(row['access'])
        log.debug('Service: %s', str(access))
        for row in graph.query(prepared_queries[Query.ENDPOINT_URL], initBindings={'distribution': access}):  #  .format(access)):
            endpoint = str(row['endpoint'])
            if check_iri(str(endpoint)):
                log.debug('Endpoint %s from DCAT dataset %s', str(endpoint), str(dataset))
                endpoints.add(endpoint)
                has_distribution = True
    return endpoints, downloads, has_distribution


def _dataset_extractor(dataset: str, lookup_endpoint: str, graph: rdflib.Graph, log: logging.Logger, red: redis.Redis) -> bool:
    log.debug('DS: %s', str(dataset))
    effective_dataset = dataset
    has_distribution = False

    # DCAT Distribution
    endpoints, downloads = set(), []
    for row in graph.query(prepared_queries[Query.DISTRIBUTION], initBindings={'dataset': dataset}):
        distribution = str(row['distribution'])
        before = len(distributions) + len(distributions_priority)
        local_endpoints, local_downloads, local_has_distribution = _distribution_extractor(distribution, dataset, effective_dataset, graph, red, log)
        after = len(distributions) + len(distributions_priority)
        if before == after:
            log.warning('No distributions extracted from %s', dataset)
        endpoints.update(local_endpoints)
        downloads.extend(local_downloads)
        has_distribution = has_distribution or local_has_distribution

    if not downloads and endpoints:
        log.warning('Only endpoint without distribution for %s', str(dataset))

    if has_distribution or len(endpoints) > 0:
        for parent in _query_parent(dataset, lookup_endpoint, log):
            log.debug('%s is a series containing %s', parent, str(dataset))
            effective_dataset = parent

    with red.pipeline() as pipe:
        for endpoint in endpoints:
            pipe.sadd(dataset_endpoint(str(effective_dataset)), endpoint)
        pipe.execute()

    return has_distribution


def _dcat_extractor(graph: rdflib.Graph, red: redis.Redis, log: logging.Logger, force: bool, graph_iri: str, lookup_endpoint: str) -> None:
    log.debug('Extracting distributions from %s', graph_iri)
    # DCAT dataset
    distribution = False
    for row in graph.query(prepared_queries[Query.DATASET]):
        dataset = str(row['dataset'])
        distribution_local = _dataset_extractor(dataset, lookup_endpoint, graph, log, red)
        distribution = distribution or distribution_local
# TODO: possibly scan for service description as well
    if distribution:
        GenericAnalyzer().get_details(graph)  # extrakce labelu - heavy!
    tasks = [process_priority.si(a, force) for a in distributions_priority]
    tasks.extend(process.si(a, force) for a in distributions)
    group(tasks).apply_async()


@celery.task(base=TrackableTask, ignore_result=True, bind=True)
def inspect_graph(self, endpoint_iri: str, graph_iri: str, force: bool) -> None:
    red = inspect_graph.redis
    log = logging.getLogger(__name__)
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        _dcat_extractor(inspector.process_graph(graph_iri), red, log, force, graph_iri, endpoint_iri)
    except (rdflib.query.ResultException, HTTPError):
        log.error('Failed to inspect graph %s: ResultException or HTTP Error', graph_iri)
    except ValueError as exc:
        raise self.retry(exc=exc, countdown=60)


def _multiply(item: Any, times: int):
    for _ in range(times):
        yield item


@celery.task(base=TrackableTask, ignore_result=True)
def batch_inspect(endpoint_iri: str, graphs: Collection[str], force: bool, chunks: int) -> AsyncResult:
    items = len(graphs)
    logging.getLogger(__name__).info('Batch of %d graphs in %s', items, endpoint_iri)
    return inspect_graph.chunks(zip(_multiply(endpoint_iri, items), graphs, _multiply(force, items)), chunks).apply_async()
    # 1000 graphs into 10 chunks of 100
