"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging
from typing import Any, Collection, Generator, Iterable, List, Set, Tuple

import rdflib
import redis
from celery import group
from celery.result import AsyncResult
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


def _query_parent(dataset_iri: str, endpoint: str) -> Generator[str, None, None]:
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
dsdistr, distrds = ds_distr()
distributions: List[str] = []
distributions_priority: List[str] = []


def _get_queue(distribution: Any, graph: rdflib.Graph) -> List[str]:
    queue = distributions
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
    return queue


def _distribution_extractor(distribution: Any, dataset: Any, effective_dataset: Any, graph: rdflib.Graph, pipe: redis.client.Pipeline, log: logging.Logger) -> Tuple[Set[str], List[str]]:
    log.debug(f'Distr: {distribution!s}')
    queue = _get_queue(distribution, graph)

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
    return endpoints, downloads


def _dataset_extractor(dataset: Any, lookup_endpoint: str, graph: rdflib.Graph, log: logging.Logger, pipe: redis.client.Pipeline) -> bool:
    log.debug(f'DS: {dataset!s}')
    effective_dataset = dataset
    distribution = False

    for parent in _query_parent(dataset, lookup_endpoint):
        log.debug(f'{parent!s} is a series containing {dataset!s}')
        effective_dataset = parent

    # DCAT Distribution
    endpoints, downloads = set(), []
    for distribution in graph.objects(dataset, dcat.distribution):
        local_endpoints, local_downloads = _distribution_extractor(distribution, dataset, effective_dataset, graph, pipe, log)
        endpoints.update(local_endpoints)
        downloads.extend(local_downloads)
    for endpoint in endpoints:
        pipe.sadd(dataset_endpoint(str(effective_dataset)), endpoint)

    if not downloads and endpoints:
        log.warning(f'Only endpoint without distribution for {dataset!s}')

    return distribution


def _dcat_extractor(graph: rdflib.Graph, red: redis.Redis, log: logging.Logger, force: bool, graph_iri: str, lookup_endpoint: str) -> None:
    log.debug(f'Extracting distributions from {graph_iri}')
    # DCAT dataset
    with TimedBlock('dcat_extractor'):
        distribution = False
        with red.pipeline() as pipe:
            for dataset in graph.subjects(RDF.type, dcat.Dataset):
                distribution_local = _dataset_extractor(dataset, lookup_endpoint, graph, log, pipe)
                distribution = distribution or distribution_local
            pipe.execute()
    # TODO: possibly scan for service description as well
        if distribution:
            GenericAnalyzer().get_details(graph)  # extrakce labelu - heavy!
    tasks = [process_priority.si(a, force) for a in distributions_priority]
    tasks.extend(process.si(a, force) for a in distributions)
    monitor.log_tasks(len(tasks))
    group(tasks).apply_async()


@celery.task(base=TrackableTask)
def inspect_graph(endpoint_iri: str, graph_iri: str, force: bool) -> None:
    red = inspect_graph.redis
    _do_inspect_graph(graph_iri, force, red, endpoint_iri)


def _do_inspect_graph(graph_iri: str, force: bool, red: redis.Redis, endpoint_iri: str) -> None:
    log = logging.getLogger(__name__)
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        _dcat_extractor(inspector.process_graph(graph_iri), red, log, force, graph_iri, endpoint_iri)
    except (rdflib.query.ResultException, HTTPError):
        log.error(f'Failed to inspect graph {graph_iri}: ResultException or HTTP Error')
    monitor.log_inspected()


@celery.task
def inspect_graphs(graphs: Iterable[str], endpoint_iri: str, force: bool) -> None:
    red = inspect_graphs.redis
    for graph in graphs:
        _do_inspect_graph(graph, force, red, endpoint_iri)


def _multiply(item: Any, times: int):
    for _ in range(times):
        yield item


def _split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


@celery.task(base=TrackableTask)
def batch_inspect(endpoint_iri: str, graphs: Collection[str], force: bool, chunks: int) -> AsyncResult:
    items = len(graphs)
    monitor.log_graph_count(items)
    logging.getLogger(__name__).info(f'Batch of {items} graphs in {endpoint_iri}')
    return inspect_graph.chunks(zip(_multiply(endpoint_iri, items), graphs, _multiply(force, items)), chunks).apply_async()
