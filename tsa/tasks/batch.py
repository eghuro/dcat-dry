"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import itertools
import logging
from typing import List, Optional, Any

import rdflib
from celery import group
from celery.result import AsyncResult
from requests.exceptions import HTTPError

from tsa.celery import celery
from tsa.dcat import Extractor
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.monitor import TimedBlock, monitor
from tsa.net import RobotsRetry
from tsa.tasks.common import TrackableTask
from tsa.tasks.process import process, process_priority


def _dcat_extractor(
    graph: Optional[rdflib.Graph], force: bool
) -> Optional[AsyncResult]:
    if graph is None:
        return None

    extractor = Extractor(graph)
    with TimedBlock("dcat_extractor"):
        result = group(
            extractor.extract(process_priority, process, force)
        ).apply_async()
        monitor.log_tasks(extractor.tasks)
        return result


@celery.task(base=TrackableTask, bind=True, max_retries=5)
def inspect_graph(
    self, endpoint_iri: str, graph_iri: str, force: bool
) -> Optional[AsyncResult]:
    """
    Extract DCAT datasets from a named graph of an endpoint,
    process them and trigger analysis of the distributions.

    Retry on crawl delay according to the SPARQL endpoint's
    robots.txt.

    :param endpoint_iri: IRI of the SPARQL endpoint
    :param graph_iri: IRI of the named graph to inspect
    :param force: force reprocessing of the distributions
    """
    try:
        return do_inspect_graph(graph_iri, force, endpoint_iri)
    except RobotsRetry as exc:
        self.retry(timeout=exc.delay)


def do_inspect_graph(
    graph_iri: str, force: bool, endpoint_iri: str
) -> Optional[AsyncResult]:
    """
    Extract DCAT datasets from a named graph of an endpoint,
    process them and trigger analysis of the distributions.

    :param endpoint_iri: IRI of the SPARQL endpoint
    :param graph_iri: IRI of the named graph to inspect
    :param force: force reprocessing of the distributions
    """
    log = logging.getLogger(__name__)
    result = None
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        result = _dcat_extractor(inspector.process_graph(graph_iri), force)
    except (rdflib.query.ResultException, HTTPError):
        log.error(
            "Failed to inspect graph %s: ResultException or HTTP Error", graph_iri
        )
    monitor.log_inspected()
    return result


def multiply(item: Any, times: int) -> Any:
    """Yield `item` `times` times."""
    for _ in range(times):
        yield item


@celery.task(base=TrackableTask)
def batch_inspect(endpoint_iri: str, graphs: List[str], chunks: int) -> AsyncResult:
    """
    Inspect a batch of graphs in parallel.

    :param endpoint_iri: IRI of the SPARQL endpoint
    :param graphs: list of named graphs in the endpoint to inspect
    :param chunks: number of tasks to split the batch into (see https://docs.celeryq.dev/en/stable/userguide/canvas.html#chunks)
    :return: AsyncResult Celery task
    """
    items = len(graphs)
    monitor.log_graph_count(items)
    log = logging.getLogger(__name__)
    log.info("Batch of %s graphs in %s", str(items), endpoint_iri)
    return inspect_graph.chunks(
        zip(multiply(endpoint_iri, items), graphs, multiply(True, items)), chunks
    ).apply_async()
