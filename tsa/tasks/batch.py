"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging
from typing import Optional

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


def test_allowed(url: str) -> bool:  # WTF?
    for prefix in [
        "https://data.cssz.cz",
        "https://rpp-opendata.egon.gov.cz",
        "https://data.mpsv.cz",
        "https://data.mvcr.gov.cz",
        "https://cedropendata.mfcr.cz",
        "https://opendata.praha.eu",
        "https://data.ctu.cz",
        "https://www.isvavai.cz",
    ]:
        if url.startswith(prefix):
            return True
    return False


def _dcat_extractor(
    graph: Optional[rdflib.Graph], log: logging.Logger, force: bool
) -> Optional[AsyncResult]:
    if graph is None:
        return None

    extractor = Extractor(graph)
    with TimedBlock("dcat_extractor"):
        extractor.extract()

    tasks = [
        process_priority.si(iri, force) for iri in extractor.priority_distributions
    ]
    tasks.extend(process.si(iri, force) for iri in extractor.distributions)
    monitor.log_tasks(len(tasks))
    return group(tasks).apply_async()


@celery.task(base=TrackableTask, bind=True, max_retries=5)
def inspect_graph(self, endpoint_iri, graph_iri, force):
    try:
        return do_inspect_graph(graph_iri, force, endpoint_iri)
    except RobotsRetry as exc:
        self.retry(timeout=exc.delay)


def do_inspect_graph(graph_iri, force, endpoint_iri):
    log = logging.getLogger(__name__)
    result = None
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        result = _dcat_extractor(inspector.process_graph(graph_iri), log, force)
    except (rdflib.query.ResultException, HTTPError):
        log.error(
            "Failed to inspect graph %s: ResultException or HTTP Error", graph_iri
        )
    monitor.log_inspected()
    return result


def multiply(item, times):
    for _ in range(times):
        yield item


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


@celery.task(base=TrackableTask)
def batch_inspect(endpoint_iri, graphs, chunks):
    items = len(graphs)
    monitor.log_graph_count(items)
    log = logging.getLogger(__name__)
    log.info("Batch of %s graphs in %s", str(items), endpoint_iri)
    return inspect_graph.chunks(
        zip(multiply(endpoint_iri, items), graphs, multiply(True, items)), chunks
    ).apply_async()
