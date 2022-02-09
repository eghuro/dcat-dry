"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging

import rdflib
import redis
from celery import group
from rdflib import Namespace
from rdflib.namespace import RDF
from requests.exceptions import HTTPError

from tsa.analyzer import GenericAnalyzer
from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.monitor import TimedBlock, monitor
from tsa.redis import dataset_endpoint, ds_distr
from tsa.settings import Config
from tsa.tasks.common import TrackableTask
from tsa.tasks.process import filter_iri, process, process_priority
from tsa.util import check_iri


def query_parent(ds, g, log):
    query = f"SELECT ?parent WHERE {{ <{ds!s}> <http://purl.org/dc/terms/isPartOf> ?parent }}"
    try:
        for parent in g.query(query):
            parent_iri = str(parent["parent"])
            yield str(parent_iri)
    except ValueError:
        log.warning(f"Failed to query parent. Query was: {query}")


def test_allowed(url: str) -> bool:
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


def _dcat_extractor(g, red, log, force, graph_iri):
    if g is None:
        return
    distributions, distributions_priority = [], []
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    dcterms = Namespace("http://purl.org/dc/terms/")
    nkod = Namespace("https://data.gov.cz/slovník/nkod/mediaTyp")
    media_priority = set(
        [
            "https://www.iana.org/assignments/media-types/application/rdf+xml",
            "https://www.iana.org/assignments/media-types/application/trig",
            "https://www.iana.org/assignments/media-types/text/n3",
            "https://www.iana.org/assignments/media-types/application/ld+json",
            "https://www.iana.org/assignments/media-types/application/n-triples",
            "https://www.iana.org/assignments/media-types/application/n-quads",
            "https://www.iana.org/assignments/media-types/text/turtle",
            "http://www.iana.org/assignments/media-types/application/rdf+xml",
            "http://www.iana.org/assignments/media-types/application/trig",
            "http://www.iana.org/assignments/media-types/text/n3",
            "http://www.iana.org/assignments/media-types/application/ld+json",
            "http://www.iana.org/assignments/media-types/application/n-triples",
            "http://www.iana.org/assignments/media-types/application/n-quads",
            "http://www.iana.org/assignments/media-types/text/turtle",
        ]
    )  # IANA
    format_priority = set(
        [
            "http://publications.europa.eu/resource/authority/file-type/RDF",
            "http://publications.europa.eu/resource/authority/file-type/RDFA",
            "http://publications.europa.eu/resource/authority/file-type/RDF_N_QUADS",
            "http://publications.europa.eu/resource/authority/file-type/RDF_N_TRIPLES",
            "http://publications.europa.eu/resource/authority/file-type/RDF_TRIG",
            "http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE",
            "http://publications.europa.eu/resource/authority/file-type/RDF_XML",
            "http://publications.europa.eu/resource/authority/file-type/JSON_LD",
            "http://publications.europa.eu/resource/authority/file-type/N3",
            "https://publications.europa.eu/resource/authority/file-type/RDF",
            "https://publications.europa.eu/resource/authority/file-type/RDFA",
            "https://publications.europa.eu/resource/authority/file-type/RDF_N_QUADS",
            "https://publications.europa.eu/resource/authority/file-type/RDF_N_TRIPLES",
            "https://publications.europa.eu/resource/authority/file-type/RDF_TRIG",
            "https://publications.europa.eu/resource/authority/file-type/RDF_TURTLE",
            "https://publications.europa.eu/resource/authority/file-type/RDF_XML",
            "https://publications.europa.eu/resource/authority/file-type/JSON_LD",
            "https://publications.europa.eu/resource/authority/file-type/N3",
        ]
    )  # EU
    queue = distributions

    log.debug(f"Extracting distributions from {graph_iri}")
    # DCAT dataset
    with TimedBlock("dcat_extractor"):
        dsdistr, distrds = ds_distr()
        distribution = False
        with red.pipeline() as pipe:
            for ds in g.subjects(RDF.type, dcat.Dataset):
                log.debug(f"DS: {ds!s}")
                effective_ds = ds

                for parent in query_parent(ds, g, log):
                    log.debug(f"{parent!s} is a series containing {ds!s}")
                    effective_ds = parent

                # DCAT Distribution
                for d in g.objects(ds, dcat.distribution):
                    log.debug(f"Distr: {d!s}")
                    # put RDF distributions into a priority queue
                    for media in g.objects(d, dcat.mediaType):
                        if str(media) in media_priority:
                            queue = distributions_priority

                    for distribution_format in g.objects(d, dcterms.format):
                        if str(distribution_format) in format_priority:
                            queue = distributions_priority

                    # data.gov.cz specific
                    for distribution_format in g.objects(d, nkod.mediaType):
                        if "rdf" in str(distribution_format):
                            queue = distributions_priority

                    # download URL to files
                    downloads = []
                    endpoints = set()
                    for download_url in g.objects(d, dcat.downloadURL):
                        # log.debug(f'Down: {download_url!s}')
                        url = str(download_url)
                        if check_iri(url) and not filter_iri(url):
                            if url.endswith("/sparql"):
                                log.info(
                                    f"Guessing {url} is a SPARQL endpoint, will use for dereferences from DCAT dataset {ds!s} (effective: {effective_ds!s})"
                                )
                                endpoints.add(url)
                            elif test_allowed(url) or not Config.LIMITED:
                                downloads.append(url)
                                distribution = True
                                log.debug(
                                    f"Distribution {url!s} from DCAT dataset {ds!s} (effective: {effective_ds!s})"
                                )
                                if url.endswith("trig"):
                                    distributions_priority.append(download_url)
                                else:
                                    queue.append(download_url)
                                pipe.sadd(f"{dsdistr}:{str(effective_ds)}", url)
                                pipe.sadd(f"{distrds}:{url}", str(effective_ds))
                            else:
                                log.debug(f"Skipping {url} due to filter")
                        else:
                            log.debug(f"{url} is not a valid download URL")

                    # scan for DCAT2 data services here as well
                    for access in g.objects(d, dcat.accessService):
                        log.debug(f"Service: {access!s}")
                        for endpoint in g.objects(access, dcat.endpointURL):
                            if check_iri(str(endpoint)):
                                log.debug(
                                    f"Endpoint {endpoint!s} from DCAT dataset {ds!s}"
                                )
                                endpoints.add(endpoint)

                    for endpoint in endpoints:
                        pipe.sadd(dataset_endpoint(str(effective_ds)), endpoint)

                    if not downloads and endpoints:
                        log.warning(f"Only endpoint without distribution for {ds!s}")

            pipe.execute()
        # TODO: possibly scan for service description as well
        if distribution:
            GenericAnalyzer().get_details(g)  # extrakce labelu - heavy!
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
#        g = rdflib.ConjunctiveGraph()
#       g.parse(data=red.get(key), format='n3')
#        red.delete(key)
#    except rdflib.plugin.PluginException:
#        log.debug('Failed to parse graph')
#        return None
#
#    return _dcat_extractor(g, red, log, False, key, None)


@celery.task(base=TrackableTask)
def inspect_graph(endpoint_iri, graph_iri, force):
    red = inspect_graph.redis
    return do_inspect_graph(graph_iri, force, red, endpoint_iri)


def do_inspect_graph(graph_iri, force, red, endpoint_iri):
    log = logging.getLogger(__name__)
    result = None
    try:
        inspector = SparqlEndpointAnalyzer(endpoint_iri)
        result = _dcat_extractor(
            inspector.process_graph(graph_iri), red, log, force, graph_iri
        )
    except (rdflib.query.ResultException, HTTPError):
        log.error(f"Failed to inspect graph {graph_iri}: ResultException or HTTP Error")
    monitor.log_inspected()
    return result


@celery.task(base=TrackableTask)
def inspect_graphs(graphs, endpoint_iri, force):
    red = inspect_graphs.redis
    do_inspect_graphs(graphs, endpoint_iri, force, red)


def do_inspect_graphs(graphs, endpoint_iri, force, red):
    return [do_inspect_graph(g, force, red, endpoint_iri) for g in graphs]


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
    log.info(f"Batch of {items} graphs in {endpoint_iri}")
    red = batch_inspect.redis
    try:
        with red.lock("notifiedFirstProcessLock", blocking_timeout=5):
            red.set("notifiedFirstProcess", "0")
    except redis.LockError:
        log.error("Failed to lock notification block in do_process")
    return inspect_graph.chunks(
        zip(multiply(endpoint_iri, items), graphs, multiply(True, items)), chunks
    ).apply_async()
