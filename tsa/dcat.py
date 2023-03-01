import itertools
import logging
from typing import Generator, Optional

import json
import redis
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.analyzer import GenericAnalyzer
from tsa.db import db_session
from tsa.model import DatasetDistribution, DatasetEndpoint, ProcessingStatus
from tsa.tasks.process import filter_iri
from tsa.util import check_iri
from tsa.extensions import redis_pool
from tsa.settings import Config

# TODO: possibly scan for service description as well
# TODO: if no graph provided, use service as a whole instead of distributions


class Extractor:
    """Extracts DCAT-AP metadata from an RDF graph."""

    _log = logging.getLogger(__name__)
    _media_priority = frozenset(
        (
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
        )
    )  # IANA

    _format_priority = frozenset(
        (
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
        )
    )  # EU
    _dcat = Namespace("http://www.w3.org/ns/dcat#")
    _dcterms = Namespace("http://purl.org/dc/terms/")
    _nkod = Namespace("https://data.gov.cz/slovník/nkod/mediaTyp")

    def __init__(self, graph: Graph):
        self.__graph = graph
        self.__db_endpoints = []
        self.__db_distributions = []
        self.__task_count = 0

    @property
    def tasks(self) -> int:
        """Number of tasks created by the extractor."""
        return self.__task_count

    def extract(self, priority, regular, force: bool):
        """
        Extracts DCAT-AP metadata from an RDF graph.

        :param priority: Celery task signature for priority tasks - known RDF formats
        :param regular: Celery task signature for regular tasks - likely not RDF format
        :param force: Force processing of all distributions
        :return: Generator of Celery tasks
        """
        self.__db_endpoints = []
        self.__db_distributions = []
        red = redis.StrictRedis(connection_pool=redis_pool)
        with red.pipeline() as pipe:
            for dataset in self.__graph.subjects(RDF.type, Extractor._dcat.Dataset):
                for task in self.__process_dataset(
                    str(dataset), priority, regular, force, pipe
                ):
                    self.__task_count += 1
                    yield task
            pipe.execute()
        self.__store_to_db()

    def __store_to_db(self) -> None:
        """Stores the extracted metadata to the database."""
        try:
            if len(self.__db_endpoints) > 0:
                db_session.execute(insert(DatasetEndpoint), self.__db_endpoints)
            if len(self.__db_distributions) > 0:
                db_session.execute(insert(DatasetDistribution), self.__db_distributions)
            if len(self.__db_endpoints) > 0 or len(self.__db_distributions) > 0:
                db_session.commit()
                GenericAnalyzer().get_details(self.__graph)
        except SQLAlchemyError:
            Extractor._log.exception("Error while saving to database")
            db_session.rollback()

    def __process_dataset(self, dataset: str, priority, regular, force: bool, pipe):
        """
        Extracts distributions from a dataset.

        :param dataset: IRI of the dataset
        :param priority: Celery task signature for priority tasks - known RDF formats
        :param regular: Celery task signature for regular tasks - likely not RDF format
        :param force: Force processing of all distributions
        :return: Generator of Celery tasks
        """
        effective_dataset_iri = self.__get_effective_dataset_iri(dataset)
        if effective_dataset_iri is None:
            return
        ofn = self.__graph.value(URIRef(dataset), Extractor._dcterms.conformsTo)
        if ofn is not None:
            ofn = str(ofn)
        distribution_iris = set()
        for distribution in self.__graph.objects(
            URIRef(dataset), Extractor._dcat.distribution
        ):
            if isinstance(distribution, URIRef):
                for task in self.__process_distribution(
                    distribution,
                    URIRef(dataset),
                    effective_dataset_iri,
                    priority,
                    regular,
                    force,
                    ofn,
                ):
                    yield task
                distribution_iris.add(str(distribution))
            else:
                Extractor._log.warning("Invalid distribution: %s", distribution)
        pipe.hsetnx("dcat", dataset, json.dumps(list(distribution_iris)))

    def __process_distribution(
        self,
        distribution: URIRef,
        dataset_iri: URIRef,
        effective_dataset_iri: str,
        priority,
        regular,
        force: bool,
        ofn: Optional[str],
    ):
        """
        Extract download URLs from a distribution,
        detect any services for the dataset.

        :param distribution: IRI of the distribution (DCAT:distribution)
        :param dataset_iri: IRI of the dataset (DCAT:Dataset)
        :param effective_dataset_iri: IRI of the dataset (DCAT:Dataset) or series it belongs to
        :param priority: Celery task signature for priority tasks - known RDF formats
        :param regular: Celery task signature for regular tasks - likely not RDF format
        :param force: Force processing of all distributions
        :param ofn: IRI of the standard the dataset conforms to
        :return: Generator of Celery tasks
        """
        task, is_priority = self.__get_task(distribution, priority, regular)
        for download_url in self.__graph.objects(
            distribution, Extractor._dcat.downloadURL
        ):
            if download_url.startswith("https://data.gov.cz/soubor/nkod"):
                # NKOD catalog - roughly GBs, it's what we're processing at the moment anyways
                # usually causes OOM crashes
                # can have various extensions
                continue
            for task, change in self.__process_distribution_url(
                str(download_url), effective_dataset_iri, task, force, priority, ofn
            ):
                # task can now become a priority task
                is_priority_after = is_priority or change
                if is_priority_after or not Config.ONLY_ONE_PRIORITY_DISTRIBUTION:
                    yield task
                    if is_priority_after and Config.ONLY_ONE_PRIORITY_DISTRIBUTION:
                        break
        for service in itertools.chain(
            self.__graph.objects(dataset_iri, Extractor._dcat.accessService),
            self.__graph.subjects(Extractor._dcat.servesDataset, dataset_iri),
        ):
            if isinstance(service, URIRef):
                self.__process_service(service, effective_dataset_iri)

    def __process_service(self, service: URIRef, effective_dataset_iri: str):
        """
        Extracts SPARQL endpoints IRIs from a service. Apply IRI filters anc checks.
        Store in the list of endpoints for database storage.

        :param service: IRI of the service (DCAT:accessService)
        :param effective_dataset_iri: IRI of the dataset (or series)
        """
        Extractor._log.debug("Service: %s", str(service))
        for endpoint in self.__graph.objects(service, Extractor._dcat.endpointURL):
            url = str(endpoint)
            if not check_iri(url) or filter_iri(url):
                continue
            self.__db_endpoints.append({"endpoint": url, "ds": effective_dataset_iri})

    def __process_distribution_url(
        self,
        url: str,
        effective_dataset_iri: str,
        standard_task,
        force: bool,
        priority_task,
        ofn: Optional[str],
    ):
        """
        Extracts download URLs from a distribution. Apply IRI filters anc checks.
        If we sense a SPARQL endpoint, we add it to the list of endpoints.

        :param url: URL of the distribution
        :param effective_dataset_iri: IRI of the dataset (or series)
        :param task: Celery task to execute
        :param force: Force processing of all distributions
        :param priority: Celery task signature for priority tasks - in case we are sure the URL is RDF to override the default task
        :param ofn: IRI of the standard the dataset conforms to
        :return: Generator of Celery task signatures with proper URL and force flag
        """
        if not check_iri(url) or filter_iri(url):
            return
        if url.endswith("/sparql"):
            Extractor._log.info(
                "Guessing %s is a SPARQL endpoint, will use for dereferences from effective DCAT dataset %s",
                url,
                effective_dataset_iri,
            )
            self.__db_endpoints.append({"endpoint": url, "ds": effective_dataset_iri})
            return
        self.__db_distributions.append(
            {
                "distr": url,
                "ds": effective_dataset_iri,
                "processed": ProcessingStatus.not_processed,
            }
        )
        if ofn is not None:
            self.__db_distributions[-1]["ofn"] = ofn
        if url.endswith("trig") or url.endswith("jsonld"):
            yield priority_task.si(url, force), True
        else:
            yield standard_task.si(url, force), False

    def __get_task(self, distribution: URIRef, priority, regular):
        """
        Returns the queue to which the distribution should be added.
        If format or media suggest RDF data, the distribution is added to the priority queue.

        :param distribution: IRI of the distribution
        :param priority: Celery task signature for priority tasks - known RDF formats
        :param regular: Celery task signature for regular tasks - likely not RDF format
        :return: tuple: task to execute (priority or regular) and priority flag
        """
        for media_type in self.__graph.objects(distribution, Extractor._dcat.mediaType):
            if str(media_type) in Extractor._media_priority:
                return priority, True
        for format in self.__graph.objects(distribution, Extractor._dcterms["format"]):
            if str(format) in Extractor._format_priority:
                return priority, True
        for distribution_format in self.__graph.objects(
            distribution, Extractor._nkod.mediaType
        ):
            if "rdf" in str(distribution_format):
                return priority, True
        return regular, False

    def __get_effective_dataset_iri(self, dataset: str) -> str:
        """
        Returns the IRI of the dataset (or series) that is the parent of the given dataset.

        :param dataset: IRI of the dataset
        :return: IRI of the parent dataset (or series) which could also be the original dataset if there is no parent
        """
        effective_dataset_iri = dataset
        try:
            for parent in self.__query_parent(dataset):
                if parent is not None:
                    effective_dataset_iri = parent
        except ValueError:
            pass
        return effective_dataset_iri

    def __query_parent(self, dataset_iri: str) -> Generator[str, None, None]:
        """
        Queries the parent of the given dataset.
        Parent is defined as the dataset that this one is isPartOf which.

        :param dataset_iri: IRI of the dataset
        :return: IRI of the parent dataset (or series)
        """
        query = f"SELECT ?parent WHERE {{ <{dataset_iri!s}> <http://purl.org/dc/terms/isPartOf> ?parent }}"
        try:
            for parent in self.__graph.query(query):
                yield str(parent["parent"])
        except ValueError:
            Extractor._log.warning("Failed to query parent. Query was: %s", query)
