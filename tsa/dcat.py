import itertools
import logging
from typing import Generator, List

from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import RDF
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.analyzer import GenericAnalyzer
from tsa.db import db_session
from tsa.model import DatasetDistribution, DatasetEndpoint
from tsa.tasks.process import filter_iri
from tsa.util import check_iri

# TODO: possibly scan for service description as well
# TODO: if no graph provided, use service as a whole instead of distributions


class Extractor:
    """Extracts DCAT-AP metadata from an RDF graph."""

    log = logging.getLogger(__name__)
    media_priority = frozenset(
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

    format_priority = frozenset(
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
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    dcterms = Namespace("http://purl.org/dc/terms/")
    nkod = Namespace("https://data.gov.cz/slovník/nkod/mediaTyp")

    def __init__(self, graph: Graph):
        self.__graph = graph
        self.__db_endpoints = []
        self.__db_distributions = []
        self.__task_count = 0

    @property
    def tasks(self) -> int:
        return self.__task_count

    def extract(self, priority, regular, force):
        """Extracts DCAT-AP metadata from an RDF graph."""
        self.__db_endpoints = []
        self.__db_distributions = []
        for dataset in self.__graph.subjects(RDF.type, Extractor.dcat.Dataset):
            for task in self.__process_dataset(str(dataset), priority, regular, force):
                self.__task_count += 1
                yield task
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
            Extractor.log.exception("Error while saving to database")
            db_session.rollback()

    def __process_dataset(self, dataset: str, priority, regular, force):
        """Extracts distributions from a dataset."""
        effective_dataset_iri = self.__get_effective_dataset_iri(dataset)
        if effective_dataset_iri is None:
            return
        for distribution in self.__graph.objects(
            URIRef(dataset), Extractor.dcat.distribution
        ):
            if isinstance(distribution, URIRef):
                for task in self.__process_distribution(
                    distribution,
                    URIRef(dataset),
                    effective_dataset_iri,
                    priority,
                    regular,
                    force,
                ):
                    yield task
            else:
                Extractor.log.warning("Invalid distribution: %s", distribution)

    def __process_distribution(
        self,
        distribution: URIRef,
        dataset_iri: URIRef,
        effective_dataset_iri: str,
        priority,
        regular,
        force,
    ):
        """
        Extract download URLs from a distribution,
        detect any services for the dataset.
        """
        task = self.__get_task(distribution, priority, regular)
        for download_url in self.__graph.objects(
            distribution, Extractor.dcat.downloadURL
        ):
            for task in self.__process_distribution_url(
                str(download_url), effective_dataset_iri, task, force, priority
            ):
                yield task
        for service in itertools.chain(
            self.__graph.objects(dataset_iri, Extractor.dcat.accessService),
            self.__graph.subjects(Extractor.dcat.servesDataset, dataset_iri),
        ):
            if isinstance(service, URIRef):
                self.__process_service(service, effective_dataset_iri)

    def __process_service(self, service: URIRef, effective_dataset_iri: str):
        """
        Extracts SPARQL endpoints IRIs from a service. Apply IRI filters anc checks.

        :param service: IRI of the service (DCAT:accessService)
        :param effective_dataset_iri: IRI of the dataset (or series)
        """
        Extractor.log.debug("Service: %s", str(service))
        for endpoint in self.__graph.objects(service, Extractor.dcat.endpointURL):
            url = str(endpoint)
            if not check_iri(url) or filter_iri(url):
                return
            self.__db_endpoints.append({"endpoint": url, "ds": effective_dataset_iri})

    def __process_distribution_url(
        self, url: str, effective_dataset_iri: str, task, force, priority
    ):
        """
        Extracts download URLs from a distribution. Apply IRI filters anc checks.
        If we sense a SPARQL endpoint, we add it to the list of endpoints.

        :param url: URL of the distribution
        :param effective_dataset_iri: IRI of the dataset (or series)
        :param queue: queue to which the URL is added (regular or priority)
        """
        if not check_iri(url) or filter_iri(url):
            return
        if url.endswith("/sparql"):
            Extractor.log.info(
                "Guessing %s is a SPARQL endpoint, will use for dereferences from effective DCAT dataset %s",
                url,
                effective_dataset_iri,
            )
            self.__db_endpoints.append({"endpoint": url, "ds": effective_dataset_iri})
            return
        if url.endswith("trig") or url.endswith("jsonld"):
            yield priority.si(url, force)
        else:
            yield task.si(url, force)

    def __get_task(self, distribution: URIRef, priority, regular):
        """
        Returns the queue to which the distribution should be added.
        If format or media suggest RDF data, the distribution is added to the priority queue.

        :param distribution: IRI of the distribution
        :return: queue to which the distribution should be added
        """
        for media_type in self.__graph.objects(distribution, Extractor.dcat.mediaType):
            if str(media_type) in Extractor.media_priority:
                return priority
        for format in self.__graph.objects(distribution, Extractor.dcterms["format"]):
            if str(format) in Extractor.format_priority:
                return priority
        for distribution_format in self.__graph.objects(
            distribution, Extractor.nkod.mediaType
        ):
            if "rdf" in str(distribution_format):
                return priority
        return regular

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
            Extractor.log.warning("Failed to query parent. Query was: %s", query)
