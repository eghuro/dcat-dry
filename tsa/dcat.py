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
    """ Extracts DCAT-AP metadata from an RDF graph. """

    log = logging.getLogger(__name__)
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
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    dcterms = Namespace("http://purl.org/dc/terms/")
    nkod = Namespace("https://data.gov.cz/slovník/nkod/mediaTyp")

    def __init__(self, graph: Graph):
        self.__graph = graph
        self.__distributions = []
        self.__distributions_priority = []
        self.__db_endpoints = []
        self.__db_distributions = []

    @property
    def priority_distributions(self) -> List[str]:
        return self.__distributions_priority

    @property
    def regular_distributions(self) -> List[str]:
        return self.__distributions

    def extract(self) -> None:
        """ Extracts DCAT-AP metadata from an RDF graph. """
        self.__db_endpoints = []
        self.__db_distributions = []
        for dataset in self.__graph.subjects(RDF.type, Extractor.dcat.Dataset):
            self.__process_dataset(str(dataset))
        self.__store_to_db()

    def __store_to_db(self) -> None:
        """ Stores the extracted metadata to the database. """
        try:
            if len(self.__db_endpoints) > 0:
                db_session.execute(insert(DatasetEndpoint).values(self.__db_endpoints))
            if len(self.__db_distributions) > 0:
                db_session.execute(
                    insert(DatasetDistribution).values(self.__db_distributions)
                )
            if len(self.__db_endpoints) > 0 or len(self.__db_distributions) > 0:
                db_session.commit()
                GenericAnalyzer().get_details(self.__graph)
        except SQLAlchemyError:
            Extractor.log.exception("Error while saving to database")
            db_session.rollback()

    def __process_dataset(self, dataset: str) -> None:
        effective_dataset_iri = self.__get_effective_dataset_iri(dataset)
        if effective_dataset_iri is None:
            return
        for distribution in self.__graph.objects(
            URIRef(dataset), Extractor.dcat.distribution
        ):
            if isinstance(distribution, URIRef):
                self.__process_distribution(distribution, effective_dataset_iri)
            else:
                Extractor.log.warning("Invalid distribution: %s", distribution)

    def __process_distribution(
        self, distribution: URIRef, effective_dataset_iri: str
    ) -> None:
        queue = self.__get_queue(distribution)
        for download_url in self.__graph.objects(
            distribution, Extractor.dcat.downloadURL
        ):
            self.__process_distribution_url(
                str(download_url), effective_dataset_iri, queue
            )
        for service in itertools.chain(
            self.__graph.objects(distribution, Extractor.dcat.accessService),
            self.__graph.subjects(Extractor.dcat.servesDataset, distribution),
        ):
            if isinstance(service, URIRef):
                self.__process_service(service, effective_dataset_iri)

    def __process_service(self, service: URIRef, effective_dataset_iri: str):
        Extractor.log.debug("Service: %s", str(service))
        for endpoint in self.__graph.objects(service, Extractor.dcat.endpointURL):
            url = str(endpoint)
            if not check_iri(url) or filter_iri(url):
                return
            self.__db_endpoints.append({"endpoint": url, "ds": effective_dataset_iri})

    def __process_distribution_url(
        self, url: str, effective_dataset_iri: str, queue: list
    ) -> None:
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
            self.__distributions_priority.append(url)
        queue.append({"ds": effective_dataset_iri, "distr": url})

    def __get_queue(self, distribution: URIRef) -> list:
        for media_type in self.__graph.objects(distribution, Extractor.dcat.mediaType):
            if str(media_type) in Extractor.media_priority:
                return self.__distributions_priority
        for format in self.__graph.objects(distribution, Extractor.dcterms["format"]):
            if str(format) in Extractor.format_priority:
                return self.__distributions_priority
        for distribution_format in self.__graph.objects(
            distribution, Extractor.nkod.mediaType
        ):
            if "rdf" in str(distribution_format):
                return self.__distributions_priority
        return self.__distributions

    def __get_effective_dataset_iri(self, dataset: str) -> str:
        effective_dataset_iri = dataset
        for parent in self.__query_parent(dataset):
            if parent is not None:
                effective_dataset_iri = parent
        return effective_dataset_iri

    def __query_parent(self, dataset_iri: str) -> Generator[str, None, None]:
        query = f"SELECT ?parent WHERE {{ <{dataset_iri!s}> <http://purl.org/dc/terms/isPartOf> ?parent }}"
        try:
            for parent in self.__graph.query(query):
                yield str(parent["parent"])
        except ValueError:
            Extractor.log.warning("Failed to query parent. Query was: %s", query)
