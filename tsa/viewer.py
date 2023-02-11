import logging
import pycouchdb
import rdflib
from tsa.settings import Config
from tsa.monitor import TimedBlock
from tsa.robots import session


class ViewerProvider:
    def __init__(self) -> None:
        """
        Initialize the provider. Connect to CouchDB.
        """
        if Config.COUCHDB_URL is None:
            return
        server = pycouchdb.Server(Config.COUCHDB_URL, session=session)
        try:
            self.__datasets = server.database("datasets")
        except pycouchdb.exceptions.NotFound:
            try:
                self.__datasets = server.create("datasets")
            except pycouchdb.exceptions.Conflict:
                self.__datasets = server.database("datasets")
        try:
            self.__distributions = server.database("distributions")
        except pycouchdb.exceptions.NotFound:
            try:
                self.__distributions = server.create("distributions")
            except pycouchdb.exceptions.Conflict:
                self.__distributions = server.database("distributions")
        # try:
        #    self.__labels = server.database('labels')
        # except pycouchdb.exceptions.NotFound:
        #    self.__labels = server.create('labels')
        # try:
        #    self.__static = server.database('static')
        # except pycouchdb.exceptions.NotFound:
        #    self.__static = server.create('static')

    def __construct_dataset(self, graph: rdflib.Graph, iri: str) -> rdflib.Graph:
        """
        Create a new Graph with only the dataset.

        :param graph: the graph to construct the dataset from
        :param iri: the IRI of the dataset
        :return: the constructed dataset
        """
        return graph.query(
            "CONSTRUCT {?s ?p ?o} WHERE {?s a dcat:Dataset; ?p ?o}",
            initNs={"dcat": rdflib.Namespace("http://www.w3.org/ns/dcat#")},
            initBindings={"s": rdflib.URIRef(iri)},
        ).graph

    def __construct_distribution(
        self, graph: rdflib.Graph, iri: rdflib.URIRef
    ) -> rdflib.Graph:
        """
        Create a new Graph with only the distribution.

        :param graph: the graph to construct the distribution from
        :param iri: the IRI of the distribution
        :return: the constructed distribution
        """
        return graph.query(
            "CONSTRUCT {?s ?p ?o} WHERE {?s a dcat:Distribution; ?p ?o}",
            initNs={"dcat": rdflib.Namespace("http://www.w3.org/ns/dcat#")},
            initBindings={"s": iri},
        ).graph

    def serialize_to_couchdb(self, graph: rdflib.Graph, iri: str) -> None:
        """
        Serialize a DCAT Dataset to CouchDB.
        See https://github.com/linkedpipes/dcat-ap-viewer/wiki/Provider:-CouchDB for schema definition.
        Inserts dataset and all distributions into respective databases.

        :param graph: the graph to serialize
        :param iri: the IRI of the distribution
        """
        log = logging.getLogger(__name__)
        with TimedBlock("couchdb"):
            try:
                self.__datasets.save(
                    {
                        "_id": iri,
                        "jsonld": self.__construct_dataset(graph, iri).serialize(
                            format="json-ld"
                        ),
                    }
                )
            except pycouchdb.exceptions.Conflict:
                pass

            for distribution in graph.objects(
                rdflib.URIRef(iri),
                rdflib.URIRef("http://www.w3.org/ns/dcat#distribution"),
            ):
                if not isinstance(distribution, rdflib.URIRef):
                    continue
                try:
                    self.__distributions.save(
                        {
                            "_id": str(distribution),
                            "jsonld": self.__construct_distribution(
                                graph, distribution
                            ).serialize(format="json-ld"),
                        }
                    )
                except pycouchdb.exceptions.Conflict:
                    pass


viewer = ViewerProvider()
