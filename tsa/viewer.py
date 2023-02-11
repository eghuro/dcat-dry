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

    def __construct(self, graph: rdflib.Graph, iri: rdflib.URIRef) -> rdflib.Graph:
        """
        Create a new Graph with only the required iri as a subject.

        :param graph: the graph to construct from
        :param iri: the IRI of the resource we are interested in
        :return: the constructed subgraph
        """
        return graph.query(
            "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o}",
            initBindings={"s": iri},
        ).graph

    def serialize_to_couchdb(self, graph: rdflib.Graph, iri: str) -> None:
        """
        Serialize a DCAT Dataset to CouchDB.
        See https://github.com/linkedpipes/dcat-ap-viewer/wiki/Provider:-CouchDB for schema definition.
        Inserts dataset and all distributions into respective databases.

        :param graph: the graph to serialize
        :param iri: the IRI of the dataset
        """
        with TimedBlock("couchdb"):
            try:
                if iri not in self.__datasets:
                    self.__datasets.save(
                        {
                            "_id": iri,
                            "jsonld": self.__construct(
                                graph, rdflib.URIRef(iri)
                            ).serialize(format="json-ld"),
                        }
                    )
                    self.__datasets.commit()
            except pycouchdb.exceptions.Conflict:
                logging.getLogger(__name__).warning("Failed to save dataset: %s", iri)

            for distribution in graph.objects(
                rdflib.URIRef(iri),
                rdflib.URIRef("http://www.w3.org/ns/dcat#distribution"),
            ):
                if not isinstance(distribution, rdflib.URIRef):
                    continue
                try:
                    iri = str(distribution)
                    if iri not in self.__distributions:
                        self.__distributions.save(
                            {
                                "_id": iri,
                                "jsonld": self.__construct(
                                    graph, distribution
                                ).serialize(format="json-ld"),
                            }
                        )
                        self.__distributions.commit()
                except pycouchdb.exceptions.Conflict:
                    logging.getLogger(__name__).warning(
                        "Failed to save distribution: %s - dataset: %s",
                        str(distribution),
                        iri,
                    )


viewer = ViewerProvider()
