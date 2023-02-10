import logging
import pycouchdb
import rdflib
from tsa.settings import Config
from tsa.monitor import TimedBlock


class ViewerProvider:
    def __init__(self) -> None:
        """
        Initialize the provider. Connect to CouchDB.
        """
        server = pycouchdb.Server(Config.COUCHDB_URL)
        try:
            self.__datasets = server.database("datasets")
        except pycouchdb.exceptions.NotFound:
            self.__datasets = server.create("datasets")
        try:
            self.__distributions = server.database("distributions")
        except pycouchdb.exceptions.NotFound:
            self.__distributions = server.create("distributions")
        # try:
        #    self.__labels = server.database('labels')
        # except pycouchdb.exceptions.NotFound:
        #    self.__labels = server.create('labels')
        # try:
        #    self.__static = server.database('static')
        # except pycouchdb.exceptions.NotFound:
        #    self.__static = server.create('static')

    def serialize_to_couchdb(self, dataset: rdflib.Graph, iri: str) -> None:
        """
        Serialize a DCAT Dataset to CouchDB.
        See https://github.com/linkedpipes/dcat-ap-viewer/wiki/Provider:-CouchDB for schema definition.
        Inserts dataset and all distributions into respective databases.

        :param graph: the graph to serialize
        :param iri: the IRI of the distribution
        """
        log = logging.getLogger(__name__)
        log.debug("Serialize to CouchDB - execution: %s", iri)
        with TimedBlock("couchdb"):
            try:
                self.__datasets.save(
                    {"_id": iri, "jsonld": graph.serialize(format="json-ld")}
                )
            except pycouchdb.exceptions.Conflict:
                log.debug("Document already exists: %s", iri)
            for distribution in graph.objects(
                rdflib.URIRef(iri),
                rdflib.URIRef("http://www.w3.org/ns/dcat#distribution"),
            ):
                try:
                    self.__distributions.save(
                        {
                            "_id": str(distribution),
                            "jsonld": graph.serialize(format="json-ld"),
                        }
                    )
                except pycouchdb.exceptions.Conflict:
                    log.debug("Document already exists: %s", str(distribution))
