try:
    import json
    import redis
    import logging
    import pycouchdb
    import rdflib
    from tsa.settings import Config
    from tsa.monitor import TimedBlock
    from tsa.robots import session
    from tsa.extensions import redis_pool
    from tsa.endpoint import SparqlEndpointAnalyzer

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

        def serialize_to_couchdb(self, endpoint) -> None:
            """
            Serialize DCAT Datasets to CouchDB.
            See https://github.com/linkedpipes/dcat-ap-viewer/wiki/Provider:-CouchDB for schema definition.
            Inserts dataset and all distributions into respective databases.
            """
            red = redis.StrictRedis(connection_pool=redis_pool, decode_responses=True)
            sparql = SparqlEndpointAnalyzer(endpoint)
            graph = rdflib.Graph(store=sparql.store)
            graph.open(endpoint)
            with TimedBlock("couchdb"):
                for dataset_iri in red.hkeys("dcat"):
                    try:
                        if dataset_iri not in self.__datasets:
                            self.__datasets.save(
                                {
                                    "_id": dataset_iri,
                                    "jsonld": self.__construct(
                                        graph, rdflib.URIRef(dataset_iri)
                                    ).serialize(format="json-ld"),
                                }
                            )
                            self.__datasets.commit()
                    except pycouchdb.exceptions.Conflict:
                        logging.getLogger(__name__).warning(
                            "Failed to save dataset: %s", dataset_iri
                        )

                    for distribution_iri in json.loads(red.hget("dcat", dataset_iri)):
                        if distribution_iri is None:
                            continue
                        try:
                            if distribution_iri not in self.__distributions:
                                self.__distributions.save(
                                    {
                                        "_id": distribution_iri,
                                        "jsonld": self.__construct(
                                            graph, rdflib.URIRef(distribution_iri)
                                        ).serialize(format="json-ld"),
                                    }
                                )
                                self.__distributions.commit()
                        except pycouchdb.exceptions.Conflict:
                            logging.getLogger(__name__).warning(
                                "Failed to save distribution: %s - dataset: %s",
                                distribution_iri,
                                dataset_iri,
                            )
                red.delete("dcat")

except ImportError:
    from tsa.settings import Config

    Config.COUCHDB_URL = None
