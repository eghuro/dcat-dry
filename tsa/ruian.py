import logging

from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from tsa.ddr import concept_index
from tsa.model import ddr_index
from tsa.robots import USER_AGENT
from tsa.util import check_iri


class RuianInspector:
    @staticmethod
    def process_references(iris):
        # query SPARQL endpoint at https://linked.cuzk.cz.opendata.cz/sparql
        log = logging.getLogger(__name__)
        processed = set()
        queue = list(iris)

        endpoint = "https://linked.cuzk.cz.opendata.cz/sparql"
        store = SPARQLStore(endpoint, headers={"User-Agent": USER_AGENT})
        ruian = Graph(store=store)
        ruian.open(endpoint)

        relationship_count = 0
        log.info("In queue initially: %s", len(queue))
        while len(queue) > 0:
            iri = queue.pop(0)
            if not check_iri(iri):
                continue
            if iri in processed:
                continue
            processed.add(iri)

            log.info("Processing %s. In queue remaining: %s", iri, len(queue))
            for token in [
                "ulice",
                "obec",
                "okres",
                "vusc",
                "regionSoudružnosti",
                "stát",
            ]:
                query = f"SELECT ?next WHERE {{ <{iri}> <https://linked.cuzk.cz/ontology/ruian/{token}> ?next }}"
                for row in ruian.query(query):
                    next_iri = row["next"]
                    queue.append(next_iri)

                    # report: (IRI, next_iri) - type: token
                    ddr_index.index(token, iri, next_iri)
                    concept_index.index(iri)
                    relationship_count = relationship_count + 1
        log.info(
            "Done proceessing RUIAN references. Processed %s, indexed %s relationships in RUIAN hierarchy.",
            len(processed),
            relationship_count,
        )
