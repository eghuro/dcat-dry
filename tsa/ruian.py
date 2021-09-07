import logging

from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from tsa.extensions import conceptIndex, ddrIndex
from tsa.robots import user_agent
from tsa.util import test_iri


class RuianInspector(object):

    def process_references(self, iris):
        # query SPARQL endpoint at https://linked.cuzk.cz.opendata.cz/sparql
        log = logging.getLogger(__name__)
        processed = set()
        queue = list(iris)

        endpoint = 'https://linked.cuzk.cz.opendata.cz/sparql'
        store = SPARQLStore(endpoint, headers={'User-Agent': user_agent})
        ruian = Graph(store=store)
        ruian.open(endpoint)

        relationship_count = 0
        log.info(f'In queue initially: {len(queue)}')
        while len(queue) > 0:
            iri = queue.pop(0)
            if not test_iri(iri):
                continue
            if iri in processed:
                continue
            processed.add(iri)

            log.info(f'Processing {iri}. In queue remaining: {len(queue)}')
            for token in ['ulice', 'obec', 'okres', 'vusc', 'regionSoudružnosti', 'stát']:
                query = f'SELECT ?next WHERE {{ <{iri}> <https://linked.cuzk.cz/ontology/ruian/{token}> ?next }}'
                # log.info(query)
                for row in ruian.query(query):
                    next_iri = row['next']
                    queue.append(next_iri)

                    # report: (IRI, next_iri) - type: token
                    ddrIndex.index(token, iri, next_iri)
                    conceptIndex.index(iri)
                    relationship_count = relationship_count + 1
        log.info(f'Done proceessing RUIAN references. Processed {len(processed)}, indexed {relationship_count} relationships in RUIAN hierarchy.')
