"""SPARQL endpoint utilities."""
import logging

from rdflib import Graph
from rdflib.parser import Parser
from rdflib.plugin import register as register_plugin
from rdflib.plugins.stores.sparqlstore import SPARQLStore, _node_to_sparql
from rdflib.query import ResultException

from tsa.monitor import TimedBlock
from tsa.robots import user_agent
from tsa.util import test_iri


class SparqlEndpointAnalyzer(object):
    """Extract DCAT datasets from a SPARQL endpoint."""

    def __query(self, named=None):
        str1 = """
        construct {
          ?ds a <http://www.w3.org/ns/dcat#Dataset>;
          <http://purl.org/dc/terms/title> ?title;
          <http://www.w3.org/ns/dcat#keyword> ?keyword;
          <http://www.w3.org/ns/dcat#distribution> ?d.

          ?d a <http://www.w3.org/ns/dcat#Distribution>;
          <http://www.w3.org/ns/dcat#downloadURL> ?downloadURL;
          <http://purl.org/dc/terms/format> ?format;
          <http://www.w3.org/ns/dcat#mediaType> ?media;
          <https://data.gov.cz/slovník/nkod/mediaType> ?mediaNkod.

          ?d <http://www.w3.org/ns/dcat#accessURL> ?accessPoint.
          ?accessPoint <http://www.w3.org/ns/dcat#endpointURL> ?endpointUrl;
          <http://www.w3.org/ns/dcat#endpointDescription> ?sd.
       }
       """

        str3 = """
       where {
         ?ds a <http://www.w3.org/ns/dcat#Dataset>.
         ?ds <http://purl.org/dc/terms/title> ?title.
         OPTIONAL {?ds <http://www.w3.org/ns/dcat#keyword> ?keyword. }
         ?ds <http://www.w3.org/ns/dcat#distribution> ?d.
         OPTIONAL { ?d <http://www.w3.org/ns/dcat#downloadURL> ?downloadURL. }
         OPTIONAL { ?d <http://purl.org/dc/terms/format> ?format. }
         OPTIONAL { ?d <http://www.w3.org/ns/dcat#mediaType> ?media. }
         OPTIONAL { ?d <http://www.w3.org/ns/dcat#accessURL> ?accessPoint.
             OPTIONAL { ?d  <http://www.w3.org/ns/dcat#accessService> ?accessService.
                ?accessService <http://www.w3.org/ns/dcat#endpointURL> ?endpointUrl.
                OPTIONAL { ?accessService <http://www.w3.org/ns/dcat#endpointDescription> ?sd. }
             }
         }
         OPTIONAL { ?d <https://data.gov.cz/slovník/nkod/mediaType> ?mediaNkod. }
       }
       """

        if named is not None:
            return f'{str1} from <{named}> {str3}'
        else:
            logging.getLogger(__name__).warn('No named graph when constructing catalog from {self.__endpoint!s}')
            return f'{str1} {str3}'

    def __init__(self, endpoint):
        if not test_iri(endpoint):
            logging.getLogger(__name__).warn(f'{endpoint!s} is not a valid endpoint URL')
            raise ValueError(endpoint)
        self.__endpoint = endpoint
        # workaround for https://github.com/RDFLib/rdflib/issues/1195
        register_plugin('application/rdf+xml; charset=UTF-8', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')

        self.store = SPARQLStore(endpoint, True, True, _node_to_sparql,
                                 'application/rdf+xml',
                                 headers={'User-Agent': user_agent})

    def process_graph(self, graph_iri):
        """Extract DCAT datasets from the given named graph of an endpoint."""
        if not test_iri(graph_iri):
            logging.getLogger(__name__).warn(f'{graph_iri!s} is not a valid graph URL')
            return None

        g = Graph(store=self.store, identifier=graph_iri)
        g.open(self.__endpoint)

        query = self.__query(graph_iri)
        # log.debug(query)

        try:
            with TimedBlock('process_graph'):
                return g.query(query).graph  # implementation detail for CONSTRUCT!
        except ResultException as e:
            logging.getLogger(__name__).error(f'Failed to process {graph_iri} in {self.__endpoint}: {str(e)}')

        return None

    def get_graphs_from_endpoint(self, endpoint):
        """Extract named graphs from the given endpoint."""
        log = logging.getLogger(__name__)
        g = Graph(store='SPARQLStore')
        g.open(endpoint)
        cnt = 0
        offset = 0
        while True:
            local_cnt = 0
            log.debug(f'Peek graphs in {endpoint}, offset: {offset}')
            for row in g.query('select ?g where { GRAPH ?g {} } LIMIT 100 OFFSET ' + str(offset)):
                cnt = cnt + 1
                local_cnt = local_cnt + 1
                yield row['g']
            if local_cnt > 0:
                offset = offset + 100
            else:
                break
        if cnt == 0:
            # certain SPARQL endpoints (aka Virtuoso) do not support queries above, so we have to use the one below
            # however, it's very inefficient and will likely timeout
            log = logging.getLogger(__name__)
            # log.warn(f'Endpoint {endpoint} does not support the preferred SPARQL query, falling back, this will likely timeout though')
            while True:
                local_cnt = 0
                log.debug(f'Peek graphs in {endpoint}, offset: {offset}')
                for row in g.query('select distinct ?g where { GRAPH ?g {?s ?p ?o} } LIMIT 100 OFFSET ' + str(offset)):
                    local_cnt = local_cnt + 1
                    yield row['g']
                if local_cnt > 0:
                    offset = offset + 100
                else:
                    break

    # TODO
    # all above is extracting DCAT for use in batch
    # however we might have some real datasets there
    # -> service description
    # and VOID

    # if we have SD of ?endpoint then use query '?x sd:endpoint ?endpoint; sd:namedGraph/sd:name ?name.' on SD to
    # get named graphs (taken from LPA)
