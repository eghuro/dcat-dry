"""SPARQL endpoint utilities."""
import logging

from rdflib import Graph
from rdflib.parser import Parser
from rdflib.plugin import register as register_plugin
from rdflib.plugins.stores.sparqlstore import SPARQLStore, _node_to_sparql
from rdflib.query import ResultException

from tsa.monitor import TimedBlock
from tsa.robots import USER_AGENT, session
from tsa.util import test_iri


class SparqlEndpointAnalyzer:

    """Extract DCAT datasets from a SPARQL endpoint."""

    @staticmethod
    def __query(named=None):
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
        logging.getLogger(__name__).warning('No named graph when constructing catalog from {self.__endpoint!s}')
        return f'{str1} {str3}'

    def __init__(self, endpoint):
        if not test_iri(endpoint):
            logging.getLogger(__name__).warning(f'{endpoint!s} is not a valid endpoint URL')
            raise ValueError(endpoint)
        self.__endpoint = endpoint
        # workaround for https://github.com/RDFLib/rdflib/issues/1195
        register_plugin('application/rdf+xml; charset=UTF-8', Parser, 'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')

        self.store = SPARQLStore(endpoint, True, True, _node_to_sparql,
                                 'application/rdf+xml',
                                 session=session,
                                 headers={'User-Agent': USER_AGENT})

    def process_graph(self, graph_iri):
        """Extract DCAT datasets from the given named graph of an endpoint."""
        if not test_iri(graph_iri):
            logging.getLogger(__name__).warning(f'{graph_iri!s} is not a valid graph URL')
            return None

        graph = Graph(store=self.store, identifier=graph_iri)
        graph.open(self.__endpoint)

        query = self.__query(graph_iri)

        try:
            with TimedBlock('process_graph'):
                return graph.query(query).graph  # implementation detail for CONSTRUCT!
        except ResultException as exc:
            logging.getLogger(__name__).error(f'Failed to process {graph_iri} in {self.__endpoint}: {str(exc)}')

        return None

    # TODO
    # all above is extracting DCAT for use in batch
    # however we might have some real datasets there
    # -> service description
    # and VOID

    # if we have SD of ?endpoint then use query '?x sd:endpoint ?endpoint; sd:namedGraph/sd:name ?name.' on SD to
    # get named graphs (taken from LPA)
