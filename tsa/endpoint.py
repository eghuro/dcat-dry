"""SPARQL endpoint utilities."""
import logging

import redis
import rfc3987
from rdflib import Graph

from tsa.extensions import redis_pool
from tsa.redis import data as data_key, expiration, KeyRoot


class SparqlEndpointAnalyzer(object):
    """Extract DCAT datasets from a SPARQL endpoint."""

    def __query(self, endpoint, named=None):
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
            logging.getLogger(__name__).warn('No named graph when constructing catalog from {endpoint!s}')
            return f'{str1} {str3}'

    def process_graph(self, endpoint, graph_iri):
        """Extract DCAT datasets from the given named graph of an endpoint."""
        log = logging.getLogger(__name__)
        if not rfc3987.match(endpoint):
            log.warn(f'{endpoint!s} is not a valid endpoint URL')
            return None
        if not rfc3987.match(graph_iri):
            log.warn(f'{graph_iri!s} is not a valid graph URL')
            return None

        g = Graph(store='SPARQLStore', identifier=graph_iri)
        g.open(endpoint)

        result = Graph()
        for s, p, o in g.query(self.__query(endpoint, graph_iri)):
            result.add( (s, p, o) )

        return result

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
            #log.warn(f'Endpoint {endpoint} does not support the preferred SPARQL query, falling back, this will likely timeout though')
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


    #if we have SD of ?endpoint then use query '?x sd:endpoint ?endpoint; sd:namedGraph/sd:name ?name.' on SD to
    #get named graphs (taken from LPA)