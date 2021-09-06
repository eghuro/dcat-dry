from urllib.parse import urljoin, urlparse
from uuid import uuid4

from rdflib import BNode, Graph, Namespace, URIRef
from rdflib.namespace import RDF, VOID

from tsa.settings import Config


def create_sd_iri(query_string):
    return urljoin(Config.SD_BASE_IRI, f'sd?{query_string}')


def generate_service_description(sd_iri, endpoint_iri, graph_iri):
    SD = Namespace('http://www.w3.org/ns/sparql-service-description#')

    o = urlparse(endpoint_iri)
    endpoint_iri = f'{o.netloc}{o.path}'

    base = f'{Config.SD_BASE_IRI}/sd/endpoint/{endpoint_iri}/'
    if graph_iri is not None:
        o = urlparse(graph_iri)
        graph_iri = f'{o.netloc}{o.path}'
        base = f'{base}graph/{graph_iri}/'
        graph_iri = URIRef(graph_iri)

    ds_description_iri = URIRef(urljoin(base, str(uuid4())))
    sd_iri = URIRef(sd_iri)
    endpoint_iri = URIRef(endpoint_iri)

    graph = Graph()

    graph.bind('sd', SD)
    graph.bind('void', VOID)
    graph.bind('rdf', RDF)

    graph.add((sd_iri, RDF.type, SD.Service))
    graph.add((sd_iri, SD.endpoint, endpoint_iri))
    graph.add((sd_iri, SD.defaultDatasetDescription, ds_description_iri))
    graph.add((ds_description_iri, RDF.type, SD.Dataset))
    graph.add((ds_description_iri, RDF.type, VOID.Dataset))
    graph.add((ds_description_iri, VOID.sparqlEndpoint, endpoint_iri))

    if graph_iri is None:
        graph.add((ds_description_iri, SD.defaultGraph, BNode()))
    else:
        graph_node_iri = URIRef(urljoin(f'{ds_description_iri}/defaultGraph/', str(uuid4())))
        graph.add((ds_description_iri, SD.namedGraph, graph_node_iri))
        graph.add((graph_node_iri, SD.name, graph_iri))

    return graph
