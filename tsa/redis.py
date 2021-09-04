from enum import Enum


class KeyRoot(Enum):
    DATA = 0
    ANALYSIS = 1
    RELATED = 2
    DISTRIBUTIONS = 3
    GRAPHS = 4
    ENDPOINTS = 5
    DELAY = 6
    DS_TITLE = 7
    CODELISTS = 8
    LABEL = 9
    TYPE = 10
    DESCRIPTION = 11
    SAME_AS = 12
    SKOS_RELATION = 13
    CONCEPT = 14


root_name = {
    KeyRoot.DATA: 'data',
    KeyRoot.ANALYSIS: 'analyze',
    KeyRoot.RELATED: 'related',
    KeyRoot.DISTRIBUTIONS: 'distributions',
    KeyRoot.GRAPHS: 'graphs',
    KeyRoot.ENDPOINTS: 'endpoints',
    KeyRoot.DELAY: 'delay',
    KeyRoot.DS_TITLE: 'dstitle',
    KeyRoot.CODELISTS: 'codelists',
    KeyRoot.LABEL: 'label',
    KeyRoot.TYPE: 'restype',
    KeyRoot.DESCRIPTION: 'description',
    KeyRoot.SAME_AS: 'same_as',
    KeyRoot.SKOS_RELATION: 'skos',
    KeyRoot.CONCEPT: 'concept'
}


EXPIRATION_CACHED = 30 * 24 * 60 * 60  # 30D
EXPIRATION_TEMPORARY = 10 * 60 * 60  # 10H
MAX_CONTENT_LENGTH = 512 * 1024 * 1024


expiration = {
    KeyRoot.DATA: EXPIRATION_CACHED,
    KeyRoot.ANALYSIS: EXPIRATION_CACHED,
    KeyRoot.RELATED: EXPIRATION_CACHED,
    KeyRoot.DISTRIBUTIONS: EXPIRATION_CACHED,
    KeyRoot.GRAPHS: 24 * 60 * 60,
}

def sanitize_key(key):
    if key is None:
        return key
    return '_'.join(key.split(':'))

def data(*args):
    if len(args) == 1:
        iri = sanitize_key(args[0])
        return f'{root_name[KeyRoot.DATA]}:{iri}'
    if len(args) == 2:
        (endpoint, graph_iri) = args
        endpoint = sanitize_key(endpoint)
        graph_iri = sanitize_key(graph_iri)
        return f'{root_name[KeyRoot.DATA]}:{endpoint}:{graph_iri}'
    raise TypeError('Submit only one (distribution IRI) or two (endpoint + graph IRIs) positional arguments')


def analysis_dataset(iri):
    iri = sanitize_key(iri)
    return f'{root_name[KeyRoot.ANALYSIS]}:{iri}'

def analysis_endpoint(endpoint, graph_iri):
    endpoint = sanitize_key(endpoint)
    graph_iri = sanitize_key(graph_iri)
    return f'{root_name[KeyRoot.ANALYSIS]}:{endpoint}:{graph_iri}'

def graph(endpoint, iri):
    endpoint = sanitize_key(endpoint)
    iri = sanitize_key(iri)
    return f'{root_name[KeyRoot.GRAPHS]}:{endpoint}:{iri}'

def delay(robots_url):
    robots_url = sanitize_key(robots_url)
    return f'delay_{robots_url!s}'

def ds_title(ds, language):
    ds = sanitize_key(ds)
    language = sanitize_key(language)
    return f'{root_name[KeyRoot.DS_TITLE]}:{ds!s}:{language}' if language is not None else f'{root_name[KeyRoot.DS_TITLE]}:{ds!s}'

def label(ds, language):
    ds = sanitize_key(ds)
    language = sanitize_key(language)
    return f'{root_name[KeyRoot.LABEL]}:{ds!s}:{language}' if language is not None else f'{root_name[KeyRoot.LABEL]}:{ds!s}'

def description(ds, language):
    ds = sanitize_key(ds)
    language = sanitize_key(language)
    return f'{root_name[KeyRoot.DESCRIPTION]}:{ds!s}:{language}' if language is not None else f'{root_name[KeyRoot.DESCRIPTION]}:{ds!s}'

def resource_type(iri):
    iri = sanitize_key(iri)
    return f'{root_name[KeyRoot.TYPE]}:{iri!s}'

def ds_distr():
    return 'dsdistr', 'distrds'

def related(rel_type, key):
    rel_type = sanitize_key(rel_type)
    key = sanitize_key(key)
    return f'{root_name[KeyRoot.RELATED]}:{rel_type!s}:{key!s}'

def codelist(obj):
    obj = sanitize_key(obj)
    return f'{root_name[KeyRoot.CODELISTS]}:{obj}'

def same_as(iri):
    iri = sanitize_key(iri)
    return f'{root_name[KeyRoot.SAME_AS]}:{iri}'

def skos_relation(iri):
    iri = sanitize_key(iri)
    return f'{root_name[KeyRoot.SKOS_RELATION]}:{iri}'


def dataset_endpoint(iri_distribution):
    iri = sanitize_key(iri_distribution)
    return f'{root_name[KeyRoot.DISTRIBUTION]}:{iri}'
