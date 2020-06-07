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
}


EXPIRATION_CACHED = 30 * 24 * 60 * 60  # 30D
EXPIRATION_TEMPORARY = 60 * 60  # 1H
MAX_CONTENT_LENGTH = 512 * 1024 * 1024


expiration = {
    KeyRoot.DATA: EXPIRATION_CACHED,
    KeyRoot.ANALYSIS: EXPIRATION_CACHED,
    KeyRoot.RELATED: EXPIRATION_CACHED,
    KeyRoot.DISTRIBUTIONS: EXPIRATION_CACHED,
    KeyRoot.GRAPHS: 24 * 60 * 60,
    KeyRoot.ENDPOINTS: 24 * 60 * 60,
}

def data(*args):
    if len(args) == 1:
        (iri) = args
        return f'{root_name[KeyRoot.DATA]}:{iri}'
    if len(args) == 2:
        (endpoint, graph_iri) = args
        return f'{root_name[KeyRoot.DATA]}:{endpoint}:{graph_iri}'
    raise TypeError('Submit only one (distribution IRI) or two (endpoint + graph IRIs) positional arguments')


def analysis_dataset(iri):
    return f'{root_name[KeyRoot.ANALYSIS]}:{iri}'

def analysis_endpoint(endpoint, graph_iri):
    return f'{root_name[KeyRoot.ANALYSIS]}:{endpoint}:{graph_iri}'

def graph(endpoint, iri):
    return f'{root_name[KeyRoot.GRAPHS]}:{endpoint}:{iri}'

def delay(robots_url):
    return f'delay_{robots_url!s}'

def ds_title(ds, language):
    return f'{root_name[KeyRoot.DS_TITLE]}:{ds!s}:{language}' if language is not None else f'{root_name[KeyRoot.DS_TITLE]}:{ds!s}'

def ds_distr():
    return 'dsdistr', 'distrds'

def related(rel_type, key):
    return f'{root_name[KeyRoot.RELATED]}:{rel_type!s}:{key!s}'

def codelist(object):
    return f'{root_name[KeyRoot.CODELISTS]}:{object}'