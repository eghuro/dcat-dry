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
    DEREFERENCE = 15
    SUBJECT = 16


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
    KeyRoot.CONCEPT: 'concept',
    KeyRoot.DEREFERENCE: 'deref',
    KeyRoot.SUBJECT: 'subpure'
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


def graph(endpoint, iri):
    endpoint = sanitize_key(endpoint)
    iri = sanitize_key(iri)
    return f'{root_name[KeyRoot.GRAPHS]}:{endpoint}:{iri}'


def delay(robots_url):
    robots_url = sanitize_key(robots_url)
    return f'delay_{robots_url!s}'


def ds_title(unsafe_label, unsafe_language):
    sanitized_label = sanitize_key(unsafe_label)
    sanitized_language = sanitize_key(unsafe_language)
    return f'{root_name[KeyRoot.DS_TITLE]}:{sanitized_label!s}:{sanitized_language}' if sanitized_language is not None else f'{root_name[KeyRoot.DS_TITLE]}:{sanitized_label!s}'


def label(unsafe_label, unsafe_language):
    sanitized_label = sanitize_key(unsafe_label)
    sanitized_language = sanitize_key(unsafe_language)
    return f'{root_name[KeyRoot.LABEL]}:{sanitized_label!s}:{sanitized_language}' if sanitized_language is not None else f'{root_name[KeyRoot.LABEL]}:{sanitized_label!s}'


def description(unsafe_label, unsafe_language):
    sanitized_label = sanitize_key(unsafe_label)
    sanitized_language = sanitize_key(unsafe_language)
    return f'{root_name[KeyRoot.DESCRIPTION]}:{sanitized_label!s}:{sanitized_language}' if sanitized_language is not None else f'{root_name[KeyRoot.DESCRIPTION]}:{sanitized_label!s}'


def resource_type(unsafe_iri):
    sanitized_iri = sanitize_key(unsafe_iri)
    return f'{root_name[KeyRoot.TYPE]}:{sanitized_iri!s}'


def ds_distr():
    return 'dsdistr', 'distrds'


def related(unsafe_rel_type, unsafe_key):
    sanitized_rel_type = sanitize_key(unsafe_rel_type)
    sanitized_key = sanitize_key(unsafe_key)
    return f'{root_name[KeyRoot.RELATED]}:{sanitized_rel_type!s}:{sanitized_key!s}'


def same_as(unsafe_iri):
    sanitized_iri = sanitize_key(unsafe_iri)
    return f'{root_name[KeyRoot.SAME_AS]}:{sanitized_iri}'


def skos_relation(unsafe_iri):
    sanitized_iri = sanitize_key(unsafe_iri)
    return f'{root_name[KeyRoot.SKOS_RELATION]}:{sanitized_iri}'


def dataset_endpoint(unsafe_iri_distribution):
    sanitized_iri = sanitize_key(unsafe_iri_distribution)
    return f'{root_name[KeyRoot.ENDPOINTS]}:{sanitized_iri}'


def pure_subject(unsafe_iri):
    sanitized_iri = sanitize_key(unsafe_iri)
    return f'{root_name[KeyRoot.SUBJECT]}:{sanitized_iri}'
