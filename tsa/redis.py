from enum import Enum
from typing import Tuple


class KeyRoot(Enum):
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
    CONCEPT = 14
    DEREFERENCE = 15
    SUBJECT = 16


root_name = {
    KeyRoot.ANALYSIS: "analyze",
    KeyRoot.RELATED: "related",
    KeyRoot.DISTRIBUTIONS: "distributions",
    KeyRoot.GRAPHS: "graphs",
    KeyRoot.ENDPOINTS: "endpoints",
    KeyRoot.DELAY: "delay",
    KeyRoot.DS_TITLE: "dstitle",
    KeyRoot.CODELISTS: "codelists",
    KeyRoot.LABEL: "label",
    KeyRoot.TYPE: "restype",
    KeyRoot.DESCRIPTION: "description",
    KeyRoot.SAME_AS: "same_as",
    KeyRoot.CONCEPT: "concept",
    KeyRoot.DEREFERENCE: "deref",
    KeyRoot.SUBJECT: "subpure",
}


EXPIRATION_CACHED = 30 * 24 * 60 * 60  # 30D
EXPIRATION_TEMPORARY = 10 * 60 * 60  # 10H
MAX_CONTENT_LENGTH = 512 * 1024 * 1024


def sanitize_key(key: str) -> str:
    if key is None:
        return key
    return "_".join(key.split(":"))


def analysis_dataset(iri: str) -> str:
    iri = sanitize_key(iri)
    return f"{root_name[KeyRoot.ANALYSIS]}:{iri}"


def graph(endpoint: str, iri: str) -> str:
    endpoint = sanitize_key(endpoint)
    iri = sanitize_key(iri)
    return f"{root_name[KeyRoot.GRAPHS]}:{endpoint}:{iri}"


def ds_distr() -> Tuple[str, str]:
    return "dsdistr", "distrds"


def related(unsafe_rel_type: str, unsafe_key: str) -> str:
    sanitized_rel_type = sanitize_key(unsafe_rel_type)
    sanitized_key = sanitize_key(unsafe_key)
    return f"{root_name[KeyRoot.RELATED]}:{sanitized_rel_type!s}:{sanitized_key!s}"


def same_as(unsafe_iri: str) -> str:
    sanitized_iri = sanitize_key(unsafe_iri)
    return f"{root_name[KeyRoot.SAME_AS]}:{sanitized_iri}"


def dataset_endpoint(unsafe_iri_distribution: str) -> str:
    sanitized_iri = sanitize_key(unsafe_iri_distribution)
    return f"{root_name[KeyRoot.ENDPOINTS]}:{sanitized_iri}"


def dereference(unsafe_iri: str) -> str:
    sanitized_iri = sanitize_key(unsafe_iri)
    return f"{root_name[KeyRoot.DEREFERENCE]}:{sanitized_iri}"
