from enum import Enum


class KeyRoot(Enum):
    ANALYSIS = 1
    DISTRIBUTIONS = 3
    DEREFERENCE = 15


root_name = {
    KeyRoot.ANALYSIS: "analyze",
    KeyRoot.DISTRIBUTIONS: "distributions",
    KeyRoot.DEREFERENCE: "deref",
}


def sanitize_key(key: str) -> str:
    if key is None:
        return key
    return "_".join(key.split(":"))


def analysis_dataset(iri: str) -> str:
    iri = sanitize_key(iri)
    return f"{root_name[KeyRoot.ANALYSIS]}:{iri}"


def dereference(unsafe_iri: str) -> str:
    sanitized_iri = sanitize_key(unsafe_iri)
    return f"{root_name[KeyRoot.DEREFERENCE]}:{sanitized_iri}"
