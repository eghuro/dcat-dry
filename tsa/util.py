import rfc3987


def check_iri(iri: str) -> bool:
    """Checks if the provided iri is a valid http or https IRI."""
    return (
        iri is not None
        and rfc3987.match(iri)
        and (iri.startswith("http://") or iri.startswith("https://"))
    )
