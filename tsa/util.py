from __future__ import annotations

import rfc3987


def check_iri(iri: str | None) -> bool:
    """
    Checks if the provided iri is a valid http or https IRI.

    :param iri: IRI to check
    :return: True if the IRI is valid, False otherwise
    """
    return (
        iri is not None
        and rfc3987.match(iri)
        and (iri.startswith("http://") or iri.startswith("https://"))
    )
