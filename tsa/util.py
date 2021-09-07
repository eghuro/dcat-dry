import rfc3987


def test_iri(iri):
    return iri is not None and rfc3987.match(iri) and (iri.startswith('http://') or iri.startswith('https://'))
