# Plan: loag_graph - expand_graph_with_dereferences - analyze_and_index (RuianEnricher)

import random

import rdflib
from rdflib.term import URIRef

from tsa.commands import divide_chunks
from tsa.tasks.process import get_iris_to_dereference, has_same_as


def test_expand_none():
    assert [] == list(get_iris_to_dereference(None, ''))


def test_expand():
    initial_iri = 'https://rpp-opendata.egon.gov.cz/odrpp/datovasada/agendy.jsonld'
    g = rdflib.Graph()
    with open('./tests/agendy.jsonld', 'r', encoding='utf-8') as graph_file:
        g.parse(data=graph_file.read(), format='json-ld')
    iris_to_dereference = get_iris_to_dereference(g, initial_iri)
    assert 'https://rpp-opendata.egon.gov.cz/odrpp/zdroj/agenda/A960' in iris_to_dereference


ns = rdflib.Namespace('http://localhost')
def get_random_graph():
    g = rdflib.Graph()

    nums = [i for i in range(100)]
    for chunk in divide_chunks(random.sample(nums, 30), 3):
        a, b, c = chunk
        g.add((ns[a], ns[b], ns[c]))
    return g


def test_has_same_as():
    assert  False == has_same_as(None)

    g = rdflib.Graph()
    assert  False == has_same_as(g)

    g = get_random_graph()
    assert False == has_same_as(g)

    g = get_random_graph()
    g.add((ns[3], URIRef('http://www.w3.org/2002/07/owl#sameAs'), ns[5]))
    assert True == has_same_as(g)
