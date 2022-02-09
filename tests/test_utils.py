from rdflib import Graph, Namespace

from tsa.redis import pure_subject
from tsa.tasks.process import filter_iri, sanitize_list, store_pure_subjects
from tsa.util import check_iri


def test_filter_iris():
    assert True == filter_iri("http://services.cuzk.cz/abc.trig")
    assert True == filter_iri("https://services.cuzk.cz/abc.jsonld")
    assert True == filter_iri("http://localhost/abc.csv.zip")
    assert True == filter_iri("http://localhost/abc.csv")
    assert True == filter_iri("http://localhost/abc.csv.gz")
    assert True == filter_iri("http://localhost/abc.xls")
    assert True == filter_iri("http://localhost/abc.docx")
    assert True == filter_iri("http://localhost/abc.xlsx")
    assert True == filter_iri("http://localhost/abc.pdf")
    assert True == filter_iri("http://vdp.cuzk.cz/abc.xml")
    assert True == filter_iri("https://vdp.cuzk.cz/abc.xml.zip")
    # assert False == filter_iri('http://vdp.cuzk.cz/abc.trig')
    # assert False == filter_iri('https://vdp.cuzk.cz/abc.trig')
    assert True == filter_iri("http://dataor.justice.cz/abc.xml")
    assert True == filter_iri("https://dataor.justice.cz/abc.xml.gz")
    assert True == filter_iri("https://apl.czso.cz/iSMS/cisexp.jsp")
    assert True == filter_iri("https://eagri.cz/abc.trig")
    assert True == filter_iri("https://volby.cz/pls/ps2017/vysledky_okres/abc.jsonld")


def test_check_iris():
    assert False == check_iri(None)
    assert False == check_iri("")
    assert False == check_iri("xzy")
    assert False == check_iri("file:///home/foo/data.trig")
    assert True == check_iri("http://localhost/test.trig")
    assert True == check_iri("https://localhost/test.trig")


def test_sanitize_list():
    assert [] == list(sanitize_list(None))
    assert [] == list(sanitize_list([None]))
    assert [] == list(sanitize_list([None, None]))
    assert [1] == list(sanitize_list([1]))
    assert [0, 1] == list(sanitize_list([0, None, 1]))


def test_store_pure():
    class RedisMock:
        def __init__(self):
            self.__list = []
            self.__key = None

        def lpush(self, key, *items):
            self.__list.extend(items)
            self.__key = key

        @property
        def list(self):
            return self.__list

        @property
        def key(self):
            return self.__key

        def clear(self):
            self.__list = []

    red = RedisMock()

    g = Graph()
    store_pure_subjects("a", g, red)
    assert red.key == None
    assert red.list == []

    ns = Namespace("file://localhost/")
    g.add((ns["a"], ns["b"], ns["c"]))
    store_pure_subjects("b", g, red)
    assert red.key == pure_subject("b")
    assert set(red.list) == set([str(ns["a"])])

    g.add((ns["a"], ns["c"], ns["d"]))
    store_pure_subjects("", g, red)
    assert red.key == pure_subject("")
    assert set(red.list) == set([str(ns["a"])])

    g.add((ns["b"], ns["c"], ns["d"]))
    store_pure_subjects(None, g, red)
    assert red.key == pure_subject(None)
    assert set(red.list) == set([str(ns["a"]), str(ns["b"])])
