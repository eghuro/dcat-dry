import rdflib


def test_jsonld():
    g = rdflib.Graph()
    g.parse("https://data.mpsv.cz/od/soubory/ciselniky/formy-soc-sluzby-ofn.jsonld")
