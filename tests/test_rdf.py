import rdflib
import requests


def test_jsonld():
    r = requests.get('https://data.mpsv.cz/od/soubory/ciselniky/formy-soc-sluzby-ofn.jsonld')
    data = r.text
    g = rdflib.Graph()
    g.parse(data=data, format="json-ld")
