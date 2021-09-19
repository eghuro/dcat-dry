import requests

from tsa.tasks.analyze import convert_jsonld


def test_jsonld():
    r = requests.get('https://data.mpsv.cz/od/soubory/ciselniky/formy-soc-sluzby-ofn.jsonld')
    data = r.text
    convert_jsonld(data)
