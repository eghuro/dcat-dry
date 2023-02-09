import requests
from bs4 import BeautifulSoup

for i in range(1, 386):
    r = requests.get("http://prefix.cc/latest/" + str(i))
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    for prefix in soup.find_all(
        "a", attrs={"rel": "rdfs:seeAlso", "property": "vann:preferredNamespaceUri"}
    ):
        prefix = prefix["href"]
        if prefix.startswith("http://"):
            print(prefix[7:].strip())
        elif prefix.startswith("https://"):
            print(prefix[8:].strip())
        else:
            print(prefix.strip())
