import click
import requests


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


@click.command()
@click.option('-g', '--graphs', required=True, help='List of graphs')
@click.option('-a', '--api', required=True, help='IRI of the API endpoint')
@click.option('-s', '--sparql', required=True, help='IRI of the SPARQL endpoint')
def main(graphs=None, api=None, sparql=None):
    iri = f'{api}/api/v1/analyze/catalog?sparql={sparql}'
    print(graphs)
    print(iri)
    with open(graphs, 'r', encoding='utf-8') as graphs_file:
        with requests.Session() as session:
            lines = graphs_file.readlines()
            for sublist in divide_chunks(lines, 1000):
                response = session.post(iri, json=sublist)
                response.raise_for_status()


if __name__ == "__main__":
    main()
