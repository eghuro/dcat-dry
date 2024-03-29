# -*- coding: utf-8 -*-
"""Click commands."""
import json
import logging
from typing import Any, Generator, List

import click
from flask import current_app
from flask.cli import with_appcontext
from more_itertools import chunked
from werkzeug.exceptions import MethodNotAllowed, NotFound

from tsa.query import query
from tsa.report import import_labels as import_labels_impl
from tsa.sameas import same_as_index
from tsa.tasks.batch import batch_inspect
from tsa.util import check_iri
from tsa.settings import Config

# register any new command in app.py: register_commands and tsa.rst in docs


def divide_chunks(
    list_to_split: List[Any], chunk_size: int
) -> Generator[Any, None, None]:
    """Yield successive `chunk_size`-sized chunks from `list_to_split.`"""
    # looping till length of the list_to_split
    for i in range(0, len(list_to_split), chunk_size):
        yield list_to_split[i : i + chunk_size]


@click.command()
@click.option("-g", "--graphs", required=True, help="List of graphs")
@click.option("-s", "--sparql", required=True, help="IRI of the SPARQL endpoint")
def batch(graphs=None, sparql=None):
    """Trigger a batch execution.
    Take a list of graphs in a text file and IRI of a SPARQL Endpoint where
    DCAT-AP catalogue is stored in the named graphs listed in the graph's file.
    """
    log = logging.getLogger(__name__)
    if not graphs:
        log.error("No graphs file provided")
        return
    if not check_iri(sparql):
        log.error("Not a valid SPARQL Endpoint: %s", sparql)
        return
    log.info("Analyzing endpoint %s", sparql)

    with open(graphs, "r", encoding="utf-8") as graphs_file:
        for chunk in chunked(graphs_file, 1000):
            batch_inspect.si(sparql, chunk, 10).apply_async()


@click.command()
@click.option(
    "-f",
    "--file",
    required=True,
    help="JSON file with labels from export labels endpoint",
)
def import_labels(file):
    with open(file, "r", encoding="utf-8") as labels_file:
        labels = json.load(labels_file)
        import_labels_impl(labels)


@click.command()
@click.option(
    "-f", "--file", required=True, help="JSON file from export sameAs endpoint"
)
def import_sameas(file):
    with open(file, "r", encoding="utf-8") as index_file:
        index = json.load(index_file)
        same_as_index.import_index(index)


@click.command()
@click.option("-s", "--sparql", required=True, help="IRI of the SPARQL endpoint")
def finalize(sparql=None):
    """Finalize the index after the batch scan is done."""
    query()
    if Config.COUCHDB_URL:
        from tsa.viewer import ViewerProvider

        viewer = ViewerProvider()
        viewer.serialize_to_couchdb(sparql)


@click.command()
@click.option("--url", default=None, help="Url to test (ex. /static/image.png)")
@click.option(
    "--order", default="rule", help="Property on Rule to order by (default: rule)"
)
@with_appcontext
def urls(url, order):
    """Display all of the url matching routes for the project.

    Borrowed from Flask-Script, converted to use Click.
    """
    rows = []
    column_length = 0
    column_headers = ("Rule", "Endpoint", "Arguments")

    if url:
        try:
            rule, arguments = current_app.url_map.bind("localhost").match(
                url, return_rule=True
            )
            rows.append((rule.rule, rule.endpoint, arguments))
            column_length = 3
        except (NotFound, MethodNotAllowed) as exc:
            rows.append((f"<{exc!s}>", None, None))
            column_length = 1
    else:
        rules = sorted(
            current_app.url_map.iter_rules(), key=lambda rule: getattr(rule, order)
        )
        for rule in rules:
            rows.append((rule.rule, rule.endpoint, None))
        column_length = 2

    str_template = ""
    table_width = 0

    if column_length >= 1:
        max_rule_length = max(len(r[0]) for r in rows)
        max_rule_length = max_rule_length if max_rule_length > 4 else 4
        str_template += "{:" + str(max_rule_length) + "}"
        table_width += max_rule_length

    if column_length >= 2:
        max_endpoint_length = max(len(str(r[1])) for r in rows)
        max_endpoint_length = max_endpoint_length if max_endpoint_length > 8 else 8
        str_template += "  {:" + str(max_endpoint_length) + "}"
        table_width += 2 + max_endpoint_length

    if column_length >= 3:
        max_arguments_length = max(len(str(r[2])) for r in rows)
        max_arguments_length = max_arguments_length if max_arguments_length > 9 else 9
        str_template += "  {:" + str(max_arguments_length) + "}"
        table_width += 2 + max_arguments_length

    click.echo(str_template.format(*column_headers[:column_length]))
    click.echo("-" * table_width)

    for row in rows:
        click.echo(str_template.format(*row[:column_length]))
