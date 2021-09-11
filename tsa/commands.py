# -*- coding: utf-8 -*-
"""Click commands."""
import logging
import os

import click
from flask import current_app
from flask.cli import with_appcontext
from werkzeug.exceptions import MethodNotAllowed, NotFound

from tsa.tasks.batch import batch_inspect
from tsa.tasks.process import dereference_one
from tsa.util import test_iri

HERE = os.path.abspath(os.path.dirname(__file__))


def divide_chunks(list_to_split, chunk_size):
    # looping till length of the list_to_split
    for i in range(0, len(list_to_split), chunk_size):
        yield list_to_split[i:i + chunk_size]


@click.command()
@click.option('-g', '--graphs', required=True, help='List of graphs')
@click.option('-s', '--sparql', required=True, help='IRI of the SPARQL endpoint')
def batch(graphs=None, sparql=None):
    """Take a list of graphs in a text file and IRI of a SPARQL Endpoint and our API.
    Trigger a batch execution.
    """
    log = logging.getLogger(__name__)
    print(graphs)
    if not test_iri(sparql):
        log.error(f'Not a valid SPARQL Endpoint: {sparql}')
        return
    if len(graphs) == 0:
        log.warning('No graphs given')
        return
    else:
        log.info(f'Analyzing endpoint {sparql}, graphs: {len(graphs)}')
    with open(graphs, 'r', encoding='utf-8') as graphs_file:
        lines = graphs_file.readlines()
        for iris in divide_chunks(lines, 1000):
            graphs = [iri.strip() for iri in iris if test_iri(iri)]
            # inspect_graphs.si(graphs, sparql, False).apply_async()
            batch_inspect.si(sparql, graphs, False, 10).apply_async()


@click.command()
@click.option('-i', '--iri', required=True, help='IRI to dereference')
def dereference(iri=None):
    dereference_one(iri, 'AD-HOC')


@click.command()
def clean():
    """Remove *.pyc and *.pyo files recursively starting at current directory.

    Borrowed from Flask-Script, converted to use Click.
    """
    for dirpath, _, filenames in os.walk('.'):
        for filename in filenames:
            if filename.endswith('.pyc') or filename.endswith('.pyo'):
                full_pathname = os.path.join(dirpath, filename)
                click.echo('Removing {}'.format(full_pathname))
                os.remove(full_pathname)


@click.command()
@click.option('--url', default=None,
              help='Url to test (ex. /static/image.png)')
@click.option('--order', default='rule',
              help='Property on Rule to order by (default: rule)')
@with_appcontext
def urls(url, order):
    """Display all of the url matching routes for the project.

    Borrowed from Flask-Script, converted to use Click.
    """
    rows = []
    column_length = 0
    column_headers = ('Rule', 'Endpoint', 'Arguments')

    if url:
        try:
            rule, arguments = (
                current_app.url_map
                           .bind('localhost')
                           .match(url, return_rule=True))
            rows.append((rule.rule, rule.endpoint, arguments))
            column_length = 3
        except (NotFound, MethodNotAllowed) as e:
            rows.append(('<{}>'.format(e), None, None))
            column_length = 1
    else:
        rules = sorted(
            current_app.url_map.iter_rules(),
            key=lambda rule: getattr(rule, order))
        for rule in rules:
            rows.append((rule.rule, rule.endpoint, None))
        column_length = 2

    str_template = ''
    table_width = 0

    if column_length >= 1:
        max_rule_length = max(len(r[0]) for r in rows)
        max_rule_length = max_rule_length if max_rule_length > 4 else 4
        str_template += '{:' + str(max_rule_length) + '}'
        table_width += max_rule_length

    if column_length >= 2:
        max_endpoint_length = max(len(str(r[1])) for r in rows)
        max_endpoint_length = (
            max_endpoint_length if max_endpoint_length > 8 else 8)
        str_template += '  {:' + str(max_endpoint_length) + '}'
        table_width += 2 + max_endpoint_length

    if column_length >= 3:
        max_arguments_length = max(len(str(r[2])) for r in rows)
        max_arguments_length = (
            max_arguments_length if max_arguments_length > 9 else 9)
        str_template += '  {:' + str(max_arguments_length) + '}'
        table_width += 2 + max_arguments_length

    click.echo(str_template.format(*column_headers[:column_length]))
    click.echo('-' * table_width)

    for row in rows:
        click.echo(str_template.format(*row[:column_length]))
