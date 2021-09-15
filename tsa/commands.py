# -*- coding: utf-8 -*-
"""Click commands."""
import json
import logging
import os

import click
from flask import current_app
from flask.cli import with_appcontext
from werkzeug.exceptions import MethodNotAllowed, NotFound

from tsa.extensions import same_as_index
from tsa.report import import_interesting as import_interesting_impl
from tsa.report import import_labels as import_labels_impl
from tsa.report import import_profiles as import_profiles_impl
from tsa.report import import_related as import_related_impl
from tsa.tasks.batch import batch_inspect
from tsa.tasks.process import dereference_one
from tsa.util import check_iri


def divide_chunks(list_to_split, chunk_size):
    # looping till length of the list_to_split
    for i in range(0, len(list_to_split), chunk_size):
        yield list_to_split[i:i + chunk_size]


@click.command()
@click.option('-g', '--graphs', required=True, help='List of graphs')
@click.option('-s', '--sparql', required=True, help='IRI of the SPARQL endpoint')
def batch(graphs=None, sparql=None):
    """Trigger a batch execution.
    Take a list of graphs in a text file and IRI of a SPARQL Endpoint and our API.
    """
    log = logging.getLogger(__name__)
    print(graphs)
    if not check_iri(sparql):
        log.error('Not a valid SPARQL Endpoint: %s', sparql)
        return
    log.info('Analyzing endpoint %s', sparql)

    with open(graphs, 'r', encoding='utf-8') as graphs_file:
        lines = graphs_file.readlines()
        log.debug('Read lines')
        for iris in divide_chunks(lines, 1000):
            print('.')
            graphs = [iri.strip() for iri in iris if check_iri(iri)]
            # inspect_graphs.si(graphs, sparql, False).apply_async()
            batch_inspect.si(sparql, graphs, False, 10).apply_async()


@click.command()
@click.option('-f', '--file', required=True, help="JSON file with labels from export labels endpoint")
def import_labels(file):
    with open(file, 'r', encoding='utf-8') as labels_file:
        labels = json.load(labels_file)
        import_labels_impl(labels)


@click.command()
@click.option('-f', '--file', required=True, help="JSON file from export sameAs endpoint")
def import_sameas(file):
    with open(file, 'r', encoding='utf-8') as index_file:
        index = json.load(index_file)
        same_as_index.import_index(index)


@click.command()
@click.option('-f', '--file', required=True, help="JSON file from export related endpoint")
def import_related(file):
    with open(file, 'r', encoding='utf-8') as related_file:
        related = json.load(related_file)
        import_related_impl(related)


@click.command()
@click.option('-f', '--file', required=True, help="JSON file from export profiles endpoint")
def import_profiles(file):
    with open(file, 'r', encoding='utf-8') as profiles_file:
        profiles = json.load(profiles_file)
        import_profiles_impl(profiles)


@click.command()
@click.option('-f', '--file', required=True, help="JSON file from export interesting endpoint")
def import_interesting(file):
    with open(file, 'r', encoding='utf-8') as interesting_file:
        interesting_datasets = json.load(interesting_file)
        if isinstance(interesting_datasets, list):
            import_interesting_impl(interesting_datasets)
        else:
            logging.getLogger(__name__).error('Incorrect file')


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
        except (NotFound, MethodNotAllowed) as exc:
            rows.append(('<{}>'.format(exc), None, None))
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
