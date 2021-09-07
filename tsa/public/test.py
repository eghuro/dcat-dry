"""Test endpoints."""
import logging

import rdflib
import redis
from flask import Blueprint, abort, current_app, make_response, request

from tsa.extensions import redis_pool
from tsa.net import fetch, get_content, guess_format
from tsa.tasks.analyze import do_analyze_and_index, load_graph
from tsa.tasks.process import dereference_one, expand_graph_with_dereferences, get_iris_to_dereference
from tsa.tasks.system import hello, system_check
from tsa.util import test_iri

blueprint = Blueprint('test', __name__, static_folder='../static')


@blueprint.route('/api/v1/test/base')
def test_basic():
    """Basic test returning hello world."""
    return 'Hello world!'


@blueprint.route('/api/v1/test/job')
def test_celery():
    """Hello world test using Celery task."""
    r = hello.delay()
    return r.get()


@blueprint.route('/api/v1/test/system')
def test_system():
    """Test systems and provide a hello world."""
    x = (system_check.s() | hello.si()).delay().get()
    log = logging.getLogger(__name__)
    log.info(f'System check result: {x!s}')
    return str(x)


@blueprint.route('/api/v1/test/analyze')
def api_test():
    iri = request.args['iri']
    log = current_app.logger
    red = redis.Redis(connection_pool=redis_pool)
    r = fetch(iri, log, red)
    guess, _ = guess_format(iri, r, log, red)
    content = get_content(iri, r, red).encode('utf-8')
    graph = load_graph(iri, content, guess)
    do_analyze_and_index(graph, iri, red)
    return ''


@blueprint.route('/api/v1/test/dereference/1')
def test_dereference1():
    iri = 'https://data.cssz.cz/dump/ukazatel-pracovni-neschopnosti-podle-delky-trvani-dpn-a-kraju.trig'

    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    try:
        to_dereference = frozenset(get_iris_to_dereference(load_graph(iri, get_content(iri, fetch(iri, log, red), red).encode('utf-8'), 'trig'), iri))
        iri_of_interest = 'https://data.cssz.cz/resource/ruian/vusc/27'
        if iri_of_interest not in to_dereference:
            log.error('Missing IRI of interest in a set to dereference')
        if not test_iri(iri_of_interest):
            log.error('Condition failed')
        r = fetch(iri_of_interest, log, red)
        guess, _ = guess_format(iri_of_interest, r, log, red)
        content = get_content(iri_of_interest, r, red).encode('utf-8')
        sub_graph = load_graph(iri_of_interest, content, guess).serialize(format='trig')
        if sub_graph is not None:
            return make_response(sub_graph)
    except:
        log.exception('Fail')
    abort(500)


@blueprint.route('/api/v1/test/dereference/2')
def test_dereference2():
    iri = 'https://data.cssz.cz/dump/ukazatel-pracovni-neschopnosti-podle-delky-trvani-dpn-a-kraju.trig'
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    graph = rdflib.ConjunctiveGraph()
    try:
        graph = expand_graph_with_dereferences(load_graph(iri, get_content(iri, fetch(iri, log, red), red).encode('utf-8'), 'trig'), iri).serialize(format='trig')
        if graph is not None:
            return make_response(graph)
    except:
        log.exception('Fail')
    abort(500)


@blueprint.route('/api/v1/test/process')
def test_process():
    """Testbed: fetch a distribution, dereference and run cube."""

    iri_distr = 'https://data.cssz.cz/dump/doba-rizeni-o-namitkach.trig'

    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    try:
        r = fetch(iri_distr, log, red)
    except:
        log.exception(f'Failed to fetch: {iri_distr}')
        abort(500)

    guess, _ = guess_format(iri_distr, r, log, red)

    try:
        content = get_content(iri_distr, r, red)
        if content is None:
            log.warning(f'No content: {iri_distr}')
            abort(500)
        else:
            content.encode('utf-8')

        graph = load_graph(iri_distr, content, guess)
        # zde ziskat resources k dereferenci a nafouknout graf
        for iri_to_dereference in frozenset(get_iris_to_dereference(graph, iri_distr)):
            log.info(f'Dereference: {iri_to_dereference}')
            try:
                r = fetch(iri_to_dereference, log, red)
                guess, _ = guess_format(iri_to_dereference, r, log, red)
                content = get_content(iri_to_dereference, r, red)
                if content is None:
                    log.warning(f'No content: {iri_to_dereference}')
                    continue
                else:
                    content.encode('utf-8')
                sub_graph = load_graph(iri_to_dereference, content, guess)
                graph += sub_graph
            except:
                log.exception(f'Failed to dereference: {iri_to_dereference}')

        return graph.serialize(format='trig')
        # do_analyze_and_index(graph, iri, red)
    except:
        log.exception(f'Failed to process: {iri_distr}')
        abort(500)


@blueprint.route('/api/v1/test/dereference/3')
def test_dereference3():
    iri = 'https://linked.cuzk.cz/resource/ruian/adresni-misto/27729389'
    return make_response(dereference_one(iri).serialize(format='trig'))
