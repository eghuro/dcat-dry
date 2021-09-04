"""Endpoints to start the analysis."""
import uuid

import redis
import rfc3987
from flask import Blueprint, abort, current_app, request, session

from tsa.extensions import redis_pool
from tsa.net import fetch, get_content, guess_format
from tsa.redis import ds_distr, ds_title
from tsa.tasks.analyze import do_analyze_and_index, load_graph
from tsa.tasks.batch import batch_inspect
from tsa.tasks.process import dereference_one, process_priority

blueprint = Blueprint('analyze', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
def api_analyze_catalog():
    """Analyze a catalog.

    URL parameters sparql, graph (optional)
    """
    force = 'force' in request.args
    if 'token' not in session:
        session['token'] = str(uuid.uuid4())

    iri = request.args.get('sparql', None)
    graph = request.args.get('graph', None)
    if iri is not None and rfc3987.match(iri):
        if graph is None:
            iris = request.get_json()
            graphs = [iri.strip() for iri in iris if rfc3987.match(iri)]
        else:
            if rfc3987.match(graph):
                graphs = [graph]
            else:
                graphs = []
        current_app.logger.info(f'Analyzing endpoint {iri}')
        if len(graphs) == 0:
            current_app.logger.warning("No graphs given")
        else:
            current_app.logger.info(f'Graphs: {len(graphs)}')

        red = redis.Redis(connection_pool=redis_pool)

        t = batch_inspect.si(iri, graphs, force, session["token"], 10).apply_async()
        current_app.logger.info(f'Batch id: {session["token"]}, task id: {t.id}')
        red.hset('taskBatchId', t.id, session["token"])
        return ''
    else:
        abort(400)


@blueprint.route('/api/v1/analyze/distributions', methods=['POST'])
def api_analyze_list():
    print('analyze list')
    #JSON object in body: {distribution_iri -> dataset_iri}
    force = 'force' in request.args
    if 'token' not in session:
        session['token'] = str(uuid.uuid4())
    distribution_dataset = request.get_json()
    red = redis.Redis(connection_pool=redis_pool)
    dsdistr, distrds = ds_distr()
    counter = 1
    for distribution_iri in distribution_dataset.keys():
        with red.pipeline() as pipe:
            dataset_iri = distribution_dataset[distribution_iri]
            pipe.sadd(f'{dsdistr}:{str(dataset_iri)}', str(distribution_iri))
            pipe.sadd(f'{distrds}:{str(distribution_iri)}', str(dataset_iri))
            key = ds_title(dataset_iri, 'cs')
            pipe.set(key, f'Dataset {counter!s}')
            counter = counter + 1

            sig = process_priority.si(distribution_iri, force)
            t = sig.apply_async()

            current_app.logger.info(f'Batch id: {session["token"]}, task id: {t.id}')
            pipe.hset('taskBatchId', t.id, session["token"])
            pipe.execute()
    return ''


@blueprint.route('/api/v1/analyze/resource', methods=['POST'])
def api_analyze_resource():
    if 'iri' not in request.args:
        abort(400)
    iri = request.args['iri']
    dereference_one(iri)


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
