"""Endpoints to start the analysis."""
import uuid

import redis
from flask import Blueprint, abort, current_app, request, session

from tsa.extensions import csrf, redis_pool
from tsa.redis import ds_distr, ds_title
from tsa.tasks.batch import batch_inspect
from tsa.tasks.process import dereference_one, process_priority
from tsa.util import test_iri

blueprint = Blueprint('analyze', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
@csrf.exempt
def api_analyze_catalog():
    """Analyze a catalog.

    URL parameters sparql, graph (optional)
    """
    force = 'force' in request.args

    iri = request.args.get('sparql', None)
    graph = request.args.get('graph', None)
    if test_iri(iri):
        if graph is None:
            iris = request.get_json()
            graphs = [iri.strip() for iri in iris if test_iri(iri)]
        elif test_iri(graph):
            graphs = [graph]
        else:
            graphs = []
        current_app.logger.info(f'Analyzing endpoint {iri}')
        if len(graphs) == 0:
            current_app.logger.warning('No graphs given')
        else:
            current_app.logger.info(f'Graphs: {len(graphs)}')

        batch_inspect.si(iri, graphs, force, 10).apply_async()
        return ''
    abort(400)


@blueprint.route('/api/v1/analyze/distributions', methods=['POST'])
@csrf.exempt
def api_analyze_list():
    print('analyze list')
    # JSON object in body: {distribution_iri -> dataset_iri}
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
            task = sig.apply_async()

            token = session['token']
            current_app.logger.info(f'Batch id: {token}, task id: {task.id}')
            pipe.hset('taskBatchId', task.id, token)
            pipe.execute()
    return ''


@blueprint.route('/api/v1/analyze/resource', methods=['POST'])
@csrf.exempt
def api_analyze_resource():
    if 'iri' not in request.args:
        abort(400)
    iri = request.args['iri']
    dereference_one(iri)
