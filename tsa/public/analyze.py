"""Endpoints to start the analysis."""
import redis
import rfc3987
import uuid
import time
from flask import Blueprint, abort, current_app, request, session

from tsa.extensions import redis_pool
from tsa.tasks.batch import inspect_graph

blueprint = Blueprint('analyze', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
def api_analyze_catalog():
    """Analyze a catalog."""
    force = 'force' in request.args
    if 'token' not in session:
        session['token'] = str(uuid.uuid4())

    iri = request.args.get('sparql', None)
    graph = request.args.get('graph', None)
    if iri is not None and graph is not None and rfc3987.match(iri) and rfc3987.match(graph):
            current_app.logger.info(f'Analyzing endpoint {iri}, named graph {graph}')
            red = redis.Redis(connection_pool=redis_pool)

            #Throttling
            key = f'batch:{session["token"]}'
            queueLength = red.scard(key)
            while queueLength > 1000:
                current_app.logger.warning(f'Queue length: {queueLength}')
                time.sleep(60)
                queueLength = red.scard(key)

            t = inspect_graph.si(iri, graph, force).apply_async()
            current_app.logger.info(f'Batch id: {session["token"]}, task id: {t.id}')
            red.hset('taskBatchId', t.id, session["token"])
            return ''
    else:
        abort(400)
