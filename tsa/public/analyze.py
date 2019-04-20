"""Endpoints to start the analysis."""
import rfc3987
from flask import Blueprint, abort, current_app, request

from tsa.tasks.analyze import analyze, process_endpoint
from tsa.tasks.batch import inspect_catalog, inspect_endpoint
from tsa.tasks.query import index_distribution_query

blueprint = Blueprint('analyze', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze/distribution', methods=['POST'])
def api_analyze_iri():
    """Analyze a distribution."""
    iri = request.args.get('iri', None)

    if iri is not None:
        current_app.logger.info(f'Analyzing distribution for: {iri}')
        if rfc3987.match(iri):
            analyze.delay(iri)
            return 'OK'
        abort(400)
    else:
        iris = []
        for iri in request.get_json():
            if rfc3987.match(iri):
                iris.append(iri)
        for iri in iris:
            current_app.logger.info(f'Analyzing distribution for: {iri}')
            analyze.delay(iri)
        return 'OK'


@blueprint.route('/api/v1/analyze/endpoint', methods=['POST'])
def api_analyze_endpoint():
    """Analyze an Endpoint."""
    iri = request.args.get('sparql', None)

    current_app.logger.info(f'Analyzing SPARQL endpoint: {iri}')

    if rfc3987.match(iri):
        (process_endpoint.si(iri) | index_distribution_query.si(iri)).apply_async()
        return 'OK'
    abort(400)


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
def api_analyze_catalog():
    """Analyze a catalog."""
    if 'iri' in request.args:
        iri = request.args.get('iri', None)
        current_app.logger.info(f'Analyzing a DCAT catalog from a distribution under {iri}')
        if rfc3987.match(iri):
            (inspect_catalog.si(iri) | index_distribution_query.si(iri)).apply_async()
            return 'OK'
        abort(400)
    elif 'sparql' in request.args:
        iri = request.args.get('sparql', None)
        current_app.logger.info(f'Analyzing datasets from an endpoint under {iri}')
        if rfc3987.match(iri):
            (inspect_endpoint.si(iri) | index_distribution_query.si(iri)).apply_async()
            return 'OK'
        abort(400)
    else:
        abort(400)