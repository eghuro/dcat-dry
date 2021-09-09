# -*- coding: utf-8 -*-
"""Query endpoints."""
import json
import uuid
from collections import defaultdict

from bson.json_util import dumps as dumps_bson
from flask import Blueprint, abort, current_app, jsonify, render_template, request
from flask_rdf.flask import returns_rdf

import tsa
from tsa.cache import cached
from tsa.extensions import csrf, mongo_db, same_as_index
from tsa.query import query
from tsa.report import (export_interesting, export_labels, export_profile, export_related, import_interesting,
                        import_labels, import_profiles, import_related, list_datasets, query_dataset)
from tsa.sd import create_sd_iri, generate_service_description
from tsa.util import test_iri

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/query/dataset', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def dcat_viewer_index_query():
    iri = request.args.get('iri', None)
    if test_iri(iri):
        current_app.logger.info(f'Valid dataset request for {iri}')
        # LABELS: key = f'dstitle:{ds!s}:{t.language}' if t.language is not None else f'dstitle:{ds!s}'

        try:
            return jsonify({
                'jsonld': query_dataset(iri)
            })
        except TypeError:
            current_app.logger.exception(f'Failed to query {iri}')
            abort(404)
    abort(400)


@blueprint.route('/api/v1/query/sameas', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def same_as():
    iri = request.args.get('iri', None)
    if test_iri(iri):
        return jsonify(list(same_as_index.lookup(iri)))
    abort(400)


@blueprint.route('/api/v1/query/analysis', methods=['POST'])
@csrf.exempt
def batch_analysis():
    """Get a big report for all required distributions."""
    result_id = str(uuid.uuid4())
    query(result_id)
    return result_id


@blueprint.route('/api/v1/query/analysis/result', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def fetch_analysis():
    batch_id = request.args.get('id', None)
    if batch_id is not None:
        analyses = defaultdict(list)
        for analysis in mongo_db.dsanalyses.find({'batch_id': batch_id}):
            res = {}
            for key in analysis.keys():
                res[key] = analysis[key]
            del res['_id']
            del res['batch_id']
            ds_iri = res['ds_iri']
            del res['ds_iri']
            analyses[ds_iri].append(res)
        if len(analyses.keys()) > 0:
            related = mongo_db.related.find({})
            if related is not None:
                related = json.loads(dumps_bson(related))[0]
                del related['_id']
                return jsonify({'analyses': analyses, 'related': related})
            return jsonify({'analyses': analyses})
        abort(404)
    abort(400)


@blueprint.route('/api/v1/export/labels', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_labels_endpoint():
    return jsonify(export_labels())


@blueprint.route('/api/v1/import/labels', methods=['PUT'])
@csrf.exempt
def import_labels_endpoint():
    labels = request.get_json()
    import_labels(labels)
    return 'OK'


@blueprint.route('/api/v1/export/related', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_related_endpoint():
    obj = export_related()
    del obj['_id']
    return jsonify(obj)


@blueprint.route('/api/v1/export/profile', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_profile_endpoint():
    lst = []
    for entry in export_profile():
        del entry['_id']
        lst.append(entry)
    return jsonify(lst)


@blueprint.route('/api/v1/export/sameas', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_sameas_endpoint():
    return jsonify(same_as_index.export_index())


@blueprint.route('/api/v1/import/sameas', methods=['PUT'])
@csrf.exempt
def import_sameas_endpoint():
    index = request.get_json()
    same_as_index.import_index(index)
    return 'OK'


@blueprint.route('/api/v1/import/related', methods=['PUT'])
@csrf.exempt
def import_related_endpoint():
    related = request.get_json()
    import_related(related)
    return 'OK'


@blueprint.route('/api/v1/import/profile', methods=['PUT'])
@csrf.exempt
def import_profile_endpoint():
    profiles = request.get_json()
    import_profiles(profiles)
    return 'OK'


@blueprint.route('/api/v1/export/interesting', methods=['GET'])
def export_interesting_endpoint():
    return jsonify(export_interesting())


@blueprint.route('/api/v1/import/interesting', methods=['POST'])
def import_interesting_endpoint():
    interesting_datasets = request.get_json()
    if isinstance(interesting_datasets, list):
        import_interesting(interesting_datasets)
        return 'OK'
    abort(400)


@blueprint.route('/list', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def view_list():
    data = list_datasets()
    current_app.logger.debug(data)
    return render_template('list.html', datasets=data)


@blueprint.route('/detail', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def view_detail():
    iri = request.args.get('iri', None)
    profile = query_dataset(iri)
    return render_template('detail.html', dataset=profile)


@blueprint.route('/sd')
@returns_rdf
def service_description():
    endpoint_iri = request.args.get('endpoint', None)
    graph_iri = request.args.get('graph', None)
    query_string = request.query_string.decode('utf-8')
    return generate_service_description(create_sd_iri(query_string), endpoint_iri, graph_iri)


@blueprint.route('/api/v1/version')
def version():
    doc = {
        'app': tsa.__version__
    }
    if tsa.__revision__ != 'PLACEHOLDER':
        doc['revision'] = tsa.__revision__
    return jsonify(doc)
