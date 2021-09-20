# -*- coding: utf-8 -*-
"""Query endpoints."""
import json
from collections import defaultdict

from bson.json_util import dumps as dumps_bson
from flask import Blueprint, abort, current_app, jsonify, render_template, request
from flask_rdf.flask import returns_rdf

import tsa
from tsa.cache import cached
from tsa.extensions import mongo_db, same_as_index
from tsa.report import export_interesting, export_labels, export_profile, export_related, list_datasets, query_dataset
from tsa.sd import create_sd_iri, generate_service_description
from tsa.util import check_iri

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/query/dataset', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def dcat_viewer_index_query():  # noqa: inconsistent-return-statements
    iri = request.args.get('iri', None)
    if check_iri(iri):
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


@blueprint.route('/api/v1/query/analysis', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def fetch_analysis():  # noqa: inconsistent-return-statements
    analyses = defaultdict(list)
    for analysis in mongo_db.dsanalyses.find({}):
        res = {}
        for a in analysis:
            res[key] = analysis[key]
        del res['_id']
        ds_iri = res['ds_iri']
        del res['ds_iri']
        analyses[ds_iri].append(res)

    if len(analyses.keys()) > 0:
        all_related = mongo_db.related.find({})
        related = defaultdict(list)
        if all_related is not None:
            for item in all_related:
                record = {}
                record['iri'] = item['iri']
                record['related'] = item['related']
                related[item['type']].append(record)
            return jsonify({'analyses': analyses, 'related': related})
        return jsonify({'analyses': analyses})
    abort(204)


@blueprint.route('/api/v1/export/labels', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_labels_endpoint():
    return jsonify(export_labels())


@blueprint.route('/api/v1/export/related', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_related_endpoint():
    obj = export_related()
    del obj['_id']
    return jsonify(obj)


@blueprint.route('/api/v1/export/profile', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_profile_endpoint():
    lst = []
    for entry in export_profile():
        del entry['_id']
        lst.append(entry)
    return jsonify(lst)


@blueprint.route('/api/v1/export/sameas', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def export_sameas_endpoint():  # noqa: inconsistent-return-statements
    return jsonify(same_as_index.export_index())


@blueprint.route('/api/v1/export/interesting', methods=['GET'])  # noqa: unused-function
def export_interesting_endpoint():
    return jsonify(export_interesting())


@blueprint.route('/list', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def view_list():
    data = list_datasets()
    current_app.logger.debug(data)
    return render_template('list.html', datasets=data)


@blueprint.route('/detail', methods=['GET'])  # noqa: unused-function
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def view_detail():  # noqa: inconsistent-return-statements
    iri = request.args.get('iri', None)
    if check_iri(iri):
        profile = query_dataset(iri)
        return render_template('detail.html', dataset=profile)
    abort(400)


@blueprint.route('/sd', methods=['GET'])  # noqa: unused-function
@returns_rdf
def service_description():  # noqa: inconsistent-return-statements
    endpoint_iri = request.args.get('endpoint', None)
    graph_iri = request.args.get('graph', None)
    query_string = request.query_string.decode('utf-8')
    if check_iri(endpoint_iri) and check_iri(graph_iri):
        return generate_service_description(create_sd_iri(query_string), endpoint_iri, graph_iri)
    abort(400)


@blueprint.route('/api/v1/version', methods=['GET'])  # noqa: unused-function
def version():
    doc = {
        'app': tsa.__version__
    }
    if tsa.__revision__ != 'PLACEHOLDER':
        doc['revision'] = tsa.__revision__
    return jsonify(doc)
