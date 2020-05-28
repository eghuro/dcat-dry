# -*- coding: utf-8 -*-
"""Query endpoints."""
import itertools
import json
from collections import defaultdict

import redis
import rfc3987
import uuid
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.extensions import redis_pool, mongo_db
from tsa.monitor import Monitor
from tsa.tasks.system import cleanup
from tsa.report import query_dataset
from tsa.cache import cached
from tsa.query import query

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/query/dataset', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def dcat_viewer_index_query():
    iri = request.args.get('iri', None)
    lang = request.args.get('language', 'cs')
    if iri is not None:
        if rfc3987.match(iri):
            current_app.logger.info(f'Valid dataset request ({lang}) for {iri}')
            #LABELS: key = f'dstitle:{ds!s}:{t.language}' if t.language is not None else f'dstitle:{ds!s}'

            translation = {
                'https://data.gov.cz/zdroj/datové-sady/https---data.cssz.cz-api-3-action-package_show-id-prehled-o-celkovem-poctu-osvc-podle-okresu': 'https://data.gov.cz/zdroj/datové-sady/CSShZbzpcn/695492977/f24c8df0bce40fd04c3c4bfc81d7e68e',
                'https://data.gov.cz/zdroj/datové-sady/https---data.cssz.cz-api-3-action-package_show-id-pomocne-ciselniky': 'https://data.gov.cz/zdroj/datové-sady/CSShZbzpcn/695492977/38534df5f78ce360ba0cf32665bb2729',
                'https://data.gov.cz/zdroj/datové-sady/https---data.cssz.cz-api-3-action-package_show-id-ukazatele-pracovni-neschopnosti-podle-pohlavi-a-diagnozy': 'https://data.gov.cz/zdroj/datové-sady/CSShZbzpcn/695492977/8d3ad288011e2ceab4c381e1fedc6dae',
                'https://data.gov.cz/zdroj/datové-sady/https---data.cssz.cz-api-3-action-package_show-id-prum-vek-u-nove-priznanych-duchodu-dle-druhu': 'https://data.gov.cz/zdroj/datové-sady/CSShZbzpcn/695492977/0c28dd4455e74063f95669aaa90fa329',
            }

            if iri in translation.keys():
                iri = translation[iri]
                current_app.logger.warn(f'Translating iri to {iri}')

            try:
                return jsonify({
                    "jsonld": query_dataset(iri)
                })
            except TypeError:
                current_app.logger.exception(f'Failed to query {iri}')
                abort(404)
    abort(400)


@blueprint.route('/api/v1/query/analysis', methods=['POST'])
def batch_analysis():
    """
    Get a big report for all required distributions.
    """
    red = redis.Redis(connection_pool=redis_pool)
    result_id = str(uuid.uuid4())
    query(result_id, red)
    return result_id


@blueprint.route('/api/v1/query/analysis/result', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def fetch_analysis():
    id = request.args.get('id', None)
    if id is not None:
        analyses = defaultdict(list)
        for analysis in mongo_db.dsanalyses.find({'batch_id': id}):
            del analysis['_id']
            del analysis['batch_id']
            ds_iri = analysis['ds_iri']
            del analysis['ds_iri']
            analyses[ds_iri].append(analysis)
        if len(analyses) > 0:
            related = mongo_db.related.find({})
            if related is not None:
                return jsonify({'analyses': analyses, 'related': related})
            return jsonify({'analyses': analyses})
        else:
            abort(404)
    else:
        abort(400)


@blueprint.route('/api/v1/cleanup', methods=['POST', 'DELETE'])
def cleanup_endpoint():
    """Clean any purgeable records, Flask cache and possibly also stats."""
    extra = ['purgeable']
    stats = 'stats' in request.args
    if stats:
        extra.extend(Monitor.KEYS)

    cleanup.si(current_app.config['CACHE_KEY_PREFIX'], extra).apply_async(queue='low_priority').get()
    return 'OK'
