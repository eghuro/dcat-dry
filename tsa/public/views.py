# -*- coding: utf-8 -*-
"""Query endpoints."""
import json
from collections import OrderedDict, defaultdict

import redis
import rfc3987
from atenvironment import environment
from celery import group
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.cache import cached
from tsa.tasks.query import index_distribution_query

from .stat import retrieve_size_stats

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
@environment('REDIS')
def api_analyze_get(redis_url):
    """Read the analysis."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    if rfc3987.match(iri):
        key = f'analyze:{iri!s}'
        if not r.exists(key):
            abort(404)
        else:
            return jsonify(json.loads(r.get(key)))
    else:
        abort(400)


@blueprint.route('/api/v1/query/distribution', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
@environment('REDIS')
def distr_index(redis_url):
    """Query an RDF distribution sumbitted for analysis."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    current_app.logger.info(f'Querying distribution for: {iri}')
    if rfc3987.match(iri):
        if not r.exists(f'ds:{iri}'):
            abort(404)
        else:
            index_distribution_query.s(iri).apply_async().get()
            return jsonify(json.loads(r.get(f'distrquery:{iri}')))
    abort(400)


def _graph_iris(r):
    for e in r.smembers('endpoints'):
        for g in r.smembers(f'graphs:{e}'):
            yield f'{e}/{g}'


@environment('REDIS')
def _get_known_distributions(redis_url):
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    distr_endpoints = r.smembers('distributions').union(frozenset(_graph_iris(r)))
    failed_skipped = r.smembers('stat:failed').union(r.smembers('stat:skipped'))
    return distr_endpoints.difference(failed_skipped)


@blueprint.route('/api/v1/query/analysis', methods=['GET'])
def known_distributions():
    """List known distributions and endpoints without failed or skipped ones."""
    return jsonify(list(_get_known_distributions()))


def skip(iri, r):
    """Condition if iri should be skipped from gathering analyses."""
    key = f'analyze:{iri!s}'
    return r.sismember('stat:failed', iri) or\
        r.sismember('stat:skipped', iri) or\
        not rfc3987.match(iri) or\
        not r.exists(key)


def missing(iri, r):
    """Condition if index query result is missing."""
    key = f'distrquery:{iri!s}'
    return not r.exists(key) and not r.sismember('stat:failed', iri) and not r.sismember('stat:skipped', iri)


def gather_analyses(iris, r):
    """Compile analyses for all iris from all analyzers."""
    analyses = []
    predicates = defaultdict(int)
    classes = defaultdict(int)

    for iri in iris:
        if skip(iri, r):
            continue
        key = f'analyze:{iri!s}'
        x = json.loads(r.get(key))
        analyses_red = []
        for y in x:
            analyses_red.append(y)
        for analysis in analyses_red:  # from several analyzers
            analysis = json.loads(analysis)
            if analysis is None:
                continue
            if 'predicates' in analysis:
                for p in analysis['predicates']:
                    predicates[p] += int(analysis['predicates'][p])
            if 'classes' in analysis:
                for c in analysis['classes']:
                    classes[c] += int(analysis['classes'][c])
            analyses.append({'iri': iri, 'analysis': analysis})

    analyses.append({'predicates': dict(OrderedDict(sorted(predicates.items(), key=lambda kv: kv[1], reverse=True)))})
    analyses.append({'classes': dict(OrderedDict(sorted(classes.items(), key=lambda kv: kv[1], reverse=True)))})

    return analyses


def fetch_missing(iris, r):
    """Trigger index distribution query where needed."""
    missing_query = []
    for iri in iris:
        if missing(iri, r):
            current_app.logger.debug(f'Missing index query result for {iri!s}')
            missing_query.append(iri)

    current_app.logger.info('Fetching missing query results')
    t = group(index_distribution_query.si(iri) for iri in missing_query).apply_async()
    t.get()


def gather_queries(iris, r):
    """Compile queries for all iris."""
    current_app.logger.info('Appending results')
    for iri in iris:
        key = f'distrquery:{iri!s}'
        if missing(iri, r):
            current_app.logger.warn(f'Missing index query result for {iri!s}')
        else:
            related = r.get(key)

            if related is not None:
                rel_json = json.loads(related)
            else:
                rel_json = {}

            yield {
                'iri': iri,
                'related': rel_json
            }


@blueprint.route('/api/v1/query/analysis', methods=['POST'])
@environment('REDIS')
def batch_analysis(redis_url):
    """
    Get a big report for all required distributions.

    Get a list of distributions in request body as JSON, compile analyses,
    query the index return the compiled report.
    """
    lst = request.get_json()
    if lst is None:
        lst = _get_known_distributions()

    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    analyses = gather_analyses(lst, r)
    if 'small' in request.args:
        current_app.logger.info('Small')
        analyses = analyses[-2:]
    fetch_missing(lst, r)
    analyses.extend(x for x in gather_queries(lst, r))
    analyses.append({
        'format': list(r.hgetall('stat:format')),
        'size': retrieve_size_stats(r)
    })
    if 'pretty' in request.args:
        return json.dumps(analyses, indent=4, sort_keys=True)
    return jsonify(analyses)
