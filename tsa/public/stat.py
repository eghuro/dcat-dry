"""Stat endpoints."""
import redis
from flask import Blueprint, jsonify

from tsa.extensions import redis_pool
from tsa.tasks.query import retrieve_size_stats

blueprint = Blueprint('stat', __name__, static_folder='../static')


@blueprint.route('/api/v1/stat/format', methods=['GET'])
def stat_format():
    """List distribution formats logged."""
    red = redis.Redis(connection_pool=redis_pool)
    return jsonify(red.hgetall('stat:format'))


@blueprint.route('/api/v1/stat/failed', methods=['GET'])
def stat_failed():
    """List failed distributions."""
    red = redis.Redis(connection_pool=redis_pool)
    return jsonify(list(red.smembers('stat:failed')))


@blueprint.route('/api/v1/stat/size', methods=['GET'])
def stat_size():
    """List min, max and average distribution size."""
    red = redis.Redis(connection_pool=redis_pool)
    return jsonify(retrieve_size_stats(red))
