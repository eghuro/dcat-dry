# -*- coding: utf-8 -*-
"""Extensions module. Each extension is initialized in the app factory located in app.py."""
import redis
import logging
from atenvironment import environment
from flask_caching import Cache
from flask_cors import CORS
from pymongo import MongoClient

cache = Cache()
cors = CORS()


@environment('REDIS')
def get_redis(redis_cfg):
    """Create a redis connectiion pool."""
    return redis.ConnectionPool().from_url(redis_cfg, charset='utf-8', decode_responses=True)


def on_error(x):
    pass


@environment('MONGO', 'MONGO_DB', default=[None, 'dcat_dry'], onerror=on_error)
def get_mongo(mongo_cfg, mongo_db):
    log = logging.getLogger(__name__)
    log.warning(mongo_cfg)
    log.warning(mongo_db)
    if mongo_cfg is None:
        client = MongoClient()
    else:
        log.warning('Setting up mongo')
        client = MongoClient(mongo_cfg)
    db = client[mongo_db]
    return client, db


redis_pool = get_redis()
mongo_client, mongo_db = get_mongo()
