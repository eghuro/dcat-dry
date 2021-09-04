# -*- coding: utf-8 -*-
"""Extensions module. Each extension is initialized in the app factory located in app.py."""
import logging

import redis
import statsd
from atenvironment import environment
from flask_caching import Cache
from flask_cors import CORS
from pymongo import MongoClient

from tsa.ddr import ConceptIndex
from tsa.ddr import DataCubeDefinitionIndex as DSD
from tsa.ddr import DataDrivenRelationshipIndex as DDR
from tsa.redis import same_as as sameas_key
from tsa.redis import skos_relation as skos_key
from tsa.sameas import Index

cache = Cache()
cors = CORS()

def on_error(x):
    pass

@environment('REDIS', default=['redis://localhost:6379/0'], onerror=on_error)
def get_redis(redis_cfg):
    """Create a redis connectiion pool."""
    log = logging.getLogger(__name__)
    log.info(f'redis cfg: {redis_cfg}')
    return redis.ConnectionPool().from_url(redis_cfg, charset='utf-8', decode_responses=True)


@environment('MONGO', 'MONGO_DB', default=[None, 'dcat_dry'], onerror=on_error)
def get_mongo(mongo_cfg, mongo_db):
    log = logging.getLogger(__name__)
    if mongo_cfg is None:
        log.warning('Mongo cfg not provided, using default')
        client = MongoClient()
    else:
        log.info('Setting up mongo')
        client = MongoClient(mongo_cfg)
    db = client[mongo_db]
    return client, db

@environment("STATSD_HOST", "STATSD_PORT", default=[None, 8125], onerror=on_error)
def get_statsd(host, port):
    try:
         return statsd.StatsClient(host=host, port=port)
    except:
        return None

redis_pool = get_redis()
sameAsIndex = Index(redis_pool, sameas_key, True)
skosIndex = Index(redis_pool, skos_key, False)
ddrIndex = DDR(redis_pool)
conceptIndex = ConceptIndex(redis_pool)
dsdIndex = DSD(redis_pool)
mongo_client, mongo_db = get_mongo()
statsd_client = get_statsd()
