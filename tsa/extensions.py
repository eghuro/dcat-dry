# -*- coding: utf-8 -*-
"""Extensions module. Each extension is initialized in the app factory located in app.py."""
import logging

import redis
from atenvironment import environment
from flask_caching import Cache
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from pymongo import MongoClient



try:
    from statsd import StatsClient
except ImportError:
    from tsa.mocks import StatsClient  # type: ignore

cache = Cache()
cors = CORS()
db = SQLAlchemy()

def on_error(missing_variable):
    logging.getLogger(__name__).debug(
        "Using default value for environment missing_variable: %s", missing_variable
    )


@environment("REDIS", default=["redis://localhost:6379/0"], onerror=on_error)
def get_redis(redis_cfg=None):
    """Create a redis connectiion pool."""
    log = logging.getLogger(__name__)
    log.info("redis cfg: %s", redis_cfg)
    return redis.ConnectionPool().from_url(
        redis_cfg, decode_responses=True
    )


@environment("MONGO", "MONGO_DB", default=[None, "dcat_dry"], onerror=on_error)
def get_mongo(mongo_cfg=None, mongo_db_name=None):
    log = logging.getLogger(__name__)
    if mongo_cfg is None:
        log.warning("Mongo cfg not provided, using default")
        client = MongoClient()
    else:
        log.info("Setting up mongo")
        client = MongoClient(mongo_cfg)
    db = client[mongo_db_name]
    return client, db


@environment("STATSD_HOST", "STATSD_PORT", default=[None, 8125], onerror=on_error)
def get_statsd(host=None, port=None):
    return StatsClient(host=host, port=port)


redis_pool = get_redis()
_, mongo_db = get_mongo()
statsd_client = get_statsd()
