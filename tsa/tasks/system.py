"""Celery tasks invoked from the API endpoints."""
import logging

import redis

from tsa.celery import celery
from tsa.extensions import redis_pool


@celery.task
def system_check():
    """Runs an availability test of additional systems.

    Tested are: redis.
    """
    log = logging.getLogger(__name__)
    log.info('System check started')

    log.info(f'Testing redis')
    red = redis.Redis(connection_pool=redis_pool)
    red.ping()
    log.info('System check successful')


@celery.task
def hello():
    """Dummy task returning hello world used for testing of Celery."""
    return 'Hello world!'


@celery.task
def cleanup(prefix, extra):
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)
    cache_items = [key for key in red.keys() if key.startswith(prefix)]
    log.debug('Flask cache items: ' + str(cache_items))
    extra.extend(cache_items)
    extra.extend(red.smembers('purgeable'))
    red.delete(*extra)
