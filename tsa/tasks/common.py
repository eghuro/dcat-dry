import logging
import os
import uuid

import redis as redis_lib
from celery import Task

from tsa.celery import celery
from tsa.extensions import redis_pool
from tsa.query import query


class TrackableTask(Task):
    _red = None

    @property
    def redis(self):
        if self._red is None:
            self._red = redis_lib.Redis(connection_pool=redis_pool)
        return self._red

    def __call__(self, *args, **kwargs):
        self.redis.set('shouldQuery', 1)
        return super(TrackableTask, self).__call__(*args, **kwargs)


@celery.task
def monitor(*args):
    log = logging.getLogger(__name__)
    redis_cfg = os.environ.get('REDIS_CELERY', None)
    pool = redis_lib.ConnectionPool().from_url(redis_cfg, charset='utf-8', decode_responses=True)
    red = redis_lib.Redis(connection_pool=pool)
    enqueued = red.llen('default') + red.llen('high_priority') + red.llen('low_priority')
    if enqueued > 0:
        log.info(f'Enqueued: {enqueued}')
        red.set('shouldQuery', 1)
    else:
        if int(red.get('shouldQuery')) == 1:
            log.info('Should query')
            red.set('shouldQuery', 2)

            log.warning('Enqueued 0, we are done and we should query')
        else:
            return

        result_id = str(uuid.uuid4())
        log.info(f'Query result id: {result_id}')
        query(result_id, red)
