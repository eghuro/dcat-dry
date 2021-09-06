import logging
import os

import redis as redis_lib
from celery import Task

from tsa.extensions import redis_pool


class TrackableTask(Task):
    _red = None

    @property
    def redis(self):
        if self._red is None:
            self._red = redis_lib.Redis(connection_pool=redis_pool)
        return self._red


    def __call__(self, *args, **kwargs):
        res = super(TrackableTask, self).__call__(*args, **kwargs)
        self.__check_queue()
        return res

    def __check_queue(self):
        # need to query redis used for celery - that's where the queues are!
        redis_cfg = os.environ.get('REDIS_CELERY', None)
        pool = redis_lib.ConnectionPool().from_url(redis_cfg, charset='utf-8', decode_responses=True)
        red = redis_lib.Redis(connection_pool=pool)
        enqueued = red.llen('default') + red.llen('high_priority') + red.llen('low_priority')
        log = logging.getLogger(__name__)
        if enqueued > 0:
            log.info(f'Enqueued: {enqueued}')
        else:
            log.warning(f'Enqueued 0, we are done')
