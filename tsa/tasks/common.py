import logging

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
        enqueued = self.__red.llen('default') + self.__red.llen('high_priority') + self.__red.llen('low_priority')
        log = logging.getLogger(__name__)
        if enqueued > 0:
            log.info(f'Enqueued: {enqueued}')
        else:
            log.warning(f'Enqueued 0, we are done')
