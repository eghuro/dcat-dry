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
        return super(TrackableTask, self).__call__(*args, **kwargs)
