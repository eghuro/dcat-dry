import logging
import os

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
        self.redis.set("shouldQuery", 1)
        return super().__call__(*args, **kwargs)


@celery.task(ignore_result=True)
def monitor(*args):  # pylint: disable=unused-argument
    log = logging.getLogger(__name__)
    redis_cfg = os.environ.get("REDIS_CELERY", None)
    pool = redis_lib.ConnectionPool().from_url(redis_cfg, charset="utf-8")
    red = redis_lib.Redis(connection_pool=pool)
    enqueued = (
        red.llen("default") + red.llen("high_priority") + red.llen("low_priority")
    )
    if enqueued > 0:
        log.info("Enqueued: %s", str(enqueued))
        red.set("shouldQuery", 1)
    else:
        should_query = red.get("shouldQuery")
        if should_query is None:
            return
        if int(should_query) == 1:
            log.info("Should query")
            red.set("shouldQuery", 2)

            log.warning("Enqueued 0, we are done and we should query")
        else:
            return

        query()
