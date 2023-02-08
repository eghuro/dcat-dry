import logging
import os

import redis as redis_lib
from celery import Task

from tsa.celery import celery
from tsa.db import db_session


class SqlAlchemyTask(Task):
    """An abstract Celery Task that ensures that the connection the the
    database is closed on task completion"""

    # http://www.prschmid.com/2013/04/using-sqlalchemy-with-celery-tasks.html
    abstract = True

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        db_session.remove()


class TrackableTask(SqlAlchemyTask):
    pass


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def monitor(*args):  # pylint: disable=unused-argument
    log = logging.getLogger(__name__)
    redis_cfg = os.environ.get("REDIS_CELERY", None)
    pool = redis_lib.ConnectionPool().from_url(redis_cfg)
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
