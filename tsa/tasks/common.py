import logging
import redis as redis_lib
#from redisrwlock import Rwlock, RwlockClient

from celery import Task

from tsa.extensions import redis_pool
from tsa.query import query

class TrackableException(BaseException):
    pass


class TrackableTask(Task):
    _red = None
    #_lockClient = None
    _batch_id = None

    @property
    def redis(self):
        if self._red is None:
            self._red = redis_lib.Redis(connection_pool=redis_pool)
        return self._red

    #@property
    #def lockClient(self):
    #    if self._lockClient is None:
    #        self._lockClient = RwlockClient()
    #    return self._lockClient


    def __call__(self, *args, **kwargs):
        my_id = self.request.id
        parent_id = self.request.parent_id
        root_id = self.request.root_id
        batch_id = None

        #client = self.lockClient
        red = self.redis
        #rwlock = client.lock('batchLock', Rwlock.WRITE, timeout=5)
        #if rwlock.status == Rwlock.OK:
        if parent_id is not None:
            batch_id = red.hget('taskBatchId', parent_id)
        if batch_id is None and root_id is not None:
            batch_id = red.hget('taskBatchId', root_id)
        if batch_id is None:
            batch_id = red.hget('taskBatchId', my_id)

        if batch_id is None:
            logging.getLogger(__name__).error(f'({self.name}) Missing batch id, my id: {my_id}, parent id: {parent_id}, root id: {root_id}')
            raise TrackableException()

        red.hset('taskBatchId', my_id, batch_id)
        red.sadd(f'batch:{batch_id}', my_id)
        self._batch_id = batch_id

            #client.unlock(rwlock)
        #elif rwlock.status == Rwlock.DEADLOCK:
            #logging.getLogger(__name__).exception('Deadlock, retrying')
            #self.retry()

        return super(TrackableTask, self).__call__(*args, **kwargs)


    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        #client = self.lockClient
        red = self.redis
        #rwlock = client.lock('batchLock', Rwlock.WRITE, timeout=0)
        #if rwlock.status == Rwlock.OK:
        batch_id = red.hget('taskBatchId', task_id)
        red.srem(f'batch:{batch_id}', task_id)
            #keep taskBatchId as we need it for children tasks
            #client.unlock(rwlock)
        #elif rwlock.status == Rwlock.DEADLOCK:
            #logging.getLogger(__name__).exception('Deadlock, retrying')
            #self.retry()

def on_complete(batch_id, red):
    if red.scard(f'batch:{batch_id}') == 0:
        logging.getLogger(__name__).warn(f'Completed batch {batch_id}')
        # here we can cleanup taskBatchId
        for key in red.hkeys('taskBatchId'):
            for b_id in red.hget('taskBatchId', key):
                if b_id == batch_id:
                    red.hdel('taskBatchId', key)
        query(batch_id, red)