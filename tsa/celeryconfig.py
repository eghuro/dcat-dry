"""Celery configuration."""
import os

broker_url = os.environ['REDIS_CELERY']
broker_pool_limit = 100
result_backend = os.environ['REDIS_CELERY']
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Europe/Prague'
enable_utc = False
task_time_limit = 6000  #smaller limits applied on some tasks
include = ['tsa.tasks.batch', 'tsa.tasks.process', 'tsa.tasks.query', 'tsa.tasks.system']
broker_transport_options = {
    'fanout_prefix': True,
    'fanout_patterns': True
}
task_create_missing_queues = True
task_default_queue = 'default'
task_routes = {
    'tsa.tasks.process.process_priority': {
        'queue': 'high_priority'
    },

    'tsa.tasks.query.*': {
        'queue': 'query'
    },

    'tsa.tasks.batch.*': {
        'queue': 'low_priority'
    },
}
