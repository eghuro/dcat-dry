"""Celery configuration."""
import os

broker_url = os.environ.get("REDIS_CELERY", None)
broker_pool_limit = 0
redis_max_connections = 20
result_backend = os.environ.get("REDIS_CELERY", None)
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
timezone = "Europe/Prague"
enable_utc = False
task_acks_late = True
worker_prefetch_multiplier = 1
worker_cancel_long_running_tasks_on_connection_loss = True
task_time_limit = 10800  # smaller limits applied on some tasks
include = [
    "tsa.tasks.batch",
    "tsa.tasks.process",
    "tsa.tasks.query",
    "tsa.tasks.system",
]
broker_transport_options = {
    "priority_steps": list(range(3)),
    "sep": ":",
    "queue_order_strategy": "priority",
    "fanout_prefix": True,
    "fanout_patterns": True,
    "max_connections": 20,
}
task_create_missing_queues = True
task_default_queue = "celery:1"
task_task_routes = {
    "tsa.tasks.process.process_priority": {"queue": "celery"},
    "tsa.tasks.query.*": {"queue": "celery:2"},
    "tsa.tasks.batch.*": {"queue": "celery:2"},
    "tsa.tasks.common.monitor": {"queue": "celery:1"},
}
beat_schedule = {
    "check-queue-every-ten-minutes": {
        "task": "tsa.tasks.common.monitor",
        "schedule": 600.0,
    },
}
