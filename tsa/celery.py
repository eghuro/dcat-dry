"""Celery setup - raven hook and configuration."""

import celery
from atenvironment import environment

from tsa.extensions import on_error
from tsa.log import logging_setup

from gevent import monkey
monkey.patch_all()
try:
    import psycogreen.gevent
    psycogreen.gevent.patch_psycopg()
except ImportError:
    pass

@environment("DSN", default=[None], onerror=on_error)
def init_sentry(dsn_str=None):
    if dsn_str is not None:
        try:
            import sentry_sdk
            from sentry_sdk.integrations.celery import CeleryIntegration
            sentry_sdk.init(dsn_str, integrations=[CeleryIntegration()])
        except ImportError:
            pass


logging_setup()
init_sentry()
celery = celery.Celery()
celery.config_from_object("tsa.celeryconfig")
