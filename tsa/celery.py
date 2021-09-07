"""Celery setup - raven hook and configuration."""

import celery
import sentry_sdk
from atenvironment import environment
from sentry_sdk.integrations.celery import CeleryIntegration

from tsa.extensions import on_error
from tsa.log import logging_setup


@environment('DSN', default=[None], onerror=on_error)
def init_sentry(dsn_str):
    if dsn_str is not None:
        sentry_sdk.init(dsn_str, integrations=[CeleryIntegration()])

logging_setup()
init_sentry()
celery = celery.Celery()
celery.config_from_object('tsa.celeryconfig')
