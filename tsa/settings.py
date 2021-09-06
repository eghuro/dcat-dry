# -*- coding: utf-8 -*-
"""Application configuration."""

import os
import uuid


class Config(object):
    """Base configuration."""

    SECRET_KEY = os.environ.get('DCAT_DRY_SECRET', str(uuid.uuid4()))
    APP_DIR = os.path.abspath(os.path.dirname(__file__))  # This directory
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_DIR, os.pardir))
    DEBUG_TB_ENABLED = True  # Disable Debug toolbar
    DEBUG_TB_INTERCEPT_REDIRECTS = True
    CACHE_TYPE = 'redis'  # Can be "memcached", "redis", etc.
    CACHE_KEY_PREFIX = 'fcache'
    CACHE_REDIS_URL = os.environ.get('REDIS', None)
    LOOKUP_ENDPOINTS = [os.environ.get('ENDPOINT', None), 'https://linked.cuzk.cz.opendata.cz/sparql']
    ANALYZE_IMMEDIATELY = True
    SD_BASE_IRI = 'https://data.eghuro.cz/resource/'


class ProdConfig(Config):
    """Production configuration."""

    ENV = 'prod'
    DEBUG = False
    DEBUG_TB_ENABLED = False  # Disable Debug toolbar


class DevConfig(Config):
    """Development configuration."""

    ENV = 'dev'
    DEBUG = True
    DEBUG_TB_ENABLED = True
    CACHE_TYPE = 'simple'  # Can be "memcached", "redis", etc.


class TestConfig(Config):
    """Test configuration."""

    ENV = 'test'
    TESTING = True
    DEBUG = True
