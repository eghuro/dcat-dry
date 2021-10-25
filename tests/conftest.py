# -*- coding: utf-8 -*-
"""Defines fixtures available to all tests."""

import pytest

from tsa.app import create_app
from tsa.settings import ProdConfig


@pytest.fixture
def app():
    """An application for the tests."""
    _app = create_app(ProdConfig)
    ctx = _app.test_request_context()
    ctx.push()

    yield _app

    ctx.pop()
