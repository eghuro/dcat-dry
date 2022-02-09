# -*- coding: utf-8 -*-
"""Test configs."""
from tsa.app import create_app
from tsa.settings import ProdConfig


def test_production_config():
    """Production config."""
    app = create_app(ProdConfig)
    assert app.config["ENV"] == "prod"
    assert app.config["DEBUG"] is False
    assert app.config["DEBUG_TB_ENABLED"] is False
