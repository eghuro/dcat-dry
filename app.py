# -*- coding: utf-8 -*-
"""Create an application instance."""
import logging

from tsa.app import create_app
from tsa.settings import ProdConfig

# Flask app factory.
app = create_app(ProdConfig)


@app.before_first_request
def setup_logging():  # noqa: unused-function
    """Set up logging to STDOUT from INFO level up in production environment."""
    if not app.debug:
        # In production mode, add log handler to sys.stderr.
        app.logger.addHandler(logging.StreamHandler())
        app.logger.setLevel(logging.INFO)
