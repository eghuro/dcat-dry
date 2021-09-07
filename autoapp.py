# -*- coding: utf-8 -*-
"""Create an application instance."""
import logging

from tsa.app import create_app
from tsa.settings import ProdConfig

# from flask.helpers import get_debug_flag

# CONFIG = DevConfig if get_debug_flag() else ProdConfig

app = create_app(ProdConfig)


@app.before_first_request
def setup_logging():
    """Set up logging to STDOUT from INFO level up in production environment."""
    if not app.debug:
        # In production mode, add log handler to sys.stderr.
        app.logger.addHandler(logging.StreamHandler())
        app.logger.setLevel(logging.INFO)
