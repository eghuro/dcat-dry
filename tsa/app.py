# -*- coding: utf-8 -*-
"""The app module, containing the app factory function."""

import sentry_sdk
from atenvironment import environment
from flask import Flask, render_template
from sentry_sdk.integrations.flask import FlaskIntegration

from tsa import commands, public
from tsa.extensions import cache, cors, csrf, on_error
from tsa.log import logging_setup


@environment('DSN', default=[None], onerror=on_error)
def create_app(config_object, dsn_str):
    """An application factory, as explained here: http://flask.pocoo.org/docs/patterns/appfactories/.

    :param config_object: The configuration object to use.
    """
    logging_setup()
    if dsn_str:
        sentry_sdk.init(
            dsn=dsn_str,
            integrations=[FlaskIntegration()]
        )

    app = Flask(__name__.split('.')[0])
    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)
    register_errorhandlers(app)
    register_shellcontext(app)
    register_commands(app)
    return app


def register_extensions(app):
    """Register Flask extensions."""
    cache.init_app(app)
    cors.init_app(app)
    csrf.init_app(app)
    return None


def register_blueprints(app):
    """Register Flask blueprints."""
    app.register_blueprint(public.views.blueprint)
    app.register_blueprint(public.test.blueprint)
    app.register_blueprint(public.analyze.blueprint)
    app.register_blueprint(public.stat.blueprint)
    return None


def register_errorhandlers(app):
    """Register error handlers."""
    def render_error(error):
        """Render error template."""
        # If a HTTPException, pull the `code` attribute; default to 500
        error_code = getattr(error, 'code', 500)
        if error_code == 400:
            error_code = 401
        return render_template('{0}.html'.format(error_code)), error_code
    for errcode in []:
        app.errorhandler(errcode)(render_error)
    return None


def register_shellcontext(app):
    """Register shell context objects."""
    def shell_context():
        """Shell context objects."""
        return {}

    app.shell_context_processor(shell_context)


def register_commands(app):
    """Register Click commands."""
    app.cli.add_command(commands.test)
    app.cli.add_command(commands.lint)
    app.cli.add_command(commands.clean)
    app.cli.add_command(commands.urls)
