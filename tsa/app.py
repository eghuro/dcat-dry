# -*- coding: utf-8 -*-
"""The app module, containing the app factory function."""

import sentry_sdk
from atenvironment import environment
from flask import Flask, render_template
from sentry_sdk.integrations.flask import FlaskIntegration

from tsa import commands, public
from tsa.extensions import cache, cors, csrf, on_error
from tsa.log import logging_setup


@environment('DSN', default=[None], onerror=on_error)  # noqa: unused-function
def create_app(config_object, dsn_str=None):
    """
    An application factory, as explained here: http://flask.pocoo.org/docs/patterns/appfactories/.

    :param config_object: The configuration object to use.
    """
    logging_setup()
    if dsn_str:
        sentry_sdk.init(
            dsn=dsn_str,
            integrations=[FlaskIntegration()]
        )

    app = Flask(__name__.split('.', maxsplit=1)[0])
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


def register_blueprints(app):
    """Register Flask blueprints."""
    app.register_blueprint(public.views.blueprint)
    app.register_blueprint(public.test.blueprint)


def register_errorhandlers(app):
    """Register error handlers."""
    def render_error(error):
        """Render error template."""
        # If a HTTPException, pull the `code` attribute; default to 500
        error_code = getattr(error, 'code', 500)
        if error_code == 400:
            error_code = 401
        return render_template(f'{error_code!s}.html'), error_code
    for errcode in []:
        app.errorhandler(errcode)(render_error)


def register_shellcontext(app):
    """Register shell context objects."""
    def shell_context():
        """Shell context objects."""
        return {}

    app.shell_context_processor(shell_context)


def register_commands(app):
    """Register Click commands."""
    app.cli.add_command(commands.clean)
    app.cli.add_command(commands.urls)
    app.cli.add_command(commands.batch)
    app.cli.add_command(commands.import_labels)
    app.cli.add_command(commands.import_labels)
    app.cli.add_command(commands.import_sameas)
    app.cli.add_command(commands.import_related)
    app.cli.add_command(commands.import_profiles)
    app.cli.add_command(commands.import_interesting)
    app.cli.add_command(commands.dereference)
