import logging.config

from atenvironment import environment

from tsa.extensions import on_error


@environment('LOGZIO_TOKEN', 'LOGZIO_URL',
             default=[None, 'https://listener-eu.logz.io:8071'],
             onerror=on_error)
def logging_setup(token, url):
    if token is None:
        logging.info('Remote logging into logz.io is not configured')
        return
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'logzioFormat': {
                'validate': False,
                'format': '{"program": "DCAT-DRY"}',
            },
        },
        'handlers': {
            'logzio': {
                'class': 'logzio.handler.LogzioHandler',
                'formatter': 'logzioFormat',
                'token': token,
                'logs_drain_timeout': 5,
                'url': url
            },
        },
        'loggers': {
            'tsa': {
                'level': 'WARNING',
                'handlers': ['logzio'],
            },
            'app.logger': {
                'level': 'WARNING',
                'handlers': ['logzio'],
            },
            'gunicorn.error': {
                'level': 'WARNING',
                'handlers': ['logzio'],
            },
            'gunicorn.access': {
                'level': 'WARNING',
                'handlers': ['logzio'],
            }
        }
    })
