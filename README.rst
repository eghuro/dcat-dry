===============================
DCAT DRY
===============================

.. |github| image:: https://img.shields.io/github/release-pre/eghuro/dcat-dry.svg
.. |licence| image:: https://img.shields.io/github/license/eghuro/dcat-dry.svg

|github|  |licence|


DCAT-AP Dataset Relationship Indexer. Indexing linked data and relationships between datasets.

Features:
 - index a distribution or a SPARQL endpoint
 - extract and index distributions from a DCAT catalog
 - extract a DCAT catalog from SPARQL endpoint and index distributions from it
 - generate a dataset profile
 - show related datasets based mainly on DataCube and SKOS vocabularies
 - indexing sameAs identities and related concepts


Build & run with Docker
----------

For DCAT-DRY service only:

.. code-block:: bash

    docker build . -t nkod-ts
    docker run -p 80:8000 --name nkod-ts nkod-ts

For the full environment use docker-compose:

.. code-block:: bash

    docker-compose up --build

Build & run manually
----------
CPython 3.8+ is supported.

Install redis server first. In following example we will assume it runs on localhost, port 6379 and DB 0 is used.

Run the following commands to bootstrap your environment ::

    git clone https://github.com/eghuro/dcat-dry
    cd dcat-dry
    pip install -r requirements.txt
    # Start redis server
    # Run concurrently
    REDIS_CELERY=redis://localhost:6379/1 REDIS=redis://localhost:6379/0 celery worker -l warning -A tsa.celery -Q high_priority -c 10 -n high
    REDIS_CELERY=redis://localhost:6379/1 REDIS=redis://localhost:6379/0 nice -n 10 celery worker -l info -A tsa.celery -Q default,query -c 20 -n default
    REDIS_CELERY=redis://localhost:6379/1 REDIS=redis://localhost:6379/0 nice -n 20 celery worker -l warning -A tsa.celery -Q low_priority -c 5 -n low
    REDIS_CELERY=redis://localhost:6379/1 REDIS=redis://localhost:6379/0 gunicorn -k gevent -w 4 -b 0.0.0.0:8000 autoapp:app
    REDIS_CELERY=redis://localhost:6379/1 REDIS=redis://localhost:6379/0 nice -n 10 celery -l info -A tsa.celery beat


In general, before running shell commands, set the ``FLASK_APP`` and
``FLASK_DEBUG`` environment variables ::

    export FLASK_APP=autoapp.py
    export FLASK_DEBUG=1


Deployment
----------

To deploy::

    export FLASK_DEBUG=0
    # Follow commands above to bootstrap the environment
In your production environment, make sure the ``FLASK_DEBUG`` environment
variable is unset or is set to ``0``, so that ``ProdConfig`` is used.


Shell
-----

To open the interactive shell, run ::

    flask shell

By default, you will have access to the flask ``app``.


Running Tests
-------------

To run all tests, run ::

    flask test


API
-------------

To start batch scan, run  ::

    flask batch -g /tmp/graphs.txt -s http://10.114.0.2:8890/sparql

Get a full result ::

    /api/v1/query/analysis

Query a dataset ::

    /api/v1/query/dataset?iri=http://abc

