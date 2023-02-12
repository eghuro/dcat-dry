Features
========

Linked data
-----------
* DCAT-AP 2.0.1 support, reading datasets from SPARQL endpoint
* JSON-LD support with recursive context loading
* IRI dereferences with support for SPARQL endpoint lookup
* DataCube vocabulary, SKOS with data driven relationships detection
* SameAs index
* OFN index
* RUIAN references
* Use of concepts defined elsewhere (codelist use)
* Use of significant resource on dimension of a datacube (Concept)
* Cross dataset linkage using sameAs
* Dataset profiling
* Label index, subject & object index

Indexer and architecture
------------------------
* Flask app using app factories and blueprints
* Celery backend with redis backend and gevent pool, priority queues
* Caching: requests from backend, requests to Flask API, LRU cache backed with redis
* Connection pooling & user agent for all requests inc. SPARQL, couchdb, pyld, robots, ...
* IRI prefix filters, robots.txt compliance, streaming requests
* postgresql database with sqlalchemy and alembic migrations
* Command line interface to start the batch processing
* (Optional) docker-compose setup
* Optional sentry integration, logz.io integration
* Optional statsd reporting
* Optional populating CouchDB for LP-DAV (has performance impact)
* Optional support for compressed distributions
