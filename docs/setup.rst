Step-by-step guide from DCAT-AP catalog to populated index
===========================================================
# Install docker and docker-compose
# Start virtuoso (in docker or elsewhere), fetch latest NKOD dump (or your DCAT-AP catalog) and load it into virtuoso, get list of named graphs in txt file
# Build docker containers: docker-compose up --build - make sure the graphs file and exclude list is correctly mounted to the container
# Setup couchdb: curl -X PUT http://admin:password@127.0.0.1:5984/_users; curl -X PUT http://admin:password@127.0.0.1:5984/_replicator
# Migrate database: docker-compose exec web alembic upgrade head  - why not automates? https://liman.io/blog/migration-dockerized-mysql-sqlalchemy-alembic and https://pythonspeed.com/articles/schema-migrations-server-startup/
# Execute:  docker-compose exec web flask batch -g /tmp/graphs.txt -s http://10.12.48.14:8890/sparql
# When execution is finished: docker-compose exec web flask finalize
