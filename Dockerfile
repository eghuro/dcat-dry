ARG POETRY_VERSION
FROM --platform=linux/amd64 debian:bullseye as base
ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=${POETRY_VERSION:-1.3.2} \
  FLASK_DEBUG=0
RUN apt-get update && \
    apt-get install -y libxml2-dev libxslt-dev libleveldb-dev gcc g++ wget build-essential checkinstall  libreadline-dev  libncursesw5-dev  libssl-dev  libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev && \
    cd /usr/src && \
    wget https://www.python.org/ftp/python/3.10.9/Python-3.10.9.tgz && \
    tar xzf Python-3.10.9.tgz && \
    cd Python-3.10.9 && \
    ./configure --enable-optimizations --prefix=/usr && \
    make install
RUN /usr/bin/python3 -m pip install poetry==$POETRY_VERSION

WORKDIR /tmp
COPY pyproject.toml poetry.lock /tmp/
RUN  poetry config virtualenvs.create false && poetry install --with logzio,robots,gevent,couchdb --without dev --no-interaction --no-ansi --no-root && apt-get purge -y gcc g++ wget build-essential checkinstall  libreadline-gplv2-dev  libncursesw5-dev  libssl-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev && apt-get autoremove -y
FROM base as runner
COPY . /tmp
CMD gunicorn -w 4 -b 0.0.0.0:8000 app:app
EXPOSE 8000
