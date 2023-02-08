ARG PYTHON_VERSION=3.8.7
ARG POETRY_VERSION
FROM --platform=linux/amd64 python:${PYTHON_VERSION}-slim
ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=${POETRY_VERSION:-1.3.2} \
  FLASK_DEBUG=0
RUN apt-get update; apt-get install -y libxml2-dev libxslt-dev; python -m pip install poetry==$POETRY_VERSION
WORKDIR /tmp
COPY pyproject.toml poetry.lock /tmp/
RUN poetry config virtualenvs.create false; poetry install --with logzio,robots,gevent --no-interaction --no-ansi --no-root
COPY . /tmp
CMD gunicorn -w 4 -b 0.0.0.0:8000 app:app
EXPOSE 8000