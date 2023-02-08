ARG PYTHON_VERSION=3.8.7
FROM --platform=linux/amd64 python:${PYTHON_VERSION}-slim
WORKDIR /tmp
RUN apt-get update; apt-get install -y libxml2-dev libxslt-dev #do not remove, as it's needed on runtime
COPY pyproject.toml poetry.lock .
RUN python -m pip install --upgrade pip poetry && poetry install --with logzio,robots

COPY . .
ENV FLASK_DEBUG=0
CMD gunicorn -w 4 -b 0.0.0.0:8000 app:app
EXPOSE 8000