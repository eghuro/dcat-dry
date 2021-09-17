ARG PYTHON_VERSION=3.8.7
FROM python:${PYTHON_VERSION}-alpine
WORKDIR /tmp
RUN apk add libstdc++ libarchive-dev binutils libxml2-dev libxslt-dev #do not remove, as it's needed on runtime
RUN apk --no-cache add gcc musl-dev; pip install lxml
COPY requirements.txt .
RUN apk --no-cache add g++ libffi-dev openssl-dev make && pip install -r requirements.txt && apk del g++ gcc musl-dev libffi-dev openssl-dev make

COPY . .

CMD gunicorn -k gevent -w 4 -b 0.0.0.0:8000 app:app
EXPOSE 8000
