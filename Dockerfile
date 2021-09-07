ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}-alpine
WORKDIR /tmp
RUN apk add libstdc++ libarchive-dev binutils libxml2-dev libxslt-dev #do not remove, as it's needed on runtime
COPY requirements.txt .
RUN apk --no-cache add g++ gcc musl-dev libffi-dev openssl-dev make && pip install -r requirements.txt && apk del g++ gcc musl-dev libffi-dev openssl-dev make

COPY . .

CMD gunicorn -k gevent -w 4 -b 0.0.0.0:8000 autoapp:app
EXPOSE 8000
