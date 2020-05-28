ARG PYTHON_VERSION=3.6
FROM python:${PYTHON_VERSION}-alpine
WORKDIR /tmp
COPY requirements.txt .
RUN apk add libstdc++ libarchive-dev binutils #do not remove, as it's needed on runtime
RUN apk --no-cache add g++ gcc musl-dev libffi-dev openssl-dev && pip install -r requirements.txt && apk del g++ gcc musl-dev libffi-dev openssl-dev

COPY . .

CMD gunicorn -k gevent -w 4 -b 0.0.0.0:8000 autoapp:app
EXPOSE 8000
