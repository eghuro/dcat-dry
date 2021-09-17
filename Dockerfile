ARG PYTHON_VERSION=3.8.7
FROM python:${PYTHON_VERSION}-slim
WORKDIR /tmp
RUN apt-get install libstdc++ libarchive-dev binutils libxml2-dev libxslt-dev #do not remove, as it's needed on runtime
COPY requirements.txt .
RUN apt-get install g++ gcc libffi-dev openssl-dev make && pip install -r requirements.txt && apt-geet purge g++ gcc libffi-dev openssl-dev make

COPY . .

CMD gunicorn -k gevent -w 4 -b 0.0.0.0:8000 app:app
EXPOSE 8000
