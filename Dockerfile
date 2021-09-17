ARG PYTHON_VERSION=3.8.7
FROM python:${PYTHON_VERSION}-slim
WORKDIR /tmp
RUN apt-get update; apt-get install -y libarchive-dev binutils libxml2-dev libxslt-dev #do not remove, as it's needed on runtime
COPY requirements.txt .
RUN apt-get install -y g++ gcc libffi-dev libssl-dev make && pip install -r requirements.txt && apt-get purge g++ gcc libffi-dev openssl-dev make

COPY . .

CMD gunicorn -k gevent -w 4 -b 0.0.0.0:8000 app:app
EXPOSE 8000
