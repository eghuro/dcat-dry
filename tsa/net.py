import logging
from io import BytesIO
from typing import Tuple

import rdflib
import redis
import requests

from tsa.monitor import monitor
from tsa.redis import MAX_CONTENT_LENGTH, KeyRoot
from tsa.redis import data as data_key
from tsa.redis import delay as delay_key
from tsa.redis import expiration
from tsa.robots import USER_AGENT
from tsa.robots import allowed as robots_allowed
from tsa.robots import session


class Skip(Exception):
    """Exception indicating we have to skip this distribution."""


class SizeException(Exception):
    """Indicating a subfile is too large."""

    def __init__(self, name):
        """Record the file name."""
        self.name = name
        super().__init__()


class RobotsRetry(Exception):

    """Exception indicating retry is neeeded because of crawl delay."""

    def __init__(self, delay):
        """Note the delay."""
        self.delay = delay
        super().__init__()


def fetch(iri, log, red):
    """Fetch the distribution. Mind robots.txt."""
    is_allowed, delay, robots_url = robots_allowed(iri)
    key = delay_key(robots_url)
    if not is_allowed:
        log.warn(f'Not allowed to fetch {iri!s} as {USER_AGENT!s}')
        raise Skip()
    wait = red.ttl(key)
    if wait > 0:
        log.info(f'Analyze {iri} in {wait} because of crawl-delay')
        raise RobotsRetry(wait)

    timeout = 5243  # ~87 min
    # a guess for 100 KB/s on data that will still make it into redis (512 MB)
    # this is mostly a safe stop in case a known RDF (tasks not time constrained) hangs along the way
    # the idea is to allow for as much time as needed for the known RDF distros, while preventing task queue "jam"
    log.debug(f'Setting timeout {timeout!s} for {iri}')
    accept = 'application/ld+json, application/trig, application/rdf+xml, text/turtle, text/n3;charset=utf-8, application/n-triples, application/n-quads, application/xml;q=0.9, text/xml;q=0.9, text/plain;q=0.9, */*;q=0.8'
    request = session.get(iri, stream=True, timeout=timeout, verify=False, headers={'Accept': accept})
    request.raise_for_status()

    if delay is not None:
        log.info(f'Recording crawl-delay of {delay} for {iri}')
        try:
            delay = int(delay)
        except ValueError:
            log.error('Invalid delay value - could not convert to int')
        else:
            try:
                red.set(key, 1)
                red.expire(key, delay)
            except redis.exceptions.ResponseError:
                log.error(f'Failed to set crawl-delay for {iri}: {delay}')
    return request


class NoContent(ValueError):
    pass


def get_content(iri: str, request: requests.Request, red: redis.Redis) -> str:
    """Load content in memory."""
    key = data_key(iri)

    chsize = 1024
    conlen = 0
    data = BytesIO()
    for chunk in request.iter_content(chunk_size=chsize):
        if chunk:
            data.write(chunk)
            conlen = conlen + len(chunk)
    with red.pipeline() as pipe:
        pipe.set(key, 'MEMORY')
        pipe.expire(key, expiration[KeyRoot.DATA])
        pipe.execute()
    monitor.log_size(conlen)
    try:
        return data.getvalue().decode('utf-8')
    except UnicodeDecodeError as exc:
        logging.getLogger(__name__).warning('Failed to load content for %s: %s', iri, exc)
    raise NoContent()


def guess_format(iri: str, request: requests.Request, log: logging.Logger) -> Tuple[str, bool]:
    """
    Guess format of the distribution.

    Skip if not known 5* distribution format.
    """
    guess = rdflib.util.guess_format(iri)
    if guess is None:
        guess = request.headers.get('content-type').split(';')[0]
    monitor.log_format(str(guess))
    if 'xml' in guess:
        guess = 'xml'
    log.debug(f'Guessing format to be {guess!s}')

    priority = set(['hturtle', 'n3', 'nquads', 'nt',
                    'trix', 'trig', 'turtle', 'xml', 'json-ld',
                    'application/x-7z-compressed',
                    'application/rdf+xml',
                    'application/ld+json', 'application/rss+xml',
                    'text/turtle'])
    regular = set(['text/xml', 'application/json', 'text/plain',
                   'application/gzip', 'application/x-zip-compressed',
                   'application/zip', 'application/x-gzip',
                   'html', 'text/html'
                   ])
    if guess not in priority.union(regular):
        log.info(f'Skipping this distribution: {iri}')
        raise Skip()

    return guess, (guess in priority)


def test_content_length(iri, request, log):
    """Test content length header if the distribution is not too large."""
    if 'Content-Length' in request.headers.keys():
        conlen = int(request.headers.get('Content-Length'))
        if conlen > MAX_CONTENT_LENGTH:
            # Due to redis limitation
            log.warn(f'Skipping {iri} as it is too large: {conlen!s}')
            raise Skip()
