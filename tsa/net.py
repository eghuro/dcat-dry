import logging
from io import BytesIO

import rdflib
import redis

from tsa.compression import SizeException
from tsa.monitor import monitor
from tsa.redis import MAX_CONTENT_LENGTH, KeyRoot
from tsa.redis import data as data_key
from tsa.redis import delay as delay_key
from tsa.redis import expiration
from tsa.robots import allowed as robots_allowed
from tsa.robots import session, user_agent


class Skip(Exception):
    """Exception indicating we have to skip this distribution."""


class RobotsRetry(Exception):
    """Exception indicating retry is neeeded because of crawl delay."""

    def __init__(self, delay):
        """Note the delay."""
        self.delay = delay


def fetch(iri, log, red):
    """Fetch the distribution. Mind robots.txt."""
    is_allowed, delay, robots_url = robots_allowed(iri)
    key = delay_key(robots_url)
    if not is_allowed:
        log.warn(f'Not allowed to fetch {iri!s} as {user_agent!s}')
        raise Skip()
    else:
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
    r = session.get(iri, stream=True, timeout=timeout, verify=False, headers={'Accept': accept})
    r.raise_for_status()

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
    return r


def store_content(iri, r, red):
    """Store contents into redis."""
    key = data_key(iri)
    if not red.exists(key):
        chsize = 1024
        conlen = 0
        with red.pipeline() as pipe:
            for chunk in r.iter_content(chunk_size=chsize):
                if chunk:
                    if len(chunk) + conlen > MAX_CONTENT_LENGTH:
                        pipe.delete(key)
                        pipe.execute()
                        raise SizeException(iri)
                    pipe.append(key, chunk)
                    conlen = conlen + len(chunk)
            pipe.expire(key, expiration[KeyRoot.DATA])
            #pipe.sadd('purgeable', key)
            pipe.execute()
        monitor.log_size(conlen)

def get_content(iri, r, red):
    """Load content in memory."""
    key = data_key(iri)

    chsize = 1024
    conlen = 0
    data = BytesIO()
    for chunk in r.iter_content(chunk_size=chsize):
        if chunk:
            data.write(chunk)
            conlen = conlen + len(chunk)
    with red.pipeline() as pipe:
        pipe.set(key, "MEMORY")
        pipe.expire(key, expiration[KeyRoot.DATA])
        #pipe.sadd('purgeable', key)
        pipe.execute()
    monitor.log_size(conlen)
    try:
        return data.getvalue().decode('utf-8')
    except UnicodeDecodeError as e:
        logging.getLogger(__name__).warning(f'Failed to load content for {iri}: {e!s}')
    return None

def guess_format(iri, r, log, red):
    """
    Guess format of the distribution.

    Skip if not known 5* distribution format.
    """
    guess = rdflib.util.guess_format(iri)
    if guess is None:
        guess = r.headers.get('content-type').split(';')[0]
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
    regular = set(['text/xml',  'application/json', 'text/plain',
                   'application/gzip', 'application/x-zip-compressed',
                   'application/zip', 'application/x-gzip',
                   'html', 'text/html'
                   ])
    if guess not in priority.union(regular):
        log.info(f'Skipping this distribution: {iri}')
        raise Skip()

    return guess, (guess in priority)


def test_content_length(iri, r, log):
    """Test content length header if the distribution is not too large."""
    if 'Content-Length' in r.headers.keys():
        conlen = int(r.headers.get('Content-Length'))
        if conlen > MAX_CONTENT_LENGTH:
            # Due to redis limitation
            log.warn(f'Skipping {iri} as it is too large: {conlen!s}')
            raise Skip()
