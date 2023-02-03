import logging
from io import BytesIO
from random import randint
from typing import Tuple
from datetime import datetime, timedelta

import requests
import urllib3
import rdflib
import redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from tsa.extensions import db
from tsa.model import RobotsDelay
from tsa.monitor import monitor
from tsa.redis import MAX_CONTENT_LENGTH
from tsa.robots import USER_AGENT
from tsa.robots import allowed as robots_allowed
from tsa.robots import session
from tsa.settings import Config

urllib3.disable_warnings()


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


def clear_cache(wait: int, log: logging.Logger) -> None:
    if wait > 0 or (not Config.ROBOTS and randint(1, 1000) < 25):  # nosec
        try:
            session.remove_expired_responses()
        except (
            AttributeError,
            ValueError,
            redis.exceptions.RedisError,
            KeyError,
            UnicodeDecodeError,
        ):
            log.exception("Failed to clean expired responses from cache")


def fetch(iri: str, log: logging.Logger) -> requests.Response:
    """Fetch the distribution. Mind robots.txt."""
    is_allowed, delay, robots_url = robots_allowed(iri)
    if not is_allowed:
        log.warn(f"Not allowed to fetch {iri!s} as {USER_AGENT!s}")
        raise Skip()
    with Session(db) as session:
        for delay in session.query(RobotsDelay).filter_by(robots_url):
            wait = (delay.expiration - datetime.now()).seconds
            clear_cache(wait, log)
            if wait > 0:
                log.info(f"Analyze {iri} in {wait} because of crawl-delay")
                raise RobotsRetry(wait)
            else:
                session.delete(delay)
            break
        session.commit()

    timeout = 10800  # 3h
    # a guess for 100 KB/s on data that will still make it into redis (512 MB)
    # this is mostly a safe stop in case a known RDF (tasks not time constrained) hangs along the way
    # the idea is to allow for as much time as needed for the known RDF distros, while preventing task queue "jam"
    log.debug(f"Setting timeout {timeout!s} for {iri}")
    accept = ". ".join(
        [
            "application/ld+json",
            "application/trig",
            "application/rdf+xml",
            "text/turtle",
            "text/n3;charset=utf-8",
            "application/n-triples",
            "application/n-quads",
            "application/xml;q=0.9",
            "text/xml;q=0.9",
            "text/plain;q=0.9",
            "*/*;q=0.8",
        ]
    )
    request = session.get(
        iri,
        stream=True,
        timeout=timeout,
        verify=False,
        allow_redirects=True,
        headers={"Accept": accept},
    )
    request.raise_for_status()

    if delay is not None:
        log.info(f"Recording crawl-delay of {delay} for {iri}")
        try:
            with Session(db) as session:
                session.add(RobotsDelay(iri=iri, expiration=datetime.now()+timedelta(seconds=int(delay))))
                session.commit()
        except ValueError:
            log.error("Invalid delay value - could not convert to int")
        except:
            log.error(f"Failed to set crawl-delay for {iri}: {delay}")
    return request


class NoContent(ValueError):
    pass


def get_content(iri: str, response: requests.Response) -> str:
    """Load content in memory."""
    chsize = 1024
    conlen = 0
    data = BytesIO()
    for chunk in response.iter_content(chunk_size=chsize):
        if chunk:
            data.write(chunk)
            conlen = conlen + len(chunk)
    monitor.log_size(conlen)
    try:
        return data.getvalue().decode("utf-8")
    except UnicodeDecodeError as exc:
        logging.getLogger(__name__).warning(
            "Failed to load content for %s: %s", iri, exc
        )
    raise NoContent()


def guess_format(
    iri: str, response: requests.Response, log: logging.Logger
) -> Tuple[str, bool]:
    """
    Guess format of the distribution.

    Skip if not known 5* distribution format.
    """
    guess = rdflib.util.guess_format(iri)
    if guess is None:
        guess = response.headers.get("content-type")
        if guess is not None:
            guess = guess.split(";")[0]
    monitor.log_format(str(guess))
    if "xml" in guess:
        guess = "xml"
    log.debug(f"Guessing format to be {guess!s}")

    priority = set(
        [
            "hturtle",
            "n3",
            "nquads",
            "nt",
            "trix",
            "trig",
            "turtle",
            "xml",
            "json-ld",
            "application/rdf+xml",
            "application/ld+json",
            "application/rss+xml",
            "text/turtle",
        ]
    )
    regular = set(["text/xml", "application/json", "text/plain", "html", "text/html"])
    if Config.COMPRESSED:
        priority.add("application/x-7z-compressed")
        regular.update(
            [
                "application/gzip",
                "application/x-zip-compressed",
                "application/zip",
                "application/x-gzip",
            ]
        )
    if guess not in priority.union(regular):
        log.info(f"Skipping this distribution: {iri}")
        raise Skip()

    return guess, (guess in priority)


def test_content_length(iri, request, log):
    """Test content length header if the distribution is not too large."""
    if "Content-Length" in request.headers.keys():
        conlen = int(request.headers.get("Content-Length"))
        if conlen > MAX_CONTENT_LENGTH:
            # Due to redis limitation
            log.warn(f"Skipping {iri} as it is too large: {conlen!s}")
            raise Skip()
