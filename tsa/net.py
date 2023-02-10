import logging
from collections import defaultdict
from datetime import datetime, timedelta
from io import BytesIO
from random import randint
from typing import Tuple, Optional

import rdflib
import redis
import requests
import urllib3
from sqlalchemy.exc import SQLAlchemyError

from tsa.db import db_session
from tsa.model import RobotsDelay
from tsa.monitor import monitor
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


class RobotsBlock:
    def __init__(self, iri: str):
        self.__iri = iri
        self.__delay = None

    @staticmethod
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

    def __enter__(self):
        log = logging.getLogger(__name__)
        is_allowed, self.__delay, robots_url = robots_allowed(self.__iri)
        if not is_allowed:
            log.warning("Not allowed to fetch %s as %s", self.__iri, USER_AGENT)
            raise Skip()
        for delay in db_session.query(RobotsDelay).filter_by(iri=robots_url):
            wait = (delay.expiration - datetime.now()).seconds
            self.clear_cache(wait, log)
            if wait > 0:
                log.info("Analyze %s in %s because of crawl-delay", self.__iri, wait)
                raise RobotsRetry(wait)
            db_session.delete(delay)
            break
        try:
            db_session.commit()
        except SQLAlchemyError:
            logging.getLogger(__name__).exception(
                "Failed do commit, rolling back expired delay removal"
            )
            db_session.rollback()

    def __exit__(self, *args):
        if self.__delay is not None:
            log = logging.getLogger(__name__)
            log.info("Recording crawl-delay of %s for %s", self.__delay, self.__iri)
            try:
                expire = datetime.now() + timedelta(seconds=int(self.__delay))
                db_session.add(RobotsDelay(iri=self.__iri, expiration=expire))
                db_session.commit()
            except ValueError:
                log.error("Invalid delay value - could not convert to int")
            except SQLAlchemyError:
                log.exception(
                    "Failed to set crawl-delay for %s: %s", self.__iri, self.__delay
                )
                db_session.rollback()


accept = ", ".join(
    (
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
    )
)


def fetch(iri: str) -> requests.Response:
    """Fetch the distribution. Mind robots.txt."""
    with RobotsBlock(iri):  # can raise Skip, RobotsRetry
        timeout = Config.TIMEOUT
        request = session.get(
            iri,
            stream=True,
            timeout=timeout,
            verify=False,
            allow_redirects=True,
            headers={"Accept": accept},
        )
        request.raise_for_status()
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


def make_guess(iri: str, response: requests.Response) -> str:
    """Guess the format."""
    guess_candidate = rdflib.util.guess_format(iri)  # type: str | None
    if guess_candidate is None:
        guess_candidate = response.headers.get("content-type")  # type: str | None
        if guess_candidate is not None:
            return guess_candidate.split(";")[0]
        return ""
    return guess_candidate


priority = (
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
)
regular = ("text/xml", "application/json", "text/plain", "html", "text/html")
decompression_map = defaultdict(lambda: None)
if Config.COMPRESSED:
    priority = priority + tuple("application/x-7z-compressed")
    regular = regular + (
        "application/gzip",
        "application/x-zip-compressed",
        "application/zip",
        "application/x-gzip",
    )
    decompression_map = defaultdict(
        lambda: None,
        {
            "application/zip": "zip",
            "application/x-zip-compressed": "zip",
            "application/x-7z-compressed": "zip",
            "application/gzip": "gzip",
            "application/x-gzip": "gzip",
        },
    )
accepted = priority + regular


def guess_format(
    iri: str, response: requests.Response, log: logging.Logger
) -> Tuple[str, bool, Optional[str]]:
    """
    Guess format of the distribution.

    Skip if not known 5* distribution format.
    """

    guess = make_guess(iri, response)
    monitor.log_format(guess)
    if "xml" in guess:
        guess = "xml"
    if "json" in guess:
        guess = "json-ld"
    log.debug("Guessing format to be %s", guess)

    if guess not in accepted:
        log.info("Skipping this distribution: %s", iri)
        raise Skip()

    return guess, (guess in priority), decompression_map[guess]
