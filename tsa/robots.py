"""User agent and robots cache."""
import os
import resource
from typing import Tuple, Union

import redis
import requests
import requests_toolbelt
from requests_cache import CachedSession
from requests_cache.backends.filesystem import FileCache

import tsa
from tsa.cache import redis_lru as lru_cache
from tsa.settings import Config

try:
    from reppy import Utility
    from reppy.parser import Rules
except ImportError:
    Config.ROBOTS = False

soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
USER_AGENT = requests_toolbelt.user_agent(
    "DRYbot", tsa.__version__, extras=[("requests", requests.__version__)]
)

session = CachedSession(
    "dry_dereference",
    backend=FileCache(use_temp=True),
    expire_after=3600,
    cache_control=True,
    allowable_codes=[200, 400, 404],
    stale_if_error=True,
)
session.headers.update({"User-Agent": USER_AGENT})
adapter = requests.adapters.HTTPAdapter(
    pool_connections=1000, pool_maxsize=(soft - 10), max_retries=3, pool_block=True
)
session.mount("http://", adapter)
session.mount("https://", adapter)


def allowed(iri: str) -> Tuple[bool, Union[int, None], str]:
    if not Config.ROBOTS:
        return True, None, None
    robots_iri = Utility.roboturl(iri)
    text = fetch_robots(robots_iri)
    if text is None:
        return True, None, robots_iri
    robots = Rules(robots_iri, 200, text, None)
    return robots.allowed(iri, USER_AGENT), robots.delay(USER_AGENT), robots_iri


@lru_cache(conn=redis.Redis.from_url(os.environ.get('REDIS', 'redis://localhost:6379/0')))
def fetch_robots(robots_iri: str) -> Union[str, None]:
    if len(robots_iri) == 0:
        return None
    response = session.get(robots_iri, verify=False)
    text = response.text
    return text
