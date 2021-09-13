"""User agent and robots cache."""
import resource
from functools import lru_cache
from typing import Tuple, Union

import redis
import requests
import requests_toolbelt

import tsa
from requests_cache import CachedSession
from requests_cache.backends.redis import RedisCache
from tsa.extensions import redis_pool

try:
    from reppy.robots import Robots
except ImportError:
    from tsa.mocks import Robots  # type: ignore

soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
USER_AGENT = requests_toolbelt.user_agent('DCAT DRY', tsa.__version__, extras=[('requests', requests.__version__)])
session = CachedSession('dry_dereference', backend=RedisCache(connection_pool=redis_pool))
session.headers.update({'User-Agent': USER_AGENT})
adapter = requests.adapters.HTTPAdapter(pool_connections=1000, pool_maxsize=(soft - 10), max_retries=3, pool_block=True)
session.mount('http://', adapter)
session.mount('https://', adapter)


def allowed(iri: str) -> Tuple[bool, Union[int, None], str]:
    robots_iri = Robots.robots_url(iri)

    text = fetch_robots(robots_iri)
    if text is None:
        return True, None, robots_iri
    robots = Robots.parse('', text)
    return robots.allowed(iri, USER_AGENT), robots.agent(USER_AGENT).delay, robots_iri


@lru_cache()
def fetch_robots(robots_iri: str) -> Union[str, None]:
    if len(robots_iri) == 0:
        return None
    red = redis.Redis(connection_pool=redis_pool)
    key = f'robots_{robots_iri}'
    if red.exists(key):
        return str(red.get(key))

    response = session.get(robots_iri, verify=False)
    with red.pipeline() as pipe:
        if response.status_code != 200:
            pipe.set(key, '')
            pipe.expire(key, 30 * 24 * 60 * 60)
            pipe.execute()
            return None

        text = response.text
        pipe.set(key, text)
        pipe.expire(key, 30 * 24 * 60 * 60)
        pipe.execute()
        return text
