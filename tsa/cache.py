"""Helper tools for caching."""
import binascii
import datetime
import inspect
import os
import pickle
from collections import namedtuple
from functools import wraps

from flask import make_response, request

from tsa.extensions import cache


def cached(
    cacheable=False,
    must_revalidate=True,
    client_only=True,
    client_timeout=0,
    server_timeout=5 * 60,
    key="view/%s",
):
    """Flask cache decorator.

    See https://codereview.stackexchange.com/q/147038,
        https://jakearchibald.com/2016/caching-best-practices/ and
        https://developers.google.com/web/fundamentals/performance/optimizing-content-efficiency/http-caching
        for more details.
    """

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            cache_key = key % request.full_path  # include querystring
            cache_policy = ""
            if not cacheable:
                cache_policy += ", no-store"  # tells the browser not to cache at all
            else:
                if (
                    must_revalidate
                ):  # this looks contradicting if you haven't read the article.
                    # no-cache doesn't mean "don't cache", it means it must check
                    # (or "revalidate" as it calls it) with the server before
                    # using the cached resource
                    cache_policy += ", no-cache"
                else:
                    # Also must-revalidate doesn't mean "must revalidate", it
                    # means the local resource can be used if it's younger than
                    # the provided max-age, otherwise it must revalidate
                    cache_policy += ", must-revalidate"

                if client_only:
                    cache_policy += ", private"
                else:
                    cache_policy += ", public"

                cache_policy += ", max-age=%d" % (client_timeout)

            headers = {}
            cache_policy = cache_policy.strip(",")
            headers["Cache-Control"] = cache_policy
            now = datetime.datetime.utcnow()

            client_etag = request.headers.get("If-None-Match")

            response = cache.get(cache_key)
            # respect the hard-refresh
            if (
                response is not None
                and request.headers.get("Cache-Control", "") != "no-cache"
            ):
                headers["X-Cache"] = "HIT from Server"
                cached_etag = response.headers.get("ETag")
                if client_etag and cached_etag and client_etag == cached_etag:
                    headers["X-Cache"] = "HIT from Client"
                    headers["X-Last-Modified"] = response.headers.get("X-LastModified")
                    response = make_response("", 304)
            else:
                response = make_response(f(*args, **kwargs))
                if response.status_code == 200 and request.method in ["GET", "HEAD"]:
                    headers["X-Cache"] = "MISS"
                    # - Added the headers to the response object instead of the
                    # headers dict so they get cached too
                    # - If you can find any faster random algorithm go for it.
                    response.headers.add("ETag", binascii.hexlify(os.urandom(4)))
                    response.headers.add("X-Last-Modified", str(now))
                    cache.set(cache_key, response, timeout=server_timeout)

            response.headers.extend(headers)
            return response

        return decorated_function

    return decorator


# source: https://gist.github.com/abulka/6ab5b2afc5d1adda6f08126a617dd02a
ALLOW_NON_REDIS_CACHING = False

_CacheInfo = namedtuple("_CacheInfo", ["hits", "misses", "maxsize", "currsize"])
_CacheInfoVerbose = namedtuple(
    "_CacheInfoVerbose", ["hits", "misses", "maxsize", "currsize", "paramsignatures"]
)


def redis_lru(maxsize=None, slice=slice(None), conn=None, optimisekwargs=True):
    """
    Simple Redis-based LRU cache decorator *.

    *conn* 	          Redis connection
    *maxsize*         maximum number of entries in LRU cache
    *slice*           slice object for restricting prototype args
    *optimisekwargs*  convert all parameter signatures into kwargs dict so only one cache
                      entry needed for semantically equiv. calls
                      (recommended, default is True)
    Original blog post
    https://blog.warrick.io/2012/12/09/redis-lru-cache-decorator-in-python.html
    Usage is as simple as prepending the decorator to a function,
    passing a Redis connection object, and the desired capacity
    of your cache.
    @redis_lru(maxsize=10000)
    def func(foo, bar):
        # some expensive operation
        return baz
    func.init(redis.StrictRedis())
    Uses 4 Redis keys, all suffixed with the function name:
        lru:keys: - sorted set, stores hash keys
        lru:vals: - hash, stores function output values
        lru:hits: - string, stores hit counter
        lru:miss: - string, stores miss counter
    * Functions prototypes must be serializable equivalent!
    Python 3 port and enhancements by Andy Bulka, abulka@gmail.com, June 2021
    -------------------------------------------------------------------------
    - Python 3 compatibility
    - redis-py 3.0 compatibile
    - Made some things more like Python 3 functools.lru_cache
        - renamed .clear() to .cache_clear()
        - renamed .info() to .cache_info()
        - .cache_info() now returns namedtuple object like Python 3
          functools.lru_cache does
        - renamed redis_lru 'capacity' parameter to 'maxsize', allow it to be
          None
    - Enable passing in `conn` via the decorator
    - Added version number to source code
    - Raise exception if redis_lru function has no redis connection
    - Added cache_clear_entry() method
    - Added verbose flag for cache_info()
    * Granular cache clearing with cache_clear_entry():
        Whilst cache_clear() clears all cache entries for the function, cache_clear_entry() is
        more granular, only clearing the particular cache entry matching the parameters passed to
        cache_clear_entry(). E.g.
            f(1) f(2) f.cache_clear() - all caches are lost
            f(1) f(2) f.cache_clear_entry(1) - cache for f(1) deleted, but f(2)
                                                still cached
        See https://stackoverflow.com/questions/56413413/lru-cache-is-it-possible-to-clear-only-a-specific-call-from-the-cache
        - Advanced discussion on the use of cache_clear_entry()
            If you have gone against the recommended default, and passed
            optimisekwargs=False to the decorator, please use cache_clear_entry() with great care
            since whilst e.g. `f(1)` and `f(param=1)` mean the same, the lru caching system will
            cache those two calls as separate entries. Then when you invalidate one style of call
            with `f.cache_clear_entry(1)` this leaves the other style of call `f(param=1)` still
            cached and returning stale values - even though semantically these two styles of call are
            the same. So if you do use cache_clear_entry() in this way, make sure you call it
            repeatedly for all possible parameter signatures that you might have used e.g.
            `f.cache_clear_entry(1)`; `f.cache_clear_entry(param=1)`.
            On the other hand if you have kept the default optimisekwargs=True on the decorator then
            you you don't need to worry about any of this - simply call either `f.cache_clear_entry(
            1)` or `f.cache_clear_entry(param=1)` and since they are semantically equivalent,
            clearing either one will clear the cache for both - because the same cache entry
            is used for both function parameter signature - made possible by 'normalising' all calls
            into a sorted, single kwarg dict and not using positional parameters at all, meaning the
            same cache entry is calculated for all semantically equivalent calls - nice.
    * Enhanced verbose flag for cache_info():
        You may now pass 'verbose' to cache_info e.g. `cache_info(verbose=True)` which returns a
        namedtuple with one additional member "paramsignatures" e.g. `["hits", "misses",
        "maxsize", "currsize", "paramsignatures"]`. The additional item "paramsignatures" in the
        tuple is a list of all the active parameter signatures being cached for this particular
        function.
        - Debugging and watching "paramsignatures" using cache_info()
            Invalidating a particular parameter signature using the enhanced cache_clear_entry(
            ...) with those parameters will delete that parameter signature from this list of tuples.
            If you are using the default and recommended optimisekwargs=True on the decorator then
            all tuples returned by cache_info(verbose=True) will be a kwarg dictionary converted
            into a sorted list of tuples, with no positional parameters e.g.
            (('bar', 2), ('baz', 'A'), ('foo', 1))
            If you are for some reason using optimisekwargs=False on the decorator then
            E.g. If you called f.cache_info(verbose=True) and got "paramsignatures" as two signatures
            [(1, 2, 'A'), (1, 2, ('baz', 'A'))] then calling f.cache_clear_entry(1, 2, 'A') then
            calling f.cache_info(verbose=True) you will see that "paramsignatures" will now be
            reported as merely one signature [(1, 2, ('baz', 'A'))]
    Tests:
        - Added asserts to the tests
        - test a second function
        - test maxsize of None
        - test maxsize of 1 and ensure cache ejection works
        - additional tests
    Tips:
        - Always call `somefunc.init(conn)` with the redis connection otherwise
            your function won't cache. Or pass `conn` in via the decorator (new feature in v1.4).
        - Call somefunc.cache_clear() at the start of your tests, since cached
            results are permanently in redis
    Example Usage:
        from fncache import redis_lru as lru_cache
        from redis_my_module import conn
        @lru_cache(maxsize=None, conn=conn)
        def getProjectIds(user) -> List[str]:
            return 1
        # Later somewhere else
        getProjectIds.cache_clear()
    """
    if maxsize is None:
        maxsize = 5000

    def decorator(func):
        cache_keys = f"lru:keys:{func.__name__}"
        cache_vals = f"lru:vals:{func.__name__}"
        cache_hits = f"lru:hits:{func.__name__}"
        cache_miss = f"lru:miss:{func.__name__}"

        lvars = [None]  # closure mutable

        def add(key, value):
            eject()
            conn = lvars[0]
            conn.incr(cache_miss)
            conn.hset(cache_vals, key, pickle.dumps(value))

            """
            Python 3, redis-py 3.0 fix
            zadd() - Set any number of element-name, score pairs to the key ``name``. Pairs
            are specified as a dict of element-names keys to score values. The score values should
            be the string representation of a double precision floating point number.

            redis-py 3.0 has changed these three commands to all accept a single positional
            argument named mapping that is expected to be a dict. For MSET and MSETNX, the
            dict is a mapping of key-names -> values. For ZADD, the dict is a mapping of
            element-names -> score. https://pypi.org/project/redis/
            """
            conn.zadd(cache_keys, {key: 0.0})

            return value

        def get(key):
            conn = lvars[0]
            value = conn.hget(cache_vals, key)
            if value:
                conn.incr(cache_hits)

                """
                Python 3, redis-py 3.0 fix
                All 2.X users that rely on ZINCRBY must swap the order of amount and value for the
                command to continue to work as intended. https://pypi.org/project/redis/
                """
                conn.zincrby(cache_keys, 1.0, key)

                value = pickle.loads(value)
            return value

        def eject():
            conn = lvars[0]
            """
            In python 2.7, the / operator is integer division if inputs are integers.
            In python 3 Integer division is achieved by using //
            """
            count = min((maxsize // 10) or 1, 1000)

            if conn.zcard(cache_keys) >= maxsize:
                eject = conn.zrange(cache_keys, 0, count)
                conn.zremrangebyrank(cache_keys, 0, count)
                conn.hdel(cache_vals, *eject)

        @wraps(func)
        def wrapper(*args, **kwargs):
            conn = lvars[0]
            if conn:
                if optimisekwargs:
                    # converts args and kwargs into just kwargs, taking into account default params
                    kwargs = inspect.getcallargs(func, *args, **kwargs)
                    items = tuple(sorted(kwargs.items()))
                else:
                    items = args + tuple(sorted(kwargs.items()))
                key = pickle.dumps(items[slice])
                if optimisekwargs:
                    return get(key) or add(key, func(**kwargs))
                else:
                    return get(key) or add(key, func(*args, **kwargs))
            else:
                if ALLOW_NON_REDIS_CACHING:
                    return func(*args, **kwargs)  # Original behaviour (deprecated)
                else:
                    raise RuntimeWarning(
                        f"redis_lru - no redis connection has been supplied "
                        f"for caching calls to {func.__name__}"
                    )

        def cache_info(verbose=False):
            conn = lvars[0]
            size = int(conn.zcard(cache_keys) or 0)
            hits, misses = (
                int(conn.get(cache_hits) or 0),
                int(conn.get(cache_miss) or 0),
            )

            if verbose:
                paramsignatures = conn.zrange(cache_keys, 0, 9999)
                return _CacheInfoVerbose(
                    hits,
                    misses,
                    maxsize,
                    size,
                    [pickle.loads(sig) for sig in paramsignatures],
                )

            return _CacheInfo(hits, misses, maxsize, size)

        def cache_clear(*args, **kwargs):
            # traditional behaviour of invalidating the entire cache for this decorated function,
            # for all parameter signatures - same as Python 3 functools.lru_cache
            conn = lvars[0]
            if conn:
                conn.delete(cache_keys, cache_vals)
                conn.delete(cache_hits, cache_miss)

        def cache_clear_entry(*args, **kwargs):
            # invalidate only the cache entry matching the parameter signature passed into this
            # method - very fancy new granular clear functionality ;-)  By default, also invalidates
            # all semantically equivalent parameter signatures.
            conn = lvars[0]
            if conn:
                if optimisekwargs:
                    kwargs = inspect.getcallargs(func, *args, **kwargs)
                    items = tuple(sorted(kwargs.items()))
                else:
                    items = args + tuple(sorted(kwargs.items()))
                key = pickle.dumps(items[slice])
                conn.hdel(cache_vals, key)  # remove cached return value from hash
                conn.zrem(cache_keys, key)  # remove param score stuff from sorted set

        def init(conn):
            lvars[0] = conn

        if conn:
            init(conn)

        wrapper.init = init
        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        wrapper.cache_clear_entry = cache_clear_entry
        return wrapper

    return decorator
