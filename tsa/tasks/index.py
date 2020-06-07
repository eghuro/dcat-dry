"""Celery tasks for indexing."""
import logging

import rdflib
from celery import group

from tsa.analyzer import AbstractAnalyzer
from tsa.celery import celery
from tsa.tasks.common import TrackableTask
from tsa.redis import data as data_key, expiration, KeyRoot, related as related_key


@celery.task(base=TrackableTask)
def index_named(iri, named):
    """Index related resources in an endpoint by initializing a SparqlGraph."""
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    return group(run_one_named_indexer.si(token, iri, named) for token in tokens).apply_async()


@celery.task(base=TrackableTask)
def run_one_named_indexer(token, iri, named):
    """Run indexer on the named graph of the endpoint."""
    g = rdflib.Graph(store='SPARQLStore', identifier=named)
    g.open(iri)
    red = run_one_named_indexer.redis
    return run_indexer(token, f'{iri}/{named}', g, red)


@celery.task(base=TrackableTask)
def index(iri, format_guess):
    """Index related resources."""
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    return group(run_one_indexer.si(token, iri, format_guess) for token in tokens).apply_async()


def run_indexer(token, iri, g, red):
    """Get all available analyzers and let them find relationships."""
    log = logging.getLogger(__name__)
    exp = expiration[KeyRoot.RELATED]

    log.info(f'Indexing {iri}')
    cnt = 0
    analyzer = get_analyzer(token)
    with red.pipeline() as pipe:
        for key, rel_type in analyzer.find_relation(g):
            log.debug(f'Distribution: {iri!s}, relationship type: {rel_type!s}, shared key: {key!s}')
            key = related_key(rel_type, key)
            pipe.sadd(key, iri)
            pipe.expire(key, exp)
            pipe.sadd('purgeable', key)
            cnt = cnt + 1
        pipe.execute()

    log.info(f'Indexed {cnt!s} records')
    return cnt


def get_analyzer(analyzer_token):
    """Retrieve an analyzer identified by its token."""
    for a in AbstractAnalyzer.__subclasses__():
        if a.token == analyzer_token:
            return a()
    raise ValueError(analyzer_token)


@celery.task(base=TrackableTask)
def run_one_indexer(token, iri, format_guess):
    """Extract graph from redis and run indexer identified by token on it."""
    log = logging.getLogger(__name__)
    red = run_one_indexer.redis
    key = data_key(iri)

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=red.get(key), format=format_guess)
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return 0
    except ValueError:
        log.debug('Failed to parse graph')
        return 0

    return run_indexer(token, iri, g, red)
