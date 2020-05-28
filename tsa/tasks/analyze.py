"""Celery tasks for running analyses."""
import json
import logging

import rdflib
import redis
from celery import chord

from tsa.analyzer import AbstractAnalyzer
from tsa.celery import celery
from tsa.tasks.common import TrackableTask
from tsa.redis import data as data_key, analysis_endpoint, analysis_dataset,expiration, KeyRoot


@celery.task(base=TrackableTask)
def analyze(iri, format_guess):
    """Actually run the analyzer."""
    key = data_key(iri)
    # analyze.redis.sadd('processed', iri)
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    chord(run_one_analyzer.si(token, key, format_guess) for token in tokens)(store_analysis.s(iri))


@celery.task(base=TrackableTask)
def store_analysis(results, iri):
    """Store results of the analysis in redis."""
    red = store_analysis.redis

    # results ... list of strings (json.dumps())
    if len(results) > 0:
        store = json.dumps({'analysis': [json.loads(x) for x in results if ((x is not None) and (len(x) > 0))], 'iri': iri})
    else:
        red.delete(data_key(iri))
        return

    key_result = analysis_dataset(iri)
    with red.pipeline() as pipe:
        pipe.set(key_result, store)
        pipe.sadd('purgeable', key_result)
        pipe.expire(key_result, expiration[KeyRoot.ANALYSIS])
        pipe.delete(data_key(iri))  # trash original content (index doesn't need it?)
        pipe.execute()


@celery.task(base=TrackableTask, throws=(UnicodeDecodeError))
def run_one_analyzer(analyzer_token, key, format_guess):
    """Run one analyzer identified by its token."""
    log = logging.getLogger(__name__)
    analyzer = get_analyzer(analyzer_token)

    try:
        g = rdflib.ConjunctiveGraph()
        log.debug('Parsing graph')
        red = run_one_analyzer.redis
        g.parse(data=red.get(key), format=format_guess)
        return json.dumps({analyzer_token: analyzer.analyze(g)})
    except (rdflib.plugin.PluginException, UnicodeDecodeError):
        log.debug('Failed to parse graph')
    except ValueError:
        log.exception(f'Missing data, key: {key}, analyzer: {analyzer_token}, format: {format_guess}')
    return None


def get_analyzer(analyzer_token):
    """Retrieve an analyzer identified by its token."""
    for a in AbstractAnalyzer.__subclasses__():
        if a.token == analyzer_token:
            return a()
    raise ValueError(analyzer_token)


@celery.task(base=TrackableTask)
def analyze_named(endpoint_iri, named_graph):
    """Analyze triples in a named graph of an endpoint."""
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    tasks = [run_one_named_analyzer.si(token, endpoint_iri, named_graph) for token in tokens]
    return chord(tasks)(store_named_analysis.s(endpoint_iri, named_graph))


@celery.task(base=TrackableTask)
def run_one_named_analyzer(token, endpoint_iri, named_graph):
    """Run an analyzer identified by its token on a triples in a named graph of an endpoint."""
    g = rdflib.Graph(store='SPARQLStore', identifier=named_graph)
    g.open(endpoint_iri)
    a = get_analyzer(token)
    return json.dumps({token: a.analyze(g)})


@celery.task(base=TrackableTask)
def store_named_analysis(results, endpoint_iri, named_graph):
    """Store results of the analysis in redis."""
    red = store_named_analysis.redis
    key = analysis_endpoint(endpoint_iri, named_graph)
    if len(results) > 0:
        store = json.dumps({
            'analysis': [json.loads(x) for x in results if ((x is not None) and (len(x) > 0))],
            'endpoint': endpoint_iri,
            'graph': named_graph
        })
        with red.pipeline() as pipe:
            pipe.sadd('purgeable', key)
            pipe.set(key, store)
            pipe.expire(key, expiration[KeyRoot.ANALYSIS])
            pipe.execute()