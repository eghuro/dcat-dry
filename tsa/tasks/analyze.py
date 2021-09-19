"""Celery tasks for running analyses."""
import json
import logging
from collections import defaultdict

import rdflib

from pyld import jsonld
from tsa.analyzer import AbstractAnalyzer
from tsa.monitor import TimedBlock
from tsa.redis import analysis_dataset
from tsa.redis import related as related_key


def convert_jsonld(data: str):
    json_data = json.loads(data)
    expanded = jsonld.expand(json_data)
    g = rdflib.Graph()
    g.parse(data=json.dumps(expanded), format="json-ld")


def load_graph(iri: str, data: str, format_guess: str, log_error_as_exception: bool=False) -> rdflib.ConjunctiveGraph:
    log = logging.getLogger(__name__)
    try:
        if format_guess.lower() == "json-ld":
            graph = convert_jsonld(data)
        else:
            graph = rdflib.ConjunctiveGraph()
            graph.parse(data=data, format=format_guess.lower())
        return graph
    except (TypeError, rdflib.exceptions.ParserError):
        log.warning(f'Failed to parse {iri} ({format_guess})')
    except (rdflib.plugin.PluginException, UnicodeDecodeError, UnicodeEncodeError, json.decoder.JSONDecodeError):
        message = f'Failed to parse graph for {iri}'
        {
            True: log.exception,
            False: log.warning
        }[log_error_as_exception](message)
    except ValueError:
        log.exception(f'Missing data, iri: {iri}, format: {format_guess}, data: {data[0:1000]}')
    return None


def do_analyze_and_index(graph, iri, red):
    log = logging.getLogger(__name__)
    if graph is None:
        log.debug(f'Graph is None for {iri}')
        return

    log.info(f'Analyze and index - execution: {iri}')
    analyses = []
    analyzers = [c for c in AbstractAnalyzer.__subclasses__() if hasattr(c, 'token')]
    log.debug(f'Analyzers: {len(analyzers)}')

    for analyzer_class in analyzers:
        analyzer_token = analyzer_class.token
        log.debug(f'Analyze and index {iri} with {analyzer_token}')
        analyzer = analyzer_class()

        analyze_and_index_one(analyses, analyzer, analyzer_class, graph, iri, log, red)
        log.debug(f'Done analyze and index {iri} with {analyzer_token}')

    log.debug(f'Done processing {iri}, now storing')
    store_analysis_result(iri, analyses, red)
    log.debug(f'Done storing {iri}')


def analyze_and_index_one(analyses, analyzer, analyzer_class, graph, iri, log, red):
    log.debug(f'Analyzing {iri} with {analyzer_class.token}')
    with TimedBlock(f'analyze.{analyzer_class.token}'):
        res = analyzer.analyze(graph, iri)
    log.debug(f'Done analyzing {iri} with {analyzer_class.token}')
    analyses.append({analyzer_class.token: res})

    log.debug(f'Find relations of {analyzer_class.token} in {iri}')
    try:
        iris_found = defaultdict(list)
        with TimedBlock(f'index.{analyzer_class.token}'):
            for common_iri, group, rel_type in analyzer.find_relation(graph):
                log.debug(f'Distribution: {iri!s}, relationship type: {rel_type!s}, common resource: {common_iri!s}, significant resource: {group!s}')
                # TODO: group IRI not used
                iris_found[(rel_type, common_iri)].append(iri)  # this is so that we sadd whole list in one call

        log.debug('Storing relations in redis')

        for item in iris_found.items():
            with red.pipeline() as pipe:
                (rel_type, key) = item[0]
                iris = item[1]
                log.debug(f'Addding {len(iris)} items into set')

                key = related_key(rel_type, key)
                pipe.sadd(key, *iris)
                pipe.execute()
    except TypeError:
        log.debug(f'Skip {analyzer_class.token} for {iri}')


def store_analysis_result(iri, analyses, red):
    with TimedBlock('analyze.store'):
        store = json.dumps({'analysis': [x for x in analyses if ((x is not None) and (len(x) > 0))], 'iri': iri})
        key_result = analysis_dataset(iri)
        with red.pipeline() as pipe:
            pipe.set(key_result, store)
            pipe.execute()
