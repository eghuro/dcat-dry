"""Celery tasks for running analyses."""
import json

import rdflib

from tsa.analyzer import *
from tsa.monitor import TimedBlock
from tsa.redis import analysis_dataset
from tsa.redis import related as related_key


def load_graph(iri, data, format_guess):
    log = logging.getLogger(__name__)
    try:
        log.debug('Parsing')
        g = rdflib.ConjunctiveGraph()
        g.parse(data=data, format=format_guess.lower())
        log.debug('Done parsing')
        return g
    except TypeError:
        log.warning(f'Failed to parse {iri} ({format_guess})')
    except (rdflib.plugin.PluginException, UnicodeDecodeError):
        log.warning(f'Failed to parse graph for {iri}')
    except UnicodeEncodeError:
        log.exception(f'Failed to parse graph for {iri}')
    except ValueError:
        log.exception(f'Missing data, iri: {iri}, format: {format_guess}, data: {data}')
    return None


def do_analyze_and_index(g, iri, red):
    log = logging.getLogger(__name__)
    if g is None:
        log.debug(f'Graph is None for {iri}')
        return

    log.info(f'Analyze and index - execution: {iri}')
    analyses = []
    analyzers = AbstractAnalyzer.__subclasses__()
    log.debug(f'Analyzers: {len(analyzers)}')

    for analyzer_class in analyzers:
        analyzer_token = analyzer_class.token
        log.debug(f'Analyze and index {iri} with {analyzer_token}')
        analyzer = analyzer_class()

        analyze_and_index_one(analyses, analyzer, analyzer_class, g, iri, log, red)
        log.debug(f'Done analyze and index {iri} with {analyzer_token}')

    log.debug(f'Done processing {iri}, now storing')
    try:
        store_analysis_result(iri, analyses, red)
    except:
        log.exception(f'Failed to store analysis for {iri}')
    log.debug(f'Done storing {iri}')


def analyze_and_index_one(analyses, analyzer, analyzer_class, g, iri, log, red):
    log.debug(f'Analyzing {iri} with {analyzer_class.token}')
    try:
        try:
            res = None
            with TimedBlock(f'analyze.{analyzer_class.token}'):
                res = analyzer.analyze(g, iri)
            log.debug(f'Done analyzing {iri} with {analyzer_class.token}')
            analyses.append({analyzer_class.token: res})
        except:
            log.exception(f'Failed to analyze {iri} with {analyzer_class.token}')
            return

        log.debug(f'Find relations of {analyzer_class.token} in {iri}')
        try:
            iris_found = defaultdict(list)
            with TimedBlock(f'index.{analyzer_class.token}'):
                for common_iri, significant_iri, rel_type in analyzer.find_relation(g):
                    log.debug(f'Distribution: {iri!s}, relationship type: {rel_type!s}, common resource: {common_iri!s}, significant resource: {significant_iri!s}')
                    #FIXME: significant IRI not used
                    iris_found[(rel_type, common_iri)].append(iri)  # this is so that we sadd whole list in one call

            log.debug('Storing relations in redis')

            for item in iris_found.items():
                with red.pipeline() as pipe:
                    (rel_type, key) = item[0]
                    iris = item[1]
                    log.debug(f'Addding {len(iris)} items into set')

                    key = related_key(rel_type, key)
                    pipe.sadd(key, *iris)
                    #pipe.expire(key, exp)
                    # pipe.sadd('purgeable', key)
                    pipe.execute()
        except TypeError:
            log.debug(f'Skip {analyzer_class.token} for {iri}')
        except:
            log.exception(f'Failed to find relations in {iri} with {analyzer_class.token}')
    except:
        log.exception(f'Unexpected exception while processing {iri} with {analyzer_class.token}')


#@celery.task(base=TrackableTask)
#def analyze_named(endpoint_iri, named_graph):
#    """Analyze triples in a named graph of an endpoint."""
#    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
#    tasks = [run_one_named_analyzer.si(token, endpoint_iri, named_graph) for token in tokens]
#    return chord(tasks)(store_named_analysis.s(endpoint_iri, named_graph))


#@celery.task(base=TrackableTask, bind=True)
#def run_one_named_analyzer(self, token, endpoint_iri, named_graph, dereference=True):
#    """Run an analyzer identified by its token on a triples in a named graph of an endpoint."""
#    g = rdflib.Graph(store='SPARQLStore', identifier=named_graph)
#    g.open(endpoint_iri)
#    a = get_analyzer(token)
#    res = a.analyze(g)
#    if dereference:
#        do_dereference(token, res, g, self)
#    return json.dumps({token: res})


#@celery.task(base=TrackableTask)
#def store_named_analysis(results, endpoint_iri, named_graph):
#    """Store results of the analysis in redis."""
#    red = store_named_analysis.redis
#    key = analysis_endpoint(endpoint_iri, named_graph)
#    if len(results) > 0:
#        store = json.dumps({
#            'analysis': [json.loads(x) for x in results if ((x is not None) and (len(x) > 0))],
#            'endpoint': endpoint_iri,
#            'graph': named_graph
#        })
#        with red.pipeline() as pipe:
            #pipe.sadd('purgeable', key)
#            pipe.set(key, store)
            # pipe.expire(key, expiration[KeyRoot.ANALYSIS])
#            pipe.execute()



def store_analysis_result(iri, analyses, red):
    with TimedBlock('analyze.store'):
        store = json.dumps({'analysis': [x for x in analyses if ((x is not None) and (len(x) > 0))], 'iri': iri})
        key_result = analysis_dataset(iri)
        with red.pipeline() as pipe:
            pipe.set(key_result, store)
            pipe.execute()
