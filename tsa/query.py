from celery import chain

from tsa.tasks.query import *

def _graph_iris(red):
    root = 'graphs:'
    for k in red.scan_iter(f'graphs:*'):
        yield k[len(root):]


def _get_known_distributions(red):
    distr_endpoints = red.smembers('distributions').union(frozenset(_graph_iris(red)))
    failed_skipped = red.smembers('stat:failed').union(red.smembers('stat:skipped'))
    return distr_endpoints.difference(failed_skipped)


def query(result_id, red):
    iris = list(_get_known_distributions(red))
    return chain([
        compile_analyses.si(iris),
        split_analyses_by_iri.s(result_id),
        merge_analyses_by_distribution_iri_and_store.s(result_id),
        extract_codelists_objects.s(),
        gen_related_ds.si(),
    ]).apply_async(queue='query')