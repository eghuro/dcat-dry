"""Celery tasks for querying."""
import math
import statistics
import json
import logging
import itertools
from collections import defaultdict, OrderedDict
from json import JSONEncoder, JSONDecoder

import redis
import rfc3987
from celery import group, chain, chord
from pymongo.errors import DocumentTooLarge

from tsa.celery import celery
from tsa.extensions import redis_pool, mongo_db
from tsa.analyzer import AbstractAnalyzer, SkosAnalyzer
from tsa.redis import EXPIRATION_CACHED, EXPIRATION_TEMPORARY, related as related_key, root_name, KeyRoot, codelist


### ANALYSIS ###

@celery.task
def compile_analyses(iris):
    red = redis.Redis(connection_pool=redis_pool)
    analyzes = [json.loads(x) for x in [red.get(f'analyze:{iri}') for iri in iris] if x is not None]
    #analyzes = [json.loads(dump) for dump in red.lrange('analyze', 0, -1)]
    #red.delete('analyses', 'predicates', 'classes', *[f'external:{iri}' for iri in iris], *[f'internal:{iri}' for iri in iris])
    return analyzes  # the BIG report


@celery.task
def split_analyses_by_iri(analyses, id):
    red = redis.Redis(connection_pool=redis_pool)
    iris = set()
    log = logging.getLogger(__name__)
    #with red.pipeline() as pipe:  # MULTI ... EXEC block
    for analysis in analyses:  # long loop
        log.debug(str(analysis))
        if 'analysis' in analysis.keys():
            content = analysis['analysis']
            if 'iri' in analysis.keys():
                iri = analysis['iri']
                iris.add(iri)
            elif 'endpoint' in analysis.keys() and 'graph' in analysis.keys():
                iri = f'{analysis["endpoint"]}'  # /{analysis["graph"]}'
                iris.add(analysis['endpoint'])  # this is because named graph is not extracted from DCAT
            else:
                log.error('Missing iri and endpoint/graph')

            log.debug(iri)
            key = f'analysis:{id}:{iri!s}'
            log.debug(key)
            red.rpush(key, json.dumps(content))
            #red.expire(key, EXPIRATION_TEMPORARY)

        else:
            log.error('Missing content')
    return list(iris)


@celery.task
def extract_codelists_objects(ds_list):
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)
    for ds in ds_list:
        if red.hexists(root_name[KeyRoot.CODELISTS], ds):  # this is a codelist
            try:
                analysis = json.loads(red.get(f'dsanalyses:{ds}'))
                # hash: object -> ds (lookup: object -> codelist)
                if "generic" in analysis:
                    try:
                        for subject in analysis["generic"]["subjects"]:  # index subjects (they will be referenced)
                            red.sadd(codelist(subject), ds)
                            red.sadd(codelist(ds), subject)
                    except KeyError:
                        pass
                        #log.exception(f'DS: {ds}')
            except TypeError:
                pass
                #log.exception(f'DS: {ds}')


def gen_analyses(id, ds, red):
    for ds_iri in ds:

        for distr_iri in red.smembers(f'dsdistr:{ds_iri}'):
            key_in = f'analysis:{id}:{distr_iri!s}'
            logging.getLogger(__name__).info(key_in)
            for a in [json.loads(a) for a in red.lrange(key_in, 0, -1)]:
                for b in a:  # flatten
                    for key in b.keys():  # 1 element
                        analysis = {'ds_iri': ds_iri, 'batch_id': id}
                        analysis[key] = b[key]  # merge dicts
                        yield analysis


@celery.task(ignore_result=True)
def merge_analyses_by_distribution_iri_and_store(iris, id):
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)

    # Get list of dcat:Dataset iris for distribution iris
    # and create a mapping of (used) distribution iris per dataset iri

    ds = []
    if len(iris) > 0:
        ds_iris = red.hmget('distrds', *iris)
        for distr_iri, ds_iri in zip(iris, ds_iris):
            if ds_iri is None:
                log.debug(f'Missing DS IRI for {distr_iri}')  # this usually means we skipped it
                continue

            key = f'dsdistr:{ds_iri}'
            with red.pipeline() as pipe:
                pipe.sadd(key, distr_iri)
                pipe.expire(key, EXPIRATION_TEMPORARY)
                pipe.sadd('relevant_distr', distr_iri)
                pipe.expire(key, EXPIRATION_CACHED)
                pipe.execute()
            ds.append(ds_iri)

    ds = set(ds)

    # Merge individual distribution analyses into DS analysis (for query endpoint)
    try:
        log.info('Cleaning mongo')
        mongo_db.dsanalyses.delete_many({})
        mongo_db.dsanalyses.insert_many([analysis for analysis in gen_analyses(id, ds, red)])
    except DocumentTooLarge:
        log.exception('Failed to store analysis for {iri}')
    log.info("Done")

    return list(ds)


### INDEX ###
reltypes = sum((analyzer.relations for analyzer in AbstractAnalyzer.__subclasses__() if 'relations' in analyzer.__dict__), [])


def relevant_ds(red):
    for ds in red.sscan_iter('relevant_distr'):
        yield ds

@celery.task
def gen_related_ds():
    red = redis.Redis(connection_pool=redis_pool)
    related_ds = {}
    distributions = set()

    for rel_type in reltypes:
        related_ds[rel_type] = []
        root = f'related:{rel_type!s}:'
        for key in red.scan_iter(match=f'related:{rel_type!s}:*'):
            token = key[len(root):]
            related_dist = red.smembers(related_key(rel_type, token))  # these are related by token
            if len(related_dist) > 0:
                structure, structural_element, element = token.split(':')
                related_ds[rel_type].append({'structure': structure, 'structural_element': element, 'element': element, 'related': list(set(red.hmget('distrds', *related_dist))) })

    try:
        mongo_db.related.delete_many({})
        mongo_db.related.insert(related_ds)
    except DocumentTooLarge:
        log.exception('Failed to store related datasets')

    del related_ds['_id']
    return related_ds


### MISC ###

@celery.task
def add_stats(analyses, stats):
    if stats:
        logging.getLogger(__name__).info('Stats')
        red = redis.Redis(connection_pool=redis_pool)
        analyses.append({
            'format': list(red.hgetall('stat:format')),
            'size': retrieve_size_stats(red) #check
        })
    return analyses


def convert_size(size_bytes):
    """Convert size in bytes into a human readable string."""
    if size_bytes == 0:
        return '0B'
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return '%s %s' % (s, size_name[i])


def retrieve_size_stats(red):
    """Load sizes from redis and calculate some stats about it."""
    lst = sorted([int(x) for x in red.lrange('stat:size', 0, -1)])
    try:
        mode = statistics.mode(lst)
    except statistics.StatisticsError:
        mode = None
    try:
        mean = statistics.mean(lst)
    except statistics.StatisticsError:
        mean = None
    try:
        stdev = statistics.stdev(lst, mean)
    except statistics.StatisticsError:
        stdev = None
    try:
        var = statistics.variance(lst, mean)
    except statistics.StatisticsError:
        var = None

    try:
        minimum = min(lst)
    except ValueError:
        minimum = None

    try:
        maximum = max(lst)
    except ValueError:
        maximum = None

    return {
        'min': convert_size(minimum),
        'max': convert_size(maximum),
        'mean': convert_size(mean),
        'mode': convert_size(mode),
        'stdev': convert_size(stdev),
        'var': var
    }


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super(JSONEncoder, self).default(obj)
