"""Celery tasks for querying."""
import json
import logging
import math
import statistics
from json import JSONEncoder

import redis
from pymongo.errors import DocumentTooLarge, OperationFailure

from tsa.celery import celery
from tsa.extensions import conceptIndex, ddrIndex, dsdIndex, get_mongo, redis_pool, sameAsIndex
from tsa.redis import EXPIRATION_CACHED, EXPIRATION_TEMPORARY, ds_distr, pure_subject
from tsa.redis import related as related_key
from tsa.redis import sanitize_key
from tsa.report import export_labels
from tsa.ruian import RuianInspector

### ANALYSIS (PROFILE) ###

def _gen_iris(red, log):
    root = 'analyze:'
    for key in red.scan_iter(match=f'distrds:*'):
        distr_iri = key[len('distrds:'):]
        key1 = f'{root}{sanitize_key(distr_iri)}'
        # log.debug(key1)
        try:
            x = red.get(key1)
        except:
            continue
        if x is None:
            continue
        analysis = json.loads(x)
        # log.debug(str(analysis))
        if 'analysis' in analysis.keys():
            content = analysis['analysis']
            if 'iri' in analysis.keys():
                iri = analysis['iri']
                yield iri, content
            elif 'endpoint' in analysis.keys() and 'graph' in analysis.keys():
                iri = f'{analysis["endpoint"]}'  # /{analysis["graph"]}'
                yield analysis['endpoint'], content  # this is because named graph is not extracted from DCAT
            else:
                log.error('Missing iri and endpoint/graph')
        else:
            log.error('Missing content')


@celery.task
def compile_analyses(batch_id):
    log =  logging.getLogger(__name__)
    log.info("Compile analyzes")
    red = redis.Redis(connection_pool=redis_pool)

    ds = set()
    for distr_iri, content in _gen_iris(red, log):
        log.debug(distr_iri)
        ds_iris = red.smembers(f'distrds:{distr_iri}')
        key = f'analysis:{batch_id}:{sanitize_key(distr_iri)}'
        #log.debug(key)
        for ds_iri in ds_iris:
            if ds_iri is None:
                with red.pipeline() as pipe:
                    pipe.rpush(key, json.dumps(content))
                    pipe.expire(key, EXPIRATION_CACHED)
                    pipe.execute()
                continue
            with red.pipeline() as pipe:
                pipe.rpush(key, json.dumps(content))
                pipe.expire(key, EXPIRATION_CACHED)
                key_1 = f'dsdistr:{ds_iri}'
                pipe.sadd(key_1, distr_iri)
                pipe.expire(key_1, EXPIRATION_TEMPORARY)
                pipe.sadd('relevant_distr', distr_iri)
                pipe.expire('relevant_distr', EXPIRATION_CACHED)
                pipe.execute()
            ds.add(ds_iri)

    return list(ds)


def gen_analyses(batch_id, ds, red):
    log =  logging.getLogger(__name__)
    log.info(f'Generate analyzes ({len(ds)})')
    for ds_iri in ds:
        for distr_iri in red.smembers(f'dsdistr:{ds_iri}'):
            key_in = f'analysis:{batch_id}:{sanitize_key(distr_iri)}'
            #logging.getLogger(__name__).info(key_in)
            for a in [json.loads(a) for a in red.lrange(key_in, 0, -1)]:
                for b in a:  # flatten
                    for key in b.keys():  # 1 element
                        analysis = {'ds_iri': ds_iri, 'batch_id': batch_id}
                        analysis[key] = b[key]  # merge dicts
                        yield analysis


@celery.task(ignore_result=True)
def store_to_mongo(ds, batch_id):
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    _, mongo_db = get_mongo()
    log.info('Cleaning mongo')
    mongo_db.dsanalyses.delete_many({})
    insert_count, gen_count = 0, 0
    for analysis in gen_analyses(batch_id, ds, red):
        gen_count = gen_count + 1
        try:
            mongo_db.dsanalyses.insert_one(analysis)
        except DocumentTooLarge:
            iri = analysis['ds_iri']
            log.exception(f'Failed to store analysis for {batch_id} (ds: {iri})')
        except OperationFailure:
            log.exception(f'Operation failure')
        else:
            insert_count = insert_count + 1
    log.info(f'Stored analyses ({insert_count}/{gen_count})')

    return ds


### INDEX ###
#reltypes = sum((analyzer.relations for analyzer in AbstractAnalyzer.__subclasses__() if 'relations' in analyzer.__dict__), [])
#reltypes.extend(['skosqb', 'conceptUsage', 'relatedConceptUsage', 'resourceOnDimension', 'conceptOnDimension', 'relatedConceptOnDimension'])
reltypes = ['qb', 'conceptUsage', 'relatedConceptUsage', 'resourceOnDimension', 'conceptOnDimension', 'relatedConceptOnDimension', 'crossSameas']


@celery.task
def gen_related_ds():
    log =  logging.getLogger(__name__)
    log.info("Generate related datasets")
    red = redis.Redis(connection_pool=redis_pool)
    related_ds = {}

    for rel_type in reltypes:
        related_ds[rel_type] = []
        root = f'related:{rel_type!s}:'
        for key in red.scan_iter(match=f'related:{rel_type!s}:*'):
            token = key[len(root):].replace('_', ':', 1)  # common element
            #log.info(f'{rel_type} - {key} - {token}')
            related_dist = set()
            for sameas_iri in sameAsIndex.lookup(token):
                related_dist.update(red.smembers(related_key(rel_type, sameas_iri)))  # these are related by sameAs of token
            all_related = set()
            for distr_iri in related_dist:
                all_related.update(red.smembers(f'distrds:{distr_iri}'))
            if len(all_related) > 1:  # do not consider sets on one candidate for conciseness
                related_ds[rel_type].append({'iri': token, 'related': list(all_related)})

    try:
        _, mongo_db = get_mongo()
        mongo_db.related.delete_many({})
        mongo_db.related.insert(related_ds)
        log = logging.getLogger(__name__)
        log.info(f'Successfully stored related datasets')
        log.debug(related_ds)
    except DocumentTooLarge:
        logging.getLogger(__name__).exception('Failed to store related datasets')

    del related_ds['_id']
    red.set('shouldQuery', 0)
    return related_ds


@celery.task
def finalize_sameas():
    log =  logging.getLogger(__name__)
    log.info("Finalize sameAs index")
    sameAsIndex.finalize()
    #skos not needed - not transitive
    log.info("Successfully finalized sameAs index")


@celery.task
def cache_labels():
    log =  logging.getLogger(__name__)
    log.info("Cache labels in mongo")
    labels = export_labels()
    _, mongo_db = get_mongo()
    mongo_db.labels.delete_many({})
    try:
        for iri in labels.keys():
            entry = labels[iri]
            entry['_id'] = iri
            mongo_db.labels.insert(entry)
        log.info("Successfully stored labels")
    except DocumentTooLarge:
        log.exception('Failed to cache labels')


def iter_subjects_objects(generic_analysis):
    for initial_iri in set(iri for iri in generic_analysis['generic']['subjects']).union(set(iri for iri in generic_analysis['generic']['objects'])):
        for iri in sameAsIndex.lookup(initial_iri):
            yield iri


def iter_generic(mongo_db):
    for doc in mongo_db.dsanalyses.find({'generic': {'$exists': True}}):
        yield doc


@celery.task
def ruian_reference():
    log = logging.getLogger(__name__)
    log.info("Look for RUIAN references")
    _, mongo_db = get_mongo()
    ruian_references = set()
    for doc in iter_generic(mongo_db):
        ds_ruian_references = set()
        for initial_iri in iter_subjects_objects(doc):
            for iri in sameAsIndex.lookup(initial_iri):
                if iri.startswith('https://linked.cuzk.cz/resource/ruian/'):
                    ruian_references.add(iri)
                    ds_ruian_references.add(iri)
        doc['ruian'] = list(ds_ruian_references)
        mongo_db.dsanalyses.update_one({"_id": doc['_id']}, {'$set': doc})
    log.info(f'RUIAN references: {len(list(ruian_references))}')
    inspector = RuianInspector()
    inspector.process_references(ruian_references)


def report_relationship(red, rel_type, resource_iri, distr_iri):
    key = related_key(rel_type, resource_iri)
    red.sadd(key, distr_iri)


@celery.task
def concept_usage():
    log = logging.getLogger(__name__)
    log.info("Concept usage")
    _, mongo_db = get_mongo()
    dsdistr, _ = ds_distr()
    counter = 0
    red = redis.Redis(connection_pool=redis_pool)
    ddr_types = ddrIndex.types()
    for doc in iter_generic(mongo_db):
        # z profilu najit vsechna s & o resources a podivat se, zda to neni skos Concept
        ds_iri = doc['ds_iri']
        #log.info(ds_iri)
        distr_iri = red.srandmember(f'{dsdistr}:{ds_iri}')

        resource_iri_cache = set()
        with red.pipeline() as pipe:
            for resource_iri in iter_subjects_objects(doc):
                if resource_iri in resource_iri_cache:
                    continue
                else:
                    resource_iri_cache.add(resource_iri)
                # type (broad / narrow apod.)
                # indexuji T -> (a, b); mam jedno z (a, b)
                # log.info(iri)
                if conceptIndex.is_concept(resource_iri):
                    # pouzit koncept (polozka ciselniku)
                    report_relationship(pipe, 'conceptUsage', resource_iri, distr_iri)
                    counter = counter + 1

                    for token in ddr_types:
                        for skos_resource_iri in ddrIndex.lookup(token, resource_iri):
                            for final_resource_iri in sameAsIndex.lookup(skos_resource_iri):
                                # pouzit related concept
                                report_relationship(pipe, 'relatedConceptUsage', final_resource_iri, distr_iri)
                                counter = counter + 1
            pipe.execute()

    log.info(f'Found relationships: {counter}')


@celery.task
def concept_definition():
    count = 0
    _, mongo_db = get_mongo()
    dsdistr, _ = ds_distr()
    log = logging.getLogger(__name__)
    log.info('Find datasets with information about concepts (codelists)')
    red = redis.Redis(connection_pool=redis_pool)
    with red.pipeline() as pipe:
        for concept in conceptIndex.iter_concepts():
            for resource_iri in sameAsIndex.lookup(concept):
                for doc in mongo_db.dsanalyses.find({'generic.subjects': resource_iri}):
                    ds_iri = doc['ds_iri']
                    distr_iri = red.srandmember(f'{dsdistr}:{ds_iri}')
                    for rel_type in ['conceptUsage', 'relatedConceptUsage', 'conceptOnDimension', 'relatedConceptOnDimension']:
                        report_relationship(pipe, rel_type, resource_iri, distr_iri)
                        count = count + 1
        pipe.execute()
    log.info(f'Found {count} relationship candidates')


@celery.task
def cross_dataset_sameas():
    _, mongo_db = get_mongo()
    dsdistr, _ = ds_distr()
    red = redis.Redis(connection_pool=redis_pool)
    for generic in iter_generic(mongo_db):
        ds_iri = generic['ds_iri']
        for distr_iri in red.sscan_iter(f'{dsdistr}:{ds_iri}'):
            for resource in red.lrange(pure_subject(distr_iri), 0, -1):
                for iri in sameAsIndex.lookup(resource):
                    report_relationship(red, 'crossSameas', iri, distr_iri)


@celery.task
def data_driven_relationships():
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    log.info('Data driven relationships')
    # projet z DSD vsechny dimenze a miry, zda to neni concept
    rel_types = ddrIndex.types()
    report = []
    for resource, distr_iri in dsdIndex.resources_on_dimension():
        for resource_iri in sameAsIndex.lookup(resource):
            # report resource na dimenzi
            if conceptIndex.is_concept(resource_iri):
                # report concept na dimenzi
                for token in rel_types:
                    for skos_resource_iri in ddrIndex.lookup(token, resource_iri):
                        # log.info(f'SKOS resource: {skos_resource_iri}')
                        if isinstance(skos_resource_iri, int):
                            continue
                        if len(skos_resource_iri) == 0:
                            continue
                        if isinstance(skos_resource_iri, list):
                            skos_resource_iri = skos_resource_iri[0]
                        for final_resource_iri in sameAsIndex.lookup(str(skos_resource_iri)):
                            # report related concept na dimenzi
                            report.append(('relatedConceptOnDimension', final_resource_iri, distr_iri))
                report.append(('conceptOnDimension', resource_iri, distr_iri))
            report.append(('resourceOnDimension', resource_iri, distr_iri))
    log.info(f'Report {len(report)} relationship candidates')
    with red.pipeline() as pipe:
        for (rel_type, resource_iri, distr_iri) in report:
            report_relationship(pipe, rel_type, resource_iri, distr_iri)
        pipe.execute()

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
