import json
import logging
from collections import defaultdict

import redis
from bson.json_util import dumps as dumps_bson
from pymongo.errors import DocumentTooLarge, OperationFailure

from tsa.enricher import AbstractEnricher, NoEnrichment
from tsa.extensions import mongo_db, redis_pool, sameAsIndex
from tsa.redis import sanitize_key

supported_languages = ["cs", "en"]
enrichers = [e() for e in AbstractEnricher.__subclasses__()]
reltypes = reltypes = ['qb', 'conceptUsage', 'relatedConceptUsage', 'resourceOnDimension', 'conceptOnDimension', 'relatedConceptOnDimension'] #sum((analyzer.relations for analyzer in AbstractAnalyzer.__subclasses__() if 'relations' in analyzer.__dict__), [])


def query_dataset(iri):
    return {
        "related": query_related(iri),
        "profile": query_profile(iri)
    }

def get_all_related():
    for r in mongo_db.related.find({}):
        return r


def query_related(ds_iri):
    log = logging.getLogger(__name__)
    log.info(f'Query related: {ds_iri}')
    try:
        all_related = get_all_related()
        #out = {}
        #for reltype in reltypes:
        #    out[reltype] = dict()
        #    for item in all_related[reltype]:
        #        token = item['iri']
        #        related = item['related']
        #        if ds_iri in related:
        #            out[reltype][token] = related
        #            out[reltype][token].remove(ds_iri)
        #return out

        ### untested ###
        out = defaultdict(list)
        for reltype in reltypes:
            log.info(reltype)
            #log.info(all_related[reltype])
            for item in all_related[reltype]:
                token = item['iri']
                related = item['related']
                #related.remove(ds_iri)
                if ds_iri in related:
                    log.info('JACKPOT!!!')
                    log.info(item)
                    related.remove(ds_iri)  # make sure it's not out of the mongo doc!
                    for related_ds_iri in related:
                        log.debug(related_ds_iri)
                        obj = {'type': reltype, 'common': token}

                        for sameas_iri in sameAsIndex.lookup(token):
                            for enricher in enrichers:
                                try:
                                    obj[enricher.token] = enricher.enrich(sameas_iri)
                                except NoEnrichment:
                                    pass

                        out[related_ds_iri].append(obj)
                #elif (len(related) > 0) and (ds_iri not in related):
                #        for iri in related:
                #            out[iri].append({'type': reltype, 'common': token})

                #for iri in related:
                #    out[iri].append({'type': reltype, 'common': token})
        out_with_labels = {}
        for iri in out:
            out_with_labels[iri] = {
                'label': create_labels(iri, supported_languages),
                'details': out[iri]
            }
        return out_with_labels

    except TypeError:
        log.exception('Failed to query related')
        return {}


def query_profile(ds_iri):
    log = logging.getLogger(__name__)

    #parse = urlparse(ds_iri)
    #path = quote(parse.path)
    #ds_iri = f'{parse.scheme}://{parse.netloc}{path}'

    log.info(f'iri: {ds_iri}')
    analyses = mongo_db.dsanalyses.find({'ds_iri': ds_iri})
    log.info("Retrieved analyses")
    json_str = dumps_bson(analyses)
    analyses = json.loads(json_str)
    #log.info(analyses)

    #so far, so good
    analysis = {}
    for a in analyses:
        log.info('...')
        del a['_id']
        del a['ds_iri']
        del a['batch_id']
        for k in a.keys():
            analysis[k] = a[k]

    log.info(analysis.keys())
    if len(analysis.keys()) == 0:
        log.error(f'Missing analysis for {ds_iri}')
        return {}

    #key = f'dsanalyses:{ds_iri}'
    #red = redis.Redis(connection_pool=redis_pool)
    #analysis = json.loads(red.get(key))  # raises TypeError if key is missing

    output = {}
    output["triples"] = analysis["generic"]["triples"]

    output["classes"] = []
    log.info(json.dumps(analysis["generic"]))
    for x in analysis["generic"]["classes"]:
        klaz = x["iri"]
        label = create_labels(klaz, supported_languages)
        output["classes"].append({'iri': klaz, 'label': label})

    output["predicates"] = analysis["generic"]["predicates"]

    #should work, but untested
    output["concepts"] = []
    if "concepts" in analysis["skos"]:
        for x in analysis["skos"]["concepts"]:
            concept = x['iri']
            output["concepts"].append({
                'iri': concept,
                'label': create_labels(concept, supported_languages)
            })

    #likely not working
    #output["codelists"] = set()
    #for o in analysis["generic"]["external"]["not_subject"]:
    #    for c in red.smembers(codelist_key(o)):  # codelists - datasets, that contain o as a subject
    #        output["codelists"].add(c)
    #output["codelists"] = list(output["codelists"])


    #should work, but untested
    output["schemata"] = []
    for x in analysis["skos"]["schema"]:
        output["schemata"].append({
            'iri': x['iri'],
            'label': create_labels(x['iri'], supported_languages)
        })

    #new
    output["datasets"] = []
    for ds in analysis["cube"]["datasets"]:
        dimensions = []
        for dim in ds['dimensions']:
            resources = []
            for res in dim['resources']:
                resources.append({
                    'iri': res,
                    'label': create_labels(res, supported_languages)
                })
            dimensions.append({
              'iri': dim['dimension'],
              'label': create_labels(dim['dimension'], supported_languages),
              'resources': resources
            })
        measures = []
        for measure in ds['measures']:
            measures.append({
                'iri': measure,
                'label': create_labels(measure, supported_languages)
            })
        output["datasets"].append(
            {
                'iri': ds['iri'],
                'dimensions': dimensions,
                'measures': measures,
                'label': create_labels(ds['iri'], supported_languages)
            }
        )

    #old
    #dimensions, measures, resources_on_dimension = set(), set(), defaultdict(list)
    #datasets = analysis["cube"]["datasets"]
    #for x in datasets:
    #    ds = x['iri']
    #    try:
    #        dimensions.update([ y["dimension"] for y in x["dimensions"]])
    #        for y in x["dimensions"]:
    #            resources_on_dimension[y["dimension"]] = y["resources"]
    #    except TypeError:
    #        dimensions.update(x["dimensions"])
    #    measures.update(x["measures"])
    #
    #output["dimensions"], output["measures"] = [], []
    #for d in dimensions:
    #    output["dimensions"].append({
    #        'iri': d,
    #        'label': create_labels(d, supported_languages),
    #        'resources': resources_on_dimension[d],
    #    })
    #
    #for m in measures:
    #    output["measures"].append({
    #        'iri': m,
    #        'label': create_labels(m, supported_languages)
    #    })

    return output


def create_labels(ds_iri, tags):
    labels = query_label(ds_iri)

    label = {}
    for tag in tags:
        label[tag] = ""

    available = set()

    if "default" in labels.keys():
        for tag in tags:
            label[tag] = labels["default"]
            available.add(tag)

    for tag in tags:
        if tag in labels.keys():
            label[tag] = labels[tag]
            available.add(tag)

    available = list(available)
    if len(available) > 0:
        for tag in tags:
            if len(label[tag]) == 0:
                label[tag] = label[available[0]]  # put anything there
    else:
        log = logging.getLogger(__name__)
        log.error(f'Missing labels for {ds_iri}')

    return label


def sanitize_label_iri_for_mongo(iri):
    return '+'.join(iri.split('.'))

def query_label(ds_iri):
    #LABELS: key = f'dstitle:{ds!s}:{t.language}' if t.language is not None else f'dstitle:{ds!s}'
    #red.set(key, title)
    ds_iri = sanitize_key(ds_iri)
    red = redis.Redis(connection_pool=redis_pool)
    result = {}
    #for x in red.scan_iter(match=f'label:{ds_iri!s}*'): #red.keys(f'label:{ds_iri!s}*'):  #FIXME
    for lang in ['cs', 'en']:
        key_lang = f'label:{ds_iri!s}:{lang}'  #FIXME
        key_default = f'label:{ds_iri!s}'
        if red.exists(key_lang):#x.startswith(prefix_lang):
            title = red.get(key_lang)
            result[lang] = title
        elif red.exists(key_default):
            result['default'] = red.get(key_default)
    return result


def export_labels():
    #key: label:{ds_iri}:{language}, ds_iri=https://
    prefix = 'label:'
    out = defaultdict(dict)
    red = redis.Redis(connection_pool=redis_pool)
    for x in red.keys(f'{prefix}*'):
        if ':' in x[len(prefix):]:
            (ds_iri, language_code) = x[len(prefix):].split(':')
        else:
            language_code = 'default'
            ds_iri = x[len(prefix):]
        out[sanitize_label_iri_for_mongo(ds_iri)][sanitize_label_iri_for_mongo(language_code)] = red.get(x)
    return out


def import_labels(labels):
    red = redis.Redis(connection_pool=redis_pool)
    with red.pipeline() as pipe:
        for ds_iri in labels.keys():
            for language_code in labels[ds_iri].keys():
                if language_code == 'default':
                    key = f'label:{ds_iri}'
                else:
                    key = f'label:{ds_iri}:{language_code}'
                pipe.set(key, labels[ds_iri][language_code])
        pipe.execute()


def export_related():
    return get_all_related()


def export_profile():
    for analysis in mongo_db.dsanalyses.find({}):
        yield analysis


def import_related(related):
    try:
        mongo_db.related.delete_many({})
        mongo_db.related.insert(related)
    except DocumentTooLarge:
        logging.getLogger(__name__).exception('Failed to store related datasets')


def import_profiles(profiles):
    mongo_db.dsanalyses.delete_many({})
    log = logging.getLogger(__name__)
    for analysis in profiles:
        try:
            mongo_db.dsanalyses.insert_one(analysis)
        except DocumentTooLarge:
            iri = analysis['ds_iri']
            log.exception(f'Failed to store analysis for ds: {iri}')
        except OperationFailure:
            log.exception(f'Operation failure')
    log.info('Stored analyses')


def export_interesting():
    for lst in mongo_db.interesting.find({}):
        return lst


def import_interesting(interesting_datasets):
    mongo_db.interesting.delete_many({})
    mongo_db.interesting.insert(list(interesting_datasets))


def list_datasets():
    listed = set()
    datasets = []
    for profile in export_profile():
        if profile['ds_iri'] not in listed:
            listed.add(profile['ds_iri'])
            datasets.append({'iri': profile['ds_iri'], 'label': query_label(profile['ds_iri'])})
    return datasets
