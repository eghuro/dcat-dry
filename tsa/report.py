import json
import logging
import redis

from collections import defaultdict
from functools import lru_cache

from tsa.analyzer import AbstractAnalyzer
from tsa.extensions import redis_pool, mongo_db
from tsa.redis import codelist as codelist_key


def query_dataset(iri):
    return {
        "related": query_related(iri),
        "profile": query_profile(iri)
    }

reltypes = sum((analyzer.relations for analyzer in AbstractAnalyzer.__subclasses__() if 'relations' in analyzer.__dict__), [])

@lru_cache()
def get_all_related():
    for r in mongo_db.related.find({}):
        return r


@lru_cache()
def query_related(ds_iri):
    log = logging.getLogger(__name__)
    try:
        all_related = get_all_related()
        out = {}
        for reltype in reltypes:
            out[reltype] = dict()
            for item in all_related[reltype]:
                token = item['iri']
                related = item['related']
                if ds_iri in related:
                    out[reltype][token] = related
                    out[reltype][token].remove(ds_iri)
        return out
    except TypeError:
        log.exception('Failed to query related')
        return {}


def query_profile(ds_iri):
    log = logging.getLogger(__name__)

    analyses = mongo_db.dsanalyses.find({'ds_iri': ds_iri})
    analysis = {}
    for a in analyses:
        del a['_id']
        del a['ds_iri']
        del a['batch_id']
        for k in a.keys():
            analysis[k] = a[k]

    #key = f'dsanalyses:{ds_iri}'
    red = redis.Redis(connection_pool=redis_pool)
    #analysis = json.loads(red.get(key))  # raises TypeError if key is missing

    supported_languages = ["cs", "en"]

    output = {}
    output["triples"] = analysis["generic"]["triples"]

    output["classes"] = []
    #log.info(json.dumps(analysis["generic"]))
    for x in analysis["generic"]["classes"]:
        label = create_labels(ds_iri, supported_languages)
        output["classes"].append({'iri': x['iri'], 'label': label})

    output["predicates"] = analysis["generic"]["predicates"]

    output["concepts"] = []
    if "concepts" in analysis["skos"]:
        for x in analysis["skos"]["concepts"]:
            concept = x['iri']
            output["concepts"].append({
                'iri': concept,
                'label': create_labels(concept, supported_languages)
            })

    #output["codelists"] = set()
    #for o in analysis["generic"]["external"]["not_subject"]:
    #    for c in red.smembers(codelist_key(o)):  # codelists - datasets, that contain o as a subject
    #        output["codelists"].add(c)
    #output["codelists"] = list(output["codelists"])

    output["schemata"] = []
    for x in analysis["skos"]["schema"]:
        output["schemata"].append({
            'iri': x['iri'],
            'label': create_labels(x['iri'], supported_languages)
        })

    dimensions, measures, resources_on_dimension = set(), set(), defaultdict(list)
    datasets = analysis["cube"]["datasets"]
    for x in datasets:
        ds = x['iri']
        try:
            dimensions.update([ y["dimension"] for y in x["dimensions"]])
            for y in x["dimensions"]:
                resources_on_dimension[y["dimension"]] = y["resources"]
        except TypeError:
            dimensions.update(x["dimensions"])
        measures.update(x["measures"])

    output["dimensions"], output["measures"] = [], []
    for d in dimensions:
        output["dimensions"].append({
            'iri': d,
            'label': create_labels(d, supported_languages),
            'resources': resources_on_dimension[d],
        })

    for m in measures:
        output["measures"].append({
            'iri': m,
            'label': create_labels(m, supported_languages)
        })

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


def query_label(ds_iri):
    #LABELS: key = f'dstitle:{ds!s}:{t.language}' if t.language is not None else f'dstitle:{ds!s}'
    #red.set(key, title)
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)
    result = {}
    for x in red.keys(f'dstitle:{ds_iri!s}*'):
        prefix_lang = f'dstitle:{ds_iri!s}:'
        if x.startswith(prefix_lang):
            language_code = x[len(prefix_lang):]
            title = red.get(x)
            result[language_code] = title
        else:
            result['default'] = red.get(x)
    return result