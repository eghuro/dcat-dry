import json
import logging
from collections import defaultdict

import redis
from rdflib import Graph
from rdflib.exceptions import ParserError
from rdflib.plugins.stores.sparqlstore import SPARQLStore, _node_to_sparql
from bson.json_util import dumps as dumps_bson
from pymongo.errors import DocumentTooLarge, OperationFailure
from sqlalchemy import select
from sqlalchemy.orm import Session
from tsa.enricher import AbstractEnricher, NoEnrichment
from tsa.extensions import mongo_db, db
from tsa.sameas import same_as_index
from tsa.model import Label
from tsa.redis import sanitize_key
from tsa.robots import USER_AGENT, session as online_session

supported_languages = ["cs", "en"]
enrichers = [e() for e in AbstractEnricher.__subclasses__()]
reltypes = [
    "qb",
    "conceptUsage",
    "relatedConceptUsage",
    "resourceOnDimension",
    "conceptOnDimension",
    "relatedConceptOnDimension",
    "crossSameas",
]


def query_dataset(iri):
    return {
        "related": query_related(iri),
        "profile": query_profile(iri),
        "label": query_label(iri),
    }


def get_all_related():
    all_related = defaultdict(list)
    for item in mongo_db.related.find({}):
        record = {}
        record["iri"] = item["iri"]
        record["related"] = item["related"]
        all_related[item["type"]].append(record)
    return all_related


def query_related(ds_iri):
    log = logging.getLogger(__name__)
    log.info("Query related: %s", ds_iri)
    try:
        all_related = get_all_related()
        # out = {}
        # for reltype in reltypes:
        #    out[reltype] = dict()
        #    for item in all_related[reltype]:
        #        token = item['iri']
        #        related = item['related']
        #        if ds_iri in related:
        #            out[reltype][token] = related
        #            out[reltype][token].remove(ds_iri)
        # return out

        ### untested ###
        out = defaultdict(list)
        for reltype in reltypes:
            log.info(reltype)
            # log.info(all_related[reltype])
            for item in all_related[reltype]:
                token = item["iri"]
                related = item["related"]
                # related.remove(ds_iri)
                if ds_iri in related:
                    # log.info('JACKPOT!!!')
                    # log.info(item)
                    related.remove(ds_iri)  # make sure it's not out of the mongo doc!
                    for related_ds_iri in related:
                        # log.debug(related_ds_iri)
                        obj = {"type": reltype, "common": token}

                        for sameas_iri in same_as_index.lookup(token):
                            for enricher in enrichers:
                                try:
                                    obj[enricher.token] = enricher.enrich(sameas_iri)
                                except NoEnrichment:
                                    pass

                        out[related_ds_iri].append(obj)
                # elif (len(related) > 0) and (ds_iri not in related):
                #        for iri in related:
                #            out[iri].append({'type': reltype, 'common': token})

                # for iri in related:
                #    out[iri].append({'type': reltype, 'common': token})
        out_with_labels = {}
        for iri in out:
            out_with_labels[iri] = {
                "label": create_labels(iri, supported_languages),
                "details": out[iri],
            }
        return out_with_labels

    except TypeError:
        log.exception("Failed to query related")
        return {}


def query_profile(ds_iri):
    log = logging.getLogger(__name__)

    # parse = urlparse(ds_iri)
    # path = quote(parse.path)
    # ds_iri = f'{parse.scheme}://{parse.netloc}{path}'

    # log.info('iri: %s', ds_iri)
    analyses = mongo_db.dsanalyses.find({"ds_iri": ds_iri})
    # log.info("Retrieved analyses")
    json_str = dumps_bson(analyses)
    analyses = json.loads(json_str)
    # log.info(analyses)

    # so far, so good
    analysis_out = {}
    for analysis in analyses:
        log.info("...")
        del analysis["_id"]
        del analysis["ds_iri"]
        for k in analysis.keys():
            analysis_out[k] = analysis[k]

    if len(analysis_out.keys()) == 0:
        log.error("Missing analysis_out for %s", ds_iri)
        return {}

    # key = f'dsanalyses:{ds_iri}'
    # red = redis.Redis(connection_pool=redis_pool)
    # analysis_out = json.loads(red.get(key))  # raises TypeError if key is missing

    output = {}
    output["triples"] = analysis_out["generic"]["triples"]

    output["classes"] = []
    for class_analysis in analysis_out["generic"]["classes"]:
        class_iri = class_analysis["iri"]
        label = create_labels(class_iri, supported_languages)
        output["classes"].append({"iri": class_iri, "label": label})

    output["predicates"] = analysis_out["generic"]["predicates"]

    # should work, but untested
    output["concepts"] = []
    if "concepts" in analysis_out["skos"]:
        for class_analysis in analysis_out["skos"]["concepts"]:
            concept = class_analysis["iri"]
            output["concepts"].append(
                {"iri": concept, "label": create_labels(concept, supported_languages)}
            )

    # likely not working
    # output["codelists"] = set()
    # for o in analysis_out["generic"]["external"]["not_subject"]:
    #    for c in red.smembers(codelist_key(o)):  # codelists - datasets, that contain o as analysis subject
    #        output["codelists"].add(c)
    # output["codelists"] = list(output["codelists"])

    # should work, but untested
    output["schemata"] = []
    for class_analysis in analysis_out["skos"]["schema"]:
        output["schemata"].append(
            {
                "iri": class_analysis["iri"],
                "label": create_labels(class_analysis["iri"], supported_languages),
            }
        )

    # new
    output["datasets"] = []
    for dataset in analysis_out["cube"]["datasets_queried"]:
        dimensions = []
        for dim in dataset["dimensions"]:
            resources = []
            for res in dim["resources"]:
                resources.append(
                    {"iri": res, "label": create_labels(res, supported_languages)}
                )
            dimensions.append(
                {
                    "iri": dim["dimension"],
                    "label": create_labels(dim["dimension"], supported_languages),
                    "resources": resources,
                }
            )
        measures = []
        for measure in dataset["measures"]:
            measures.append(
                {"iri": measure, "label": create_labels(measure, supported_languages)}
            )
        output["datasets"].append(
            {
                "iri": dataset["iri"],
                "dimensions": dimensions,
                "measures": measures,
                "label": create_labels(dataset["iri"], supported_languages),
            }
        )

    # old
    # dimensions, measures, resources_on_dimension = set(), set(), defaultdict(list)
    # datasets = analysis_out["cube"]["datasets"]
    # for class_analysis in datasets:
    #    dataset = class_analysis['iri']
    #    try:
    #        dimensions.update([ y["dimension"] for y in class_analysis["dimensions"]])
    #        for y in class_analysis["dimensions"]:
    #            resources_on_dimension[y["dimension"]] = y["resources"]
    #    except TypeError:
    #        dimensions.update(class_analysis["dimensions"])
    #    measures.update(class_analysis["measures"])
    #
    # output["dimensions"], output["measures"] = [], []
    # for d in dimensions:
    #    output["dimensions"].append({
    #        'iri': d,
    #        'label': create_labels(d, supported_languages),
    #        'resources': resources_on_dimension[d],
    #    })
    #
    # for m in measures:
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
            if label[tag] is None:
                continue
            if len(label[tag]) == 0:
                label[tag] = label[available[0]]  # put anything there
    else:
        log = logging.getLogger(__name__)
        log.error("Missing labels for %s", ds_iri)

    return label


def sanitize_label_iri_for_mongo(iri):
    return "+".join(iri.split("."))


def query_label(ds_iri):
    print(f"Query label for {ds_iri}")
    result = defaultdict(set)
    with Session(db) as session:
        stmt = select(Label).where(iri=ds_iri)
        for label in session.scalars(stmt):
            if label.language_code in supported_languages:
                result[label.language_code].add(label.label)

    if len(result.keys()) == 0:
        logging.getLogger(__name__).warning(
            f"Fetching title for {ds_iri} from endpoint"
        )
        endpoint = "https://data.gov.cz/sparql"
        q = f"select ?label where {{<{ds_iri}> <http://purl.org/dc/terms/title> ?label}} LIMIT 10"
        store = SPARQLStore(
            endpoint, session=online_session, headers={"User-Agent": USER_AGENT}
        )
        graph = Graph(store=store)
        graph.open(endpoint)
        try:
            for row in graph.query(q):
                label = row["label"]
                try:
                    value, language = label.value, label.language
                    result[language] = value
                    result["default"] = value
                except AttributeError:
                    result["default"] = label
        except ParserError:
            logging.getLogger(__name__).exception(f"Failed to parse title for {ds_iri}")

    return result


def export_labels():
    out = defaultdict(defaultdict(set))
    with Session(db) as session:
        stmt = select(Label)
        for label in session.scalars(stmt):
            out[sanitize_label_iri_for_mongo(label.iri)][sanitize_label_iri_for_mongo(label.language_code)].add(label.label)
    return out


def import_labels(labels):
    with Session(db) as session:
        for ds_iri in labels.keys():
            for language_code in labels[ds_iri].keys():
                for value in labels[ds_iri][language_code]:
                    lang = language_code
                    if lang == 'default':
                        lang = None
                    label = Label(
                        iri = ds_iri,
                        language_code = lang,
                        label = value
                    )
                    session.add(label)
        session.commit()


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
        logging.getLogger(__name__).exception("Failed to store related datasets")


def import_profiles(profiles):
    mongo_db.dsanalyses.delete_many({})
    log = logging.getLogger(__name__)
    for analysis in profiles:
        try:
            mongo_db.dsanalyses.insert_one(analysis)
        except DocumentTooLarge:
            iri = analysis["ds_iri"]
            log.exception("Failed to store analysis for ds: %s", iri)
        except OperationFailure:
            log.exception("Operation failure")
    log.info("Stored analyses")


def export_interesting():
    for doc in mongo_db.interesting.find({}):
        return doc["iris"]


def import_interesting(interesting_datasets):
    mongo_db.interesting.delete_many({})
    mongo_db.interesting.insert({"iris": list(interesting_datasets)})


def list_datasets():
    logging.getLogger(__name__).info("List datasets report")
    listed = set()
    for profile in export_profile():
        if profile["ds_iri"] not in listed:
            listed.add(profile["ds_iri"])
    return list(listed)
