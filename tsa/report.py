import json
import logging
from collections import defaultdict

from rdflib import Graph
from rdflib.exceptions import ParserError
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from tsa.db import db_session
from tsa.enricher import AbstractEnricher, NoEnrichment
from tsa.sameas import same_as_index
from tsa.model import Label, Related, DatasetDistribution, Analysis
from tsa.net import RobotsBlock, RobotsRetry, Skip
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

def query_related(ds_iri):
    log = logging.getLogger(__name__)
    log.info("Query related: %s", ds_iri)
    out = defaultdict(list)
    sameAs = same_as_index.snapshot()
    # (token, ds_type) k danemu ds_iri
    for item in db_session.query(Related).filter_by(ds=ds_iri):
        # ostatni ds pro (token, ds_type)
        for related_ds in db_session.query(Related).filter(Related.token==item.token, Related.type==item.type, Related.ds != ds_iri):
            # gen_related_ds jiz proslo sameAs
            obj = {"type": related_ds.type, "common": related_ds.token}

            # enrichment
            for sameas_iri in sameAs[related_ds.token]:
                for enricher in enrichers:
                    try:
                        obj[enricher.token] = enricher.enrich(sameas_iri)
                    except NoEnrichment:
                        pass

            out[related_ds.ds].append(obj)
    out_with_labels = {}
    for iri in out:
        out_with_labels[iri] = {
            "label": create_labels(iri, supported_languages),
            "details": out[iri],
        }
    return out_with_labels


def query_profile(ds_iri):
    log = logging.getLogger(__name__)
    analysis_out = {}
    for dist in db_session.query(DatasetDistribution).filter_by(relevant=True, ds=ds_iri):
        for analysis in db_session.query(Analysis).filter_by(iri=dist.distr):
            analysis_out[analysis.analyzer] = analysis.data

    if len(analysis_out.keys()) == 0:
        log.error("Missing analysis_out for %s", ds_iri)
        return {}

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


def query_label(ds_iri):
    print(f"Query label for {ds_iri}")
    result = defaultdict(set)
    for label in db_session.query(Label).filter_by(iri=ds_iri):
        if label.language_code in supported_languages:
            result[label.language_code].add(label.label)

    if len(result.keys()) == 0:
        logging.getLogger(__name__).warning(
            f"Fetching title for {ds_iri} from endpoint"
        )
        endpoint = "https://data.gov.cz/sparql"
        q = f"select ?label where {{<{ds_iri}> <http://purl.org/dc/terms/title> ?label}} LIMIT 10"
        try:
            with RobotsBlock(endpoint):
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
        except (RobotsRetry, Skip):
            logging.getLogger(__name__).warning(f"Not allowed to fetch {ds_iri} from endpoint right now")

    result = dict(result)
    for key in result.keys():
        result[key] = list(result[key])
    return result

def convert_labels_for_json_export(out):
    for key1 in out.keys():
        out[key1]=dict(out[key1])
        for key2 in out[key1]:
            out[key1][key2] = list(out[key1][key2])
    return out

def export_labels():
    out = {}
    for label in db_session.query(Label):
        key1 = label.iri
        if key1 not in out.keys():
            out[key1] = defaultdict(set)
        out[key1][label.language_code].add(label.label)
    return convert_labels_for_json_export(out)


def import_labels(labels):
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
                db_session.add(label)
    try:
        db_session.commit()
    except:
        logging.getLogger(__name__).exception("Failed do commit, rolling back import labels")
        db_session.rollback()


def export_related():
    all_related = defaultdict(defaultdict(set))
    for item in db_session.query(Related):
        if item.ds1 == item.ds2:
            continue
        all_related[item.type][item.ds1].add(item.ds2)
    return all_related


def export_profile():
    analysis_out = {}
    for dist in db_session.query(DatasetDistribution).filter_by(relevant=True):
        for analysis in db_session.query(Analysis).filter_by(iri=dist.distr):
            analysis_out[analysis.analyzer] = analysis.data
    return analysis_out


def export_interesting():
    interesting = set()
    data = export_related()
    for key in data.keys():
        for ds in data[key].keys():
            if len(data[key][ds]) > 0:
                interesting.add(ds)
    # ds, ze existuje (token, type) in Related t.z. existuje (token, type, ds2) in Related, kde ds != ds2 


def list_datasets():
    logging.getLogger(__name__).info("List datasets report")
    listed = set()
    for profile in export_profile():
        if profile["ds_iri"] not in listed:
            listed.add(profile["ds_iri"])
    return list(listed)
