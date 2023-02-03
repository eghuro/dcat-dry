"""Celery tasks for querying."""
import json
import logging
import uuid
from json import JSONEncoder

import redis
from pymongo.errors import DocumentTooLarge, OperationFailure
from sqlalchemy.orm import Session

from tsa.celery import celery
from tsa.db import db_session
from tsa.ddr import concept_index, dsd_index, ddr_index
from tsa.model import DatasetDistribution, Relationship, PureSubject
from tsa.extensions import mongo_db, redis_pool, db
from tsa.sameas import same_as_index
from tsa.notification import message_to_mattermost
from tsa.redis import EXPIRATION_CACHED
from tsa.redis import sanitize_key
from tsa.report import export_labels
from tsa.ruian import RuianInspector
from tsa.tasks.common import SqlAlchemyTask

# === ANALYSIS (PROFILE) ===


def _gen_iris(red, log):
    root = "analyze:"

    for d in db_session.query(DatasetDistribution):
        distr_iri = d.distr_iri
        key1 = f"{root}{sanitize_key(distr_iri)}"
        analysis_json_string = red.get(key1)
        if analysis_json_string is None:
            continue
        analysis = json.loads(analysis_json_string)
        if "analysis" in analysis.keys():
            content = analysis["analysis"]
            if "iri" in analysis.keys():
                iri = analysis["iri"]
                yield iri, content
            elif "endpoint" in analysis.keys() and "graph" in analysis.keys():
                yield analysis[
                    "endpoint"
                ], content  # this is because named graph is not extracted from DCAT
            else:
                log.error("Missing iri and endpoint/graph")
        else:
            log.error("Missing content")


@celery.task(base=SqlAlchemyTask)
def compile_analyses():
    log = logging.getLogger(__name__)
    log.info("Compile analyzes")
    red = redis.Redis(connection_pool=redis_pool)
    batch_id = str(uuid.uuid4())

    dataset_iris = set()

    for distr_iri, content in _gen_iris(red, log):
        log.debug(distr_iri)
        key = f"analysis:{batch_id}:{sanitize_key(distr_iri)}"
        for ds_iri in db.query(DatasetDistribution.ds).filter_by(distr=distr_iri).distinct():
            if ds_iri is None:
                with red.pipeline() as pipe:
                    pipe.rpush(key, json.dumps(content))
                    pipe.expire(key, EXPIRATION_CACHED)
                    pipe.execute()
                continue
            with red.pipeline() as pipe:
                pipe.rpush(key, json.dumps(content))
                pipe.expire(key, EXPIRATION_CACHED)
                pipe.sadd("relevant_distr", distr_iri)
                pipe.expire("relevant_distr", EXPIRATION_CACHED)
                pipe.execute()
            with Session(db) as session:
                session.add(DatasetDistribution(ds=ds_iri, distr=distr_iri))
                session.commit()
            dataset_iris.add(ds_iri)

    return list(dataset_iris), batch_id


def gen_analyses(batch_id, dataset_iris, red):
    log = logging.getLogger(__name__)
    log.info(f"Generate analyzes ({len(dataset_iris)})")
    for ds_iri in dataset_iris:
        for distr_iri in db.query(DatasetDistribution.distr).filter_by(ds=ds_iri).distinct():
            key_in = f"analysis:{batch_id}:{sanitize_key(distr_iri)}"
            for analyses_json in [
                json.loads(analysis_json_string)
                for analysis_json_string in red.lrange(key_in, 0, -1)
            ]:
                for analysis_json in analyses_json:  # flatten
                    for key in analysis_json.keys():  # 1 element
                        analysis = {"ds_iri": ds_iri}
                        analysis[key] = analysis_json[key]  # merge dicts
                        yield analysis


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def store_to_mongo(dataset_iris_and_batch_id):
    dataset_iris, batch_id = dataset_iris_and_batch_id
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    log.info("Cleaning mongo")
    mongo_db.dsanalyses.delete_many({})
    insert_count, gen_count = 0, 0
    for analysis in gen_analyses(batch_id, dataset_iris, red):
        gen_count = gen_count + 1
        try:
            mongo_db.dsanalyses.insert_one(analysis)
        except DocumentTooLarge:
            iri = analysis["ds_iri"]
            log.exception(
                f"Failed to store analysis for {batch_id} (dataset_iris: {iri})"
            )
        except OperationFailure:
            log.exception("Operation failure")
        else:
            insert_count = insert_count + 1
    log.info(f"Stored analyses ({insert_count}/{gen_count})")

    return dataset_iris


# == INDEX ==
# reltypes = sum((analyzer.relations for analyzer in AbstractAnalyzer.__subclasses__() if 'relations' in analyzer.__dict__), [])
# reltypes.extend(['skosqb', 'conceptUsage', 'relatedConceptUsage', 'resourceOnDimension', 'conceptOnDimension', 'relatedConceptOnDimension'])
reltypes = [
    "qb",
    "conceptUsage",
    "relatedConceptUsage",
    "resourceOnDimension",
    "conceptOnDimension",
    "relatedConceptOnDimension",
    "crossSameas",
]


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def gen_related_ds():
    log = logging.getLogger(__name__)
    log.warning("Generate related datasets")
    red = redis.Redis(connection_pool=redis_pool)
    related_ds = []

    interesting_datasets = set()

    for rel_type in reltypes:
        for token in db_session.query(Relationship.group).filter_by(type=rel_type).distinct():
            log.debug(f"type: {rel_type}, token: {token}")
            related_dist = set()
            for sameas_iri in same_as_index.lookup(token):
                for rel_dist in db.query(Relationship.candidate).filter_by(type=rel_type, group=sameas_iri):
                    related_dist.add(rel_dist)
                # these are related by sameAs of token
            all_related = set()
            for distr_iri in related_dist:
                for ds in db.query(DatasetDistribution.ds).filter_by(dist=distr_iri):
                    all_related.add(ds)
            if (len(all_related) > 1):
                # do not consider sets on one candidate for conciseness
                related_ds.append(
                    {"iri": token, "related": list(all_related), "type": rel_type}
                )
                interesting_datasets.update(all_related)

    try:
        mongo_db.related.delete_many({})
        if len(related_ds) > 0:
            mongo_db.related.insert_many(related_ds)

        mongo_db.interesting.delete_many({})
        mongo_db.interesting.insert_one({"iris": list(interesting_datasets)})

        log = logging.getLogger(__name__)
        log.info(
            f"Successfully stored related datasets, interesting: {len(interesting_datasets)}"
        )
        # log.debug(related_ds)
    except DocumentTooLarge:
        logging.getLogger(__name__).exception("Failed to store related datasets")

    red.set("shouldQuery", 0)
    # message_to_mattermost("Done!")


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def finalize_sameas():
    log = logging.getLogger(__name__)
    log.info("Finalize sameAs index")
    message_to_mattermost("Finalize sameAs index - query pipeline started")
    same_as_index.finalize()
    # skos not needed - not transitive
    log.info("Successfully finalized sameAs index")


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def cache_labels():
    log = logging.getLogger(__name__)
    log.info("Cache labels in mongo")
    labels = export_labels()
    mongo_db.labels.delete_many({})
    try:
        for (iri, entry) in labels.items():
            entry["_id"] = iri
            mongo_db.labels.insert(entry)
        log.info("Successfully stored labels")
    except DocumentTooLarge:
        log.exception("Failed to cache labels")


def iter_subjects_objects(generic_analysis):
    for initial_iri in set(
        iri for iri in generic_analysis["generic"]["subjects"]
    ).union(set(iri for iri in generic_analysis["generic"]["objects"])):
        for iri in same_as_index.lookup(initial_iri):
            yield iri


def iter_generic(mongo_db):
    for doc in mongo_db.dsanalyses.find({"generic": {"$exists": True}}):
        yield doc


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def ruian_reference():
    log = logging.getLogger(__name__)
    log.info("Look for RUIAN references")
    ruian_references = set()
    for doc in iter_generic(mongo_db):
        ds_ruian_references = set()
        for initial_iri in iter_subjects_objects(doc):
            for iri in same_as_index.lookup(initial_iri):
                if iri.startswith("https://linked.cuzk.cz/resource/ruian/"):
                    ruian_references.add(iri)
                    ds_ruian_references.add(iri)
        doc["ruian"] = list(ds_ruian_references)
        mongo_db.dsanalyses.update_one({"_id": doc["_id"]}, {"$set": doc})
    log.info(f"RUIAN references: {len(list(ruian_references))}")
    RuianInspector.process_references(ruian_references)


def report_relationship(session, rel_type, resource_iri, distr_iri):
    session.add(Relationship(type=rel_type, group=resource_iri, candidate=distr_iri))


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def concept_usage():
    log = logging.getLogger(__name__)
    log.info("Concept usage")

    counter = 0
    ddr_types = ddr_index.types()
    for doc in iter_generic(mongo_db):
        # z profilu najit vsechna s & o resources a podivat se, zda to neni skos Concept
        ds_iri = doc["ds_iri"]
        distr_iri = db.query(DatasetDistribution.distr).filter_by(ds=ds_iri).first()

        resource_iri_cache = set()
        with Session(db) as session:
            for resource_iri in iter_subjects_objects(doc):
                if resource_iri in resource_iri_cache:
                    continue
                resource_iri_cache.add(resource_iri)
                # type (broad / narrow apod.)
                # indexuji T -> (a, b); mam jedno z (a, b)
                if concept_index.is_concept(resource_iri):
                    # pouzit koncept (polozka ciselniku)
                    report_relationship(session, "conceptUsage", resource_iri, distr_iri)
                    counter = counter + 1

                    for token in ddr_types:
                        for skos_resource_iri in ddr_index.lookup(token, resource_iri):
                            for final_resource_iri in same_as_index.lookup(
                                skos_resource_iri
                            ):
                                # pouzit related concept
                                report_relationship(
                                    session,
                                    "relatedConceptUsage",
                                    final_resource_iri,
                                    distr_iri,
                                )
                                counter = counter + 1
            session.commit()

    log.info(f"Found relationships: {counter}")


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def concept_definition():
    count = 0
    log = logging.getLogger(__name__)
    log.info("Find datasets with information about concepts (codelists)")
    with Session(db) as session:
        for concept in concept_index.iter_concepts():
            for resource_iri in same_as_index.lookup(concept):
                for doc in mongo_db.dsanalyses.find({"generic.subjects": resource_iri}):
                    ds_iri = doc["ds_iri"]
                    distr_iri = db.query(DatasetDistribution.distr).filter_by(ds=ds_iri).first()
                    for rel_type in [
                        "conceptUsage",
                        "relatedConceptUsage",
                        "conceptOnDimension",
                        "relatedConceptOnDimension",
                    ]:
                        report_relationship(session, rel_type, resource_iri, distr_iri)
                        count = count + 1
        session.commit()
    log.info(f"Found {count} relationship candidates")


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def cross_dataset_sameas():
    with Session(db) as session:
        # pure_subject -> select PureSubject with respective distribution iri
        for generic in iter_generic(mongo_db):
            ds_iri = generic["ds_iri"]
            for distr_iri in db.query(DatasetDistribution.distr).filter_by(ds=ds_iri).distinct():
                for resource in db.query(PureSubject.subject_iri).filter_by(distribution_iri=distr_iri).distinct():
                    for iri in same_as_index.lookup(resource):
                        report_relationship(session, "crossSameas", iri, distr_iri)
        session.commit()


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def data_driven_relationships():
    log = logging.getLogger(__name__)
    log.info("Data driven relationships")
    # projet z DSD vsechny dimenze a miry, zda to neni concept
    
    rel_types = ddr_index.types()
    report = []
    for resource, distr_iri in dsd_index.resources_on_dimension():
        for resource_iri in same_as_index.lookup(resource):
            # report resource na dimenzi
            if concept_index.is_concept(resource_iri):
                # report concept na dimenzi
                for token in rel_types:
                    for skos_resource_iri in ddr_index.lookup(token, resource_iri):
                        if isinstance(skos_resource_iri, int):
                            continue
                        if len(skos_resource_iri) == 0:
                            continue
                        if isinstance(skos_resource_iri, list):
                            skos_resource_iri = skos_resource_iri[0]
                        for final_resource_iri in same_as_index.lookup(
                            str(skos_resource_iri)
                        ):
                            # report related concept na dimenzi
                            report.append(
                                (
                                    "relatedConceptOnDimension",
                                    final_resource_iri,
                                    distr_iri,
                                )
                            )
                report.append(("conceptOnDimension", resource_iri, distr_iri))
            report.append(("resourceOnDimension", resource_iri, distr_iri))
    log.info(f"Report {len(report)} relationship candidates")
    with Session(db) as session:
        for (rel_type, resource_iri, distr_iri) in report:
            report_relationship(session, rel_type, resource_iri, distr_iri)
        session.commit()


# ## MISC ###


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super(JSONEncoder, self).default(obj)
