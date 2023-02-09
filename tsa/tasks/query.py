"""Celery tasks for querying."""
import logging
from json import JSONEncoder

import redis
from celery import chord
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import join

from tsa.celery import celery
from tsa.db import db_session
from tsa.ddr import concept_index, ddr_index, dsd_index
from tsa.extensions import redis_pool
from tsa.model import (
    Analysis,
    DatasetDistribution,
    Related,
    Relationship,
    SubjectObject,
)
from tsa.net import RobotsRetry
from tsa.ruian import RuianInspector
from tsa.sameas import same_as_index
from tsa.tasks.common import SqlAlchemyTask

# === ANALYSIS (PROFILE) ===


@celery.task(base=SqlAlchemyTask)
def compile_analyses():
    log = logging.getLogger(__name__)
    log.info("Compile analyses")

    update = []
    for dataset_distribution in db_session.query(DatasetDistribution).all():
        for _ in (
            db_session.query(Analysis)
            .filter_by(iri=dataset_distribution.distr)
            .limit(1)
            .all()
        ):
            update.append([{"id": dataset_distribution.id, "relevant": True}])
    try:
        db_session.bulk_update_mappings(DatasetDistribution, update)
        db_session.commit()
    except SQLAlchemyError:
        log.exception("Failed to mark relevant distributions")
        db_session.rollback()


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


# gen_related_ds in chord, Tasks used within a chord must not ignore their results.


@celery.task(ignore_result=False)
def gen_pair(rel_type, token, same_as):
    related_dist = set()
    for sameas_iri in same_as[token]:
        for relationship in db_session.query(Relationship).filter_by(
            type=rel_type, group=sameas_iri
        ):
            rel_dist = relationship.candidate
            related_dist.add(rel_dist)
        # these are related by sameAs of token
    all_related = set()
    for distr_iri in related_dist:
        for relationship in (
            db_session.query(DatasetDistribution).filter_by(dist=distr_iri).all()
        ):
            all_related.add(relationship.ds)
    if len(all_related) > 1:
        # do not consider sets on one candidate for conciseness
        return {"iri": token, "related": list(all_related), "type": rel_type}
    return {"iri": token, "related": [], "type": rel_type}


@celery.task(ignore_result=False, base=SqlAlchemyTask)
def gen2():
    same_as = same_as_index.snapshot()
    pairs = []
    for rel_type in reltypes:
        for relationship in (
            db_session.query(Relationship).filter_by(type=rel_type).distinct().all()
        ):
            pairs.append((rel_type, relationship.group))
    raise ValueError()
    return chord(gen_pair.si(rel_type, token, same_as) for (rel_type, token) in pairs)(
        related_to_mongo.s()
    )


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def gen_related_ds():
    log = logging.getLogger(__name__)
    log.warning("Generate related datasets")
    related_ds = []

    same_as = same_as_index.snapshot()
    for rel_type in reltypes:
        for relationship in (
            db_session.query(Relationship).filter_by(type=rel_type).distinct().all()
        ):
            token = relationship.group
            log.debug("type: %s, token: %s", rel_type, token)
            related_dist = set()
            for sameas_iri in same_as[token]:
                for neighbour in (
                    db_session.query(Relationship)
                    .filter_by(type=rel_type, group=sameas_iri)
                    .all()
                ):
                    rel_dist = neighbour.candidate
                    related_dist.add(rel_dist)
                # these are related by sameAs of token
            all_related = set()
            for distr_iri in related_dist:
                for related_dataset in (
                    db_session.query(DatasetDistribution)
                    .filter_by(dist=distr_iri)
                    .all()
                ):
                    all_related.add(related_dataset.ds)
            if len(all_related) > 1:
                # do not consider sets on one candidate for conciseness
                for dataset in all_related:
                    related_ds.append({"token": token, "ds": dataset, "type": rel_type})

    db_session.bulk_insert_mappings(Related, related_ds)
    red = redis.Redis(connection_pool=redis_pool)
    red.set("shouldQuery", 0)


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def finalize_sameas():
    log = logging.getLogger(__name__)
    log.info("Finalize sameAs index")
    # message_to_mattermost("Finalize sameAs index - query pipeline started")
    same_as_index.finalize()
    # skos not needed - not transitive
    log.info("Successfully finalized sameAs index")


def iter_subjects_objects_sql(distr_iri):
    same_as = same_as_index.snapshot()
    for record in (
        db_session.query(SubjectObject).filter_by(distribution_iri=distr_iri).all()
    ):
        for iri in same_as[record.iri]:
            yield iri


@celery.task(ignore_result=True, base=SqlAlchemyTask, bind=True, max_retries=5)
def ruian_reference(self):
    log = logging.getLogger(__name__)
    log.info("Look for RUIAN references")
    ruian_references = set()
    same_as = same_as_index.snapshot()
    references = []
    for analysis in db_session.query(Analysis).filter_by(analyzer="generic").all():
        ds_ruian_references = set()
        doc = analysis.data
        for initial_iri in set(iri for iri in doc["subjects"]).union(
            set(iri for iri in doc["objects"])
        ):
            for iri in same_as[initial_iri]:
                if iri.startswith("https://linked.cuzk.cz/resource/ruian/"):
                    ruian_references.add(iri)
                    ds_ruian_references.add(iri)
        references.append(
            {
                "iri": analysis.iri,
                "analyzer": "ruian",
                "data": list(ds_ruian_references),
            }
        )
    try:
        db_session.bulk_insert_mappings(Analysis, references)
        db_session.commit()
    except SQLAlchemyError:
        log.exception("Failed to mark relevant distributions")
        db_session.rollback()
    log.info("RUIAN references: %s", str(len(list(ruian_references))))
    try:
        RuianInspector.process_references(ruian_references)
    except RobotsRetry as e:
        self.retry(e.delay)


def report_relationship_bulk(reports):
    try:
        db_session.bulk_insert_mappings(Relationship, reports)
        db_session.commit()
    except SQLAlchemyError:
        logging.getLogger(__name__).exception("Failed to bulk report relationships")
        db_session.rollback()


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def concept_usage():
    log = logging.getLogger(__name__)
    log.info("Concept usage")

    counter = 0
    ddr_types = ddr_index.types()
    same_as = same_as_index.snapshot()
    reports = []
    for distribution in db_session.query(DatasetDistribution).filter_by(relevant=True):
        # z profilu najit vsechna s & o resources a podivat se, zda to neni skos Concept
        distr_iri = distribution.distr

        resource_iri_cache = set()
        for resource_iri in iter_subjects_objects_sql(distr_iri):
            if resource_iri in resource_iri_cache:
                continue
            resource_iri_cache.add(resource_iri)
            # type (broad / narrow apod.)
            # indexuji T -> (a, b); mam jedno z (a, b)
            if concept_index.is_concept(resource_iri):
                # pouzit koncept (polozka ciselniku)
                reports.append(
                    {
                        "type": "conceptUsage",
                        "group": resource_iri,
                        "candidate": distr_iri,
                    }
                )
                counter = counter + 1

                for token in ddr_types:
                    for skos_resource_iri in ddr_index.lookup(token, resource_iri):
                        for final_resource_iri in same_as[skos_resource_iri]:
                            # pouzit related concept
                            reports.append(
                                {
                                    "type": "relatedConceptUsage",
                                    "group": final_resource_iri,
                                    "candidate": distr_iri,
                                }
                            )
                            counter = counter + 1
    report_relationship_bulk(reports)
    log.info("Found relationships: %s", str(counter))


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def concept_definition():
    count = 0
    log = logging.getLogger(__name__)
    log.info("Find datasets with information about concepts (codelists)")
    same_as = same_as_index.snapshot()
    reports = []
    for concept in concept_index.iter_concepts():
        for resource_iri in same_as[concept]:
            for subject_object in db_session.query(SubjectObject).filter_by(
                pure_subject=True
            ):
                distr_iri = subject_object.distribution_iri
                for rel_type in [
                    "conceptUsage",
                    "relatedConceptUsage",
                    "conceptOnDimension",
                    "relatedConceptOnDimension",
                ]:
                    reports.append(
                        {
                            "type": rel_type,
                            "group": resource_iri,
                            "candidate": distr_iri,
                        }
                    )
                    count = count + 1
    report_relationship_bulk(reports)
    log.info("Found %s relationship candidates", str(count))


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def cross_dataset_sameas():
    # to cleanup duplicit records:
    # delete from ddr where id not in (select max(id) from ddr group by relationship_type, iri1, iri2 );
    # pure_subject -> select PureSubject with respective distribution iri
    same_as = same_as_index.snapshot()
    stmt = (
        select(DatasetDistribution)
        .select_from(
            join(
                DatasetDistribution,
                SubjectObject,
                DatasetDistribution.distr == SubjectObject.distribution_iri,
            )
        )
        .filter(
            DatasetDistribution.relevant == True, SubjectObject.pure_subject == True
        )
        .distinct()
    )
    log = logging.getLogger(__name__)
    reports = []
    for row in db_session.execute(stmt).all():
        log.debug(row)
        distr_iri = row[2]
        resource = row[5]
        for iri in same_as[resource]:
            reports.append(
                {"type": "crossSameas", "group": iri, "candidate": distr_iri}
            )
    report_relationship_bulk(reports)


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def data_driven_relationships():
    log = logging.getLogger(__name__)
    log.info("Data driven relationships")
    # projet z DSD vsechny dimenze a miry, zda to neni concept

    rel_types = ddr_index.types()
    same_as = same_as_index.snapshot()
    report = []
    for resource, distr_iri in dsd_index.resources_on_dimension():
        for resource_iri in same_as[resource]:
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
                        for final_resource_iri in same_as[str(skos_resource_iri)]:
                            # report related concept na dimenzi
                            report.append(
                                {
                                    "type": "relatedConceptOnDimension",
                                    "group": final_resource_iri,
                                    "candidate": distr_iri,
                                }
                            )
                report.append(
                    {
                        "type": "conceptOnDimension",
                        "group": resource_iri,
                        "candidate": distr_iri,
                    }
                )
            report.append(
                {
                    "type": "resourceOnDimension",
                    "group": resource_iri,
                    "candidate": distr_iri,
                }
            )
    log.info("Report %s relationship candidates", str(len(report)))
    report_relationship_bulk(report)


# ## MISC ###


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super(JSONEncoder, self).default(obj)
