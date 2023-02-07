"""Celery tasks for querying."""
import logging
from json import JSONEncoder

import redis
from sqlalchemy import select, insert
from sqlalchemy.orm import join
from celery import chord

from tsa.celery import celery
from tsa.db import db_session
from tsa.ddr import concept_index, dsd_index, ddr_index
from tsa.model import DatasetDistribution, Relationship, SubjectObject, Analysis, Related
from tsa.net import RobotsRetry
from tsa.extensions import redis_pool
from tsa.sameas import same_as_index
from tsa.ruian import RuianInspector
from tsa.tasks.common import SqlAlchemyTask

# === ANALYSIS (PROFILE) ===

@celery.task(base=SqlAlchemyTask)
def compile_analyses():
    log = logging.getLogger(__name__)
    log.info("Compile analyses")

    update = []
    for d in db_session.query(DatasetDistribution):
        for _ in db_session.query(Analysis).filter_by(iri=d.distr).limit(1):
            update.append([{'id': d.id, 'relevant': True}])
    try:
        db_session.bulk_update_mappings(DatasetDistribution, update)
        db_session.commit()
    except:
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
def gen_pair(rel_type, token, sameAs):
    related_dist = set()
    for sameas_iri in sameAs[token]:
        for s in db_session.query(Relationship).filter_by(type=rel_type, group=sameas_iri):
            rel_dist = s.candidate
            related_dist.add(rel_dist)
        # these are related by sameAs of token
    all_related = set()
    for distr_iri in related_dist:
        for s in db_session.query(DatasetDistribution).filter_by(dist=distr_iri):
            all_related.add(s.ds)
    if (len(all_related) > 1):
        # do not consider sets on one candidate for conciseness
        return {"iri": token, "related": list(all_related), "type": rel_type}


@celery.task(ignore_result=False, base=SqlAlchemyTask)
def gen2():
    sameAs = same_as_index.snapshot()
    pairs = []
    for rel_type in reltypes:
        for r in db_session.query(Relationship).filter_by(type=rel_type).distinct():
            pairs.append((rel_type, r.token))
    return chord(gen_pair.si(rel_type, token, sameAs) for (rel_type, token) in pairs)(related_to_mongo.s())


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def gen_related_ds():
    log = logging.getLogger(__name__)
    log.warning("Generate related datasets")
    related_ds = []

    sameAs = same_as_index.snapshot()
    for rel_type in reltypes:
        for r in db_session.query(Relationship).filter_by(type=rel_type).distinct():
            token = r.token
            log.debug(f"type: {rel_type}, token: {token}")
            related_dist = set()
            for sameas_iri in sameAs[token]:
                for s in db_session.query(Relationship).filter_by(type=rel_type, group=sameas_iri):
                    rel_dist = s.candidate
                    related_dist.add(rel_dist)
                # these are related by sameAs of token
            all_related = set()
            for distr_iri in related_dist:
                for s in db_session.query(DatasetDistribution).filter_by(dist=distr_iri):
                    all_related.add(s.ds)
            if (len(all_related) > 1):
                # do not consider sets on one candidate for conciseness
                for ds in all_related:
                    related_ds.append(
                        {"token": token, "ds": ds, "type": rel_type}
                    )
    
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
    sameAs = same_as_index.snapshot()
    for record in db_session.query(SubjectObject).filter_by(distr_iri):
        for iri in sameAs[record.iri]:
            yield iri


@celery.task(ignore_result=True, base=SqlAlchemyTask, bind=True, max_retries=5)
def ruian_reference(self):
    log = logging.getLogger(__name__)
    log.info("Look for RUIAN references")
    ruian_references = set()
    sameAs = same_as_index.snapshot()
    references = []
    for analysis in db_session.query(Analysis).filter_by(analyzer='generic'):
        ds_ruian_references = set()
        doc = analysis.data
        for initial_iri in set(iri for iri in doc["generic"]["subjects"]).union(set(iri for iri in doc["generic"]["objects"])):
            for iri in sameAs[initial_iri]:
                if iri.startswith("https://linked.cuzk.cz/resource/ruian/"):
                    ruian_references.add(iri)
                    ds_ruian_references.add(iri)
        references.append({'iri': analysis.iri, 'analyzer': 'ruian', 'data': list(ds_ruian_references)})
    try:
        db_session.bulk_insert_mappings(Analysis, references)
        db_session.commit()
    except:
        log.exception("Failed to mark relevant distributions")
        db_session.rollback()
    log.info(f"RUIAN references: {len(list(ruian_references))}")
    try:
        RuianInspector.process_references(ruian_references)
    except RobotsRetry as e:
        self.retry(e.delay)


def report_relationship_bulk(reports):
    try:
        db_session.execute(insert(Relationship, reports))
        db_session.commit()
    except:
        logging.getLogger(__name__).exception("Failed to bulk report relationships")
        db_session.rollback()

@celery.task(ignore_result=True, base=SqlAlchemyTask)
def concept_usage():
    log = logging.getLogger(__name__)
    log.info("Concept usage")

    counter = 0
    ddr_types = ddr_index.types()
    sameAs = same_as_index.snapshot()
    reports = []
    for o in db_session.query(DatasetDistribution).filter_by(relevant=True):
        # z profilu najit vsechna s & o resources a podivat se, zda to neni skos Concept
        distr_iri = o.distr

        resource_iri_cache = set()
        for resource_iri in iter_subjects_objects_sql(distr_iri):
            if resource_iri in resource_iri_cache:
                continue
            resource_iri_cache.add(resource_iri)
            # type (broad / narrow apod.)
            # indexuji T -> (a, b); mam jedno z (a, b)
            if concept_index.is_concept(resource_iri):
                # pouzit koncept (polozka ciselniku)
                reports.append({
                    "type": "conceptUsage",
                    "group": resource_iri,
                    "candidate": distr_iri})
                counter = counter + 1

                for token in ddr_types:
                    for skos_resource_iri in ddr_index.lookup(token, resource_iri):
                        for final_resource_iri in sameAs[skos_resource_iri]:
                            # pouzit related concept
                            reports.append({
                                "type": "relatedConceptUsage",
                                "group": final_resource_iri,
                                "candidate": distr_iri
                            })
                            counter = counter + 1
    report_relationship_bulk(reports)
    log.info(f"Found relationships: {counter}")


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def concept_definition():
    count = 0
    log = logging.getLogger(__name__)
    log.info("Find datasets with information about concepts (codelists)")
    sameAs = same_as_index.snapshot()
    reports = []
    for concept in concept_index.iter_concepts():
        for resource_iri in sameAs[concept]:
            for so in db_session.query(SubjectObject).filter_by(pureSubject=True):
                distr_iri = so.distribution_iri
                for rel_type in [
                    "conceptUsage",
                    "relatedConceptUsage",
                    "conceptOnDimension",
                    "relatedConceptOnDimension",
                ]:
                    reports.append({
                        "type": rel_type,
                        "group": resource_iri,
                        "candidate": distr_iri})
                    count = count + 1
    report_relationship_bulk(reports)
    log.info(f"Found {count} relationship candidates")


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def cross_dataset_sameas():
    # to cleanup duplicit records:
    # delete from ddr where id not in (select max(id) from ddr group by relationship_type, iri1, iri2 );
    # pure_subject -> select PureSubject with respective distribution iri
    sameAs = same_as_index.snapshot()
    stmt = select(DatasetDistribution).select_from(join(DatasetDistribution, SubjectObject, DatasetDistribution.distr == SubjectObject.distribution_iri)).filter(DatasetDistribution.relevant==True, SubjectObject.pureSubject==True).distinct()
    log = logging.getLogger(__name__)
    reports = []
    for row in db_session.execute(stmt).all():
        log.debug(row)
        distr_iri = row[2]
        resource = row[5]
        for iri in sameAs[resource]:
            reports.append({
                "type": "crossSameas",
                "group": iri,
                "candidate": distr_iri
            })
    report_relationship_bulk(reports)


@celery.task(ignore_result=True, base=SqlAlchemyTask)
def data_driven_relationships():
    log = logging.getLogger(__name__)
    log.info("Data driven relationships")
    # projet z DSD vsechny dimenze a miry, zda to neni concept
    
    rel_types = ddr_index.types()
    sameAs = same_as_index.snapshot()
    report = []
    for resource, distr_iri in dsd_index.resources_on_dimension():
        for resource_iri in sameAs[resource]:
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
                        for final_resource_iri in sameAs[str(skos_resource_iri)]:
                            # report related concept na dimenzi
                            report.append(
                                {
                                    "type": "relatedConceptOnDimension",
                                    "group": final_resource_iri,
                                    "candidate": distr_iri,
                                }
                            )
                report.append({
                    "type": "conceptOnDimension",
                    "group": resource_iri,
                    "candidate": distr_iri
                })
            report.append({
                "type": "resourceOnDimension",
                "group": resource_iri,
                "candidate": distr_iri
            })
    log.info(f"Report {len(report)} relationship candidates")
    report_relationship_bulk(report)


# ## MISC ###


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super(JSONEncoder, self).default(obj)
