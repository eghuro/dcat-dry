import logging
from typing import Dict, Generator, Sequence, Tuple

import rfc3987
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.db import db_session
from tsa.model import DDR, Concept, Datacube


class DataDrivenRelationshipIndex:
    @staticmethod
    def bulk_index(data) -> None:
        log = logging.getLogger(__name__)
        cleaned = []
        for record in data:
            iri1 = record["iri1"]
            iri2 = record["iri2"]
            relationship_type = record["relationship_type"]
            if (
                iri1 is None
                or len(iri1) == 0
                or iri2 is None
                or len(iri2) == 0
                or relationship_type is None
                or len(relationship_type) == 0
            ):
                continue
            if not rfc3987.match(iri1) or not iri1.startswith("http"):
                log.debug(
                    "Not an iri: %s (relationship_type: %s)", iri1, relationship_type
                )
                continue
            if not rfc3987.match(iri2) or not iri2.startswith("http"):
                log.debug(
                    "Not an iri: %s (relationship_type: %s)", iri2, relationship_type
                )
                continue
            cleaned.append(record)
            cleaned.append(
                {"relationship_type": relationship_type, "iri1": iri2, "iri2": iri1}
            )
        if len(cleaned) > 0:
            try:
                insert_stmt = insert(DDR).values(cleaned).on_conflict_do_nothing()
                db_session.execute(insert_stmt)
                db_session.commit()
            except SQLAlchemyError:
                logging.getLogger(__name__).exception(
                    "Failed do commit, rolling back DataDrivenRelationshipIndex bulk index"
                )
                db_session.rollback()

    @staticmethod
    def types() -> Generator[str, None, None]:
        for relationship_type in (
            db_session.query(DDR.relationship_type).distinct().all()
        ):
            yield relationship_type

    @staticmethod
    def lookup(relationship_type: str, resource_iri: str) -> Generator[str, None, None]:
        for ddr in (
            db_session.query(DDR)
            .filter_by(relationship_type=relationship_type, iri1=resource_iri)
            .all()
        ):
            yield ddr.iri2


ddr_index = DataDrivenRelationshipIndex()


class ConceptIndex:
    @staticmethod
    def bulk_insert(data: Sequence[str]) -> None:
        if data is None or len(data) == 0:
            return
        clean_data = tuple(
            {"iri": iri} for iri in data if (iri is not None) and (len(iri) > 0)
        )
        if len(clean_data) == 0:
            return
        insert_stmt = insert(Concept).values(clean_data).on_conflict_do_nothing()
        try:
            db_session.execute(insert_stmt)
            db_session.commit()
        except SQLAlchemyError:
            logging.getLogger(__name__).exception(
                "Failed do commit, rolling back ConceptIndex bulk index"
            )
            db_session.rollback()

    @staticmethod
    def is_concept(iri: str) -> bool:
        if iri is None or len(iri) == 0:
            return False
        return db_session.query(Concept).filter_by(iri=iri).count() > 0

    @staticmethod
    def iter_concepts() -> Generator[str, None, None]:
        for concept_iri in db_session.query(Concept.iri).distinct().all():
            yield concept_iri


class DataCubeDefinitionIndex:
    @staticmethod
    def index(dsd: Sequence[Dict], iri: str) -> None:
        # momentalne si jen ulozime resources na dimenzi
        for dataset in dsd:
            for dimension in dataset["dimensions"]:
                for rod in dimension["resources"]:
                    db_session.add(Datacube(iri=iri, rod=rod))
        try:
            db_session.commit()
        except SQLAlchemyError:
            logging.getLogger(__name__).exception(
                "Failed do commit, rolling back DataCubeDefinitionIndex index"
            )
            db_session.rollback()

    @staticmethod
    def resources_on_dimension() -> Generator[Tuple[str, str], None, None]:
        for datacube in db_session.query(Datacube).all():
            yield datacube.rod, datacube.iri


concept_index = ConceptIndex()
dsd_index = DataCubeDefinitionIndex()
