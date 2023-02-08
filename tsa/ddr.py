import logging
from typing import Dict, Generator, List, Tuple

import rfc3987
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.db import db_session
from tsa.model import DDR, Concept, Datacube


class DataDrivenRelationshipIndex:
    def bulk_index(self, data) -> None:
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

    def index(self, relationship_type: str, iri1: str, iri2: str) -> None:
        log = logging.getLogger(__name__)
        if not rfc3987.match(iri1) or not iri1.startswith("http"):
            log.debug("Not an iri: %s (relationship_type: %s)", iri1, relationship_type)
            return
        if not rfc3987.match(iri2) or not iri2.startswith("http"):
            log.debug("Not an iri: %s (relationship_type: %s)", iri2, relationship_type)
            return

        try:
            db_session.add(
                DDR(relationship_type=relationship_type, iri1=iri1, iri2=iri2)
            )
            db_session.add(
                DDR(relationship_type=relationship_type, iri1=iri2, iri2=iri1)
            )
            db_session.commit()
        except SQLAlchemyError:
            logging.getLogger(__name__).exception(
                "Failed do commit, rolling back DataDrivenRelationshipIndex index"
            )
            db_session.rollback()

    def types(self) -> Generator[str, None, None]:
        for type in db_session.query(DDR.relationship_type).distinct():
            yield type

    def lookup(
        self, relationship_type: str, resource_iri: str
    ) -> Generator[str, None, None]:
        for ddr in db_session.query(DDR).filter_by(
            relationship_type=relationship_type, iri1=resource_iri
        ):
            yield ddr.iri2


ddr_index = DataDrivenRelationshipIndex()


class ConceptIndex:
    def bulk_insert(self, data):
        if data is None or len(data) == 0:
            return
        clean_data = [
            {"iri": iri} for iri in data if (iri is not None) and (len(iri) > 0)
        ]
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

    def index(self, iri: str) -> None:
        try:
            db_session.add(Concept(iri=iri))
            db_session.commit()
        except SQLAlchemyError:
            logging.getLogger(__name__).exception(
                "Failed do commit, rolling back ConceptIndex index"
            )
            db_session.rollback()

    def is_concept(self, iri: str) -> bool:
        if iri is None or len(iri) == 0:
            return False
        for _ in db_session.query(Concept).filter_by(iri=iri):
            return True
        return False

    def iter_concepts(self) -> Generator[str, None, None]:
        for concept in db_session.query(Concept):
            yield concept.iri


class DataCubeDefinitionIndex:
    def index(self, dsd: List[Dict], iri: str) -> None:
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

    def resources_on_dimension(self) -> Generator[Tuple[str, str], None, None]:
        for dq in db_session.query(Datacube):
            yield dq.rod, dq.iri


concept_index = ConceptIndex()
dsd_index = DataCubeDefinitionIndex()
