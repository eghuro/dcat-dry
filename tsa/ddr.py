import logging
from typing import Dict, Generator, List, Tuple

import redis
import rfc3987
from redis import ConnectionPool
from sqlalchemy.exc import PendingRollbackError, IntegrityError

from tsa.db import db_session
from tsa.model import DDR, Concept
from tsa.extensions import db, redis_pool


class DataDrivenRelationshipIndex:
    def index(self, relationship_type: str, iri1: str, iri2: str) -> None:
        log = logging.getLogger(__name__)
        if not rfc3987.match(iri1) or not iri1.startswith("http"):
            log.debug("Not an iri: %s (relationship_type: %s)", iri1, relationship_type)
            return
        if not rfc3987.match(iri2) or not iri2.startswith("http"):
            log.debug("Not an iri: %s (relationship_type: %s)", iri2, relationship_type)
            return

        try:
            db_session.add(DDR(relationship_type=relationship_type, iri1=iri1, iri2=iri2))
            db_session.add(DDR(relationship_type=relationship_type, iri1=iri2, iri2=iri1))
            db_session.commit()
        except PendingRollbackError:
            db_session.rollback()
        except IntegrityError:
            db_session.rollback()

    def types(self) -> Generator[str, None, None]:
        for type in db_session.query(DDR.relationship_type).distinct():
                yield type

    def lookup(
        self, relationship_type: str, resource_iri: str
    ) -> Generator[str, None, None]:
        for ddr in db_session.query(DDR).filter_by(relationship_type=relationship_type, iri1=resource_iri):
            yield ddr.iri2

ddr_index = DataDrivenRelationshipIndex()

class ConceptIndex:

    def index(self, iri: str) -> None:
        try:
            db_session.add(Concept(iri=iri))
            db_session.commit()
        except PendingRollbackError:
            db_session.rollback()
        except IntegrityError:
            db_session.rollback()

    def is_concept(self, iri: str) -> bool:
        for _ in db_session.query(Concept).filter_by(iri=iri):
            return True
        return False

    def iter_concepts(self) -> Generator[str, None, None]:
        for concept in db_session.query(Concept):
            yield concept.iri


class DataCubeDefinitionIndex:
    def __init__(self, redis_pool: ConnectionPool):
        self.__red = redis.Redis(connection_pool=redis_pool)

    def index(self, dsd: List[Dict], iri: str) -> None:
        # momentalne si jen ulozime resources na dimenzi
        with self.__red.pipeline() as pipe:
            # pipe.sadd('dsd:rod', iri)
            for dataset in dsd:
                for dimension in dataset["dimensions"]:
                    if len(dimension["resources"]) > 0:
                        pipe.sadd(f"dsd:rod:{iri}", *dimension["resources"])
            pipe.execute()

    def resources_on_dimension(self) -> Generator[Tuple[str, str], None, None]:
        #         for key in self.__red.smembers(match=f'dsd:rod'):
        for key in self.__red.scan_iter(match="dsd:rod:*"):
            iri = str(key)[8:]
            for rod in self.__red.sscan_iter(f"dsd:rod:{iri}"):
                yield str(rod), iri

concept_index = ConceptIndex()
dsd_index = DataCubeDefinitionIndex(redis_pool)