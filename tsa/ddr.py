import logging
from typing import Dict, Generator, List, Tuple

import redis
import rfc3987
from redis import ConnectionPool
from sqlalchemy import select
from sqlalchemy.orm import Session

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

        with Session(db) as session:
            session.add(DDR(relationship_type=relationship_type, iri1=iri1, iri2=iri2))
            session.add(DDR(relationship_type=relationship_type, iri1=iri2, iri2=iri1))
            session.commit()

    def types(self) -> Generator[str, None, None]:
        with Session(db) as session:
            for type in session.query(DDR.relationship_type).distinct():
                yield type

    def lookup(
        self, relationship_type: str, resource_iri: str
    ) -> Generator[str, None, None]:
        with Session(db) as session:
            for ddr in session.query(DDR).filter_by(relationship_type=relationship_type, iri1=resource_iri):
                yield ddr.iri2


class ConceptIndex:

    def index(self, iri: str) -> None:
        try:
            with Session(db) as session:
                session.add(Concept(iri=iri))
                session.commit()
        except:
            pass

    def is_concept(self, iri: str) -> bool:
        with Session(db) as session:
            for _ in session.query(Concept).filter_by(iri=iri):
                return True
        return False

    def iter_concepts(self) -> Generator[str, None, None]:
        with Session(db) as session:
            for concept in session.query(Concept):
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