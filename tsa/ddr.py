import logging
from typing import Dict, Generator, List, Tuple

import redis
import rfc3987
from redis.client import ConnectionPool

from tsa.redis import KeyRoot, root_name


class DataDrivenRelationshipIndex:

    def __init__(self, redis_pool: ConnectionPool):
        self.__red = redis.Redis(connection_pool=redis_pool)

    def index(self, relationship_type: str, iri1: str, iri2: str) -> None:
        log = logging.getLogger(__name__)
        if not rfc3987.match(iri1) or not iri1.startswith('http'):
            log.debug('Not an iri: %s (relationship_type: %s)', iri1, relationship_type)
            return
        if not rfc3987.match(iri2) or not iri2.startswith('http'):
            log.debug('Not an iri: %s (relationship_type: %s)', iri2, relationship_type)
            return

        with self.__red.pipeline() as pipe:
            pipe.sadd(f'ddr:{relationship_type}:{iri1}', iri2)
            pipe.sadd(f'ddr:{relationship_type}:{iri2}', iri2)
            pipe.execute()

    def types(self) -> Generator[str, None, None]:
        # ddr:<type>:<iri>
        for key in self.__red.scan_iter(match='ddr:*'):
            yield str(key)[4:].split(':', maxsplit=1)[0]

    def lookup(self, relationship_type: str, resource_iri: str) -> Generator[str, None, None]:
        for iri in self.__red.sscan_iter(f'ddr:{relationship_type}:{resource_iri}'):
            if isinstance(iri, list):
                for element in iri:
                    yield element
            elif isinstance(iri, str):
                yield iri


class ConceptIndex:

    def __init__(self, redis_pool: ConnectionPool):
        self.__red = redis.Redis(connection_pool=redis_pool)

    def index(self, iri: str) -> None:
        self.__red.sadd(root_name[KeyRoot.CONCEPT], iri)

    def is_concept(self, iri: str) -> bool:
        return self.__red.sismember(root_name[KeyRoot.CONCEPT], iri)

    def iter_concepts(self) -> Generator[str, None, None]:
        for iri in self.__red.sscan_iter(root_name[KeyRoot.CONCEPT]):
            yield iri


class DataCubeDefinitionIndex:

    def __init__(self, redis_pool: ConnectionPool):
        self.__red = redis.Redis(connection_pool=redis_pool)

    def index(self, dsd: List[Dict], iri: str) -> None:
        # momentalne si jen ulozime resources na dimenzi
        with self.__red.pipeline() as pipe:
            # pipe.sadd('dsd:rod', iri)
            for dataset in dsd:
                for dimension in dataset['dimensions']:
                    if len(dimension['resources']) > 0:
                        pipe.sadd(f'dsd:rod:{iri}', *dimension['resources'])
            pipe.execute()

    def resources_on_dimension(self) -> Generator[Tuple[str, str], None, None]:
        #         for key in self.__red.smembers(match=f'dsd:rod'):
        for key in self.__red.scan_iter(match='dsd:rod:*'):
            iri = str(key)[8:]
            for rod in self.__red.sscan_iter(f'dsd:rod:{iri}'):
                yield str(rod), iri
