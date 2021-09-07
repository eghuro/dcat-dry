import logging

import redis
import rfc3987

from tsa.redis import KeyRoot, root_name


class DataDrivenRelationshipIndex(object):
    def __init__(self, redis_pool):
        self.__red = redis.Redis(connection_pool=redis_pool)

    def index(self, relationship_type, iri1, iri2):
        log = logging.getLogger(__name__)
        if not rfc3987.match(iri1) or not iri1.startswith('http'):
            log.error(f'Not an iri: {iri1} (relationship_type: {relationship_type})')
            return
        if not rfc3987.match(iri2) or not iri2.startswith('http'):
            log.error(f'Not an iri: {iri2} (relationship_type: {relationship_type})')
            return

        with self.__red.pipeline() as pipe:
            pipe.sadd(f'ddr:{relationship_type}:{iri1}', iri2)
            pipe.sadd(f'ddr:{relationship_type}:{iri2}', iri2)
            pipe.execute()

    def types(self):
        # ddr:<type>:<iri>
        for key in self.__red.scan_iter(match='ddr:*'):
            yield key[4:].split(':')[0]

    def lookup(self, relationship_type, resource_iri):
        for iri in self.__red.sscan_iter(f'ddr:{relationship_type}:{resource_iri}'):
            if isinstance(iri, list):
                for el in iri:
                    yield el
            elif isinstance(iri, str):
                yield iri
            # else:
            #    logging.getLogger(__name__).error(f'Bad iri type. iri: {iri!s}, type: {type(iri)!s}, rel type: {relationship_type}, resource: {resource_iri}')


class ConceptIndex(object):
    def __init__(self, redis_pool):
        self.__red = redis.Redis(connection_pool=redis_pool)

    def index(self, iri):
        self.__red.sadd(root_name[KeyRoot.CONCEPT], iri)

    def is_concept(self, iri):
        return self.__red.sismember(root_name[KeyRoot.CONCEPT], iri)

    def iter_concepts(self):
        for iri in self.__red.sscan_iter(root_name[KeyRoot.CONCEPT]):
            yield iri


class DataCubeDefinitionIndex(object):
    def __init__(self, redis_pool):
        self.__red = redis.Redis(connection_pool=redis_pool)

    def index(self, dsd, iri):
        # momentalne si jen ulozime resources na dimenzi
        with self.__red.pipeline() as pipe:
            # pipe.sadd('dsd:rod', iri)
            for dataset in dsd:
                for dimension in dataset['dimensions']:
                    pipe.sadd(f'dsd:rod:{iri}', *dimension['resources'])
            pipe.execute()

    def resources_on_dimension(self):
        #         for key in self.__red.smembers(match=f'dsd:rod'):
        for key in self.__red.scan_iter(match='dsd:rod:*'):
            iri = key[8:]
            for rod in self.__red.sscan_iter(f'dsd:rod:{iri}'):
                yield rod, iri
