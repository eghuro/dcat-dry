import json
import logging
from abc import ABC

import redis
from redis.exceptions import RedisError

from tsa.extensions import redis_pool
from tsa.redis import analysis_dataset


class AbstractEnricher(ABC):
    pass


class NoEnrichment(Exception):
    pass


class RuianEnricher(AbstractEnricher):
    token = 'ruian'

    def __init__(self):
        self.__redis = redis.Redis(connection_pool=redis_pool)

    def enrich(self, ruian_iri):
        root = 'https://linked.cuzk.cz/resource/ruian/'
        if ruian_iri.startswith(root):
            key = analysis_dataset(ruian_iri)
            try:
                payload = self.__redis.get(key)
                if payload is None:
                    raise NoEnrichment()
                for analysis in json.loads(payload)['analysis']:
                    if 'ruian' in analysis.keys() and ruian_iri in analysis['ruian'].keys():
                        return analysis['ruian'][ruian_iri]
                raise NoEnrichment()
            except RedisError as err:
                logging.getLogger(__name__).exception('Redis error loading ruian analysis for %s', ruian_iri)
                raise NoEnrichment() from err
            except ValueError as err:
                logging.getLogger(__name__).exception(f'Value error loading ruian analysis for %s', ruian_iri)
                raise NoEnrichment() from err

            # return {
            #    'vusc': ruian_iri[len(root):].split('/')[0],
            #    'iri': ruian_iri
            # }
        else:
            raise NoEnrichment()


class TimeEnricher(AbstractEnricher):
    token = 'date'

    def __init__(self):
        self.__redis = redis.Redis(connection_pool=redis_pool)

    def enrich(self, time_iri):
        root = 'http://reference.data.gov.uk/id/gregorian-day/'
        if time_iri.startswith(root):
            key = analysis_dataset(time_iri)
            try:
                for analysis in json.loads(self.__redis.get(key))['analysis']:
                    if 'time' in analysis.keys() and time_iri in analysis['time'].keys():
                        return analysis['time'][time_iri]
                raise NoEnrichment()
            except RedisError as err:
                logging.getLogger(__name__).exception(f'Redis error loading time analysis for %s', time_iri)
                raise NoEnrichment() from err
            except ValueError as err:
                logging.getLogger(__name__).exception(f'Value error loading time analysis for %s', time_iri)
                raise NoEnrichment() from err

            # return {
            #    'date': time_iri[len(root):]
            # }
        else:
            raise NoEnrichment()
