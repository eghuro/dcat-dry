import json
import logging
from abc import ABC

import redis

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
                for a in json.loads(payload)['analysis']:
                    if 'ruian' in a.keys():
                        if ruian_iri in a['ruian'].keys():
                            return a['ruian'][ruian_iri]
                raise NoEnrichment()
            except:
                logging.getLogger(__name__).exception(f'Missing ruian analysis for {ruian_iri}')
                raise NoEnrichment()

            #return {
            #    'vusc': ruian_iri[len(root):].split('/')[0],
            #    'iri': ruian_iri
            #}
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
                for a in json.loads(self.__redis.get(key))['analysis']:
                    if 'time' in a.keys():
                        if time_iri in a['time'].keys():
                            return a['time'][time_iri]
                raise NoEnrichment()
            except:
                logging.getLogger(__name__).exception(f'Missing time analysis for {time_iri}')
                raise NoEnrichment()

            # return {
            #    'date': time_iri[len(root):]
            # }
        else:
            raise NoEnrichment()
