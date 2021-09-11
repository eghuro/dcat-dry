import logging

from celery import chain

from tsa.tasks.query import (cache_labels, compile_analyses, concept_definition, concept_usage, cross_dataset_sameas,
                             data_driven_relationships, finalize_sameas, gen_related_ds, ruian_reference,
                             store_to_mongo)
from tsa.util import message_to_mattermost


def query(result_id):
    log = logging.getLogger(__name__)
    log.info('query: build celery canvas')
    message_to_mattermost('building query canvas')
    return chain([
        finalize_sameas.si(),  # no dependecies
        compile_analyses.si(result_id), store_to_mongo.s(result_id),

        cross_dataset_sameas.si(),
        ruian_reference.si(),  # mongo + sameas
        data_driven_relationships.si(),  # sameas, ruian

        concept_usage.si(),  # mongo + sameas + ddr (related concept)
        concept_definition.si(),
        cache_labels.si(),
        gen_related_ds.si(),
    ]).apply_async(queue='query')
