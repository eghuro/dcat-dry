import logging

from celery import chain

from tsa.tasks.query import (
    compile_analyses,
    concept_definition,
    concept_usage,
    cross_dataset_sameas,
    data_driven_relationships,
    finalize_sameas,
    gen2,
    ruian_reference
)


def query():
    log = logging.getLogger(__name__)
    log.info("query: build celery canvas")
    # message_to_mattermost("building query canvas")
    return chain(
        [
            finalize_sameas.si(),  # no dependecies, must
            compile_analyses.si(), # mark relevant
            cross_dataset_sameas.si(),  # must
            ruian_reference.si(),  # sameas -> bulk insert DDR, concept
            data_driven_relationships.si(),  # sameas, ruian
            concept_usage.si(),  # sameas + ddr (related concept)
            concept_definition.si(),
            gen2.si(),
        ]
    ).apply_async(queue="query")
