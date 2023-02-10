"""Celery tasks for running analyses."""
import json
import logging
from collections import defaultdict
from typing import Tuple

import rdflib
from pyld import jsonld
from rdflib.exceptions import ParserError
from requests.exceptions import HTTPError, RequestException
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.analyzer import AbstractAnalyzer
from tsa.db import db_session
from tsa.model import Analysis, Relationship
from tsa.monitor import TimedBlock
from tsa.robots import USER_AGENT
from tsa.settings import Config

jsonld.set_document_loader(
    jsonld.requests_document_loader(
        timeout=Config.TIMEOUT, headers={"User-Agent": USER_AGENT}
    )
)


def convert_jsonld(data: str) -> rdflib.ConjunctiveGraph:
    graph = rdflib.ConjunctiveGraph()
    try:
        json_data = json.loads(data)
        # normalize a document using the RDF Dataset Normalization Algorithm
        # (URDNA2015), see: http://json-ld.github.io/normalization/spec/
        normalized = jsonld.normalize(
            json_data, {"algorithm": "URDNA2015", "format": "application/nquads"}
        )
        if isinstance(normalized, dict):
            raise TypeError()
        graph.parse(data=normalized, format="application/n-quads")
    except (TypeError, ParserError):
        logging.getLogger(__name__).warning(
            "Failed to parse expanded JSON-LD, falling back"
        )
        graph.parse(data=data, format="json-ld")
    except (HTTPError, RequestException):
        logging.getLogger(__name__).warning(
            "HTTP Error expanding JSON-LD, falling back"
        )
        graph.parse(data=data, format="json-ld")
    return graph


def load_graph(
    iri: str, data: str, format_guess: str, log_error_as_exception: bool = False
) -> rdflib.ConjunctiveGraph:
    log = logging.getLogger(__name__)
    try:
        format_guess = format_guess.lower()
        if format_guess == "json-ld":
            graph = convert_jsonld(data)
        else:
            graph = rdflib.ConjunctiveGraph()
            graph.parse(data=data, format=format_guess)
        return graph
    except (TypeError, ParserError):
        log.warning("Failed to parse %s (%s)", iri, format_guess)
    except (
        rdflib.plugin.PluginException,
        UnicodeDecodeError,
        UnicodeEncodeError,
        json.decoder.JSONDecodeError,
    ):
        message = f"Failed to parse graph for {iri}"
        {True: log.exception, False: log.warning}[log_error_as_exception](message)
    except ValueError:
        log.exception(
            "Missing data, iri: %s, format: %s, data: %s",
            iri,
            format_guess,
            data[0:1000],
        )
    return rdflib.ConjunctiveGraph()


def do_analyze_and_index(graph: rdflib.Graph, iri: str) -> None:
    log = logging.getLogger(__name__)
    if graph is None:
        log.debug("Graph is None for %s", iri)
        return

    log.debug("Analyze and index - execution: %s", iri)

    analyzers = [c for c in AbstractAnalyzer.__subclasses__() if hasattr(c, "token")]
    log.debug("Analyzers: %s", str(len(analyzers)))

    def gen_analyzes():
        for analyzer_class in analyzers:
            analyzer_token = analyzer_class.token
            log.debug("Analyze and index %s with %s", iri, analyzer_token)
            analyzer = analyzer_class()

            token, res = analyze_and_index_one(
                analyzer, analyzer_class, graph, iri, log
            )
            yield {"iri": iri, "analyzer": token, "data": res}
            log.debug("Done analyze and index %s with %s", iri, analyzer_token)

    try:
        db_session.execute(
            insert(Analysis), gen_analyzes(), execution_options={"stream_results": True}
        )
        db_session.commit()
    except SQLAlchemyError:
        logging.getLogger(__name__).exception("Failed to store analyses in DB")
        db_session.rollback()
    log.info("Done storing %s", iri)


def analyze_and_index_one(
    analyzer, analyzer_class, graph, distribution_iri, log
) -> Tuple[str, str]:
    log.debug("Find relations of %s in %s", analyzer_class.token, distribution_iri)
    try:

        def check(common, group, rel_type):
            if common is None or len(common) == 0:
                return False
            if group is None or len(group) == 0:
                return False
            if rel_type is None or len(rel_type) == 0:
                return False
            return True

        def gen_relations():
            with TimedBlock(f"index.{analyzer_class.token}"):
                for common_iri, group, rel_type in analyzer.find_relation(graph):
                    log.debug(
                        "Distribution: %s, relationship type: %s, common resource: %s, significant resource: %s",
                        distribution_iri,
                        rel_type,
                        common_iri,
                        group,
                    )
                    if check(common_iri, group, rel_type):
                        yield {
                            "type": rel_type,
                            "group": group,
                            "common": common_iri,
                            "candidate": distribution_iri,
                        }

        try:
            db_session.execute(
                insert(Relationship),
                gen_relations(),
                execution_options={"stream_results": True},
            )
            db_session.commit()
        except SQLAlchemyError:
            log.exception("Failed to store relations in DB")
            db_session.rollback()
    except TypeError:
        log.debug("Skip %s for %s", analyzer_class.token, distribution_iri)

    log.info("Analyzing %s with %s", distribution_iri, analyzer_class.token)
    with TimedBlock(f"analyze.{analyzer_class.token}"):
        res = analyzer.analyze(graph, distribution_iri)
    log.info("Done analyzing %s with %s", distribution_iri, analyzer_class.token)
    return analyzer_class.token, res
