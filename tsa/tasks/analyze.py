"""Celery tasks for running analyses."""
import os
import json
import logging
import tempfile
from functools import partial
from typing import Tuple, Any, TextIO

import rdflib
from requests import Response
from pyld import jsonld
from rdflib import plugin, URIRef
from rdflib.exceptions import ParserError
from rdflib.store import Store
from requests.exceptions import HTTPError, RequestException
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.analyzer import AbstractAnalyzer
from tsa.db import db_session
from tsa.model import Analysis, Relationship
from tsa.monitor import TimedBlock
from tsa.net import accept, fetch, StreamedFile
from tsa.robots import session
from tsa.settings import Config
from tsa.util import check_iri

jsonld.set_document_loader(
    jsonld.requests_document_loader(
        timeout=Config.TIMEOUT,
        session=session,
        headers={"Accept": accept},
    )
)


def recursive_expander_hook(level: int, decoded: dict) -> dict:
    """
    Recursively expands JSON-LD contexts and imports.

    In case we are at the maximum recursion level, the context and import
    are not expanded and the value is returned as is.

    :param level: current recursion level
    :param decoded: decoded JSON
    :return: decoded JSON with expanded contexts and imports
    """
    next_hook = partial(recursive_expander_hook, level + 1)

    def test_if_should_dereference(value: Any) -> bool:
        if isinstance(value, str):
            return check_iri(value)
        return False

    try:
        if "@import" in decoded and test_if_should_dereference(decoded["@import"]):
            logging.getLogger(__name__).debug(
                "Expanding @import %s", decoded["@import"]
            )
            response = fetch(decoded["@import"])
            del decoded["@import"]
            decoded = json.load(
                StreamedFile(decoded["@import"], response), object_hook=next_hook
            ).update(decoded)
        if "@context" in decoded:
            if test_if_should_dereference(decoded["@context"]):
                logging.getLogger(__name__).debug(
                    "Expanding @context %s", decoded["@context"]
                )
                response = fetch(decoded["@context"])
                remote = json.load(
                    StreamedFile(decoded["@context"], response), object_hook=next_hook
                )
                decoded["@context"] = remote
            elif isinstance(decoded["@context"], list):
                for i, context in enumerate(decoded["@context"]):
                    if test_if_should_dereference(context):
                        logging.getLogger(__name__).debug(
                            "Expanding @context %s", context
                        )
                        response = fetch(context)
                        remote = json.load(
                            StreamedFile(context, response), object_hook=next_hook
                        )
                        decoded["@context"][i] = remote
    except UnicodeDecodeError:
        logging.getLogger(__name__).warning("Failed to decode JSON-LD, falling back")
    return decoded


def convert_jsonld(data, graph) -> None:
    """
    Normalize and parse JSON-LD with possible context expansion.

    As the JSON-LD parser does not support context expansion,
    we do it recusively as part of JSON loading.

    :param data: JSON-LD data (File-like object)
    :param graph: RDF graph to parse into
    :raises ParserError: if the JSON-LD could not be parsed
    """
    try:
        # normalize a document using the RDF Dataset Normalization Algorithm
        # (URDNA2015), see: http://json-ld.github.io/normalization/spec/
        normalized = jsonld.normalize(
            json.load(data, object_hook=partial(recursive_expander_hook, 0)),
            {"algorithm": "URDNA2015", "format": "application/nquads"},
        )
        if isinstance(normalized, dict):
            raise TypeError()
        graph.parse(data=normalized, format="application/n-quads")
    except (TypeError, ParserError):
        logging.getLogger(__name__).warning(
            "Failed to parse expanded JSON-LD, falling back"
        )
        # graph.parse(data=data, format="json-ld")
    except (HTTPError, RequestException):
        logging.getLogger(__name__).warning(
            "HTTP Error expanding JSON-LD, falling back"
        )
        # graph.parse(data=data, format="json-ld")
    # return graph


def load_graph(
    iri: str,
    data: Response,
    format_guess: str,
    storage_file_name: str,
    log_error_as_exception: bool = False,
) -> rdflib.ConjunctiveGraph:
    """
    Load a graph from a string with loaded distribution.

    :param iri: IRI of the distribution
    :param data: the data to load (Response object)
    :param format_guess: guessed format of the data
    :param storage_file_name: where to store the graph in - in case of on disk storage. This should not exist yet. If it does, there will be an error. Usually, the caller sets up a temporary directory and then creates any name - since it's the only file in the directory, it's safe to use.
    :param log_error_as_exception: log exceptions as exception with trace (if not, log errors as warning)
    :return: the loaded graph or an empty graph if loading failed
    """
    log = logging.getLogger(__name__)
    try:
        format_guess = format_guess.lower()
        store = plugin.get("LevelDB", Store)(identifier=URIRef(storage_file_name))
        graph = rdflib.ConjunctiveGraph(store)
        graph.open(storage_file_name, create=True)
        if format_guess == "json-ld":
            convert_jsonld(StreamedFile(iri, data), graph)
        else:
            # parse graph directly from the response so that we read the data directly into the store (in case of large graphs)
            # this leverages the fact that requests.Response.raw is a file-like object
            graph.parse(file=StreamedFile(iri, data), format=format_guess)
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
        {True: log.exception, False: log.warning}[log_error_as_exception](
            "Missing data, iri: %s, format: %s", iri, format_guess
        )
    return rdflib.ConjunctiveGraph()


def do_analyze_and_index(graph: rdflib.Graph, iri: str) -> None:
    """
    Analyze and index a graph.

    :param graph: the graph to analyze
    :param iri: the IRI of the distribution
    """
    log = logging.getLogger(__name__)
    log.debug("Analyze and index - execution: %s", iri)
    analyzers = [c for c in AbstractAnalyzer.__subclasses__() if hasattr(c, "token")]
    log.debug("Analyzers: %s", str(len(analyzers)))

    def gen_analyzes():
        for analyzer_class in analyzers:
            analyzer_token = analyzer_class.token
            log.debug("Analyze and index %s with %s", iri, analyzer_token)
            analyzer = analyzer_class()

            token, res = analyze_and_index_one(analyzer, graph, iri, log)
            yield {"iri": iri, "analyzer": token, "data": res}
            log.debug("Done analyze and index %s with %s", iri, analyzer_token)

    try:
        db_session.execute(insert(Analysis), gen_analyzes())
        db_session.commit()
    except SQLAlchemyError:
        logging.getLogger(__name__).exception("Failed to store analyses in DB")
        db_session.rollback()
    log.info("Done storing %s", iri)


def analyze_and_index_one(
    analyzer: AbstractAnalyzer,
    graph: rdflib.Graph,
    distribution_iri: str,
    log: logging.Logger,
) -> Tuple[str, str]:
    """
    Analyze and index a graph with single analyzer.

    :param analyzer: the analyzer instance to use
    :param analyzer_class: the analyzer class
    :param graph: the graph to analyze
    :param distribution_iri: the IRI of the distribution
    :param log: the logger to use
    :return: the analyzer token and the result
    """
    log.debug("Find relations of %s in %s", analyzer.token, distribution_iri)
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
            with TimedBlock(f"index.{analyzer.token}"):
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
            db_session.execute(insert(Relationship), gen_relations())
            db_session.commit()
        except SQLAlchemyError:
            log.exception("Failed to store relations in DB")
            db_session.rollback()
    except TypeError:
        log.debug("Skip %s for %s", analyzer.token, distribution_iri)

    log.info("Analyzing %s with %s", distribution_iri, analyzer.token)
    with TimedBlock(f"analyze.{analyzer.token}"):
        res = analyzer.analyze(graph, distribution_iri)
    log.info("Done analyzing %s with %s", distribution_iri, analyzer.token)
    return analyzer.token, res
