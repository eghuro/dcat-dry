"""Celery tasks for running analyses."""
import json
import logging
from collections import defaultdict

import rdflib
from pyld import jsonld
from requests.exceptions import HTTPError, RequestException
from sqlalchemy import insert

from tsa.analyzer import AbstractAnalyzer
from tsa.db import db_session
from tsa.monitor import TimedBlock
from tsa.model import Relationship, Analysis
from tsa.net import fetch, get_content


def dereference_remote_context(iri: str) -> dict:
    response = fetch(iri)
    content = get_content(iri, response)
    return json.loads(content)["@context"]


def convert_jsonld(data: str) -> rdflib.ConjunctiveGraph:
    g = rdflib.ConjunctiveGraph()
    expanded = None
    try:
        json_data = json.loads(data)
        options = {}
        if "@context" in json_data and isinstance(json_data["@context"], str):
            if json_data["@context"].startswith('http'):
                logging.getLogger(__name__).info("Fetch remote context %s", json_data["@context"])
                json_data["@context"] = dereference_remote_context(json_data["@context"])
        if "@context" in json_data and "@base" in json_data["@context"]:
            options["base"] = json_data["@context"]["@base"]
        expanded = jsonld.expand(json_data, options=options)
        g.parse(data=json.dumps(expanded), format="json-ld")
    except (TypeError, rdflib.exceptions.ParserError):
        #if expanded is not None:
        #    expanded_data = json.dumps(expanded)
        #else:
        #    expanded_data = "**ERROR**, data was: " + str(data)
        logging.getLogger(__name__).warning("Failed to parse expanded JSON-LD, falling back")
        g.parse(data=data, format="json-ld")
    except (HTTPError, RequestException):
        logging.getLogger(__name__).warning("HTTP Error expanding JSON-LD, falling back")
        g.parse(data=data, format="json-ld")
    return g


def load_graph(
    iri: str, data: str, format_guess: str, log_error_as_exception: bool = False
) -> rdflib.ConjunctiveGraph:  # noqa: E252
    log = logging.getLogger(__name__)
    try:
        if format_guess.lower() == "json-ld":
            graph = convert_jsonld(data)
        else:
            graph = rdflib.ConjunctiveGraph()
            graph.parse(data=data, format=format_guess.lower())
        return graph
    except (TypeError, rdflib.exceptions.ParserError):
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
    return None


def do_analyze_and_index(graph: rdflib.Graph, iri: str) -> None:
    log = logging.getLogger(__name__)
    if graph is None:
        log.debug("Graph is None for %s", iri)
        return

    log.debug("Analyze and index - execution: %s", iri)

    analyzers = [c for c in AbstractAnalyzer.__subclasses__() if hasattr(c, "token")]
    log.debug("Analyzers: %s", str(len(analyzers)))

    store = []
    for analyzer_class in analyzers:
        analyzer_token = analyzer_class.token
        log.debug("Analyze and index %s with %s", iri, analyzer_token)
        analyzer = analyzer_class()

        token, res = analyze_and_index_one(analyzer, analyzer_class, graph, iri, log)
        store.append({'iri': iri, 'analyzer': token, 'data': res})
        log.debug("Done analyze and index %s with %s", iri, analyzer_token)

    log.debug("Done processing %s, now storing", iri)
    if len(store) > 0:
        try:
            db_session.execute(insert(Analysis).values(store))
            db_session.commit()
        except:
            logging.getLogger(__name__).exception("Failed to store analyses in DB")
            db_session.rollback()
    log.info("Done storing %s", iri)


def analyze_and_index_one(
    analyzer, analyzer_class, graph, iri, log
) -> None:
    log.info("Analyzing %s with %s", iri, analyzer_class.token)
    with TimedBlock(f"analyze.{analyzer_class.token}"):
        res = analyzer.analyze(graph, iri)
    log.info(
        "Done analyzing %s with %s: %s", iri, analyzer_class.token, json.dumps(res)
    )

    log.debug("Find relations of %s in %s", analyzer_class.token, iri)
    try:
        iris_found = defaultdict(list)
        with TimedBlock(f"index.{analyzer_class.token}"):
            for common_iri, group, rel_type in analyzer.find_relation(graph):
                log.debug(
                    "Distribution: %s, relationship type: %s, common resource: %s, significant resource: %s",
                    iri,
                    rel_type,
                    common_iri,
                    group,
                )
                # TODO: group IRI not used
                iris_found[(rel_type, common_iri)].append(
                    iri
                )  # this is so that we sadd whole list in one call

        log.debug("Storing relations in DB")

        relationships = []
        for item in iris_found.items():
            (rel_type, key) = item[0]
            if rel_type is None or len(rel_type) == 0:
                continue
            if key is None or len(key) == 0:
                continue
            iris = item[1]
            # log.debug("Adding %s items into set", str(len(iris)))
            for iri in iris:
                if iri is not None and len(iri) > 0:
                    relationships.append({'type': rel_type, 'group': key, 'candidate': iri})
        if len(relationships) > 0:
            try:
                db_session.execute(insert(Relationship).values(relationships))
                db_session.commit()
            except:
                log.exception("Failed to store relations in DB")
                db_session.rollback()
    except TypeError:
        log.debug("Skip %s for %s", analyzer_class.token, iri)

    return analyzer_class.token, res

