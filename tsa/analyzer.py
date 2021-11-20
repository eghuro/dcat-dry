"""Dataset analyzer."""

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable, DefaultDict, Generator, Optional, Tuple

import rdflib
import redis
from rdflib import RDF, Graph, Literal, URIRef

from tsa.extensions import concept_index, ddr_index, dsd_index, redis_pool, same_as_index
from tsa.redis import description as desc_query
from tsa.redis import label as label_query
from tsa.redis import resource_type
from tsa.util import check_iri


class AbstractAnalyzer(ABC):
    """Abstract base class allowing to fetch all available analyzers on runtime."""

    @abstractmethod
    def find_relation(self, graph: Graph) -> Optional[Generator[Tuple[str, str, str], None, None]]:
        pass

    @abstractmethod
    def analyze(self, graph: Graph, iri: str) -> dict:
        pass


class QbDataset:
    """Model for reporting DataCube dataset.

    The model contains sets of dimensions and measures used.
    """

    def __init__(self):
        """Init model by initializing sets."""
        self.dimensions = set()
        self.measures = set()


class CubeAnalyzer(AbstractAnalyzer):
    """RDF dataset analyzer focusing on DataCube."""

    token = 'cube'  # nosec

    def find_relation(self, graph: Graph) -> Generator[Tuple[str, str, str], None, None]:
        """We consider DSs to be related if they share a resource on dimension."""
        log = logging.getLogger(__name__)
        log.debug('Looking up significant resources')
        query = """
        SELECT DISTINCT ?component ?resource
        WHERE {
            ?_ a <http://purl.org/linked-data/cube#Observation>; ?component ?resource.
            FILTER (isIRI(?resource))
        }
        """
        qres = graph.query(query)
        for row in qres:
            yield str(row.resource), str(row.component), 'qb'

    @staticmethod
    def __dimensions(graph: Graph) -> DefaultDict:
        dimensions = defaultdict(set)
        qb_query = """
        SELECT DISTINCT ?dsd ?dimension
        WHERE {
            ?dsd <http://purl.org/linked-data/cube#component>/<http://purl.org/linked-data/cube#dimension> ?dimension.
        }
        """
        qres = graph.query(qb_query)
        for row in qres:
            dimensions[str(row.dsd)].add(str(row.dimension))

        return dimensions

    @staticmethod
    def __dataset_structures(graph: Graph, structures: DefaultDict) -> DefaultDict:
        dataset_structures = defaultdict(set)
        qb_query = """
        SELECT DISTINCT ?ds ?structure
        WHERE {
            ?ds <http://purl.org/linked-data/cube#structure> ?structure.
        }
        """
        qres = graph.query(qb_query)
        for row in qres:
            if str(row.structure) in structures.keys():
                dataset_structures[str(row.ds)].update(structures[str(row.structure)])

        return dataset_structures

    @staticmethod
    def __resource_on_dimension(graph: Graph) -> Generator[Tuple[URIRef, URIRef, str], None, None]:
        log = logging.getLogger(__name__)
        log.debug('Looking up resources on dimensions')
        ds_dimensions = CubeAnalyzer.__dataset_structures(graph, CubeAnalyzer.__dimensions(graph))
        log.debug('Dimensions: %s', ds_dimensions)

        ds_query = """
            SELECT DISTINCT ?observation ?dataset
            WHERE {
                ?observation <http://purl.org/linked-data/cube#dataSet> ?dataset.
            }
        """
        # see http://www.w3.org/TR/2014/REC-vocab-data-cube-20140116/ -> qb:dataSet ( Domain: qb:Observation -> Range: qb:DataSet )
        qres0 = graph.query(ds_query)
        for row in qres0:
            for dimension in ds_dimensions[str(row.dataset)]:
                qb_query = f'SELECT ?resource WHERE {{ <{row.observation!s}> <{dimension!s}> ?resource. }}'
                qres1 = graph.query(qb_query)
                for row1 in qres1:
                    yield str(row.dataset), str(row1.resource), str(dimension)

    @staticmethod
    def __datasets_queried(graph: Graph) -> DefaultDict[str, QbDataset]:
        datasets_queried = defaultdict(QbDataset)  # type: DefaultDict[str, QbDataset]
        query = """
        PREFIX qb: <http://purl.org/linked-data/cube#>
        SELECT DISTINCT ?ds ?dimension ?measure WHERE {
        ?ds qb:structure/qb:component ?component.
        { ?component qb:dimension ?dimension. } UNION { ?component qb:measure ?measure. }
        }
        """
        for row in graph.query(query):
            dataset = str(row['ds'])
            dimension = str(row['dimension'])
            measure = str(row['measure'])
            datasets_queried[dataset].dimensions.add(dimension)
            datasets_queried[dataset].measures.add(measure)
        return datasets_queried

    def analyze(self, graph: Graph, iri: str) -> dict:
        """Analysis of a datacube."""
        resource_dimension = defaultdict(set)
        for _, resource, dimension in CubeAnalyzer.__resource_on_dimension(graph):
            resource_dimension[str(dimension)].add(str(resource))

        datasets_processed = []
        # in the query above either dimension or measure could have been None and still added into set, cleaning here
        none = str(None)
        for dataset_iri, dataset in CubeAnalyzer.__datasets_queried(graph).items():
            dataset.dimensions.discard(none)
            dataset.measures.discard(none)

            datasets_processed.append({
                'dataset_iri': dataset_iri,
                'dimensions': [{'dimension': dimension, 'resources': list(resource_dimension[dimension])} for dimension in dataset.dimensions],
                'measures': list(dataset.measures)
            })

        summary = {
            'datasets_queried': datasets_processed
        }

        dsd_index.index(datasets_processed, iri)

        return summary


class SkosAnalyzer(AbstractAnalyzer):
    """RDF dataset analyzer focusing on SKOS."""

    token = 'skos'  # nosec

    @staticmethod
    def _scheme_count_query(scheme: str) -> str:
        return f'SELECT (count(*) as ?count) WHERE {{ ?_ <http://www.w3.org/2004/02/skos/core#inScheme> <{scheme}> }}'

    @staticmethod
    def _count_query(concept: str) -> str:
        return f'SELECT DISTINCT ?a (count(?a) as ?count) WHERE {{ OPTIONAL {{ ?a ?b <{concept}>.}} OPTIONAL {{ <{concept}> ?b ?a.}} }}'

    @staticmethod
    def _scheme_top_concept(scheme: str) -> str:
        return """
        SELECT ?concept WHERE {
            OPTIONAL { ?concept <http://www.w3.org/2004/02/skos/core#topConceptOf>
        """ + f'<{scheme}>.}}' + """
            OPTIONAL {
        """ + f'<{scheme}>' + """
            <http://www.w3.org/2004/02/skos/core#hasTopConcept> ?concept }
        }
        """

    def analyze(self, graph: Graph, iri: str) -> dict:
        """Analysis of SKOS concepts and related properties presence in a dataset."""
        log = logging.getLogger(__name__)

        concepts = [str(row['concept']) for row in graph.query("""
        SELECT DISTINCT ?concept WHERE {
            OPTIONAL {?concept a <http://www.w3.org/2004/02/skos/core#Concept>.}
            OPTIONAL {?concept <http://www.w3.org/2004/02/skos/core#inScheme> ?_. }
        }
        """)]

        concept_count = []
        for concept_iri in concepts:
            if not check_iri(concept_iri):
                log.debug('%s is not a valid IRI', concept_iri)
                continue
            for row in graph.query(SkosAnalyzer._count_query(concept_iri)):
                concept_count.append({'iri': concept_iri, 'count': row['count']})

        schemes = [row['scheme'] for row in graph.query("""
        SELECT DISTINCT ?scheme WHERE {
            OPTIONAL {?scheme a <http://www.w3.org/2004/02/skos/core#ConceptScheme>.}
            OPTIONAL {?_ <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme.}
        }
        """)]

        schemes_count, top_concept = [], []
        for schema in schemes:
            if not check_iri(schema):
                log.debug('%s is a not valid IRI', schema)
                continue
            for row in graph.query(SkosAnalyzer._scheme_count_query(str(schema))):
                schemes_count.append({'iri': schema, 'count': row['count']})

            top_concept.extend([{'schema': schema, 'concept': str(row['concept'])} for row in graph.query(SkosAnalyzer._scheme_top_concept(str(schema)))])

        collections = [str(row['coll']) for row in graph.query("""
        SELECT DISTINCT ?coll WHERE {
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#Collection>. }
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#OrderedCollection>. }
            OPTIONAL { ?a <http://www.w3.org/2004/02/skos/core#member> ?coll. }
            OPTIONAL { ?coll <http://www.w3.org/2004/02/skos/core#memberList> ?b. }
        }
        """)]

        ord_collections = [str(row['coll']) for row in graph.query("""
        SELECT DISTINCT ?coll WHERE {
            ?coll a <http://www.w3.org/2004/02/skos/core#OrderedCollection>.
        }
        """)]

        return {
            'concept': concept_count,
            'schema': schemes_count,
            'topConcepts': top_concept,
            'collection': collections,
            'orderedCollection': ord_collections
        }

    def find_relation(self, graph: Graph) -> None:
        """Lookup relationships based on SKOS vocabularies.

        Datasets are related if they share a resources that are:
            - in the same skos:scheme
            - in the same skos:collection
            - skos:exactMatch
            - related by skos:related, skos:semanticRelation, skos:broader,
        skos:broaderTransitive, skos:narrower, skos:narrowerTransitive
        """
        # -> zde do structure indexu
        concepts = [str(row['concept']) for row in graph.query("""
        SELECT DISTINCT ?concept WHERE {
            OPTIONAL {?concept a <http://www.w3.org/2004/02/skos/core#Concept>.}
            OPTIONAL {?concept <http://www.w3.org/2004/02/skos/core#inScheme> ?_. }
        }
        """)]

        for concept_iri in concepts:
            if check_iri(concept_iri):
                concept_index.index(concept_iri)

        query = 'SELECT ?a ?scheme WHERE {?a <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme.}'
        for row in graph.query(query):
            ddr_index.index('inScheme', str(row['scheme']), str(row['a']))

        query = 'SELECT ?collection ?a WHERE {?collection <http://www.w3.org/2004/02/skos/core#member> ?a. }'
        for row in graph.query(query):
            ddr_index.index('member', str(row['collection']), str(row['a']))
            concept_index.index(str(row['a']))

        for token in ['exactMatch', 'mappingRelation', 'closeMatch', 'relatedMatch']:
            for row in graph.query(f'SELECT ?a ?b WHERE {{ ?a <http://www.w3.org/2004/02/skos/core#{token}> ?b. }}'):
                ddr_index.index(token, str(row['a']), str(row['b']))
                concept_index.index(str(row['a']))
                concept_index.index(str(row['b']))

        for row in graph.query("""
        SELECT ?a ?b WHERE {
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#related> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#semanticRelation> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#broader> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#broaderTransitive> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#narrower> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#narrowerTransitive> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#broadMatch> ?b.}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#narrowMatch> ?b.}
        }
        """):
            ddr_index.index('broadNarrow', str(row['a']), str(row['b']))
            concept_index.index(str(row['a']))
            concept_index.index(str(row['b']))


class GenericAnalyzer(AbstractAnalyzer):
    """Basic RDF dataset analyzer inspecting general properties not related to any particular vocabulary."""

    token = 'generic'  # nosec

    @staticmethod
    def _count(graph: Graph) -> Tuple[int, DefaultDict, DefaultDict, list, list, list]:
        triples = 0
        predicates_count = defaultdict(int)  # type: DefaultDict[str, int]
        classes_count = defaultdict(int)  # type: DefaultDict[str, int]
        objects, subjects, locally_typed = [], [], []

        for subject, predicate, objekt in graph:  # object is reserved
            triples += 1
            pred = str(predicate)
            obj = str(objekt)
            sub = str(subject)

            if check_iri(pred):
                predicates_count[pred] += 1

            if check_iri(obj):
                if predicate == RDF.type:
                    if check_iri(sub):
                        classes_count[obj] += 1
                        locally_typed.append(sub)
                else:
                    objects.append(obj)

            if check_iri(sub):
                subjects.append(sub)

        return triples, predicates_count, classes_count, objects, subjects, locally_typed

    def analyze(self, graph: Graph, iri: str) -> dict:  # noqa: unused-variable
        """Basic graph analysis."""
        triples, initial_predicates_count, initial_classes_count, objects_list, subjects_list, locally_typed_list = self._count(graph)
        predicates_count = [{'iri': iri, 'count': count} for (iri, count) in initial_predicates_count.items()]
        classes_count = [{'iri': iri, 'count': count} for (iri, count) in initial_classes_count.items()]

        # external resource ::
        #   - objekty, ktere nejsou subjektem v tomto grafu
        #   - objekty, ktere nemaji typ v tomto grafu

        objects = set(objects_list)
        subjects = set(subjects_list)
        locally_typed = set(locally_typed_list)

        external_1 = objects.difference(subjects)
        external_2 = objects.difference(locally_typed)
        # toto muze byt SKOS Concept definovany jinde

        self.get_details(graph)

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': classes_count,
            'subjects': list(subjects),
            'objects': list(objects),
            'external': {
                'not_subject': list(external_1),
                'no_type': list(external_2)
            },
            'internal': list(objects.difference(external_1.union(external_2)))
        }
        return summary

    def get_details(self, graph: Graph) -> None:
        query = """
        SELECT DISTINCT ?x ?label ?type ?description WHERE {
        OPTIONAL { ?x <http://www.w3.org/2000/01/rdf-schema#label> ?label }
        OPTIONAL { ?x <http://www.w3.org/2004/02/skos/core#prefLabel> ?label }
        OPTIONAL { ?x <http://www.w3.org/2004/02/skos/core#altLabel> ?label }
        OPTIONAL { ?x <http://schema.org/name> ?label }
        OPTIONAL { ?x <http://schema.org/alternateName> ?label }
        OPTIONAL { ?x <http://purl.org/dc/terms/title> ?label }
        OPTIONAL { ?x a ?type. FILTER (isIRI(?type)) }
        OPTIONAL { ?x <http://www.w3.org/2000/01/rdf-schema#comment> ?description }
        }
        """
        red = redis.Redis(connection_pool=redis_pool)

        for row in graph.query(query):
            iri = str(row['x'])
            if check_iri(iri):
                self._extract_detail(row, iri, red)

    def _extract_detail(self, row: rdflib.query.Result, iri: str, red: redis.client.Redis) -> None:
        with red.pipeline() as pipe:
            self.extract_label(str(row['label']), iri, pipe, label_query)
            self.extract_label(str(row['description']), iri, pipe, desc_query)

            type_of_iri = row['type']
            if type_of_iri is not None:
                key = resource_type(iri)
                pipe.sadd(key, type_of_iri)
            pipe.execute()

    @staticmethod
    def extract_label(literal: Any, iri: str, pipe: redis.client.Pipeline, query: Callable) -> None:
        if literal is not None and isinstance(literal, Literal):
            value, language = literal.value, literal.language
            key = query(iri, language)
            pipe.set(key, value)

    def find_relation(self, graph: Graph) -> None:
        """Two distributions are related if they share resources that are owl:sameAs."""
        for row in graph.query('SELECT DISTINCT ?a ?b WHERE { ?a <http://www.w3.org/2002/07/owl#sameAs> ?b. }'):
            same_as_index.index(str(row['a']), str(row['b']))


class SchemaHierarchicalGeoAnalyzer(AbstractAnalyzer):

    token = 'schema-hierarchical-geo'  # nosec

    def find_relation(self, graph: Graph) -> Generator[Tuple[str, str, str], None, None]:
        query = """
        PREFIX schema: <http://schema.org/>
        SELECT ?what ?place WHERE {
            ?what schema:containedInPlace ?place.
        }
        """
        for row in graph.query(query):
            what = str(row['what'])
            place = str(row['place'])
            yield place, what, 'containedInPlace'

    def analyze(self, graph: Graph, iri: str) -> dict:
        return {}


class AbstractEnricher(AbstractAnalyzer):

    def find_relation(self, graph: Graph) -> None:
        pass  # enrichers do not discover relations


class TimeAnalyzer(AbstractEnricher):
    token = 'time'  # nosec

    def analyze(self, graph: Graph, iri: str) -> dict:  # noqa: unused-variable
        query = """
        PREFIX interval: <http://reference.data.gov.uk/def/intervals/>
        SELECT ?day_iri ?day ?month ?year
        WHERE {
            ?day_iri a interval:Day;
            interval:ordinalDayOfMonth ?day;
            interval:ordinalMonthOfYear ?month;
            interval:ordinalYear ?year
        }
        """
        result = {}
        for row in graph.query(query):
            day_iri = str(row['day_iri'])
            day = str(row['day']).zfill(2)
            month = str(row['month']).zfill(2)
            year = str(row['year'])
            result[day_iri] = f'{year}-{month}-{day}'
        return result


class RuianAnalyzer(AbstractEnricher):
    token = 'ruian'  # nosec

    def analyze(self, graph: Graph, iri: str) -> dict:  # noqa: unused-variable
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX gml: <http://www.opengis.net/ont/gml#>
        SELECT DISTINCT ?iri ?name ?type_iri ?type_label ?longitude ?latitude WHERE {
            ?iri schema:name ?name; a ?type_iri; geo:hasGeometry/gml:pointMember/schema:geo ?geo.
            ?geo schema:longitude ?longitude.
            ?geo schema:latitude ?latitude.
            ?type_iri skos:prefLabel ?type_label.
        }
        """
        result = {}
        ruian_prefix = 'https://linked.cuzk.cz/resource/ruian/'
        for row in graph.query(query):
            ruian_iri = str(row['iri'])
            name = str(row['name'])
            ruian_type_iri = str(row['type_iri'])
            ruian_type_label = str(row['type_label'])
            longitude = str(row['longitude'])
            latitude = str(row['latitude'])
            if ruian_iri.startswith(ruian_prefix) and ruian_type_iri.startswith(ruian_prefix):
                result[ruian_iri] = {
                    'iri': ruian_iri,
                    'name': name,
                    'type': {
                        'iri': ruian_type_iri,
                        'name': ruian_type_label
                    },
                    'latitude': latitude,
                    'longitude': longitude
                }
        return result
