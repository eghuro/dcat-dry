"""Dataset analyzer."""

import logging
from abc import ABC
from collections import defaultdict

import redis
import rfc3987

from tsa.extensions import redis_pool
from tsa.redis import ds_title


class AbstractAnalyzer(ABC):
    """Abstract base class allowing to fetch all available analyzers on runtime."""


class CubeAnalyzer(AbstractAnalyzer):
    """RDF dataset analyzer focusing on DataCube."""

    token = 'cube'
    relations = ['qb']

    def find_relation(self, graph):
        """We consider DSs to be related if they share a resource on dimension."""
        log = logging.getLogger(__name__)
        log.debug('Looking up resources used on a dimension')
        for ds, resource, dimension, structure in self.__resource_on_dimension(graph):
            # log.debug(f'Dataset: {ds} - Resource on dimension: {resource}')

            # relation type, structure, structural element, element
            yield 'qb', structure, dimension, resource

    def __dimensions(self, graph):
        d = defaultdict(set)
        qb_query = """
        SELECT ?dsd ?dimension
        WHERE {
            ?dsd a <http://purl.org/linked-data/cube#DataStructureDefinition>;
            <http://purl.org/linked-data/cube#component>/<http://purl.org/linked-data/cube#dimension> ?dimension.
        }
        """
        qres = graph.query(qb_query)
        for row in qres:
            d[row.dsd].add(row.dimension)

        return d

    def __dataset_dimensions(self, graph, dimensions):
        d = defaultdict(set)
        qb_query = """
        SELECT ?ds ?structure
        WHERE {
            ?ds a <http://purl.org/linked-data/cube#DataSet>;
            <http://purl.org/linked-data/cube#structure> ?structure.
        }
        """
        qres = graph.query(qb_query)
        for row in qres:
            if row.structure in dimensions.keys():
                d[row.ds].update(dimensions[row.structure])

        return d

    def __resource_on_dimension(self, graph):
        log = logging.getLogger(__name__)
        log.debug('Looking up resources on dimensions')
        dim = self.__dimensions(graph)   # keys: structures, values: dimensions
        log.debug(f'Datasets: {dim.keys()!s}')

        #prepare reverse mapping dimension -> structure
        ds_for_dimension = defaultdict(set)
        for ds in dim.keys():
            for dim in dim[ds]:
                ds_for_dimension[dim].add(ds)

        ds_dimensions = self.__dataset_dimensions(graph, dim)
        log.debug(f'Dimensions: {ds_dimensions!s}')

        ds_query = """
            SELECT ?observation ?dataset
            WHERE {
                ?observation a <http://purl.org/linked-data/cube#Observation>;
                <http://purl.org/linked-data/cube#dataSet> ?dataset.
            }
        """
        qres0 = graph.query(ds_query)
        for row in qres0:
            for dimension in ds_dimensions[row.dataset]:
                qb_query = f'SELECT ?resource WHERE {{ <{row.observation!s}> <{dimension!s}> ?resource. }}'
                qres1 = graph.query(qb_query)
                for row1 in qres1:
                    for ds in ds_for_dimension[dimension]:
                        yield row.dataset, row1.resource, dimension, ds

    def analyze(self, graph):
        """Analysis of a datacube."""
        datasets = defaultdict(QbDataset)
        q = '''
        PREFIX qb: <http://purl.org/linked-data/cube#>
        SELECT DISTINCT ?ds ?dimension ?measure WHERE {
        ?ds a qb:DataSet; qb:structure ?structure.
        ?structure qb:component ?component.
        { ?component qb:dimension ?dimension. } UNION { ?component qb:measure ?measure. }
        }
        '''
        for row in graph.query(q):
            dataset = str(row['ds'])
            dimension = str(row['dimension'])
            measure = str(row['measure'])
            datasets[dataset].dimensions.add(dimension)
            datasets[dataset].measures.add(measure)

        resource_dimension = defaultdict(set)
        for dataset, resource, dimension in self.__resource_on_dimension(graph):
            resource_dimension[str(dimension)].add(str(resource))

        d = []
        # in the query above either dimension or measure could have been None and still added into set, cleaning here
        none = str(None)
        for k in datasets.keys():
            datasets[k].dimensions.discard(none)
            datasets[k].measures.discard(none)

            d.append({
                'iri': k,
                'dimensions': [{'dimension': dimension, 'resources': list(resource_dimension[dimension])} for dimension in datasets[k].dimensions],
                'measures': list(datasets[k].measures)
            })

        summary = {
            'datasets': d
        }

        return summary


class SkosAnalyzer(AbstractAnalyzer):
    """RDF dataset analyzer focusing on SKOS."""

    token = 'skos'
    relations = ['inScheme', 'collection', 'exactMatch', 'mappingRelation', 'closeMatch', 'relatedMatch', 'broadNarrow']

    @staticmethod
    def _scheme_count_query(scheme):
        return f'SELECT (count(*) as ?count) WHERE {{ ?_ <http://www.w3.org/2004/02/skos/core#inScheme> <{scheme}> }}'

    @staticmethod
    def _count_query(concept):
        return f'SELECT DISTINCT ?a (count(?a) as ?count) WHERE {{ ?a ?b <{concept}>. }}'

    @staticmethod
    def _scheme_top_concept(scheme):
        q = """
        SELECT ?concept WHERE {
            OPTIONAL { ?concept <http://www.w3.org/2004/02/skos/core#topConceptOf>
        """ + f'<{scheme}>.}}' + """
            OPTIONAL {
        """ + f'<{scheme}>' + """
            <http://www.w3.org/2004/02/skos/core#hasTopConcept> ?concept }
        }
        """
        return q

    def analyze(self, graph):
        """Analysis of SKOS concepts and related properties presence in a dataset."""
        log = logging.getLogger(__name__)
        top_concept = dict()

        concepts = [row['concept'] for row in graph.query("""
        SELECT DISTINCT ?concept WHERE {
            ?concept a <http://www.w3.org/2004/02/skos/core#Concept>.
        }
        """)]

        concept_count = []
        for c in concepts:
            if not rfc3987.match(c):
                log.debug(f'{c} is a not valid IRI')
                continue
            for row in graph.query(SkosAnalyzer._count_query(c)):
                concept_count.append({'iri': c, 'count': row['count']})

        schemes = [row['scheme'] for row in graph.query("""
        SELECT DISTINCT ?scheme WHERE {
            OPTIONAL {?scheme a <http://www.w3.org/2004/02/skos/core#ConceptScheme>.}
            OPTIONAL {?_ <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme.}
        }
        """)]

        schemes_count, top_concept = [], []
        for schema in schemes:
            if not rfc3987.match(schema):
                log.debug(f'{schema} is a not valid IRI')
                continue
            for row in graph.query(SkosAnalyzer._scheme_count_query(str(schema))):
                schemes_count.append({'iri': schema, 'count': row['count']})

            top_concept.extend([{'schema':schema, 'concept': row['concept']} for row in graph.query(SkosAnalyzer._scheme_top_concept(str(schema)))])

        collections = [row['coll'] for row in graph.query("""
        SELECT DISTINCT ?coll WHERE {
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#Collection>. }
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#OrderedCollection>. }
            OPTIONAL { ?a <http://www.w3.org/2004/02/skos/core#member> ?coll. }
            OPTIONAL { ?coll <http://www.w3.org/2004/02/skos/core#memberList> ?b. }
        }
        """)]

        ord_collections = [row['coll'] for row in graph.query("""
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

    def find_relation(self, graph):
        """Lookup relationships based on SKOS vocabularies.

        Datasets are related if they share a resources that are:
            - in the same skos:scheme
            - in the same skos:collection
            - skos:exactMatch
            - related by skos:related, skos:semanticRelation, skos:broader,
        skos:broaderTransitive, skos:narrower, skos:narrowerTransitive
        """
        q = 'SELECT DISTINCT ?scheme WHERE {?a <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme.}'
        for row in graph.query(q):
            yield row['scheme'], 'inScheme'

        q = 'SELECT DISTINCT ?collection WHERE {?collection <http://www.w3.org/2004/02/skos/core#member> ?a. }'
        for row in graph.query(q):
            yield row['collection'], 'collection'

        for token in ['exactMatch', 'mappingRelation', 'closeMatch', 'relatedMatch']:
            for row in graph.query(f'SELECT ?a ?b WHERE {{ ?a <http://www.w3.org/2004/02/skos/core#{token}> ?b. }}'):
                yield row['a'], token
                yield row['b'], token

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
            yield row['a'], 'broadNarrow'
            yield row['b'], 'broadNarrow'


class GenericAnalyzer(AbstractAnalyzer):
    """Basic RDF dataset analyzer inspecting general properties not related to any particular vocabulary."""

    token = 'generic'
    relations = ['sameAs', 'seeAlso']

    def analyze(self, graph):
        """Basic graph analysis."""

        triples = 0
        for row in graph.query('select (COUNT(*) as ?c) where { ?s ?p ?o}'):
            triples = int(row['c'])

        predicates_count = []
        q = 'SELECT DISTINCT ?p (COUNT(?p) AS ?count) WHERE { ?s ?p ?o . } GROUP BY ?p ORDER BY DESC(?count)'
        for row in graph.query(q):
            predicates_count.append({'iri': row['p'], 'count': row['count']})

        classes_count = []
        for row in graph.query('SELECT DISTINCT ?c (COUNT(?c) AS ?count) WHERE { ?s a ?c . } GROUP BY ?c ORDER BY DESC(?count)'):
            classes_count.append({'iri': row['c'], 'count': row['count']})

        # external resource ::
        #   - objekty, ktere nejsou subjektem v tomto grafu
        #   - objekty, ktere nemaji typ v tomto grafu

        q = 'SELECT DISTINCT ?o WHERE { ?s ?p ?o. FILTER (isIRI(?o))}'
        objects = set([row['o'] for row in graph.query(q)])
        q = 'SELECT DISTINCT ?s WHERE { ?s ?p ?o. FILTER (isIRI(?o))}'
        subjects = set([row['s'] for row in graph.query(q)])
        q = 'SELECT DISTINCT ?s WHERE { ?s a ?c. FILTER (isIRI(?c))}'
        locally_typed = set([row['s'] for row in graph.query(q)])

        external_1 = objects.difference(subjects)
        external_2 = objects.difference(locally_typed)
        # toto muze byt SKOS Concept definovany jinde

        q = '''
        SELECT ?x ?label WHERE {
        OPTIONAL { ?x <http://www.w3.org/2000/01/rdf-schema#label> ?label }
        OPTIONAL { ?x <http://www.w3.org/2004/02/skos/core#prefLabel> ?label }
        OPTIONAL { ?x <http://www.w3.org/2004/02/skos/core#altLabel> ?label }
        }
        '''
        red = redis.Redis(connection_pool=redis_pool)
        with red.pipeline() as pipe:
            for row in graph.query(q):
                iri = row['x']
                label = row['label']
                if '@' in label:
                    value, language = label.split('@')
                else:
                    value, language = label, None
                key = ds_title(iri, language)
                pipe.set(key, value)
            pipe.execute()

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': classes_count,
            'subjects': list(subjects),
            'external': {
                'not_subject': list(external_1),
                'no_type': list(external_2)
            }
        }
        return summary

    def find_relation(self, graph):
        """Two distributions are related if they share resources that are owl:sameAs."""
        for row in graph.query('SELECT ?a ?b WHERE { ?a <http://www.w3.org/2002/07/owl#sameAs> ?b. }'):
            yield row['a'], 'sameAs'
            yield row['b'], 'sameAs'

        for row in graph.query('SELECT ?a ?b WHERE { {?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#seeAlso> ?b.} UNION {?a <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?b.} }'):
            yield row['a'], 'seeAlso'
            yield row['b'], 'seeAlso'


class SchemaHierarchicalGeoAnalyzer(AbstractAnalyzer):

    token = 'schema-hierarchical-geo'
    relations = ['containedInPlace']

    def find_relation(self, graph):
        q = '''
        PREFIX schema: <http://schema.org/>
        SELECT ?x ?place WHERE {
            ?x schema:containedInPlace ?place.
        }
        '''
        for row in graph.query(q):
            x = str(row['x'])
            place = str(row['place'])
            yield x, 'containedInPlace'
            yield place, 'containedInPlace'

    def analyze(self, graph):
        return {}


class QbDataset(object):
    """Model for reporting DataCube dataset.

    The model contains sets of dimensions and measures used.
    """

    def __init__(self):
        """Init model by initializing sets."""
        self.dimensions = set()
        self.measures = set()
