"""Dataset analyzer."""

import logging
from abc import ABC
from collections import defaultdict


class AbstractAnalyzer(ABC):
    """Abstract base class allowing to fetch all available analyzers on runtime."""


class CubeAnalyzer(AbstractAnalyzer):
    """RDF dataset analyzer focusing on DataCube."""

    token = 'cube'

    def find_relation(self, graph):
        """We consider DSs to be related if they share a resource on dimension."""
        log = logging.getLogger(__name__)
        log.debug('Looking up resources used on a dimension')
        for ds, resource in self.__resource_on_dimension(graph):
            log.debug(f'Dataset: {ds} - Resource on dimension: {resource}')
            yield resource, 'qb'

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
        ds_dimensions = self.__dataset_dimensions(graph, self.__dimensions(graph))
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
                    yield row.dataset, row1.resource

    def analyze(self, graph):
        """Analysis of a datacube."""
        datasets = defaultdict(QbDataset)
        prefix = 'http://purl.org/linked-data/cube#'
        for row in graph.query(f'SELECT DISTINCT ?ds WHERE {{?ds a <{prefix}DataSet>}}'):
            dataset = row['ds']
            qa = f'SELECT DISTINCT ?structure WHERE {{ <{dataset}> <{prefix}structure> ?structure }}'
            for row in graph.query(qa):
                structure = row['structure']
                qb = f'SELECT DISTINCT ?component WHERE {{ <{structure}> <{prefix}component> ?component }}'
                for row in graph.query(qb):
                    component = row['component']

                    qc = f'SELECT DISTINCT ?dimension WHERE {{ <{component}> <{prefix}dimension> ?dimension }}'
                    for row in graph.query(qc):
                        dimension = row['dimension']
                        datasets[str(dataset)].dimensions.add(str(dimension))
                    qd = f'SELECT DISTINCT ?measure WHERE {{ <{component}> <{prefix}measure> ?measure }}'
                    for row in graph.query(qd):
                        measure = row['measure']
                        datasets[str(dataset)].measures.add(str(measure))

        d = {}
        for k in datasets.keys():
            d[k] = {}
            d[k]['dimensions'] = list(datasets[k].dimensions)
            d[k]['measures'] = list(datasets[k].measures)

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
        return f'SELECT ?a (count(?a) as ?count) WHERE {{ ?a ?b <{concept}>. }}'

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
        concept_count = dict()
        schemes_count = dict()
        top_concept = dict()

        concepts = [row['concept'] for row in graph.query("""
        SELECT DISTINCT ?concept WHERE {
            ?concept a <http://www.w3.org/2004/02/skos/core#Concept>.
        }
        """)]

        for c in concepts:
            for row in graph.query(SkosAnalyzer._count_query(c)):
                concept_count[c] = row['count']

        schemes = [row['scheme'] for row in graph.query("""
        SELECT DISTINCT ?scheme WHERE {
            OPTIONAL {?scheme a <http://www.w3.org/2004/02/skos/core#ConceptScheme>.}
            OPTIONAL {?_ <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme.}
        }
        """)]

        for schema in schemes:
            for row in graph.query(SkosAnalyzer._scheme_count_query(str(schema))):
                schemes_count[schema] = row['count']

        for schema in schemes:
            top_concept[schema] = [row['concept'] for row in graph.query(SkosAnalyzer._scheme_top_concept(str(schema)))]

        collections = [row['coll'] for row in graph.query("""
        SELECT DISTINCT ?coll WHERE {
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#Collection>. }
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#OrderedCollection>. }
            OPTIONAL { ?_ <http://www.w3.org/2004/02/skos/core#member> ?coll. }
            OPTIONAL { ?coll <http://www.w3.org/2004/02/skos/core#memberList> ?_. }
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
        q = 'SELECT DISTINCT ?scheme WHERE {?a <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme}'
        for row in graph.query(q):
            yield row['scheme'], 'inScheme'

        q = 'SELECT DISTINCT ?collection WHERE {?collection <http://www.w3.org/2004/02/skos/core#member> ?a}'
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

    def analyze(self, graph):
        """Basic graph analysis."""
        predicates_count = dict()
        classes_count = dict()

        triples = None
        for row in graph.query('select (COUNT(*) as ?c) where { ?s ?p ?o}'):
            triples = row['c']

        q = 'SELECT ?p (COUNT(?p) AS ?count) WHERE { ?s ?p ?o . } GROUP BY ?p ORDER BY DESC(?count)'
        for row in graph.query(q):
            predicates_count[row['p']] = row['count']

        for row in graph.query('SELECT ?c (COUNT(?c) AS ?count) WHERE { ?s a ?c . } GROUP BY ?c ORDER BY DESC(?count)'):
            classes_count[row['c']] = row['count']

        # external resource ::
        #   - objekty, ktere nejsou subjektem v tomto grafu
        #   - objekty, ktere nemaji typ v tomto grafu

        q = 'SELECT DISTINCT ?o WHERE { ?s ?p ?o . FILTER (URI (?o))}'
        objects = set([row['o'] for row in graph.query(q)])
        q = 'SELECT DISTINCT ?s WHERE { ?s ?p ?o. }'
        subjects = set([row['s'] for row in graph.query(q)])
        q = 'SELECT DISTINCT ?s WHERE { ?s a ?t. }'
        locally_typed = set([row['s'] for row in graph.query(q)])

        external_1 = objects.difference(subjects)
        external_2 = objects.difference(locally_typed)
        # toto muze byt SKOS Concept definovany jinde

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': classes_count,
            'external': {
                'not_subject': list(external_1),
                'no_type': list(external_2)
            }
        }
        return summary

    def find_relation(self, graph):
        """Two distributions are related if they share resources that are owl:sameAs."""
        for row in graph.query('SELECT ?a ?b WHERE { ?a <http://www.w3.org/2002/07/owl#sameAs> ?b }'):
            yield row['a'], 'sameAs'
            yield row['b'], 'sameAs'


class QbDataset(object):
    """Model for reporting DataCube dataset.

    The model contains sets of dimensions and measures used.
    """

    def __init__(self):
        """Init model by initializing sets."""
        self.dimensions = set()
        self.measures = set()
