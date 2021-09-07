import logging

import redis

from tsa.net import test_iri


class Index(object):
    def __init__(self, redis_pool, index_key, symmetric):
        self.__red = redis.Redis(connection_pool=redis_pool)
        self.__key = index_key
        self.__symmetric = symmetric

    def lookup(self, base_iri):
        try:
            if test_iri(base_iri):
                yielded_base = False
                for iri in self.__red.sscan_iter(self.__key(base_iri)):
                    if test_iri(iri):
                        yield iri
                        if iri == base_iri:
                            yielded_base = True
                if not yielded_base:
                    yield base_iri  # reflexivity
        except TypeError:
            logging.getLogger(__name__).exception(f'TypeError in lookup, iri: {base_iri}')

    def index(self, iri1, iri2):
        # iri1 owl:sameAs iri2
        with self.__red.pipeline() as pipe:  # symmetry
            pipe.sadd(self.__key(iri1), iri2)
            if self.__symmetric:
                pipe.sadd(self.__key(iri2), iri1)
            pipe.execute()

    def finalize(self):
        graph = {}
        for key in self.__red.scan_iter(match=self.__key('*')):
            iri = key[len(self.__key('')):].replace('_', ':', 1)
            neighbours = [x for x in self.__red.sscan_iter(key)]
            graph[iri] = neighbours

        for node in graph.keys():
            visited = self.__bfs(graph, node)
            with self.__red.pipeline() as pipe:
                # add all reachable nodes into index (transitivity)
                for iri in visited:
                    pipe.sadd(self.__key(node), iri)
                pipe.execute()

    def __bfs(self, graph, initial):
        visited = []
        queue = [initial]
        while queue:
            node = queue.pop(0)
            if node not in visited:
                visited.append(node)
                try:
                    neighbours = graph[node]
                    for neighbour in neighbours:
                        queue.append(neighbour)
                except KeyError:
                    logging.getLogger(__name__).warning(f'Key error in BFS: {node}, key: {self.__key("")}')
        return visited

    def export_index(self):
        result = {}
        for key in self.__red.scan_iter(match=self.__key('*')):
            iri = key[len(self.__key('')):]
            values = [x for x in self.__red.sscan_iter(key)]
            result[iri] = values
        return result

    def import_index(self, index):
        for key in self.__red.scan_iter(self.__key('*')):
            self.__red.delete(key)
        for key in index.keys():
            with self.__red.pipeline() as pipe:
                for value in index[key]:
                    pipe.sadd(self.__key(key), value)
                pipe.execute()
