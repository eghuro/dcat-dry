import logging
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from tsa.db import db_session
from tsa.extensions import db
from tsa.model import DDR
from tsa.util import check_iri


class Index:
    def __init__(self, index_key, symmetric):
        self.__key = index_key
        self.__symmetric = symmetric

    def lookup(self, base_iri):
        try:
            if check_iri(base_iri):
                yielded_base = False
                for iri in db_session.query(DDR.iri2).filter_by(relationship_type=self.__key, iri1=base_iri).distinct():
                    if check_iri(iri):
                        yield iri
                        if iri == base_iri:
                            yielded_base = True
                if not yielded_base:
                    yield base_iri  # reflexivity
        except TypeError:
            logging.getLogger(__name__).exception(
                "TypeError in lookup, iri: %s", base_iri
            )

    def index(self, iri1, iri2):
        # iri1 owl:sameAs iri2
        db_session.add(DDR(relationship_type=self.__key, iri1=iri1, iri2=iri2))
        if self.__symmetric:
            db_session.add(DDR(relationship_type=self.__key, iri1=iri2, iri2=iri1))
        db_session.commit()

    def finalize(self):
        graph = defaultdict(set)
        try:
            for ddr in db_session.query(DDR).filter_by(relationship_type=self.__key):
                graph[ddr.iri1].add(ddr.iri2)

            for node in graph.keys():  # noqa: consider-iterating-dictionary
                visited = self.__bfs(graph, node)
                # add all reachable nodes into index (transitivity)
                for iri in visited:
                    db_session.add(DDR(iri1=node, iri2=iri, relationship_type=self.__key))
                db_session.commit()
        except:
            logging.getLogger(__name__).exception("Oops")

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
                    logging.getLogger(__name__).warning(
                        "Key error in BFS: %s, key: %s", node, self.__key("")
                    )
        return visited

    def export_index(self):
        raise ValueError()
        result = {}
        for key in self.__red.scan_iter(match=self.__key("*")):
            iri = key[len(self.__key("")) :]
            values = list(self.__red.sscan_iter(key))
            result[iri] = values
        return result

    def import_index(self, index):
        raise ValueError()
        for key in self.__red.scan_iter(self.__key("*")):
            self.__red.delete(key)
        for key in index.keys():
            with self.__red.pipeline() as pipe:
                for value in index[key]:
                    pipe.sadd(self.__key(key), value)
                pipe.execute()

same_as_index = Index('sameAs', True)