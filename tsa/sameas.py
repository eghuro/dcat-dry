import logging
from collections import defaultdict

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from tsa.db import db_session
from tsa.model import DDR
from tsa.util import check_iri


class Index:
    def __init__(self, index_key: str, symmetric: bool):
        self.__key = index_key
        self.__symmetric = symmetric
        if self.__key is None or len(self.__key) == 0:
            raise ValueError(self.__key)

    def snapshot(self):
        index = defaultdict(set)
        for record in db_session.query(DDR).filter_by(relationship_type=self.__key):
            if check_iri(record.iri2) and check_iri(record.iri1):
                index[record.iri1].add(record.iri2)
                index[record.iri2].add(record.iri1)
        return index

    def bulk_index(self, pairs):
        def gen_data():
            for (a, b) in pairs:
                if a is not None and len(a) > 0 and b is not None and len(b) > 0:
                    yield {"relationship_type": self.__key, "iri1": a, "iri2": b}
                    if self.__symmetric:
                        yield {
                            "relationship_type": self.__key,
                            "iri1": b,
                            "iri2": a,
                        }

        insert_stmt = insert(DDR).on_conflict_do_nothing()
        try:
            db_session.execute(insert_stmt, gen_data())
            db_session.commit()
        except SQLAlchemyError:
            logging.getLogger(__name__).exception(
                "Failed do commit, rolling back bulk index"
            )
            db_session.rollback()

    def finalize(self):

        try:

            def gen_values():
                graph = defaultdict(set)
                for ddr in db_session.query(DDR).filter_by(
                    relationship_type=self.__key
                ):
                    graph[ddr.iri1].add(ddr.iri2)
                for node in graph.keys():  # noqa: consider-iterating-dictionary
                    visited = self.__bfs(graph, node)
                    # add all reachable nodes into index (transitivity)
                    for iri in visited:
                        yield {
                            "iri1": node,
                            "iri2": iri,
                            "relationship_type": self.__key,
                        }

            insert_stmt = insert(DDR).on_conflict_do_nothing()
            try:
                db_session.execute(insert_stmt, gen_values())
                db_session.commit()
            except SQLAlchemyError:
                logging.getLogger(__name__).exception("Failed fo finalize index")
                db_session.rollback()
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
                        "Key error in BFS: %s, key: %s", node, self.__key
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


same_as_index = Index("sameAs", True)
