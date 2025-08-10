"""
Graph class implementation
"""
from collections import defaultdict
from dataclasses import dataclass, field

from typing import (
    Generic,
    Hashable,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    Tuple,
)

from .johnson_cycle_detection import simple_cycles
from .kahn_toposort import kahn_toposort, CycleError


T = TypeVar("T", bound=Hashable)


@dataclass
class Graph(Generic[T]):
    """ Graph structure """
    relations: defaultdict[T, Set[T]] = field(
        default_factory=lambda: defaultdict(set)
    )

    @classmethod
    def from_edges_nodes(
        cls,
        edges: Iterable[Tuple[T, T]],
        nodes: Optional[Iterable[T]] = None
    ) -> "Graph":
        """ Create a graph from edges and nodes """
        graph = cls()
        nodes = nodes or []
        for u, v in edges:
            graph.add_edge(u, v)
        for u in nodes:
            graph.add_node(u)
        return graph

    def add_node(self, node: T) -> "Graph":
        """ Add a node to the graph """
        _ = self.relations[node]
        return self

    def add_edge(
        self, origin: T, destination: T,
    ) -> "Graph":
        """ Add an edge to the graph """
        _ = (
            self.relations[origin],
            self.relations[destination]
        )
        self.relations[origin].add(destination)
        return self

    def get_edges(self) -> List[Tuple[T, T]]:
        """ Return the edges list """
        return [
            (orig, dest)
            for orig, dest_list in self.relations.items()
            for dest in dest_list
        ]

    def get_nodes(self) -> List[T]:
        """ Return the nodes """
        return list(self.relations)

    def get_cycles(self) -> List[List[T]]:
        """ Detect if graph has cycle """
        return list(
            simple_cycles(
                edges=self.get_edges(),
                nodes=self.get_nodes()
            )
        )

    def toposort(self) -> List[T]:
        """ Topological sort the nodes """
        try:
            sorted_ = kahn_toposort(
                edges=self.get_edges(),
                nodes=self.get_nodes()
            )
        except CycleError as exc:
            get_cycles = self.get_cycles()
            raise CycleError(
                message="Cycles detected in graph. Please "
                        f"check. ({get_cycles}).",
                nodes_in_cycle=get_cycles
            ) from exc
        except Exception as exc:
            raise exc

        return sorted_
