"""
Graph class implementation
"""
from collections import defaultdict
from dataclasses import dataclass, field

from typing import (
    Generic,
    Hashable,
    List,
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

    def sort_nodes(self) -> List[T]:
        """ Sort the nodes """
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
