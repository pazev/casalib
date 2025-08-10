"""
Module implements the Kahn topological sorting.
"""
from collections import defaultdict, deque
from typing import (
    Deque,
    DefaultDict,
    Dict,
    Hashable,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)


T = TypeVar("T", bound=Hashable)


class CycleError(ValueError):
    """Raised when a cycle is detected in the graph."""
    def __init__(
        self, message: str, nodes_in_cycle: List[List[T]]
    ) -> None:
        super().__init__(message)
        self.nodes_in_cycle = nodes_in_cycle


def kahn_toposort(
    edges: Iterable[Tuple[T, T]],
    nodes: Optional[Iterable[T]] = None,
) -> List[T]:
    """
    Topologically sort a directed acyclic graph (DAG) using
    Kahn's algorithm.

    Args:
        edges: Iterable of (u, v) directed edges meaning u
            must come before v.
        nodes: Optional Iterable with all nodes in the
            graph. If omitted, the node set is inferred from
            edges.

    Returns:
        A list with a valid topological ordering of nodes.

    Raises:
        CycleError: if the graph contains a cycle (i.e., it
        is not a DAG).
    """
    # Build adjacency (as sets to avoid counting duplicate
    # edges) and in-degrees
    adj: DefaultDict[T, Set[T]] = defaultdict(set)
    indeg: Dict[T, int] = {}

    # Initialize node universe
    if nodes is not None:
        for n in nodes:
            indeg.setdefault(n, 0)

    # Build graph
    for u, v in edges:
        if u not in indeg:
            indeg[u] = 0
        if v not in indeg:
            indeg[v] = 0
        # Only count an edge once
        if v not in adj[u]:
            adj[u].add(v)
            indeg[v] += 1

    # Queue of nodes with no incoming edges
    q: Deque[T] = deque(
        n for n, d in indeg.items() if d == 0
    )

    order: List[T] = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in adj[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    if len(order) != len(indeg):
        # Nodes still having indegree > 0 are part of (or
        # blocked by) a cycle
        cyclic = {n for n, d in indeg.items() if d > 0}
        raise CycleError(
            "Graph contains a cycle; topological sort not "
            "possible.",
            [list(cyclic)]
        )

    return order
