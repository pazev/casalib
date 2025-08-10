"""
Implementation of Johnson algorithm, that detect all cycles
in a graph.

This code was created by ChatGPT, and must be checked in the
future.
"""
# pylint: disable=too-many-locals
# pylint: disable=invalid-name
# pylint: disable=stop-iteration-return
# pylint: disable=too-many-statements

from collections import defaultdict
from typing import (
    DefaultDict,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)


T = TypeVar("T", bound=Hashable)


def simple_cycles(
    edges: Iterable[Tuple[T, T]],
    nodes: Optional[Iterable[T]] = None,
) -> Iterator[List[T]]:
    """
    Generate all simple cycles in a directed graph using
        Johnson's algorithm.

    Args:
        edges: Iterable of (u, v) directed edges (u -> v).
        nodes: Optional iterable of all nodes to include. If
            omitted, nodes are inferred from edges (isolated
            nodes are ignored since they can't form cycles).

    Yields:
        Cycles as lists of nodes [v0, v1, ..., v_{k-1}]
            where (v_{i} -> v_{i+1}) and (v_{k-1} -> v0) are
            edges. Each simple cycle is yielded exactly
            once.

    Notes:
        - Nodes can be any hashable objects.
        - Self-loops (u -> u) are yielded as [u].
        - Time can grow with the number of cycles; this
            enumerates *all* of them.
    """
    # ---- Build directed graph as adjacency sets (dedupe
    #   parallel edges) ----
    adj: DefaultDict[T, Set[T]] = defaultdict(set)
    node_set: Set[T] = set()
    for u, v in edges:
        adj[u].add(v)
        node_set.add(u)
        node_set.add(v)

    if nodes is not None:
        node_set |= set(nodes)  # include provided nodes
                                #   (isolated allowed)

    # Keep only nodes that actually exist in the graph
    #   structure (Johnson’s algorithm works on subgraphs
    #   containing edges; isolated nodes won’t produce
    #   cycles anyway, but they can stay harmlessly.)
    for n in list(node_set):
        adj.setdefault(n, set())

    # A stable node ordering is required by Johnson's
    #   algorithm. We’ll preserve the insertion order from
    #   `node_set` as listed (Python 3.7+ dict/set are
    #   insertion-ordered), but to be explicit and stable
    #   across runs, convert to a list once.
    V: List[T] = list(adj.keys())
    index: Dict[T, int] = {v: i for i, v in enumerate(V)}

    # ---- Tarjan's SCC (returns list of SCCs as sets) ----
    def tarjan_scc(nodes_subset: List[T]) -> List[Set[T]]:
        idx = 0
        index_map: Dict[T, int] = {}
        lowlink: Dict[T, int] = {}
        on_stack: Set[T] = set()
        stack: List[T] = []
        sccs: List[Set[T]] = []

        def strongconnect(v: T) -> None:
            nonlocal idx
            index_map[v] = idx
            lowlink[v] = idx
            idx += 1
            stack.append(v)
            on_stack.add(v)

            for w in adj[v]:
                if w not in nodes_subgraph:  # respect
                                             # subgraph
                                             # restriction
                    continue
                if w not in index_map:
                    strongconnect(w)
                    lowlink[v] = min(lowlink[v], lowlink[w])
                elif w in on_stack:
                    lowlink[v] = (
                        min(lowlink[v], index_map[w])
                    )

            # Root of SCC
            if lowlink[v] == index_map[v]:
                comp: Set[T] = set()
                while True:
                    w = stack.pop()
                    on_stack.remove(w)
                    comp.add(w)
                    if w == v:
                        break
                sccs.append(comp)

        nodes_subgraph = set(nodes_subset)
        for v in nodes_subset:
            if v not in index_map:
                strongconnect(v)
        return sccs

    # ---- Johnson's algorithm core ----
    blocked: Set[T] = set()
    B: DefaultDict[T, Set[T]] = defaultdict(set)
    stack: List[T] = []

    def unblock(u: T) -> None:
        if u in blocked:
            blocked.remove(u)
            for w in list(B[u]):
                B[u].remove(w)
                if w in blocked:
                    unblock(w)

    def circuit(s: T, subgraph_nodes: Set[T]) -> Iterator[List[T]]:
        """ DFS-style search from s using Johnson's blocking
            technique.
        """
        found_cycle = False
        stack.append(s)
        blocked.add(s)

        for w in adj[s]:
            if w not in subgraph_nodes:
                continue
            if w == start_node:
                # Found a cycle
                yield_cycle = stack[:]  # copy
                yield from [yield_cycle]
                found_cycle = True
            elif w not in blocked:
                for cyc in circuit(w, subgraph_nodes):
                    yield cyc
                    found_cycle = True

        if found_cycle:
            unblock(s)
        else:
            for w in adj[s]:
                if w in subgraph_nodes:
                    B[w].add(s)

        stack.pop()
        return found_cycle  # type: ignore[return-value]

    # Iterate over subgraphs starting at the least-indexed
    #   remaining vertex and progressively remove start
    #   nodes (as in Johnson).
    start_idx = 0
    while start_idx < len(V):
        # Subgraph induced by nodes with index >= start_idx
        subV = [v for v in V if index[v] >= start_idx]
        if not subV:
            break

        # Find SCCs of this subgraph
        sccs = tarjan_scc(subV)

        # Choose an SCC that can contain a cycle: size > 1
        #   or a self-loop present. Among those, pick the
        #   one whose minimum-index node is minimal overall.
        candidate_scc: Optional[Set[T]] = None
        candidate_min_idx = None

        for comp in sccs:
            if len(comp) == 1:
                v = next(iter(comp))
                if v not in adj[v]:  # no self-loop -> no
                                     # cycle
                    continue
            min_idx = min(index[v] for v in comp)
            if (
                (candidate_min_idx is None) or
                (min_idx < candidate_min_idx)
            ):
                candidate_min_idx = min_idx
                candidate_scc = comp

        if candidate_scc is None:
            break  # no more cycles

        # Start node is the least-index node in that SCC
        start_node = min(
            candidate_scc, key=lambda x: index[x]
        )

        # Reset Johnson-state
        blocked.clear()
        B.clear()
        stack.clear()

        # Run circuit from start_node within the SCC’s
        #   induced subgraph
        subgraph_nodes = set(candidate_scc)
        # Yield cycles; ensure they are rotated to start at
        #   start_node and closed
        for cyc in circuit(start_node, subgraph_nodes) or []:
            # cyc is a path stack [s, ..., x] where an edge
            #   x->s exists.
            # Normalize rotation to start at start_node and
            #   output as list (no repeated end).
            #   (Already starts at s by construction.)
            yield cyc[:]  # [s, ..., x]

        # Remove start_node from consideration for the next
        #   iteration by deleting all its outgoing edges
        #   (Johnson’s step)
        adj[start_node].clear()
        start_idx = index[start_node] + 1
