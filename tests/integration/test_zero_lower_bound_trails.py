"""
#1230: a variable-length path with lower bound 0 (`*0..N`) is a TRAIL like any other: an edge may not
repeat but a NODE may, so a walk can come back to its start through a cycle. Until #1230 the
open `*0..N` CTE kept NODE-uniqueness (`NOT has(path_nodes, next)`), which silently dropped every
trail that revisits a node (standard: `*0..3` returned 100 rows where 149 is correct on the cyclic
`social_integration` graph).

Independent oracle: enumerate trails over the edge list in Python (zero-hop rows = every node).
Also covers the hop+path uniqueness guard (#1175) for `*0..N`.
"""

import collections

import pytest
from conftest import execute_cypher


def _rows(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, f"{query}: {result}"
    return result["results"]


def _graph(schema, label, rel, id_prop):
    edges = [(r["a"], r["b"]) for r in _rows(
        schema, f"MATCH (a:{label})-[:{rel}]->(b:{label}) RETURN a.{id_prop} AS a, b.{id_prop} AS b")]
    nodes = {r["a"] for r in _rows(schema, f"MATCH (a:{label}) RETURN a.{id_prop} AS a")}
    nodes |= {x for e in edges for x in e}
    out, inn = collections.defaultdict(list), collections.defaultdict(list)
    for i, (a, b) in enumerate(edges):
        out[a].append((i, b))
        inn[b].append((i, a))
    return nodes, out, inn


def _count(graph, segments):
    """Trails matching consecutive segments `(direction, lo, hi)` with GLOBAL edge uniqueness."""
    nodes, out, inn = graph

    def moves(node, d):
        return out[node] if d == "out" else inn[node] if d == "in" else out[node] + inn[node]

    total = 0

    def rec(si, node, used):
        nonlocal total
        if si == len(segments):
            total += 1
            return
        d, lo, hi = segments[si]

        def walk(n, used, k):
            if k >= lo:
                rec(si + 1, n, used)
            if k == hi:
                return
            for i, nxt in moves(n, d):
                if i not in used:
                    walk(nxt, used | {i}, k + 1)

        walk(node, used, 0)

    for s in nodes:
        rec(0, s, frozenset())
    return total


def _arrow(d, rel, lo, hi):
    spec = f"[:{rel}*{lo}..{hi}]" if (lo, hi) != (1, 1) else f"[:{rel}]"
    return {"out": f"-{spec}->", "in": f"<-{spec}-", "both": f"-{spec}-"}[d]


LAYOUTS = [
    ("social_integration", "User", "FOLLOWS", "user_id", ("out", "in", "both")),
    ("standard", "User", "FOLLOWS", "user_id", ("out", "in", "both")),
    ("social_polymorphic", "User", "FOLLOWS", "user_id", ("out", "in", "both")),
    # the denormalized undirected path is a separate (two-arm split) strategy with its own
    # under-count; directed forms are covered here
    ("denormalized_flights", "Airport", "FLIGHT", "code", ("out", "in")),
]


@pytest.mark.parametrize("schema, label, rel, id_prop, directions", LAYOUTS)
def test_pure_zero_lower_bound_paths_match_the_trail_enumeration(
        schema, label, rel, id_prop, directions):
    graph = _graph(schema, label, rel, id_prop)
    for d in directions:
        for lo, hi in ((0, 1), (0, 2), (0, 3), (1, 3)):
            got = int(_rows(
                schema, f"MATCH (a:{label}){_arrow(d, rel, lo, hi)}(b:{label}) "
                        "RETURN count(*) AS n")[0]["n"])
            assert got == _count(graph, [(d, lo, hi)]), f"{schema} {d} *{lo}..{hi}"


def test_zero_lower_bound_returns_cycles_back_to_the_start():
    """Closed form: every 2-cycle a->b->a must be counted for *0..2."""
    schema, label, rel = "social_integration", "User", "FOLLOWS"
    graph = _graph(schema, label, rel, "user_id")
    closed = int(_rows(schema, f"MATCH (a:{label})-[:{rel}*0..3]->(a) RETURN count(*) AS n")[0]["n"])
    nodes, out, inn = graph
    expected = 0
    for s in nodes:
        def walk(n, used, k):
            nonlocal expected
            if n == s:
                expected += 1          # zero-hop row (k=0) and every closed trail
            if k == 3:
                return
            for i, nxt in out[n]:
                if i not in used:
                    walk(nxt, used | {i}, k + 1)
        walk(s, frozenset(), 0)
    assert closed == expected


@pytest.mark.parametrize("hop_dir", ["out", "in"])
@pytest.mark.parametrize("order", ["hop+path", "path+hop"])
def test_hop_next_to_a_zero_lower_bound_path_is_edge_unique(hop_dir, order):
    schema, label, rel = "social_integration", "User", "FOLLOWS"
    graph = _graph(schema, label, rel, "user_id")
    segments = [(hop_dir, 1, 1), ("out", 0, 2)] if order == "hop+path" \
        else [("out", 0, 2), (hop_dir, 1, 1)]
    pattern = "(n0:User)"
    for i, (d, lo, hi) in enumerate(segments):
        pattern += _arrow(d, rel, lo, hi) + f"(n{i + 1}:User)"
    got = int(_rows(schema, f"MATCH {pattern} RETURN count(*) AS n")[0]["n"])
    assert got == _count(graph, segments), pattern
