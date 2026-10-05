"""
#1233: an UNDIRECTED fixed hop next to a variable-length path.

`(c)-[:R]-(a)-[:R*1..2]->(b)` failed Code 47 (the hop correlation reached the path CTE body) and, once
that is fixed, the legacy two-arm split rendered each arm as bare `FROM vlp AS t` with the hop join
missing — silently wrong counts. Oracle: Python trail enumeration with GLOBAL edge uniqueness.
"""

import itertools

import pytest
from test_zero_lower_bound_trails import _arrow, _count, _graph, _rows

SCHEMA, LABEL, REL = "social_integration", "User", "FOLLOWS"


def _query(segments):
    pattern = f"(n0:{LABEL})"
    for i, (d, lo, hi) in enumerate(segments):
        pattern += _arrow(d, REL, lo, hi) + f"(n{i + 1}:{LABEL})"
    return f"MATCH {pattern} RETURN count(*) AS n"


@pytest.mark.parametrize("path_dir, bounds", list(itertools.product(
    ["out", "in", "both"], [(1, 2), (0, 2), (2, 3), (1, 3)])))
@pytest.mark.parametrize("order", ["hop+path", "path+hop"])
def test_undirected_hop_next_to_a_path_matches_the_trail_enumeration(order, path_dir, bounds):
    graph = _graph(SCHEMA, LABEL, REL, "user_id")
    path = (path_dir, *bounds)
    segments = [("both", 1, 1), path] if order == "hop+path" else [path, ("both", 1, 1)]
    got = _rows(SCHEMA, _query(segments))
    assert got and "n" in got[0], (_query(segments), got)
    assert int(got[0]["n"]) == _count(graph, segments), _query(segments)


def _segments_variants(path):
    hop = ("both", 1, 1)
    return {
        "hop+hop+path": [hop, hop, path],
        "hop+path+hop": [hop, path, hop],
        "directed-hop+path+undirected-hop": [("out", 1, 1), path, hop],
    }


@pytest.mark.parametrize("variant", ["hop+hop+path", "hop+path+hop", "directed-hop+path+undirected-hop"])
@pytest.mark.parametrize("path_dir", ["out", "in", "both"])
@pytest.mark.parametrize("bounds", [(1, 2), (0, 2)])
def test_several_hops_around_a_path(variant, path_dir, bounds):
    graph = _graph(SCHEMA, LABEL, REL, "user_id")
    segments = _segments_variants((path_dir, *bounds))[variant]
    got = _rows(SCHEMA, _query(segments))
    assert int(got[0]["n"]) == _count(graph, segments), _query(segments)


@pytest.mark.parametrize("schema, label, rel, id_prop, path_dirs", [
    ("social_polymorphic", "User", "FOLLOWS", "user_id", ["out", "in", "both"]),
    ("denormalized_flights", "Airport", "FLIGHT", "code", ["out", "in"]),
])
def test_undirected_hop_next_to_a_path_on_other_layouts(schema, label, rel, id_prop, path_dirs):
    graph = _graph(schema, label, rel, id_prop)
    for path_dir, bounds in itertools.product(path_dirs, [(1, 2), (0, 2)]):
        path = (path_dir, *bounds)
        for segments in ([("both", 1, 1), path], [path, ("both", 1, 1)],
                         [("both", 1, 1), path, ("both", 1, 1)]):
            pattern = f"(n0:{label})"
            for i, (d, lo, hi) in enumerate(segments):
                pattern += _arrow(d, rel, lo, hi) + f"(n{i + 1}:{label})"
            got = _rows(schema, f"MATCH {pattern} RETURN count(*) AS n")
            assert int(got[0]["n"]) == _count(graph, segments), f"{schema} {pattern}"


# --- separate MATCH clauses: Cypher's edge uniqueness does not span clauses, so no guard is owed ----

def _trails_from(graph, a, lo, hi, d):
    _, out, inn = graph

    def moves(n):
        return out[n] if d == "out" else inn[n] if d == "in" else out[n] + inn[n]

    total = 0

    def walk(n, used, k):
        nonlocal total
        if k >= lo:
            total += 1
        if k == hi:
            return
        for i, nxt in moves(n):
            if i not in used:
                walk(nxt, used | {i}, k + 1)

    walk(a, frozenset(), 0)
    return total


@pytest.mark.parametrize("hop_dir", ["out", "in", "both"])
@pytest.mark.parametrize("path_dir", ["out", "in", "both"])
@pytest.mark.parametrize("keyword", ["MATCH", "OPTIONAL MATCH"])
def test_hop_and_path_in_separate_clauses(hop_dir, path_dir, keyword):
    graph = _graph(SCHEMA, LABEL, REL, "user_id")
    nodes, out, inn = graph
    hop = _arrow(hop_dir, REL, 1, 1)
    path = _arrow(path_dir, REL, 1, 2)
    # the hop `(c)<hop>(a)` seen from `a`: how many `c` reach it
    reach = {"out": inn, "in": out}
    degree = {a: len(inn[a]) if hop_dir == "out" else len(out[a]) if hop_dir == "in"
              else len(out[a]) + len(inn[a]) for a in nodes}
    paths = {a: _trails_from(graph, a, 1, 2, path_dir) for a in nodes}
    if keyword == "OPTIONAL MATCH":
        want = sum(degree[a] * max(1, paths[a]) for a in nodes)
    else:
        want = sum(degree[a] * paths[a] for a in nodes)
    got = _rows(SCHEMA, f"MATCH (c:{LABEL}){hop}(a:{LABEL}) {keyword} (a){path}(b:{LABEL}) "
                        "RETURN count(*) AS n")
    assert int(got[0]["n"]) == want


# --- shapes the hop-vs-path edge guard does not cover stay LOUD (never a silently high count) -----

@pytest.mark.parametrize("query", [
    # a WITH scope
    "MATCH (c:User) WITH c MATCH (c)-[:FOLLOWS]-(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN count(*) AS n",
    # shortestPath next to an undirected hop in one clause
    "MATCH p = shortestPath((c:User)-[:FOLLOWS*1..3]->(a:User)), (a)-[:FOLLOWS]-(z:User) "
    "RETURN count(*) AS n",
])
def test_unguarded_shapes_are_refused(query):
    from conftest import execute_cypher
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" not in result, (query, result)
