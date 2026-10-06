"""
#1287: a fixed hop in front of a variable-length path that ENDS at a node bound earlier (a comma-joined
`MATCH (c)` or a node carried through WITH).

The path's endpoint side was a CartesianProduct, so its VLP context could not be built and the path
was expanded as an ordinary hop: its own relationship joined as a second, untied edge table
(427 rows vs 167). The hop-vs-path relationship-uniqueness guard (#1175) was also skipped for a
comma/cartesian scope and for every post-WITH scope.

The oracle enumerates trails (relationship-unique within each MATCH) over the edge list the engine
returns for a plain single hop.
"""

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _edges():
    rows = _rows("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b")
    return [(r["a"], r["b"]) for r in rows]


def _trails(edges, src, lo, hi, used):
    out = []

    def walk(node, path):
        if lo <= len(path) <= hi:
            out.append(node)
        if len(path) == hi:
            return
        for i, (s, d) in enumerate(edges):
            if s == node and i not in path and i not in used:
                walk(d, path | {i})

    walk(src, frozenset())
    return out


def _hop_then_path_into(edges, c, lo, hi):
    """Count (a)-[e]->(b)-[*lo..hi]->(c), the path not reusing e."""
    return sum(
        1
        for i, (a, b) in enumerate(edges)
        for end in _trails(edges, b, lo, hi, {i})
        if end == c
    )


def test_hop_before_path_into_a_comma_bound_node():
    edges = _edges()
    users = {r["c"] for r in _rows("MATCH (c:User) RETURN c.user_id AS c")}
    expected = sum(_hop_then_path_into(edges, c, 1, 2) for c in users)
    q = (
        "MATCH (c:User) MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS*1..2]->(c) "
        "RETURN count(*) AS k"
    )
    assert _rows(q)[0]["k"] == expected


@pytest.mark.parametrize("lo, hi", [(1, 2), (1, 3)])
def test_hop_before_path_into_a_with_carried_node(lo, hi):
    edges = _edges()
    expected = sum(_hop_then_path_into(edges, c, lo, hi) for _, c in edges)
    q = (
        "MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
        f"MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS*{lo}..{hi}]->(c) RETURN count(*) AS k"
    )
    assert _rows(q)[0]["k"] == expected


def test_hop_into_carried_node_then_path():
    edges = _edges()
    expected = sum(
        len(_trails(edges, c, 1, 2, {i}))
        for _, c in edges
        for i, (n0, c2) in enumerate(edges)
        if c2 == c
    )
    q = (
        "MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c "
        "MATCH (n0:User)-[:FOLLOWS]->(c)-[:FOLLOWS*1..2]->(n2:User) RETURN count(*) AS k"
    )
    assert _rows(q)[0]["k"] == expected


# --- #1294: a hop BETWEEN two nodes carried by the same WITH -----------------------------------------
# Both endpoints are CTE-backed, so the hop's join was cleared as stale: the MATCH no longer required
# the edge (111 rows vs 99). It is now kept, tied to the carried `z` and to the path's start `c`.


@pytest.mark.parametrize("carried", ["c, z", "z, c"])
@pytest.mark.parametrize("lo, hi", [(1, 2), (2, 3)])
def test_hop_between_two_carried_nodes_then_path(carried, lo, hi):
    edges = _edges()
    expected = 0
    for z, c in edges:
        for i, (s, d) in enumerate(edges):
            if (s, d) == (z, c):
                expected += len(_trails(edges, c, lo, hi, {i}))
    q = (
        f"MATCH (z:User)-[:FOLLOWS]->(c:User) WITH {carried} "
        f"MATCH (z)-[:FOLLOWS]->(c)-[:FOLLOWS*{lo}..{hi}]->(b:User) RETURN count(*) AS k"
    )
    assert _rows(q)[0]["k"] == expected
