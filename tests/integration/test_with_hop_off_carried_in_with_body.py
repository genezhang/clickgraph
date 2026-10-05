"""
#1283: a hop off a node carried through WITH, inside the body of a SECOND WITH (denormalized layout).

The second WITH's body was rendered with the carried node's CTE reference filtered out (a node
scanning a WITH CTE was mistaken for a fresh table scan), so the hop's tie to the CTE was never made
and the orphan-alias pass CROSS JOINed the CTE: 36 rows where 8 are right.

The oracle enumerates the same patterns over the edge list the engine returns for a plain single hop.
"""

import collections

import pytest
from conftest import execute_cypher

SCHEMA = "denormalized_flights"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _edges():
    rows = _rows("MATCH (z:Airport)-[:FLIGHT]->(c:Airport) RETURN z.code AS z, c.code AS c")
    return [(r["z"], r["c"]) for r in rows]


def _bag(query):
    return collections.Counter((r["x"], r["y"]) for r in _rows(query))


CARRY = "MATCH (z:Airport)-[:FLIGHT]->(c:Airport) WITH c "


def test_hop_off_carried_node_closed_by_a_second_with():
    edges = _edges()
    expected = collections.Counter(
        (c, a) for _, c in edges for (s, a) in edges if s == c
    )
    q = CARRY + "MATCH (c)-[:FLIGHT]->(a:Airport) WITH c, a RETURN c.code AS x, a.code AS y"
    assert _bag(q) == expected


@pytest.mark.parametrize("carried", ["a", "c, a"])
def test_hop_off_carried_node_then_a_hop_after_the_second_with(carried):
    edges = _edges()
    expected = collections.Counter()
    for _, c in edges:
        for s, a in edges:
            if s != c:
                continue
            for s2, b in edges:
                if s2 == a:
                    expected[(a, b)] += 1
    q = (
        CARRY
        + f"MATCH (c)-[:FLIGHT]->(a:Airport) WITH {carried} "
        + "MATCH (a)-[:FLIGHT]->(b:Airport) RETURN a.code AS x, b.code AS y"
    )
    assert _bag(q) == expected


def test_path_off_carried_node_in_a_with_body_is_refused_not_cross_joined():
    q = CARRY + "MATCH (c)-[:FLIGHT*1..2]->(a:Airport) WITH c, a RETURN count(*) AS k"
    result = execute_cypher(q, schema_name=SCHEMA, raise_on_error=False)
    assert "results" not in result or result.get("error"), result


# --- a path between two carried nodes inside a WITH body (standard layout) -------------------------
# The body was `FROM <path CTE> CROSS JOIN <WITH CTE>`: the path was never tied to the carried nodes.
# LDBC IC1's shape gave every friend the distance to the NEAREST one (1, 1, 1 instead of 1, 2, 3).


def _std(query):
    result = execute_cypher(query, schema_name="social_integration", raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def test_shortest_path_between_carried_nodes_in_a_with_body():
    rows = _std(
        "MATCH (p:User), (f:User) WHERE p.user_id = 1 AND f.user_id IN [2, 7, 15] WITH p, f "
        "MATCH path = shortestPath((p)-[:FOLLOWS*1..3]-(f)) "
        "WITH min(length(path)) AS d, f RETURN f.user_id AS fid, d"
    )
    assert {(r["fid"], r["d"]) for r in rows} == {(2, 1), (7, 2), (15, 3)}


def test_undirected_path_between_carried_nodes_in_a_with_body():
    # Trails 15-7 and 15-7-3 (7 and 3 are linked by two edges): 3, the same as with no second WITH.
    q = (
        "MATCH (p:User), (f:User) WHERE p.user_id = 15 AND f.user_id IN [7, 3] WITH p, f "
        "MATCH (p)-[:FOLLOWS*1..2]-(f) {w}RETURN count(*) AS k"
    )
    assert _std(q.format(w="WITH p, f "))[0]["k"] == _std(q.format(w=""))[0]["k"] == 3
