"""
#1308: a WHERE comparing an endpoint of a variable-length path with a node OUTSIDE the path
(`MATCH (z)-[:R]->(c)-[:R*1..2]->(b) WHERE b = z`).

The conjunct was categorized as a filter of the path's recursive CTE, which cannot see `z`: an
unresolvable alias (Code 47) on most layouts, and on the denormalized `*0..N` arms it was dropped
(24 rows where 6 are right). It is now kept out of the CTE and applied in the outer query.

The oracle enumerates trails (relationship-unique within each MATCH) over the edge list the engine
returns for a plain single hop.
"""

import re

import pytest
from conftest import execute_cypher

LAYOUTS = {
    "standard": ("social_integration", "User", "FOLLOWS", "user_id"),
    "denormalized": ("denormalized_flights", "Airport", "FLIGHT", "code"),
    "polymorphic": ("social_polymorphic", "User", "FOLLOWS", "user_id"),
}


def _result(schema, query):
    return execute_cypher(query, schema_name=schema, raise_on_error=False)


def _count(schema, query):
    result = _result(schema, query)
    assert "results" in result, (query, result)
    return result["results"][0]["k"]


def _edges(schema, label, rel, key):
    rows = _result(schema, f"MATCH (a:{label})-[:{rel}]->(b:{label}) RETURN a.{key} AS a, b.{key} AS b")
    return [(r["a"], r["b"]) for r in rows["results"]]


def _trails(edges, src, lo, hi, used=frozenset()):
    """(end node, edge ids) of every trail of length lo..hi from src avoiding `used`."""
    out = []

    def walk(node, path):
        if lo <= len(path) <= hi:
            out.append((node, path))
        if len(path) == hi:
            return
        for i, (s, d) in enumerate(edges):
            if s == node and i not in path and i not in used:
                walk(d, path | {i})

    walk(src, frozenset())
    return out


def _hop_then_path(edges, lo, hi):
    """(z, c, b) for `(z)-[:R]->(c)-[:R*lo..hi]->(b)`, the path not reusing the hop's edge."""
    return [
        (z, c, b)
        for i, (z, c) in enumerate(edges)
        for b, _ in _trails(edges, c, lo, hi, frozenset({i}))
    ]


CHECKS = {
    "b = z": lambda z, c, b: b == z,
    "b <> z": lambda z, c, b: b != z,
    "b.K = z.K": lambda z, c, b: b == z,
    "c <> z": lambda z, c, b: c != z,
}


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("lo, hi", [(1, 2), (0, 2), (2, 3)])
@pytest.mark.parametrize("where", CHECKS)
def test_path_endpoint_compared_with_the_hop_before_it(layout, lo, hi, where):
    schema, label, rel, key = LAYOUTS[layout]
    edges = _edges(schema, label, rel, key)
    expected = sum(1 for z, c, b in _hop_then_path(edges, lo, hi) if CHECKS[where](z, c, b))
    q = (
        f"MATCH (z:{label})-[:{rel}]->(c:{label})-[:{rel}*{lo}..{hi}]->(b:{label}) "
        f"WHERE {where.replace('K', key)} RETURN count(*) AS k"
    )
    assert _count(schema, q) == expected, q


@pytest.mark.parametrize("layout", LAYOUTS)
def test_path_start_compared_with_the_hop_after_it(layout):
    # The conjunct is the trailing hop's, not the path's: unchanged by #1308, kept as a guard.
    # (Its bare form `d <> c` still fails loudly on the trailing hop, Code 47.)
    schema, label, rel, key = LAYOUTS[layout]
    edges = _edges(schema, label, rel, key)
    expected = 0
    for c in {s for s, _ in edges}:
        for b, used in _trails(edges, c, 1, 2):
            expected += sum(1 for i, (s, d) in enumerate(edges) if s == b and i not in used and d != c)
    q = (
        f"MATCH (c:{label})-[:{rel}*1..2]->(b:{label})-[:{rel}]->(d:{label}) "
        f"WHERE d.{key} <> c.{key} RETURN count(*) AS k"
    )
    assert _count(schema, q) == expected, q


@pytest.mark.parametrize("layout", ["standard", "polymorphic", "denormalized"])
@pytest.mark.parametrize("where", ["z.K = b.K", "z.K <> b.K"])
def test_path_endpoint_compared_with_a_carried_node_outside_the_path(layout, where):
    # `z` is carried but not in the pattern. The predicate is a column equality with the WITH CTE,
    # which used to be taken as the CTE's join key in place of the path's tie to `c` (111 vs 12).
    schema, label, rel, key = LAYOUTS[layout]
    edges = _edges(schema, label, rel, key)
    keep = (lambda z, b: z == b) if "<>" not in where else (lambda z, b: z != b)
    expected = sum(1 for z, c in edges for b, _ in _trails(edges, c, 1, 2) if keep(z, b))
    q = (
        f"MATCH (z:{label})-[:{rel}]->(c:{label}) WITH c, z "
        f"MATCH (c)-[:{rel}*1..2]->(b:{label}) WHERE {where.replace('K', key)} RETURN count(*) AS k"
    )
    assert _count(schema, q) == expected, q


def test_optional_path_compared_with_an_outside_node_is_refused():
    result = _result(
        "social_integration",
        "MATCH (z:User)-[:FOLLOWS]->(c:User) OPTIONAL MATCH (c)-[:FOLLOWS*1..2]->(b:User) "
        "WHERE b <> z RETURN count(*) AS k",
    )
    assert "results" not in result or result.get("error"), result


# --- review of #1309 -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "query, expected",
    [
        ("MATCH (z:User)-[:FOLLOWS]->(a:User) WITH z, a "
         "MATCH (a)-[:FOLLOWS*1..3]->(a) WHERE z.age > a.age RETURN count(*) AS k", 14),
        # next to a hop the closed path already loses its closure (#1310: 77 unfiltered vs 14)
        ("MATCH (z:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..3]->(a) "
         "WHERE z.user_id <> a.user_id RETURN count(*) AS k", 14),
    ],
)
def test_closed_path_compared_with_an_outside_node_is_never_wrong(query, expected):
    # A closed path keeps its old placement for an outside conjunct (inside the CTE: refused
    # loudly), until #1310 restores the closure next to a hop. It must never be answered wrong.
    result = _result("social_integration", query)
    if "results" in result:
        assert result["results"][0]["k"] == expected, query


@pytest.mark.parametrize(
    "query",
    [
        # the other path's endpoint was resolved against this path's CTE (73 vs 159)
        "MATCH (a:User)-[:FOLLOWS*1..2]->(c:User)-[:FOLLOWS*1..2]->(b:User) "
        "WHERE b.user_id > a.user_id RETURN count(*) AS k",
        # a WHERE equality became the WITH CTE's join key; `c` was left untied (160 vs 20)
        "MATCH (z:User)-[:FOLLOWS]->(c:User) WITH c, z "
        "MATCH (c)<-[:FOLLOWS*1..2]-(b:User) WHERE b.user_id = z.user_id RETURN count(*) AS k",
    ],
)
def test_unsupported_outside_comparisons_are_refused(query):
    result = _result("social_integration", query)
    assert "results" not in result or result.get("error"), result
