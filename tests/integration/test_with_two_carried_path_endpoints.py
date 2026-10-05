"""
`WITH c, z MATCH (z)-[:FOLLOWS*1..2]->(c)`: a variable-length path between two nodes carried by the
same WITH CTE (P-4b, docs/design/WITH_EXPORT_CONTRACT.md).

The CTE was tied to the path by ONE endpoint only (the one its alias `c_z` happens to start with), so
the path ran from every node to `c`: 95 rows where 20 are right (standard), 10 where 5 (polymorphic).

The oracle enumerates trails (relationship-unique within the second MATCH) over the edge list the
engine returns for a plain single hop.
"""

import collections

import pytest
from conftest import execute_cypher

CASES = {
    "social_integration": ("User", "FOLLOWS", "user_id"),
    "social_polymorphic": ("User", "FOLLOWS", "user_id"),
}


def _rows(query, schema):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _edges(schema):
    label, rel, key = CASES[schema]
    rows = _rows(
        f"MATCH (z:{label})-[:{rel}]->(c:{label}) RETURN z.{key} AS z, c.{key} AS c", schema
    )
    return [(str(r["z"]), str(r["c"])) for r in rows]


def _trail_count(edges, src, dst, lo, hi):
    count = 0

    def walk(node, used):
        nonlocal count
        if lo <= len(used) <= hi and node == dst:
            count += 1
        if len(used) == hi:
            return
        for i, (s, d) in enumerate(edges):
            if s == node and i not in used:
                walk(d, used | {i})

    walk(src, frozenset())
    return count


@pytest.mark.parametrize("schema", list(CASES))
@pytest.mark.parametrize("carried", ["c, z", "z, c"])
@pytest.mark.parametrize(
    "path, lo, hi, forward",
    [
        ("(z)-[:{rel}*1..2]->(c)", 1, 2, True),
        ("(c)-[:{rel}*1..2]->(z)", 1, 2, False),
        ("(z)-[:{rel}*2..3]->(c)", 2, 3, True),
    ],
)
def test_path_between_two_carried_nodes_is_tied_at_both_ends(schema, carried, path, lo, hi, forward):
    label, rel, key = CASES[schema]
    edges = _edges(schema)
    expected = collections.Counter()
    for z, c in edges:
        src, dst = (z, c) if forward else (c, z)
        n = _trail_count(edges, src, dst, lo, hi)
        if n:
            expected[(z, c)] += n
    q = (
        f"MATCH (z:{label})-[:{rel}]->(c:{label}) WITH {carried} "
        f"MATCH {path.format(rel=rel)} RETURN z.{key} AS z, c.{key} AS c"
    )
    got = collections.Counter((str(r["z"]), str(r["c"])) for r in _rows(q, schema))
    assert got == expected, q
