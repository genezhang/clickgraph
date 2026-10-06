"""
#1291: a variable-length path out of a node carried by a WITH from an EARLIER variable-length path.

`MATCH (c)-[*1..2]->(a) WITH a MATCH (a)-[*1..2]->(b)` exported `start_id` (c!) for the carried `a`:
PlanCtx keeps one VLP entry per alias for the whole query, and the later path (where `a` is the START)
overwrote the earlier one (where `a` is the END). Every `a` was replaced by the path's source: 485 rows
where 339 are right, silently, on every layout.

The oracle enumerates trails (relationship-unique within each MATCH, not across the WITH) over the
edge list the engine returns for a plain single hop.
"""

import pytest
from conftest import execute_cypher

LAYOUTS = {
    "standard": ("social_integration", "User", "FOLLOWS", "user_id"),
    "denormalized": ("denormalized_flights", "Airport", "FLIGHT", "code"),
}


def _rows(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _edges(schema, label, rel, key):
    rows = _rows(schema, f"MATCH (a:{label})-[:{rel}]->(b:{label}) RETURN a.{key} AS a, b.{key} AS b")
    return [(r["a"], r["b"]) for r in rows]


def _trails(edges, src, lo, hi):
    out = []

    def walk(node, used):
        if lo <= len(used) <= hi:
            out.append(node)
        if len(used) == hi:
            return
        for i, (s, d) in enumerate(edges):
            if s == node and i not in used:
                walk(d, used | {i})

    walk(src, frozenset())
    return out


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("lo1, hi1, lo2, hi2", [(1, 2, 1, 2), (2, 2, 1, 2), (1, 2, 2, 3)])
def test_path_out_of_the_end_of_an_earlier_path(layout, lo1, hi1, lo2, hi2):
    if layout == "denormalized" and (lo1, hi1) == (2, 2):
        # An exact-length first path is expanded as hops; on this layout the carried endpoint is
        # refused loudly (`a.Dest` unknown, same on main). Not wrong rows, not this bug.
        pytest.skip("exact-length path before WITH on the denormalized layout fails loudly")
    schema, label, rel, key = LAYOUTS[layout]
    edges = _edges(schema, label, rel, key)
    sources = {s for s, _ in edges}
    expected = sum(
        len(_trails(edges, a, lo2, hi2))
        for c in sources
        for a in _trails(edges, c, lo1, hi1)
    )
    q = (
        f"MATCH (c:{label})-[:{rel}*{lo1}..{hi1}]->(a:{label}) WITH a "
        f"MATCH (a)-[:{rel}*{lo2}..{hi2}]->(b:{label}) RETURN count(*) AS k"
    )
    assert _rows(schema, q)[0]["k"] == expected


@pytest.mark.parametrize("layout", LAYOUTS)
def test_carried_start_of_an_earlier_path_is_still_the_start(layout):
    # `c` is the START of the first path: the endpoint column must stay `start_id`.
    schema, label, rel, key = LAYOUTS[layout]
    edges = _edges(schema, label, rel, key)
    sources = {s for s, _ in edges}
    expected = sum(
        len(_trails(edges, c, 1, 2)) * len(_trails(edges, c, 1, 2)) for c in sources
    )
    q = (
        f"MATCH (c:{label})-[:{rel}*1..2]->(a:{label}) WITH c "
        f"MATCH (c)-[:{rel}*1..2]->(b:{label}) RETURN count(*) AS k"
    )
    assert _rows(schema, q)[0]["k"] == expected
