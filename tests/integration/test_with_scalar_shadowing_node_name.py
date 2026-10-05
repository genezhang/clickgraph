"""
#1263: a WITH scalar that reuses the NAME of a pattern node (`WITH a.user_id AS a`).

Over an undirected hop the WITH body is a UNION of the direction arms. The CTE's column metadata was
read from the first ARM's own whole-node items (`a.age`, `a.city`, ...) although the emitter projects
the plan's select onto every arm — so the CTE exposed only the scalar `a`, while `RETURN a` expanded the
node's columns (`a_b.a_age`): Code 47. Renaming the scalars (`AS x`) always worked.
"""

import collections

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _bag(rows, keys):
    return collections.Counter(tuple(r[k] for k in keys) for r in rows)


@pytest.mark.parametrize("hop", ["-[:FOLLOWS]->", "<-[:FOLLOWS]-", "-[:FOLLOWS]-"])
def test_scalar_named_like_the_node_matches_the_plain_rows(hop):
    plain = _bag(
        _rows(f"MATCH (a:User){hop}(b:User) RETURN a.user_id AS a, b.user_id AS b"), ("a", "b")
    )
    shadow = _bag(
        _rows(f"MATCH (a:User){hop}(b:User) WITH a.user_id AS a, b.user_id AS b RETURN a, b"),
        ("a", "b"),
    )
    assert shadow == plain


@pytest.mark.parametrize("hop", ["-[:FOLLOWS]->", "-[:FOLLOWS]-"])
def test_shadow_scalar_survives_a_second_with_and_a_filter(hop):
    plain = _bag(
        _rows(f"MATCH (a:User){hop}(b:User) RETURN a.user_id AS a, b.user_id AS b"), ("a", "b")
    )
    chained = _bag(
        _rows(
            f"MATCH (a:User){hop}(b:User) WITH a.user_id AS a, b.user_id AS b "
            "WITH a, b WHERE 1 = 1 RETURN a, b"
        ),
        ("a", "b"),
    )
    assert chained == plain


def test_one_side_shadowed_one_renamed():
    plain = _bag(
        _rows("MATCH (a:User)-[:FOLLOWS]-(b:User) RETURN a.user_id AS x, b.user_id AS b"),
        ("x", "b"),
    )
    mixed = _bag(
        _rows("MATCH (a:User)-[:FOLLOWS]-(b:User) WITH a.user_id AS x, b.user_id AS b RETURN x, b"),
        ("x", "b"),
    )
    assert mixed == plain
