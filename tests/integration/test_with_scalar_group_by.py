"""
#1222: a scalar carried through a WITH and then grouped (`WITH a.age AS ag RETURN ag, count(*)`)
rendered `GROUP BY ag.user_id` (Code 47): the CTE-backed alias of a scalar was treated like a
node and given a placeholder id. The scalar's single column IS its value.

Differential oracle: the same grouping WITHOUT the WITH (`RETURN a.age AS ag, count(*)`).
"""

import pytest
from conftest import execute_cypher


def _rows(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, f"{query}: {result}"
    return result["results"]


def _as_dict(rows, key):
    return {r[key]: int(r["n"]) for r in rows}


CASES = [
    ("social_integration", "(a:User)", "a.age"),
    ("social_integration", "(a:User)", "a.city"),
    ("social_polymorphic", "(a:User)", "a.name"),
    ("standard", "(a:User)", "a.name"),
]


@pytest.mark.parametrize("schema, node, expr", CASES)
def test_grouping_a_with_scalar_matches_grouping_the_expression(schema, node, expr):
    direct = _as_dict(
        _rows(schema, f"MATCH {node} RETURN {expr} AS g, count(*) AS n"), "g")
    assert len(direct) > 1, "need several groups for the oracle to mean something"
    via_with = _as_dict(
        _rows(schema, f"MATCH {node} WITH {expr} AS g RETURN g, count(*) AS n"), "g")
    assert via_with == direct


def test_scalar_group_with_two_keys_and_order():
    schema = "social_integration"
    direct = _rows(schema, "MATCH (a:User) RETURN a.city AS c, a.age AS g, count(*) AS n "
                           "ORDER BY c, g")
    via_with = _rows(schema, "MATCH (a:User) WITH a.city AS c, a.age AS g "
                             "RETURN c, g, count(*) AS n ORDER BY c, g")
    assert via_with == direct


def test_length_of_a_path_grouped_after_with():
    schema = "social_integration"
    direct = _as_dict(_rows(
        schema, "MATCH p=(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN length(p) AS l, count(*) AS n"),
        "l")
    via_with = _as_dict(_rows(
        schema, "MATCH p=(a:User)-[:FOLLOWS*1..2]->(b:User) WITH length(p) AS len "
                "RETURN len, count(*) AS n"), "len")
    assert via_with == direct and len(direct) == 2
