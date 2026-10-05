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


# ---------------------------------------------------------------------------
# #1225: the OUTPUT NAME of a WITH-carried scalar. `WITH a.age AS ag` used to be registered as a
# rename of the node `a` (its labels copied onto `ag`), so `ag` was typed as a Node:
# `RETURN ag AS x` dropped the alias (rows keyed `ag`), a chained `WITH ag, count(*)` keyed the
# column `ag.id`, and `WITH u, u.name AS n RETURN n` produced three `n.*` columns.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("schema, node, expr", [
    ("social_integration", "(a:User)", "a.age"),
    ("social_polymorphic", "(a:User)", "a.name"),
    ("standard", "(a:User)", "a.name"),
])
def test_return_alias_of_a_with_scalar_is_honored(schema, node, expr):
    direct = _rows(schema, f"MATCH {node} RETURN {expr} AS x ORDER BY x")
    via_with = _rows(schema, f"MATCH {node} WITH {expr} AS g RETURN g AS x ORDER BY x")
    assert via_with == direct and direct and set(direct[0]) == {"x"}


def test_bare_return_keeps_the_with_name():
    rows = _rows("social_integration", "MATCH (a:User) WITH a.age AS ag RETURN ag ORDER BY ag")
    assert rows and set(rows[0]) == {"ag"}


def test_scalar_through_two_withs_keeps_its_name():
    direct = _rows("social_integration",
                   "MATCH (a:User) RETURN a.age AS ag, count(*) AS c ORDER BY ag")
    via = _rows("social_integration",
                "MATCH (a:User) WITH a.age AS ag WITH ag, count(*) AS c "
                "RETURN ag, c ORDER BY ag")
    assert via == direct
    renamed = _rows("social_integration",
                    "MATCH (a:User) WITH a.age AS ag WITH ag, count(*) AS c "
                    "RETURN ag AS y, c ORDER BY y")
    assert [(r["y"], r["c"]) for r in renamed] == [(r["ag"], r["c"]) for r in direct]


def test_node_and_a_property_of_it_carried_together():
    rows = _rows("social_integration",
                 "MATCH (u:User) WITH u, u.name AS n RETURN n ORDER BY n LIMIT 3")
    direct = _rows("social_integration", "MATCH (u:User) RETURN u.name AS n ORDER BY n LIMIT 3")
    assert rows == direct and set(rows[0]) == {"n"}


def test_property_output_named_like_its_source_node():
    rows = _rows("social_integration", "MATCH (a:User) WITH a.age AS a RETURN a ORDER BY a LIMIT 3")
    direct = _rows("social_integration", "MATCH (a:User) RETURN a.age AS a ORDER BY a LIMIT 3")
    assert rows == direct


# ---------------------------------------------------------------------------
# #1227: a scalar exported by the SAME WITH as the node it was read from
# (`WITH a, a.age AS ag`): the render layer also treated it as a rename of `a` and published `ag`
# with a's property mapping/label, so `GROUP BY ag` stayed a bare alias (Code 184).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("schema, expr", [
    ("social_integration", "a.age"),
    ("social_polymorphic", "a.name"),
    ("standard", "a.name"),
])
def test_scalar_exported_next_to_its_node_groups_correctly(schema, expr):
    direct = _as_dict(
        _rows(schema, f"MATCH (a:User) RETURN {expr} AS g, count(*) AS n"), "g")
    via_with = _as_dict(
        _rows(schema, f"MATCH (a:User) WITH a, {expr} AS g RETURN g, count(*) AS n"), "g")
    assert via_with == direct and len(direct) > 1


def test_scalar_next_to_its_node_keeps_order_and_name():
    schema = "social_integration"
    direct = _rows(schema, "MATCH (a:User) RETURN a.age AS x, count(*) AS n ORDER BY x")
    via = _rows(schema, "MATCH (a:User) WITH a, a.age AS ag RETURN ag AS x, count(*) AS n "
                        "ORDER BY x")
    assert via == direct
