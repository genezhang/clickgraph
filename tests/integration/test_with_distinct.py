"""
`WITH DISTINCT` de-duplicates over ALL the WITH's columns, whether or not later clauses read them.

- the CTE column pruner removed every column nothing downstream read — all of them for `count(*)` — which
  changed which rows survive (`WITH DISTINCT a.p AS x, b.q AS y RETURN count(*)` counted distinct x) and,
  with the select emptied, lost the DISTINCT altogether (`SELECT *`: 20 rows where 7 distinct values exist);
- over an undirected hop the body is a UNION of direction arms; `SELECT DISTINCT ... UNION ALL SELECT
  DISTINCT ...` only de-duplicates within each arm.
"""

import pytest
from conftest import execute_cypher


def _scalar(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, (query, result)
    return int(result["results"][0]["n"])


@pytest.mark.parametrize("schema", ["social_integration", "social_polymorphic"])
@pytest.mark.parametrize("hop", ["-[:FOLLOWS]->", "<-[:FOLLOWS]-", "-[:FOLLOWS]-"])
def test_with_distinct_one_column_counts_distinct_values(schema, hop):
    pat = f"MATCH (a:User){hop}(b:User)"
    want = _scalar(schema, f"{pat} RETURN count(DISTINCT a.user_id) AS n")
    assert _scalar(schema, f"{pat} WITH DISTINCT a.user_id AS x RETURN count(*) AS n") == want


@pytest.mark.parametrize("schema", ["social_integration", "social_polymorphic"])
@pytest.mark.parametrize("hop", ["-[:FOLLOWS]->", "-[:FOLLOWS]-"])
def test_with_distinct_pair_counts_distinct_pairs_even_when_one_column_is_unread(schema, hop):
    pat = f"MATCH (a:User){hop}(b:User)"
    pairs = execute_cypher(
        f"{pat} RETURN DISTINCT a.user_id AS x, b.user_id AS y", schema_name=schema,
        raise_on_error=False)["results"]
    assert _scalar(
        schema, f"{pat} WITH DISTINCT a.user_id AS x, b.user_id AS y RETURN count(*) AS n"
    ) == len(pairs)


def test_with_distinct_over_an_undirected_hop_is_distinct_across_both_directions():
    pat = "MATCH (a:User)-[:FOLLOWS]-(b:User)"
    result = execute_cypher(
        f"{pat} WITH DISTINCT a.user_id AS a RETURN a", schema_name="social_polymorphic",
        raise_on_error=False)["results"]
    values = [r["a"] for r in result]
    assert len(values) == len(set(values)), values


# --- `WITH <key>, count(*) AS n` followed by a clause that reads neither column ----------------------

@pytest.mark.parametrize("schema, pattern, key", [
    ("social_integration", "(a:User)-[:FOLLOWS]->(b:User)", "a.user_id"),
    ("social_integration", "(a:User)-[:FOLLOWS]-(b:User)", "a.user_id"),
    ("social_polymorphic", "(a:User)-[:FOLLOWS]-(b:User)", "a.user_id"),
    ("fk_edge", "(o:Order)-[:PLACED_BY]->(c:Customer)", "o.order_id"),
    ("fk_edge", "(o:Order)-[:PLACED_BY]->(c:Customer)", "c.customer_id"),
])
def test_number_of_groups_after_a_with_aggregate(schema, pattern, key):
    want = _scalar(schema, f"MATCH {pattern} RETURN count(DISTINCT {key}) AS n")
    got = _scalar(schema, f"MATCH {pattern} WITH {key} AS x, count(*) AS c RETURN count(*) AS n")
    assert got == want
