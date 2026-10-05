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
