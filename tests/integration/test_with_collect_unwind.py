"""
#1271: `WITH collect(x) AS xs UNWIND xs AS y` aborted the server (stack overflow).

The plan is Unwind(WithClause(..)). The chained WITH builder's loop condition (a copy of
`has_with_clause_in_graph_rel` with no `Unwind` arm) was false while the dispatcher's predicate was
true, so the plan was rendered unchanged and re-entered the builder forever.
"""

import pytest
from conftest import execute_cypher


def _rows(schema, query):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


@pytest.mark.parametrize("schema, label, idp", [
    ("social_integration", "User", "user_id"),
    ("social_polymorphic", "User", "user_id"),
    ("fk_edge", "Customer", "customer_id"),
])
def test_collect_then_unwind_round_trips_the_rows(schema, label, idp):
    plain = sorted(str(r["x"]) for r in _rows(schema, f"MATCH (a:{label}) RETURN a.{idp} AS x"))
    through = sorted(str(r["x"]) for r in _rows(
        schema, f"MATCH (a:{label}) WITH collect(a.{idp}) AS xs UNWIND xs AS x RETURN x"))
    assert through == plain


def test_collect_over_a_pattern_then_unwind_counts_the_matches():
    q = "MATCH (a:User)-[:FOLLOWS]->(b:User)"
    plain = _rows("social_integration", f"{q} RETURN count(*) AS n")[0]["n"]
    through = _rows("social_integration",
                    f"{q} WITH collect(b.user_id) AS bs UNWIND bs AS b RETURN count(*) AS n")[0]["n"]
    assert int(through) == int(plain)


def test_unwind_literal_collect_unwind_again():
    rows = _rows("social_integration",
                 "UNWIND [1, 2, 3] AS v WITH collect(v) AS vs UNWIND vs AS w RETURN w ORDER BY w")
    assert [int(r["w"]) for r in rows] == [1, 2, 3]
