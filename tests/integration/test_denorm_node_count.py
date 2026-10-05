"""
#1240: `MATCH (a:Airport) RETURN count(*)` on a fully-denormalized layout (the node exists only as
the from-/to-role columns of the edge table) returned 1 — the from-/to-role arms projected a
CONSTANT and their UNION DISTINCT collapsed every row. `count(a)` and the property forms were right.
"""

import pytest
from conftest import execute_cypher


def _rows(query, schema="denormalized_flights"):
    result = execute_cypher(query, schema_name=schema, raise_on_error=False)
    assert "results" in result, f"{query}: {result}"
    return result["results"]


def _airports():
    return {r["c"]: r for r in _rows("MATCH (a:Airport) RETURN a.code AS c, a.state AS s, a.city AS city")}


def test_bare_count_star_counts_distinct_nodes():
    airports = _airports()
    assert len(airports) > 3
    assert int(_rows("MATCH (a:Airport) RETURN count(*) AS n")[0]["n"]) == len(airports)
    assert int(_rows("MATCH (a:Airport) RETURN count(a) AS n")[0]["n"]) == len(airports)


def test_count_star_with_a_filter():
    airports = _airports()
    codes = sorted(airports)
    for k in (2, 3, len(codes)):
        subset = codes[:k]
        listed = ", ".join(f"'{c}'" for c in subset)
        got = int(_rows(f"MATCH (a:Airport) WHERE a.code IN [{listed}] RETURN count(*) AS n")[0]["n"])
        assert got == len(subset), subset


def test_grouping_by_a_non_unique_property_counts_nodes_not_distinct_values():
    airports = _airports()
    by_state = {}
    for a in airports.values():
        by_state[a["s"]] = by_state.get(a["s"], 0) + 1
    assert max(by_state.values()) > 1, "need a state with several airports"
    for query in [
        "MATCH (a:Airport) RETURN a.state AS s, count(*) AS n",
        "MATCH (a:Airport) RETURN a.state AS s, count(a) AS n",
    ]:
        got = {r["s"]: int(r["n"]) for r in _rows(query)}
        assert got == by_state, query
    unaliased = {r["a.state"]: int(r["n"]) for r in _rows(
        "MATCH (a:Airport) RETURN a.state, count(*) AS n")}
    assert unaliased == by_state


def test_grouping_by_the_id_itself_with_an_alias():
    airports = _airports()
    rows = _rows("MATCH (a:Airport) RETURN a.code AS c, count(*) AS n")
    assert {r["c"] for r in rows} == set(airports) and all(int(r["n"]) == 1 for r in rows)
