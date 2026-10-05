"""
#1261: a `WITH` that renames or projects over an UNDIRECTED hop answered ONE direction.

The WITH body is a UNION of the two direction arms (the plan itself is the first arm, `union.input`
the rest). The CTE column pruner emptied the body's own SELECT when nothing downstream read a column
(`WITH a.user_id AS x RETURN count(*)`), and the emitter's "union arms only" form then dropped the plan's
own arm: 20 rows where the undirected hop has 40 (every edge seen from both ends).
"""

import collections

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"
HOP = "MATCH (a:User)-[:FOLLOWS]-(b:User)"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _count(query):
    return int(_rows(query)[0]["n"])


def test_the_hop_is_seen_from_both_ends():
    out = _count("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN count(*) AS n")
    assert _count(f"{HOP} RETURN count(*) AS n") == 2 * out


@pytest.mark.parametrize(
    "projection",
    [
        "a.user_id AS x",
        "a.user_id AS x, b.user_id AS y",
        "a AS x, b AS y",
        "a.user_id AS x, b",
        "a, b.user_id AS y",
    ],
)
def test_with_projection_keeps_both_directions(projection):
    plain = _count(f"{HOP} RETURN count(*) AS n")
    assert _count(f"{HOP} WITH {projection} RETURN count(*) AS n") == plain


def test_rows_are_the_same_multiset_with_and_without_the_with():
    plain = collections.Counter(
        (r["a"], r["b"])
        for r in _rows(f"{HOP} RETURN a.user_id AS a, b.user_id AS b")
    )
    through = collections.Counter(
        (r["x"], r["y"])
        for r in _rows(f"{HOP} WITH a.user_id AS x, b.user_id AS y RETURN x, y")
    )
    assert through == plain
    # both orientations of every edge are present
    assert all(plain[(b, a)] == plain[(a, b)] for (a, b) in plain)


def test_aggregation_after_the_with_matches_the_plain_aggregation():
    plain = {
        int(r["a"]): int(r["n"])
        for r in _rows(f"{HOP} RETURN a.user_id AS a, count(*) AS n")
    }
    through = {
        int(r["x"]): int(r["n"])
        for r in _rows(f"{HOP} WITH a.user_id AS x, b.user_id AS y RETURN x, count(*) AS n")
    }
    assert through == plain
