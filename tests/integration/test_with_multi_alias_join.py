"""
#1214: `WITH a, count(b) AS n MATCH (a)-[:R]->(f)` joined the WITH CTE twice (`AS a_n ON 1 = 1`
and `AS a ON ...`), multiplying every row by the CTE's size (200 where 20 is correct).

Differential oracle, no brute force needed: carrying a node through WITH and matching from it
must count the same rows as matching the two patterns in one clause when the WITH neither
filters nor aggregates; with an aggregate the second MATCH repeats once per GROUP.
"""

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _scalar(query):
    result = execute_cypher(query, schema_name=SCHEMA)
    assert "results" in result, f"query failed: {result}"
    return int(next(iter(result["results"][0].values())))


def test_two_carried_nodes_then_match_from_one():
    joined = _scalar("MATCH (c:User)-[:FOLLOWS]->(b:User) MATCH (b)-[:FOLLOWS]->(z:User) "
                     "RETURN count(*)")
    carried = _scalar("MATCH (c:User)-[:FOLLOWS]->(b:User) WITH c, b "
                      "MATCH (b)-[:FOLLOWS]->(z:User) RETURN count(*)")
    assert carried == joined


def test_carried_node_with_aggregate_then_match():
    # authors x their FOLLOWS edges: one row per (author, followed) pair, NOT times the groups
    edges = execute_cypher("MATCH (a:User)-[:FOLLOWS]->(f:User) RETURN a.user_id AS a",
                           schema_name=SCHEMA)["results"]
    authors = {r["a"] for r in execute_cypher(
        "MATCH (a:User)-[:AUTHORED]->(p:Post) RETURN DISTINCT a.user_id AS a",
        schema_name=SCHEMA)["results"]}
    expected = sum(1 for r in edges if r["a"] in authors)
    got = _scalar("MATCH (a:User)-[:AUTHORED]->(p:Post) WITH a, count(p) AS n "
                  "MATCH (a)-[:FOLLOWS]->(f:User) RETURN count(*)")
    assert got == expected


def test_where_on_a_carried_node():
    joined = _scalar("MATCH (c:User)-[:FOLLOWS]->(b:User) WHERE c.user_id < 3 "
                     "MATCH (b)-[:FOLLOWS]->(z:User) RETURN count(*)")
    carried = _scalar("MATCH (c:User)-[:FOLLOWS]->(b:User) WITH c, b WHERE c.user_id < 3 "
                      "MATCH (b)-[:FOLLOWS]->(z:User) RETURN count(*)")
    assert carried == joined


@pytest.mark.parametrize("carry", ["c, b", "b, c"])
def test_after_a_path_scope(carry):
    # the same pattern with the WITH replaced by a second MATCH is the oracle
    path_then_hop = _scalar(
        f"MATCH (c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) WITH {carry} "
        "MATCH (b)-[:FOLLOWS]->(z:User) RETURN count(*)")
    no_with_pairs = _scalar(
        "MATCH (c:User)-[:FOLLOWS]->(a:User)-[:FOLLOWS*1..2]->(b:User) RETURN count(*)")
    assert 0 < path_then_hop
    # every (c, a, b) row extends by b's out-degree: bounded above by rows x max degree
    assert path_then_hop <= no_with_pairs * 20
