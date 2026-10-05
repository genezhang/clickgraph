"""
The arms of a Cypher `UNION` are independent queries; the answer is the bag (UNION ALL) / set (UNION)
of each arm run alone (#1273).

Before the fix the analyzer merged every arm's pattern metadata:
- relationship uniqueness was applied to NO arm (`a->b<-c` UNION ALL itself: 32 rows, not 2 x 14);
- a node NAME shared by two arms looked like one node, so cross-branch joins invented the other arm's
  edge join inside this arm (`P UNION ALL (a->b->AUTHORED p)`: 21 rows, not 30);
- an alias OPTIONAL in one arm made the same-named alias of a plain arm a LEFT JOIN;
- `EMPTY UNION <undirected arm>` kept the duplicates (the inner connector was always UNION ALL).
"""

import collections

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"

CHAIN = "MATCH (a:User)-[:FOLLOWS]->(b:User)<-[:FOLLOWS]-(c:User) RETURN a.user_id AS v"
HOP = "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS v"
AUTHORED = "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:AUTHORED]->(p:Post) WHERE a.user_id < 4 RETURN a.user_id AS v"
TWO_HOP = "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) WHERE a.user_id < 4 RETURN a.user_id AS v"
REVERSED = "MATCH (a:User)<-[:FOLLOWS]-(b:User) WHERE a.user_id < 8 RETURN a.user_id AS v"
REVERSED_PLAIN = "MATCH (a:User)<-[:FOLLOWS]-(b:User) RETURN a.user_id AS v"
OPTIONAL = "MATCH (a:User) OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.user_id AS v"
UNDIRECTED = "MATCH (a:User)-[:FOLLOWS]-(b:User) RETURN a.user_id AS v"
EMPTY = "MATCH (z:User) WHERE z.user_id < 0 RETURN z.user_id AS v"


def _bag(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return collections.Counter(str(r["v"]) for r in result["results"])


@pytest.mark.parametrize(
    "left,right",
    [
        (CHAIN, CHAIN),
        (HOP, AUTHORED),
        (AUTHORED, HOP),
        (REVERSED, AUTHORED),
        (TWO_HOP, AUTHORED),
        (OPTIONAL, HOP),
        (HOP, OPTIONAL),
        (REVERSED_PLAIN, OPTIONAL),
        (UNDIRECTED, HOP),
        (CHAIN, UNDIRECTED),
    ],
)
def test_union_all_is_the_bag_sum_of_the_arms(left, right):
    assert _bag(f"{left} UNION ALL {right}") == _bag(left) + _bag(right)


@pytest.mark.parametrize(
    "left,right",
    [
        (CHAIN, CHAIN),
        (REVERSED, AUTHORED),
        (REVERSED_PLAIN, OPTIONAL),
        (UNDIRECTED, HOP),
        (EMPTY, UNDIRECTED),
        (UNDIRECTED, EMPTY),
    ],
)
def test_union_is_the_set_union_of_the_arms(left, right):
    want = set(_bag(left)) | set(_bag(right))
    assert _bag(f"{left} UNION {right}") == collections.Counter(want)


DENORM = "denormalized_flights"


def _denorm_bag(query):
    result = execute_cypher(query, schema_name=DENORM, raise_on_error=False)
    assert "results" in result, (query, result)
    return collections.Counter(str(r["v"]) for r in result["results"])


@pytest.mark.parametrize("node_first", [True, False])
def test_union_all_after_denormalized_node_scan_keeps_every_arm_row(node_first):
    """A denormalized node scan is itself a DISTINCT union of its from/to columns; as a later arm of a
    `UNION ALL` its connector must not de-duplicate the arms before it."""
    hop = "MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code AS v"
    node = "MATCH (x:Airport) RETURN x.code AS v"
    left, right = (node, hop) if node_first else (hop, node)
    assert _denorm_bag(f"{left} UNION ALL {right}") == _denorm_bag(left) + _denorm_bag(right)
