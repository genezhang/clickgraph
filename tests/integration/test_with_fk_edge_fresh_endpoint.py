"""
`WITH a MATCH (a)-[:AUTHORED]->(p:Post)` on a FK-edge whose FRESH endpoint owns the FK (the edge table is
the Post table, one-to-many from the user).

The pruned join plan left no FROM marker and the CTE-backed `a` became the FROM, so the Post table was
never joined: rows were lost or not multiplied (20 where 40 are right) and `p.post_id` was unresolved
(Code 47). The post-WITH OPTIONAL form demoted the Post table to a LEFT JOIN that the unreferenced-join
pass then dropped (20 where 40).
"""

import collections

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _bag(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return collections.Counter(str(r["v"]) for r in result["results"])


POSTS = "MATCH (a:User)-[:AUTHORED]->(p:Post) RETURN a.user_id AS v"
HOPS = {
    "-[:FOLLOWS]->": "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS v",
    "<-[:FOLLOWS]-": "MATCH (a:User)<-[:FOLLOWS]-(b:User) RETURN a.user_id AS v",
    "-[:FOLLOWS]-": "MATCH (a:User)-[:FOLLOWS]-(b:User) RETURN a.user_id AS v",
}


def _expected(hop, optional):
    posts = _bag(POSTS)
    out = collections.Counter()
    for a, n in _bag(HOPS[hop]).items():
        out[a] = n * (max(posts[a], 1) if optional else posts[a])
    return +out


@pytest.mark.parametrize("hop", list(HOPS))
@pytest.mark.parametrize("returns", ["a.user_id", "a.user_id AS x, b.user_id"])
def test_post_with_fk_edge_hop_multiplies_rows(hop, returns):
    w = "WITH a" if "b." not in returns else "WITH a, b"
    q = f"MATCH (a:User){hop}(b:User) {w} MATCH (a)-[:AUTHORED]->(p:Post) RETURN a.user_id AS v"
    assert _bag(q) == _expected(hop, optional=False)


@pytest.mark.parametrize("hop", list(HOPS))
def test_post_with_fk_edge_hop_endpoint_is_readable(hop):
    q = f"MATCH (a:User){hop}(b:User) WITH a MATCH (a)-[:AUTHORED]->(p:Post) RETURN a.user_id AS v, p.post_id AS w"
    result = execute_cypher(q, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, result
    assert sum(_expected(hop, optional=False).values()) == len(result["results"])


@pytest.mark.parametrize("hop", list(HOPS))
def test_post_with_optional_fk_edge_hop_keeps_the_fan_out(hop):
    q = f"MATCH (a:User){hop}(b:User) WITH a OPTIONAL MATCH (a)-[:AUTHORED]->(p:Post) RETURN a.user_id AS v"
    assert _bag(q) == _expected(hop, optional=True)
