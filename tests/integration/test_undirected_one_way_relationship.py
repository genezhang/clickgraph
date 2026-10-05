"""
#1255: an undirected single-hop size() / EXISTS / NOT pattern over a one-way relationship
(`AUTHORED: User -> Post`) used to emit `from = u OR to = u`; the `to` leg compares a POST id with a
user id, so users that share a number with some post "authored" it (users 11-20 got a count of 1;
the FK-edge EXISTS form failed with Code 47). The labels allow one orientation only, so the undirected
pattern must equal the directed one.
"""

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"]


def _by_user(query):
    return {int(r["id"]): int(r["n"]) for r in _rows(query)}


def test_undirected_size_equals_the_directed_size():
    directed = _by_user(
        "MATCH (u:User) RETURN u.user_id AS id, size((u)-[:AUTHORED]->()) AS n"
    )
    undirected = _by_user(
        "MATCH (u:User) RETURN u.user_id AS id, size((u)-[:AUTHORED]-()) AS n"
    )
    assert undirected == directed
    # the data must discriminate: some users have no posts but share a number with a post
    assert any(v == 0 and uid <= 20 for uid, v in directed.items()), directed


@pytest.mark.parametrize("negate", ["", "NOT "])
@pytest.mark.parametrize("form", ["EXISTS {{ {p} }}", "{p}"])
def test_undirected_exists_and_not_equal_the_directed_forms(negate, form):
    def ids(pattern):
        pred = negate + form.format(p=pattern)
        return {int(r["id"]) for r in _rows(
            f"MATCH (u:User) WHERE {pred} RETURN u.user_id AS id"
        )}

    assert ids("(u)-[:AUTHORED]-()") == ids("(u)-[:AUTHORED]->()")


def test_a_relationship_that_can_run_either_way_keeps_both_legs():
    # FOLLOWS is User -> User: undirected degree = out + in
    out = _by_user("MATCH (u:User) RETURN u.user_id AS id, size((u)-[:FOLLOWS]->()) AS n")
    inc = _by_user("MATCH (u:User) RETURN u.user_id AS id, size((u)<-[:FOLLOWS]-()) AS n")
    both = _by_user("MATCH (u:User) RETURN u.user_id AS id, size((u)-[:FOLLOWS]-()) AS n")
    assert both == {uid: out[uid] + inc[uid] for uid in out}
