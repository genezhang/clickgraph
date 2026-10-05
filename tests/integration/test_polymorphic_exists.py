"""
#1247: EXISTS / NOT (pattern) over a POLYMORPHIC edge table ignored the relationship type (and the
endpoint label columns), so `EXISTS { (u)<-[:FOLLOWS]-() }` meant "some interaction reaches u" (4
users where FOLLOWS gives 3). An unlabeled outer-bound endpoint `(u)` failed with
"Node schema not found for type '$any'". The oracle is the same pattern as a MATCH.
"""

import itertools

import pytest
from conftest import execute_cypher

SCHEMA = "social_polymorphic"

TYPES = ["FOLLOWS", "LIKES", "AUTHORED"]
ARROWS = ["-[:{t}]->", "<-[:{t}]-", "-[:{t}]-"]
TARGETS = ["()", "(:User)", "(:Post)"]


def _ids(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return {int(r["id"]) for r in result["results"]}


@pytest.mark.parametrize(
    "rel_type,arrow,target",
    list(itertools.product(TYPES, ARROWS, TARGETS)),
)
def test_exists_matches_the_match_oracle(rel_type, arrow, target):
    pattern = "(u)" + arrow.format(t=rel_type) + target
    oracle = _ids(f"MATCH {pattern.replace('(u)', '(u:User)', 1)} RETURN DISTINCT u.user_id AS id")
    everyone = _ids("MATCH (u:User) RETURN u.user_id AS id")

    exists = _ids(f"MATCH (u:User) WHERE EXISTS {{ {pattern} }} RETURN u.user_id AS id")
    bare = _ids(f"MATCH (u:User) WHERE {pattern} RETURN u.user_id AS id")
    not_exists = _ids(f"MATCH (u:User) WHERE NOT EXISTS {{ {pattern} }} RETURN u.user_id AS id")
    not_bare = _ids(f"MATCH (u:User) WHERE NOT {pattern} RETURN u.user_id AS id")

    assert exists == oracle, pattern
    assert bare == oracle, pattern
    assert not_exists == everyone - oracle, pattern
    assert not_bare == everyone - oracle, pattern


def test_the_type_actually_discriminates():
    # guards the sweep above against passing on data where every type agrees
    incoming = {
        t: _ids(f"MATCH (u:User) WHERE EXISTS {{ (u)<-[:{t}]-() }} RETURN u.user_id AS id")
        for t in TYPES
    }
    assert len({frozenset(v) for v in incoming.values()}) > 1, incoming


def test_multi_type_exists_is_still_refused_not_truncated():
    result = execute_cypher(
        "MATCH (u:User) WHERE EXISTS { (u)-[:FOLLOWS|LIKES]->() } RETURN u.user_id AS id",
        schema_name=SCHEMA,
        raise_on_error=False,
    )
    assert "results" not in result, result
