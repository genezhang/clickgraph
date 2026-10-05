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


def _counts(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return {int(r["id"]): int(r["n"]) for r in result["results"]}


@pytest.mark.parametrize("rel_type", TYPES)
@pytest.mark.parametrize("target", ["()", "(:User)", "(:Post)"])
def test_size_pattern_matches_the_match_oracle(rel_type, target):
    """size((u)-[:T]->X) per user == the MATCH count (zero-filled); undirected = out + in."""
    everyone = _ids("MATCH (u:User) RETURN u.user_id AS id")

    def oracle(arrow):
        got = _counts(
            f"MATCH (u:User){arrow.format(t=rel_type)}{target} RETURN u.user_id AS id, count(*) AS n"
        )
        return {uid: got.get(uid, 0) for uid in everyone}

    def sized(arrow):
        got = _counts(
            f"MATCH (u:User) RETURN u.user_id AS id, "
            f"size((u){arrow.format(t=rel_type)}{target}) AS n"
        )
        return {uid: got.get(uid, 0) for uid in everyone}

    out, inc = oracle("-[:{t}]->"), oracle("<-[:{t}]-")
    assert sized("-[:{t}]->") == out
    assert sized("<-[:{t}]-") == inc
    assert sized("-[:{t}]-") == {uid: out[uid] + inc[uid] for uid in everyone}


@pytest.mark.parametrize("rel_type", TYPES)
def test_undirected_unlabeled_target_aggregate_is_out_plus_in_1250(rel_type):
    """`(u)-[:T]-()` per user == `->()` + `<-()` (the directed forms were already right)."""
    everyone = _ids("MATCH (u:User) RETURN u.user_id AS id")

    def counts(arrow):
        got = _counts(
            f"MATCH (u:User){arrow.format(t=rel_type)}() RETURN u.user_id AS id, count(*) AS n"
        )
        return {uid: got.get(uid, 0) for uid in everyone}

    out, inc, both = counts("-[:{t}]->"), counts("<-[:{t}]-"), counts("-[:{t}]-")
    assert both == {uid: out[uid] + inc[uid] for uid in everyone}
    total = _counts(f"MATCH (u:User)-[:{rel_type}]-() RETURN 1 AS id, count(*) AS n")
    assert total[1] == sum(out.values()) + sum(inc.values())


def _scalar(query, column="n"):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return result["results"][0][column]


@pytest.mark.parametrize("uid", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("arrow", ["-[:FOLLOWS]->", "<-[:FOLLOWS]-", "-[:FOLLOWS]-"])
def test_aggregate_argument_reads_the_anchor_not_the_anonymous_target_1252(uid, arrow):
    matches = len(
        _ids(f"MATCH (u:User){arrow}(:User) WHERE u.user_id = {uid} RETURN u.user_id AS id")
    )
    exists = matches > 0
    q = f"MATCH (u:User){arrow}() WHERE u.user_id = {uid} RETURN "
    assert int(_scalar(q + "count(DISTINCT u) AS n")) == (1 if exists else 0)
    assert int(_scalar(q + "count(DISTINCT u.user_id) AS n")) == (1 if exists else 0)
    if exists:
        assert int(_scalar(q + "max(u.user_id) AS n")) == uid
        assert int(_scalar(q + "min(u.user_id) AS n")) == uid


def test_undirected_distinct_anchor_count_includes_target_only_users_1252():
    # user 4 is only ever followed: the reverse direction must contribute it
    assert int(_scalar("MATCH (u:User)-[:FOLLOWS]-() RETURN count(DISTINCT u) AS n")) == 4
