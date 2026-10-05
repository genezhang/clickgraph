"""
#1238: a bare relationship pattern used as a boolean — `WHERE (a)-[:R]->()` — was refused
("a graph pattern … in scalar-expression context is not supported") while its negation
`WHERE NOT (a)-[:R]->()` worked. A positive pattern predicate is an existence test and now lowers
to `EXISTS { pattern }`; the NOT form keeps its own lowering.

Python oracle over the FOLLOWS edge list and the user table.
"""

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _rows(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, f"{query}: {result}"
    return result["results"]


def _model():
    edges = {(int(r["a"]), int(r["b"])) for r in _rows(
        "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.user_id AS a, b.user_id AS b")}
    ages = {int(r["a"]): r["age"] for r in _rows(
        "MATCH (a:User) RETURN a.user_id AS a, a.age AS age")}
    return edges, ages


def _count(query):
    return int(_rows(query)[0]["n"])


def test_positive_and_negative_pattern_predicates_partition_the_users():
    edges, ages = _model()
    has_out = {a for a, _ in edges}
    pos = _count("MATCH (a:User) WHERE (a)-[:FOLLOWS]->() RETURN count(*) AS n")
    neg = _count("MATCH (a:User) WHERE NOT (a)-[:FOLLOWS]->() RETURN count(*) AS n")
    assert pos == len(has_out) and neg == len(ages) - len(has_out) and pos > 0 and neg > 0


@pytest.mark.parametrize("threshold", [25, 30, 40])
def test_pattern_predicate_combined_with_and_or(threshold):
    edges, ages = _model()
    has_out = {a for a, _ in edges}
    older = {u for u, age in ages.items() if age is not None and age > threshold}
    assert _count(f"MATCH (a:User) WHERE (a)-[:FOLLOWS]->() AND a.age > {threshold} "
                  "RETURN count(*) AS n") == len(has_out & older)
    assert _count(f"MATCH (a:User) WHERE (a)-[:FOLLOWS]->() OR a.age > {threshold} "
                  "RETURN count(*) AS n") == len(has_out | older)


def test_pattern_predicate_between_two_bound_nodes():
    edges, ages = _model()
    assert _count("MATCH (a:User), (b:User) WHERE (a)-[:FOLLOWS]->(b) "
                  "RETURN count(*) AS n") == len(edges)
    # reversed direction
    assert _count("MATCH (a:User), (b:User) WHERE (a)<-[:FOLLOWS]-(b) "
                  "RETURN count(*) AS n") == len(edges)


def test_typed_end_node():
    edges, ages = _model()
    assert _count("MATCH (a:User) WHERE (a)-[:FOLLOWS]->(:User) RETURN count(*) AS n") == \
        len({a for a, _ in edges})


def test_a_two_hop_pattern_predicate_stays_loud():
    # the existing EXISTS multi-hop guard (#574): only the first hop would be checked
    result = execute_cypher(
        "MATCH (a:User) WHERE (a)-[:FOLLOWS]->()-[:FOLLOWS]->() RETURN count(*) AS n",
        schema_name=SCHEMA, raise_on_error=False)
    assert "results" not in result and "#574" in str(result), result


def test_a_hop_bound_or_ored_types_still_fail_loudly():
    for query in [
        "MATCH (a:User) WHERE (a)-[:FOLLOWS*1..2]->() RETURN count(*) AS n",
        "MATCH (a:User) WHERE (a)-[:FOLLOWS|FRIENDS_WITH]->() RETURN count(*) AS n",
    ]:
        result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
        assert "results" not in result and "#588" in str(result), result


def test_pattern_predicate_in_case_and_projection():
    edges, ages = _model()
    has_out = {a for a, _ in edges}
    rows = _rows("MATCH (u:User) RETURN u.user_id AS id, "
                 "CASE WHEN (u)-[:FOLLOWS]->() THEN 1 ELSE 0 END AS c, (u)-[:FOLLOWS]->() AS h")
    assert {int(r["id"]) for r in rows if r["c"] == 1} == has_out
    assert {int(r["id"]) for r in rows if r["h"] in (True, 1)} == has_out
    assert _count("MATCH (u:User) WHERE CASE WHEN (u)-[:FOLLOWS]->() THEN true ELSE false END "
                  "RETURN count(*) AS n") == len(has_out)
