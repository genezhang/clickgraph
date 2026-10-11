"""
#1244: an unlabeled pattern over the polymorphic edge table that the renderer could not resolve to a
concrete source used to render `SELECT count(*)` with no FROM and answer 1 (silent wrong). It now
counts every relationship of the requested types whatever its endpoint labels; the oracle is the
sum of the labeled forms, which must keep returning the right counts.
"""

import pytest
from conftest import execute_cypher

SCHEMA = "social_polymorphic"


def _run(query):
    return execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)


TYPES = ["FOLLOWS", "LIKES", "AUTHORED", "COMMENTED", "SHARED"]
LABELS = ["User", "Post"]


def _count(query):
    result = _run(query)
    assert "results" in result, (query, result)
    return int(result["results"][0]["n"])


@pytest.mark.parametrize("rel, types, expected", [
    ("[r]", TYPES, 10),
    ("[r:FOLLOWS]", ["FOLLOWS"], 4),
    ("[r:FOLLOWS|LIKES]", ["FOLLOWS", "LIKES"], 7),
])
def test_unresolved_pattern_counts_every_matching_relationship(rel, types, expected):
    oracle = sum(_count(f"MATCH (a:{label})-[:{t}]->(b) RETURN count(*) AS n")
                 for t in types for label in LABELS)
    assert _count(f"MATCH (a)-{rel}->(b) RETURN count(*) AS n") == oracle == expected


def test_labeled_forms_still_count_correctly():
    follows = int(_run("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN count(*) AS n")["results"][0]["n"])
    users = int(_run("MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN count(r) AS n")["results"][0]["n"])
    assert follows == users > 0
    total = int(_run("MATCH (a:User)-[r]->(b) RETURN count(*) AS n")["results"][0]["n"])
    assert total >= follows
