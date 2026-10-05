"""
#1244: an unlabeled pattern over the polymorphic edge table that the renderer could not resolve to a
concrete source used to render `SELECT count(*)` with no FROM and answer 1 (silent wrong). It is
refused loudly; the labeled forms must keep returning the right counts.
"""

import pytest
from conftest import execute_cypher

SCHEMA = "social_polymorphic"


def _run(query):
    return execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)


@pytest.mark.parametrize("query", [
    "MATCH (a)-[r]->(b) RETURN count(*) AS n",
    "MATCH (a)-[r:FOLLOWS]->(b) RETURN count(*) AS n",
    "MATCH (a)-[r:FOLLOWS|LIKES]->(b) RETURN count(*) AS n",
])
def test_unresolved_pattern_is_refused(query):
    result = _run(query)
    assert "results" not in result and "#1244" in str(result), result


def test_labeled_forms_still_count_correctly():
    follows = int(_run("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN count(*) AS n")["results"][0]["n"])
    users = int(_run("MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN count(r) AS n")["results"][0]["n"])
    assert follows == users > 0
    total = int(_run("MATCH (a:User)-[r]->(b) RETURN count(*) AS n")["results"][0]["n"])
    assert total >= follows
