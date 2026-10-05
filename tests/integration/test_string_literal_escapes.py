"""
#1173: backslash escapes inside string literals.

`'it\\'s'` ended the literal at the escaped quote and failed to parse. Each literal below must
round-trip to the value Neo4j gives it, checked through real ClickHouse row values.
"""

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"


def _value(literal):
    result = execute_cypher(f"RETURN {literal} AS r", schema_name=SCHEMA)
    assert "results" in result, f"query failed: {result}"
    return result["results"][0]["r"]


@pytest.mark.parametrize(
    "literal, expected",
    [
        (r"'it\'s'", "it's"),
        (r'"it\'s"', "it's"),
        (r'"say \"hi\""', 'say "hi"'),
        (r"'say \"hi\"'", 'say "hi"'),
        (r"'a\\b'", "a\\b"),
        (r"'tab\there'", "tab\there"),
        (r"'x\ny'", "x\ny"),
        (r"'café'", "café"),
        # an escaped backslash before the closing quote is a backslash, not an escaped quote
        (r"'it\\'", "it\\"),
        (r"'a\\\'b'", "a\\'b"),
        ("\"it's\"", "it's"),
    ],
)
def test_literal_round_trips(literal, expected):
    assert _value(literal) == expected


def test_escaped_quote_in_predicates_matches_the_unescaped_form():
    users = execute_cypher("MATCH (u:User) RETURN count(*) AS n", schema_name=SCHEMA)
    total = int(users["results"][0]["n"])
    for predicate in [
        r"""'it\'s' = "it's" """,
        r"'it\'s' STARTS WITH 'it\''",
        r"'it\'s' =~ '.*\'s'",
        r"'it\'s' IN ['x', 'it\'s']",
    ]:
        # evaluates to true for every row, so the count must equal the unfiltered one
        got = execute_cypher(
            f"MATCH (u:User) WHERE {predicate} RETURN count(*) AS n", schema_name=SCHEMA
        )
        assert "results" in got, f"{predicate}: {got}"
        assert int(got["results"][0]["n"]) == total, predicate
