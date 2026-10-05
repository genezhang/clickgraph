"""
#1234: a WITH body over a 3-hop pattern with an undirected hop (a UNION of direction arms) failed
Code 47: the arm's joins were collected on their own with no FROM marker for the anchor `n0`, the
edge-uniqueness Filter made the arm "complex", and the topological sort then found nothing to start
from. The error was swallowed by the inner-scope handler, so the second arm rendered unplanned.
"""

import itertools

import pytest
from conftest import execute_cypher

SCHEMA = "social_integration"
ARROWS = {"o": "-[:FOLLOWS]->", "i": "<-[:FOLLOWS]-", "u": "-[:FOLLOWS]-"}


def _count(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return int(result["results"][0]["n"])


def _pattern(dirs):
    return "MATCH (n0:User)" + "".join(
        f"{ARROWS[d]}(n{k + 1}:User)" for k, d in enumerate(dirs)
    )


@pytest.mark.parametrize("dirs", ["".join(d) for d in itertools.product("oiu", repeat=3)])
def test_three_hop_with_matches_the_plain_count(dirs):
    pat = _pattern(dirs)
    plain = _count(f"{pat} RETURN count(*) AS n")
    assert _count(f"{pat} WITH n0, count(*) AS k RETURN sum(k) AS n") == plain
    assert _count(f"{pat} WITH n0, n3 RETURN count(*) AS n") == plain
    assert _count(f"{pat} WITH n0 AS x, n3 AS y RETURN count(*) AS n") == plain


@pytest.mark.parametrize("dirs", ["ouo", "oou", "uoo", "uuu"])
def test_four_hop_with_matches_the_plain_count(dirs):
    pat = _pattern(dirs + "o")
    plain = _count(f"{pat} RETURN count(*) AS n")
    assert _count(f"{pat} WITH n0, count(*) AS k RETURN sum(k) AS n") == plain
