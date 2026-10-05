"""
#1257: on the FK-edge layout (`PLACED_BY: Order -> Customer`) an unlabeled outer-bound endpoint was
read through the relationship's SOURCE role, so `NOT (c)<-[:PLACED_BY]-()` correlated Customer `c` via
`c.order_id` (Code 47). Every pattern-predicate form must equal the same pattern as a MATCH.
"""

import itertools

import pytest
from conftest import execute_cypher

SCHEMA = "fk_edge"


def _ids(query):
    result = execute_cypher(query, schema_name=SCHEMA, raise_on_error=False)
    assert "results" in result, (query, result)
    return {int(r["id"]) for r in result["results"]}


ANCHORS = [("Order", "order_id"), ("Customer", "customer_id")]
ARROWS = ["-[:PLACED_BY]->", "<-[:PLACED_BY]-", "-[:PLACED_BY]-"]
TARGETS = ["()", "(:Order)", "(:Customer)"]


@pytest.mark.parametrize(
    "anchor,idp,arrow,target",
    [(a, i, r, t) for (a, i), r, t in itertools.product(ANCHORS, ARROWS, TARGETS)],
)
def test_pattern_predicates_match_the_match_oracle(anchor, idp, arrow, target):
    pattern = f"(a){arrow}{target}"
    labeled = pattern.replace("(a)", f"(a:{anchor})", 1)
    oracle_result = execute_cypher(
        f"MATCH {labeled} RETURN DISTINCT a.{idp} AS id",
        schema_name=SCHEMA,
        raise_on_error=False,
    )
    if "results" not in oracle_result:
        pytest.skip("pattern is not valid for these labels")
    oracle = {int(r["id"]) for r in oracle_result["results"]}
    everyone = _ids(f"MATCH (a:{anchor}) RETURN a.{idp} AS id")

    def ids(pred):
        return _ids(f"MATCH (a:{anchor}) WHERE {pred} RETURN a.{idp} AS id")

    assert ids(f"EXISTS {{ {pattern} }}") == oracle
    assert ids(pattern) == oracle
    assert ids(f"NOT EXISTS {{ {pattern} }}") == everyone - oracle
    assert ids(f"NOT {pattern}") == everyone - oracle
