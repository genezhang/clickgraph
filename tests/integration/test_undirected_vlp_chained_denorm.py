"""
#1155: on the denormalized layout an UNDIRECTED variable-length path chained to another hop was
rendered with no recursive CTE at all (the CTE was generated, left unreferenced and dropped as
dead): the path ran as ONE hop whatever its bound — `*1..1`, `*1..2` and `*1..3` returned the same
rows, ~3x off an independent oracle. It is now refused loudly; layouts whose undirected path is a
single walk (standard, polymorphic) are unchanged and their counts must GROW with the bound.
"""

import pytest
from conftest import execute_cypher


def _run(schema, pattern):
    return execute_cypher(f"MATCH {pattern} RETURN count(*) AS n", schema_name=schema,
                          raise_on_error=False)


@pytest.mark.parametrize("schema", ["denormalized_flights", "ontime_flights"])
@pytest.mark.parametrize("pattern", [
    "(o:Airport)-[:FLIGHT*1..2]-(d:Airport)-[:FLIGHT]->(e:Airport)",
    "(o:Airport)-[:FLIGHT]->(d:Airport)-[:FLIGHT*1..2]-(e:Airport)",
])
def test_chained_undirected_path_on_denormalized_is_refused(schema, pattern):
    result = _run(schema, pattern)
    assert "results" not in result and "#1155" in str(result), result


@pytest.mark.parametrize("schema, label, rel", [
    ("social_integration", "User", "FOLLOWS"),
    ("standard", "User", "FOLLOWS"),
])
def test_chained_undirected_path_on_a_single_walk_layout_still_honors_the_bound(schema, label, rel):
    counts = []
    for hi in (1, 2, 3):
        pattern = f"(o:{label})-[:{rel}*1..{hi}]-(d:{label})-[:{rel}]->(e:{label})"
        result = _run(schema, pattern)
        assert "results" in result, result
        counts.append(int(result["results"][0]["n"]))
    assert counts[0] < counts[1] < counts[2], counts


def test_directed_chain_on_denormalized_still_renders_and_grows():
    counts = []
    for hi in (1, 2, 3):
        result = _run("denormalized_flights",
                      f"(o:Airport)-[:FLIGHT*1..{hi}]->(d:Airport)-[:FLIGHT]->(e:Airport)")
        assert "results" in result, result
        counts.append(int(result["results"][0]["n"]))
    assert counts[0] <= counts[1] <= counts[2] and counts[0] < counts[2], counts


def test_unchained_undirected_path_on_denormalized_is_unaffected():
    result = _run("denormalized_flights", "(o:Airport)-[:FLIGHT*1..2]-(d:Airport)")
    assert "results" in result and int(result["results"][0]["n"]) > 0, result
