"""Offline tests for the comparator rules (no Neo4j or ClickGraph needed)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import compare  # noqa: E402


def neo(columns, rows):
    """A Neo4j tx-API result: rows are lists of (value, meta)."""
    return {"columns": columns, "data": [{"row": [v for v, _ in r], "meta": [m for _, m in r]} for r in rows]}


def test_numbers_and_booleans_normalize():
    assert compare.norm_value(True) == 1
    assert compare.norm_value(3) == compare.norm_value(3.0)
    assert compare.norm_value(0.1 + 0.2) == compare.norm_value(0.3)


def test_node_is_flattened_and_nulls_and_loader_key_dropped():
    result = neo(["u"], [[({"name": "A", "__cg_id": "[1]"}, {"type": "node"})]])
    expected = compare.expected_from_neo4j("MATCH (u) RETURN u", result)
    cg = [{"u.name": "A", "u.age": None}]
    assert compare.compare_expected("MATCH (u) RETURN u", expected, cg)[0] == "MATCH"


def test_relationship_endpoint_columns_are_not_compared():
    result = neo(["r"], [[({"since": 1}, {"type": "relationship"})]])
    expected = compare.expected_from_neo4j("MATCH ()-[r]->() RETURN r", result)
    cg = [{"r.from_id": 7, "r.to_id": 8, "r.since": 1}]
    assert compare.compare_expected("MATCH ()-[r]->() RETURN r", expected, cg)[0] == "MATCH"


def test_wrong_value_is_a_mismatch():
    result = neo(["n"], [[(40, None)]])
    expected = compare.expected_from_neo4j("RETURN 40 AS n", result)
    assert compare.compare_expected("RETURN 40 AS n", expected, [{"n": 60}])[0] == "MISMATCH"


def test_limit_without_order_compares_counts_only():
    q = "MATCH (n) RETURN n.x LIMIT 2"
    expected = compare.expected_from_neo4j(q, neo(["n.x"], [[(1, None)], [(2, None)]]))
    assert compare.compare_expected(q, expected, [{"n.x": 5}, {"n.x": 6}])[0] == "LIMIT_COUNT_ONLY"
    assert compare.compare_expected(q, expected, [{"n.x": 5}])[0] == "MISMATCH"


def test_id_functions_are_incomparable():
    try:
        compare.expected_from_neo4j("MATCH (n) WHERE id(n) = 1 RETURN n", neo(["n"], []))
    except compare.Incomparable:
        return
    raise AssertionError("id() must be incomparable")


def test_paths_are_incomparable():
    result = neo(["p"], [[([{"a": 1}, {}, {"b": 2}], [{"type": "node"}, {"type": "relationship"}, {"type": "node"}])]])
    try:
        compare.expected_from_neo4j("MATCH p=()-->() RETURN p", result)
    except compare.Incomparable:
        return
    raise AssertionError("paths must be incomparable")


def test_trailing_limit_is_stripped():
    assert compare.without_trailing_limit("MATCH (n) RETURN n ORDER BY n.x LIMIT 10") == "MATCH (n) RETURN n ORDER BY n.x"
    assert compare.without_trailing_limit("MATCH (n) RETURN n") is None
