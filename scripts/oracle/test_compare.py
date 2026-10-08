"""Offline tests for the comparator rules (no Neo4j or ClickGraph needed)."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import compare  # noqa: E402


def neo(columns, rows):
    """A Neo4j tx-API result; each row is a list of (value, meta)."""
    return {"columns": columns, "data": [{"row": [v for v, _ in r], "meta": [m for _, m in r]} for r in rows]}


def scalar_rows(col, values):
    return neo([col], [[(v, None)] for v in values])


def verdict(cypher, neo_result, cg, neo_full=None):
    return compare.compare_expected(cypher, compare.expected_from_neo4j(cypher, neo_result, neo_full), cg)[0]


def test_integers_are_exact_and_integral_floats_equal_ints():
    assert compare.norm_value(1700000000001) != compare.norm_value(1700000000002)
    assert compare.norm_value(3) == compare.norm_value(3.0)
    assert compare.norm_value(0.1 + 0.2) == compare.norm_value(0.3)
    assert compare.norm_value(True) is True  # booleans are not integers


def test_node_is_flattened_and_nulls_and_loader_keys_dropped():
    q = "MATCH (u) RETURN u"
    result = neo(["u"], [[({"name": "A", "__cg_id": "[1]"}, {"type": "node"})]])
    assert verdict(q, result, [{"u.name": "A", "u.age": None}]) == "MATCH"


def test_relationship_endpoints_are_compared():
    q = "MATCH ()-[r]->() RETURN r"
    rel = {"since": 1, "__cg_from": "[7]", "__cg_to": "[8]"}
    result = neo(["r"], [[(rel, {"type": "relationship"})]])
    assert verdict(q, result, [{"r.from_id": 7, "r.to_id": 8, "r.since": 1}]) == "MATCH"
    assert verdict(q, result, [{"r.from_id": 8, "r.to_id": 7, "r.since": 1}]) == "MISMATCH"


def test_null_optional_entity_is_no_columns_on_both_sides():
    q = "MATCH (u) OPTIONAL MATCH (u)-->(p) RETURN u.id, p"
    result = neo(["u.id", "p"], [[(1, None), (None, None)]])
    assert verdict(q, result, [{"u.id": 1, "p.x": None, "p.y": None}]) == "MATCH"


def test_wrong_value_is_a_mismatch():
    assert verdict("RETURN 40 AS n", scalar_rows("n", [40]), [{"n": 60}]) == "MISMATCH"


def test_limit_requires_a_valid_subset_of_the_full_answer():
    q = "MATCH (n) RETURN n.x LIMIT 2"
    limited, full = scalar_rows("n.x", [1, 2]), scalar_rows("n.x", [1, 2, 3])
    assert verdict(q, limited, [{"n.x": 3}, {"n.x": 1}], full) == "MATCH"  # another valid pick
    assert verdict(q, limited, [{"n.x": 9}, {"n.x": 1}], full) == "MISMATCH"  # 9 is not an answer
    assert verdict(q, limited, [{"n.x": 1}], full) == "MISMATCH"  # too few


def test_order_by_sequence_is_checked_ties_accepted():
    q = "MATCH (n) RETURN n.k, n.v ORDER BY n.k"
    result = neo(["n.k", "n.v"], [[(1, None), ("a", None)], [(1, None), ("b", None)], [(2, None), ("c", None)]])
    tied = [{"n.k": 1, "n.v": "b"}, {"n.k": 1, "n.v": "a"}, {"n.k": 2, "n.v": "c"}]
    reversed_ = [{"n.k": 2, "n.v": "c"}, {"n.k": 1, "n.v": "a"}, {"n.k": 1, "n.v": "b"}]
    assert verdict(q, result, tied) == "MATCH"
    assert verdict(q, result, reversed_) == "MISMATCH"


def test_order_by_limit_rejects_a_wrong_top_n():
    q = "MATCH (n) RETURN n.age ORDER BY n.age DESC LIMIT 2"
    limited, full = scalar_rows("n.age", [40, 30]), scalar_rows("n.age", [40, 30, 20])
    assert verdict(q, limited, [{"n.age": 40}, {"n.age": 30}], full) == "MATCH"
    assert verdict(q, limited, [{"n.age": 20}, {"n.age": 30}], full) == "MISMATCH"


def test_union_arm_limit_is_unverified():
    q = "MATCH (n) RETURN n.x AS v LIMIT 1 UNION ALL MATCH (m) RETURN m.y AS v LIMIT 1"
    assert verdict(q, scalar_rows("v", [1, 2]), [{"v": 1}, {"v": 2}, {"v": 3}]) == "UNVERIFIED"


def test_id_functions_and_tx_paths_are_incomparable():
    # A path in the tx API's form: its answer is the Query API's (needs_typed).
    tx_path = neo(["p"], [[([{"a": 1}, {}, {"b": 2}], [{"type": "node"}, {"type": "relationship"}, {"type": "node"}])]])
    assert compare.needs_typed(tx_path)
    for cypher, result in [
        ("MATCH (n) WHERE id(n) = 1 RETURN n", neo(["n"], [])),
        ("MATCH p=()-->() RETURN p", tx_path),
    ]:
        try:
            compare.expected_from_neo4j(cypher, result)
        except compare.Incomparable:
            continue
        raise AssertionError(f"{cypher} must be incomparable")


def test_wrong_outcome_signature_is_stable_and_specific():
    a = compare.signature("MISMATCH", "x", [{"n": 1}])
    assert a == compare.signature("MISMATCH", "y", [{"n": 1}])
    assert a != compare.signature("MISMATCH", "x", [{"n": 2}])
    assert compare.signature("CG_ERROR", "Code 47 t12", None) == compare.signature("CG_ERROR", "Code 47 t3", None)


def _node(i, **props):
    return {"elementId": f"x{i}", "labels": ["User"], "properties": {"__cg_id": f"[{i}]", **props}}


def _cg_node(i, **props):
    return {"elementId": f"User:{i}-", "labels": ["User"], "properties": props}


def test_paths_compare_by_elements_on_the_query_api():
    neo_rel = {"elementId": "r", "startNodeElementId": "x1", "endNodeElementId": "x2", "type": "T",
               "properties": {"__cg_from": "[1]", "__cg_to": "[2]", "w": 1.0}}
    cg_rel = {"elementId": "T:1->2-", "startNodeElementId": "User:1-", "endNodeElementId": "User:2-",
              "type": "T", "properties": {"w": 1, "gone": None}}
    typed = {"data": {"fields": ["p", "n"], "values": [[[_node(1, a=1), neo_rel, _node(2, a=2)], 3]]}}
    exp = compare.expected_from_neo4j("MATCH p = (a)-->(b) RETURN p, 3 AS n", typed)
    cg = [{"p": [_cg_node(1, a=1), cg_rel, _cg_node(2, a=2)], "n": 3}]
    assert compare.compare_expected("q", exp, cg)[0] == "MATCH"
    # Reversed endpoints, or another property, differ.
    flipped = dict(cg_rel, startNodeElementId="User:2-", endNodeElementId="User:1-")
    assert compare.compare_expected("q", exp, [{"p": [_cg_node(1, a=1), flipped, _cg_node(2, a=2)], "n": 3}])[0] == "MISMATCH"
    assert compare.compare_expected("q", exp, [{"p": [_cg_node(1, a=9), cg_rel, _cg_node(2, a=2)], "n": 3}])[0] == "MISMATCH"


def test_an_empty_list_keeps_its_column_in_the_tx_api():
    # The tx API's meta has no entry for an empty list.
    result = {"columns": ["l", "n"], "data": [{"row": [[], 1], "meta": [None]}]}
    assert not compare.needs_typed(result)
    exp = compare.expected_from_neo4j("RETURN [] AS l, 1 AS n", result)
    assert compare.compare_expected("RETURN [] AS l, 1 AS n", exp, [{"l": [], "n": 1}])[0] == "MATCH"
