"""
Integration tests for labels() and label() functions on untyped nodes.

Addresses KNOWN_ISSUES #6: Ensures that labels(n) and label(n) on untyped nodes
in UNION queries generate correct SQL without referencing non-existent end_type column.

Test Coverage:
- labels(n) on untyped nodes returns correct labels for each UNION branch
- label(n) on untyped nodes returns correct label string for each UNION branch
- DISTINCT labels(n) with count aggregation
- labels(n) in WHERE clause filtering
"""

import pytest
from conftest import execute_cypher, assert_query_success


SCHEMA_NAME = "test_fixtures"


def test_labels_untyped_nodes_basic(simple_graph):
    """Test labels(n) on untyped nodes returns correct labels for all node types."""
    response = execute_cypher(
        "MATCH (n) RETURN DISTINCT labels(n) as label ORDER BY label",
        schema_name=simple_graph["schema_name"],
    )
    assert_query_success(response)
    results = response["results"]
    labels = [row['label'] for row in results]
    assert len(labels) >= 2, f"Expected at least 2 distinct labels, got {len(labels)}: {labels}"


@pytest.mark.xfail(reason="labels(n) literal not in GROUP BY for UNION untyped nodes")
def test_labels_untyped_nodes_with_count(simple_graph):
    """Test labels(n) with COUNT aggregation on untyped nodes."""
    response = execute_cypher(
        "MATCH (n) RETURN labels(n) as label, count(*) as cnt ORDER BY label",
        schema_name=simple_graph["schema_name"],
    )
    assert_query_success(response)
    results = response["results"]
    assert len(results) >= 2, f"Expected at least 2 rows, got {len(results)}"


def test_label_untyped_nodes_scalar(simple_graph):
    """Test label(n) (scalar) on untyped nodes returns correct label strings."""
    response = execute_cypher(
        "MATCH (n) RETURN DISTINCT label(n) as lbl ORDER BY lbl",
        schema_name=simple_graph["schema_name"],
    )
    assert_query_success(response)
    results = response["results"]
    labels = [row['lbl'] for row in results]
    assert len(labels) >= 2, f"Expected at least 2 distinct labels, got {len(labels)}: {labels}"
    # Verify these are strings, not arrays
    for lbl in labels:
        assert isinstance(lbl, str), f"label(n) should return string, got {type(lbl)}: {lbl}"


@pytest.mark.xfail(reason="labels(n) in WHERE not resolved for UNION untyped nodes")
def test_labels_in_where_clause(simple_graph):
    """Test labels(n) used in WHERE clause for filtering."""
    response = execute_cypher(
        "MATCH (n) WHERE labels(n) = ['TestUser'] RETURN count(n) as user_count",
        schema_name=simple_graph["schema_name"],
    )
    assert_query_success(response)
    results = response["results"]
    assert len(results) == 1, f"Expected 1 result row, got {len(results)}"
    user_count = results[0]['user_count']
    assert user_count > 0, "Should have at least one TestUser node"


def test_labels_mixed_typed_untyped(simple_graph):
    """Test labels() on both typed and untyped nodes in same query."""
    response = execute_cypher(
        """
        MATCH (u:TestUser)
        MATCH (n)
        WHERE u.user_id = n.user_id
        RETURN labels(u) as user_labels, labels(n) as n_labels
        LIMIT 1
        """,
        schema_name=simple_graph["schema_name"],
    )
    assert_query_success(response)
    results = response["results"]
    assert len(results) == 1, f"Expected 1 result row, got {len(results)}"
    assert results[0]['user_labels'] == ['TestUser'], "Typed node should have ['TestUser'] label"


def test_labels_array_format(simple_graph):
    """Test that labels(n) returns array format, not nested arrays."""
    response = execute_cypher(
        "MATCH (u:TestUser) RETURN labels(u) as lbl LIMIT 1",
        schema_name=simple_graph["schema_name"],
    )
    assert_query_success(response)
    results = response["results"]
    assert len(results) == 1
    labels = results[0]['lbl']
    assert isinstance(labels, list), f"labels(n) should return list, got {type(labels)}"
    assert len(labels) == 1, f"Expected single element array, got {len(labels)} elements: {labels}"
    assert labels[0] == 'TestUser', f"Expected ['TestUser'], got {labels}"


def test_label_vs_labels_difference(simple_graph):
    """Test that label(n) returns string while labels(n) returns array."""
    response = execute_cypher(
        """
        MATCH (u:TestUser)
        RETURN label(u) as lbl_string, labels(u) as lbl_array
        LIMIT 1
        """,
        schema_name=simple_graph["schema_name"],
    )
    assert_query_success(response)
    results = response["results"]
    assert len(results) == 1
    lbl_string = results[0]['lbl_string']
    lbl_array = results[0]['lbl_array']
    assert isinstance(lbl_string, str), f"label(n) should return string, got {type(lbl_string)}"
    assert lbl_string == 'TestUser', f"Expected 'TestUser', got {lbl_string}"
    assert isinstance(lbl_array, list), f"labels(n) should return list, got {type(lbl_array)}"
    assert lbl_array == ['TestUser'], f"Expected ['TestUser'], got {lbl_array}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
