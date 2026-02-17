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
from conftest import execute_cypher


def test_labels_untyped_nodes_basic(connection_with_data):
    """Test labels(n) on untyped nodes returns correct labels for all node types."""
    query = "MATCH (n) RETURN DISTINCT labels(n) as label ORDER BY label"
    
    result = execute_cypher(connection_with_data, query)
    
    # Should return both User and Post labels
    labels = [row['label'] for row in result]
    assert len(labels) == 2, f"Expected 2 distinct labels, got {len(labels)}: {labels}"
    assert ['Post'] in labels, "Should have Post label"
    assert ['User'] in labels, "Should have User label"


def test_labels_untyped_nodes_with_count(connection_with_data):
    """Test labels(n) with COUNT aggregation on untyped nodes."""
    query = "MATCH (n) RETURN labels(n) as label, count(*) as cnt ORDER BY label"
    
    result = execute_cypher(connection_with_data, query)
    
    # Should return counts for each label type
    assert len(result) == 2, f"Expected 2 rows (User and Post), got {len(result)}"
    
    # Check that both labels are present with counts
    labels_dict = {tuple(row['label']): row['cnt'] for row in result}
    assert ('Post',) in labels_dict, "Should have Post label"
    assert ('User',) in labels_dict, "Should have User label"
    assert labels_dict[('User',)] > 0, "Should have at least one User"
    assert labels_dict[('Post',)] > 0, "Should have at least one Post"


def test_label_untyped_nodes_scalar(connection_with_data):
    """Test label(n) (scalar) on untyped nodes returns correct label strings."""
    query = "MATCH (n) RETURN DISTINCT label(n) as lbl ORDER BY lbl"
    
    result = execute_cypher(connection_with_data, query)
    
    # Should return both User and Post as strings (not arrays)
    labels = [row['lbl'] for row in result]
    assert len(labels) == 2, f"Expected 2 distinct labels, got {len(labels)}: {labels}"
    assert 'Post' in labels, "Should have 'Post' string"
    assert 'User' in labels, "Should have 'User' string"
    
    # Verify these are strings, not arrays
    for lbl in labels:
        assert isinstance(lbl, str), f"label(n) should return string, got {type(lbl)}: {lbl}"


def test_labels_in_where_clause(connection_with_data):
    """Test labels(n) used in WHERE clause for filtering."""
    query = "MATCH (n) WHERE labels(n) = ['User'] RETURN count(n) as user_count"
    
    result = execute_cypher(connection_with_data, query)
    
    assert len(result) == 1, f"Expected 1 result row, got {len(result)}"
    user_count = result[0]['user_count']
    assert user_count > 0, "Should have at least one User node"


def test_labels_mixed_typed_untyped(connection_with_data):
    """Test labels() on both typed and untyped nodes in same query."""
    query = """
        MATCH (u:User)
        MATCH (n)
        WHERE u.user_id = n.user_id
        RETURN labels(u) as user_labels, labels(n) as n_labels
        LIMIT 1
    """
    
    result = execute_cypher(connection_with_data, query)
    
    assert len(result) == 1, f"Expected 1 result row, got {len(result)}"
    assert result[0]['user_labels'] == ['User'], "Typed node should have ['User'] label"
    assert result[0]['n_labels'] == ['User'], "Untyped node matching user should have ['User'] label"


def test_labels_array_format(connection_with_data):
    """Test that labels(n) returns array format, not nested arrays."""
    query = "MATCH (u:User) RETURN labels(u) as lbl LIMIT 1"
    
    result = execute_cypher(connection_with_data, query)
    
    assert len(result) == 1
    labels = result[0]['lbl']
    
    # Should be a single array: ['User']
    assert isinstance(labels, list), f"labels(n) should return list, got {type(labels)}"
    assert len(labels) == 1, f"Expected single element array, got {len(labels)} elements: {labels}"
    assert labels[0] == 'User', f"Expected ['User'], got {labels}"
    assert isinstance(labels[0], str), f"Array element should be string, got {type(labels[0])}"


def test_label_vs_labels_difference(connection_with_data):
    """Test that label(n) returns string while labels(n) returns array."""
    query = """
        MATCH (u:User)
        RETURN label(u) as lbl_string, labels(u) as lbl_array
        LIMIT 1
    """
    
    result = execute_cypher(connection_with_data, query)
    
    assert len(result) == 1
    lbl_string = result[0]['lbl_string']
    lbl_array = result[0]['lbl_array']
    
    # label(n) should return string
    assert isinstance(lbl_string, str), f"label(n) should return string, got {type(lbl_string)}"
    assert lbl_string == 'User', f"Expected 'User', got {lbl_string}"
    
    # labels(n) should return array
    assert isinstance(lbl_array, list), f"labels(n) should return list, got {type(lbl_array)}"
    assert lbl_array == ['User'], f"Expected ['User'], got {lbl_array}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
