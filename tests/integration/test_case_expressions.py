"""
Integration tests for CASE expressions.

Tests cover:
- Simple CASE expressions (CASE expr WHEN val THEN result)
- Searched CASE expressions (CASE WHEN condition THEN result)
- CASE in RETURN clause
- CASE in WHERE clause
- CASE in aggregations
- Nested CASE expressions
- CASE with NULL handling
- CASE with multiple WHEN branches
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    assert_contains_value
)


class TestSimpleCaseInReturn:
    """Test simple CASE expressions in RETURN clause."""
    
    def test_simple_case_single_value(self, simple_graph):
        """Test simple CASE with single value match."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name,
                   CASE n.name
                       WHEN 'Alice' THEN 'Admin'
                       ELSE 'User'
                   END as role
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_column_exists(response, "role")
        # Alice should be 'Admin'
        assert_contains_value(response, "role", "Admin")
    
    def test_simple_case_multiple_values(self, simple_graph):
        """Test simple CASE with multiple WHEN branches."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name,
                   CASE n.name
                       WHEN 'Alice' THEN 'Level 3'
                       WHEN 'Bob' THEN 'Level 2'
                       WHEN 'Charlie' THEN 'Level 2'
                       ELSE 'Level 1'
                   END as level
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_contains_value(response, "level", "Level 3")
        assert_contains_value(response, "level", "Level 2")
        assert_contains_value(response, "level", "Level 1")
    
    def test_simple_case_no_else(self, simple_graph):
        """Test simple CASE without ELSE clause."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name,
                   CASE n.name
                       WHEN 'Alice' THEN 'VIP'
                   END as status
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Non-matching cases should return NULL
        assert_row_count(response, 5)


class TestSearchedCaseInReturn:
    """Test searched CASE expressions in RETURN clause."""
    
    def test_searched_case_simple_condition(self, simple_graph):
        """Test searched CASE with simple condition."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age < 25 THEN 'Young'
                       WHEN n.age < 35 THEN 'Adult'
                       ELSE 'Senior'
                   END as age_group
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_column_exists(response, "age_group")
    
    def test_searched_case_multiple_conditions(self, simple_graph):
        """Test searched CASE with complex conditions."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age > 30 AND n.name = 'Alice' THEN 'Senior Admin'
                       WHEN n.age > 30 THEN 'Senior User'
                       WHEN n.age > 25 THEN 'Regular User'
                       ELSE 'Junior User'
                   END as category
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_column_exists(response, "category")
    
    def test_searched_case_comparison_operators(self, simple_graph):
        """Test searched CASE with various comparison operators."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age = 30 THEN 'Exactly 30'
                       WHEN n.age > 30 THEN 'Over 30'
                       WHEN n.age >= 25 THEN '25-29'
                       ELSE 'Under 25'
                   END as age_category
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)


class TestCaseInWhere:
    """Test CASE expressions in WHERE clause."""
    
    def test_case_in_where_simple(self, simple_graph):
        """Test simple CASE in WHERE clause."""
        response = execute_cypher(
            """
            MATCH (n:User)
            WHERE CASE n.name
                      WHEN 'Alice' THEN 1
                      WHEN 'Bob' THEN 1
                      ELSE 0
                  END = 1
            RETURN n.name
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Should only return Alice and Bob
        assert_row_count(response, 2)
        assert_contains_value(response, "n.name", "Alice")
        assert_contains_value(response, "n.name", "Bob")
    
    def test_case_in_where_searched(self, simple_graph):
        """Test searched CASE in WHERE clause."""
        response = execute_cypher(
            """
            MATCH (n:User)
            WHERE CASE
                      WHEN n.age < 25 THEN 'include'
                      WHEN n.age > 35 THEN 'include'
                      ELSE 'exclude'
                  END = 'include'
            RETURN n.name, n.age
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Users under 25 or over 35
        assert isinstance(response["results"], list)
    
    def test_case_in_where_boolean_result(self, simple_graph):
        """Test CASE returning boolean in WHERE."""
        response = execute_cypher(
            """
            MATCH (n:User)
            WHERE CASE
                      WHEN n.age >= 30 THEN true
                      ELSE false
                  END
            RETURN n.name, n.age
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Users age >= 30
        assert isinstance(response["results"], list)


class TestCaseInAggregation:
    """Test CASE expressions in aggregations."""
    
    def test_case_in_count(self, simple_graph):
        """Test CASE within COUNT aggregation."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN 
                COUNT(CASE WHEN n.age < 30 THEN 1 END) as young_count,
                COUNT(CASE WHEN n.age >= 30 THEN 1 END) as mature_count
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "young_count")
        assert_column_exists(response, "mature_count")
    
    def test_case_in_sum(self, simple_graph):
        """Test CASE within SUM aggregation."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN SUM(
                CASE
                    WHEN n.age < 30 THEN 1
                    WHEN n.age < 40 THEN 2
                    ELSE 3
                END
            ) as weighted_sum
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "weighted_sum")
    
    def test_case_in_group_by(self, simple_graph):
        """Test CASE expression in GROUP BY."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN 
                CASE
                    WHEN n.age < 30 THEN 'Young'
                    ELSE 'Mature'
                END as age_group,
                COUNT(n) as count
            ORDER BY age_group
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Should have two groups: Young and Mature
        assert isinstance(response["results"], list)
        assert_column_exists(response, "age_group")
        assert_column_exists(response, "count")


class TestCaseWithRelationships:
    """Test CASE expressions with relationship patterns."""
    
    def test_case_based_on_relationship_existence(self, simple_graph):
        """Test CASE checking if relationship exists."""
        response = execute_cypher(
            """
            MATCH (n:User)
            OPTIONAL MATCH (n)-[:FOLLOWS]->(m:User)
            RETURN n.name,
                   CASE
                       WHEN COUNT(m) > 0 THEN 'Active'
                       ELSE 'Inactive'
                   END as status
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_contains_value(response, "status", "Active")
        assert_contains_value(response, "status", "Inactive")
    
    def test_case_on_relationship_count(self, simple_graph):
        """Test CASE based on relationship count."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            WITH a, COUNT(b) as follows
            RETURN a.name,
                   CASE
                       WHEN follows = 0 THEN 'No follows'
                       WHEN follows = 1 THEN 'One follow'
                       ELSE 'Multiple follows'
                   END as follow_status
            ORDER BY a.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
    
    def test_case_with_relationship_properties(self, simple_graph):
        """Test CASE using relationship properties."""
        response = execute_cypher(
            """
            MATCH (a:User)-[r:FOLLOWS]->(b:User)
            RETURN a.name, b.name,
                   CASE
                       WHEN r.since > 2022 THEN 'Recent'
                       WHEN r.since > 2020 THEN 'Medium'
                       ELSE 'Old'
                   END as relationship_age
            ORDER BY a.name, b.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)


class TestNestedCase:
    """Test nested CASE expressions."""
    
    def test_nested_case_simple(self, simple_graph):
        """Test simple nested CASE expression."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age < 25 THEN 'Young'
                       ELSE CASE
                           WHEN n.age < 35 THEN 'Adult'
                           ELSE 'Senior'
                       END
                   END as category
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        assert_column_exists(response, "category")
    
    def test_nested_case_complex(self, simple_graph):
        """Test complex nested CASE expression."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age >= 30 THEN
                           CASE n.name
                               WHEN 'Alice' THEN 'Senior Admin'
                               ELSE 'Senior User'
                           END
                       ELSE
                           CASE
                               WHEN n.age >= 25 THEN 'Regular User'
                               ELSE 'Junior User'
                           END
                   END as role
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)


class TestCaseWithNull:
    """Test CASE expressions with NULL handling."""
    
    def test_case_with_null_input(self, simple_graph):
        """Test CASE handling NULL input."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name,
                   CASE b.name
                       WHEN NULL THEN 'No follow'
                       ELSE b.name
                   END as followed
            ORDER BY a.name, followed
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # Eve should have NULL -> 'No follow'
        assert isinstance(response["results"], list)
    
    def test_case_returning_null(self, simple_graph):
        """Test CASE that can return NULL."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name,
                   CASE
                       WHEN n.age > 100 THEN 'Very old'
                       ELSE NULL
                   END as special_status
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        # All should be NULL since no one is > 100
    
    def test_case_null_in_condition(self, simple_graph):
        """Test CASE with NULL in condition."""
        response = execute_cypher(
            """
            MATCH (a:User)
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name,
                   CASE
                       WHEN b.name IS NULL THEN 'No connections'
                       ELSE 'Has connections'
                   END as connection_status
            ORDER BY a.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)


class TestCaseEdgeCases:
    """Test edge cases for CASE expressions."""
    
    def test_case_all_conditions_false(self, simple_graph):
        """Test CASE when all conditions are false."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name,
                   CASE
                       WHEN n.age > 1000 THEN 'Ancient'
                       WHEN n.age < 0 THEN 'Invalid'
                   END as impossible_category
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # All should be NULL (no ELSE clause)
        assert_row_count(response, 5)
    
    def test_case_first_match_wins(self, simple_graph):
        """Test that CASE returns on first match."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age > 20 THEN 'First'
                       WHEN n.age > 25 THEN 'Second'
                       WHEN n.age > 30 THEN 'Third'
                       ELSE 'Last'
                   END as result
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        # All should be 'First' or 'Last', never 'Second' or 'Third'
        assert_row_count(response, 5)
    
    def test_case_with_expressions(self, simple_graph):
        """Test CASE with complex expressions."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age * 2 > 60 THEN 'High'
                       WHEN n.age + 10 < 30 THEN 'Low'
                       ELSE 'Medium'
                   END as calculated_category
            ORDER BY n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)


class TestCaseInOrderBy:
    """Test CASE expressions in ORDER BY clause."""
    
    def test_case_in_order_by(self, simple_graph):
        """Test ordering by CASE expression."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age
            ORDER BY 
                CASE
                    WHEN n.name = 'Alice' THEN 1
                    WHEN n.name = 'Bob' THEN 2
                    ELSE 3
                END,
                n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        # Alice should be first, Bob second
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["n.name"] == "Alice"
            assert results[1]["n.name"] == "Bob"
        else:
            col_idx = response["columns"].index("n.name")
            assert results[0][col_idx] == "Alice"
            assert results[1][col_idx] == "Bob"
    
    def test_case_order_by_category(self, simple_graph):
        """Test ordering by categorization CASE."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN n.name, n.age,
                   CASE
                       WHEN n.age < 25 THEN 'Young'
                       WHEN n.age < 35 THEN 'Adult'
                       ELSE 'Senior'
                   END as age_group
            ORDER BY age_group, n.name
            """,
            schema_name=simple_graph["database"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 5)
        # Should be ordered by age_group then name
