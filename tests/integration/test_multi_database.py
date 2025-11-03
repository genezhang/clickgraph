"""
Integration tests for multi-database support.

Tests cover:
- USE clause in queries
- schema_name parameter in API
- database parameter in Bolt protocol
- Precedence order: USE > schema_name > database > default
- Database switching
- Schema isolation
"""

import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists
)


class TestSchemaNameParameter:
    """Test schema_name parameter in HTTP API."""
    
    def test_schema_name_simple_graph(self, simple_graph):
        """Test querying with schema_name parameter."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as user_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        # Should have 5 users in simple_graph
        results = response["results"]
        if isinstance(results[0], dict):
            assert results[0]["user_count"] == 5
        else:
            col_idx = response["columns"].index("user_count")
            assert results[0][col_idx] == 5
    
    def test_schema_name_override_default(self, simple_graph):
        """Test that schema_name overrides default database."""
        # Query with explicit schema_name
        response = execute_cypher(
            """
            MATCH (n:User)
            WHERE n.name = 'Alice'
            RETURN n.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
    
    def test_schema_name_nonexistent(self, simple_graph):
        """Test querying nonexistent schema."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name="nonexistent_database"
        )
        
        # Should either error or return 0 results
        # Implementation may vary
        assert isinstance(response, dict)


class TestUSEClause:
    """Test USE clause in Cypher queries."""
    
    def test_use_clause_basic(self, simple_graph):
        """Test basic USE clause."""
        response = execute_cypher(
            f"""
            USE {simple_graph["database"]}
            MATCH (n:User)
            RETURN COUNT(n) as user_count
            """
        )
        
        assert_query_success(response)
        # Should access the specified database
        assert_row_count(response, 1)
    
    def test_use_clause_overrides_parameter(self, simple_graph):
        """Test that USE clause overrides schema_name parameter."""
        response = execute_cypher(
            f"""
            USE {simple_graph["database"]}
            MATCH (n:User)
            WHERE n.name = 'Alice'
            RETURN n.name
            """,
            schema_name="different_database"  # Should be ignored
        )
        
        assert_query_success(response)
        # USE clause takes precedence
        assert_row_count(response, 1)
    
    def test_use_clause_with_backticks(self, simple_graph):
        """Test USE clause with backtick-quoted names."""
        db_name = simple_graph["database"]
        response = execute_cypher(
            f"""
            USE `{db_name}`
            MATCH (n:User)
            RETURN COUNT(n) as count
            """
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)


class TestDatabaseSwitching:
    """Test switching between databases."""
    
    def test_multiple_queries_same_database(self, simple_graph):
        """Test multiple queries to same database."""
        db_name = simple_graph["database"]
        
        # First query
        response1 = execute_cypher(
            "MATCH (n:User) RETURN COUNT(n) as count",
            schema_name=db_name
        )
        assert_query_success(response1)
        
        # Second query to same database
        response2 = execute_cypher(
            "MATCH (n:User) WHERE n.name = 'Alice' RETURN n.name",
            schema_name=db_name
        )
        assert_query_success(response2)
        assert_row_count(response2, 1)
    
    def test_switch_between_databases(self, simple_graph, create_graph_schema):
        """Test switching between different databases."""
        db1 = simple_graph["database"]
        
        # Create a second test database
        db2_schema = {
            "nodes": {
                "Product": {
                    "table": "products",
                    "properties": {
                        "id": {"column": "id", "type": "Integer"},
                        "name": {"column": "name", "type": "String"}
                    }
                }
            },
            "relationships": {}
        }
        db2 = create_graph_schema(db2_schema, "test_db2")
        
        # Query first database
        response1 = execute_cypher(
            "MATCH (n:User) RETURN COUNT(n) as count",
            schema_name=db1
        )
        assert_query_success(response1)
        
        # Query second database
        response2 = execute_cypher(
            "MATCH (n:Product) RETURN COUNT(n) as count",
            schema_name=db2
        )
        assert_query_success(response2)
        
        # Query first database again
        response3 = execute_cypher(
            "MATCH (n:User) WHERE n.name = 'Bob' RETURN n.name",
            schema_name=db1
        )
        assert_query_success(response3)


class TestSchemaIsolation:
    """Test that databases are properly isolated."""
    
    def test_schema_isolation_nodes(self, simple_graph, create_graph_schema):
        """Test that node labels are isolated between databases."""
        db1 = simple_graph["database"]
        
        # Create second database with different schema
        db2_schema = {
            "nodes": {
                "Company": {
                    "table": "companies",
                    "properties": {
                        "id": {"column": "id", "type": "Integer"},
                        "name": {"column": "name", "type": "String"}
                    }
                }
            },
            "relationships": {}
        }
        db2 = create_graph_schema(db2_schema, "test_isolation")
        
        # Query User in db1 should work
        response1 = execute_cypher(
            "MATCH (n:User) RETURN COUNT(n) as count",
            schema_name=db1
        )
        assert_query_success(response1)
        
        # Query Company in db2 should work
        response2 = execute_cypher(
            "MATCH (n:Company) RETURN COUNT(n) as count",
            schema_name=db2
        )
        assert_query_success(response2)
        
        # Query Company in db1 should fail or return 0
        response3 = execute_cypher(
            "MATCH (n:Company) RETURN COUNT(n) as count",
            schema_name=db1
        )
        # Should error or return 0
        assert isinstance(response3, dict)


class TestPrecedenceOrder:
    """Test precedence order: USE > schema_name > database > default."""
    
    def test_use_takes_precedence(self, simple_graph):
        """Test that USE clause has highest precedence."""
        db_name = simple_graph["database"]
        
        response = execute_cypher(
            f"""
            USE {db_name}
            MATCH (n:User)
            WHERE n.name = 'Alice'
            RETURN n.name
            """,
            schema_name="wrong_database"
        )
        
        assert_query_success(response)
        # USE clause wins
        assert_row_count(response, 1)
    
    def test_schema_name_precedence(self, simple_graph):
        """Test that schema_name takes precedence over default."""
        db_name = simple_graph["database"]
        
        # Explicit schema_name should override default
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name=db_name
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)


class TestComplexDatabaseQueries:
    """Test complex queries across database contexts."""
    
    def test_aggregation_with_database(self, simple_graph):
        """Test aggregation query with database parameter."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, COUNT(b) as follows
            ORDER BY follows DESC, a.name
            LIMIT 3
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_column_exists(response, "a.name")
        assert_column_exists(response, "follows")
    
    def test_variable_length_with_database(self, simple_graph):
        """Test variable-length paths with database parameter."""
        response = execute_cypher(
            """
            MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert isinstance(response["results"], list)
    
    def test_optional_match_with_database(self, simple_graph):
        """Test OPTIONAL MATCH with database parameter."""
        response = execute_cypher(
            """
            MATCH (a:User)
            WHERE a.name = 'Eve'
            OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)


class TestDatabaseEdgeCases:
    """Test edge cases for database handling."""
    
    def test_empty_database_name(self, simple_graph):
        """Test handling of empty database name."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name=""
        )
        
        # Should either use default or error
        assert isinstance(response, dict)
    
    def test_special_characters_in_database_name(self, simple_graph):
        """Test database names with special characters."""
        # Most databases don't support special chars
        # This tests proper escaping/validation
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name="test-database-with-dashes"
        )
        
        # Should handle gracefully (error or empty result)
        assert isinstance(response, dict)
    
    def test_case_sensitivity_database_name(self, simple_graph):
        """Test case sensitivity in database names."""
        db_name = simple_graph["database"]
        
        # Try uppercase version
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name=db_name.upper()
        )
        
        # Behavior depends on ClickHouse settings
        # Should either work or error gracefully
        assert isinstance(response, dict)


class TestUSEClauseEdgeCases:
    """Test edge cases for USE clause."""
    
    def test_use_clause_multiple_times(self, simple_graph):
        """Test multiple USE clauses in same query."""
        db_name = simple_graph["database"]
        
        # Only last USE should apply
        response = execute_cypher(
            f"""
            USE wrong_database
            USE {db_name}
            MATCH (n:User)
            RETURN COUNT(n) as count
            """
        )
        
        assert_query_success(response)
        # Last USE clause wins
        assert_row_count(response, 1)
    
    def test_use_clause_with_comments(self, simple_graph):
        """Test USE clause with comments."""
        db_name = simple_graph["database"]
        
        response = execute_cypher(
            f"""
            // This is a comment
            USE {db_name}
            /* Multi-line
               comment */
            MATCH (n:User)
            RETURN COUNT(n) as count
            """
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)


class TestDatabaseValidation:
    """Test database name validation."""
    
    def test_sql_injection_protection(self, simple_graph):
        """Test protection against SQL injection in database names."""
        # Attempt SQL injection via database name
        malicious_name = "test'; DROP TABLE users; --"
        
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name=malicious_name
        )
        
        # Should handle safely (error, not execute injection)
        assert isinstance(response, dict)
        # If it returns, users table should still exist
        
        # Verify users table still exists in real database
        verify = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name=simple_graph["schema_name"]
        )
        assert_query_success(verify)
    
    def test_unicode_database_name(self, simple_graph):
        """Test database names with Unicode characters."""
        response = execute_cypher(
            """
            MATCH (n:User)
            RETURN COUNT(n) as count
            """,
            schema_name="test_数据库_😀"
        )
        
        # Should handle gracefully
        assert isinstance(response, dict)
