#!/usr/bin/env python3
"""
Comprehensive WITH clause property mapping tests.

Tests that property mappings are correctly applied in WITH clause expressions.
For example: MATCH (u:User) WITH u.name AS userName should map 'name' -> 'full_name'
based on the schema configuration.
"""
import pytest
import requests

BASE_URL = "http://localhost:8080"


def check_query(query: str, expected_patterns: list, forbidden_patterns: list = None) -> tuple[bool, str]:
    """
    Test a query and check for expected patterns in generated SQL.
    
    Returns: (success: bool, message: str)
    """
    forbidden_patterns = forbidden_patterns or []
    
    try:
        resp = requests.post(
            f"{BASE_URL}/query",
            json={"query": query, "sql_only": True},
            timeout=10
        )
        result = resp.json()
        sql = result.get("generated_sql", result.get("body", ""))
        
        missing = [p for p in expected_patterns if p not in sql]
        found_forbidden = [p for p in forbidden_patterns if p in sql]
        
        if missing or found_forbidden:
            msg = f"Query: {query}\n"
            if missing:
                msg += f"Missing patterns: {missing}\n"
            if found_forbidden:
                msg += f"Forbidden patterns found: {found_forbidden}\n"
            msg += f"Generated SQL: {sql[:500]}"
            return False, msg
        
        return True, ""
    except Exception as e:
        return False, f"Error: {e}"


class TestBasicPropertyMapping:
    """Basic property mapping tests."""
    
    def test_simple_property_name(self, verify_clickgraph_running):
        """Test that u.name maps to full_name."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.name AS userName RETURN userName LIMIT 1",
            ["full_name"],
            ["u.name AS"]
        )
        assert success, msg
    
    def test_simple_property_email(self, verify_clickgraph_running):
        """Test that u.email maps to email_address."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.email AS email RETURN email LIMIT 1",
            ["email_address"],
            ["u.email AS"]
        )
        assert success, msg
    
    def test_identity_mapping(self, verify_clickgraph_running):
        """Test that user_id maps to itself (identity mapping)."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.user_id AS id RETURN id LIMIT 1",
            ["user_id AS"],
            []
        )
        assert success, msg


class TestWithNodePassthrough:
    """Tests for WITH clauses that include both nodes and scalar expressions."""
    
    def test_node_with_property(self, verify_clickgraph_running):
        """Test: WITH u, u.name AS userName."""
        success, msg = check_query(
            "MATCH (u:User) WITH u, u.name AS userName RETURN userName LIMIT 1",
            ["full_name AS \"userName\""],
            []
        )
        assert success, msg
    
    def test_node_with_multiple_properties(self, verify_clickgraph_running):
        """Test: WITH u, u.name AS n, u.email AS e."""
        success, msg = check_query(
            "MATCH (u:User) WITH u, u.name AS n, u.email AS e RETURN n, e LIMIT 1",
            ["full_name AS \"n\"", "email_address AS \"e\""],
            []
        )
        assert success, msg


class TestNestedFunctions:
    """Tests for nested function expressions in WITH clause."""
    
    def test_substring(self, verify_clickgraph_running):
        """Test: substring(u.name, 1, 5)."""
        success, msg = check_query(
            "MATCH (u:User) WITH substring(u.name, 1, 5) AS prefix RETURN prefix LIMIT 1",
            ["substring(u.full_name"],
            ["substring(u.name"]
        )
        assert success, msg
    
    def test_tolower(self, verify_clickgraph_running):
        """Test: toLower(u.name)."""
        success, msg = check_query(
            "MATCH (u:User) WITH toLower(u.name) AS lowerName RETURN lowerName LIMIT 1",
            ["lower(u.full_name"],
            ["lower(u.name"]
        )
        assert success, msg
    
    def test_toupper(self, verify_clickgraph_running):
        """Test: toUpper(u.email)."""
        success, msg = check_query(
            "MATCH (u:User) WITH toUpper(u.email) AS upperEmail RETURN upperEmail LIMIT 1",
            ["upper(u.email_address"],
            ["upper(u.email)"]
        )
        assert success, msg


class TestArithmeticExpressions:
    """Tests for arithmetic expressions in WITH clause."""
    
    def test_addition(self, verify_clickgraph_running):
        """Test: u.user_id + 100."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.user_id + 100 AS offsetId RETURN offsetId LIMIT 1",
            ["user_id + 100", "AS \"offsetId\""],
            []
        )
        assert success, msg
    
    def test_multiplication(self, verify_clickgraph_running):
        """Test: u.user_id * 2."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.user_id * 2 AS doubled RETURN doubled LIMIT 1",
            ["user_id * 2", "AS \"doubled\""],
            []
        )
        assert success, msg


class TestMultipleProperties:
    """Tests for multiple property expressions."""
    
    def test_multiple_scalar_expressions(self, verify_clickgraph_running):
        """Test: u.name AS n, u.email AS e, u.user_id AS id."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.name AS n, u.email AS e, u.user_id AS id RETURN n, e, id LIMIT 1",
            ["full_name AS \"n\"", "email_address AS \"e\"", "user_id AS \"id\""],
            []
        )
        assert success, msg


class TestWithAggregation:
    """Tests for WITH clause with aggregation."""
    
    def test_count_with_property(self, verify_clickgraph_running):
        """Test: count(u.name)."""
        success, msg = check_query(
            "MATCH (u:User) WITH count(u.name) AS nameCount RETURN nameCount",
            ["count("],
            []
        )
        assert success, msg
    
    def test_group_by_property(self, verify_clickgraph_running):
        """Test: GROUP BY with property."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.country AS country, count(*) AS cnt RETURN country, cnt LIMIT 5",
            ["country"],
            []
        )
        assert success, msg


class TestWithModifiers:
    """Tests for WITH clause with modifiers (DISTINCT, ORDER BY, etc.)."""
    
    def test_with_distinct(self, verify_clickgraph_running):
        """Test: WITH DISTINCT u.name."""
        success, msg = check_query(
            "MATCH (u:User) WITH DISTINCT u.name AS userName RETURN userName LIMIT 5",
            ["DISTINCT", "full_name"],
            []
        )
        assert success, msg
    
    def test_with_order_by(self, verify_clickgraph_running):
        """Test: WITH u.name ORDER BY."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.name AS userName ORDER BY userName RETURN userName LIMIT 5",
            ["full_name"],
            []
        )
        assert success, msg


class TestRelationshipPatterns:
    """Tests for relationship patterns with property mapping."""
    
    def test_start_node_property(self, verify_clickgraph_running):
        """Test: a.name from start node."""
        success, msg = check_query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a.name AS followerName RETURN followerName LIMIT 3",
            ["full_name"],
            []
        )
        assert success, msg
    
    def test_end_node_property(self, verify_clickgraph_running):
        """Test: b.name from end node."""
        success, msg = check_query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH b.name AS followedName RETURN followedName LIMIT 3",
            ["full_name"],
            []
        )
        assert success, msg
    
    def test_both_nodes_properties(self, verify_clickgraph_running):
        """Test: a.name, b.name from both nodes."""
        success, msg = check_query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a.name AS aName, b.name AS bName RETURN aName, bName LIMIT 3",
            ["full_name AS \"aName\"", "full_name AS \"bName\""],
            []
        )
        assert success, msg


class TestComplexExpressions:
    """Tests for complex expressions in WITH clause."""
    
    def test_string_concatenation(self, verify_clickgraph_running):
        """Test: u.name + '@' + u.email."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.name + '@' + u.email AS combo RETURN combo LIMIT 1",
            ["full_name", "email_address"],
            []
        )
        assert success, msg
    
    def test_case_expression(self, verify_clickgraph_running):
        """Test: CASE WHEN u.is_active THEN u.name ELSE 'inactive' END."""
        success, msg = check_query(
            "MATCH (u:User) WITH CASE WHEN u.is_active THEN u.name ELSE 'inactive' END AS status RETURN status LIMIT 1",
            ["full_name", "is_active"],
            []
        )
        assert success, msg


class TestEdgeCases:
    """Edge case tests for WITH clause property mapping."""
    
    def test_coalesce_function(self, verify_clickgraph_running):
        """Test: COALESCE(u.name, 'default')."""
        success, msg = check_query(
            "MATCH (u:User) WITH coalesce(u.name, 'default') AS userName RETURN userName LIMIT 1",
            ["full_name"],
            []
        )
        assert success, msg
    
    def test_nested_case(self, verify_clickgraph_running):
        """Test: Nested CASE expression."""
        success, msg = check_query(
            "MATCH (u:User) WITH CASE WHEN u.is_active THEN CASE WHEN u.country = 'USA' THEN u.name ELSE 'non-US' END ELSE 'inactive' END AS status RETURN status LIMIT 1",
            ["full_name", "is_active", "country"],
            []
        )
        assert success, msg
    
    def test_list_with_properties(self, verify_clickgraph_running):
        """Test: [u.name, u.email] list expression."""
        success, msg = check_query(
            "MATCH (u:User) WITH [u.name, u.email] AS info RETURN info LIMIT 1",
            ["full_name", "email_address"],
            []
        )
        assert success, msg
    
    def test_multiple_refs_same_node(self, verify_clickgraph_running):
        """Test: Multiple references to same property."""
        success, msg = check_query(
            "MATCH (u:User) WITH u.name AS n1, u.name AS n2, toLower(u.name) AS lower_n RETURN n1, n2, lower_n LIMIT 1",
            ["full_name AS \"n1\"", "full_name AS \"n2\"", "lower(u.full_name"],
            []
        )
        assert success, msg
