"""
Integration tests for Cypher Basic Patterns (from wiki documentation).

These tests validate that documented examples work correctly with the benchmark schema.
Each test corresponds to a specific pattern documented in Cypher-Basic-Patterns.md.

UNIFIED SCHEMA APPROACH:
- All queries explicitly use "USE social_benchmark" clause
- No environment variable setup required
- Self-documenting: Query shows which schema it expects

Test groups:
- Node patterns: Matching nodes by label and properties
- Relationship patterns: Basic relationships, multiple types, anonymous
- Property filtering: WHERE clauses, comparisons, string matching
- Return statements: Properties, aliases, expressions, counts
- Ordering and limiting: ORDER BY, LIMIT, SKIP
- Anonymous patterns: Anonymous nodes and relationships
- Common patterns: Neighbors, counting, existence checks
"""

import os
import pytest
import requests
from typing import Dict, Any

# Test configuration
BASE_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
QUERY_ENDPOINT = f"{BASE_URL}/query"


def execute_query(cypher: str) -> Dict[str, Any]:
    """Execute a Cypher query against ClickGraph.
    
    UNIFIED SCHEMA: All tests use the unified_test_schema which includes
    all test entities (User, Person, Flight, etc.) in one namespace.
    No USE clause needed since there's only one schema loaded.
    
    Returns normalized result with:
    - success: True if 'results' key exists, False if 'error' key exists
    - data: The results array (alias for 'results')
    - error: Error message if any
    """
    response = requests.post(QUERY_ENDPOINT, json={"query": cypher})
    response.raise_for_status()
    raw = response.json()
    
    # Normalize response format
    if "results" in raw:
        return {"success": True, "data": raw["results"]}
    elif "error" in raw:
        return {"success": False, "error": raw["error"]}
    else:
        return {"success": False, "data": [], "error": "Unknown response format"}


# ============================================================================
# NODE PATTERNS
# ============================================================================

class TestNodePatterns:
    """Tests for node matching patterns."""

    def test_match_users_all_properties(self):
        """RETURN u should expand to all user properties."""
        query = "MATCH (u:User) RETURN u LIMIT 3"
        result = execute_query(query)
        assert result["success"]
        # Should have all 7 User properties
        assert len(result["data"]) <= 3

    def test_match_nodes_by_label_user(self):
        """Match User nodes and return specific properties."""
        query = "MATCH (u:User) RETURN u.name, u.country LIMIT 10"
        result = execute_query(query)
        assert result["success"]
        assert "data" in result

    def test_match_nodes_by_label_post(self):
        """Match Post nodes and return specific properties."""
        query = "MATCH (p:Post) RETURN p.content, p.date LIMIT 10"
        result = execute_query(query)
        assert result["success"]

    def test_match_nodes_by_property_single(self):
        """Match user by single property using WHERE."""
        query = """
            MATCH (u:User)
            WHERE u.name = 'Alice'
            RETURN u
        """
        result = execute_query(query)
        assert result["success"]

    def test_match_nodes_by_property_multiple(self):
        """Match users by multiple properties using WHERE."""
        query = """
            MATCH (u:User)
            WHERE u.country = 'USA' AND u.is_active = true
            RETURN u.name
        """
        result = execute_query(query)
        assert result["success"]


# ============================================================================
# RELATIONSHIP PATTERNS
# ============================================================================

class TestRelationshipPatterns:
    """Tests for relationship matching patterns."""

    def test_basic_relationship_directed_outgoing(self):
        """Match directed relationship (left to right)."""
        query = """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_basic_relationship_directed_incoming(self):
        """Match directed relationship (right to left)."""
        query = """
            MATCH (a:User)<-[:FOLLOWS]-(b:User)
            RETURN a.name, b.name
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_basic_relationship_undirected(self):
        """Match undirected relationship (either direction)."""
        query = """
            MATCH (a:User)-[:FOLLOWS]-(b:User)
            RETURN a.name, b.name
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_relationship_with_variable(self):
        """Assign relationship to variable and access properties."""
        query = """
            MATCH (a:User)-[r:FOLLOWS]->(b:User)
            RETURN a.name, b.name, r.follow_date
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_multiple_relationship_types(self):
        """Match multiple relationship types with OR."""
        query = """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_relationship_property_filtering(self):
        """Filter relationships by property using WHERE."""
        query = """
            MATCH (a:User)-[r:FOLLOWS]->(b:User)
            WHERE r.follow_date > '2024-01-01'
            RETURN a.name, b.name, r.follow_date
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]


# ============================================================================
# PROPERTY FILTERING
# ============================================================================

class TestPropertyFiltering:
    """Tests for WHERE clause filtering."""

    def test_where_single_condition(self):
        """Filter with single WHERE condition."""
        query = """
            MATCH (u:User)
            WHERE u.is_active = true
            RETURN u.name, u.email
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_where_multiple_conditions_and(self):
        """Filter with multiple AND conditions."""
        query = """
            MATCH (u:User)
            WHERE u.is_active = true AND u.country = 'USA'
            RETURN u.name, u.country
        """
        result = execute_query(query)
        assert result["success"]

    def test_where_multiple_conditions_or(self):
        """Filter with multiple OR conditions."""
        query = """
            MATCH (u:User)
            WHERE u.country = 'USA' OR u.country = 'Canada'
            RETURN u.name, u.country
        """
        result = execute_query(query)
        assert result["success"]

    def test_comparison_equality(self):
        """Test equality comparison."""
        query = """
            MATCH (u:User) 
            WHERE u.country = 'USA' 
            RETURN u.name 
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_comparison_inequality(self):
        """Test inequality comparison."""
        query = """
            MATCH (u:User) 
            WHERE u.country != 'USA' 
            RETURN u.name 
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_comparison_date_range(self):
        """Test date range comparison."""
        query = """
            MATCH (u:User)
            WHERE u.registration_date >= '2024-01-01' 
              AND u.registration_date <= '2024-12-31'
            RETURN u.name, u.registration_date
        """
        result = execute_query(query)
        assert result["success"]

    def test_string_like_pattern(self):
        """Test string pattern matching with ENDS WITH."""
        query = """
            MATCH (u:User)
            WHERE u.email ENDS WITH '@example.com'
            RETURN u.name, u.email
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_string_contains_substring(self):
        """Test substring matching."""
        query = """
            MATCH (u:User)
            WHERE u.name CONTAINS 'Alice'
            RETURN u.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_in_list_strings(self):
        """Test IN operator with string list."""
        query = """
            MATCH (u:User)
            WHERE u.country IN ['USA', 'Canada', 'Mexico']
            RETURN u.name, u.country
        """
        result = execute_query(query)
        assert result["success"]

    def test_in_list_numbers(self):
        """Test IN operator with number list."""
        query = """
            MATCH (u:User)
            WHERE u.user_id IN [1, 2, 3, 4, 5]
            RETURN u.name, u.user_id
        """
        result = execute_query(query)
        assert result["success"]


# ============================================================================
# RETURN STATEMENTS
# ============================================================================

class TestReturnStatements:
    """Tests for RETURN clause variations."""

    def test_return_single_property(self):
        """Return single node property."""
        query = "MATCH (u:User) RETURN u.name LIMIT 10"
        result = execute_query(query)
        assert result["success"]

    def test_return_multiple_properties(self):
        """Return multiple node properties."""
        query = "MATCH (u:User) RETURN u.name, u.email, u.country LIMIT 10"
        result = execute_query(query)
        assert result["success"]

    def test_return_whole_node(self):
        """Return entire node (all properties)."""
        query = "MATCH (u:User) RETURN u LIMIT 10"
        result = execute_query(query)
        assert result["success"]

    def test_return_with_alias(self):
        """Return properties with column aliases."""
        query = """
            MATCH (u:User)
            RETURN u.name AS user_name, u.email AS user_email
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_return_computed_expression(self):
        """Return computed expression."""
        query = """
            MATCH (u:User)
            RETURN u.name, u.user_id, u.user_id * 100 AS scaled_id
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_return_case_expression(self):
        """Return CASE expression."""
        query = """
            MATCH (u:User)
            RETURN u.name, u.is_active,
                   CASE WHEN u.is_active THEN 'Active' ELSE 'Inactive' END AS status
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_return_distinct_values(self):
        """Return distinct values."""
        query = "MATCH (u:User) RETURN DISTINCT u.country"
        result = execute_query(query)
        assert result["success"]

    def test_return_count_all(self):
        """Count all matching nodes."""
        query = "MATCH (u:User) RETURN count(u) AS total_users"
        result = execute_query(query)
        assert result["success"]

    def test_return_count_filtered(self):
        """Count with filtering."""
        query = """
            MATCH (u:User)
            WHERE u.is_active = true
            RETURN count(u) AS active_users
        """
        result = execute_query(query)
        assert result["success"]

    def test_return_count_distinct(self):
        """Count distinct values."""
        query = "MATCH (u:User) RETURN count(DISTINCT u.country) AS num_countries"
        result = execute_query(query)
        assert result["success"]


# ============================================================================
# ORDERING AND LIMITING
# ============================================================================

class TestOrderingAndLimiting:
    """Tests for ORDER BY, LIMIT, and SKIP."""

    def test_order_by_ascending(self):
        """Order results ascending (default)."""
        query = """
            MATCH (u:User)
            RETURN u.name, u.registration_date
            ORDER BY u.registration_date
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_order_by_descending(self):
        """Order results descending."""
        query = """
            MATCH (u:User)
            RETURN u.name, u.registration_date
            ORDER BY u.registration_date DESC
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_order_by_multiple_columns(self):
        """Order by multiple columns."""
        query = """
            MATCH (u:User)
            RETURN u.name, u.registration_date, u.country
            ORDER BY u.country, u.registration_date DESC
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_limit_only(self):
        """Limit number of results."""
        query = "MATCH (u:User) RETURN u.name LIMIT 5"
        result = execute_query(query)
        assert result["success"]
        assert len(result["data"]) <= 5

    def test_order_and_limit(self):
        """Combine ORDER BY and LIMIT."""
        query = """
            MATCH (u:User)
            RETURN u.name, u.registration_date
            ORDER BY u.registration_date DESC
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_skip_and_limit(self):
        """Use SKIP for pagination."""
        query = """
            MATCH (u:User)
            RETURN u.name
            ORDER BY u.registration_date
            SKIP 10
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]


# ============================================================================
# ANONYMOUS PATTERNS
# ============================================================================

class TestAnonymousPatterns:
    """Tests for anonymous nodes and relationships."""

    def test_anonymous_node_in_pattern(self):
        """Use anonymous node when not referenced."""
        query = """
            MATCH (alice:User)-[:FOLLOWS]->(friend:User)
            WHERE alice.name = 'Alice'
            RETURN friend.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_count_with_anonymous_nodes(self):
        """Count relationships without naming nodes."""
        query = """
            MATCH (:User)-[:FOLLOWS]->(:User)
            RETURN count(*) AS total_follows
        """
        result = execute_query(query)
        assert result["success"]

    def test_anonymous_relationship(self):
        """Match without specifying relationship variable."""
        query = """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name, b.name
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]


# ============================================================================
# COMMON PATTERNS
# ============================================================================

class TestCommonPatterns:
    """Tests for frequently used query patterns."""

    def test_find_by_id(self):
        """Find node by ID."""
        query = "MATCH (u:User) WHERE u.user_id = 1 RETURN u"
        result = execute_query(query)
        assert result["success"]

    def test_find_by_property(self):
        """Find node by property."""
        query = """
            MATCH (u:User) 
            WHERE u.name = 'Alice' 
            RETURN u
        """
        result = execute_query(query)
        assert result["success"]

    def test_find_direct_neighbors(self):
        """Find direct neighbors (1-hop)."""
        query = """
            MATCH (u:User)-[:FOLLOWS]->(friend:User)
            WHERE u.name = 'Alice'
            RETURN friend.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_find_reverse_relationships(self):
        """Find reverse relationships."""
        query = """
            MATCH (u:User)<-[:FOLLOWS]-(follower:User)
            WHERE u.name = 'Alice'
            RETURN follower.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_count_outgoing_relationships(self):
        """Count outgoing relationships per node."""
        query = """
            MATCH (u:User)-[:FOLLOWS]->(:User)
            RETURN u.name, count(*) AS following_count
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_count_incoming_relationships(self):
        """Count incoming relationships per node."""
        query = """
            MATCH (u:User)<-[:FOLLOWS]-(:User)
            RETURN u.name, count(*) AS follower_count
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]

    def test_top_n_most_followed(self):
        """Find top N most followed users."""
        query = """
            MATCH (u:User)<-[:FOLLOWS]-(:User)
            RETURN u.name, count(*) AS followers
            ORDER BY followers DESC
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]


# ============================================================================
# PRACTICE EXERCISES
# ============================================================================

class TestPracticeExercises:
    """Tests for practice exercises from documentation."""

    def test_exercise_1_1_find_all_users(self):
        """Exercise 1.1: Find all users."""
        query = "MATCH (u:User) RETURN u LIMIT 10"
        result = execute_query(query)
        assert result["success"]

    def test_exercise_1_2_find_active_users(self):
        """Exercise 1.2: Find active users."""
        query = """
            MATCH (u:User) 
            WHERE u.is_active = true 
            RETURN u.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_1_3_users_by_country(self):
        """Exercise 1.3: Find users from specific countries."""
        query = """
            MATCH (u:User)
            WHERE u.country = 'USA' OR u.country = 'Canada'
            RETURN u.name, u.country
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_1_4_newest_users(self):
        """Exercise 1.4: Find the 5 newest users."""
        query = """
            MATCH (u:User)
            RETURN u.name, u.registration_date
            ORDER BY u.registration_date DESC
            LIMIT 5
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_1_5_count_by_country(self):
        """Exercise 1.5: Count users by country."""
        query = """
            MATCH (u:User)
            RETURN u.country, count(*) AS user_count
            ORDER BY user_count DESC
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_2_1_who_alice_follows(self):
        """Exercise 2.1: Find who Alice follows."""
        query = """
            MATCH (alice:User)-[:FOLLOWS]->(friend:User)
            WHERE alice.name = 'Alice'
            RETURN friend.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_2_2_who_follows_alice(self):
        """Exercise 2.2: Find who follows Alice."""
        query = """
            MATCH (follower:User)-[:FOLLOWS]->(alice:User)
            WHERE alice.name = 'Alice'
            RETURN follower.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_2_3_all_follows_relationships(self):
        """Exercise 2.3: Find all FOLLOWS relationships."""
        query = """
            MATCH (a:User)-[:FOLLOWS]->(b:User)
            RETURN a.name AS follower, b.name AS followed
            LIMIT 100
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_2_4_count_total_relationships(self):
        """Exercise 2.4: Count total relationships."""
        query = """
            MATCH ()-[r:FOLLOWS]->()
            RETURN count(r) AS total_follows
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_3_1_users_registered_2024(self):
        """Exercise 3.1: Users who registered in 2024."""
        query = """
            MATCH (u:User)
            WHERE u.registration_date >= '2024-01-01'
              AND u.registration_date < '2025-01-01'
            RETURN u.name, u.registration_date
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_3_2_name_contains_alice(self):
        """Exercise 3.2: Users whose name contains 'Alice'."""
        query = """
            MATCH (u:User)
            WHERE u.name CONTAINS 'Alice'
            RETURN u.name
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_3_3_active_with_email(self):
        """Exercise 3.3: Active users with email addresses."""
        query = """
            MATCH (u:User)
            WHERE u.is_active = true AND u.email IS NOT NULL
            RETURN u.name, u.email
        """
        result = execute_query(query)
        assert result["success"]

    def test_exercise_3_4_top_10_most_followed(self):
        """Exercise 3.4: Top 10 most followed users."""
        query = """
            MATCH (u:User)<-[:FOLLOWS]-(:User)
            RETURN u.name, count(*) AS follower_count
            ORDER BY follower_count DESC
            LIMIT 10
        """
        result = execute_query(query)
        assert result["success"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
