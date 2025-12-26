#!/usr/bin/env python3
"""
Integration tests for pattern comprehensions feature.

Tests pattern comprehension syntax: [(pattern) WHERE condition | projection]
which is rewritten to OPTIONAL MATCH + collect() internally.
"""

import pytest
from datetime import date
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
)


@pytest.fixture(scope="module")
def setup_pattern_comp_data(clickhouse_client):
    """Set up test data for pattern comprehension tests."""
    
    # Drop existing tables
    clickhouse_client.command("DROP TABLE IF EXISTS brahmand.pattern_comp_users")
    clickhouse_client.command("DROP TABLE IF EXISTS brahmand.pattern_comp_follows")
    
    # Create users table
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS brahmand.pattern_comp_users (
            user_id UInt32,
            full_name String,
            country String,
            city String
        ) ENGINE = MergeTree()
        ORDER BY user_id
    """)
    
    # Create follows table
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS brahmand.pattern_comp_follows (
            follower_id UInt32,
            followed_id UInt32,
            follow_date Date
        ) ENGINE = MergeTree()
        ORDER BY (follower_id, followed_id)
    """)
    
    # Insert test users
    users_data = [
        (1, "Alice", "USA", "NYC"),
        (2, "Bob", "USA", "SF"),
        (3, "Charlie", "UK", "London"),
        (4, "Diana", "USA", "LA"),
        (5, "Eve", "Canada", "Toronto"),
    ]
    clickhouse_client.insert("brahmand.pattern_comp_users", users_data, column_names=["user_id", "full_name", "country", "city"])
    
    # Insert follows relationships
    # Alice follows: Bob, Charlie, Diana
    # Bob follows: Alice, Diana
    # Charlie follows: Alice
    # Diana follows: none
    # Eve follows: Alice, Bob
    follows_data = [
        (1, 2, date(2024, 1, 1)),  # Alice -> Bob
        (1, 3, date(2024, 1, 2)),  # Alice -> Charlie
        (1, 4, date(2024, 1, 3)),  # Alice -> Diana
        (2, 1, date(2024, 1, 4)),  # Bob -> Alice
        (2, 4, date(2024, 1, 5)),  # Bob -> Diana
        (3, 1, date(2024, 1, 6)),  # Charlie -> Alice
        (5, 1, date(2024, 1, 7)),  # Eve -> Alice
        (5, 2, date(2024, 1, 8)),  # Eve -> Bob
    ]
    clickhouse_client.insert("brahmand.pattern_comp_follows", follows_data, column_names=["follower_id", "followed_id", "follow_date"])
    
    yield
    
    # Cleanup
    clickhouse_client.command("DROP TABLE IF EXISTS brahmand.pattern_comp_users")
    clickhouse_client.command("DROP TABLE IF EXISTS brahmand.pattern_comp_follows")


def test_simple_pattern_comprehension(setup_pattern_comp_data):
    """Test simple pattern comprehension without WHERE clause."""
    cypher = """
        MATCH (u:PatternCompUser)
        WHERE u.user_id = 1
        RETURN u.name, [(u)-[:PATTERN_COMP_FOLLOWS]->(f) | f.name] AS friends
    """
    
    result = execute_cypher(cypher)
    assert_query_success(result)
    assert_row_count(result, 1)
    
    row = result["results"][0]
    assert row["u.name"] == "Alice"
    
    friends = row["friends"]
    assert isinstance(friends, list)
    assert set(friends) == {"Bob", "Charlie", "Diana"}


def test_pattern_comprehension_with_where(setup_pattern_comp_data):
    """Test pattern comprehension with WHERE clause filter."""
    cypher = """
        MATCH (u:PatternCompUser)
        WHERE u.user_id = 1
        RETURN u.name, [(u)-[:PATTERN_COMP_FOLLOWS]->(f) WHERE f.country = 'USA' | f.name] AS us_friends
    """
    
    result = execute_cypher(cypher)
    assert_query_success(result)
    assert_row_count(result, 1)
    
    row = result["results"][0]
    assert row["u.name"] == "Alice"
    
    us_friends = row["us_friends"]
    assert isinstance(us_friends, list)
    assert set(us_friends) == {"Bob", "Diana"}


def test_multiple_pattern_comprehensions(setup_pattern_comp_data):
    """Test multiple pattern comprehensions in same query."""
    cypher = """
        MATCH (u:PatternCompUser)
        WHERE u.user_id = 1
        RETURN u.name,
               [(u)-[:PATTERN_COMP_FOLLOWS]->(f) | f.name] AS friends,
               [(u)<-[:PATTERN_COMP_FOLLOWS]-(follower) | follower.name] AS followers
    """
    
    result = execute_cypher(cypher)
    assert_query_success(result)
    assert_row_count(result, 1)
    
    row = result["results"][0]
    assert row["u.name"] == "Alice"
    
    friends = row["friends"]
    followers = row["followers"]
    
    assert isinstance(friends, list)
    assert isinstance(followers, list)
    
    assert set(friends) == {"Bob", "Charlie", "Diana"}
    assert set(followers) == {"Bob", "Charlie", "Eve"}


def test_pattern_comprehension_empty_result(setup_pattern_comp_data):
    """Test pattern comprehension returns empty list when no matches."""
    cypher = """
        MATCH (u:PatternCompUser)
        WHERE u.user_id = 4
        RETURN u.name, [(u)-[:PATTERN_COMP_FOLLOWS]->(f) | f.name] AS friends
    """
    
    result = execute_cypher(cypher)
    assert_query_success(result)
    assert_row_count(result, 1)
    
    row = result["results"][0]
    assert row["u.name"] == "Diana"
    
    friends = row["friends"]
    assert isinstance(friends, list)
    assert len(friends) == 0


def test_pattern_comprehension_with_expression(setup_pattern_comp_data):
    """Test pattern comprehension with expression projection."""
    cypher = """
        MATCH (u:PatternCompUser)
        WHERE u.user_id = 1
        RETURN u.name, [(u)-[:PATTERN_COMP_FOLLOWS]->(f) | f.name + ' from ' + f.country] AS friend_locations
    """
    
    result = execute_cypher(cypher)
    assert_query_success(result)
    assert_row_count(result, 1)
    
    row = result["results"][0]
    friend_locations = row["friend_locations"]
    
    assert isinstance(friend_locations, list)
    assert len(friend_locations) == 3
    
    expected = {"Bob from USA", "Charlie from UK", "Diana from USA"}
    assert set(friend_locations) == expected
