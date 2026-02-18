"""
Test collect(node) + UNWIND pattern with tuple property mapping

These tests verify that after collecting nodes and unwinding them,
property access works correctly using tuple indices.
"""

import pytest

from conftest import execute_cypher


SCHEMA_NAME = "social_integration"


@pytest.mark.xfail(reason="collect+unwind CTE does not propagate all needed columns to outer query")
def test_collect_unwind_single_property():
    """Test basic collect + UNWIND with single property access"""
    response = execute_cypher(
        """
        MATCH (u:User)
        WITH u, collect(u) as users
        UNWIND users as user
        RETURN user.name
        LIMIT 3
        """,
        schema_name=SCHEMA_NAME,
    )
    
    assert "results" in response
    assert len(response["results"]) == 3
    assert all("user.name" in row for row in response["results"])
    assert all(row["user.name"] is not None for row in response["results"])


@pytest.mark.xfail(reason="collect+unwind CTE does not propagate all needed columns to outer query")
def test_collect_unwind_multiple_properties():
    """Test collect + UNWIND with multiple property access"""
    response = execute_cypher(
        """
        MATCH (u:User)
        WITH u, collect(u) as users
        UNWIND users as user
        RETURN user.name, user.email, user.city
        LIMIT 3
        """,
        schema_name=SCHEMA_NAME,
    )
    
    assert "results" in response
    assert len(response["results"]) == 3
    
    for row in response["results"]:
        assert "user.name" in row
        assert "user.email" in row
        assert "user.city" in row
        assert row["user.name"] is not None
        assert row["user.email"] is not None
        assert row["user.city"] is not None


@pytest.mark.xfail(reason="collect+unwind CTE does not propagate all needed columns to outer query")
def test_collect_unwind_with_ordering():
    """Test collect + UNWIND with explicit ordering"""
    response = execute_cypher(
        """
        MATCH (u:User)
        WITH u, collect(u) as users
        UNWIND users as user
        RETURN user.name, user.city
        ORDER BY user.city
        LIMIT 3
        """,
        schema_name=SCHEMA_NAME,
    )
    
    assert "results" in response
    assert len(response["results"]) == 3
    assert all("user.name" in row for row in response["results"])
    assert all("user.city" in row for row in response["results"])
    
    cities = [row["user.city"] for row in response["results"]]
    assert cities == sorted(cities), "Results should be ordered by city"


def test_collect_unwind_with_aggregate():
    """Test collect + UNWIND combined with aggregation"""
    response = execute_cypher(
        """
        MATCH (u:User)
        WITH u, collect(u) as users
        UNWIND users as user
        RETURN user.country, count(*) as user_count
        ORDER BY user_count DESC
        LIMIT 3
        """,
        schema_name=SCHEMA_NAME,
    )
    
    assert "results" in response
    assert len(response["results"]) > 0
    
    for row in response["results"]:
        assert "user.country" in row
        assert "user_count" in row
        assert isinstance(row["user_count"], int)
        assert row["user_count"] > 0


def test_collect_unwind_distinct_properties():
    """Test collect + UNWIND to get distinct property values"""
    response = execute_cypher(
        """
        MATCH (u:User)
        WITH u, collect(u) as users
        UNWIND users as user
        RETURN DISTINCT user.city
        ORDER BY user.city
        """,
        schema_name=SCHEMA_NAME,
    )
    
    assert "results" in response
    cities = [row["user.city"] for row in response["results"]]
    assert len(cities) == len(set(cities)), "Cities should be distinct"
    assert cities == sorted(cities), "Cities should be ordered"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
