"""
Integration tests for multiple UNWIND clauses (cartesian product)

Tests verify that multiple consecutive UNWIND clauses generate correct
cartesian products using multiple ARRAY JOIN clauses in ClickHouse.
"""

import pytest
import requests

BASE_URL = "http://localhost:8080"


def execute_query(query: str, sql_only: bool = False):
    """Execute a Cypher query against ClickGraph"""
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "sql_only": sql_only},
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return response.json()


def test_double_unwind_simple():
    """Test basic double UNWIND with literals - 2x2 cartesian product"""
    query = """
    UNWIND [1, 2] AS x
    UNWIND [10, 20] AS y
    RETURN x, y
    ORDER BY x, y
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) == 4, "Expected 2×2=4 rows"
    
    # Verify cartesian product
    expected = [
        {"x": 1, "y": 10},
        {"x": 1, "y": 20},
        {"x": 2, "y": 10},
        {"x": 2, "y": 20},
    ]
    for i, expected_row in enumerate(expected):
        assert result["results"][i]["x"] == expected_row["x"]
        assert result["results"][i]["y"] == expected_row["y"]


def test_triple_unwind():
    """Test triple UNWIND - 2x2x2 cartesian product"""
    query = """
    UNWIND [1, 2] AS x
    UNWIND [10, 20] AS y
    UNWIND [100, 200] AS z
    RETURN x, y, z
    ORDER BY x, y, z
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) == 8, "Expected 2×2×2=8 rows"
    
    # Verify first and last rows
    assert result["results"][0]["x"] == 1
    assert result["results"][0]["y"] == 10
    assert result["results"][0]["z"] == 100
    
    assert result["results"][7]["x"] == 2
    assert result["results"][7]["y"] == 20
    assert result["results"][7]["z"] == 200


def test_multiple_unwind_with_filtering():
    """Test multiple UNWIND with WHERE clause filtering"""
    query = """
    UNWIND [1, 2, 3] AS x
    UNWIND [10, 20, 30] AS y
    WHERE x + y > 25
    RETURN x, y, x + y AS sum
    ORDER BY x, y
    """
    result = execute_query(query)
    
    assert "results" in result
    # Should filter out combinations where x+y <= 25
    # Valid: (1,30)=31, (2,20)=22(NO), (2,30)=32, (3,10)=13(NO), (3,20)=23(NO), (3,30)=33
    # Actually: 31, 32, 33 = 3 rows? Let me recalculate:
    # x=1: 1+10=11(no), 1+20=21(no), 1+30=31(yes) = 1
    # x=2: 2+10=12(no), 2+20=22(no), 2+30=32(yes) = 1  
    # x=3: 3+10=13(no), 3+20=23(no), 3+30=33(yes) = 1
    # Total = 3 rows
    assert len(result["results"]) == 3
    
    # All sums should be > 25
    for row in result["results"]:
        assert row["sum"] > 25


def test_multiple_unwind_with_aggregation():
    """Test multiple UNWIND with aggregation"""
    query = """
    UNWIND [1, 2] AS x
    UNWIND [10, 20] AS y
    RETURN count(*) AS total_combinations, sum(x * y) AS sum_products
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) == 1
    
    # Should have 4 combinations
    assert result["results"][0]["total_combinations"] == 4
    
    # Sum of products: 1*10 + 1*20 + 2*10 + 2*20 = 10+20+20+40 = 90
    assert result["results"][0]["sum_products"] == 90


def test_multiple_unwind_with_strings():
    """Test multiple UNWIND with string values"""
    query = """
    UNWIND ['a', 'b'] AS letter
    UNWIND [1, 2, 3] AS num
    RETURN letter, num
    ORDER BY letter, num
    """
    result = execute_query(query)
    
    assert "results" in result
    assert len(result["results"]) == 6, "Expected 2×3=6 rows"
    
    # Check first combination
    assert result["results"][0]["letter"] == "a"
    assert result["results"][0]["num"] == 1
    
    # Check last combination
    assert result["results"][5]["letter"] == "b"
    assert result["results"][5]["num"] == 3


def test_multiple_unwind_sql_generation():
    """Verify SQL generation produces multiple ARRAY JOIN clauses"""
    query = """
    UNWIND [1, 2] AS x
    UNWIND [10, 20] AS y
    RETURN x, y
    """
    result = execute_query(query, sql_only=True)
    
    sql = result.get("generated_sql", "")
    
    # Should have two ARRAY JOIN clauses
    assert sql.count("ARRAY JOIN") == 2
    assert "ARRAY JOIN [1, 2] AS x" in sql or "ARRAY JOIN [10, 20] AS y" in sql
    
    # Should use system.one as dummy table
    assert "system.one" in sql


def test_unwind_with_varying_sizes():
    """Test multiple UNWIND with different sized arrays"""
    query = """
    UNWIND [1, 2, 3, 4] AS x
    UNWIND [10, 20] AS y
    RETURN x, y, x * y AS product
    ORDER BY x, y
    """
    result = execute_query(query)
    
    assert "results" in result
    # 4 × 2 = 8 rows
    assert len(result["results"]) == 8
    
    # Verify products are correct
    assert result["results"][0]["product"] == 10  # 1*10
    assert result["results"][1]["product"] == 20  # 1*20
    assert result["results"][6]["product"] == 40  # 4*10
    assert result["results"][7]["product"] == 80  # 4*20


if __name__ == "__main__":
    # Run tests
    print("Running multiple UNWIND integration tests...")
    pytest.main([__file__, "-v"])
