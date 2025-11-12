"""
E2E Tests: Parameter + Function Integration

Tests parameter substitution combined with Neo4j function translation.
Self-contained test bucket with dedicated schema and data.

Run with:
    pytest tests/e2e/test_param_func_e2e.py -v
    
Debug mode (preserve data after tests):
    CLICKGRAPH_DEBUG=1 pytest tests/e2e/test_param_func_e2e.py -v
"""

import pytest
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from e2e_framework import TestBucket, test_bucket, clickgraph_client, e2e_framework


# Test bucket configuration
BUCKET_DIR = Path(__file__).parent / "buckets" / "param_func"
PARAM_FUNC_BUCKET = {
    "name": "param_func",
    "database": "test_param_func",
    "schema_file": BUCKET_DIR / "schema.yaml",
    "setup_sql": BUCKET_DIR / "setup.sql",
    "teardown_sql": BUCKET_DIR / "teardown.sql"
}


@pytest.fixture(scope="module")
def param_func_bucket(e2e_framework):
    """Module-scoped bucket for all parameter+function tests."""
    bucket = TestBucket(**PARAM_FUNC_BUCKET)
    
    if not e2e_framework.setup_bucket(bucket):
        pytest.fail("Failed to setup param_func bucket")
    
    yield bucket
    
    e2e_framework.teardown_bucket(bucket)


class TestParameterFunctionBasics:
    """Basic parameter + function combinations."""
    
    def test_function_in_return_with_parameter_filter(self, clickgraph_client, param_func_bucket):
        """Test: Function in RETURN with parameter in WHERE."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            WHERE u.age > $minAge
            RETURN toUpper(u.name) AS upper_name, u.age
            ORDER BY u.age
            """,
            parameters={"minAge": 30},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        assert len(result["results"]) > 0
        
        # All users should have age > 30
        for row in result["results"]:
            assert row["age"] > 30
            # Name should be uppercase
            assert row["upper_name"].isupper()
    
    def test_function_on_property_with_parameter_comparison(self, clickgraph_client, param_func_bucket):
        """Test: Function applied to property compared with parameter."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            WHERE toUpper(u.status) = $status
            RETURN u.name, u.status
            """,
            parameters={"status": "ACTIVE"},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        assert len(result["results"]) > 0
        
        # All results should have active status
        for row in result["results"]:
            assert row["status"].upper() == "ACTIVE"
    
    def test_math_function_with_parameters(self, clickgraph_client, param_func_bucket):
        """Test: Math function in WHERE with parameter."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            WHERE abs(u.age - $targetAge) < $tolerance
            RETURN u.name, u.age
            ORDER BY u.age
            """,
            parameters={"targetAge": 30, "tolerance": 5},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        
        # All results should be within tolerance
        for row in result["results"]:
            assert abs(row["age"] - 30) < 5


class TestParameterFunctionComplex:
    """Complex scenarios with multiple parameters and functions."""
    
    def test_multiple_functions_multiple_parameters(self, clickgraph_client, param_func_bucket):
        """Test: Multiple string functions with parameter filters."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            WHERE u.age >= $minAge AND u.age <= $maxAge
            RETURN 
                toUpper(u.name) AS upper_name,
                toLower(u.email) AS lower_email,
                u.age
            ORDER BY u.age
            """,
            parameters={"minAge": 25, "maxAge": 35},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        assert len(result["results"]) > 0
        
        for row in result["results"]:
            # Age in range
            assert 25 <= row["age"] <= 35
            # Functions applied correctly
            assert row["upper_name"].isupper()
            assert row["lower_email"].islower()
    
    def test_aggregation_with_parameter_and_functions(self, clickgraph_client, param_func_bucket):
        """Test: Aggregation functions with parameter filter."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            WHERE u.age > $minAge
            RETURN 
                count(u) AS user_count,
                avg(u.age) AS avg_age,
                min(u.age) AS min_age,
                max(u.age) AS max_age
            """,
            parameters={"minAge": 25},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        assert len(result["results"]) == 1
        
        row = result["results"][0]
        assert row["user_count"] > 0
        assert row["min_age"] > 25
        assert row["avg_age"] >= row["min_age"]
        assert row["avg_age"] <= row["max_age"]
    
    def test_case_expression_with_parameters(self, clickgraph_client, param_func_bucket):
        """Test: CASE expression using parameter thresholds."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            RETURN 
                u.name,
                u.age,
                CASE 
                    WHEN u.age < $youngThreshold THEN 'young'
                    WHEN u.age < $middleThreshold THEN 'middle'
                    ELSE 'senior'
                END AS age_category
            ORDER BY u.age
            """,
            parameters={"youngThreshold": 30, "middleThreshold": 40},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        assert len(result["results"]) > 0
        
        for row in result["results"]:
            if row["age"] < 30:
                assert row["age_category"] == "young"
            elif row["age"] < 40:
                assert row["age_category"] == "middle"
            else:
                assert row["age_category"] == "senior"


class TestParameterFunctionWithRelationships:
    """Test parameter + function with relationship queries."""
    
    def test_function_on_relationship_traversal_with_parameters(self, clickgraph_client, param_func_bucket):
        """Test: Functions on relationship traversal with parameter filters."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)-[r:PLACED]->(o:Order)
            WHERE o.total > $minTotal
            RETURN 
                toUpper(u.name) AS user_name,
                u.age,
                o.total,
                ceil(o.total) AS rounded_total
            ORDER BY o.total DESC
            """,
            parameters={"minTotal": 100},
            schema_name=param_func_bucket.schema_name
        )
        
        print(f"\nDEBUG: Result keys: {list(result.keys())}")
        assert "results" in result
        print(f"DEBUG: Number of results: {len(result.get('results', []))}")
        
        # Debug: print actual column names
        if result.get("results"):
            print(f"DEBUG: First row keys: {list(result['results'][0].keys())}")
            print(f"DEBUG: First row: {result['results'][0]}")

        # NOTE: Properties without explicit AS clauses are returned with table prefix
        # e.g., u.age is returned as 'age', o.total as 'o.total' (ClickHouse behavior)
        for row in result["results"]:
            # Check for total (can be 'total' or 'o.total' depending on CH version)
            total = row.get("total") or row.get("o.total")
            assert total > 100
            assert row["user_name"]  # Has explicit alias
            assert row["rounded_total"] >= total  # Has explicit alias
            assert row["user_name"].isupper()
            # Ceiling should round up - use the variable we already extracted
            assert row["rounded_total"] >= total
    
    def test_aggregation_on_relationships_with_parameters(self, clickgraph_client, param_func_bucket):
        """Test: Aggregate relationship data with parameter filters."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)-[:PLACED]->(o:Order)
            WHERE u.age > $minAge
            RETURN 
                toUpper(u.name) AS name,
                count(o) AS order_count,
                sum(o.total) AS total_spent
            ORDER BY total_spent DESC
            """,
            parameters={"minAge": 25},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        
        for row in result["results"]:
            assert row["name"].isupper()
            assert row["order_count"] > 0
            assert row["total_spent"] > 0


class TestParameterFunctionEdgeCases:
    """Edge cases and special scenarios."""
    
    def test_nested_functions_with_parameters(self, clickgraph_client, param_func_bucket):
        """Test: Nested function calls with parameters."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            WHERE u.age > $minAge
            RETURN 
                toUpper(substring(u.name, $start, $length)) AS short_upper_name,
                u.name
            ORDER BY u.name
            """,
            parameters={"minAge": 25, "start": 0, "length": 3},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        assert len(result["results"]) > 0
        
        for row in result["results"]:
            # Should be first 3 chars, uppercase
            assert len(row["short_upper_name"]) <= 3
            assert row["short_upper_name"].isupper()
    
    def test_coalesce_with_parameters(self, clickgraph_client, param_func_bucket):
        """Test: Coalesce function with parameter defaults."""
        result = clickgraph_client.query_json(
            """
            MATCH (p:Product)
            RETURN 
                p.name,
                coalesce(p.price, $defaultPrice) AS effective_price
            """,
            parameters={"defaultPrice": 0.0},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        assert len(result["results"]) > 0
        
        # All products have prices, so should not use default
        for row in result["results"]:
            assert row["effective_price"] > 0
    
    def test_parameter_in_multiple_functions_same_query(self, clickgraph_client, param_func_bucket):
        """Test: Same parameter used in multiple functions."""
        result = clickgraph_client.query_json(
            """
            MATCH (u:User)
            WHERE u.age > $threshold
            RETURN 
                u.name,
                CASE WHEN u.age > $threshold + 10 THEN 'much older' ELSE 'slightly older' END AS category,
                abs(u.age - $threshold) AS age_diff
            ORDER BY age_diff
            """,
            parameters={"threshold": 30},
            schema_name=param_func_bucket.schema_name
        )
        
        assert "results" in result
        
        for row in result["results"]:
            # All should have positive age_diff since age > threshold
            assert row["age_diff"] > 0


if __name__ == "__main__":
    # Run with: python tests/e2e/test_param_func_e2e.py
    pytest.main([__file__, "-v", "--tb=short"])
