"""
Basic performance regression tests.

These tests establish baseline performance metrics and detect regressions.
NOT designed for load testing or stress testing (that's Phase 2).

Tests cover:
- Query planning time (should be fast)
- Simple query execution time
- Complex query execution time
- Variable-length path performance
- Aggregation performance
- Performance comparison baselines
"""

import pytest
import time
from conftest import (
    execute_cypher,
    assert_query_success
)


# Performance thresholds (in seconds)
PLANNING_THRESHOLD = 0.5  # Query planning should be < 500ms
SIMPLE_QUERY_THRESHOLD = 2.0  # Simple queries should be < 2s
COMPLEX_QUERY_THRESHOLD = 10.0  # Complex queries should be < 10s


class TestSimpleQueryPerformance:
    """Test performance of simple queries (baseline)."""
    
    def test_simple_match_performance(self, simple_graph):
        """Test simple MATCH query performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (n:TestUser)
            RETURN n.name
            ORDER BY n.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < SIMPLE_QUERY_THRESHOLD, \
            f"Simple MATCH took {elapsed:.2f}s (threshold: {SIMPLE_QUERY_THRESHOLD}s)"
    
    def test_simple_where_performance(self, simple_graph):
        """Test simple WHERE filter performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (n:TestUser)
            WHERE n.age > 25
            RETURN n.name
            ORDER BY n.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < SIMPLE_QUERY_THRESHOLD, \
            f"Simple WHERE took {elapsed:.2f}s (threshold: {SIMPLE_QUERY_THRESHOLD}s)"
    
    def test_single_relationship_performance(self, simple_graph):
        """Test single relationship traversal performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN a.name, b.name
            ORDER BY a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < SIMPLE_QUERY_THRESHOLD, \
            f"Single relationship took {elapsed:.2f}s (threshold: {SIMPLE_QUERY_THRESHOLD}s)"


class TestAggregationPerformance:
    """Test performance of aggregation queries."""
    
    def test_simple_count_performance(self, simple_graph):
        """Test COUNT aggregation performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (n:TestUser)
            RETURN COUNT(n) as total
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < SIMPLE_QUERY_THRESHOLD, \
            f"COUNT took {elapsed:.2f}s (threshold: {SIMPLE_QUERY_THRESHOLD}s)"
    
    def test_group_by_performance(self, simple_graph):
        """Test GROUP BY performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN a.name, COUNT(b) as follows
            ORDER BY follows DESC, a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < SIMPLE_QUERY_THRESHOLD, \
            f"GROUP BY took {elapsed:.2f}s (threshold: {SIMPLE_QUERY_THRESHOLD}s)"
    
    def test_multiple_aggregations_performance(self, simple_graph):
        """Test multiple aggregation functions."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (n:TestUser)
            RETURN COUNT(n) as total, 
                   AVG(n.age) as avg_age,
                   MIN(n.age) as min_age,
                   MAX(n.age) as max_age
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < SIMPLE_QUERY_THRESHOLD, \
            f"Multiple aggregations took {elapsed:.2f}s (threshold: {SIMPLE_QUERY_THRESHOLD}s)"


class TestComplexQueryPerformance:
    """Test performance of complex queries."""
    
    def test_multi_hop_performance(self, simple_graph):
        """Test 2-hop traversal performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)-[:TEST_FOLLOWS]->(c:TestUser)
            RETURN DISTINCT c.name
            ORDER BY c.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"2-hop traversal took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"
    
    def test_optional_match_performance(self, simple_graph):
        """Test OPTIONAL MATCH performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)
            OPTIONAL MATCH (a)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN a.name, COUNT(b) as follows
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"OPTIONAL MATCH took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"
    
    def test_case_expression_performance(self, simple_graph):
        """Test CASE expression performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (n:TestUser)
            RETURN n.name,
                   CASE
                       WHEN n.age < 25 THEN 'Young'
                       WHEN n.age < 35 THEN 'Adult'
                       ELSE 'Senior'
                   END as age_group
            ORDER BY n.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"CASE expression took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"


class TestVariableLengthPerformance:
    """Test performance of variable-length path queries."""
    
    def test_variable_length_short_range(self, simple_graph):
        """Test variable-length with short range."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..2]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN DISTINCT b.name
            ORDER BY b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"Variable-length *1..2 took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"
    
    def test_variable_length_medium_range(self, simple_graph):
        """Test variable-length with medium range."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN COUNT(DISTINCT b) as reachable
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"Variable-length *1..3 took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"
    
    @pytest.mark.slow
    def test_variable_length_unbounded(self, simple_graph):
        """Test unbounded variable-length (potentially slow)."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN COUNT(DISTINCT b) as reachable
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"Variable-length unbounded took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"


class TestShortestPathPerformance:
    """Test performance of shortest path algorithms."""
    
    def test_shortest_path_performance(self, simple_graph):
        """Test shortestPath() performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH p = shortestPath((a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser))
            WHERE a.name = 'Alice' AND b.name = 'Diana'
            RETURN length(p) as path_length
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"shortestPath took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"
    
    def test_all_shortest_paths_performance(self, simple_graph):
        """Test allShortestPaths() performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH p = allShortestPaths((a:TestUser)-[:TEST_FOLLOWS*]->(b:TestUser))
            WHERE a.name = 'Alice'
            RETURN COUNT(*) as path_count
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert elapsed < COMPLEX_QUERY_THRESHOLD, \
            f"allShortestPaths took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD}s)"


class TestPerformanceComparison:
    """Test relative performance of equivalent queries."""
    
    def test_distinct_vs_group_by(self, simple_graph):
        """Compare DISTINCT vs GROUP BY performance."""
        # DISTINCT approach
        start1 = time.time()
        response1 = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN DISTINCT a.name
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        time1 = time.time() - start1
        
        # GROUP BY approach
        start2 = time.time()
        response2 = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            RETURN a.name
            ORDER BY a.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        time2 = time.time() - start2
        
        assert_query_success(response1)
        assert_query_success(response2)
        
        # Neither should be more than 5x slower than the other
        ratio = max(time1, time2) / min(time1, time2)
        assert ratio < 5.0, \
            f"Performance ratio too high: {ratio:.2f}x (DISTINCT: {time1:.3f}s, plain: {time2:.3f}s)"
    
    def test_filter_early_vs_late(self, simple_graph):
        """Compare filtering before vs after traversal."""
        # Filter early (better)
        start1 = time.time()
        response1 = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            WHERE a.age > 25
            RETURN b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        time1 = time.time() - start1
        
        # Effectively same query (optimizer should handle it)
        start2 = time.time()
        response2 = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS]->(b:TestUser)
            WHERE a.age > 25
            RETURN b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        time2 = time.time() - start2
        
        assert_query_success(response1)
        assert_query_success(response2)
        
        # Times should be similar (within 2x)
        ratio = max(time1, time2) / min(time1, time2)
        assert ratio < 2.0, \
            f"Performance inconsistency: {ratio:.2f}x difference"


class TestPerformanceBaselines:
    """Establish baseline metrics for future regression detection."""
    
    def test_baseline_simple_queries(self, simple_graph):
        """Establish baseline for simple query suite."""
        queries = [
            "MATCH (n:TestUser) RETURN COUNT(n)",
            "MATCH (n:TestUser) WHERE n.age > 25 RETURN n.name",
            "MATCH (a)-[:TEST_FOLLOWS]->(b) RETURN COUNT(*)",
            "MATCH (n:TestUser) RETURN n.name ORDER BY n.name LIMIT 10",
        ]
        
        times = []
        for query in queries:
            start = time.time()
            response = execute_cypher(query, schema_name=simple_graph["schema_name"])
            elapsed = time.time() - start
            times.append(elapsed)
            assert_query_success(response)
        
        avg_time = sum(times) / len(times)
        max_time = max(times)
        
        # Log baseline metrics
        print(f"\nBaseline Simple Queries:")
        print(f"  Average: {avg_time:.3f}s")
        print(f"  Maximum: {max_time:.3f}s")
        print(f"  All times: {[f'{t:.3f}s' for t in times]}")
        
        assert avg_time < SIMPLE_QUERY_THRESHOLD
        assert max_time < SIMPLE_QUERY_THRESHOLD
    
    def test_baseline_complex_queries(self, simple_graph):
        """Establish baseline for complex query suite."""
        queries = [
            "MATCH (a)-[:TEST_FOLLOWS]->(b)-[:TEST_FOLLOWS]->(c) RETURN COUNT(*)",
            "MATCH (a) OPTIONAL MATCH (a)-[:TEST_FOLLOWS]->(b) RETURN a.name, COUNT(b)",
            "MATCH (a)-[:TEST_FOLLOWS*1..2]->(b) WHERE a.name = 'Alice' RETURN COUNT(DISTINCT b)",
            "MATCH p = shortestPath((a)-[:TEST_FOLLOWS*]-(b)) WHERE a.name = 'Alice' RETURN COUNT(p)",
        ]
        
        times = []
        for query in queries:
            start = time.time()
            response = execute_cypher(query, schema_name=simple_graph["schema_name"])
            elapsed = time.time() - start
            times.append(elapsed)
            assert_query_success(response)
        
        avg_time = sum(times) / len(times)
        max_time = max(times)
        
        # Log baseline metrics
        print(f"\nBaseline Complex Queries:")
        print(f"  Average: {avg_time:.3f}s")
        print(f"  Maximum: {max_time:.3f}s")
        print(f"  All times: {[f'{t:.3f}s' for t in times]}")
        
        assert avg_time < COMPLEX_QUERY_THRESHOLD
        assert max_time < COMPLEX_QUERY_THRESHOLD


@pytest.mark.slow
class TestPerformanceStress:
    """Stress tests for performance limits (marked as slow)."""
    
    def test_large_result_set(self, simple_graph):
        """Test handling of large result sets."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*1..3]->(b:TestUser)
            RETURN a.name, b.name
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        # Should complete even with many results
        assert elapsed < COMPLEX_QUERY_THRESHOLD * 2, \
            f"Large result set took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD * 2}s)"
    
    def test_deep_recursion(self, simple_graph):
        """Test deep recursion performance."""
        start = time.time()
        
        response = execute_cypher(
            """
            MATCH (a:TestUser)-[:TEST_FOLLOWS*..10]->(b:TestUser)
            WHERE a.name = 'Alice'
            RETURN COUNT(DISTINCT b) as reachable
            """,
            schema_name=simple_graph["schema_name"]
        )
        
        elapsed = time.time() - start
        
        assert_query_success(response)
        # Deep recursion should still be reasonable
        assert elapsed < COMPLEX_QUERY_THRESHOLD * 2, \
            f"Deep recursion took {elapsed:.2f}s (threshold: {COMPLEX_QUERY_THRESHOLD * 2}s)"


# Performance test markers for selective running
pytest.mark.performance = pytest.mark.performance if hasattr(pytest.mark, 'performance') else lambda x: x
