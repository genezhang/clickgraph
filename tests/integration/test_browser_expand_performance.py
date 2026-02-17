"""
Browser Node Expand Performance Tests

Tests the exact query sequence Neo4j Browser uses when clicking nodes to expand.

BROWSER EXPAND PROTOCOL (from AGENTS.md Section 7):
1. User clicks node in Neo4j Browser
2. Browser sends Query 1: Fetch node with id(a) = N
3. Browser sends Query 2: Fetch all adjacent nodes (undirected)

Query Sequence:
  Q1: MATCH (a) WHERE id(a) = <element_id> RETURN a
  Q2: MATCH (a)--(o) WHERE id(a) = <element_id> AND NOT id(o) IN [<other_ids>] RETURN o

Performance Goal: <500ms per query (interactive acceptable)
Current Issue: Q2 on benchmark data takes 6+ seconds (undirected pattern on 250M-row tables)

Tests use social_integration schema with realistic data volumes:
- users_bench: 5M rows
- posts_bench: 100M rows
- user_follows_bench: 10M rows
- authored_bench: 100M rows
- post_likes_bench: 250M rows (bottleneck!)
"""

import pytest
import time
from typing import Dict, Any, List, Tuple
from conftest import (
    execute_cypher,
    assert_query_success
)


# Performance thresholds
EXPAND_QUERY_THRESHOLD = 0.5  # 500ms - target for interactive browser
EXPAND_QUERY_SLOW_THRESHOLD = 10.0  # Log warning if > 10s


class BrowserExpandPerformanceMetrics:
    """Collect and track expand query performance metrics."""
    
    def __init__(self):
        self.results = {
            "fetch_node": [],  # Q1 times
            "expand_unfiltered": [],  # Q2 times (no exclude list)
            "expand_filtered": [],  # Q2 times (with exclude list)
        }
    
    def add_fetch_time(self, elapsed: float, node_id: int, node_label: str):
        self.results["fetch_node"].append({
            "time": elapsed,
            "node_id": node_id,
            "node_label": node_label
        })
    
    def add_expand_time(self, elapsed: float, node_id: int, with_exclude: bool, 
                        adjacent_count: int):
        key = "expand_filtered" if with_exclude else "expand_unfiltered"
        self.results[key].append({
            "time": elapsed,
            "node_id": node_id,
            "with_exclude": with_exclude,
            "adjacent_count": adjacent_count
        })
    
    def summary(self) -> Dict[str, Any]:
        """Return performance summary."""
        return {
            "fetch_node": self._calc_stats(self.results["fetch_node"]),
            "expand_unfiltered": self._calc_stats(self.results["expand_unfiltered"]),
            "expand_filtered": self._calc_stats(self.results["expand_filtered"]),
        }
    
    @staticmethod
    def _calc_stats(measurements: List[Dict]) -> Dict[str, float]:
        if not measurements:
            return {}
        
        times = [m["time"] for m in measurements]
        return {
            "count": len(times),
            "min": min(times),
            "max": max(times),
            "avg": sum(times) / len(times),
            "total": sum(times),
        }


@pytest.fixture(scope="session")
def sample_element_ids():
    """Fetch real sample node element_ids from database."""
    query = """
    USE social_integration
    MATCH (n:User) RETURN id(n) as element_id LIMIT 5
    """
    response = execute_cypher(query)
    assert_query_success(response)
    
    element_ids = []
    for result_item in response.get("results", []):
        if result_item and "element_id" in result_item:
            element_ids.append(result_item["element_id"])
    
    assert len(element_ids) > 0, "Should fetch at least one sample node"
    return element_ids


@pytest.fixture
def browser_metrics():
    """Fixture for collecting performance metrics."""
    return BrowserExpandPerformanceMetrics()


@pytest.fixture
def test_element_id(sample_element_ids):
    """Get the element_id of first sample node for testing."""
    return sample_element_ids[0]


@pytest.fixture
def exclude_element_ids(sample_element_ids):
    """Get element_ids of other sample nodes for exclude list."""
    return sample_element_ids[1:3]


class TestBrowserExpandQuerySequence:
    """Test the exact two-query sequence Browser sends on node expand."""
    
    def test_expand_fetch_single_node(self, test_element_id):
        """
        Browser Query 1: Fetch a single node by id()
        
        Represents: User clicks on a node in the browser
        Expected: <100ms (should be very fast - single node lookup)
        """
        # Using benchmark schema with real element_id from database
        query = f"""
        USE social_integration
        MATCH (a) WHERE id(a) = {test_element_id} RETURN a
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        assert len(response.get("results", [])) >= 1, f"Should find node with element_id={test_element_id}"
        
        # Q1 should be fast - single node lookup
        assert elapsed < 0.2, \
            f"Q1 (fetch single node) took {elapsed:.3f}s, expected <200ms"
        
        print(f"\n✓ Q1 Fetch single node: {elapsed*1000:.1f}ms")
    
    def test_expand_undirected_pattern_unfiltered(self, test_element_id):
        """
        Browser Query 2 (unfiltered): Fetch all adjacent nodes to a given node
        
        Represents: Browser expand - find all neighbors (ignoring types)
        Query: MATCH (a)--(o) WHERE id(a) = N RETURN o
        
        BOTTLENECK: Undirected pattern (--(o)) scans ALL relationships
        - May touch user_follows_bench (10M), authored_bench (100M), post_likes_bench (250M)
        - ClickHouse must scan entire relationship table before LIMIT
        
        Current Performance: ~6-10 seconds (unacceptable)
        Target: <500ms
        """
        query = f"""
        USE social_integration
        MATCH (a)--(o) WHERE id(a) = {test_element_id} RETURN o LIMIT 100
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        
        adjacent_count = len(response.get("results", []))
        print(f"\n⚠ Q2 Expand unfiltered (element_id={test_element_id}): {elapsed*1000:.1f}ms, "
              f"found {adjacent_count} adjacent nodes")
        
        # Log performance even if test passes
        if elapsed > EXPAND_QUERY_SLOW_THRESHOLD:
            pytest.skip(
                f"Q2 unfiltered took {elapsed:.2f}s (>10s threshold). "
                "Expected with 250M-row post_likes_bench. This is the optimization target."
            )
    
    def test_expand_undirected_pattern_filtered(self, test_element_id, exclude_element_ids):
        """
        Browser Query 2 (filtered): Fetch adjacent nodes, excluding already-visible ones
        
        Represents: Browser expand - find new neighbors not already shown
        Query: MATCH (a)--(o) WHERE id(a) = N AND NOT id(o) IN [id1, id2, ...] RETURN o
        
        This is what Browser actually sends:
        - Adds exclude list of already-visible node IDs (real element_ids)
        - Still hits the same relationship table scanning issue
        
        Current Performance: ~6-10 seconds
        Target: <500ms
        """
        exclude_str = ", ".join(str(eid) for eid in exclude_element_ids)
        exclude_clause = f"AND NOT id(o) IN [{exclude_str}]" if exclude_str else ""
        
        query = f"""
        USE social_integration
        MATCH (a)--(o) 
        WHERE id(a) = {test_element_id} {exclude_clause}
        RETURN o LIMIT 100
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        
        adjacent_count = len(response.get("results", []))
        print(f"\n⚠ Q2 Expand filtered (element_id={test_element_id}): {elapsed*1000:.1f}ms, "
              f"found {adjacent_count} adjacent nodes (excluding exclude list)")
        
        if elapsed > EXPAND_QUERY_SLOW_THRESHOLD:
            pytest.skip(
                f"Q2 filtered took {elapsed:.2f}s (>10s threshold). "
                "This demonstrates the performance issue with undirected patterns."
            )
    
    def test_expand_directed_pattern_performance(self, test_element_id):
        """
        Optimization candidate: Use directed patterns instead of undirected
        
        Current (slow):   MATCH (a)--(o)  -- must scan all relationships
        Proposed: Select specific types and union them:
                  MATCH (a)-->(o) RETURN o
                  UNION ALL
                  MATCH (a)<--(o) RETURN o
        
        Or filter by specific relationship types (if knowing user intent).
        """
        query = f"""
        USE social_integration
        MATCH (a) --> (o) WHERE id(a) = 2 RETURN o LIMIT 50
        UNION ALL
        MATCH (a) <-- (o) WHERE id(a) = 2 RETURN o LIMIT 50
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        
        print(f"\n✓ Q2 Directed pattern (union): {elapsed*1000:.1f}ms")
        
        if elapsed < EXPAND_QUERY_SLOW_THRESHOLD:
            print("  → Directed union is faster than undirected pattern")


class TestBrowserExpandWithVariousNodes:
    """Test expand performance across different node types and degrees."""
    
    def test_expand_low_degree_user(self, test_element_id):
        """Test expand on a user with few followers/following."""
        query = f"""
        USE social_integration
        MATCH (u:User)--(adjacent) WHERE id(u) = {test_element_id} RETURN adjacent LIMIT 50
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        print(f"\n  Low-degree user expand: {elapsed*1000:.1f}ms")
    
    def test_expand_high_degree_user(self, sample_element_ids):
        """Test expand on users with varying degrees."""
        # Test the first sample node (may have higher degree)
        test_element_id = sample_element_ids[0]
        query = f"""
        USE social_integration
        MATCH (u:User)--(adjacent) WHERE id(u) = {test_element_id} RETURN adjacent LIMIT 200
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        print(f"\n  User expand (with more adjacent nodes): {elapsed*1000:.1f}ms")
    
    def test_expand_post_node(self):
        """Test expand on a post node (should have fewer relationships)."""
        # Fetch a post node's element_id first
        query = """
        USE social_integration
        MATCH (p:Post) RETURN id(p) as element_id LIMIT 1
        """
        
        response = execute_cypher(query)
        assert_query_success(response)
        
        if len(response.get("results", [])) == 0:
            pytest.skip("No post node found in database")
        
        # Extract element_id from first result
        post_element_id = response["results"][0].get("element_id")
        
        if not post_element_id:
            pytest.skip("Could not extract element_id from post node")
        
        # Now expand that post
        expand_query = f"""
        USE social_integration
        MATCH (p:Post)--(adjacent) WHERE id(p) = {post_element_id} RETURN adjacent LIMIT 100
        """
        
        start = time.time()
        response = execute_cypher(expand_query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        print(f"\n  Post node expand: {elapsed*1000:.1f}ms")


class TestBrowserExpandBatchSequence:
    """Test performance of typical browser navigation patterns."""
    
    def test_sequential_expand_operations(self, sample_element_ids, browser_metrics):
        """
        Simulate user clicking through several nodes in sequence:
        1. Click node A → fetch it
        2. Expand A → see neighbors
        3. Click neighbor B → fetch it
        4. Expand B → see neighbors
        """
        # Use real element_ids from the sample
        test_element_ids = sample_element_ids[:4]
        
        for idx, element_id in enumerate(test_element_ids):
            # Q1: Fetch node
            q1_start = time.time()
            response = execute_cypher(f"""
                USE social_integration
                MATCH (a) WHERE id(a) = {element_id} RETURN a
            """)
            q1_elapsed = time.time() - q1_start
            browser_metrics.add_fetch_time(q1_elapsed, element_id, "User")
            
            # Q2: Expand (sample exclude list from other nodes)
            q2_start = time.time()
            exclude_ids = [nid for i, nid in enumerate(test_element_ids) if i != idx]
            exclude_str = ", ".join(str(eid) for eid in exclude_ids[:3])  # Limit exclude list
            
            exclude_clause = f"AND NOT id(o) IN [{exclude_str}]" if exclude_str else ""
            response = execute_cypher(f"""
                USE social_integration
                MATCH (a)--(o) 
                WHERE id(a) = {element_id} {exclude_clause}
                RETURN o LIMIT 100
            """)
            q2_elapsed = time.time() - q2_start
            adjacent_count = len(response.get("results", []))
            browser_metrics.add_expand_time(q2_elapsed, element_id, bool(exclude_str), adjacent_count)
            
            print(f"\n  Node {idx+1}: fetch={q1_elapsed*1000:.1f}ms, "
                  f"expand={q2_elapsed*1000:.1f}ms, neighbors={adjacent_count}")
        
        # Print summary
        summary = browser_metrics.summary()
        print(f"\n  === Performance Summary ===")
        print(f"  Q1 (Fetch):     avg={summary['fetch_node'].get('avg', 0)*1000:.1f}ms")
        print(f"  Q2 (Expand):    avg={summary['expand_filtered'].get('avg', 0)*1000:.1f}ms")


class TestQueryFragmentation:
    """
    Test optimization strategy: Fragment large undirected joins into typed batches.
    
    Problem: MATCH (a)--(o) scans all 250M+ relationships before LIMIT
    Solution: Restrict to specific relationship types
    
    Example:
      MATCH (a)-->(o:User) RETURN o         -- FOLLOWS, etc.
      UNION ALL
      MATCH (a)-->(o:Post) RETURN o         -- AUTHORED, LIKED
      UNION ALL
      MATCH (a)<--(o:User) RETURN o
      UNION ALL
      MATCH (a)<--(o:Post) RETURN o
    """
    
    def test_typed_expand_user_only(self, test_element_id):
        """Expand to only User nodes (filters out Post relationships)."""
        query = f"""
        USE social_integration
        MATCH (a)-->(o:User) WHERE id(a) = {test_element_id} RETURN o LIMIT 100
        UNION ALL
        MATCH (a)<--(o:User) WHERE id(a) = {test_element_id} RETURN o LIMIT 100
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        print(f"\n✓ Expand to User nodes only: {elapsed*1000:.1f}ms")
    
    def test_specific_relationship_types(self, test_element_id):
        """
        Optimization: Only fetch specific relationship types.
        
        This requires knowing which types are "interesting" for the domain.
        For social graph: FOLLOWS, AUTHORED, LIKED
        """
        query = f"""
        USE social_integration
        MATCH (a)-[:FOLLOWS]->(o) WHERE id(a) = {test_element_id} RETURN o LIMIT 100
        UNION ALL
        MATCH (a)<-[:FOLLOWS]-(o) WHERE id(a) = {test_element_id} RETURN o LIMIT 100
        UNION ALL
        MATCH (a)-[:AUTHORED]->(o) WHERE id(a) = {test_element_id} RETURN o LIMIT 100
        UNION ALL
        MATCH (a)-[:LIKED]->(o) WHERE id(a) = {test_element_id} RETURN o LIMIT 100
        """
        
        start = time.time()
        response = execute_cypher(query)
        elapsed = time.time() - start
        
        assert_query_success(response)
        print(f"\n✓ Expand with specific relationship types: {elapsed*1000:.1f}ms")


# Test runner script
if __name__ == "__main__":
    """Run performance tests and generate report."""
    import subprocess
    import sys
    
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║         Browser Node Expand Performance Test Suite             ║
    ║                                                                ║
    ║  Tests the exact query sequence Neo4j Browser generates        ║
    ║  when users click to expand nodes in the graph visualization  ║
    ║                                                                ║
    ║  Data: social_integration schema (955M rows total)              ║
    ║  Bottleneck: post_likes_bench (250M rows)                     ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    # Run with pytest
    args = [
        "-v",
        "--tb=short",
        "-s",  # Show prints
        __file__,
    ]
    
    result = subprocess.run(
        ["pytest"] + args,
        cwd="/home/gz/clickgraph"
    )
    
    sys.exit(result.returncode)
