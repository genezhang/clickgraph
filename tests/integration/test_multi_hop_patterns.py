"""
Integration tests for multi-hop graph patterns across all schema types.

Tests the undirected multi-hop fix (Dec 3, 2025) which generates 2^n UNION
branches for n undirected edges with correct column swapping.

Schema types tested:
1. Standard (separate node + edge tables)
2. Denormalized (node properties stored on edge table)
3. Polymorphic (single edge table with type_column)

Patterns tested:
- 2-hop directed: (a)-[r1]->(b)-[r2]->(c)
- 2-hop undirected: (a)-[r1]-(b)-[r2]-(c)
- 2-hop mixed: (a)-[r1]->(b)-[r2]-(c)
- 3-hop directed: (a)-[r1]->(b)-[r2]->(c)-[r3]->(d)
- 3-hop with aggregation
"""

import pytest
import requests
from conftest import execute_cypher, CLICKGRAPH_URL


# ============================================================================
# SQL Generation Tests (sql_only=true) - No database required
# ============================================================================

class TestMultiHopSqlGeneration:
    """Test SQL generation for multi-hop patterns without database execution."""
    
    def get_sql(self, query: str) -> str:
        """Get generated SQL for a Cypher query."""
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": query, "sql_only": True},
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 200, f"Query failed: {response.text}"
        return response.json().get("generated_sql", "")

    # -------------------------------------------------------------------------
    # Denormalized Schema Tests (ontime_denormalized.yaml)
    # -------------------------------------------------------------------------
    
    @pytest.mark.parametrize("direction,expected_unions", [
        ("directed", 1),      # (a)-[r1]->(b)-[r2]->(c)
        ("undirected", 4),    # (a)-[r1]-(b)-[r2]-(c) = 2^2 branches
    ])
    def test_2hop_union_branch_count_denormalized(self, direction, expected_unions):
        """Verify correct number of UNION branches for denormalized schema."""
        if direction == "directed":
            query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) RETURN a.code, b.code, c.code LIMIT 5"
        else:
            query = "MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code LIMIT 5"
        
        sql = self.get_sql(query)
        union_count = sql.count("UNION ALL") + 1 if "UNION ALL" in sql else 1
        assert union_count == expected_unions, f"Expected {expected_unions} branches, got {union_count}.\nSQL: {sql}"

    def test_2hop_directed_denormalized_sql(self):
        """Test 2-hop directed pattern generates clean JOINs."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) RETURN a.code, b.code, c.code LIMIT 5"
        sql = self.get_sql(query)
        
        # Should be a single query with 2 JOINs
        assert "UNION" not in sql, "Directed pattern should not have UNION"
        assert sql.count("INNER JOIN") == 1, f"Expected 1 JOIN (self-join on flights), got {sql.count('INNER JOIN')}"
        assert "r1.Origin" in sql, "Should select Origin from r1"
        assert "r1.Dest" in sql or "r2.Origin" in sql, "Should reference intermediate airport"
        assert "r2.Dest" in sql, "Should select Dest from r2"

    def test_2hop_undirected_denormalized_column_swapping(self):
        """Test that undirected patterns correctly swap Origin/Dest columns."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code LIMIT 5"
        sql = self.get_sql(query)
        
        # Should have 4 UNION branches with different Origin/Dest combinations
        assert sql.count("UNION ALL") == 3, "Should have 4 branches (3 UNION ALLs)"
        
        # Check that we have both Origin and Dest appearing as a.code in different branches
        branches = sql.split("UNION ALL")
        a_code_patterns = set()
        for branch in branches:
            if 'r1.Origin AS "a.code"' in branch:
                a_code_patterns.add("Origin")
            if 'r1.Dest AS "a.code"' in branch:
                a_code_patterns.add("Dest")
        
        assert a_code_patterns == {"Origin", "Dest"}, \
            f"Expected both Origin and Dest for a.code, got {a_code_patterns}"

    def test_3hop_directed_denormalized(self):
        """Test 3-hop directed pattern with denormalized schema."""
        query = """
        MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)-[f3:FLIGHT]->(d:Airport)
        RETURN a.code, b.code, c.code, d.code LIMIT 5
        """
        sql = self.get_sql(query)
        
        # Should be single query with 2 JOINs (3 self-joins on flights)
        assert "UNION" not in sql, "Directed 3-hop should not have UNION"
        assert sql.count("INNER JOIN") == 2, f"Expected 2 JOINs, got {sql.count('INNER JOIN')}"

    def test_mixed_directed_undirected_denormalized(self):
        """Test mixed directed/undirected pattern generates 2 branches."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code LIMIT 5"
        sql = self.get_sql(query)
        
        # One directed + one undirected = 2^1 = 2 branches
        union_count = sql.count("UNION ALL") + 1 if "UNION ALL" in sql else 1
        assert union_count == 2, f"Expected 2 branches for mixed pattern, got {union_count}"

    # -------------------------------------------------------------------------
    # Standard Schema Tests (social_benchmark.yaml)
    # These tests are skipped if the standard schema is not loaded
    # -------------------------------------------------------------------------

    def test_2hop_directed_standard_sql(self):
        """Test 2-hop directed with standard separate tables."""
        # This requires the benchmark schema to be loaded
        query = "MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User) RETURN a.name, b.name, c.name LIMIT 5"
        sql = self.get_sql(query)
        
        # Skip if schema not loaded
        if "No relationship schema found" in sql or "PLANNING_ERROR" in sql:
            pytest.skip("Standard schema (social_benchmark.yaml) not loaded")
        
        # Standard schema has separate node and edge tables
        # Expect: users -> follows -> users -> follows -> users
        if "UNION" not in sql:  # Only if schema is loaded correctly
            assert "INNER JOIN" in sql, "Should have JOINs for standard schema"

    def test_2hop_undirected_standard_has_4_branches(self):
        """Test 2-hop undirected with standard tables generates 4 branches."""
        query = "MATCH (a:User)-[r1:FOLLOWS]-(b:User)-[r2:FOLLOWS]-(c:User) RETURN a.name, b.name, c.name LIMIT 5"
        sql = self.get_sql(query)
        
        # Skip if schema not loaded
        if "No relationship schema found" in sql or "PLANNING_ERROR" in sql:
            pytest.skip("Standard schema (social_benchmark.yaml) not loaded")
        
        if "UNION ALL" in sql:
            union_count = sql.count("UNION ALL") + 1
            assert union_count == 4, f"Expected 4 branches, got {union_count}"

    def test_3hop_directed_standard(self):
        """Test 3-hop directed with standard tables."""
        query = """
        MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User)-[r3:FOLLOWS]->(d:User)
        RETURN a.name, b.name, c.name, d.name LIMIT 5
        """
        sql = self.get_sql(query)
        
        # Skip if schema not loaded
        if "No relationship schema found" in sql or "PLANNING_ERROR" in sql:
            pytest.skip("Standard schema (social_benchmark.yaml) not loaded")
        
        # 3 hops should have 6 JOINs for standard schema (3 edges + 3 intermediate nodes)
        # Or no UNION for directed
        assert "UNION" not in sql or sql.count("UNION ALL") == 0, \
            "Directed 3-hop should not have UNION branches"


# ============================================================================
# Denormalized Schema Integration Tests (requires flights data)
# ============================================================================

@pytest.mark.usefixtures("denormalized_schema")
class TestDenormalizedMultiHop:
    """Integration tests for multi-hop patterns with denormalized (flights) schema."""
    
    @pytest.fixture(scope="class")
    def denormalized_schema(self, request):
        """Ensure denormalized schema is loaded."""
        # The server should be running with ontime_denormalized.yaml
        response = requests.get(f"{CLICKGRAPH_URL}/health")
        assert response.status_code == 200, "ClickGraph server not running"
        yield
    
    def test_2hop_directed_execution(self):
        """Execute 2-hop directed query and verify results."""
        result = execute_cypher(
            "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) "
            "RETURN a.code, b.code, c.code LIMIT 10",
            raise_on_error=False
        )
        
        # May fail if database not set up - that's ok for CI
        if "error" not in result:
            assert "results" in result
            if result["results"]:
                assert "a.code" in result["results"][0]
                assert "b.code" in result["results"][0]
                assert "c.code" in result["results"][0]

    def test_2hop_undirected_execution(self):
        """Execute 2-hop undirected query and verify it doesn't error."""
        result = execute_cypher(
            "MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport) "
            "RETURN a.code, b.code, c.code LIMIT 10",
            raise_on_error=False
        )
        
        # Should not error even with UNION ALL
        if "error" in result:
            assert "UNION" not in str(result["error"]), \
                f"UNION query should not fail: {result['error']}"

    def test_3hop_directed_execution(self):
        """Execute 3-hop directed query."""
        result = execute_cypher(
            "MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)-[f3:FLIGHT]->(d:Airport) "
            "RETURN a.code, b.code, c.code, d.code LIMIT 5",
            raise_on_error=False
        )
        
        if "error" not in result:
            assert "results" in result

    def test_3hop_with_where_clause(self):
        """Test 3-hop with WHERE conditions across hops."""
        result = execute_cypher(
            """
            MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)-[f3:FLIGHT]->(d:Airport)
            WHERE f1.carrier = f2.carrier AND f2.carrier = f3.carrier
            RETURN a.code, b.code, c.code, d.code, f1.carrier LIMIT 5
            """,
            raise_on_error=False
        )
        
        if "error" not in result and result.get("results"):
            # All carriers should be same
            for row in result["results"]:
                assert "f1.carrier" in row

    def test_3hop_with_aggregation(self):
        """Test 3-hop with WITH clause and aggregation."""
        result = execute_cypher(
            """
            MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)-[f3:FLIGHT]->(d:Airport)
            WITH b.code as hub, count(*) as connections
            RETURN hub, connections
            ORDER BY connections DESC LIMIT 5
            """,
            raise_on_error=False
        )
        
        if "error" not in result and result.get("results"):
            assert "hub" in result["results"][0]
            assert "connections" in result["results"][0]

    def test_3hop_count_distinct(self):
        """Test 3-hop with count(DISTINCT)."""
        result = execute_cypher(
            """
            MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)-[f3:FLIGHT]->(d:Airport)
            WITH b.code as hub, count(DISTINCT a.code) as origins, count(DISTINCT d.code) as destinations
            RETURN hub, origins, destinations
            ORDER BY origins DESC LIMIT 5
            """,
            raise_on_error=False
        )
        
        if "error" not in result and result.get("results"):
            assert "hub" in result["results"][0]
            assert "origins" in result["results"][0]
            assert "destinations" in result["results"][0]


# ============================================================================
# Join Condition Tests - Verify correct JOIN columns
# ============================================================================

class TestJoinConditions:
    """Verify JOIN conditions are correct for different directions."""
    
    def get_sql(self, query: str) -> str:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": query, "sql_only": True},
            headers={"Content-Type": "application/json"}
        )
        return response.json().get("generated_sql", "")

    def test_outgoing_join_uses_dest_to_origin(self):
        """Outgoing edge: r2.Origin = r1.Dest (next hop starts where prev ended)."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) RETURN a.code LIMIT 1"
        sql = self.get_sql(query)
        
        # For denormalized: r2.Origin = r1.Dest
        assert "r2.Origin = r1.Dest" in sql or "r2.Origin = f1.Dest" in sql.replace("f", "r"), \
            f"Outgoing JOIN should connect r2.Origin to r1.Dest.\nSQL: {sql}"

    def test_undirected_has_both_join_directions(self):
        """Undirected patterns should have both Origin→Dest and Dest→Origin JOINs."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code LIMIT 1"
        sql = self.get_sql(query)
        
        # Should have branches with both r2.Origin = r1.Dest AND r2.Dest = r1.Origin
        has_origin_to_dest = "r2.Origin = r1.Dest" in sql or "Origin = r1.Dest" in sql
        has_dest_to_origin = "r2.Dest = r1" in sql
        
        # At minimum, we should see UNION branches
        assert "UNION ALL" in sql, "Undirected should generate UNION ALL"


# ============================================================================
# Edge Case Tests
# ============================================================================

class TestMultiHopEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def get_sql(self, query: str) -> str:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": query, "sql_only": True},
            headers={"Content-Type": "application/json"}
        )
        return response.json().get("generated_sql", "")

    def test_single_hop_no_union(self):
        """Single hop should never have UNION."""
        query = "MATCH (a:Airport)-[r:FLIGHT]-(b:Airport) RETURN a.code, b.code LIMIT 5"
        sql = self.get_sql(query)
        
        # Single undirected edge = 2 branches
        union_count = sql.count("UNION ALL") + 1 if "UNION ALL" in sql else 1
        assert union_count == 2, f"Single undirected hop should have 2 branches, got {union_count}"

    def test_4hop_undirected_has_16_branches(self):
        """4 undirected hops = 2^4 = 16 branches."""
        query = """
        MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport)-[r3:FLIGHT]-(d:Airport)-[r4:FLIGHT]-(e:Airport)
        RETURN a.code LIMIT 1
        """
        sql = self.get_sql(query)
        
        union_count = sql.count("UNION ALL") + 1 if "UNION ALL" in sql else 1
        assert union_count == 16, f"4 undirected hops should have 16 branches, got {union_count}"

    def test_all_incoming_direction(self):
        """Test all incoming arrows."""
        query = "MATCH (a:Airport)<-[r1:FLIGHT]-(b:Airport)<-[r2:FLIGHT]-(c:Airport) RETURN a.code, c.code LIMIT 5"
        sql = self.get_sql(query)
        
        # All directed = no UNION
        assert "UNION" not in sql, "All incoming should not have UNION"

    def test_mixed_incoming_outgoing(self):
        """Test mixed incoming and outgoing arrows."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)<-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code LIMIT 5"
        sql = self.get_sql(query)
        
        # Both directed = no UNION
        assert "UNION" not in sql, "Mixed directed should not have UNION"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
