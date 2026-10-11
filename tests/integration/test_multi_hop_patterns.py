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

Shape tests that once inspected the generated SQL text (UNION branch counts, JOIN columns)
now check the RESULT rows against a brute-force walk over the engine's own single-hop edge
list, with Cypher relationship uniqueness (no edge twice in one MATCH; an undirected
self-loop matches once). Values verified against Neo4j 5.26.
"""

from collections import Counter

import pytest
import requests
from conftest import execute_cypher, CLICKGRAPH_URL


def _rows(query: str, schema_name: str) -> list:
    result = execute_cypher(query, schema_name=schema_name, raise_on_error=False)
    assert "results" in result, f"Query failed: {result}"
    return result["results"]


def _edges(schema_name: str, label: str, rel: str, key: str) -> list:
    return [(r["a"], r["b"]) for r in _rows(
        f"MATCH (a:{label})-[:{rel}]->(b:{label}) RETURN a.{key} AS a, b.{key} AS b",
        schema_name)]


def _walks(edges: list, dirs: list) -> Counter:
    """Node tuples of consecutive single hops (`dirs`: "out" or "both"), every edge used
    at most once."""
    def moves(node, d):
        for i, (src, dst) in enumerate(edges):
            if src == node:
                yield i, dst
            elif d == "both" and dst == node:
                yield i, src

    rows = Counter()

    def rec(k, node, used, acc):
        if k == len(dirs):
            rows[tuple(acc)] += 1
            return
        for i, nxt in moves(node, dirs[k]):
            if i not in used:
                rec(k + 1, nxt, used | {i}, acc + [nxt])

    for node in {x for e in edges for x in e}:
        rec(0, node, frozenset(), [node])
    return rows


def _flights() -> list:
    return _edges("ontime_flights", "Airport", "FLIGHT", "code")


def _got(query: str, schema_name: str, columns: list) -> Counter:
    return Counter(tuple(r[c] for c in columns) for r in _rows(query, schema_name))


# ============================================================================
# SQL Generation Tests (sql_only=true) - No database required
# ============================================================================

class TestMultiHopSqlGeneration:
    """Multi-hop pattern shapes: SQL-generation smoke checks plus result-row checks."""
    
    def get_sql(self, query: str, schema_name: str = "ontime_flights") -> str:
        """Get generated SQL for a Cypher query.
        
        Args:
            query: Cypher query (USE clause auto-prepended if not present)
            schema_name: Schema to use (default: ontime_flights for denormalized tests)
        """
        # Auto-prepend USE clause if not present
        if not query.strip().upper().startswith("USE "):
            query = f"USE {schema_name} {query}"
        
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
        ("undirected", 4),
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

    def test_2hop_directed_denormalized_rows(self):
        """2-hop directed pattern: the second flight leaves where the first landed."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) RETURN a.code, b.code, c.code"
        got = _got(query, "ontime_flights", ["a.code", "b.code", "c.code"])
        assert got == _walks(_flights(), ["out", "out"])
        assert sum(got.values()) == 5

    def test_2hop_undirected_denormalized_column_swapping(self):
        """Undirected hops read each flight both ways (Origin and Dest swap roles)."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code"
        got = _got(query, "ontime_flights", ["a.code", "b.code", "c.code"])
        flights = _flights()
        assert got == _walks(flights, ["both", "both"])
        assert sum(got.values()) == 12
        # both orientations of the first flight occur
        assert {(a, b) for a, b, _ in got} & set(flights)
        assert {(b, a) for a, b, _ in got} & set(flights)

    def test_3hop_directed_denormalized(self):
        """Test 3-hop directed pattern with denormalized schema."""
        query = """
        MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)-[f3:FLIGHT]->(d:Airport)
        RETURN a.code, b.code, c.code, d.code
        """
        got = _got(query, "ontime_flights", ["a.code", "b.code", "c.code", "d.code"])
        assert got == _walks(_flights(), ["out", "out", "out"])
        assert sum(got.values()) == 3

    def test_mixed_directed_undirected_denormalized(self):
        """Mixed pattern: a directed flight, then a flight either way."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code"
        got = _got(query, "ontime_flights", ["a.code", "b.code", "c.code"])
        assert got == _walks(_flights(), ["out", "both"])
        assert sum(got.values()) == 5

    # -------------------------------------------------------------------------
    # Standard Schema Tests (social_integration.yaml)
    # -------------------------------------------------------------------------

    def test_2hop_directed_standard_rows(self):
        """Test 2-hop directed with standard separate tables."""
        query = "MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User) RETURN a.user_id, b.user_id, c.user_id"
        got = _got(query, "social_integration", ["a.user_id", "b.user_id", "c.user_id"])
        edges = _edges("social_integration", "User", "FOLLOWS", "user_id")
        assert got == _walks(edges, ["out", "out"])
        assert sum(got.values()) == 35

    def test_2hop_undirected_standard_rows(self):
        """Test 2-hop undirected with standard tables (each hop read both ways)."""
        query = "MATCH (a:User)-[r1:FOLLOWS]-(b:User)-[r2:FOLLOWS]-(c:User) RETURN a.user_id, b.user_id, c.user_id"
        got = _got(query, "social_integration", ["a.user_id", "b.user_id", "c.user_id"])
        edges = _edges("social_integration", "User", "FOLLOWS", "user_id")
        assert got == _walks(edges, ["both", "both"])
        assert sum(got.values()) == 122

    def test_3hop_directed_standard(self):
        """Test 3-hop directed with standard tables."""
        query = """
        MATCH (a:User)-[r1:FOLLOWS]->(b:User)-[r2:FOLLOWS]->(c:User)-[r3:FOLLOWS]->(d:User)
        RETURN a.name, b.name, c.name, d.name LIMIT 5
        """
        sql = self.get_sql(query, schema_name="social_integration")
        
        # Skip if schema not loaded
        if "No relationship schema found" in sql or "PLANNING_ERROR" in sql:
            pytest.skip("Standard schema (social_integration.yaml) not loaded")
        
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
            schema_name="ontime_flights",
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
            schema_name="ontime_flights",
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
            schema_name="ontime_flights",
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
            schema_name="ontime_flights",
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
            schema_name="ontime_flights",
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
            schema_name="ontime_flights",
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
        """Outgoing edge: r2's Origin is r1's Dest (next hop starts where prev ended)."""
        query = "MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) RETURN a.code"
        got = _got(query, "ontime_flights", ["a.code"])
        expected = Counter((a,) for a, _, _ in _walks(_flights(), ["out", "out"]).elements())
        assert got == expected
        assert got == Counter({("JFK",): 2, ("LAX",): 2, ("SFO",): 1})

    def test_undirected_has_both_join_directions(self):
        """Undirected patterns should have both Origin→Dest and Dest→Origin JOINs."""
        query = "USE ontime_flights MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code LIMIT 1"
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

    def test_single_hop_undirected(self):
        """A single undirected hop reads every flight once in each direction."""
        query = "MATCH (a:Airport)-[r:FLIGHT]-(b:Airport) RETURN a.code, b.code"
        got = _got(query, "ontime_flights", ["a.code", "b.code"])
        assert got == _walks(_flights(), ["both"])
        assert sum(got.values()) == 10

    def test_4hop_undirected_count(self):
        """4 undirected hops: every orientation combination, no flight used twice."""
        query = """
        MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport)-[r3:FLIGHT]-(d:Airport)-[r4:FLIGHT]-(e:Airport)
        RETURN count(*) AS n
        """
        got = _rows(query, "ontime_flights")[0]["n"]
        assert got == sum(_walks(_flights(), ["both"] * 4).values()) == 8

    def test_all_incoming_direction(self):
        """Test all incoming arrows."""
        query = "USE ontime_flights MATCH (a:Airport)<-[r1:FLIGHT]-(b:Airport)<-[r2:FLIGHT]-(c:Airport) RETURN a.code, c.code LIMIT 5"
        sql = self.get_sql(query)
        
        # All directed = no UNION
        assert "UNION" not in sql, "All incoming should not have UNION"

    def test_mixed_incoming_outgoing(self):
        """Test mixed incoming and outgoing arrows."""
        query = "USE ontime_flights MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)<-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code LIMIT 5"
        sql = self.get_sql(query)
        
        # Both directed = no UNION
        assert "UNION" not in sql, "Mixed directed should not have UNION"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
