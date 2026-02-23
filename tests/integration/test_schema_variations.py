#!/usr/bin/env python3
"""
Test GraphRAG patterns across different schema variations.

Schema variations tested:
1. Standard - Separate node + edge tables
2. FK-edge - FK column as relationship
3. Denormalized - Node properties embedded in edge table
4. Polymorphic - Single table with type_column discriminator
5. Composite ID - Multi-column node identity
"""

import pytest
import requests
from typing import Dict, Any, List

CLICKGRAPH_URL = "http://localhost:8080"
GRAPHRAG_URL = "http://localhost:3000"


class SchemaTestConfig:
    """Configuration for a schema variation."""
    def __init__(self, name: str, schema_file: str, schema_name: str):
        self.name = name
        self.schema_file = schema_file
        self.schema_name = schema_name


# Schema configurations
SCHEMAS = {
    "standard": SchemaTestConfig(
        name="Standard (separate node+edge tables)",
        schema_file="schemas/test/unified_test_multi_schema.yaml",
        schema_name="standard"
    ),
    "fk_edge": SchemaTestConfig(
        name="FK-edge (FK column as relationship)",
        schema_file="schemas/test/unified_test_multi_schema.yaml",
        schema_name="fk_edge"
    ),
    "denormalized": SchemaTestConfig(
        name="Denormalized (node props in edge table)",
        schema_file="schemas/test/unified_test_multi_schema.yaml",
        schema_name="denormalized_flights"
    ),
    "polymorphic": SchemaTestConfig(
        name="Polymorphic (single table, type_column)",
        schema_file="schemas/test/unified_test_multi_schema.yaml",
        schema_name="polymorphic"
    ),
    "composite_id": SchemaTestConfig(
        name="Composite ID (multi-column node ID)",
        schema_file="schemas/test/unified_test_multi_schema.yaml",
        schema_name="composite_id"
    ),
    "coupled_edges": SchemaTestConfig(
        name="Coupled Edges (multiple relationships from one table)",
        schema_file="schemas/test/unified_test_multi_schema.yaml",
        schema_name="zeek_merged_test"
    ),
}


def query_clickgraph(cypher: str, schema_name: str) -> Dict[str, Any]:
    """Execute a Cypher query via ClickGraph."""
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": cypher, "schema_name": schema_name},
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    if response.status_code != 200:
        return {"error": response.text, "status_code": response.status_code}
    return response.json()


def query_graphrag(query: str, expand: Dict = None, schema_name: str = None) -> Dict[str, Any]:
    """Execute a GraphRAG query."""
    payload = {
        "query": query,
        "connection": {
            "type": "clickgraph",
            "url": CLICKGRAPH_URL,
            "schema_name": schema_name
        }
    }
    if expand:
        payload["expand"] = expand
    response = requests.post(
        f"{GRAPHRAG_URL}/v1/query",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    if response.status_code != 200:
        return {"error": response.text, "status_code": response.status_code}
    return response.json()


# ============================================================================
# SCHEMA 1: Standard (separate node + edge tables)
# ============================================================================

@pytest.fixture(scope="class")
def standard_schema():
    return SCHEMAS["standard"]


class TestStandardSchema:
    """Tests for standard schema (separate node + edge tables)."""
    
    @pytest.fixture(autouse=True)
    def setup(self, standard_schema, verify_clickgraph_running):
        self.config = standard_schema
    
    def test_single_edge_outgoing(self):
        """Test: Single edge type, outgoing direction."""
        result = query_clickgraph(
            "MATCH (u:User)-[:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, neighbor.name LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
        assert len(result["results"]) > 0
    
    def test_single_edge_incoming(self):
        """Test: Single edge type, incoming direction."""
        result = query_clickgraph(
            "MATCH (u:User)<-[:FOLLOWS]-(follower:User) "
            "WHERE u.user_id = 2 "
            "RETURN follower.user_id, follower.name LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_single_edge_bidirectional(self):
        """Test: Single edge type, bidirectional."""
        result = query_clickgraph(
            "MATCH (u:User)-[:FOLLOWS]-(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, neighbor.name LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_multi_edge_type(self):
        """Test: Multiple edge types."""
        result = query_clickgraph(
            "MATCH (u:User)-[:FOLLOWS|AUTHORED]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN target LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_vlp_single_type(self):
        """Test: VLP with single edge type."""
        result = query_clickgraph(
            "MATCH (u:User)-[:FOLLOWS*1..2]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id, neighbor.name LIMIT 20",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
        # Verify no duplicates
        ids = [r["neighbor.user_id"] for r in result["results"]]
        assert len(ids) == len(set(ids)), "DISTINCT failed - duplicates found"
    
    def test_return_type_function(self):
        """Test: RETURN type(r) for edge type."""
        result = query_clickgraph(
            "MATCH (u:User)-[r]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN type(r), target LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_edge_properties(self):
        """Test: RETURN edge properties."""
        result = query_clickgraph(
            "MATCH (u:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN r.follow_date, neighbor.name LIMIT 5",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_graphrag_expansion(self):
        """Test: GraphRAG expansion."""
        result = query_graphrag(
            "MATCH (u:User) WHERE u.user_id = 1 RETURN u",
            expand={"depth": 1, "direction": "out", "edge_types": ["FOLLOWS"]},
            schema_name=self.config.schema_name
        )
        assert "nodes" in result, f"Expansion failed: {result.get('error', 'unknown')}"
        assert result["stats"]["node_count"] > 0


# ============================================================================
# SCHEMA 2: FK-edge (FK column as relationship)
# ============================================================================

@pytest.fixture(scope="class")
def fk_edge_schema():
    return SCHEMAS["fk_edge"]


class TestFKEdgeSchema:
    """Tests for FK-edge schema (FK column as relationship)."""
    
    @pytest.fixture(autouse=True)
    def setup(self, fk_edge_schema, verify_clickgraph_running):
        self.config = fk_edge_schema
    
    def test_single_edge_outgoing(self):
        """Test: Order PLACED_BY Customer."""
        result = query_clickgraph(
            "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) "
            "WHERE o.order_id = 1 "
            "RETURN o.order_id, c.name LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_single_edge_incoming(self):
        """Test: Customer has Orders."""
        result = query_clickgraph(
            "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) "
            "WHERE c.customer_id = 1 "
            "RETURN o.order_id, o.order_date LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_vlp_outgoing(self):
        """Test: VLP from Customer to Orders."""
        result = query_clickgraph(
            "MATCH (c:Customer)<-[:PLACED_BY*1..2]-(o:Order) "
            "WHERE c.customer_id = 1 "
            "RETURN DISTINCT o.order_id, o.total_amount LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_type_function(self):
        """Test: RETURN type(r)."""
        result = query_clickgraph(
            "MATCH (o:Order)-[r]->(c:Customer) "
            "WHERE o.order_id = 1 "
            "RETURN type(r), c.name LIMIT 5",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_graphrag_expansion(self):
        """Test: GraphRAG expansion."""
        result = query_graphrag(
            "MATCH (o:Order) WHERE o.order_id = 1 RETURN o",
            expand={"depth": 1, "direction": "out", "edge_types": ["PLACED_BY"]},
            schema_name=self.config.schema_name
        )
        assert "nodes" in result, f"Expansion failed: {result.get('error', 'unknown')}"


# ============================================================================
# SCHEMA 3: Denormalized (node properties in edge table)
# ============================================================================

@pytest.fixture(scope="class")
def denormalized_schema():
    return SCHEMAS["denormalized"]


class TestDenormalizedSchema:
    """Tests for denormalized schema (node properties in edge table)."""
    
    @pytest.fixture(autouse=True)
    def setup(self, denormalized_schema, verify_clickgraph_running):
        self.config = denormalized_schema
    
    def test_single_flight(self):
        """Test: Flight between airports."""
        result = query_clickgraph(
            "MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport) "
            "WHERE origin.code = 'JFK' "
            "RETURN dest.code, dest.city LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_bidirectional_flight(self):
        """Test: Bidirectional flight pattern."""
        result = query_clickgraph(
            "MATCH (a:Airport)-[:FLIGHT]-(b:Airport) "
            "WHERE a.code = 'JFK' "
            "RETURN DISTINCT b.code, b.city LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_vlp_flights(self):
        """Test: VLP for multi-hop flights."""
        result = query_clickgraph(
            "MATCH (a:Airport)-[:FLIGHT*1..2]->(dest:Airport) "
            "WHERE a.code = 'JFK' "
            "RETURN DISTINCT dest.code, dest.city LIMIT 20",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_edge_properties(self):
        """Test: RETURN flight properties."""
        result = query_clickgraph(
            "MATCH (origin:Airport)-[r:FLIGHT]->(dest:Airport) "
            "WHERE origin.code = 'JFK' "
            "RETURN r.carrier, r.flight_num, dest.code LIMIT 5",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_graphrag_expansion(self):
        """Test: GraphRAG expansion."""
        result = query_graphrag(
            "MATCH (a:Airport) WHERE a.code = 'JFK' RETURN a",
            expand={"depth": 1, "direction": "out", "edge_types": ["FLIGHT"]},
            schema_name=self.config.schema_name
        )
        assert "nodes" in result, f"Expansion failed: {result.get('error', 'unknown')}"


# ============================================================================
# SCHEMA 4: Polymorphic (single table, type_column)
# ============================================================================

@pytest.fixture(scope="class")
def polymorphic_schema():
    return SCHEMAS["polymorphic"]


class TestPolymorphicSchema:
    """Tests for polymorphic schema (single table with type_column)."""
    
    @pytest.fixture(autouse=True)
    def setup(self, polymorphic_schema, verify_clickgraph_running):
        self.config = polymorphic_schema
    
    def test_single_edge_type_follows(self):
        """Test: FOLLOWS edge type."""
        result = query_clickgraph(
            "MATCH (u:User)-[:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, neighbor.name LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_single_edge_type_likes(self):
        """Test: LIKES edge type."""
        result = query_clickgraph(
            "MATCH (u:User)-[:LIKES]->(p:Post) "
            "WHERE u.user_id = 1 "
            "RETURN p.post_id, p.content LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_multi_edge_type(self):
        """Test: Multiple edge types from same table."""
        result = query_clickgraph(
            "MATCH (u:User)-[:FOLLOWS|LIKES]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN target LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_vlp_polymorphic(self):
        """Test: VLP with polymorphic edges."""
        result = query_clickgraph(
            "MATCH (u:User)-[:FOLLOWS*1..2]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id, neighbor.name LIMIT 20",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_type_function(self):
        """Test: RETURN type(r) for polymorphic edges."""
        result = query_clickgraph(
            "MATCH (u:User)-[r]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN type(r), target LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
        # Verify different edge types are returned
        types = set(r["type(r)"] for r in result["results"])
        assert len(types) > 1, "Expected multiple edge types in polymorphic schema"
    
    def test_return_edge_properties(self):
        """Test: RETURN edge properties from polymorphic table."""
        result = query_clickgraph(
            "MATCH (u:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN r.created_at, neighbor.name LIMIT 5",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_graphrag_expansion(self):
        """Test: GraphRAG expansion."""
        result = query_graphrag(
            "MATCH (u:User) WHERE u.user_id = 1 RETURN u",
            expand={"depth": 1, "direction": "out", "edge_types": ["FOLLOWS"]},
            schema_name=self.config.schema_name
        )
        assert "nodes" in result, f"Expansion failed: {result.get('error', 'unknown')}"


# ============================================================================
# SCHEMA 5: Composite ID (multi-column node identity)
# ============================================================================

@pytest.fixture(scope="class")
def composite_id_schema():
    return SCHEMAS["composite_id"]


class TestCompositeIDSchema:
    """Tests for composite ID schema (multi-column node identity)."""
    
    @pytest.fixture(autouse=True)
    def setup(self, composite_id_schema, verify_clickgraph_running):
        self.config = composite_id_schema
    
    def test_single_edge_owns(self):
        """Test: Customer OWNS Account (single-to-composite key)."""
        result = query_clickgraph(
            "MATCH (c:Customer)-[:OWNS]->(a:Account) "
            "WHERE c.customer_id = 1 "
            "RETURN a.bank_id, a.account_number, a.balance LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_single_edge_transferred(self):
        """Test: Account TRANSFERRED to Account (composite-to-composite)."""
        result = query_clickgraph(
            "MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) "
            "WHERE a1.bank_id = 'B001' AND a1.account_number = 'ACC001' "
            "RETURN a2.bank_id, a2.account_number, a2.balance LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_vlp_transfers(self):
        """Test: VLP for multi-hop transfers."""
        result = query_clickgraph(
            "MATCH (a:Account)-[:TRANSFERRED*1..2]->(dest:Account) "
            "WHERE a.bank_id = 'B001' AND a.account_number = 'ACC001' "
            "RETURN DISTINCT dest.bank_id, dest.account_number LIMIT 20",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_edge_properties(self):
        """Test: RETURN edge properties with composite IDs."""
        result = query_clickgraph(
            "MATCH (a1:Account)-[r:TRANSFERRED]->(a2:Account) "
            "WHERE a1.bank_id = 'B001' "
            "RETURN r.amount, r.transfer_date, a2.bank_id LIMIT 5",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_graphrag_expansion(self):
        """Test: GraphRAG expansion with composite IDs."""
        result = query_graphrag(
            "MATCH (a:Account) WHERE a.bank_id = 'B001' AND a.account_number = 'ACC001' RETURN a",
            expand={"depth": 1, "direction": "out", "edge_types": ["TRANSFERRED"]},
            schema_name=self.config.schema_name
        )
        assert "nodes" in result, f"Expansion failed: {result.get('error', 'unknown')}"


# ============================================================================
# SCHEMA 6: Coupled Edges (multiple relationships from one table)
# ============================================================================

@pytest.fixture(scope="class")
def coupled_edges_schema():
    return SCHEMAS["coupled_edges"]


class TestCoupledEdgesSchema:
    """Tests for coupled edges schema (multiple relationships from one table).
    
    This schema demonstrates the Zeek network log pattern where:
    - dns_log table defines 2 edges: REQUESTED (IP->Domain) and RESOLVED_TO (Domain->ResolvedIP)
    - conn_log table defines 1 edge: ACCESSED (IP->IP)
    
    Key pattern: A single table row represents a chain of relationships.
    """
    
    @pytest.fixture(autouse=True)
    def setup(self, coupled_edges_schema, verify_clickgraph_running):
        self.config = coupled_edges_schema
    
    def test_dns_requested_edge(self):
        """Test: DNS REQUESTED edge (IP->Domain)."""
        result = query_clickgraph(
            "MATCH (ip:IP)-[:REQUESTED]->(domain:Domain) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN domain.name, ip.ip LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
        # 192.168.1.10 requested 3 domains
        assert len(result["results"]) >= 2
    
    def test_dns_resolved_to_edge(self):
        """Test: DNS RESOLVED_TO edge (Domain->ResolvedIP)."""
        result = query_clickgraph(
            "MATCH (domain:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP) "
            "WHERE domain.name = 'example.com' "
            "RETURN rip.ip LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_coupled_dns_path(self):
        """Test: Coupled path IP->Domain->ResolvedIP (both edges from dns_log)."""
        result = query_clickgraph(
            "MATCH (ip:IP)-[:REQUESTED]->(domain:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN domain.name, rip.ip LIMIT 20",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_conn_accessed_edge(self):
        """Test: Connection ACCESSED edge (IP->IP)."""
        result = query_clickgraph(
            "MATCH (src:IP)-[:ACCESSED]->(dest:IP) "
            "WHERE src.ip = '192.168.1.10' "
            "RETURN dest.ip LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_edge_properties_dns(self):
        """Test: RETURN edge properties from dns_log."""
        result = query_clickgraph(
            "MATCH (ip:IP)-[r:REQUESTED]->(domain:Domain) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN r.qtype, r.rcode, domain.name LIMIT 5",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_edge_properties_conn(self):
        """Test: RETURN edge properties from conn_log."""
        result = query_clickgraph(
            "MATCH (src:IP)-[r:ACCESSED]->(dest:IP) "
            "WHERE src.ip = '192.168.1.10' "
            "RETURN r.service, r.duration, dest.ip LIMIT 5",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_return_type_function(self):
        """Test: RETURN type(r) for coupled edges."""
        result = query_clickgraph(
            "MATCH (ip:IP)-[r]->(target) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN type(r), target LIMIT 10",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"
    
    def test_multi_edge_type(self):
        """Test: Multiple edge types from same source."""
        result = query_clickgraph(
            "MATCH (ip:IP)-[:REQUESTED|ACCESSED]->(target) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN target LIMIT 20",
            self.config.schema_name
        )
        assert "results" in result, f"Query failed: {result.get('error', 'unknown')}"


# ============================================================================
# PYTEST FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def verify_clickgraph_running():
    """Verify ClickGraph is running before tests."""
    try:
        response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=5)
        assert response.status_code == 200
    except Exception:
        pytest.fail(f"ClickGraph not running at {CLICKGRAPH_URL}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
