#!/usr/bin/env python3
"""
Test SQL generation across different schema variations.

Schema variations tested:
1. Standard - Separate node + edge tables
2. FK-edge - FK column as relationship
3. Denormalized - Node properties embedded in edge table
4. Polymorphic - Single table with type_column discriminator
5. Composite ID - Multi-column node identity
6. Coupled Edges - Multiple relationships from one table

All tests use sql_only mode since the schema-variation databases
(db_standard, db_fk_edge, etc.) may not exist in the test ClickHouse.
"""

import pytest
import requests
from typing import Dict, Any

CLICKGRAPH_URL = "http://localhost:8080"


class SchemaTestConfig:
    """Configuration for a schema variation."""
    def __init__(self, name: str, schema_name: str):
        self.name = name
        self.schema_name = schema_name


# Schema configurations
SCHEMAS = {
    "standard": SchemaTestConfig(
        name="Standard (separate node+edge tables)",
        schema_name="standard"
    ),
    "fk_edge": SchemaTestConfig(
        name="FK-edge (FK column as relationship)",
        schema_name="fk_edge"
    ),
    "denormalized": SchemaTestConfig(
        name="Denormalized (node props in edge table)",
        schema_name="denormalized_flights"
    ),
    "polymorphic": SchemaTestConfig(
        name="Polymorphic (single table, type_column)",
        schema_name="polymorphic"
    ),
    "composite_id": SchemaTestConfig(
        name="Composite ID (multi-column node ID)",
        schema_name="composite_id"
    ),
    "coupled_edges": SchemaTestConfig(
        name="Coupled Edges (multiple relationships from one table)",
        schema_name="zeek_merged_test"
    ),
}


def query_sql_only(cypher: str, schema_name: str) -> Dict[str, Any]:
    """Generate SQL for a Cypher query without executing it."""
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": cypher, "schema_name": schema_name, "sql_only": True},
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    if response.status_code != 200:
        return {"error": response.text, "status_code": response.status_code}
    return response.json()


def assert_sql_generated(result: Dict, *keywords: str):
    """Assert that SQL was generated and optionally contains keywords."""
    assert "error" not in result, f"Query failed: {result.get('error', 'unknown')}"
    sql = result.get("generated_sql", "")
    assert sql and "SELECT" in sql, f"No valid SQL generated: {sql[:200]}"
    for kw in keywords:
        assert kw in sql, f"Expected '{kw}' in SQL:\n{sql[:500]}"


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
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, neighbor.name LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "user_follows", "full_name")

    def test_single_edge_incoming(self):
        result = query_sql_only(
            "MATCH (u:User)<-[:FOLLOWS]-(follower:User) "
            "WHERE u.user_id = 2 "
            "RETURN follower.user_id, follower.name LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "user_follows", "full_name")

    def test_single_edge_bidirectional(self):
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS]-(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, neighbor.name LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "UNION ALL")

    def test_multi_edge_type(self):
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS|AUTHORED]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN target LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "UNION ALL")

    def test_vlp_single_type(self):
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS*1..2]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id, neighbor.name LIMIT 20",
            self.config.schema_name
        )
        assert_sql_generated(result, "RECURSIVE", "user_follows")

    def test_return_type_function(self):
        result = query_sql_only(
            "MATCH (u:User)-[r]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN type(r), target LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result)

    def test_return_edge_properties(self):
        result = query_sql_only(
            "MATCH (u:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN r.follow_date, neighbor.name LIMIT 5",
            self.config.schema_name
        )
        assert_sql_generated(result, "follow_date", "full_name")

    def test_graphrag_expansion(self):
        """Multi-type VLP expansion across FOLLOWS and AUTHORED edges."""
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN target LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "vlp_multi_type")


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
        result = query_sql_only(
            "MATCH (o:Order)-[:PLACED_BY]->(c:Customer) "
            "WHERE o.order_id = 1 "
            "RETURN o.order_id, c.name LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "orders_fk", "customers_fk")

    def test_single_edge_incoming(self):
        result = query_sql_only(
            "MATCH (c:Customer)<-[:PLACED_BY]-(o:Order) "
            "WHERE c.customer_id = 1 "
            "RETURN o.order_id, o.order_date LIMIT 10",
            self.config.schema_name
        )
        # FK-edge: customer_id FK is in orders_fk, no JOIN to customers_fk needed
        assert_sql_generated(result, "orders_fk")

    def test_vlp_outgoing(self):
        result = query_sql_only(
            "MATCH (c:Customer)<-[:PLACED_BY*1..2]-(o:Order) "
            "WHERE c.customer_id = 1 "
            "RETURN DISTINCT o.order_id, o.total_amount LIMIT 10",
            self.config.schema_name
        )
        # PLACED_BY is Order->Customer (not transitive), VLP degenerates to single hop
        assert_sql_generated(result, "orders_fk")

    def test_return_type_function(self):
        result = query_sql_only(
            "MATCH (o:Order)-[r]->(c:Customer) "
            "WHERE o.order_id = 1 "
            "RETURN type(r), c.name LIMIT 5",
            self.config.schema_name
        )
        assert_sql_generated(result)

    def test_graphrag_expansion(self):
        """Multi-hop VLP expansion through FK-edge relationship.

        PLACED_BY is Order->Customer (non-transitive), so VLP degenerates
        to single hop — no recursive CTE, just direct table scan.
        """
        result = query_sql_only(
            "MATCH p = (c:Customer)<-[:PLACED_BY*1..2]-(o:Order) "
            "WHERE c.customer_id = 1 "
            "RETURN length(p), o.order_id LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "orders_fk")


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
        result = query_sql_only(
            "MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport) "
            "WHERE origin.code = 'JFK' "
            "RETURN dest.code, dest.city LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "flights")

    def test_bidirectional_flight(self):
        result = query_sql_only(
            "MATCH (a:Airport)-[:FLIGHT]-(b:Airport) "
            "WHERE a.code = 'JFK' "
            "RETURN DISTINCT b.code, b.city LIMIT 10",
            self.config.schema_name
        )
        # Bidirectional with DISTINCT uses UNION DISTINCT
        assert_sql_generated(result, "UNION")

    def test_vlp_flights(self):
        result = query_sql_only(
            "MATCH (a:Airport)-[:FLIGHT*1..2]->(dest:Airport) "
            "WHERE a.code = 'JFK' "
            "RETURN DISTINCT dest.code, dest.city LIMIT 20",
            self.config.schema_name
        )
        # VLP CTE is defined separately, outer query references vlp alias
        assert_sql_generated(result, "vlp_")

    def test_return_edge_properties(self):
        result = query_sql_only(
            "MATCH (origin:Airport)-[r:FLIGHT]->(dest:Airport) "
            "WHERE origin.code = 'JFK' "
            "RETURN r.carrier, r.flight_num, dest.code LIMIT 5",
            self.config.schema_name
        )
        assert_sql_generated(result, "carrier", "flight_number")

    def test_graphrag_expansion(self):
        """VLP expansion with path variable on denormalized flights."""
        result = query_sql_only(
            "MATCH p = (a:Airport)-[:FLIGHT*1..2]->(dest:Airport) "
            "WHERE a.code = 'JFK' "
            "RETURN length(p), dest.code LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "vlp_")


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
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN neighbor.user_id, neighbor.name LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "interactions", "'FOLLOWS'")

    def test_single_edge_type_likes(self):
        result = query_sql_only(
            "MATCH (u:User)-[:LIKES]->(p:Post) "
            "WHERE u.user_id = 1 "
            "RETURN p.post_id, p.content LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "interactions", "'LIKES'")

    def test_multi_edge_type(self):
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS|LIKES]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN target LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "interactions")

    def test_vlp_polymorphic(self):
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS*1..2]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN DISTINCT neighbor.user_id, neighbor.name LIMIT 20",
            self.config.schema_name
        )
        assert_sql_generated(result, "RECURSIVE", "interactions", "'FOLLOWS'")

    def test_return_type_function(self):
        result = query_sql_only(
            "MATCH (u:User)-[r]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN type(r), target LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result)

    def test_return_edge_properties(self):
        result = query_sql_only(
            "MATCH (u:User)-[r:FOLLOWS]->(neighbor:User) "
            "WHERE u.user_id = 1 "
            "RETURN r.created_at, neighbor.name LIMIT 5",
            self.config.schema_name
        )
        assert_sql_generated(result, "interactions")

    def test_graphrag_expansion(self):
        """Multi-type VLP expansion across FOLLOWS and LIKES edges."""
        result = query_sql_only(
            "MATCH (u:User)-[:FOLLOWS|LIKES*1..2]->(target) "
            "WHERE u.user_id = 1 "
            "RETURN target LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "vlp_multi_type")


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
        result = query_sql_only(
            "MATCH (c:Customer)-[:OWNS]->(a:Account) "
            "WHERE c.customer_id = 1 "
            "RETURN a.bank_id, a.account_number, a.balance LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "account_ownership", "accounts")

    def test_single_edge_transferred(self):
        result = query_sql_only(
            "MATCH (a1:Account)-[:TRANSFERRED]->(a2:Account) "
            "WHERE a1.bank_id = 'B001' AND a1.account_number = 'ACC001' "
            "RETURN a2.bank_id, a2.account_number, a2.balance LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "transfers", "accounts")

    def test_vlp_transfers(self):
        result = query_sql_only(
            "MATCH (a:Account)-[:TRANSFERRED*1..2]->(dest:Account) "
            "WHERE a.bank_id = 'B001' AND a.account_number = 'ACC001' "
            "RETURN DISTINCT dest.bank_id, dest.account_number LIMIT 20",
            self.config.schema_name
        )
        assert_sql_generated(result, "RECURSIVE", "transfers")

    def test_return_edge_properties(self):
        result = query_sql_only(
            "MATCH (a1:Account)-[r:TRANSFERRED]->(a2:Account) "
            "WHERE a1.bank_id = 'B001' "
            "RETURN r.amount, r.transfer_date, a2.bank_id LIMIT 5",
            self.config.schema_name
        )
        assert_sql_generated(result, "amount", "transfer_date")

    def test_graphrag_expansion(self):
        """Multi-hop VLP expansion with composite ID transfers."""
        result = query_sql_only(
            "MATCH p = (a:Account)-[:TRANSFERRED*1..2]->(dest:Account) "
            "WHERE a.bank_id = 'B001' AND a.account_number = 'ACC001' "
            "RETURN length(p), dest.bank_id, dest.account_number LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "vlp_")


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
    """

    @pytest.fixture(autouse=True)
    def setup(self, coupled_edges_schema, verify_clickgraph_running):
        self.config = coupled_edges_schema

    def test_dns_requested_edge(self):
        result = query_sql_only(
            "MATCH (ip:IP)-[:REQUESTED]->(domain:Domain) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN domain.name, ip.ip LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "dns_log")

    def test_dns_resolved_to_edge(self):
        result = query_sql_only(
            "MATCH (domain:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP) "
            "WHERE domain.name = 'example.com' "
            "RETURN rip.ip LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "dns_log")

    def test_coupled_dns_path(self):
        result = query_sql_only(
            "MATCH (ip:IP)-[:REQUESTED]->(domain:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN domain.name, rip.ip LIMIT 20",
            self.config.schema_name
        )
        assert_sql_generated(result, "dns_log")

    def test_conn_accessed_edge(self):
        result = query_sql_only(
            "MATCH (src:IP)-[:ACCESSED]->(dest:IP) "
            "WHERE src.ip = '192.168.1.10' "
            "RETURN dest.ip LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result, "conn_log")

    def test_return_edge_properties_dns(self):
        result = query_sql_only(
            "MATCH (ip:IP)-[r:REQUESTED]->(domain:Domain) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN r.qtype, r.rcode, domain.name LIMIT 5",
            self.config.schema_name
        )
        assert_sql_generated(result, "qtype_name", "rcode_name")

    def test_return_edge_properties_conn(self):
        result = query_sql_only(
            "MATCH (src:IP)-[r:ACCESSED]->(dest:IP) "
            "WHERE src.ip = '192.168.1.10' "
            "RETURN r.service, r.duration, dest.ip LIMIT 5",
            self.config.schema_name
        )
        assert_sql_generated(result, "service", "duration")

    def test_return_type_function(self):
        result = query_sql_only(
            "MATCH (ip:IP)-[r]->(target) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN type(r), target LIMIT 10",
            self.config.schema_name
        )
        assert_sql_generated(result)

    def test_multi_edge_type(self):
        result = query_sql_only(
            "MATCH (ip:IP)-[:REQUESTED|ACCESSED]->(target) "
            "WHERE ip.ip = '192.168.1.10' "
            "RETURN target LIMIT 20",
            self.config.schema_name
        )
        assert_sql_generated(result, "UNION ALL")


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
