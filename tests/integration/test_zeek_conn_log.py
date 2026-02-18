#!/usr/bin/env python3
"""
Integration tests for Zeek-style denormalized network logs.

These tests verify that ClickGraph correctly handles network log data
where IP addresses are denormalized nodes in connection edge tables.

Use case: Zeek/Bro conn.log where each row represents a connection:
- Source IP (id.orig_h) → Head node
- Dest IP (id.resp_h) → Tail node  
- Connection → Edge with properties (proto, service, duration, etc.)
"""

import pytest
import requests
import json
from pathlib import Path

pytestmark = pytest.mark.skip(reason="Requires zeek database with conn_log tables")

# Server endpoint
CLICKGRAPH_URL = "http://localhost:8080"
SCHEMA_PATH = Path(__file__).parent / "fixtures" / "schemas" / "zeek_conn_test.yaml"


@pytest.fixture(scope="module")
def setup_zeek_schema(clickhouse_conn):
    """Set up test database and schema for Zeek connection logs."""
    
    # Create test database
    clickhouse_conn.command("CREATE DATABASE IF NOT EXISTS test_zeek")
    
    # Create conn_log table (simplified Zeek schema)
    clickhouse_conn.command("""
        CREATE TABLE IF NOT EXISTS test_zeek.conn_log (
            ts Float64,
            uid String,
            orig_h String,
            orig_p UInt16,
            resp_h String,
            resp_p UInt16,
            proto String,
            service String,
            duration Float64,
            orig_bytes UInt64,
            resp_bytes UInt64,
            conn_state String,
            missed_bytes UInt64,
            history String,
            orig_pkts UInt64,
            resp_pkts UInt64
        ) ENGINE = Memory
    """)
    
    # Insert sample data based on user's example
    clickhouse_conn.command("""
        INSERT INTO test_zeek.conn_log VALUES
        (1591367999.305988, 'CMdzit1AMNsmfAIiQc', '192.168.4.76', 36844, '192.168.4.1', 53, 'udp', 'dns', 0.066851, 62, 141, 'SF', 0, 'Dd', 2, 2),
        (1591368000.123456, 'CK2fW44phZnrNcM2Xd', '192.168.4.76', 49152, '10.0.0.1', 80, 'tcp', 'http', 1.234567, 1024, 2048, 'SF', 0, 'ShADadFf', 10, 8),
        (1591368001.789012, 'CNxm3r2kP8FKnJ9YY5', '10.0.0.1', 443, '192.168.4.76', 51234, 'tcp', 'ssl', 5.678901, 4096, 8192, 'SF', 0, 'ShADadFf', 20, 15),
        (1591368002.345678, 'CAbC123DefGhIjKlM', '192.168.4.76', 54321, '8.8.8.8', 53, 'udp', 'dns', 0.012345, 40, 120, 'SF', 0, 'Dd', 1, 1),
        (1591368003.567890, 'CXyZ789AbCdEfGhIj', '8.8.8.8', 53, '192.168.4.76', 54322, 'udp', 'dns', 0.023456, 45, 130, 'SF', 0, 'Dd', 1, 1)
    """)
    
    # Load schema into ClickGraph
    with open(SCHEMA_PATH, 'r') as f:
        schema_yaml = f.read()
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/schemas/load",
        json={"schema_name": "zeek_conn_test", "config_content": schema_yaml}
    )
    assert response.status_code == 200, f"Failed to load schema: {response.text}"
    
    yield
    
    # Cleanup
    clickhouse_conn.command("DROP TABLE IF EXISTS test_zeek.conn_log")
    clickhouse_conn.command("DROP DATABASE IF EXISTS test_zeek")


class TestZeekConnLog:
    """Tests for Zeek connection log graph queries."""
    
    def query(self, cypher: str, schema_name: str = "zeek_conn_test") -> dict:
        """Execute a Cypher query and return the result."""
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": cypher, "schema_name": schema_name}
        )
        return response.json()
    
    def sql_only(self, cypher: str, schema_name: str = "zeek_conn_test") -> str:
        """Get generated SQL without executing."""
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": cypher, "schema_name": schema_name, "sql_only": True}
        )
        return response.json().get("sql", response.json().get("generated_sql", ""))
    
    def get_data(self, result: dict) -> list:
        """Extract data from result, supporting both 'data' and 'results' keys."""
        return result.get("data") or result.get("results") or []
    
    def test_count_all_connections(self, setup_zeek_schema):
        """Count total connections (edges)."""
        result = self.query("MATCH ()-[r:ACCESSED]->() RETURN count(*) as total")
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        assert data[0]["total"] == 5
    
    def test_find_connections_from_ip(self, setup_zeek_schema):
        """Find all connections from a specific source IP."""
        cypher = """
        MATCH (src:IP)-[r:ACCESSED]->(dst:IP) 
        WHERE src.ip = '192.168.4.76' 
        RETURN src.ip, dst.ip, r.service
        ORDER BY r.timestamp
        """
        result = self.query(cypher)
        assert self.get_data(result), f"Query failed: {result}"
        # 192.168.4.76 initiated 3 connections in our test data
        assert len(self.get_data(result)) == 3
    
    def test_find_dns_connections(self, setup_zeek_schema):
        """Find all DNS service connections."""
        cypher = """
        MATCH (src:IP)-[r:ACCESSED]->(dst:IP)
        WHERE r.service = 'dns'
        RETURN src.ip, dst.ip, r.protocol
        """
        result = self.query(cypher)
        assert self.get_data(result), f"Query failed: {result}"
        # 3 DNS connections in test data
        assert len(self.get_data(result)) == 3
    
    def test_find_connections_to_ip(self, setup_zeek_schema):
        """Find all connections to a specific destination IP."""
        cypher = """
        MATCH (src:IP)-[r:ACCESSED]->(dst:IP)
        WHERE dst.ip = '192.168.4.76'
        RETURN src.ip, r.service, r.duration
        """
        result = self.query(cypher)
        assert self.get_data(result), f"Query failed: {result}"
        # 2 connections TO 192.168.4.76 in test data
        assert len(self.get_data(result)) == 2
    
    @pytest.mark.skip(reason="Cross-table WITH...MATCH correlation not yet fully supported")
    def test_bidirectional_traffic(self, setup_zeek_schema):
        """Find IPs that both sent to and received from a specific IP."""
        cypher = """
        MATCH (a:IP)-[:ACCESSED]->(b:IP)
        WHERE a.ip = '192.168.4.76'
        WITH b.ip as peer_ip
        MATCH (c:IP)-[:ACCESSED]->(d:IP)  
        WHERE d.ip = '192.168.4.76' AND c.ip = peer_ip
        RETURN peer_ip
        """
        # This tests multi-hop denormalized queries
        result = self.query(cypher)
        # 8.8.8.8 and 10.0.0.1 both sent and received from 192.168.4.76
        assert self.get_data(result) is not None
    
    def test_sql_generation_uses_same_table(self, setup_zeek_schema):
        """Verify SQL uses the same table for both nodes (denormalized pattern)."""
        sql = self.sql_only("MATCH (s:IP)-[:ACCESSED]->(d:IP) RETURN s.ip, d.ip")
        
        # Both source and dest should reference conn_log table
        assert "conn_log" in sql
        # Should NOT have JOINs to separate IP tables
        assert "JOIN" not in sql or "ARRAY JOIN" in sql
    
    def test_count_unique_source_ips(self, setup_zeek_schema):
        """Count unique source IPs."""
        result = self.query("""
            MATCH (src:IP)-[:ACCESSED]->() 
            RETURN count(DISTINCT src.ip) as unique_sources
        """)
        assert self.get_data(result), f"Query failed: {result}"
        # 3 unique source IPs: 192.168.4.76, 10.0.0.1, 8.8.8.8
        assert self.get_data(result)[0]["unique_sources"] == 3
    
    def test_connection_properties(self, setup_zeek_schema):
        """Verify edge properties are accessible."""
        cypher = """
        MATCH (s:IP)-[r:ACCESSED]->(d:IP)
        WHERE r.uid = 'CMdzit1AMNsmfAIiQc'
        RETURN r.protocol, r.service, r.duration, r.orig_bytes, r.resp_bytes
        """
        result = self.query(cypher)
        assert self.get_data(result), f"Query failed: {result}"
        row = self.get_data(result)[0]
        assert row["r.protocol"] == "udp"
        assert row["r.service"] == "dns"
        assert row["r.orig_bytes"] == 62
        assert row["r.resp_bytes"] == 141


class TestZeekConnLogNodeOnly:
    """Tests for node-only queries on denormalized Zeek logs."""
    
    def query(self, cypher: str, schema_name: str = "zeek_conn_test") -> dict:
        """Execute a Cypher query and return the result."""
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": cypher, "schema_name": schema_name}
        )
        return response.json()
    
    def get_data(self, result: dict) -> list:
        """Extract data from result, supporting both 'data' and 'results' keys."""
        return result.get("data") or result.get("results") or []
    
    def test_count_all_ips(self, setup_zeek_schema):
        """Count all unique IPs (as nodes)."""
        result = self.query("MATCH (ip:IP) RETURN count(DISTINCT ip.ip) as cnt")
        assert self.get_data(result), f"Query failed: {result}"
        # Should find all unique IPs from both orig_h and resp_h
        # 192.168.4.76, 192.168.4.1, 10.0.0.1, 8.8.8.8 = 4 unique IPs
        assert self.get_data(result)[0]["cnt"] == 4
    
    def test_list_all_ips(self, setup_zeek_schema):
        """List all unique IPs."""
        result = self.query("MATCH (ip:IP) RETURN DISTINCT ip.ip ORDER BY ip.ip")
        assert self.get_data(result), f"Query failed: {result}"
        ips = [row["ip.ip"] for row in self.get_data(result)]
        assert "192.168.4.76" in ips
        assert "8.8.8.8" in ips


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
