#!/usr/bin/env python3
"""
Integration tests for Zeek Merged schema - Cross-Table Correlation Patterns.

Tests three basic patterns to correlate across two log tables (dns_log + conn_log):

1. **Single-Table Denormalized**: MATCH on just dns_log or just conn_log
   - Tests SingleTableScan and EdgeToEdge join strategies

2. **Sequential WITH...MATCH (Cross-Table)**:
   MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain)
   WITH src, d
   MATCH (src2:IP)-[:CONNECTED_TO]->(dest:IP)
   WHERE src2.ip = src.ip
   - First MATCH uses dns_log
   - Second MATCH uses conn_log
   - WITH bridges the two (CartesianProduct or correlated subquery)

3. **Multi-Hop Same Table**:
   MATCH (a:IP)-[r1:CONNECTED_TO]->(b:IP)-[r2:CONNECTED_TO]->(c:IP)
   - Tests EdgeToEdge join strategy within conn_log
   
Schema: zeek_merged_test
Tables:
  - test_zeek.dns_log: IP -[:DNS_REQUESTED]-> Domain
  - test_zeek.conn_log: IP -[:CONNECTED_TO]-> IP
"""

import pytest
import requests
import json
from pathlib import Path

# Server endpoint
CLICKGRAPH_URL = "http://localhost:8080"
SCHEMA_PATH = Path(__file__).parent / "fixtures" / "schemas" / "zeek_merged_test.yaml"


@pytest.fixture(scope="module")
def setup_zeek_merged(clickhouse_conn):
    """Set up test database and schema for Zeek merged logs."""
    
    # Create test database
    clickhouse_conn.command("CREATE DATABASE IF NOT EXISTS test_zeek")
    
    # Create dns_log table
    clickhouse_conn.command("""
        CREATE TABLE IF NOT EXISTS test_zeek.dns_log (
            ts Float64,
            uid String,
            orig_h String,
            orig_p UInt16,
            resp_h String,
            resp_p UInt16,
            proto String,
            query String,
            qtype_name String,
            rcode_name String,
            answers Array(String)
        ) ENGINE = Memory
    """)
    
    # Create conn_log table
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
            conn_state String
        ) ENGINE = Memory
    """)
    
    # Insert DNS log data
    # Client 192.168.1.10 looks up example.com → resolves to 93.184.216.34
    # Client 192.168.1.10 looks up malware.bad → resolves to 10.0.0.99
    # Client 192.168.1.20 looks up google.com → resolves to 142.250.80.46
    clickhouse_conn.command("""
        INSERT INTO test_zeek.dns_log VALUES
        (1700000001.0, 'DNS001', '192.168.1.10', 54321, '8.8.8.8', 53, 'udp', 'example.com', 'A', 'NOERROR', ['93.184.216.34']),
        (1700000002.0, 'DNS002', '192.168.1.10', 54322, '8.8.8.8', 53, 'udp', 'malware.bad', 'A', 'NOERROR', ['10.0.0.99']),
        (1700000003.0, 'DNS003', '192.168.1.20', 54323, '8.8.8.8', 53, 'udp', 'google.com', 'A', 'NOERROR', ['142.250.80.46']),
        (1700000004.0, 'DNS004', '192.168.1.10', 54324, '8.8.8.8', 53, 'udp', 'cdn.example.com', 'CNAME', 'NOERROR', ['example.com'])
    """)
    
    # Insert connection log data
    # 192.168.1.10 connects to 93.184.216.34 (the resolved IP of example.com)
    # 192.168.1.10 connects to 10.0.0.99 (the resolved IP of malware.bad)
    # 192.168.1.20 connects to 142.250.80.46 (the resolved IP of google.com)
    # 93.184.216.34 connects back to 192.168.1.10 (bidirectional)
    clickhouse_conn.command("""
        INSERT INTO test_zeek.conn_log VALUES
        (1700000010.0, 'CONN001', '192.168.1.10', 49001, '93.184.216.34', 443, 'tcp', 'ssl', 2.5, 1024, 4096, 'SF'),
        (1700000011.0, 'CONN002', '192.168.1.10', 49002, '10.0.0.99', 80, 'tcp', 'http', 0.5, 512, 256, 'SF'),
        (1700000012.0, 'CONN003', '192.168.1.20', 49003, '142.250.80.46', 443, 'tcp', 'ssl', 3.0, 2048, 8192, 'SF'),
        (1700000013.0, 'CONN004', '93.184.216.34', 443, '192.168.1.10', 49001, 'tcp', 'ssl', 0.1, 100, 200, 'SF'),
        (1700000014.0, 'CONN005', '192.168.1.10', 49004, '192.168.1.20', 22, 'tcp', 'ssh', 60.0, 10000, 20000, 'SF')
    """)
    
    # Load schema into ClickGraph
    with open(SCHEMA_PATH, 'r') as f:
        schema_yaml = f.read()
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/schemas/load",
        json={"schema_name": "zeek_merged_test", "config_content": schema_yaml}
    )
    assert response.status_code == 200, f"Failed to load schema: {response.text}"
    
    yield
    
    # Cleanup
    clickhouse_conn.command("DROP TABLE IF EXISTS test_zeek.dns_log")
    clickhouse_conn.command("DROP TABLE IF EXISTS test_zeek.conn_log")
    clickhouse_conn.command("DROP DATABASE IF EXISTS test_zeek")


class TestZeekMergedHelpers:
    """Helper methods for test classes."""
    
    @staticmethod
    def query(cypher: str, schema_name: str = "zeek_merged_test") -> dict:
        """Execute a Cypher query and return the result."""
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": cypher, "schema_name": schema_name}
        )
        return response.json()
    
    @staticmethod
    def sql_only(cypher: str, schema_name: str = "zeek_merged_test") -> str:
        """Get generated SQL without executing."""
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": cypher, "schema_name": schema_name, "sql_only": True}
        )
        return response.json().get("sql", "")


# ============================================================================
# Pattern 1: Single-Table Denormalized Queries
# ============================================================================

class TestSingleTableDNS(TestZeekMergedHelpers):
    """Tests for DNS_REQUESTED relationship (dns_log table only)."""
    
    def test_count_dns_requests(self, setup_zeek_merged):
        """Count total DNS requests."""
        result = self.query("MATCH ()-[r:DNS_REQUESTED]->() RETURN count(*) as total")
        assert result.get("data"), f"Query failed: {result}"
        assert result["data"][0]["total"] == 4
    
    def test_dns_requests_from_ip(self, setup_zeek_merged):
        """Find all DNS requests from a specific IP."""
        cypher = """
        MATCH (src:IP)-[r:DNS_REQUESTED]->(d:Domain) 
        WHERE src.ip = '192.168.1.10'
        RETURN src.ip, d.name, r.qtype
        ORDER BY r.timestamp
        """
        result = self.query(cypher)
        assert result.get("data"), f"Query failed: {result}"
        # 192.168.1.10 made 3 DNS requests
        assert len(result["data"]) == 3
        domains = [row["d.name"] for row in result["data"]]
        assert "example.com" in domains
        assert "malware.bad" in domains
    
    def test_dns_sql_no_join(self, setup_zeek_merged):
        """Verify DNS query uses single table without JOINs."""
        sql = self.sql_only("MATCH (s:IP)-[:DNS_REQUESTED]->(d:Domain) RETURN s.ip, d.name")
        # Should use dns_log table
        assert "dns_log" in sql
        # Should NOT have JOINs (single table denormalized)
        assert "JOIN" not in sql or "ARRAY JOIN" in sql


class TestSingleTableConn(TestZeekMergedHelpers):
    """Tests for CONNECTED_TO relationship (conn_log table only)."""
    
    def test_count_connections(self, setup_zeek_merged):
        """Count total connections."""
        result = self.query("MATCH ()-[r:CONNECTED_TO]->() RETURN count(*) as total")
        assert result.get("data"), f"Query failed: {result}"
        assert result["data"][0]["total"] == 5
    
    def test_connections_from_ip(self, setup_zeek_merged):
        """Find all connections from a specific IP."""
        cypher = """
        MATCH (src:IP)-[r:CONNECTED_TO]->(dst:IP) 
        WHERE src.ip = '192.168.1.10'
        RETURN src.ip, dst.ip, r.service
        ORDER BY r.timestamp
        """
        result = self.query(cypher)
        assert result.get("data"), f"Query failed: {result}"
        # 192.168.1.10 made 3 outbound connections
        assert len(result["data"]) == 3
        destinations = [row["dst.ip"] for row in result["data"]]
        assert "93.184.216.34" in destinations
        assert "10.0.0.99" in destinations
    
    def test_conn_sql_no_join(self, setup_zeek_merged):
        """Verify connection query uses single table without JOINs."""
        sql = self.sql_only("MATCH (s:IP)-[:CONNECTED_TO]->(d:IP) RETURN s.ip, d.ip")
        # Should use conn_log table
        assert "conn_log" in sql
        # Should NOT have JOINs (single table denormalized)
        assert "JOIN" not in sql or "ARRAY JOIN" in sql


# ============================================================================
# Pattern 2: Cross-Table Correlation with WITH...MATCH
# ============================================================================

class TestCrossTableCorrelation(TestZeekMergedHelpers):
    """
    Tests for cross-table correlation patterns.
    
    Key pattern: DNS lookup followed by connection to resolved IP.
    
    MATCH (src:IP)-[dns:DNS_REQUESTED]->(d:Domain)
    WITH src, d, dns
    MATCH (src2:IP)-[conn:CONNECTED_TO]->(dest:IP)
    WHERE src2.ip = src.ip
    RETURN src.ip, d.name, dest.ip
    """
    
    def test_dns_then_connect_basic(self, setup_zeek_merged):
        """
        Find IPs that performed DNS lookup and then made connections.
        
        First MATCH: Find DNS requests (dns_log)
        WITH: Pass variables
        Second MATCH: Find connections (conn_log)
        WHERE: Correlate on source IP
        """
        cypher = """
        MATCH (src:IP)-[dns:DNS_REQUESTED]->(d:Domain)
        WITH src.ip as source_ip, d.name as domain
        MATCH (src2:IP)-[conn:CONNECTED_TO]->(dest:IP)
        WHERE src2.ip = source_ip
        RETURN DISTINCT source_ip, domain, dest.ip as dest_ip
        ORDER BY source_ip, domain
        """
        result = self.query(cypher)
        assert result.get("data"), f"Query failed: {result}"
        # Should find correlations
        assert len(result["data"]) > 0
        
    def test_dns_then_connect_to_resolved_ip(self, setup_zeek_merged):
        """
        Find cases where IP looked up a domain and connected to its resolved IP.
        
        This is the key threat detection pattern:
        - Client looks up "malware.bad" → resolves to 10.0.0.99
        - Client then connects to 10.0.0.99
        
        Note: This requires checking if dest.ip is in dns.answers array.
        """
        cypher = """
        MATCH (src:IP)-[dns:DNS_REQUESTED]->(d:Domain)
        WITH src.ip as source_ip, d.name as domain, dns.answers as resolved_ips
        MATCH (src2:IP)-[conn:CONNECTED_TO]->(dest:IP)
        WHERE src2.ip = source_ip
        RETURN DISTINCT source_ip, domain, dest.ip as dest_ip, resolved_ips
        ORDER BY source_ip
        """
        result = self.query(cypher)
        assert result.get("data"), f"Query failed: {result}"
        # Verify we got results with both DNS and connection data
        for row in result["data"]:
            assert "source_ip" in row
            assert "domain" in row
            assert "dest_ip" in row
    
    def test_cross_table_sql_structure(self, setup_zeek_merged):
        """Verify cross-table query generates correct SQL structure."""
        cypher = """
        MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain)
        WITH src.ip as source_ip
        MATCH (src2:IP)-[:CONNECTED_TO]->(dest:IP)
        WHERE src2.ip = source_ip
        RETURN source_ip, dest.ip
        """
        sql = self.sql_only(cypher)
        
        # Should reference both tables
        assert "dns_log" in sql
        assert "conn_log" in sql
        
        # Should have some form of correlation (CTE, subquery, or CROSS JOIN with WHERE)
        has_correlation = (
            "WITH" in sql or      # CTE
            "IN (" in sql or      # Subquery
            "JOIN" in sql or      # JOIN
            "CROSS" in sql        # CROSS JOIN with WHERE
        )
        assert has_correlation, f"Expected correlation structure in SQL: {sql}"


# ============================================================================
# Pattern 3: Multi-Hop Same Table (EdgeToEdge)
# ============================================================================

class TestMultiHopSameTable(TestZeekMergedHelpers):
    """
    Tests for multi-hop patterns within the same table.
    
    Key pattern: A connects to B, B connects to C
    Uses EdgeToEdge join strategy.
    """
    
    def test_two_hop_connections(self, setup_zeek_merged):
        """
        Find two-hop connection chains: A -> B -> C
        
        In our data:
        - 192.168.1.10 -> 93.184.216.34 (CONN001)
        - 93.184.216.34 -> 192.168.1.10 (CONN004)
        - So chain: 192.168.1.10 -> 93.184.216.34 -> 192.168.1.10
        
        Also:
        - 192.168.1.10 -> 192.168.1.20 (CONN005)
        - 192.168.1.20 -> 142.250.80.46 (CONN003)
        - So chain: 192.168.1.10 -> 192.168.1.20 -> 142.250.80.46
        """
        cypher = """
        MATCH (a:IP)-[r1:CONNECTED_TO]->(b:IP)-[r2:CONNECTED_TO]->(c:IP)
        RETURN a.ip, b.ip, c.ip, r1.service as svc1, r2.service as svc2
        ORDER BY a.ip
        """
        result = self.query(cypher)
        assert result.get("data"), f"Query failed: {result}"
        # Should find the two-hop chains
        assert len(result["data"]) >= 1
    
    def test_two_hop_sql_uses_join(self, setup_zeek_merged):
        """Verify two-hop query generates proper JOIN for EdgeToEdge."""
        cypher = """
        MATCH (a:IP)-[r1:CONNECTED_TO]->(b:IP)-[r2:CONNECTED_TO]->(c:IP)
        RETURN a.ip, b.ip, c.ip
        """
        sql = self.sql_only(cypher)
        
        # Should have JOIN for the second hop
        assert "JOIN" in sql
        # Should reference conn_log twice (aliased)
        conn_log_count = sql.lower().count("conn_log")
        assert conn_log_count >= 2, f"Expected conn_log at least twice, got {conn_log_count} in: {sql}"
    
    def test_two_hop_with_filter(self, setup_zeek_merged):
        """Find two-hop chains with filtering."""
        cypher = """
        MATCH (a:IP)-[r1:CONNECTED_TO]->(b:IP)-[r2:CONNECTED_TO]->(c:IP)
        WHERE a.ip = '192.168.1.10'
        RETURN a.ip, b.ip, c.ip
        """
        result = self.query(cypher)
        assert result.get("data"), f"Query failed: {result}"
        # All results should start from 192.168.1.10
        for row in result["data"]:
            assert row["a.ip"] == "192.168.1.10"
    
    def test_three_hop_connections(self, setup_zeek_merged):
        """
        Find three-hop connection chains: A -> B -> C -> D
        
        May have fewer results depending on data connectivity.
        """
        cypher = """
        MATCH (a:IP)-[:CONNECTED_TO]->(b:IP)-[:CONNECTED_TO]->(c:IP)-[:CONNECTED_TO]->(d:IP)
        RETURN a.ip, b.ip, c.ip, d.ip
        """
        sql = self.sql_only(cypher)
        
        # Should have multiple JOINs for the three hops
        join_count = sql.upper().count("JOIN")
        # At least 2 JOINs for 3 hops (or CTEs)
        has_structure = join_count >= 2 or "WITH" in sql.upper()
        assert has_structure, f"Expected multi-hop structure in SQL: {sql}"


# ============================================================================
# Pattern 4: Mixed Patterns
# ============================================================================

class TestMixedPatterns(TestZeekMergedHelpers):
    """Tests for combinations of the basic patterns."""
    
    def test_dns_and_multi_hop_conn(self, setup_zeek_merged):
        """
        Complex pattern: DNS lookup, then trace two-hop connections.
        
        MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain)
        WITH src.ip as source_ip
        MATCH (a:IP)-[:CONNECTED_TO]->(b:IP)-[:CONNECTED_TO]->(c:IP)
        WHERE a.ip = source_ip
        RETURN source_ip, b.ip, c.ip
        """
        cypher = """
        MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain)
        WITH DISTINCT src.ip as source_ip
        MATCH (a:IP)-[:CONNECTED_TO]->(b:IP)-[:CONNECTED_TO]->(c:IP)
        WHERE a.ip = source_ip
        RETURN source_ip, b.ip as hop1, c.ip as hop2
        """
        result = self.query(cypher)
        assert result.get("data") is not None, f"Query failed: {result}"
        # May have results depending on data connectivity
    
    def test_aggregation_across_tables(self, setup_zeek_merged):
        """
        Count DNS lookups and connections per source IP.
        
        Note: This might need UNION or subquery correlation.
        """
        # First, get DNS counts
        dns_result = self.query("""
            MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain)
            RETURN src.ip, count(*) as dns_count
            ORDER BY src.ip
        """)
        assert dns_result.get("data"), f"DNS query failed: {dns_result}"
        
        # Then, get connection counts
        conn_result = self.query("""
            MATCH (src:IP)-[:CONNECTED_TO]->(dst:IP)
            RETURN src.ip, count(*) as conn_count
            ORDER BY src.ip
        """)
        assert conn_result.get("data"), f"Conn query failed: {conn_result}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
