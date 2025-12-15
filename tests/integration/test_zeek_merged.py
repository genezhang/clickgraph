#!/usr/bin/env python3
"""
Integration tests for Zeek Merged schema - Cross-Table Correlation Patterns.

Tests the merged zeek_dns_log + zeek_conn_log schema with all query variations
from GitHub issue #12.

Schema: zeek_merged_test (3 relationships across 2 tables)

From dns_log (2 coupled edges):
  - (src:IP)-[:REQUESTED]->(d:Domain)     "192.168.1.10 requested example.com"
  - (d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)  "example.com resolved_to 93.184.216.34"

From conn_log (1 edge):
  - (src:IP)-[:ACCESSED]->(dest:IP)       "192.168.1.10 accessed 93.184.216.34"

Test Patterns:
1. Single-table DNS queries (REQUESTED, RESOLVED_TO)
2. Single-table Connection queries (ACCESSED)
3. Coupled DNS path: (IP)-[:REQUESTED]->(Domain)-[:RESOLVED_TO]->(ResolvedIP)
4. Cross-table correlation: DNS lookup followed by connection to resolved IP
5. Multi-hop same-table patterns (EdgeToEdge)

Query Variations from Issue #12:
- Multi-path comma pattern: MATCH (a)-[]->(b), (a)-[]->(c)
- Sequential same-node: MATCH (a)-[]->(b) MATCH (a)-[]->(c)
- WITH...MATCH correlation: WITH src MATCH (src)-[]->(dest)
- Predicate correlation: WHERE srcip1.ip = srcip2.ip
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
            answers Array(String),
            TTLs Array(UInt32)
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
    # Test data designed for correlation scenarios:
    # - 192.168.1.10 looks up example.com → resolves to ['93.184.216.34']
    # - 192.168.1.10 looks up malware.bad → resolves to ['10.0.0.99']
    # - 192.168.1.20 looks up google.com → resolves to ['142.250.80.46']
    # - 192.168.1.10 looks up cdn.example.com → resolves to ['93.184.216.34', '93.184.216.35'] (multiple IPs)
    # Use JSONEachRow format to handle arrays properly
    clickhouse_conn.command("""
        INSERT INTO test_zeek.dns_log FORMAT JSONEachRow
        {"ts":1700000001.0,"uid":"DNS001","orig_h":"192.168.1.10","orig_p":54321,"resp_h":"8.8.8.8","resp_p":53,"proto":"udp","query":"example.com","qtype_name":"A","rcode_name":"NOERROR","answers":["93.184.216.34"],"TTLs":[3600]}
        {"ts":1700000002.0,"uid":"DNS002","orig_h":"192.168.1.10","orig_p":54322,"resp_h":"8.8.8.8","resp_p":53,"proto":"udp","query":"malware.bad","qtype_name":"A","rcode_name":"NOERROR","answers":["10.0.0.99"],"TTLs":[3600]}
        {"ts":1700000003.0,"uid":"DNS003","orig_h":"192.168.1.20","orig_p":54323,"resp_h":"8.8.8.8","resp_p":53,"proto":"udp","query":"google.com","qtype_name":"A","rcode_name":"NOERROR","answers":["142.250.80.46"],"TTLs":[300]}
        {"ts":1700000004.0,"uid":"DNS004","orig_h":"192.168.1.10","orig_p":54324,"resp_h":"8.8.8.8","resp_p":53,"proto":"udp","query":"cdn.example.com","qtype_name":"A","rcode_name":"NOERROR","answers":["93.184.216.34","93.184.216.35"],"TTLs":[60,60]}
    """)
    
    # Insert connection log data
    # Test data with correlations to DNS lookups:
    # - 192.168.1.10 -> 93.184.216.34:443 (matches DNS lookup for example.com and cdn.example.com)
    # - 192.168.1.10 -> 10.0.0.99:80 (matches DNS lookup for malware.bad) ⚠️ Threat indicator!
    # - 192.168.1.20 -> 142.250.80.46:443 (matches DNS lookup for google.com)
    # - 93.184.216.34 -> 192.168.1.10:49001 (reverse connection, no DNS correlation)
    # - 192.168.1.10 -> 192.168.1.20:22 (lateral movement, no DNS)
    # Use JSONEachRow format to handle column order correctly
    clickhouse_conn.command("""
        INSERT INTO test_zeek.conn_log FORMAT JSONEachRow
        {"uid":"CONN001","ts":1700000010.0,"orig_h":"192.168.1.10","orig_p":49001,"resp_h":"93.184.216.34","resp_p":443,"proto":"tcp","service":"ssl","duration":2.5,"orig_bytes":1024,"resp_bytes":4096,"conn_state":"SF"}
        {"uid":"CONN002","ts":1700000011.0,"orig_h":"192.168.1.10","orig_p":49002,"resp_h":"10.0.0.99","resp_p":80,"proto":"tcp","service":"http","duration":0.5,"orig_bytes":512,"resp_bytes":256,"conn_state":"SF"}
        {"uid":"CONN003","ts":1700000012.0,"orig_h":"192.168.1.20","orig_p":49003,"resp_h":"142.250.80.46","resp_p":443,"proto":"tcp","service":"ssl","duration":3.0,"orig_bytes":2048,"resp_bytes":8192,"conn_state":"SF"}
        {"uid":"CONN004","ts":1700000013.0,"orig_h":"93.184.216.34","orig_p":443,"resp_h":"192.168.1.10","resp_p":49001,"proto":"tcp","service":"ssl","duration":0.1,"orig_bytes":100,"resp_bytes":200,"conn_state":"SF"}
        {"uid":"CONN005","ts":1700000014.0,"orig_h":"192.168.1.10","orig_p":49004,"resp_h":"192.168.1.20","resp_p":22,"proto":"tcp","service":"ssh","duration":60.0,"orig_bytes":10000,"resp_bytes":20000,"conn_state":"SF"}
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
        if response.status_code != 200:
            print(f"ERROR {response.status_code}: {response.text}")
        return response.json()
    
    @staticmethod
    def sql_only(cypher: str, schema_name: str = "zeek_merged_test") -> str:
        """Get generated SQL without executing."""
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": cypher, "schema_name": schema_name, "sql_only": True}
        )
        return response.json().get("sql", response.json().get("generated_sql", ""))
    
    @staticmethod
    def get_data(result: dict) -> list:
        """Extract data from result, supporting both 'data' and 'results' keys."""
        return result.get("data") or result.get("results") or []


# ============================================================================
# Pattern 1: Single-Table DNS Queries (REQUESTED relationship)
# ============================================================================

class TestSingleTableRequested(TestZeekMergedHelpers):
    """Tests for REQUESTED relationship (dns_log table only)."""
    
    def test_count_dns_requests(self, setup_zeek_merged):
        """Count total DNS requests."""
        result = self.query("MATCH ()-[r:REQUESTED]->() RETURN count(*) as total")
        assert self.get_data(result), f"Query failed: {result}"
        assert self.get_data(result)[0]["total"] == 4
    
    def test_dns_requests_from_ip(self, setup_zeek_merged):
        """Find all DNS requests from a specific IP."""
        cypher = """
        MATCH (src:IP)-[r:REQUESTED]->(d:Domain) 
        WHERE src.ip = '192.168.1.10'
        RETURN src.ip, d.name, r.qtype
        ORDER BY r.timestamp
        """
        result = self.query(cypher)
        assert self.get_data(result), f"Query failed: {result}"
        # 192.168.1.10 made 3 DNS requests
        assert len(self.get_data(result)) == 3
        domains = [row["d.name"] for row in self.get_data(result)]
        assert "example.com" in domains
        assert "malware.bad" in domains
        assert "cdn.example.com" in domains
    
    def test_dns_requests_with_answers(self, setup_zeek_merged):
        """Query DNS requests with resolved answers array."""
        cypher = """
        MATCH (src:IP)-[r:REQUESTED]->(d:Domain)
        WHERE d.name = 'cdn.example.com'
        RETURN src.ip, d.name, r.answers
        """
        result = self.query(cypher)
        assert self.get_data(result), f"Query failed: {result}"
        row = self.get_data(result)[0]
        assert row["src.ip"] == "192.168.1.10"
        # answers should be array with 2 IPs
        answers = row.get("r.answers") or row.get("answers")
        assert len(answers) == 2
    
    def test_dns_sql_structure(self, setup_zeek_merged):
        """Verify DNS query uses single table without JOINs."""
        sql = self.sql_only("MATCH (s:IP)-[:REQUESTED]->(d:Domain) RETURN s.ip, d.name")
        # Should use dns_log table
        assert "dns_log" in sql
        # Should NOT have JOINs (single table denormalized)
        assert "JOIN" not in sql or "ARRAY JOIN" in sql


# ============================================================================
# Pattern 2: Coupled DNS Path (REQUESTED + RESOLVED_TO in same table)
# ============================================================================

class TestCoupledDNSPath(TestZeekMergedHelpers):
    """
    Tests for coupled DNS path: (IP)-[:REQUESTED]->(Domain)-[:RESOLVED_TO]->(ResolvedIP)
    Both relationships come from the same dns_log table.
    """
    
    def test_full_dns_path(self, setup_zeek_merged):
        """
        Query the full DNS resolution path.
        
        Pattern: src looked up domain which resolved to IPs
        """
        cypher = """
        MATCH (src:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
        WHERE src.ip = '192.168.1.10'
        RETURN src.ip, d.name, rip.ip
        ORDER BY d.name
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should see all DNS lookups from 192.168.1.10 with their resolved IPs
    
    def test_dns_path_for_specific_domain(self, setup_zeek_merged):
        """Query DNS path for a specific domain."""
        cypher = """
        MATCH (src:IP)-[req:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
        WHERE d.name = 'example.com'
        RETURN src.ip, d.name, rip.ip, req.timestamp
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
    
    def test_dns_path_sql_structure(self, setup_zeek_merged):
        """Verify coupled DNS path uses same table (CoupledSameRow strategy)."""
        cypher = """
        MATCH (src:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
        RETURN src.ip, d.name, rip.ip
        """
        sql = self.sql_only(cypher)
        # Should use dns_log table
        assert "dns_log" in sql
        # Coupled edges in same table shouldn't need explicit JOIN between them


# ============================================================================
# Pattern 3: Single-Table Connection Queries (ACCESSED relationship)
# ============================================================================

class TestSingleTableAccessed(TestZeekMergedHelpers):
    """Tests for ACCESSED relationship (conn_log table only)."""
    
    def test_count_connections(self, setup_zeek_merged):
        """Count total connections."""
        result = self.query("MATCH ()-[r:ACCESSED]->() RETURN count(*) as total")
        assert self.get_data(result), f"Query failed: {result}"
        assert self.get_data(result)[0]["total"] == 5
    
    def test_connections_from_ip(self, setup_zeek_merged):
        """Find all connections from a specific IP."""
        cypher = """
        MATCH (src:IP)-[r:ACCESSED]->(dst:IP) 
        WHERE src.ip = '192.168.1.10'
        RETURN src.ip, dst.ip, r.service
        ORDER BY r.timestamp
        """
        result = self.query(cypher)
        assert self.get_data(result), f"Query failed: {result}"
        # 192.168.1.10 made 3 outbound connections
        assert len(self.get_data(result)) == 3
        destinations = [row["dst.ip"] for row in self.get_data(result)]
        assert "93.184.216.34" in destinations
        assert "10.0.0.99" in destinations
        assert "192.168.1.20" in destinations
    
    def test_connections_with_service_filter(self, setup_zeek_merged):
        """Filter connections by service type."""
        cypher = """
        MATCH (src:IP)-[r:ACCESSED]->(dst:IP)
        WHERE r.service = 'ssl'
        RETURN src.ip, dst.ip, r.service, r.duration
        ORDER BY r.timestamp
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should return SSL connections
        for row in data:
            assert row["r.service"] == "ssl"
    
    def test_conn_sql_structure(self, setup_zeek_merged):
        """Verify connection query uses single table without JOINs."""
        sql = self.sql_only("MATCH (s:IP)-[:ACCESSED]->(d:IP) RETURN s.ip, d.ip")
        # Should use conn_log table
        assert "conn_log" in sql
        # Should NOT have JOINs (single table denormalized)
        assert "JOIN" not in sql or "ARRAY JOIN" in sql


# ============================================================================
# Pattern 4: Multi-Hop Same Table (EdgeToEdge within conn_log)
# ============================================================================

class TestMultiHopConnections(TestZeekMergedHelpers):
    """
    Tests for multi-hop patterns within the same table.
    Pattern: A accesses B, B accesses C (EdgeToEdge join strategy)
    """
    
    def test_two_hop_connections(self, setup_zeek_merged):
        """
        Find two-hop connection chains: A -> B -> C
        
        In our data:
        - 192.168.1.10 -> 93.184.216.34 (CONN001)
        - 93.184.216.34 -> 192.168.1.10 (CONN004)
        - So chain: 192.168.1.10 -> 93.184.216.34 -> 192.168.1.10 (loop back!)
        
        Also:
        - 192.168.1.10 -> 192.168.1.20 (CONN005)
        - 192.168.1.20 -> 142.250.80.46 (CONN003)
        - So chain: 192.168.1.10 -> 192.168.1.20 -> 142.250.80.46
        """
        cypher = """
        MATCH (a:IP)-[r1:ACCESSED]->(b:IP)-[r2:ACCESSED]->(c:IP)
        RETURN a.ip, b.ip, c.ip, r1.service as svc1, r2.service as svc2
        ORDER BY a.ip
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should find at least one two-hop chain
        assert len(data) >= 1
    
    def test_two_hop_with_filter(self, setup_zeek_merged):
        """Find two-hop chains starting from specific IP."""
        cypher = """
        MATCH (a:IP)-[r1:ACCESSED]->(b:IP)-[r2:ACCESSED]->(c:IP)
        WHERE a.ip = '192.168.1.10'
        RETURN a.ip, b.ip, c.ip
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # All results should start from 192.168.1.10
        for row in data:
            assert row["a.ip"] == "192.168.1.10"
    
    def test_two_hop_sql_uses_join(self, setup_zeek_merged):
        """Verify two-hop query generates proper JOIN for EdgeToEdge."""
        cypher = """
        MATCH (a:IP)-[r1:ACCESSED]->(b:IP)-[r2:ACCESSED]->(c:IP)
        RETURN a.ip, b.ip, c.ip
        """
        sql = self.sql_only(cypher)
        
        # Should have JOIN for the second hop
        assert "JOIN" in sql
        # Should reference conn_log twice (aliased)
        conn_log_count = sql.lower().count("conn_log")
        assert conn_log_count >= 2, f"Expected conn_log at least twice, got {conn_log_count} in: {sql}"


# ============================================================================
# Pattern 5: Cross-Table Correlation (Issue #12 Patterns)
# DNS lookup followed by connection to resolved IP
# ============================================================================

class TestCrossTableCorrelation(TestZeekMergedHelpers):
    """
    Tests for cross-table correlation patterns from GitHub issue #12.
    
    Key threat detection pattern:
    - Client looks up "malware.bad" → resolves to 10.0.0.99
    - Client then connects to 10.0.0.99
    - Find this correlation!
    """
    
    # -------------------------------------------------------------------------
    # Variation 1: Multi-path comma pattern (two paths in same MATCH)
    # -------------------------------------------------------------------------
    
    def test_comma_pattern_cross_table(self, setup_zeek_merged):
        """
        Multi-path comma pattern: Find DNS lookups AND connections from same source.
        
        Cypher: MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), 
                      (srcip)-[:ACCESSED]->(dest:IP)
                RETURN ...
        
        This should correlate the same srcip across dns_log and conn_log.
        """
        cypher = """
        MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), 
              (srcip)-[:ACCESSED]->(dest:IP)
        WHERE srcip.ip = '192.168.1.10'
        RETURN DISTINCT srcip.ip, d.name, dest.ip
        ORDER BY d.name
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # srcip.ip should always be 192.168.1.10
        for row in data:
            assert row["srcip.ip"] == "192.168.1.10"
        # Verify we have cross-table results (both domains and connections)
        assert len(data) >= 2, "Should have at least 2 correlations"
    
    # -------------------------------------------------------------------------
    # Variation 2: Full path with comma (DNS path + connection)
    # -------------------------------------------------------------------------
    
    def test_comma_pattern_full_dns_path(self, setup_zeek_merged):
        """
        Full DNS path combined with connection in same MATCH.
        
        Pattern from issue #12:
        MATCH (srcip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(destips:ResolvedIP), 
              (srcip)-[:ACCESSED]->(destip:IP)
        WHERE ...
        """
        cypher = """
        MATCH (srcip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP), 
              (srcip)-[:ACCESSED]->(dest:IP)
        WHERE srcip.ip = '192.168.1.10'
        RETURN srcip.ip, d.name, rip.ip as resolved_ip, dest.ip as accessed_ip
        ORDER BY d.name
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should have correlations
        assert len(data) >= 1, "Should find at least one cross-table correlation"
    
    # -------------------------------------------------------------------------
    # Variation 3: Sequential MATCH (same node reused)
    # -------------------------------------------------------------------------
    
    def test_sequential_match_same_node(self, setup_zeek_merged):
        """
        Sequential MATCH with same node variable reused.
        
        MATCH (srcip:IP)-[:REQUESTED]->(d:Domain)
        MATCH (srcip)-[:ACCESSED]->(dest:IP)
        WHERE srcip.ip = '192.168.1.10'
        RETURN ...
        """
        cypher = """
        MATCH (srcip:IP)-[:REQUESTED]->(d:Domain)
        MATCH (srcip)-[:ACCESSED]->(dest:IP)
        WHERE srcip.ip = '192.168.1.10'
        RETURN DISTINCT srcip.ip, d.name, dest.ip
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should have correlations
        assert len(data) >= 1, "Should find at least one correlation"
    
    # -------------------------------------------------------------------------
    # Variation 4: WITH...MATCH correlation
    # -------------------------------------------------------------------------
    
    def test_with_match_correlation(self, setup_zeek_merged):
        """
        WITH...MATCH pattern for cross-table correlation.
        
        First MATCH: Find DNS requests (dns_log)
        WITH: Pass variables
        Second MATCH: Find connections (conn_log)
        WHERE: Correlate on source IP
        """
        cypher = """
        MATCH (src:IP)-[dns:REQUESTED]->(d:Domain)
        WITH src.ip as source_ip, d.name as domain
        MATCH (src2:IP)-[conn:ACCESSED]->(dest:IP)
        WHERE src2.ip = source_ip
        RETURN DISTINCT source_ip, domain, dest.ip as dest_ip
        ORDER BY source_ip, domain
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should find correlations
        assert len(data) > 0
    
    # -------------------------------------------------------------------------
    # Variation 5: Predicate-based correlation
    # -------------------------------------------------------------------------
    
    def test_predicate_correlation(self, setup_zeek_merged):
        """
        Predicate-based correlation using WHERE clause.
        
        Pattern from issue #12:
        MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(destips:ResolvedIP), 
              (srcip2:IP)-[:ACCESSED]->(destip:IP)
        WHERE srcip1.ip = srcip2.ip AND destip.ip IN destips
        """
        cypher = """
        MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), 
              (srcip2:IP)-[:ACCESSED]->(destip:IP)
        WHERE srcip1.ip = srcip2.ip
        RETURN DISTINCT srcip1.ip as source, d.name as domain, destip.ip as accessed
        ORDER BY source
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should find correlations
        assert len(data) >= 1, "Should find at least one correlation"
    
    # -------------------------------------------------------------------------
    # Variation 6: Find DNS-then-connect to resolved IP (full threat pattern)
    # This is the key use case from issue #12
    # -------------------------------------------------------------------------
    
    def test_dns_then_connect_to_resolved_ip(self, setup_zeek_merged):
        """
        Find cases where IP looked up a domain and connected to its resolved IP.
        
        This is the KEY threat detection pattern from issue #12:
        - Client looks up "malware.bad" → resolves to 10.0.0.99
        - Client then connects to 10.0.0.99
        
        Simplified version without array containment (IN operator).
        """
        cypher = """
        MATCH (src:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP), 
              (src)-[:ACCESSED]->(dest:IP)
        RETURN DISTINCT src.ip, d.name as domain, rip.ip as resolved, dest.ip as accessed
        ORDER BY domain
        """
        result = self.query(cypher)
        data = self.get_data(result)
        assert data, f"Query failed: {result}"
        # Should find cross-table correlations
        assert len(data) >= 1, "Should find at least one DNS-to-connection correlation"


# ============================================================================
# Pattern 6: SQL Generation Verification
# ============================================================================

class TestSQLGeneration(TestZeekMergedHelpers):
    """Tests to verify correct SQL generation for various patterns."""
    
    def test_single_table_dns_sql(self, setup_zeek_merged):
        """Verify single-table DNS query SQL."""
        sql = self.sql_only("""
            MATCH (src:IP)-[r:REQUESTED]->(d:Domain)
            WHERE src.ip = '192.168.1.10'
            RETURN src.ip, d.name, r.qtype
        """)
        assert "dns_log" in sql
        assert "192.168.1.10" in sql
    
    def test_single_table_conn_sql(self, setup_zeek_merged):
        """Verify single-table connection query SQL."""
        sql = self.sql_only("""
            MATCH (src:IP)-[r:ACCESSED]->(dst:IP)
            WHERE src.ip = '192.168.1.10'
            RETURN src.ip, dst.ip, r.service
        """)
        assert "conn_log" in sql
        assert "192.168.1.10" in sql
    
    def test_coupled_dns_path_sql(self, setup_zeek_merged):
        """Verify coupled DNS path SQL (same table, 2 edges)."""
        sql = self.sql_only("""
            MATCH (src:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
            RETURN src.ip, d.name, rip.ip
        """)
        assert "dns_log" in sql
        # Both REQUESTED and RESOLVED_TO come from same table
    
    def test_multi_hop_conn_sql(self, setup_zeek_merged):
        """Verify multi-hop connection SQL (EdgeToEdge)."""
        sql = self.sql_only("""
            MATCH (a:IP)-[:ACCESSED]->(b:IP)-[:ACCESSED]->(c:IP)
            RETURN a.ip, b.ip, c.ip
        """)
        assert "conn_log" in sql
        assert "JOIN" in sql  # Should have JOIN for second hop


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
