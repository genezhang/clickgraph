# LDBC SNB Benchmark for ClickGraph

## Database Configuration

### ClickHouse LDBC Instance
- **Container**: `clickhouse-ldbc`
- **Port**: 18123 (not default 8123)
- **Credentials**: test_user/test_pass
- **Database**: ldbc
- **Schema**: `benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml`

### Starting ClickGraph Server for LDBC

```bash
cd /home/gz/clickgraph

export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="ldbc"
export GRAPH_CONFIG_PATH="benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml"
export RUST_LOG=warn

cargo run --bin clickgraph &> /tmp/clickgraph_ldbc.log &
```

## Running the Audit

```bash
python3 benchmarks/ldbc_snb/scripts/audit_sql_generation.py
```

## Current Status (December 19, 2025)

**Pass Rate**: 29/41 (70%)
- ✅ All 7 short queries pass
- ✅ 10/14 complex queries pass
- ✅ 12/20 BI queries pass

**Remaining Issues**:
1. UNWIND variable scope (3 queries)
2. WITH expression aliases (2 queries)
3. Parser crashes (7 queries)

See `KNOWN_ISSUES.md` for detailed analysis.
