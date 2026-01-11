# LDBC SNB Benchmark for ClickGraph

## Overview

This directory contains the LDBC Social Network Benchmark (SNB) implementation for ClickGraph. The benchmark uses official LDBC queries to test ClickGraph's Cypher-to-SQL translation capabilities.

**Current Status (January 2026)**: 4/5 Interactive Short (IS) queries passing, 3/4 Interactive Complex (IC) queries tested passing.

## Quick Start

### Start ClickHouse & ClickGraph
```bash
cd /home/gz/clickgraph

# Start ClickHouse LDBC instance
docker-compose up -d clickhouse-ldbc

# Configure environment for LDBC
export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="ldbc"
export GRAPH_CONFIG_PATH="benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml"
export RUST_LOG=warn

# Start ClickGraph server
cargo run --bin clickgraph &> /tmp/clickgraph_ldbc.log &
```

### Run Full Benchmark Suite
```bash
cd benchmarks/ldbc_snb/scripts
./run_full_benchmark.sh sf10
```

## Query Organization

### Official Queries (Primary Testing)

Located in `queries/official/` with subdirectories:
- `interactive/short-*.cypher` (7 queries) - Simple lookups
- `interactive/complex-*.cypher` (14 queries) - Multi-hop graph traversals
- `bi/*.cypher` (20 queries) - Business intelligence analytics

**Total**: 41 official LDBC queries

**Usage**: Test official queries first. These represent the LDBC benchmark standard.

### Adapted Queries (When Official Fails)

Located in `queries/adapted/` - Created only when official queries cannot work due to:
1. **Syntax Workarounds**: ClickGraph parser limitations requiring query restructuring
2. **Bug Workarounds**: Known bugs that need alternative syntax (e.g., OPTIONAL MATCH + inline property)
3. **Schema Mapping**: Query adjustments for ClickGraph's schema representation

**Example**: `interactive-short-7.cypher` (adapted)
- **Official version**: Uses `MATCH (m:Message {id: $messageId})<-...` (inline property)
- **Issue**: Generates SQL error with OPTIONAL MATCH: `Unknown expression identifier 't34.PersonId'`
- **Adapted version**: Splits to `MATCH (m:Message) WHERE m.id = $messageId MATCH (m)<-...`
- **Status**: Workaround needed until inline property + OPTIONAL MATCH bug is fixed

**Current Adapted Queries**:
- `interactive-short-7.cypher` - OPTIONAL MATCH + inline property workaround
- `interactive-complex-9.cypher` - CTE column naming workaround (underscores vs dots)
- Additional queries as needed

### Custom Queries (Development Only)

Located in `queries/custom/` - Used for:
- Feature testing
- Simplified examples
- Development experiments

**Note**: NOT used for official benchmarking.

## Database Configuration

### ClickHouse LDBC Instance
- **Container**: `clickhouse-ldbc`
- **Port**: 18123 (not default 8123)
- **Credentials**: test_user/test_pass
- **Database**: ldbc
- **Schema**: `benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml`

## Individual Benchmarks

### Data Loading
```bash
cd benchmarks/ldbc_snb/scripts
./benchmark_data_loading.sh sf10
```
Measures: rows/sec, MB/sec, per-table timing  
Output: `results/loading/loading_benchmark_*.json`

### Query Performance
```bash
export WARMUP_RUNS=1 BENCHMARK_RUNS=3
./benchmark_query_performance.sh
```
Tests: LDBC query suite  
Measures: min/max/avg/median duration, rows returned  
Output: `results/performance/query_benchmark_*.json`

### Concurrent Load
```bash
export TEST_DURATION=30 CONNECTION_COUNTS="1 2 4 8 16 32"
./benchmark_concurrent_load.sh
```
Tests: Representative queries at each concurrency level  
Measures: QPS, P50/P90/P99 latency, error rate  
Output: `results/concurrency/concurrent_benchmark_*.json`

## Testing Workflow

1. **Test Official Query First**
   ```bash
   ./test_query.sh queries/official/interactive/short-1.cypher
   ```

2. **If Official Fails**, determine if:
   - Parser cannot handle syntax → Create adapted version with workaround
   - Known bug prevents correct SQL generation → Create adapted version, document bug
   - Schema mismatch → Verify schema, adjust if needed

3. **Document Adaptation Reason** in commit message and test files

## Query Status Summary

### Interactive Short (IS): 4/5 Passing ✅
- ✅ IS-1: Person profile
- ✅ IS-2: Recent messages
- ✅ IS-3: Friends
- ✅ IS-5: Message creator
- ⚠️ IS-7: Replies (adapted version works, official has bug)

Missing: IS-4, IS-6 (not yet in benchmark dataset)

### Interactive Complex (IC): 3/4 Tested ✅
- ✅ IC-2: Recent messages by friends
- ✅ IC-6: Tag co-occurrence
- ✅ IC-12: Expert search
- ⚠️ IC-9: Shortest path (adapted version works, official has CTE bug)

### Business Intelligence (BI): In Progress
Testing underway for BI query suite.

## Known Issues

See project-level [KNOWN_ISSUES.md](../../KNOWN_ISSUES.md) for:
- OPTIONAL MATCH + inline property bug (affects IS-7)
- CTE column naming inconsistency (affects IC-9)
- Additional parser/generator limitations

## Additional Documentation

- `QUERY_FEATURE_ANALYSIS.md` - Detailed feature usage analysis across all LDBC queries
- `DATA_LOADING_GUIDE.md` - Dataset setup and loading procedures
