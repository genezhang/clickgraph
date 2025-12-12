# LDBC SNB Benchmark Results - ClickGraph v0.5.5

**Date**: December 11, 2025  
**Dataset**: LDBC SNB SF0.1 (Scale Factor 0.1)  
**Hardware**: WSL2 Ubuntu 24.04 on Windows 11  
**ClickHouse Version**: 25.8

## Summary

| Category | Queries | Passed | Success Rate |
|----------|---------|--------|--------------|
| Interactive Short (IS) | 7 | 7 | 100% |
| Interactive Complex (IC) | 11 | 11 | 100% |
| Business Intelligence (BI) | 6 | 6 | 100% |
| **Total** | **24** | **24** | **100%** |

## Official LDBC Coverage

The official LDBC SNB Interactive benchmark has:
- **IS1-IS7**: 7 Interactive Short queries ✅ (7/7 = 100%)
- **IC1-IC14**: 14 Interactive Complex queries ⚠️ (11/14 = 79%)
  - Fully supported: IC1, IC2, IC3, IC4, IC5, IC7, IC8, IC9, IC11, IC12, IC13
  - Not tested: IC6 (requires UNWIND), IC10 (requires datetime()), IC14 (requires allShortestPaths + reduce)
- **IU1-IU8**: 8 Interactive Update queries ❌ (out of scope - read-only engine)

**Total Benchmark Coverage: 18/21 read queries (86%)**

## Performance Results (5 iterations, avg/min/max in ms)

### Interactive Short Queries
| Query | Description | Avg | Min | Max | Rows |
|-------|-------------|-----|-----|-----|------|
| IS1 | Person Profile | 8.5 | 7.6 | 10.0 | 1 |
| IS2 | Recent Posts | 38.2 | 34.4 | 42.9 | 10 |
| IS3 | Friends | 24.2 | 22.6 | 26.4 | 10 |
| IS4 | Message Content | 5.1 | 4.5 | 6.2 | 1 |
| IS5 | Message Creator | 13.0 | 11.9 | 14.1 | 1 |
| IS6 | Forum of Message | 13.7 | 12.7 | 14.7 | 1 |
| IS7 | Replies to Message | 350.5 | 173.0 | 555.8 | 4 |

### Interactive Complex Queries
| Query | Description | Avg | Min | Max | Rows |
|-------|-------------|-----|-----|-----|------|
| IC1 | Friends with Name (Variable Path *1..3) | 62.3 | 57.1 | 70.2 | 1 |
| IC2 | Recent Friend Posts | 421.0 | 251.0 | 548.4 | 20 |
| IC3 | Friends in Countries | 212.9 | 196.9 | 229.7 | 0 |
| IC4 | New Topics | 279.1 | 243.0 | 296.0 | 9 |
| IC5 | New Groups | 419.5 | 214.6 | 545.7 | 20 |
| IC7 | Recent Likers | 148.4 | 135.5 | 168.0 | 0 |
| IC8 | Recent Replies | 636.3 | 417.4 | 949.0 | 20 |
| IC9 | FoF Posts | 472.8 | 400.1 | 555.9 | 20 |
| IC11 | Job Referral | 46.5 | 44.9 | 49.9 | 0 |
| IC12 | Expert Search | 770.9 | 437.1 | 953.7 | 5 |
| IC13 | Path Between Two People | 27.1 | 25.9 | 28.3 | 1 |

### Business Intelligence Queries
| Query | Description | Avg | Min | Max | Rows |
|-------|-------------|-----|-----|-----|------|
| BI1 | Post Count | 3.5 | 2.8 | 4.2 | 1 |
| BI2 | Posts per Tag | 170.7 | 155.7 | 196.7 | 20 |
| BI3 | Forum Members | 232.2 | 166.8 | 372.6 | 20 |
| BI4 | Top Content Creators | 178.4 | 171.0 | 191.9 | 20 |
| BI5 | Tag Engagement | 335.9 | 311.5 | 368.5 | 20 |
| BI6 | Related Tags | 322.8 | 227.2 | 488.4 | 11940 |

## Average Performance

- **Overall Average**: 200.9ms
- **IS Queries Average**: ~65ms (mostly single lookups)
- **IC Queries Average**: ~318ms (multi-hop traversals)
- **BI Queries Average**: ~207ms (aggregation queries)

## Dataset Statistics

| Entity | Count |
|--------|-------|
| Person | ~1,700 |
| Post | ~4.1M |
| Comment | ~3.6M |
| Forum | ~29K |

## Known Limitations

1. **Undirected Relationships with Aggregations**: When using undirected relationship patterns (e.g., `-[:KNOWS]-`) combined with aggregations like `count()`, the current SQL generation may have issues. Use directed relationships (`-[:KNOWS]->`) for queries with aggregations.

2. **Variable-Length Paths with WITH**: Combining variable-length paths (`*1..n`) with `WITH DISTINCT` causes SQL generation issues where CTE references aren't properly included. Simplify queries by avoiding this combination.

3. **Inline Property Syntax**: The inline property syntax `MATCH (n {id: X})` has known issues. Use `WHERE n.id = X` instead.

4. **Unsupported Functions**: The following Cypher functions are not yet supported:
   - `UNWIND` - Required for IC6
   - `datetime()` - Required for IC10
   - `allShortestPaths()` - Required for IC14
   - `reduce()` - Required for IC14
   - `head()`, `tail()` - Collection operations
   - `coalesce()` - Null handling

5. **Schema-Specific Relationship Names**: LDBC uses specific relationship type names:
   - `HAS_CREATOR` for Post→Person
   - `COMMENT_HAS_CREATOR` for Comment→Person
   - `REPLY_OF_POST` for Comment→Post
   - `REPLY_OF_COMMENT` for Comment→Comment

## Running the Benchmark

```bash
# Start ClickGraph with LDBC schema
export GRAPH_CONFIG_PATH="./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml"
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="default"
export CLICKHOUSE_DATABASE="ldbc"
./target/release/clickgraph

# Run benchmark
python3 benchmarks/ldbc_snb/scripts/query_audit.py --benchmark --iterations 5
```

## Comparison Notes

LDBC SNB is designed for comparing graph database performance. While direct comparison with Neo4j or other graph databases requires:
- Same hardware
- Same scale factor
- Same query semantics

ClickGraph's approach of translating Cypher to ClickHouse SQL means performance depends heavily on:
- ClickHouse table engines (MergeTree, etc.)
- Index configuration
- Table ordering keys
- Data distribution

For analytical workloads, ClickHouse's columnar storage often provides excellent performance for aggregation-heavy queries (BI queries), while point lookups (IS queries) may be slower than dedicated graph databases with native graph storage.
