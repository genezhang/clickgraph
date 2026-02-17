# Known Issues

**Last Updated**: February 17, 2026

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).

---

## Active Issues

### 1. Neo4j Desktop / NeoDash WebSocket Connection (GitHub #57)
**Status**: Open  
**Error**: `Invalid magic preamble: [71, 69, 84, 32], expected: [96, 96, 176, 23]`  
**Cause**: Neo4j Desktop and NeoDash send HTTP/WebSocket upgrade requests (`GET ...`) instead of raw Bolt TCP. ClickGraph's Bolt listener expects the raw Bolt handshake preamble.  
**Impact**: Cannot connect via Neo4j Desktop or hosted NeoDash. Neo4j Browser (standalone), Python driver, and Cypher Shell work fine.  
**Workaround**: Use Neo4j Browser at `http://localhost:7474` (standalone) or connect via Python/Java drivers directly.

### 2. Shortest Path on Dense Graphs
**Status**: Performance limitation  
**Error**: `MEMORY_LIMIT_EXCEEDED` or query timeout  
**Cause**: Recursive CTE-based shortest path explores all paths. Dense graphs cause exponential explosion.  
**Workaround**: Use bounded path length: `shortestPath((a)-[:FOLLOWS*1..5]->(b))`

### 3. Aggregations on Empty Results Return Empty Array
**Status**: Semantics mismatch with Neo4j (compatibility issue)  
**Error**: None (behavior mismatch)  
**Impact**: Medium — Breaks Neo4j compatibility, client code must check for empty arrays  
**Cause**: Result handling layer doesn't distinguish between "no rows" vs "aggregation on no rows"

**Expected Neo4j Behavior** (aggregations always return one row):
```cypher
MATCH (p:Post)-[r]->(u:User) RETURN count(*) as result
→ {"results":[{"result": 0}]}

MATCH (p:Post)-[r]->(u:User) RETURN sum(p.post_id), avg(p.post_id), min(p.post_id), max(p.post_id), collect(p.content)
→ {"results":[{"sum": 0, "avg": NULL, "min": NULL, "max": NULL, "collect": []}]}
```

**Actual ClickGraph Behavior** (returns empty array for all aggregations):
```cypher
MATCH (p:Post)-[r]->(u:User) RETURN count(*), sum(...), avg(...), etc.
→ {"results":[]}
```

**Aggregate Function Comparison**:

| Function | Neo4j (Empty) | ClickHouse (Empty) | ClickGraph (Empty) | ✅ Match Neo4j? |
|----------|---------------|--------------------|--------------------|----------------|
| `count(*)` | `0` | `0` | `[]` | ❌ |
| `sum(expr)` | `0` | `0` | `[]` | ❌ |
| `avg(expr)` | `NULL` | `nan` | `[]` | ❌ |
| `min(expr)` | `NULL` | `0` | `[]` | ❌ |
| `max(expr)` | `NULL` | `0` | `[]` | ❌ |
| `collect(expr)` | `[]` | `[]` | `[]` | ❌ (structure wrong) |

**ClickGraph Issue**: Returns `{"results": []}` instead of `{"results": [{"count(*)": 0, ...}]}`

**Root Cause**: In `src/server/handlers.rs` lines 1032-1035, the result handler directly wraps ClickHouse rows without checking if the query contains aggregations. ClickHouse returns 0 rows for empty matches, but SQL aggregations should always return 1 row.

**Fix Location**: Need to detect aggregate queries and ensure at least one result row is returned. Options:
1. Modify SQL generation to use `SELECT ... UNION ALL SELECT 0 WHERE NOT EXISTS(...)`
2. Post-process results in handlers.rs to inject default aggregate row when empty
3. Modify ClickHouse query to use `WITH TOTALS` or similar mechanism

**Workaround**: Client code must check `if (response.results.length === 0)` and supply default aggregate values (0 for count/sum, null for avg/min/max, [] for collect)

---

## Recently Fixed (February 2026)

### Pattern Comprehensions ✅ (Feb 13)
**Fix**: Commit f144108 - Full implementation with CTE+JOIN
- Added target_label/target_property extraction from pattern AST
- Fixed aggregation type detection (collect() → GroupArray)
- Generated INNER JOIN to target node table
- Fixed relationship name matching and JOIN conditions
**Tests**: `tests/integration/test_pattern_comprehensions.py` passing

### Empty Plans with Column References ✅ (Feb 16)
**Fix**: Commits e5ca181 + b3697e2
- Empty plans now use `FROM system.one WHERE false` for valid SQL
- RETURN-only queries (e.g., `RETURN 1`) properly handled
- Column references in Empty plans replaced with typed defaults
**Location**: `src/render_plan/plan_builder.rs` lines 2301-2400

### labels(n) on Untyped Nodes ✅ (Feb 17)
**Fix**: PR #104 - Branch-specific label extraction
- Extract single label from each UNION branch's GraphNode
- Temporarily override plan_ctx during projection tagging
- Tightened VLP detection with `is_cte_reference()` check
**Tests**: `tests/integration/test_labels_untyped_nodes.py` with 7 test cases

---

## Out of Scope (by design)

ClickGraph is a **read-only** analytical query engine:
- ❌ Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`)
- ❌ Schema DDL (`CREATE INDEX`, `CREATE CONSTRAINT`)
- ❌ Transaction management (`BEGIN`, `COMMIT`, `ROLLBACK`)
- ❌ Stored procedures (APOC/GDS) — only built-in `db.*` procedures

---

## Historical Fixes (Pre-February 2026)

| Issue | Fix | PR |
|---|---|---|
| Unlabeled nodes + labeled rel invalid UNION branches (#6) | Phase 0 Case 5 in TypeInference | path-direction-fix |
| Property filtering on unlabeled nodes invalid branches (#7) | Phase 2 property-based candidate filtering | path-direction-fix |
| Relationship property access fails with CTE structure (#8) | pattern_union CTE exposes direct columns | path-direction-fix |
| Query-level UNION fails plan context merge (#9) | Fixed branch pruning for TypeInference placeholders | path-direction-fix |
| `count(n)` on untyped nodes returns wrong value | Aggregation placed above UNION, not inside branches | path-direction-fix |
| FOLLOWS self-join returns empty | from_node/to_node aliases for same-table JOINs | path-direction-fix |
| JSON column order alphabetical instead of query order | serde_json `preserve_order` feature | path-direction-fix |
| UNION `__label__` injection not projection-guided | `returns_whole_entity()` checks Projection items | path-direction-fix |
| UNWIND crash with collect(DISTINCT) | Fixed infinite WITH iteration + DISTINCT handling | #91 |
| Cross-session ID leakage between tenants | IdMapper scoped by schema + tenant | #85 |
| Query cache ignores tenant_id | Cache key includes tenant_id + view_parameters | main |
| PackStream arrays/objects not encoded | Recursive PackStream encoding | #83 |
| UNION column mismatch (literal + aggregate) | Extracted helper, fixed branch construction | #84 |
| Browser click-to-expand failures (5 schema types) | CTE naming, JOIN fixes, VLP rendering | #70–#82 |
| Browser EXPLAIN probe noise | EXPLAIN handler returns empty SUCCESS | #85 |
| Session commands not working in browser | ConnectionState::Streaming fix | #85 |
