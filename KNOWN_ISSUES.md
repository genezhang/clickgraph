# Known Issues

**Last Updated**: February 17, 2026

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).

---

## Active Issues

### 1. Shortest Path on Dense Graphs
**Status**: Performance limitation  
**Error**: `MEMORY_LIMIT_EXCEEDED` or query timeout  
**Cause**: Recursive CTE-based shortest path explores all paths. Dense graphs cause exponential explosion.  
**Workaround**: Use bounded path length: `shortestPath((a)-[:FOLLOWS*1..5]->(b))`

---

## Recently Fixed (February 2026)

### Aggregations on Empty Results ✅ (Dec 21, 2025)
**Fix**: Commit 734d65f - Unified aggregation logic
- ClickHouse aggregations now properly return 1 row with default values (count=0, etc.)
- Made `extract_select_items()` aggregation-aware
- Unified WITH and RETURN aggregation code paths
**Tests**: `test_aggregation_empty_result`, `test_count_empty_result` expecting 1 row with count=0

### Neo4j Desktop / NeoDash WebSocket Connection ✅ (Feb 2)
**Fix**: PR #64 (commit 6755d22) - Full WebSocket Bolt transport
- Added `websocket.rs` with WebSocketBoltAdapter implementing AsyncRead/AsyncWrite
- Server detects HTTP GET/POST requests on Bolt port → WebSocket handshake
- Neo4j Desktop and NeoDash now connect successfully via Bolt WebSocket
**Location**: `src/server/bolt_protocol/websocket.rs` + integration in `src/server/mod.rs`

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
