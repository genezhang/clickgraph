# Known Issues

**Last Updated**: February 16, 2026

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

### 3. Pattern Comprehensions
**Status**: Parsed but not executed  
**Error**: `PatternComprehensionNotRewritten`  
**Example**: `[(a)-[r]->(b) | b.name]`  
**Impact**: Low-Medium — blocks 1-2 LDBC queries (bi-8)  
**Cause**: AST and LogicalExpr exist, but the rewrite pass to convert to SQL is not implemented

---

## Out of Scope (by design)

ClickGraph is a **read-only** analytical query engine:
- ❌ Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`)
- ❌ Schema DDL (`CREATE INDEX`, `CREATE CONSTRAINT`)
- ❌ Transaction management (`BEGIN`, `COMMIT`, `ROLLBACK`)
- ❌ Stored procedures (APOC/GDS) — only built-in `db.*` procedures

---

## Recently Fixed (February 2026)

| Issue | Fix | PR |
|---|---|---|
| UNWIND crash with collect(DISTINCT) | Fixed infinite WITH iteration + DISTINCT handling | #91 |
| Cross-session ID leakage between tenants | IdMapper scoped by schema + tenant | #85 |
| Query cache ignores tenant_id | Cache key includes tenant_id + view_parameters | main |
| PackStream arrays/objects not encoded | Recursive PackStream encoding | #83 |
| UNION column mismatch (literal + aggregate) | Extracted helper, fixed branch construction | #84 |
| Browser click-to-expand failures (5 schema types) | CTE naming, JOIN fixes, VLP rendering | #70–#82 |
| Browser EXPLAIN probe noise | EXPLAIN handler returns empty SUCCESS | #85 |
| Session commands not working in browser | ConnectionState::Streaming fix | #85 |
