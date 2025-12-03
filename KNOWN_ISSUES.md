# Known Issues

**Active Issues**: 4  
**Test Results**: 534/534 unit tests passing (100%)  
**Last Updated**: December 3, 2025

---

## Active Issues

### 1. Undirected Multi-Hop Patterns Generate Broken SQL

**Status**: 🐛 Bug  
**Severity**: HIGH  
**Identified**: December 3, 2025

**Problem**: Undirected multi-hop patterns like `(a)-[r1]-(b)-[r2]-(c)` generate invalid SQL with wrong table aliases and missing JOINs.

**Example Query**:
```cypher
MATCH (a:Airport)-[r1:FLIGHT]-(b:Airport)-[r2:FLIGHT]-(c:Airport) RETURN a.code, b.code, c.code
```

**Current (BROKEN) SQL**:
```sql
SELECT r1.Origin AS "a.code"  -- Wrong: r1 not in FROM clause!
FROM test_integration.flights AS r2
UNION ALL 
SELECT r1.Origin AS "a.code"  -- Same wrong reference
FROM test_integration.flights AS r2
```

**Expected SQL** (similar to directed multi-hop but with UNION for bidirectionality):
```sql
SELECT r1.Origin AS "a.code", r1.Dest AS "b.code", r2.Dest AS "c.code"
FROM test_integration.flights AS r1
INNER JOIN test_integration.flights AS r2 ON r2.Origin = r1.Dest
-- With proper UNION ALL for backward directions
```

**Root Cause**: The `BidirectionalUnion` optimizer pass transforms `Direction::Either` patterns into `Union { Outgoing, Incoming }`, but this transformation breaks the nested `GraphRel` structure that multi-hop JOIN inference depends on. The JOIN inference works correctly for directed patterns but fails when the plan contains Union nodes.

**Workaround**: Use directed patterns:
```cypher
-- ✅ Works correctly
MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) RETURN a.code, b.code, c.code
```

**Fix Required**: Refactor `BidirectionalUnion` optimizer or `GraphJoinInference` to properly handle Union nodes while preserving multi-hop JOIN relationships. This requires careful coordination between the bidirectional expansion and the join inference phases.

---

### 1b. Denormalized Node UNION Column Order (FIXED in v0.5.4)

**Status**: ✅ FIXED (December 2, 2025)  
**Fix**: Sort properties alphabetically before expanding them in UNION branches to ensure consistent column order.

**Root Cause**: HashMap iteration order is non-deterministic in Rust. When denormalized nodes (like Airport in flight data) generate UNION ALL branches for from/to positions, each branch iterated over properties in different orders.

**Problem SQL** (before fix):
```sql
SELECT airport, state, code, city FROM flights  -- Branch 1
UNION ALL
SELECT code, city, airport, state FROM flights  -- Branch 2 (wrong order!)
```

**Correct SQL** (after fix):
```sql
SELECT airport, city, code, state FROM flights  -- Branch 1 (alphabetical)
UNION ALL
SELECT airport, city, code, state FROM flights  -- Branch 2 (same order!)
```

---

### 2. Undirected Patterns - Relationship Uniqueness

**Status**: 🔧 Blocked by Issue #1  
**Severity**: HIGH  
**Identified**: November 22, 2025  
**Blocker**: Issue #1 (Undirected Multi-Hop Patterns) must be fixed first

**Problem**: For undirected multi-hop patterns, the same relationship can be traversed twice (forward and backward) without proper ID-based uniqueness checks.

**Root Cause**: `(from_id, to_id)` is NOT always a unique key - temporal/transactional graphs can have multiple edges between same nodes.

**Prepared Solution** (ready to implement once Issue #1 is fixed):  
Helper functions for generating pairwise uniqueness filters are prepared in `src/render_plan/plan_builder_helpers.rs` (commented out). These generate SQL like:
```sql
WHERE NOT (tuple(r1.from_id, r1.to_id) = tuple(r2.from_id, r2.to_id))
```

**Schema Enhancement**: Add optional `edge_id` field to schema config:
```yaml
relationships:
  - type_name: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    edge_id: id  # Unique relationship identifier
```

**Design Doc**: `notes/CRITICAL_relationship_vs_node_uniqueness.md`

---

### 3. Anonymous Nodes Without Labels Not Supported

**Status**: 📋 Limitation  
**Severity**: LOW  
**Identified**: December 2, 2025

**Problem**: Anonymous nodes without labels cannot be resolved to tables:
```cypher
MATCH ()-[r:FOLLOWS]->() RETURN r LIMIT 5  -- ❌ Broken SQL
MATCH ()-[r]->() RETURN r LIMIT 5          -- ❌ Also broken
```

**Root Cause**: Without a label, the query planner cannot determine which node table to use. The anonymous node gets a generated alias (e.g., `aeba9f1d7f`) but no `table_name`, causing invalid SQL with dangling references.

**Workaround**: Always specify node labels:
```cypher
MATCH (:User)-[r:FOLLOWS]->(:User) RETURN r LIMIT 5  -- ✅ Works
MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r LIMIT 5  -- ✅ Works
```

**Future Enhancement**: For schemas with a single relationship type or polymorphic edge table, the system could infer node types from the relationship's `from_node_label`/`to_node_label` configuration. Deferred for now.

---

### 4. Disconnected Patterns Generate Invalid SQL

**Status**: 🐛 Bug  
**Severity**: MEDIUM  
**Identified**: November 20, 2025

**Problem**: Comma-separated patterns without shared nodes generate invalid SQL:
```cypher
MATCH (user:User), (other:User) WHERE user.user_id = 1 RETURN other.user_id
```

**Current**: Generates SQL referencing `user` not in FROM clause → ClickHouse error  
**Expected**: Either throw `DisconnectedPatternFound` error OR generate CROSS JOIN

**Location**: `src/query_planner/logical_plan/match_clause.rs` - disconnection check not triggering

---

## Recently Resolved

See [CHANGELOG.md](CHANGELOG.md) for full history.

**December 2025**:
- ✅ Parser rejects invalid syntax (WHERE AND, invalid operators)
- ✅ Regex operator `=~` → `match()` function
- ✅ Polymorphic multi-type JOIN filters
- ✅ VLP min_hops filtering, aggregation support
- ✅ collect() → groupArray() mapping
- ✅ Fixed-length VLP inline JOINs
- ✅ RETURN whole relationship expansion
- ✅ Graph functions (type, id, labels)
- ✅ RETURN whole node (all schema types)
- ✅ Zero-length path patterns (*0..N)
- ✅ Bolt protocol PackStream support

**November 2025**:
- ✅ OPTIONAL MATCH support
- ✅ EXISTS subqueries
- ✅ WITH + MATCH chaining
- ✅ Multi-schema architecture
- ✅ Variable-length paths (*1..3)
- ✅ Shortest path algorithms
