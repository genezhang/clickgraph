# Known Issues

**Active Issues**: 3  
**Test Results**: 534/534 unit tests passing (100%)  
**Last Updated**: December 2, 2025

---

## Active Issues

### 1. ~~Undirected Patterns - Direction Logic Bug in UNION ALL~~ ✅ FIXED

**Status**: ✅ FIXED (December 2, 2025)  
**Fix**: Ensured each UNION branch has independent `joined_entities` state and correctly swaps `from_id`/`to_id` columns based on direction.

**Correct SQL now generated**:
```sql
SELECT ... FROM users AS u1
JOIN follows AS r ON r.follower_id = u1.user_id  -- Branch 1: outgoing
JOIN users AS u2 ON u2.user_id = r.followed_id
UNION ALL
SELECT ... FROM users AS u1
JOIN follows AS r ON r.followed_id = u1.user_id  -- Branch 2: incoming (swapped!)
JOIN users AS u2 ON u2.user_id = r.follower_id
```

---

### ~~2.~~ 1. Undirected Patterns - Relationship Uniqueness

**Status**: 🔧 Requires relationship IDs in schema  
**Severity**: HIGH  
**Identified**: November 22, 2025

**Problem**: For undirected multi-hop patterns, the same relationship can be traversed twice (forward and backward) without proper ID-based uniqueness checks.

**Root Cause**: `(from_id, to_id)` is NOT always a unique key - temporal/transactional graphs can have multiple edges between same nodes.

**Solution**: Add optional `relationship_id` field to schema config:
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
