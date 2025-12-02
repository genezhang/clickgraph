# Known Issues

**Active Issues**: 3  
**Test Results**: 534/534 unit tests passing (100%)  
**Last Updated**: December 2, 2025

---

## Active Issues

### 1. Undirected Patterns - ClickHouse OR-in-JOIN Limitation

**Status**: 🔧 Needs UNION ALL implementation  
**Severity**: HIGH  
**Identified**: November 29, 2025

**Problem**: Undirected patterns `(a)-[r]-(b)` generate OR conditions in JOINs, which ClickHouse handles incorrectly (missing rows).

**Current SQL** (problematic):
```sql
INNER JOIN follows AS r ON (r.follower_id = a.user_id OR r.followed_id = a.user_id)
```

**Solution**: Generate UNION ALL of two directed queries instead:
```sql
-- Direction 1
SELECT ... FROM users AS a JOIN follows AS r ON r.follower_id = a.user_id ...
UNION ALL
-- Direction 2  
SELECT ... FROM users AS a JOIN follows AS r ON r.followed_id = a.user_id ...
```

**Affected Tests**: `test_relationship_degree`, `test_undirected_relationship`

**Design Doc**: `notes/bidirectional-union-approach.md`

---

### 2. Undirected Patterns - Relationship Uniqueness

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

### 3. Disconnected Patterns Generate Invalid SQL

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

## Known Limitations

### Zero-Length Path Pattern (*0)
Patterns like `*0..3` are not supported. Use `*1..3` instead.

### RETURN Node on Denormalized Schemas
`RETURN a` (whole node) returns empty for denormalized schemas. Workaround: explicitly list properties `RETURN a.prop1, a.prop2`.

### Bolt Protocol PackStream
PackStream binary parsing is stubbed. Works with Neo4j drivers that send text-based messages.

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

**November 2025**:
- ✅ OPTIONAL MATCH support
- ✅ EXISTS subqueries
- ✅ WITH + MATCH chaining
- ✅ Multi-schema architecture
- ✅ Variable-length paths (*1..3)
- ✅ Shortest path algorithms
