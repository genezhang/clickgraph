# Known Issues

**Active Issues**: 2  
**Test Results**: 537/537 unit tests passing (100%)  
**Last Updated**: December 3, 2025

For recently fixed issues, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Active Issues

### 1. Anonymous Nodes Without Labels Not Supported

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

### 2. Disconnected Patterns Generate Invalid SQL

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
