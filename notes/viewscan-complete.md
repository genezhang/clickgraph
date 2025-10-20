# ViewScan: Complete Schema-Driven Query Planning

**Status**: ✅ **COMPLETE** (Both nodes and relationships)  
**Date**: October 18, 2025  
**Commit**: TBD

## Summary
ViewScan enables fully schema-driven query planning where Cypher labels and relationship types are resolved to actual ClickHouse tables via YAML configuration instead of hardcoded mappings.

## What ViewScan Does

### For Node Queries
**Before ViewScan**:
```rust
// Hardcoded: "User" → "User" table
Scan { table: "User", alias: "u" }
```

**After ViewScan**:
```rust
// Schema lookup: "User" label → "users" table (from YAML)
ViewScan { 
    source_table: "users",  // ← Looked up from schema
    alias: "u",
    label: "User"
}
```

### For Relationship Queries
**Before** (Partial schema support):
```rust
// Type→Table: HARDCODED in rel_type_to_table_name()
// Columns: from schema ✅
fn rel_type_to_table_name(rel_type: &str) -> String {
    match rel_type {
        "FRIENDS_WITH" => "friendships".to_string(),  // ❌ Hardcoded
        ...
    }
}
```

**After** (Full schema support):
```rust
// Both type→table AND columns from schema ✅
fn rel_type_to_table_name(rel_type: &str) -> String {
    if let Some(schema_lock) = GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                return rel_schema.table_name.clone();  // ✅ From schema
            }
        }
    }
    // Fallback to hardcoded for backwards compat
    match rel_type { ... }
}
```

## How It Works

### Architecture

```
Cypher Query
    ↓
Parser (AST)
    ↓
Query Planner
    ├→ Node Pattern: (u:User)
    │   └→ try_generate_view_scan("u", "User")
    │       └→ GLOBAL_GRAPH_SCHEMA.get_node_schema("User")
    │           └→ ViewScan { source_table: "users", ... }
    │
    └→ Relationship Pattern: -[r:FRIENDS_WITH]->
        └→ rel_type_to_table_name("FRIENDS_WITH")
            └→ GLOBAL_GRAPH_SCHEMA.get_rel_schema("FRIENDS_WITH")
                └→ "friendships" table + columns
    ↓
SQL Generator
    └→ SELECT ... FROM users AS u 
       JOIN friendships AS r ON ...
```

### Key Files

**Schema Loading** (`server/graph_catalog.rs`):
- Loads YAML via `GRAPH_CONFIG_PATH` env var
- Populates `GLOBAL_GRAPH_SCHEMA` singleton
- Available to all query planning code

**Node ViewScan** (`query_planner/logical_plan/match_clause.rs`):
```rust
fn try_generate_view_scan(alias: &str, label: &str) -> Option<Arc<LogicalPlan>> {
    if let Some(schema_lock) = GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            if let Ok(node_schema) = schema.get_node_schema(label) {
                return Some(Arc::new(LogicalPlan::ViewScan(ViewScan {
                    source_table: node_schema.table_name.clone(),
                    alias: alias.to_string(),
                    label: label.to_string(),
                    selected_columns: vec![],
                })));
            }
        }
    }
    None
}
```

**Relationship Schema Lookup** (`render_plan/plan_builder.rs`):
```rust
fn rel_type_to_table_name(rel_type: &str) -> String {
    // Schema lookup first
    if let Some(schema_lock) = GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                return rel_schema.table_name.clone();
            }
        }
    }
    // Hardcoded fallback for backwards compatibility
    match rel_type { ... }
}
```

## Configuration Example

**YAML** (`social_network.yaml`):
```yaml
views:
  - name: social_graph
    nodes:
      user:
        source_table: users        # ← ViewScan uses this
        id_column: user_id
        label: User               # ← Cypher uses this
        property_mappings:
          name: name
          age: age
    
    relationships:
      friends_with:
        source_table: friendships  # ← rel_type_to_table_name() uses this
        type_name: FRIENDS_WITH   # ← Cypher uses this
        from_column: user_id
        to_column: friend_id
        from_node_type: User
        to_node_type: User
```

**Query**:
```cypher
MATCH (u:User)-[r:FRIENDS_WITH]->(f:User)
WHERE u.age > 25
RETURN u.name, f.name
```

**Generated SQL**:
```sql
SELECT u.name, f.name
FROM users AS u              -- ← ViewScan resolved "User" → "users"
JOIN friendships AS r        -- ← rel_type_to_table_name() resolved "FRIENDS_WITH" → "friendships"
  ON u.user_id = r.user_id   -- ← Columns from schema
 AND r.friend_id = f.user_id
JOIN users AS f
WHERE u.age > 25
```

## Testing

**Test Setup**:
```powershell
# Using new test infrastructure
.\test_server.ps1 -Start
.\test_server.ps1 -Test
```

**Test Queries**:
1. Node ViewScan: `MATCH (u:User) RETURN u.name`
2. Relationship ViewScan: `MATCH (u:User)-[r:FRIENDS_WITH]->(f) RETURN u.name, f.name`
3. Combined: `MATCH (u:User)-[r:FRIENDS_WITH]->(f:User) WHERE u.age > 25 RETURN u.name, f.name`

## Design Decisions

### Why Keep Hardcoded Fallback?
**Decision**: Keep hardcoded mappings as fallback in `rel_type_to_table_name()`

**Rationale**:
- Backwards compatibility with queries that don't have YAML config
- Graceful degradation if schema fails to load
- Easier testing (can run without full environment setup)

**Trade-off**: Slight code duplication vs. robustness

### Why Not ViewScan for Relationships?
**Current**: Relationships use `rel_type_to_table_name()` helper function  
**Alternative**: Create `ViewRelScan` plan node (like `ViewScan` for nodes)

**Decision**: Use helper function for now

**Rationale**:
- Relationships have more complex structure (from_column, to_column, node types)
- Helper function is simpler and achieves the same goal
- Can refactor to ViewRelScan later if needed
- Current approach tested and working

### Global Schema Singleton
**Decision**: Use `OnceCell<RwLock<GraphSchema>>` for global schema

**Rationale**:
- Query planning code needs schema access
- Passing schema through all function calls is cumbersome
- Read-heavy workload (query planning) benefits from RwLock
- OnceCell ensures initialization happens once

**Trade-off**: Global state vs. clean dependency injection

## Limitations

1. **Schema reload**: Requires server restart to pick up YAML changes
2. **Type checking**: No validation that YAML types match ClickHouse schema
3. **Error messages**: Schema lookup failures silently fall back to hardcoded
4. **Multi-view**: Currently uses first view in YAML, no view selection

## Future Enhancements

1. **Hot reload**: Watch YAML file and reload schema without restart
2. **Schema validation**: Verify YAML against actual ClickHouse tables
3. **Better errors**: Warn when using fallback instead of silent degradation
4. **View selection**: Allow queries to specify which view to use
5. **ViewRelScan**: Unified plan node for relationships (like ViewScan for nodes)

## Related Files
- Implementation: `query_planner/logical_plan/match_clause.rs:51-67`
- Relationships: `render_plan/plan_builder.rs:92-113`
- Schema loading: `server/graph_catalog.rs:23-55`
- Column lookup: `render_plan/plan_builder.rs:412-424`
- Tests: `test_runner.py` (comprehensive test suite)

## Gotchas

1. **Case sensitivity**: YAML `label: User` must match Cypher `MATCH (u:User)`
2. **Type vs table**: Relationship `type_name` (Cypher) != `source_table` (ClickHouse)
3. **Alias required**: ViewScan needs alias field for proper SQL generation
4. **Schema timing**: GLOBAL_GRAPH_SCHEMA must be initialized before first query
5. **RwLock**: Must use `try_read()` not `read()` to avoid deadlocks

## Success Criteria

✅ Node queries resolve labels via schema  
✅ Relationship queries resolve types via schema  
✅ Backwards compatible with hardcoded mappings  
✅ YAML-only mode works (no ClickHouse connection needed)  
✅ Proper SQL aliases generated  
✅ All existing tests pass  

---

**Key Insight**: ViewScan transforms ClickGraph from a hardcoded graph layer into a **truly schema-driven** system where YAML configuration defines the graph model completely.
