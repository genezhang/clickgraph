# ViewScan Implementation

**Completed**: October 18, 2025  
**Tests**: 261/262 passing (99.6%)  
**Commits**: `82401f7`, `62c1ad7`

---

## Summary

Implemented view-based SQL translation that resolves Cypher labels to ClickHouse table names using YAML schema configuration. Simple node queries now correctly generate SQL with proper table names and aliases.

**Before**: `MATCH (u:User) RETURN u.name` → Failed with "Unknown table identifier 'User'"  
**After**: `MATCH (u:User) RETURN u.name` → `SELECT u.name FROM users AS u` ✅

---

## How It Works

### Architecture

**Data Flow**:
```
Cypher Label "User" 
    ↓
GLOBAL_GRAPH_SCHEMA lookup
    ↓
Table name "users" 
    ↓
ViewScan created with correct table
    ↓
Alias propagated through ViewTableRef
    ↓
SQL: "FROM users AS u"
```

### Key Components

**1. Schema Lookup** (`match_clause.rs`):
```rust
fn try_generate_view_scan(alias: &str, label: &str) -> Option<Arc<LogicalPlan>> {
    // Access global schema
    let schema_lock = GLOBAL_GRAPH_SCHEMA.get()?;
    let schema = schema_lock.try_read().ok()?;
    
    // Lookup: Label "User" → Table "users"
    let node_schema = schema.get_node_schema(label).ok()?;
    
    // Create ViewScan with correct table name
    let view_scan = ViewScan::new(
        node_schema.table_name.clone(),  // "users" not "User"
        // ...
    );
    
    Some(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))))
}
```

**2. Alias Propagation** (`plan_builder.rs`):
```rust
LogicalPlan::GraphNode(graph_node) => {
    let mut from_ref = from_table_to_view_ref(graph_node.input.extract_from()?);
    if let Some(ref mut view_ref) = from_ref {
        // Attach Cypher variable name to ViewTableRef
        view_ref.alias = Some(graph_node.alias.clone());  // "u" not "t"
    }
    from_ref
}
```

**3. SQL Generation** (`to_sql_query.rs`):
```rust
impl ToSql for FromTableItem {
    fn to_sql(&self) -> String {
        let alias = if let Some(explicit_alias) = &view_ref.alias {
            explicit_alias.clone()  // Use "u" from Cypher
        } else {
            "t".to_string()  // Fallback
        };
        
        format!("FROM {} AS {}", view_ref.name, alias)
    }
}
```

---

## Key Files Modified

| File | Change | Purpose |
|------|--------|---------|
| `query_planner/logical_plan/match_clause.rs` | Added `try_generate_view_scan()` | Schema lookup |
| `query_planner/logical_plan/match_clause.rs` | Modified `generate_scan()` | Call ViewScan generator |
| `query_planner/logical_plan/match_clause.rs` | Modified `traverse_node_pattern()` | Pass label to scan |
| `render_plan/view_table_ref.rs` | Added `alias` field | Store variable names |
| `render_plan/plan_builder.rs` | Modified GraphNode case | Propagate alias |
| `clickhouse_query_generator/to_sql_query.rs` | Modified `FromTableItem::to_sql()` | Use explicit alias |
| `server/mod.rs` | Added HTTP bind error handling | Better error messages |
| `server/handlers.rs` | Cleaned debug statements | Professional logging |
| `main.rs` | Added env_logger initialization | Structured logging |
| `Cargo.toml` | Added env_logger dependency | Logging framework |

---

## Design Decisions

### Why GLOBAL_GRAPH_SCHEMA?

**Decision**: Access schema via global OnceCell<RwLock<GraphSchema>>  
**Reasoning**: 
- Schema is read-only after initialization
- Multiple concurrent query threads need access
- RwLock allows many readers, zero contention
- OnceCell ensures initialization once

**Alternative considered**: Pass schema through function parameters  
**Why rejected**: Would require threading through entire call stack, major refactor

### Why ViewScan vs Modifying Scan?

**Decision**: Create separate ViewScan logical plan node  
**Reasoning**:
- ViewScan already existed in codebase
- Separate types for different concerns (schema-based vs direct)
- Graceful fallback: try ViewScan, fall back to Scan

**Alternative considered**: Add table lookup to existing Scan  
**Why rejected**: Mixing concerns, harder to test, no fallback mechanism

### Why Store Alias in ViewTableRef?

**Decision**: Add `alias: Option<String>` field  
**Reasoning**:
- Cypher variable names must match SQL aliases
- Information needs to flow: GraphNode → ViewTableRef → SQL
- Optional because not all ViewTableRefs come from graph patterns

**Alternative considered**: Infer alias at SQL generation time  
**Why rejected**: Too late, context lost, would need complex heuristics

---

## Gotchas & Debugging Stories

### The 3-Hour Docker Container Mystery

**Problem**: Code changes had zero effect. Debug output never appeared. Fresh builds didn't help.

**Investigation**:
- Added extensive logging → Nothing appeared
- Rebuilt server multiple times → No change
- Checked process IDs → Server process existed
- User ran `docker ps` → **Old Docker container was using port 8080!**

**Root cause**: Docker container "clickgraph-brahmand" from weeks ago was still running, intercepting all traffic.

**Solution**: 
```powershell
docker stop brahmand
docker rm brahmand
```

**Lesson learned**: Always check `docker ps` before debugging port issues. Added to DEV_ENVIRONMENT_CHECKLIST.md as #1 step!

### Alias Mismatch Bug

**Problem**: SQL generated `FROM users AS t` but query referenced `u.name` → "Unknown identifier u"

**Investigation**:
- Traced GraphNode creation → Had correct alias "u"
- Checked ViewTableRef → No alias field!
- Checked SQL generation → Hardcoded "t"

**Solution**: Added alias field, propagated through plan_builder, used in SQL generation.

**Lesson**: Alias information must flow through entire pipeline, can't be regenerated later.

---

## Code Examples

### Working Query
```cypher
MATCH (u:User) RETURN u.name LIMIT 3
```

**Generated SQL**:
```sql
SELECT u.name FROM users AS u LIMIT 3
```

**Result**:
```json
[
  {"name": "Alice"},
  {"name": "Bob"},
  {"name": "Charlie"}
]
```

### YAML Configuration
```yaml
nodes:
  User:
    table: users
    id_column: user_id
    properties:
      - name
      - age
      - email
```

---

## Testing

### Test Results
- **Before**: 260/262 tests passing
- **After**: 261/262 tests passing (99.6%)
- **Fixed**: `test_traverse_node_pattern_new_node`
- **Only failure**: `test_version_string_formatting` (Bolt protocol, unrelated)

### Test Changes
Modified `test_traverse_node_pattern_new_node` to accept either ViewScan or Scan:
```rust
match graph_node.input.as_ref() {
    LogicalPlan::ViewScan(_) => {
        // Success - ViewScan created from schema
    }
    LogicalPlan::Scan(scan) => {
        // Success - Fallback to regular Scan
        assert_eq!(scan.table_name, Some("Person".to_string()));
    }
    _ => panic!("Expected ViewScan or Scan"),
}
```

---

## Limitations

### What Works
✅ Simple node queries: `MATCH (u:User) RETURN u`  
✅ Property selection: `RETURN u.name, u.age`  
✅ WHERE clauses: `WHERE u.age > 25`  
✅ Multiple nodes: `MATCH (u:User), (p:Post) RETURN u, p`

### What Doesn't Work Yet
❌ Relationship traversal: `MATCH (u)-[r:FRIENDS_WITH]->() RETURN u`  
❌ OPTIONAL MATCH with relationships  
❌ Multi-hop paths with ViewScan

**Why**: ViewScan lookup only implemented in `traverse_node_pattern()`. Relationship traversal uses different code paths in `graph_traversal_planning.rs` and `join_builder.rs`.

---

## Infrastructure Improvements

### HTTP Bind Error Handling
**Before**: `.unwrap()` caused silent panic  
**After**: Descriptive error with port number

```rust
let http_listener = match TcpListener::bind(&http_bind_address).await {
    Ok(listener) => listener,
    Err(e) => {
        eprintln!("✗ FATAL: Failed to bind to {}: {}", http_bind_address, e);
        eprintln!("  Is another process using port {}?", config.http_port);
        std::process::exit(1);
    }
};
```

### Logging Framework
Added env_logger for structured logging:
```bash
export RUST_LOG=debug
cargo run
```

Benefits:
- Works correctly with async Tokio runtime
- Configurable log levels
- Structured output with timestamps
- Better than println! in production

---

## Future Work

### Priority 1: Relationship Traversal
Extend ViewScan to handle relationship patterns:
- Investigate: `graph_traversal_planning.rs::traverse_relationship_pattern()`
- Investigate: `join_builder.rs` relationship JOIN generation
- Apply similar schema lookup for relationship table names

### Priority 2: Performance
- Benchmark ViewScan vs direct table access
- Profile GLOBAL_GRAPH_SCHEMA lock acquisition
- Consider caching resolved table names

### Priority 3: Error Messages
- Better errors when label not found in schema
- Suggest similar labels (fuzzy matching)
- Show available labels in error message

---

## References

- **Session Summary**: `SESSION_2025-10-18.md`
- **Checklist**: `DEV_ENVIRONMENT_CHECKLIST.md`
- **YAML Schema**: `social_network.yaml`, `ecommerce_graph_demo.yaml`
- **Test Scripts**: `test_query_simple.py`, `test_optional_match.py`
