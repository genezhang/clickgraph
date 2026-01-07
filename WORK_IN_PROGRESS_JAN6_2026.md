# Work In Progress - Multi-Type VLP Property Extraction
**Date**: January 6, 2026  
**Session**: Implementing JSON property extraction for multi-type VLP (Functionality #1)

## 🎯 Goal
Enable property access (`x.name`, `x.email`, etc.) on multi-type VLP endpoint nodes where the node type is determined at runtime.

Example query:
```cypher
MATCH (u:User)-[:FOLLOWS|AUTHORED*1]->(x)
WHERE u.user_id = 1
RETURN label(x), x.name, x.content
```

Expected: `x` can be User or Post, properties extracted from JSON at runtime.

## ✅ What's Implemented

### 1. Thread-Local Tracking Infrastructure
**File**: `src/clickhouse_query_generator/to_sql_query.rs` (lines 23-37)

```rust
thread_local! {
    static MULTI_TYPE_VLP_ALIASES: RefCell<HashMap<String, String>> = 
        RefCell::new(HashMap::new());
}
```

Tracks which Cypher aliases map to multi-type VLP CTEs.

### 2. CTE Detection & Registration
**File**: `src/clickhouse_query_generator/to_sql_query.rs` (lines 140-161)

Modified `populate_cte_property_mappings()` to detect CTEs with prefix `vlp_multi_type_*` and register their endpoint aliases.

Log message confirms tracking works:
```
🎯 Tracked multi-type VLP alias: 'x' → CTE 'vlp_multi_type_u_x'
```

### 3. JSON Property Extraction Logic
**File**: `src/clickhouse_query_generator/to_sql_query.rs` (lines ~1105-1155)

Added logic in `PropertyAccessExp` rendering to:
- Detect if table alias is in `MULTI_TYPE_VLP_ALIASES`
- For CTE columns (`end_type`, `end_id`): direct access
- For regular properties: generate `JSON_VALUE(x.end_properties, '$.property')`

**This code is complete but NOT being invoked (see Current Issue below)**

### 4. Analyzer Validation Bypass
**Files**: 
- `src/query_planner/analyzer/filter_tagging.rs` (lines ~548-585)
- `src/query_planner/analyzer/projection_tagging.rs` (lines ~444-467)

Added multi-type VLP detection to skip strict property validation during analysis phase:

```rust
// In filter_tagging.rs
let is_multi_type_vlp = if let Some(labels) = table_ctx.get_labels() {
    labels.len() > 1
} else if plan.is_some() {
    Self::is_multi_type_vlp_endpoint(plan.unwrap(), &property_access.table_alias.0)
} else {
    false
};

if is_multi_type_vlp {
    // Skip validation, return property as-is
    return Ok(LogicalExpr::PropertyAccessExp(property_access));
}
```

Helper function `is_multi_type_vlp_endpoint()` traverses plan tree to detect GraphRel with:
- `variable_length.is_some()`
- `labels.len() > 1` (multiple relationship types)
- `right_connection == alias` (this alias is the endpoint)

## 🔧 Current Build Status
- ✅ **Builds successfully** (3.72-5.28s, 104 warnings)
- ✅ **Server starts** and responds to health check
- ✅ **Query doesn't error** (analyzer bypass working)
- ❌ **Wrong SQL generated** (JSON extraction not triggered)

## ❌ Current Issue: SQL Generation Problem

### What We Get
Query executes but returns raw CTE columns:

```json
{
  "results": [
    {
      "end_id": "1",
      "end_properties": "[\"2024-01-16 10:00:00\",\"Hello world!\",1]",
      "end_type": "Post"
    }
  ]
}
```

### Generated SQL (Wrong)
```sql
WITH vlp_multi_type_u_x AS (
  SELECT 'User' AS end_type, toString(u2.user_id) AS end_id, 
         toJSONString((u2.email_address AS email, ...)) AS end_properties
  FROM ...
  UNION ALL
  SELECT 'Post' AS end_type, toString(p2.post_id) AS end_id,
         toJSONString((p2.created_at AS date, ...)) AS end_properties
  FROM ...
)
SELECT 
  end_type AS "end_type",      -- ❌ Should be: x.end_type AS "label(x)"
  end_id AS "end_id",          -- ❌ Should not be in RETURN
  end_properties AS "end_properties"  -- ❌ Should not be in RETURN
FROM vlp_multi_type_u_x AS x
LIMIT 5
```

### Expected SQL
```sql
WITH vlp_multi_type_u_x AS (...)
SELECT 
  x.end_type AS "label(x)",
  JSON_VALUE(x.end_properties, '$.name') AS "x.name",
  JSON_VALUE(x.end_properties, '$.content') AS "x.content"
FROM vlp_multi_type_u_x AS x
WHERE x.start_id = '1'
LIMIT 5
```

## 🔍 Root Cause Analysis

The SELECT clause items are not going through the `PropertyAccessExp` rendering path in `to_sql_query.rs`. 

**Hypothesis**: When we bypass property resolution in `projection_tagging`, the LogicalExpr items remain as:
- `ScalarFnCall("label", ...)` 
- `PropertyAccessExp("x", "name")`
- `PropertyAccessExp("x", "content")`

But during SQL generation, these are being rendered directly to column names instead of being transformed through our JSON extraction logic.

**Evidence**:
1. ✅ Tracking happens: `🎯 Tracked multi-type VLP alias: 'x' → CTE 'vlp_multi_type_u_x'`
2. ❌ No logs from PropertyAccessExp JSON extraction code
3. ❌ SELECT clause shows raw column names, not JSON_VALUE calls

## 🐛 Debugging Steps Taken

1. ✅ Verified MULTI_TYPE_VLP_ALIASES tracking works (log confirms)
2. ✅ Verified CTE generation is correct (SQL shows proper CTE)
3. ✅ Verified analyzer bypass works (no validation errors)
4. ❌ Need to trace why PropertyAccessExp rendering isn't invoked

## 📋 Next Steps to Resume

### Immediate: Debug SQL Rendering Path

1. **Add debug logging** in `to_sql_query.rs` PropertyAccessExp handler:
   ```rust
   RenderExpr::PropertyAccessExp(PropertyAccess { table_alias, column }) => {
       log::info!("🔍 Rendering PropertyAccessExp: {}.{}", table_alias.0, column.raw());
       let col_name = column.raw();
       
       // Check if this is a multi-type VLP endpoint
       let multi_type_json_result = MULTI_TYPE_VLP_ALIASES.with(|mvla| {
           log::info!("🔍 Checking MULTI_TYPE_VLP_ALIASES for: {}", table_alias.0);
           // ... rest of code
       });
   }
   ```

2. **Check LogicalPlan structure** before SQL generation:
   - What are the actual expressions in Projection.items?
   - Are they PropertyAccessExp or something else?
   - Are the table aliases correct?

3. **Trace render path**:
   - How does LogicalExpr → RenderExpr conversion happen?
   - Where is the conversion missing for our case?

### Alternative Approach: Force Property Transformation

Instead of skipping property resolution entirely, we could:

1. In `projection_tagging`, when detecting multi-type VLP:
   - Still create PropertyAccessExp with proper table alias
   - But mark them specially (e.g., with a flag or wrapper)
   - Let them flow through to SQL generation

2. Or: Transform them immediately to the target format:
   ```rust
   // In projection_tagging for multi-type VLP
   item.expression = LogicalExpr::PropertyAccessExp(PropertyAccess {
       table_alias: property_access.table_alias.clone(),
       column: PropertyValue::Column(property_access.column.raw().to_string()),
   });
   ```

## 📝 Test Query for Quick Validation

```bash
# Start server
CLICKHOUSE_URL="http://localhost:8123" \
CLICKHOUSE_USER="test_user" \
CLICKHOUSE_PASSWORD="test_pass" \
GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml" \
RUST_LOG=info \
./target/debug/clickgraph

# Test query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "USE social_benchmark MATCH (u:User)-[:FOLLOWS|AUTHORED*1]->(x) WHERE u.user_id = 1 RETURN label(x), x.name, x.content LIMIT 5"
  }'

# Check SQL generation
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "USE social_benchmark MATCH (u:User)-[:FOLLOWS|AUTHORED*1]->(x) WHERE u.user_id = 1 RETURN label(x), x.name, x.content LIMIT 5",
    "sql_only": true
  }'
```

## 🎓 Key Learnings

1. **Analyzer runs before SQL generation**: TypeInference → FilterTagging → ProjectionTagging → SQL Generation
2. **Multi-type VLP detection must happen early**: TypeInference doesn't set labels for VLP endpoints yet
3. **Property validation must be skipped**: Otherwise analyzer rejects queries with properties that don't exist on all types
4. **SQL generation needs context**: The PropertyAccessExp handler needs to know which aliases are multi-type VLP

## 📚 Related Files

### Modified Files
- `src/clickhouse_query_generator/to_sql_query.rs` - JSON extraction logic
- `src/query_planner/analyzer/filter_tagging.rs` - Validation bypass + helper
- `src/query_planner/analyzer/projection_tagging.rs` - Validation bypass

### Key Reference Files
- `src/clickhouse_query_generator/multi_type_vlp_joins.rs` - CTE generation (working)
- `src/query_planner/analyzer/type_inference.rs` - Label inference (line 198)
- `src/query_planner/analyzer/projection_tagging.rs` - label()/labels() handling (lines 739-790)

## 🔧 Environment

- **System**: Ubuntu 22.04 (travelling)
- **Rust**: Latest stable (104 warnings, 0 errors)
- **ClickHouse**: Docker container on localhost:8123
- **Test Data**: social_benchmark schema (users_bench, posts_bench, user_follows_bench)

## 💡 Implementation Strategy

Original plan had 3 parts:
1. **Property extraction from JSON** (THIS SESSION - 80% done)
2. Property filtering in WHERE clause (future)
3. ORDER BY on JSON properties (future)

We're stuck on the final 20% of part 1: making the SQL generation invoke our JSON extraction code.

---

**Resume Point**: Debug why PropertyAccessExp rendering isn't being invoked for multi-type VLP SELECT items. Add logging to trace the render path from LogicalPlan.Projection to final SQL SELECT clause.
