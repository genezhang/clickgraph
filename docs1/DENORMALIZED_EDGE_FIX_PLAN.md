# Denormalized Edge Test Case Analysis

## The LAX Query Problem

### Query
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.origin = 'LAX'
```

### Schema (Denormalized)
```yaml
nodes:
  - label: Airport
    table: flights  # ⚠️ Same table as edge!
    from_node_properties:
      code: Origin
      city: OriginCityName
    to_node_properties:
      code: Dest
      city: DestCityName

relationships:
  - type: FLIGHT
    table: flights
    from_id: Origin
    to_id: Dest
```

### What Should Happen

**Expected SQL**:
```sql
SELECT * 
FROM flights AS f
WHERE f.Origin = 'LAX'
```

**Key Points**:
- **No JOIN** - single table scan
- **Alias mapping**: `a` → `f`, `b` → `f` (both map to edge alias)
- **Property mapping**: `a.origin` → `f.Origin` (from_node_properties)

### What Currently Happens

**Current Flow**:

1. **Parser**: Creates AST with:
   - Node `a` with alias `"a"`
   - Edge `f` with alias `"f"`  
   - Node `b` with alias `"b"`
   - Filter: `PropertyAccess("a", "origin")`

2. **LogicalPlan**: Creates:
   - ViewScan for `a` (table: flights, alias: "a")
   - ViewScan for `f` (table: flights, alias: "f")
   - ViewScan for `b` (table: flights, alias: "b")
   - GraphJoin connecting them
   - Filter: `PropertyAccess("a", "origin")`

3. **Analyzer/filter_tagging**: Tags filter:
   ```rust
   PropertyAccess {
       table_alias: "a",     // ⚠️ WRONG! Should be "f"
       column: "origin",     // ⚠️ WRONG! Should be "Origin"
   }
   ```

4. **RenderPlan/plan_builder**: 
   - Detects denormalized pattern
   - Tries to remap aliases
   - **BUG**: Remapping logic is incomplete/incorrect

5. **Generated SQL** (WRONG):
   ```sql
   SELECT *
   FROM flights AS f
   WHERE a.origin = 'LAX'  -- ⚠️ 'a' doesn't exist!
   ```

### Root Cause

The alias `"a"` is used throughout the pipeline, but:
- When SQL is generated, only `"f"` exists as a table alias
- The remapping from `"a"` → `"f"` happens too late and is fragile
- Property mapping (`origin` → `Origin`) is also applied incorrectly

---

## Current Code Flow (Detailed)

### Step 1: Schema Loading
```rust
// src/graph_catalog/graph_schema.rs
// When loading ontime_denormalized.yaml

pub struct NodeSchema {
    pub label: String,
    pub table: String,
    pub from_node_properties: HashMap<String, String>,  // code → Origin
    pub to_node_properties: HashMap<String, String>,    // code → Dest
    // ...
}

// Detection happens here:
if node.table == edge.table {
    // This is denormalized!
    // But this information is NOT propagated effectively
}
```

### Step 2: LogicalPlan Creation
```rust
// src/query_planner/logical_plan/match_clause.rs
// For MATCH (a:Airport)-[f:Flight]->(b:Airport)

ViewScan {
    source_table: "flights",
    alias: "a",  // ⚠️ Problem starts here
    is_denormalized: true,  // Flag is set
    // ...
}

ViewScan {
    source_table: "flights",
    alias: "f",
    is_denormalized: false,  // Edge itself not "denormalized"
    // ...
}

ViewScan {
    source_table: "flights",
    alias: "b",
    is_denormalized: true,
    // ...
}

GraphJoins {
    left: ViewScan(a),
    center: ViewScan(f),
    right: ViewScan(b),
    // Should realize: no joins needed!
}
```

### Step 3: Filter Tagging
```rust
// src/query_planner/analyzer/filter_tagging.rs
// WHERE a.origin = 'LAX'

fn tag_property_filter(filter: LogicalExpr) -> LogicalExpr {
    match filter {
        PropertyAccess { alias: "a", property: "origin" } => {
            // ⚠️ Current: Uses "a" directly
            PropertyAccess {
                table_alias: "a",
                column: "origin",
            }
            
            // ✅ Should be: Resolve to edge alias
            PropertyAccess {
                table_alias: "f",  // Resolved
                column: "Origin",  // Mapped via from_node_properties
            }
        }
    }
}
```

### Step 4: RenderPlan Building
```rust
// src/render_plan/plan_builder.rs

fn build_render_plan(logical_plan: LogicalPlan) -> RenderPlan {
    // Detects denormalized pattern
    let denorm_aliases = get_denormalized_aliases(&logical_plan);
    // Returns: {"a", "b"}
    
    // Tries to remap filters
    for filter in filters {
        if denorm_aliases.contains(&filter.table_alias) {
            // ⚠️ Remapping logic is HERE - too late!
            // Should map "a" → "f"
            // But which edge? Multiple patterns possible!
        }
    }
    
    // Generates FROM clause
    RenderPlan {
        from: ViewTableRef {
            table: "flights",
            alias: "f",  // Only "f" in SQL!
        },
        joins: vec![],  // No joins (correct!)
        filters: FilterItems(
            PropertyAccess("a", "origin")  // ⚠️ Still wrong
        ),
    }
}
```

### Step 5: SQL Generation
```rust
// src/render_plan/render_plan.rs

impl ToSql for RenderPlan {
    fn to_sql(&self) -> String {
        let mut sql = String::new();
        sql.push_str("SELECT * FROM ");
        sql.push_str(&self.from.to_sql());  // "flights AS f"
        
        if let Some(filter) = &self.filters.0 {
            sql.push_str(" WHERE ");
            sql.push_str(&filter.to_sql());  // "a.origin = 'LAX'"
        }
        
        sql
    }
}

// Result: SELECT * FROM flights AS f WHERE a.origin = 'LAX'
//                                               ↑ ERROR: 'a' not defined
```

---

## Why Current Approach Fails

### Problem 1: Late Binding
- Alias resolution happens in `render_plan` (too late)
- By then, context about graph structure is lost
- Hard to determine which edge table aliases should be used

### Problem 2: Information Loss
- `get_denormalized_aliases()` only returns set of aliases: `{"a", "b"}`
- Doesn't know:
  - Which edge they belong to (`"f"`)
  - Which position (from/to)
  - Which property mappings to use

### Problem 3: Scattered Logic
- Schema detection: `graph_catalog`
- Flag setting: `logical_plan/view_scan.rs`
- Alias collection: `render_plan/plan_builder_helpers.rs`
- Remapping attempt: `render_plan/plan_builder.rs`
- No single source of truth

---

## Proposed Solution: Early Resolution

### New Flow with AliasResolutionContext

```rust
// src/query_planner/analyzer/alias_resolution.rs

pub struct AliasResolutionContext {
    // Maps Cypher alias → SQL table alias
    alias_map: HashMap<String, String>,
    // Maps (Cypher alias, property) → (SQL alias, column)
    property_map: HashMap<(String, String), (String, String)>,
}

// After logical plan creation, before filter tagging:
let resolution_ctx = resolve_aliases(&logical_plan, schema)?;

// For MATCH (a:Airport)-[f:Flight]->(b:Airport):
// alias_map: {
//   "a" → "f",
//   "b" → "f",
//   "f" → "f",
// }
// property_map: {
//   ("a", "origin") → ("f", "Origin"),
//   ("a", "city") → ("f", "OriginCityName"),
//   ("b", "origin") → ("f", "Dest"),
//   ("b", "city") → ("f", "DestCityName"),
//   ("f", "flight_num") → ("f", "FlightNum"),
// }
```

### Updated Filter Tagging

```rust
// src/query_planner/analyzer/filter_tagging.rs

fn tag_property_filter(
    filter: LogicalExpr,
    resolution_ctx: &AliasResolutionContext,
) -> LogicalExpr {
    match filter {
        PropertyAccess { alias: "a", property: "origin" } => {
            // Look up in resolution context
            let (sql_alias, sql_column) = resolution_ctx
                .resolve_property("a", "origin");
            
            PropertyAccess {
                table_alias: sql_alias,   // "f"
                column: sql_column,        // "Origin"
            }
        }
    }
}
```

### Simplified RenderPlan Building

```rust
// src/render_plan/plan_builder.rs

fn build_render_plan(logical_plan: LogicalPlan) -> RenderPlan {
    // No need to detect denormalized aliases!
    // Filters already have correct aliases from analyzer
    
    RenderPlan {
        from: ViewTableRef {
            table: "flights",
            alias: "f",
        },
        joins: vec![],
        filters: FilterItems(
            PropertyAccess("f", "Origin")  // ✅ Correct!
        ),
    }
}
```

### Final SQL

```sql
SELECT * 
FROM flights AS f 
WHERE f.Origin = 'LAX'
```

✅ **Correct!**

---

## Implementation Checklist

### Phase 1: Core Infrastructure
- [ ] Create `alias_resolution.rs` in `query_planner/analyzer/`
- [ ] Define `AliasResolutionContext` struct
- [ ] Define `ResolvedAlias` struct
- [ ] Implement `resolve()` and `resolve_property()` methods

### Phase 2: Pattern Detection
- [ ] Implement `resolve_aliases()` function
- [ ] Handle `GraphJoins` pattern
- [ ] Detect denormalized nodes via schema
- [ ] Build alias_map
- [ ] Build property_map
- [ ] Store in `PlanCtx`

### Phase 3: Integration
- [ ] Modify `filter_tagging.rs` to use resolution context
- [ ] Update `PropertyAccess` creation
- [ ] Pass resolution context through analyzer passes
- [ ] Update `PlanCtx` to include resolution context

### Phase 4: Cleanup
- [ ] Remove `is_denormalized` from `ViewScan`
- [ ] Delete `get_denormalized_aliases()` from helpers
- [ ] Remove denormalized-specific logic from `plan_builder.rs`
- [ ] Simplify RenderPlan building

### Phase 5: Testing
- [ ] Create test for LAX query
- [ ] Test with full ontime dataset
- [ ] Test mixed scenarios (some denorm, some not)
- [ ] Test variable-length paths with denormalized nodes
- [ ] Update all existing denormalized tests

### Phase 6: Documentation
- [ ] Update architecture docs
- [ ] Add inline comments explaining resolution
- [ ] Update STATUS.md
- [ ] Add to CHANGELOG.md

---

## Test Cases to Validate

### 1. Simple Denormalized Query
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.origin = 'LAX'
```
**Expected**: Single table scan, no joins, `f.Origin = 'LAX'`

### 2. Both Nodes Filtered
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.origin = 'LAX' AND b.origin = 'JFK'
```
**Expected**: `f.Origin = 'LAX' AND f.Dest = 'JFK'`

### 3. Edge Property Filter
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE f.distance > 1000
```
**Expected**: `f.Distance > 1000`

### 4. Mixed Properties
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.city = 'Los Angeles' AND f.carrier = 'AA'
```
**Expected**: `f.OriginCityName = 'Los Angeles' AND f.Carrier = 'AA'`

### 5. Return Denormalized Properties
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
RETURN a.city, f.flight_num, b.airport
```
**Expected**: `SELECT f.OriginCityName, f.FlightNum, f.DestAirportName FROM flights AS f`

---

## Success Criteria

1. ✅ LAX query generates correct SQL
2. ✅ All 5 test cases pass
3. ✅ No `is_denormalized` references in `render_plan/`
4. ✅ All existing tests still pass
5. ✅ Clean separation: graph concepts end at analyzer boundary

---

## Timeline Estimate

- **Phase 1**: 3 hours
- **Phase 2**: 6 hours
- **Phase 3**: 4 hours
- **Phase 4**: 2 hours
- **Phase 5**: 6 hours
- **Phase 6**: 2 hours

**Total**: ~23 hours (3 working days)
