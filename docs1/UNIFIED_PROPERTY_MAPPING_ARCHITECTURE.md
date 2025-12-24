# Unified Property Mapping Architecture

**Date**: November 24, 2025  
**Critical Insight**: Three schema patterns need unified property mapping

---

## The Three Schema Patterns

### Pattern 1: Standard Schema (✅ Fully Implemented)
```
Nodes: Separate Tables          Edges: Separate Tables
┌──────────────┐                ┌──────────────┐
│ users        │                │ follows      │
│ - user_id    │◄───from_id─────│ - from_id    │
│ - username   │                │ - to_id      │
│ - email      │────to_id───────► - follow_date│
└──────────────┘                └──────────────┘

Property Mapping:
  User.name → users.username
  FOLLOWS.date → follows.follow_date
```

### Pattern 2: Denormalized Nodes (⚠️ Schema Loaded, Query Broken)
```
Nodes: Virtual (no physical table)    Edge: Physical Table
                                      ┌────────────────────────┐
[Airport] ◄────Origin──────           │ flights                │
                                      │ - Origin (node a)      │
                                      │ - OriginCityName       │
                                      │ - Dest (node b)        │
                                      │ - DestCityName         │
                                      │ - Distance (edge)      │
                                      │ - Carrier              │
                                      └────────────────────────┘
                ─────Dest─────► [Airport]

Property Mapping (Position-Dependent!):
  Airport(as from).code → flights.Origin
  Airport(as from).city → flights.OriginCityName
  Airport(as to).code   → flights.Dest
  Airport(as to).city   → flights.DestCityName
  Flight.distance       → flights.Distance
```

### Pattern 3: Polymorphic Edges (⚠️ Schema Loaded, Query Missing)
```
Nodes: Separate Tables              Edge: Single Table, Multiple Types
┌──────────────┐                    ┌────────────────────────────────┐
│ users        │◄───from_id─────    │ interactions                   │
│ - user_id    │                    │ - from_id                      │
└──────────────┘                    │ - to_id                        │
                                    │ - interaction_type (FOLLOWS,   │
┌──────────────┐                    │   LIKES, AUTHORED, etc.)       │
│ posts        │────to_id───────►   │ - from_type (User, Post, etc.) │
│ - post_id    │                    │ - to_type (User, Post, etc.)   │
└──────────────┘                    │ - timestamp                    │
                                    │ - weight                       │
                                    └────────────────────────────────┘

Property Mapping (Type-Dependent!):
  FOLLOWS.created_at → interactions.timestamp WHERE interaction_type='FOLLOWS'
  LIKES.created_at   → interactions.timestamp WHERE interaction_type='LIKES'
  All types share:   weight → interactions.weight
```

---

## Current State: Fragmented Property Mapping

### Problem: Different Patterns Handled Differently

| Pattern | Schema Loading | Property Mapping | Query Generation | Status |
|---------|---------------|------------------|------------------|--------|
| **Standard** | ✅ graph_schema.rs | ✅ ViewScan.property_mapping | ✅ filter_tagging, render_plan | **Working** |
| **Denormalized** | ✅ graph_schema.rs (from_node_properties, to_node_properties) | ⚠️ Partial (only in CTE gen for var-length paths) | ❌ Broken (alias not resolved) | **Broken** |
| **Polymorphic** | ✅ graph_schema.rs (expands to multiple RelationshipSchema) | ❌ Missing (no runtime type filtering) | ❌ Missing (no WHERE clause gen) | **Not Implemented** |

### Scattered Logic Today

```
Standard Properties:
  ├─ Schema: graph_schema.rs (NodeSchema.property_mappings)
  ├─ ViewScan: logical_plan/view_scan.rs (property_mapping HashMap)
  ├─ Filter: analyzer/filter_tagging.rs (uses PropertyAccess)
  └─ SQL: render_plan/render_expr.rs (PropertyValue.to_sql)

Denormalized Properties:
  ├─ Schema: graph_schema.rs (NodeSchema.from_properties, to_properties)
  ├─ ViewScan: ⚠️ NOT populated into property_mapping correctly!
  ├─ CTE: render_plan/cte_generation.rs:518 (special function)
  └─ Filter: ❌ Broken - uses wrong alias

Polymorphic Properties:
  ├─ Schema: graph_schema.rs (RelationshipSchema per type)
  ├─ Type Filter: ❌ Missing - no WHERE interaction_type='FOLLOWS'
  ├─ FROM Clause: ❌ Missing - no UNION for [:TYPE1|TYPE2]
  └─ Node Filter: ❌ Missing - no WHERE from_type='User'
```

---

## Root Cause: No Unified Property Resolution

### What We Need: Single Property Resolution System

```
Property Access Request:
  Input: (alias, property, context)
  Context: {
    pattern: Standard | Denormalized | Polymorphic
    position: From | To | Center (for denormalized)
    type_filter: "FOLLOWS" (for polymorphic)
    node_filter: ("User", "Post") (for polymorphic)
  }
  Output: PropertyValue (Column or Expression)
```

### Current vs Needed

**Current (Broken)**:
```rust
// Standard: Works
ViewScan.property_mapping.get("name") → PropertyValue::Column("username")

// Denormalized: Broken
ViewScan.property_mapping.get("city") → None  // ❌ Wrong!
// Should look up: from_properties["city"] → "OriginCityName"
//             or: to_properties["city"] → "DestCityName"

// Polymorphic: Missing
ViewScan.property_mapping.get("created_at") → PropertyValue::Column("timestamp")
// But WHERE clause missing: AND interaction_type = 'FOLLOWS'
```

**Needed (Unified)**:
```rust
// Unified resolution:
resolve_property(
    alias: "a",
    property: "city",
    context: PropertyContext {
        pattern: Denormalized,
        position: From,
    }
) → PropertyValue::Column("OriginCityName")

resolve_property(
    alias: "f",
    property: "created_at",
    context: PropertyContext {
        pattern: Polymorphic,
        type_value: "FOLLOWS",
    }
) → PropertyValue::Column("timestamp")
   + type_filter: "interaction_type = 'FOLLOWS'"
```

---

## Unified Architecture Design

### Core Concept: Property Resolution Context

```rust
// src/graph_catalog/property_resolution.rs (NEW)

/// Unified property resolution for all schema patterns
pub struct PropertyResolver {
    schema: Arc<GraphSchema>,
    pattern_detectors: HashMap<String, PatternInfo>,
}

/// Detected pattern for an alias
pub enum PatternInfo {
    Standard {
        node_or_edge_schema: SchemaRef,
    },
    Denormalized {
        edge_schema: Arc<RelationshipSchema>,
        node_label: String,
        position: NodePosition,
    },
    Polymorphic {
        edge_schema: Arc<RelationshipSchema>,
        type_value: String,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    },
}

pub enum NodePosition {
    From,
    To,
}

impl PropertyResolver {
    /// Resolve property access to SQL representation
    pub fn resolve_property(
        &self,
        alias: &str,
        property: &str,
    ) -> Result<PropertyResolution, GraphSchemaError> {
        let pattern_info = self.pattern_detectors.get(alias)
            .ok_or_else(|| GraphSchemaError::UnknownAlias(alias.to_string()))?;
        
        match pattern_info {
            PatternInfo::Standard { node_or_edge_schema } => {
                self.resolve_standard_property(node_or_edge_schema, property)
            }
            PatternInfo::Denormalized { edge_schema, position, .. } => {
                self.resolve_denormalized_property(edge_schema, position, property)
            }
            PatternInfo::Polymorphic { edge_schema, type_value, .. } => {
                self.resolve_polymorphic_property(edge_schema, type_value, property)
            }
        }
    }
}

/// Result of property resolution
pub struct PropertyResolution {
    /// The PropertyValue (column or expression)
    pub property_value: PropertyValue,
    
    /// Additional WHERE clause filters needed
    /// For polymorphic: ["interaction_type = 'FOLLOWS'", "from_type = 'User'"]
    pub type_filters: Vec<String>,
    
    /// The SQL table alias to use
    pub table_alias: String,
}
```

### Integration Points

#### 1. Schema Loading (graph_schema.rs)
**No Changes Needed** - Already has all metadata:
- ✅ `NodeSchema.from_properties` / `to_properties` (denormalized)
- ✅ `RelationshipSchema.type_column` / `type_values` (polymorphic)
- ✅ `property_mappings` (standard)

#### 2. ViewScan Creation (logical_plan/view_scan.rs)
**Enhanced to populate from all patterns**:

```rust
// When creating ViewScan for node "a" in denormalized pattern:
let property_mapping = if is_denormalized {
    // Build from from_properties or to_properties based on position
    build_denormalized_property_mapping(
        node_schema,
        position, // From or To
    )
} else {
    // Standard: copy from node_schema.property_mappings
    node_schema.property_mappings.clone()
};

ViewScan {
    property_mapping,  // Now correct for all patterns!
    // ...
}
```

#### 3. PropertyResolver (NEW Component)
**Built during schema_inference**:

```rust
// src/query_planner/analyzer/property_resolution.rs (NEW)

impl PropertyResolver {
    pub fn build_from_logical_plan(
        plan: &LogicalPlan,
        schema: &GraphSchema,
    ) -> Result<Self, AnalyzerError> {
        let mut pattern_detectors = HashMap::new();
        
        // Walk LogicalPlan and detect patterns
        walk_plan(plan, &mut |alias, view_scan| {
            let pattern_info = detect_pattern(view_scan, schema)?;
            pattern_detectors.insert(alias.clone(), pattern_info);
        })?;
        
        Ok(PropertyResolver {
            schema: schema.clone(),
            pattern_detectors,
        })
    }
    
    fn detect_pattern(
        view_scan: &ViewScan,
        schema: &GraphSchema,
    ) -> Result<PatternInfo, AnalyzerError> {
        // Check if denormalized
        if view_scan.is_denormalized {
            return Ok(PatternInfo::Denormalized { /* ... */ });
        }
        
        // Check if polymorphic
        if let Some(edge_schema) = schema.get_rel_schema(&view_scan.source_table) {
            if edge_schema.type_column.is_some() {
                return Ok(PatternInfo::Polymorphic { /* ... */ });
            }
        }
        
        // Default: standard
        Ok(PatternInfo::Standard { /* ... */ })
    }
}
```

#### 4. filter_tagging.rs (Modified)
**Uses PropertyResolver**:

```rust
pub fn tag_filters(
    plan: LogicalPlan,
    ctx: &mut PlanCtx,
    property_resolver: &PropertyResolver,  // NEW
    schema: &GraphSchema,
) -> Result<LogicalPlan, AnalyzerError> {
    match filter_expr {
        LogicalExpr::PropertyAccess { alias, property } => {
            // Unified resolution!
            let resolution = property_resolver.resolve_property(&alias, &property)?;
            
            // Create PropertyAccess with resolved values
            let mut filter = PropertyAccess {
                table_alias: resolution.table_alias,
                column: resolution.property_value.column_name(),
            };
            
            // For polymorphic: add type filters
            if !resolution.type_filters.is_empty() {
                filter = add_type_filters(filter, resolution.type_filters);
            }
            
            filter
        }
    }
}
```

#### 5. RenderPlan (Modified for Polymorphic)
**Generates type filter WHERE clauses**:

```rust
// For polymorphic edges, add WHERE interaction_type = 'FOLLOWS'
if let Some(type_filter) = edge_type_filter {
    render_plan.filters.push(type_filter);
}
```

---

## Implementation Plan: Unified Property Mapping

### Phase 1: Core PropertyResolver (4 hours)

**Files**:
- `src/graph_catalog/property_resolution.rs` (NEW, ~400 lines)

**Components**:
1. `PropertyResolver` struct
2. `PatternInfo` enum
3. `PropertyResolution` struct
4. `detect_pattern()` function
5. `resolve_standard_property()`
6. `resolve_denormalized_property()`
7. `resolve_polymorphic_property()`

**Tests**:
- Unit tests for each pattern
- Test position-dependent resolution (denormalized)
- Test type-dependent resolution (polymorphic)

### Phase 2: Enhance ViewScan Population (3 hours)

**Files**:
- `src/query_planner/logical_plan/view_scan.rs` (modify)
- `src/query_planner/analyzer/view_resolver.rs` (modify)

**Changes**:
1. When creating ViewScan for denormalized node:
   ```rust
   // Instead of empty property_mapping:
   property_mapping: populate_from_node_properties(
       node_schema,
       position,  // From or To
   )
   ```

2. When creating ViewScan for polymorphic edge:
   ```rust
   // Add type_filter metadata:
   type_value: Some("FOLLOWS"),
   type_column: Some("interaction_type"),
   ```

### Phase 3: Integrate with AliasResolutionContext (2 hours)

**Files**:
- `src/query_planner/analyzer/alias_resolution.rs` (modify)

**Changes**:
```rust
pub struct AliasResolutionContext {
    alias_map: HashMap<String, String>,
    property_resolver: PropertyResolver,  // NEW: replaces view_scan_map
}

impl AliasResolutionContext {
    pub fn resolve_property(&self, alias: &str, property: &str) 
        -> Result<PropertyResolution, AnalyzerError> 
    {
        // 1. Resolve alias: "a" → "f"
        let sql_alias = self.alias_map.get(alias).unwrap_or(alias);
        
        // 2. Use PropertyResolver for unified resolution
        let mut resolution = self.property_resolver.resolve_property(sql_alias, property)?;
        
        // 3. Override table_alias if remapped
        resolution.table_alias = sql_alias.to_string();
        
        Ok(resolution)
    }
}
```

### Phase 4: Polymorphic Query Support (5 hours)

**Files**:
- `src/render_plan/plan_builder.rs` (modify)
- `src/render_plan/polymorphic_edge_handler.rs` (NEW, ~300 lines)

**Features**:
1. **Type Filter Generation**:
   ```sql
   WHERE interaction_type = 'FOLLOWS'
   ```

2. **UNION for Multiple Types** ([:TYPE1|TYPE2]):
   ```sql
   SELECT * FROM interactions WHERE interaction_type = 'FOLLOWS'
   UNION ALL
   SELECT * FROM interactions WHERE interaction_type = 'LIKES'
   ```

3. **Node Type Filtering**:
   ```sql
   WHERE from_type = 'User' AND to_type = 'Post'
   ```

### Phase 5: Testing (6 hours)

**Test Coverage**:

1. **Standard Pattern** (verify no regression):
   ```cypher
   MATCH (u:User)-[:FOLLOWS]->(u2:User)
   RETURN u.name, u2.name
   ```

2. **Denormalized Pattern** (LAX query):
   ```cypher
   MATCH (a:Airport)-[f:Flight]->(b:Airport) 
   WHERE a.city = 'Los Angeles'
   RETURN a.code, f.carrier, b.code
   ```

3. **Polymorphic Single Type**:
   ```cypher
   MATCH (u:User)-[:FOLLOWS]->(u2:User)
   RETURN u.name, u2.name
   ```
   Expected SQL:
   ```sql
   SELECT ... FROM users u1
   JOIN interactions f ON ...
   JOIN users u2 ON ...
   WHERE f.interaction_type = 'FOLLOWS'
     AND f.from_type = 'User'
     AND f.to_type = 'User'
   ```

4. **Polymorphic Multiple Types**:
   ```cypher
   MATCH (u:User)-[:FOLLOWS|LIKES]->(u2:User)
   RETURN u.name, u2.name, type(f)
   ```
   Expected SQL:
   ```sql
   (SELECT ... WHERE interaction_type = 'FOLLOWS')
   UNION ALL
   (SELECT ... WHERE interaction_type = 'LIKES')
   ```

5. **Mixed Nodes** (User→Post, different types):
   ```cypher
   MATCH (u:User)-[:AUTHORED]->(p:Post)
   RETURN u.name, p.title
   ```
   Expected SQL:
   ```sql
   WHERE interaction_type = 'AUTHORED'
     AND from_type = 'User'
     AND to_type = 'Post'
   ```

### Phase 6: Documentation (2 hours)

**Documents to Update**:
- `docs/UNIFIED_PROPERTY_MAPPING.md` (NEW)
- `STATUS.md` - Update pattern support status
- `CHANGELOG.md` - Add unified property mapping
- `docs/wiki/Schema-Configuration-Advanced.md` - All patterns documented

---

## Key Benefits of Unified Approach

### 1. **Consistent Property Resolution**
- ✅ All patterns use same `PropertyResolver`
- ✅ No scattered logic across different files
- ✅ Easy to debug (single point of resolution)

### 2. **Correct ViewScan Population**
- ✅ Denormalized nodes have proper property_mapping
- ✅ Polymorphic edges have type metadata
- ✅ Standard nodes unchanged (no regression)

### 3. **Extensibility**
- ✅ Easy to add new patterns (e.g., hyperedges)
- ✅ Template for future schema features
- ✅ Clear abstraction boundary

### 4. **Testability**
- ✅ PropertyResolver testable independently
- ✅ Each pattern has isolated tests
- ✅ Integration tests for complex queries

### 5. **Performance**
- ✅ Single pattern detection pass
- ✅ Cached in PropertyResolver
- ✅ No repeated tree walks

---

## Architecture Comparison

### Before (Fragmented)
```
Standard Properties:
  schema.rs → view_scan.rs → filter_tagging.rs → render_expr.rs
  ✅ Works

Denormalized Properties:
  schema.rs → ??? → cte_generation.rs (only var-length)
  ❌ Broken for simple queries

Polymorphic Properties:
  schema.rs → ??? → (not implemented)
  ❌ Missing
```

### After (Unified)
```
All Properties:
  schema.rs → PropertyResolver → AliasResolutionContext → filter_tagging.rs
            ↑
            │
     Unified Resolution
     - Standard: property_mappings
     - Denormalized: from_properties[position]
     - Polymorphic: property_mappings + type_filter
  
  ✅ All patterns work
  ✅ Single code path
  ✅ Consistent behavior
```

---

## Migration Strategy

### Step 1: Add PropertyResolver (Non-Breaking)
- New file, no changes to existing code
- Can be tested independently

### Step 2: Enhance ViewScan Population (Backward Compatible)
- Improves ViewScan.property_mapping for denormalized
- Existing code still works

### Step 3: Integrate with AliasResolutionContext
- Replaces view_scan_map with property_resolver
- Unified interface

### Step 4: Add Polymorphic Support
- New functionality, no existing code affected
- Opt-in via schema config

### Step 5: Deprecate Scattered Logic
- Remove special case in cte_generation.rs
- Consolidate into PropertyResolver

---

## Timeline

| Phase | Description | Hours | Dependencies |
|-------|-------------|-------|--------------|
| 1 | PropertyResolver core | 4 | None |
| 2 | ViewScan population | 3 | Phase 1 |
| 3 | AliasResolutionContext | 2 | Phase 1, 2 |
| 4 | Polymorphic query support | 5 | Phase 1, 2, 3 |
| 5 | Testing | 6 | All |
| 6 | Documentation | 2 | All |
| **Total** | | **22 hours** | **~3 days** |

---

## Success Criteria

### Must Have ✅
1. All three patterns work for simple queries
2. LAX query (denormalized) generates valid SQL
3. Polymorphic single-type query works
4. No regression in standard pattern tests
5. PropertyResolver has 90%+ test coverage

### Nice to Have 🎯
1. Polymorphic multi-type (UNION) support
2. Performance benchmarks
3. Debug logging for property resolution
4. Migration guide for custom schemas

### Future Work 📋
1. Hyperedges (>2 nodes)
2. Temporal properties
3. Graph projections
4. Schema evolution support

---

## Open Questions

### Q1: Should PropertyResolver be in graph_catalog or query_planner?
**Recommendation**: `graph_catalog/property_resolution.rs`
- It's schema-related functionality
- Works with NodeSchema/RelationshipSchema
- Can be reused across query planner stages

### Q2: How to handle property expressions for denormalized nodes?
**Answer**: PropertyValue already supports expressions:
```rust
// Denormalized property can be expression:
from_properties: {
    "full_name": "concat(origin_first, ' ', origin_last)"
}

// PropertyResolver returns:
PropertyValue::Expression("concat(origin_first, ' ', origin_last)")
```

### Q3: What about denormalized properties in CTEs (variable-length)?
**Answer**: PropertyResolver used there too:
```rust
// In cte_generation.rs:
let property_resolution = property_resolver.resolve_property(alias, property)?;
let column_expr = property_resolution.property_value.to_sql(table_alias);
```

### Q4: How to handle polymorphic edges with denormalized nodes?
**Answer**: Combine patterns:
```rust
PatternInfo::PolymorphicDenormalized {
    edge_schema,
    type_value,
    position,
}
```
This is a future extension, not needed for v1.

---

## Summary

**Current State**:
- 3 schema patterns, 3 different implementations
- Denormalized broken (alias not resolved)
- Polymorphic missing (no query support)
- Fragmented property mapping logic

**Proposed State**:
- 1 unified PropertyResolver
- All patterns work correctly
- Clean abstraction
- Easy to extend

**Impact**:
- ~22 hours implementation
- ~450 new lines, ~100 lines modified
- No breaking changes to schema format
- Foundation for future features

**Next Step**: Review and approve unified architecture approach.
