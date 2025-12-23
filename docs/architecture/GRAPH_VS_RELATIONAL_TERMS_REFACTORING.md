# Graph vs Relational Terms: Architectural Assessment

**Date**: December 22, 2025  
**Context**: Investigating mixing of graph logical concepts with SQL physical concepts

## The Problem: Abstraction Level Mixing

### Current Architecture (WRONG)

```
Cypher Parser
    ↓
GraphNode { input: Arc<LogicalPlan::Scan> }  ← SQL concept at graph level!
    ↓
Analyzer Passes (work on GraphNode with Scan/ViewScan inside)
    ↓
SQL Generator (extracts Scan/ViewScan from GraphNode.input)
```

**Key Issues:**
1. **Parser creates Scan/ViewScan** - SQL physical concepts created during Cypher parsing
2. **GraphNode wraps SQL structures** - `input: Arc<LogicalPlan>` holds Scan/ViewScan
3. **Analyzers manipulate SQL** - Graph analyzers work with Scan.table_name, ViewScan properties
4. **No clean separation** - Graph logic mixed with SQL execution details

### Current Flow (Detailed)

#### 1. Parser Phase (`match_clause.rs`)
```rust
// Line 313: generate_scan() creates Scan/ViewScan during parsing!
fn generate_scan(alias: String, label: Option<String>, plan_ctx: &PlanCtx) 
    -> LogicalPlanResult<Arc<LogicalPlan>> {
    if let Some(label_str) = &label {
        // Try ViewScan first (lines 335-342)
        if let Some(view_scan) = try_generate_view_scan(&alias, &label_str, plan_ctx) {
            return Ok(view_scan);  // ← ViewScan created at parse time!
        }
        // Fallback to Scan
        Ok(Arc::new(LogicalPlan::Scan(Scan { 
            table_alias: Some(alias), 
            table_name: Some(table_from_schema) 
        })))
    } else {
        // Anonymous node: Scan with table_name: None
        Ok(Arc::new(LogicalPlan::Scan(Scan { 
            table_alias: Some(alias), 
            table_name: None  // ← This is why we check scan.table_name.is_none()!
        })))
    }
}

// Line 1419: GraphNode wraps the Scan/ViewScan
Arc::new(LogicalPlan::GraphNode(GraphNode {
    input: scan,  // ← Arc<LogicalPlan::Scan> or Arc<LogicalPlan::ViewScan>
    alias: "u",
    label: Some("User"),
    is_denormalized: false,
    projected_columns: None,
}))
```

#### 2. Analyzer Phase (50+ passes)
```rust
// SchemaInference (schema_inference.rs:78-127)
LogicalPlan::Scan(scan) => {
    // Manipulate table_name during analysis!
    let table_name = if let Some(label) = table_ctx.get_label_opt() {
        Some(node_schema.full_table_name())
    } else {
        None  // Anonymous nodes keep table_name: None
    };
    Transformed::Yes(Arc::new(LogicalPlan::Scan(Scan {
        table_name,
        table_alias: scan.table_alias.clone(),
    })))
}

// plan_builder_helpers.rs: 5 checks for anonymous nodes
if scan.table_name.is_none() {  // ← Special handling for SQL concept!
    // Anonymous node logic
}
```

#### 3. SQL Generation Phase (`plan_builder.rs`)
```rust
// extract_from (lines 6440-6630) extracts FROM clause from GraphJoins
match plan.as_ref() {
    LogicalPlan::GraphNode(gn) => {
        match gn.input.as_ref() {
            LogicalPlan::ViewScan(vs) => {
                // Extract ViewScan properties for SQL
                format!("{}.{} AS {}", vs.database, vs.source_table, gn.alias)
            }
            LogicalPlan::Scan(scan) => {
                if let Some(table_name) = &scan.table_name {
                    format!("{} AS {}", table_name, gn.alias)
                } else {
                    // Anonymous node: what to do?
                }
            }
        }
    }
}
```

### Why This Happened

**Historical Context:**
- ClickGraph forked from Brahmand (upstream project)
- Brahmand used traditional node/edge tables → Scan made sense
- ClickGraph evolved to **view-based exclusively** → ViewScan should dominate
- Parser logic inherited Scan creation → never refactored
- GraphNode structure kept `input: Arc<LogicalPlan>` → no clean graph abstraction

**Architectural Debt:**
- Scan/ViewScan are **SQL physical execution concepts** (table access methods)
- GraphNode/GraphRel are **graph logical concepts** (nodes and relationships)
- Mixing them = **no separation of concerns**

---

## Proposed Architecture (CORRECT)

### Clean Separation of Concerns

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. CYPHER AST (open_cypher_parser)                             │
│    Pure syntax: AstNode, AstRelationship, AstPathPattern       │
└────────────────┬────────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. GRAPH LOGICAL PLAN (query_planner/logical_plan)             │
│    Pure graph semantics:                                        │
│    - GraphNode { alias, label, properties }                    │
│    - GraphRel { left, right, relationship_type }               │
│    - GraphJoins { anchor_node, pattern }                       │
│    NO Scan/ViewScan/Table references!                          │
└────────────────┬────────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. GRAPH ANALYZERS (query_planner/analyzer)                    │
│    Work on pure graph structures:                              │
│    - Schema resolution: label → NodeSchema                     │
│    - Type inference: infer missing labels/types                │
│    - Join inference: determine join patterns                   │
│    NO table_name checks, NO ViewScan manipulation!             │
└────────────────┬────────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. SQL LOGICAL PLAN (new layer or clickhouse_query_generator)  │
│    Translate graph → relational:                               │
│    - GraphNode { label: "User" } → ViewScan(users table)      │
│    - GraphRel { type: "FOLLOWS" } → ViewScan(follows table)   │
│    - GraphJoins → SQL JOINs with ON conditions                │
│    THIS is where Scan/ViewScan are created!                    │
└────────────────┬────────────────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. SQL GENERATION (clickhouse_query_generator)                 │
│    Render SQL text:                                            │
│    - ViewScan → SELECT ... FROM database.table AS alias        │
│    - Joins → ... JOIN ... ON ...                              │
└─────────────────────────────────────────────────────────────────┘
```

### New GraphNode Structure

```rust
// Current (WRONG)
pub struct GraphNode {
    pub input: Arc<LogicalPlan>,  // ← Holds Scan/ViewScan (SQL concept!)
    pub alias: String,
    pub label: Option<String>,
    pub is_denormalized: bool,
    pub projected_columns: Option<Vec<(String, String)>>,
}

// Proposed (CORRECT)
pub struct GraphNode {
    pub alias: String,
    pub label: Option<String>,
    pub properties: Vec<PropertyRef>,  // Cypher properties requested
    // Resolved during analysis (NOT at parse time):
    pub resolved_schema: Option<ResolvedNodeSchema>,
}

pub struct ResolvedNodeSchema {
    pub database: String,
    pub table_name: String,
    pub is_denormalized: bool,
    pub property_mappings: HashMap<String, String>,
    // All the info needed for SQL generation, but NOT SQL structures!
}
```

### Key Changes

#### 1. Parser Changes (`match_clause.rs`)

**Before:**
```rust
fn generate_scan(...) -> Arc<LogicalPlan> {
    // Creates Scan/ViewScan immediately
    if let Some(view_scan) = try_generate_view_scan(...) {
        return Ok(view_scan);  // ← SQL concept at parse time!
    }
}
```

**After:**
```rust
fn create_graph_node(...) -> Arc<LogicalPlan> {
    // Pure graph structure
    Arc::new(LogicalPlan::GraphNode(GraphNode {
        alias: "u",
        label: Some("User"),  // Label only, no table yet
        properties: vec![],
        resolved_schema: None,  // Resolved later by analyzer
    }))
}
```

#### 2. Analyzer Changes (`schema_inference.rs`)

**Before:**
```rust
LogicalPlan::Scan(scan) => {
    // Manipulate SQL structure
    let table_name = node_schema.full_table_name();
    Transformed::Yes(Arc::new(LogicalPlan::Scan(Scan {
        table_name: Some(table_name),
        ...
    })))
}
```

**After:**
```rust
LogicalPlan::GraphNode(gn) => {
    // Resolve graph schema
    let resolved = if let Some(label) = &gn.label {
        let node_schema = graph_schema.get_node_schema(label)?;
        Some(ResolvedNodeSchema {
            database: node_schema.database.clone(),
            table_name: node_schema.table_name.clone(),
            property_mappings: node_schema.property_mappings.clone(),
            is_denormalized: node_schema.is_denormalized,
        })
    } else {
        // Anonymous node: infer from relationship (existing logic)
        infer_node_schema_from_relationship(...)
    };
    
    Transformed::Yes(Arc::new(LogicalPlan::GraphNode(GraphNode {
        resolved_schema: resolved,
        ..gn.clone()
    })))
}
```

#### 3. SQL Generator Changes (`plan_builder.rs`)

**Before:**
```rust
match gn.input.as_ref() {
    LogicalPlan::ViewScan(vs) => {
        // Extract from existing ViewScan
        format!("{}.{} AS {}", vs.database, vs.source_table, gn.alias)
    }
}
```

**After:**
```rust
match gn.resolved_schema {
    Some(schema) => {
        // Create ViewScan/table reference here (first time!)
        format!("{}.{} AS {}", schema.database, schema.table_name, gn.alias)
    }
    None => {
        // Error: Schema resolution failed
        return Err(SqlGenerationError::UnresolvedNode(gn.alias))
    }
}
```

---

## Impact Assessment

### Files to Change

#### High Impact (Core Structures)
1. **`src/query_planner/logical_plan/mod.rs`** (100-150 lines)
   - Redefine `GraphNode` structure (remove `input` field)
   - Add `ResolvedNodeSchema` struct
   - Remove `Scan`/`ViewScan` variants OR mark as deprecated

2. **`src/query_planner/logical_plan/match_clause.rs`** (300-400 lines)
   - Replace `generate_scan()` with `create_graph_node()`
   - Remove all ViewScan creation logic (lines 313-700)
   - Simplify node pattern processing

3. **`src/query_planner/logical_expr/mod.rs`** (50-100 lines)
   - Remove Scan creation in EXISTS subqueries (lines 556-610)
   - Create pure GraphNode structures

#### Medium Impact (Analyzers)
4. **`src/query_planner/analyzer/schema_inference.rs`** (150-200 lines)
   - Convert from Scan manipulation to schema resolution
   - Add ResolvedNodeSchema population
   - Remove `push_inferred_table_names_to_scan()`

5. **`src/query_planner/analyzer/graph_join_inference.rs`** (100-150 lines)
   - Remove `scan.table_name.is_none()` checks (lines 3468, 3477)
   - Use `gn.resolved_schema` instead
   - Simplify anonymous node handling

6. **`src/query_planner/analyzer/projected_columns_resolver.rs`** (50-100 lines)
   - Update to work with `resolved_schema`
   - Remove ViewScan extraction logic

7. **`src/query_planner/analyzer/plan_builder_helpers.rs`** (50 lines)
   - Remove 5 instances of `scan.table_name.is_none()` checks
   - Use graph-level abstractions

#### Medium Impact (SQL Generation)
8. **`src/clickhouse_query_generator/plan_builder.rs`** (200-300 lines)
   - Create ViewScan/Scan during SQL generation (first time!)
   - `extract_from()`: Access `gn.resolved_schema` instead of `gn.input`
   - Add helper functions for ViewScan creation

9. **`src/clickhouse_query_generator/plan_builder_helpers.rs`** (100 lines)
   - Update helper functions to use `resolved_schema`

#### Low Impact (Other Analyzers)
10-20. Various analyzer passes (10-30 lines each):
    - `duplicate_scans_removing.rs`
    - `filter_tagging.rs`
    - `bidirectional_union.rs`
    - `query_validation.rs`
    - `cte_column_resolver.rs`
    - Pattern matching updates for new GraphNode structure

### Lines of Code Estimate

| Component | Files | Est. Lines Changed |
|-----------|-------|-------------------|
| Core structures | 3 | 500-650 |
| Analyzers | 7 | 550-750 |
| SQL generation | 2 | 300-400 |
| Other passes | 10-15 | 200-450 |
| **TOTAL** | **22-27** | **1550-2250** |

### Time Estimate

| Phase | Duration | Notes |
|-------|----------|-------|
| Design refinement | 4-6 hours | Finalize ResolvedNodeSchema structure |
| Core structure changes | 8-12 hours | GraphNode, remove Scan deps |
| Parser changes | 6-8 hours | Remove generate_scan(), create GraphNode |
| Analyzer updates | 12-16 hours | Schema resolution, remove Scan checks |
| SQL generation | 8-12 hours | ViewScan creation at SQL time |
| Testing & debugging | 16-24 hours | Integration tests, edge cases |
| Documentation | 4-6 hours | Architecture docs, migration notes |
| **TOTAL** | **58-84 hours** | **1.5-2 weeks full-time** |

### Risk Assessment

#### High Risk Areas
1. **Anonymous node inference** - Currently relies on `scan.table_name.is_none()`
   - Mitigation: Use `gn.label.is_none()` or `gn.resolved_schema.is_none()`

2. **Denormalized nodes** - Complex logic in ViewScan creation
   - Mitigation: Move denormalization logic to ResolvedNodeSchema

3. **CTE references** - Scans used for CTE placeholders
   - Mitigation: Add `CteReference` variant or use resolved_schema

4. **Bidirectional relationships** - ViewScan UNION for undirected edges
   - Mitigation: Defer UNION creation to SQL generation

#### Medium Risk Areas
5. **Optional patterns** - LEFT JOIN logic may assume Scan existence
6. **Variable-length paths** - CTE generation accesses ViewScan properties
7. **Property mappings** - Currently stored in ViewScan

#### Low Risk Areas
8. **Simple node patterns** - Direct translation
9. **Relationship patterns** - GraphRel already graph-oriented
10. **WHERE filters** - Independent of node structure

### Benefits

#### Immediate
- ✅ **Clean architecture**: Graph concepts in graph layer, SQL in SQL layer
- ✅ **Easier to reason about**: No more "why does parser create ViewScan?"
- ✅ **Simpler analyzers**: Work with graph structures, not SQL
- ✅ **Eliminates checks**: No more `scan.table_name.is_none()`

#### Long-term
- ✅ **Extensibility**: Easy to add new graph features without SQL coupling
- ✅ **Multiple backends**: Could target PostgreSQL, Neo4j, etc. (not just ClickHouse)
- ✅ **Optimization**: Graph-level optimizations separate from SQL optimizations
- ✅ **Testing**: Can test graph logic without SQL generation

### Costs

#### Development
- ⚠️ **2 weeks full-time** work for complete refactoring
- ⚠️ **High cognitive load** - touches core architecture
- ⚠️ **Testing burden** - Must validate all 485+ tests still pass

#### Risk
- ⚠️ **Breaking changes** - All analyzers need updates
- ⚠️ **Subtle bugs** - Edge cases in schema resolution
- ⚠️ **Performance** - Could impact query planning time (minimal)

---

## Recommendation

### Option A: Full Refactoring Now
**Effort**: 1.5-2 weeks  
**Benefit**: Clean architecture, proper separation  
**Risk**: High (breaks many components)

**When to choose:**
- No immediate deadlines
- Want to fix architectural debt
- Planning major features that benefit from clean abstractions

### Option B: Incremental Refactoring
**Effort**: 3-4 weeks (spread over time)  
**Benefit**: Same as Option A, but safer  
**Risk**: Medium (gradual transition)

**Phases:**
1. Week 1: Add `resolved_schema` field, populate in parallel with Scan/ViewScan
2. Week 2: Convert SQL generation to use `resolved_schema`, keep `input` for fallback
3. Week 3: Remove `input` field, force all code to use `resolved_schema`
4. Week 4: Clean up, remove Scan/ViewScan from logical plan

### Option C: Document as Technical Debt
**Effort**: 2-4 hours (documentation)  
**Benefit**: Awareness for future work  
**Risk**: None (status quo)

**When to choose:**
- Focused on feature delivery (xfail fixes)
- Architectural cleanup can wait
- V0.7.0 refactoring cycle

---

## Decision Points

### Questions to Answer

1. **Priority**: Is clean architecture worth 2 weeks now?
2. **Timing**: Do we have runway for major refactoring?
3. **Scope**: Full refactoring or incremental?
4. **Target**: Include in v0.6.x or defer to v0.7.0?

### Next Steps if YES

1. Create detailed design doc for ResolvedNodeSchema
2. Set up feature branch: `refactor/graph-vs-relational-separation`
3. Phase 1: Add resolved_schema field (non-breaking)
4. Phase 2: Migrate analyzers
5. Phase 3: Migrate SQL generation
6. Phase 4: Remove input field
7. Comprehensive testing & validation

### Next Steps if NO (Document Debt)

1. Create technical debt entry in ROADMAP.md
2. Add comments in code: "TODO: Move to resolved_schema (v0.7.0)"
3. Continue with xfail test fixes
4. Revisit after v0.6.0 release
