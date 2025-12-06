# Unified Schema Abstraction Proposal

## Current Problem

The schema variation handling in ClickGraph has grown organically, leading to:

1. **4,880+ lines** in `graph_join_inference.rs` with scattered conditionals
2. **Multiple detection functions** that get called repeatedly
3. **Inconsistent terminology** (denormalized, traditional, polymorphic, coupled, mixed)
4. **Ping-ponging bugs** - fixing one schema type breaks another

### Current Decision Points Spread Across Code

```
├── is_node_denormalized_on_edge() - check if node uses edge table
├── edge_has_node_properties() - check if edge has node props  
├── is_fully_denormalized_edge_table() - check if BOTH nodes denormalized
├── classify_edge_table_pattern() - Traditional/FullyDenormalized/Mixed
├── are_edges_coupled() - check if two edges share table and coupling node
├── rel_schema.type_column.is_some() - polymorphic detection
├── left_is_polymorphic_any/right_is_polymorphic_any - $any node checks
└── detect_vlp_schema_type() - VLP-specific schema classification
```

Each of these gets checked at different points, leading to complex nested conditionals.

---

## Proposed Unified Abstraction

### Core Insight

Every graph pattern has a **data access strategy** for each component:
- **Node**: Where do we get node properties? (own table / edge table / virtual)
- **Edge**: Where is the edge? (separate table / node table / polymorphic table)
- **Join**: How do we connect them? (explicit JOIN / implicit same-row / none needed)

### Unified Schema Context

```rust
/// Complete schema context for a graph pattern, computed ONCE at pattern analysis
#[derive(Debug, Clone)]
pub struct PatternSchemaContext {
    /// Left node access strategy
    pub left_node: NodeAccessStrategy,
    /// Right node access strategy  
    pub right_node: NodeAccessStrategy,
    /// Edge access strategy
    pub edge: EdgeAccessStrategy,
    /// Join strategy for this pattern
    pub join_strategy: JoinStrategy,
    /// Coupled edge info (if applicable)
    pub coupled_context: Option<CoupledEdgeContext>,
}

/// How to access node data
#[derive(Debug, Clone, PartialEq)]
pub enum NodeAccessStrategy {
    /// Node has its own table, JOIN required
    OwnTable {
        table: String,
        id_column: String,
        properties: PropertyMappings,
    },
    /// Node properties embedded in edge table
    EmbeddedInEdge {
        edge_alias: String,
        properties: PropertyMappings,
        is_from_node: bool,
    },
    /// Virtual node (e.g., in polymorphic $any patterns)
    Virtual {
        label: String,
    },
}

/// How to access edge data
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeAccessStrategy {
    /// Standard separate edge table
    SeparateTable {
        table: String,
        from_id: String,
        to_id: String,
        properties: PropertyMappings,
    },
    /// Polymorphic edge with type discriminator
    Polymorphic {
        table: String,
        from_id: String,
        to_id: String,
        type_column: String,
        type_values: Vec<String>,
        from_label_column: Option<String>,
        to_label_column: Option<String>,
    },
    /// Edge table IS the node table (FK-edge pattern)
    FkEdge {
        node_table: String,
        fk_column: String,
    },
}

/// How to generate JOINs for this pattern
#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy {
    /// Traditional: JOIN node → edge → node
    Traditional {
        left_join_col: String,
        right_join_col: String,
    },
    /// Fully denormalized: Single table scan, no JOINs
    SingleTableScan {
        table: String,
    },
    /// Mixed: One node JOINed, other embedded
    MixedAccess {
        joined_node: NodePosition,
        join_col: String,
    },
    /// Multi-hop denormalized: Edge-to-edge JOIN
    EdgeToEdge {
        prev_edge_col: String,
        curr_edge_col: String,
    },
    /// Coupled edges: Same table row, alias unification
    CoupledSameRow {
        unified_alias: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodePosition {
    Left,
    Right,
}
```

---

## Benefits of Unified Abstraction

### 1. Single Point of Schema Analysis

Instead of scattered checks throughout `graph_join_inference.rs`:

```rust
// BEFORE (scattered throughout 4800+ lines)
let left_is_denormalized = is_node_denormalized_on_edge(&left_node, &rel_schema, true);
let right_has_props = edge_has_node_properties(&rel_schema, false);
let edge_is_fully_denormalized = left_is_denormalized && right_is_denormalized || ...
let edges_are_coupled = graph_schema.are_edges_coupled(&prev_rel_type, rel_type);
// ... repeated at multiple locations

// AFTER (computed once at the start)
let schema_ctx = PatternSchemaContext::analyze(
    &left_node_schema,
    &right_node_schema, 
    &rel_schema,
    &graph_schema,
    plan_ctx
);

// Then use simple pattern matching
match schema_ctx.join_strategy {
    JoinStrategy::SingleTableScan { table } => { /* denormalized path */ }
    JoinStrategy::Traditional { .. } => { /* standard path */ }
    JoinStrategy::EdgeToEdge { .. } => { /* multi-hop denormalized */ }
    JoinStrategy::CoupledSameRow { .. } => { /* coupled optimization */ }
    _ => { /* other cases */ }
}
```

### 2. Exhaustive Pattern Matching

Rust's exhaustive `match` ensures we handle ALL cases:

```rust
match (schema_ctx.left_node, schema_ctx.right_node) {
    (NodeAccessStrategy::OwnTable { .. }, NodeAccessStrategy::OwnTable { .. }) => {
        // Traditional: both nodes have own tables
    }
    (NodeAccessStrategy::EmbeddedInEdge { .. }, NodeAccessStrategy::EmbeddedInEdge { .. }) => {
        // Fully denormalized: both nodes in edge table
    }
    (NodeAccessStrategy::OwnTable { .. }, NodeAccessStrategy::EmbeddedInEdge { .. }) |
    (NodeAccessStrategy::EmbeddedInEdge { .. }, NodeAccessStrategy::OwnTable { .. }) => {
        // Mixed: one embedded, one separate
    }
    (NodeAccessStrategy::Virtual { .. }, _) | (_, NodeAccessStrategy::Virtual { .. }) => {
        // Polymorphic $any pattern
    }
}
```

No more forgetting edge cases!

### 3. Clear Separation of Concerns

```
┌─────────────────────────────────────────────────────────────┐
│                   Schema Analysis Layer                      │
│  (graph_catalog/pattern_schema.rs - NEW)                    │
│                                                             │
│  analyze_pattern() → PatternSchemaContext                   │
│  - Computes NodeAccessStrategy for each node                │
│  - Computes EdgeAccessStrategy                              │
│  - Determines JoinStrategy                                  │
│  - Detects coupled edges                                    │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    Query Planning Layer                      │
│  (query_planner/analyzer/graph_join_inference.rs)           │
│                                                             │
│  Uses PatternSchemaContext to:                              │
│  - Generate appropriate JOINs                               │
│  - Resolve property accesses                                │
│  - Handle multi-hop patterns                                │
│  NO schema detection logic here!                            │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    SQL Generation Layer                      │
│  (clickhouse_query_generator/)                              │
│                                                             │
│  Uses JoinStrategy from context to generate SQL             │
│  NO schema detection logic here!                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Create Unified Types (Low Risk) ✅ DONE

1. Add `PatternSchemaContext` and related types to `src/graph_catalog/pattern_schema.rs`
2. Implement `PatternSchemaContext::analyze()` using existing detection functions
3. Add comprehensive unit tests for all schema combinations

### Phase 2: Parallel Path (Safe Refactor) ✅ DONE

1. Add new `compute_pattern_context()` that bridges GraphRel → PatternSchemaContext
2. Add logging to compare new vs old detection logic
3. Wire into `infer_graph_join()` entry point

### Phase 3: Add `handle_graph_pattern_v2()` ✅ DONE

1. Create `handle_graph_pattern_v2()` using exhaustive pattern matching
2. Handle all `JoinStrategy` variants with clean match arms
3. Add FK-edge pattern support (`FkEdgeJoin` strategy)

### Phase 4: Wire Up and Validate (IN PROGRESS)

1. Wire `handle_graph_pattern_v2()` to `infer_graph_join()`
2. Test all schema variations
3. Remove old code path once validated

### Phase 5: Cleanup

1. Remove duplicate detection functions
2. Update documentation
3. Archive old investigation notes

---

## Complete Schema Variation Matrix

The unified abstraction handles ALL these combinations:

### Node Data Location Strategies

| Strategy | Description | SQL Pattern | Example |
|----------|-------------|-------------|---------|
| `OwnTable` | Node has dedicated table | `JOIN node_table` | users, posts |
| `EmbeddedInEdge` | Properties in edge table | Read from edge columns | airports in flights |
| `Virtual` | Polymorphic $any | Runtime label resolution | `?` node in polymorphic query |

### Edge Data Location Strategies

| Strategy | Description | SQL Pattern | Example |
|----------|-------------|-------------|---------|
| `SeparateTable` | Standard edge table | `FROM edge_table` | follows, likes |
| `Polymorphic` | Type discriminator column | `WHERE type = 'X'` | interactions table |
| `FkEdge` | FK column on node table | No separate edge table | parent_id, customer_id |

### Join Strategies

| Strategy | Description | SQL Generated | When Used |
|----------|-------------|---------------|-----------|
| `Traditional` | node→edge→node | 2-3 JOINs | Separate tables |
| `SingleTableScan` | All from one table | No JOINs | Fully denormalized |
| `MixedAccess` | One node embedded | 1 JOIN | Mixed schema |
| `EdgeToEdge` | Edge-to-edge JOIN | `e2.col = e1.col` | Multi-hop denormalized |
| `CoupledSameRow` | Alias unification | No additional JOIN | Coupled edges |
| `FkEdgeJoin` | FK self-join | `child.fk = parent.id` | Hierarchical/FK patterns |

### Full Combination Matrix

| # | Left Node | Right Node | Edge | Join Strategy | Real-World Example |
|---|-----------|------------|------|---------------|-------------------|
| 1 | OwnTable | OwnTable | Separate | Traditional | `(u:User)-[:FOLLOWS]->(f:User)` |
| 2 | OwnTable | OwnTable | Polymorphic | Traditional+Filter | `(u:User)-[:LIKES]->(p:Post)` |
| 3 | OwnTable | OwnTable | FkEdge (self) | FkEdgeJoin | `(c:Object)-[:PARENT]->(p:Object)` |
| 4 | OwnTable | OwnTable | FkEdge (cross) | FkEdgeJoin | `(o:Order)-[:PLACED_BY]->(c:Customer)` |
| 5 | Embedded | Embedded | Same Table | SingleTableScan | `(a:Airport)-[:FLIGHT]->(b:Airport)` |
| 6 | Embedded | Embedded | Same (multi-hop) | EdgeToEdge | `(a)-[f1:FLIGHT]->(b)-[f2:FLIGHT]->(c)` |
| 7 | Embedded | Embedded | Coupled | CoupledSameRow | `(ip)-[:DNS]->(domain)-[:RESOLVED]->(resolved_ip)` |
| 8 | OwnTable | Embedded | Mixed | MixedAccess(Right) | `(u:User)-[:PILOTED]->(a:Airport)` |
| 9 | Embedded | OwnTable | Mixed | MixedAccess(Left) | `(a:Airport)-[:PILOTED_BY]->(u:User)` |
| 10 | Virtual | OwnTable | Polymorphic | Traditional+Filter | `(?)-[:INTERACTION]->(u:User)` |
| 11 | OwnTable | Virtual | Polymorphic | Traditional+Filter | `(u:User)-[:INTERACTION]->(?)` |
| 12 | Virtual | Virtual | Polymorphic | Varies | `(?)-[:INTERACTION]->(?)` |

### UNION ALL Cases (Handled at CTE Level, Not Join Level)

| Pattern | Description | SQL Generated |
|---------|-------------|---------------|
| `[:TYPE1\|TYPE2]` | Multiple rel types | `UNION ALL` of type CTEs |
| `(a)-[r]-(b)` | Bidirectional | `UNION ALL` of both directions |
| `(n:Label)` standalone | Multi-source label | `UNION ALL` across sources |

---

## Key Abstraction Principle

**Where is the table and properties?** → Determines `NodeAccessStrategy` and `EdgeAccessStrategy`

**What SQL operation is needed?** → Determines `JoinStrategy`:
- **JOIN**: Separate tables need explicit joins
- **UNION ALL**: Multiple sources/types need union (handled at CTE level)
- **None**: Same table, just read columns

---

## Immediate Benefits

1. **Bug Prevention**: Exhaustive matching prevents "forgot this case" bugs
2. **Readability**: Clear intent, no detective work to understand what schema type we're handling
3. **Testability**: Each `PatternSchemaContext` can be unit tested independently
4. **Maintainability**: New schema types = add enum variant, compiler shows all places to update

---

## Questions to Consider

1. Should `PatternSchemaContext` be computed during parsing or planning?
2. How do we handle multi-hop patterns where each hop may have different strategies?
3. Should we cache `PatternSchemaContext` in `PlanCtx`?
4. How does this interact with OPTIONAL MATCH and WHERE clause filtering?

---

## Conclusion

The current scattered approach with multiple detection functions and nested conditionals has led to:
- Complex, hard-to-reason-about code
- Ping-pong bugs when fixing one schema type
- Fear of touching the code

A unified `PatternSchemaContext` abstraction would:
- Centralize schema analysis
- Enable exhaustive pattern matching
- Dramatically reduce code complexity
- Make the codebase more maintainable

The refactor can be done incrementally with parallel paths to ensure safety.
