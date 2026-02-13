# logical_plan — AGENTS.md

> Intermediate representation (IR) between Cypher AST and ClickHouse SQL generation.

## 1. Module Purpose

The `logical_plan` module defines the **core data structures** and **building logic** that transform a parsed Cypher AST into a tree of `LogicalPlan` nodes. This tree is the canonical intermediate representation consumed by:

- **Analyzer passes** (`query_planner/analyzer/`) — rewrite and optimize the plan
- **Render phase** (`render_plan/`) — translate the plan into ClickHouse SQL

The module does NOT execute queries or generate SQL directly.

**Scope**: Read-only analytical queries. No CREATE/SET/DELETE/MERGE support.

```
Cypher Query → Parser → AST → [logical_plan] → LogicalPlan tree → Analyzers → Render → SQL
                               ^^^^^^^^^^^^^^^
                               THIS MODULE
```

## 2. File Inventory (10,055 lines)

| File | Lines | Purpose |
|------|------:|---------|
| `mod.rs` | 1,939 | Core `LogicalPlan` enum, all plan node structs, rebuild_or_clone impls, Display, tests |
| `plan_builder.rs` | 479 | Main entry point `build_logical_plan()`, WITH clause chaining |
| `match_clause/traversal.rs` | 1,669 | MATCH pattern traversal, GraphNode/GraphRel construction |
| `match_clause/view_scan.rs` | 974 | ViewScan generation for nodes and relationships from schema |
| `match_clause/helpers.rs` | 663 | Utility functions (property conversion, scan generation, context registration) |
| `match_clause/tests.rs` | 1,581 | Unit tests for match clause processing |
| `match_clause/schema_filter.rs` | 138 | Property-based schema filtering (Track C optimization) |
| `match_clause/mod.rs` | 46 | Re-exports from match_clause submodules |
| `match_clause/errors.rs` | 8 | Match clause error types |
| `return_clause.rs` | 745 | RETURN → Projection/GroupBy, pattern comprehension rewriting |
| `with_clause.rs` | 579 | WITH → WithClause, scope boundary, pattern comprehension metadata |
| `view_scan.rs` | 306 | `ViewScan` struct definition with all fields |
| `where_clause.rs` | 260 | WHERE → Filter, UNION branch alias rewriting |
| `optional_match_clause.rs` | 245 | OPTIONAL MATCH → LEFT JOIN semantics via `is_optional` flag |
| `unwind_clause.rs` | 171 | UNWIND → Unwind (ARRAY JOIN) |
| `projection_view.rs` | 84 | `Projection` helpers for ViewScan integration |
| `filter_view.rs` | 48 | `Filter` helpers for ViewScan integration |
| `errors.rs` | 46 | `LogicalPlanError` enum |
| `order_by_clause.rs` | 36 | ORDER BY → OrderBy |
| `skip_n_limit_clause.rs` | 38 | SKIP/LIMIT → Skip/Limit |

## 3. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        plan_builder.rs                              │
│  build_logical_plan(ast, schema) → (Arc<LogicalPlan>, PlanCtx)     │
│                                                                     │
│  Processing Order:                                                  │
│  ┌──────────┐   ┌───────────────┐   ┌────────┐   ┌───────┐        │
│  │  MATCH   │──▶│OPTIONAL MATCH │──▶│ UNWIND │──▶│ WITH  │        │
│  │ clauses  │   │   clauses     │   │clauses │   │clause │        │
│  └──────────┘   └───────────────┘   └────────┘   └───┬───┘        │
│                                                       │ recursive  │
│  ┌──────────┐   ┌───────────────┐   ┌────────┐       ▼            │
│  │  WHERE   │◀──│   ORDER BY    │◀──│ RETURN │  (subsequent       │
│  │  clause  │   │   SKIP/LIMIT  │   │ clause │   MATCH/WITH)      │
│  └──────────┘   └───────────────┘   └────────┘                    │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Resulting LogicalPlan Tree                        │
│                                                                     │
│  Projection(friend.name)            ← return_clause.rs              │
│    └─ Filter(u.active = true)       ← where_clause.rs              │
│        └─ GraphRel(f:FOLLOWS)       ← match_clause/traversal.rs    │
│            ├─ left: GraphNode(u)                                    │
│            │   └─ ViewScan(users)   ← match_clause/view_scan.rs    │
│            ├─ center: ViewScan(follows)                             │
│            └─ right: GraphNode(friend)                              │
│                └─ ViewScan(users)                                   │
└─────────────────────────────────────────────────────────────────────┘
```

## 4. LogicalPlan Enum — All Variants

```rust
pub enum LogicalPlan {
    Empty,              // Sentinel / leaf node (no data)
    ViewScan(Arc<ViewScan>),  // Table scan with property mapping
    GraphNode(GraphNode),     // Named graph node pattern
    GraphRel(GraphRel),       // Relationship pattern (3-way: left/center/right)
    Filter(Filter),           // WHERE predicate
    Projection(Projection),   // RETURN / SELECT items
    GroupBy(GroupBy),          // GROUP BY (from aggregation in RETURN/WITH)
    OrderBy(OrderBy),         // ORDER BY
    Skip(Skip),               // OFFSET
    Limit(Limit),             // LIMIT
    Cte(Cte),                 // Named CTE wrapper
    GraphJoins(GraphJoins),   // Computed JOIN plan (from GraphJoinInference analyzer)
    Union(Union),             // UNION / UNION ALL
    PageRank(PageRank),       // PageRank algorithm node
    Unwind(Unwind),           // ARRAY JOIN (UNWIND)
    CartesianProduct(CartesianProduct), // CROSS JOIN / LEFT JOIN for disconnected patterns
    WithClause(WithClause),   // WITH scope boundary + projection
}
```

### Node Hierarchy (inner → outer)

```
Leaf nodes: Empty, ViewScan(Arc<ViewScan>), PageRank
Single-input wrappers: GraphNode, Filter, Projection, GroupBy, OrderBy, Skip, Limit, Cte, Unwind, GraphJoins, WithClause
Multi-input: GraphRel (left/center/right), CartesianProduct (left/right), Union (vec<inputs>)
```

## 5. Key Structs

### ViewScan — Table scan with schema mapping

The foundational leaf node. Maps a ClickHouse table to graph properties.

```rust
pub struct ViewScan {
    pub source_table: String,           // e.g., "brahmand.users_bench"
    pub view_filter: Option<LogicalExpr>, // Pre-applied filter
    pub property_mapping: HashMap<String, PropertyValue>,  // graph_prop → CH column
    pub id_column: String,              // Node/edge ID column
    pub output_schema: Vec<String>,     // Available property names
    pub from_id: Option<String>,        // Relationship source ID column
    pub to_id: Option<String>,          // Relationship target ID column
    pub use_final: bool,                // ClickHouse FINAL keyword
    pub is_denormalized: bool,          // Node embedded in edge table
    pub from_node_properties: Option<HashMap<String, PropertyValue>>,  // Denormalized FROM
    pub to_node_properties: Option<HashMap<String, PropertyValue>>,    // Denormalized TO
    pub type_column: Option<String>,    // Polymorphic edge discriminator
    pub schema_filter: Option<SchemaFilter>, // YAML-defined always-on filter
    pub node_label: Option<String>,     // Label (essential for denormalized nodes)
    // + view_parameter_names/values, type_values, from/to_label_column
}
```

### GraphNode — Named node pattern

```rust
pub struct GraphNode {
    pub input: Arc<LogicalPlan>,    // ViewScan or Empty (denormalized)
    pub alias: String,              // "u", "friend", "t1" (generated)
    pub label: Option<String>,      // "User", "Airport"
    pub is_denormalized: bool,      // Skip CTE/JOIN for this node
    pub projected_columns: Option<Vec<(String, String)>>,  // From GraphJoinInference
    pub node_types: Option<Vec<String>>,  // Multi-type inference candidates
}
```

### GraphRel — Relationship pattern (⚠️ CRITICAL: left/right convention)

```rust
pub struct GraphRel {
    pub left: Arc<LogicalPlan>,     // ALWAYS source node (from_id)
    pub center: Arc<LogicalPlan>,   // Relationship ViewScan
    pub right: Arc<LogicalPlan>,    // ALWAYS target node (to_id)
    pub alias: String,              // "f", "r", "t2"
    pub direction: Direction,       // Original syntactic direction (display only!)
    pub left_connection: String,    // Alias connecting to from_id
    pub right_connection: String,   // Alias connecting to to_id
    pub is_rel_anchor: bool,        // Whether this rel is the FROM table
    pub variable_length: Option<VariableLengthSpec>,  // VLP: *1..3, *2, *
    pub shortest_path_mode: Option<ShortestPathMode>, // shortestPath mode
    pub path_variable: Option<String>,  // "p" from MATCH p = (a)-[]->(b)
    pub where_predicate: Option<LogicalExpr>, // Pushed-down filter
    pub labels: Option<Vec<String>>,    // [:TYPE1|TYPE2] labels
    pub is_optional: Option<bool>,      // OPTIONAL MATCH → LEFT JOIN
    pub anchor_connection: Option<String>, // Base MATCH node for OPTIONAL
    pub cte_references: HashMap<String, String>,  // alias → CTE name
    pub pattern_combinations: Option<Vec<TypeCombination>>, // Multi-type inference
    pub was_undirected: Option<bool>,   // Split from Direction::Either
}
```

### WithClause — Scope boundary + projection

```rust
pub struct WithClause {
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
    pub distinct: bool,
    pub order_by: Option<Vec<OrderByItem>>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
    pub where_clause: Option<LogicalExpr>,
    pub exported_aliases: Vec<String>,  // Visible downstream
    pub cte_name: Option<String>,       // Populated by CteSchemaResolver
    pub cte_references: HashMap<String, String>,
    pub pattern_comprehensions: Vec<PatternComprehensionMeta>,
}
```

### Other Important Structs

| Struct | Key Fields | Purpose |
|--------|-----------|---------|
| `Filter` | `input`, `predicate: LogicalExpr` | WHERE clause conditions |
| `Projection` | `input`, `items: Vec<ProjectionItem>`, `distinct`, `pattern_comprehensions` | RETURN clause (SELECT) |
| `GroupBy` | `input`, `expressions`, `having_clause`, `is_materialization_boundary` | Aggregation grouping |
| `GraphJoins` | `input`, `joins: Vec<Join>`, `optional_aliases`, `anchor_table`, `cte_references`, `correlation_predicates` | Computed JOIN plan |
| `CartesianProduct` | `left`, `right`, `is_optional`, `join_condition` | CROSS JOIN / LEFT JOIN |
| `Union` | `inputs: Vec<Arc<LogicalPlan>>`, `union_type` | UNION ALL / UNION DISTINCT |
| `Unwind` | `input`, `expression`, `alias`, `label`, `tuple_properties` | ARRAY JOIN |
| `Cte` | `input`, `name` | Named CTE wrapper |
| `Join` | `table_name`, `table_alias`, `joining_on`, `join_type`, `pre_filter`, `graph_rel` | Single JOIN operation |
| `ProjectionItem` | `expression: LogicalExpr`, `col_alias: Option<ColumnAlias>` | One SELECT item |
| `VariableLengthSpec` | `min_hops`, `max_hops` | VLP range (*1..3, *2, *) |
| `PatternComprehensionMeta` | `correlation_var`, `direction`, `rel_types`, `agg_type`, `result_alias` | Pattern comprehension metadata for CTE generation |

## 6. Plan Building Flow

### Entry Point

```rust
// In mod.rs:
pub fn evaluate_query(ast, schema, ...) → (Arc<LogicalPlan>, PlanCtx)
pub fn evaluate_cypher_statement(stmt, schema, ...) → (Arc<LogicalPlan>, PlanCtx)

// Delegates to plan_builder.rs:
pub fn build_logical_plan(ast, schema, ...) → (Arc<LogicalPlan>, PlanCtx)
```

### Clause Processing Order (plan_builder.rs)

1. **MATCH / OPTIONAL MATCH** (interleaved via `reading_clauses`)
   - `match_clause::evaluate_match_clause()` → ViewScans + GraphNodes + GraphRels
   - `optional_match_clause::evaluate_optional_match_clause()` → same but with `is_optional=true`
2. **UNWIND** → `unwind_clause::evaluate_unwind_clause()` → Unwind node
3. **WITH** → `with_clause::evaluate_with_clause()` → WithClause node
   - Recursive: `process_with_clause_chain()` handles chained `WITH...MATCH...WITH`
   - Creates child `PlanCtx` scope with only exported aliases
4. **WHERE** → `where_clause::evaluate_where_clause()` → Filter node
5. **RETURN** → `return_clause::evaluate_return_clause()` → Projection (+ GroupBy if aggregation)
6. **ORDER BY** → `order_by_clause::evaluate_order_by_clause()` → OrderBy node
7. **SKIP** → `skip_n_limit_clause::evaluate_skip_clause()` → Skip node
8. **LIMIT** → `skip_n_limit_clause::evaluate_limit_clause()` → Limit node

### MATCH Clause Processing (match_clause/)

```
evaluate_match_clause(match_clause, plan, plan_ctx)
  │
  ├─ For each path_pattern:
  │   ├─ PathPattern::Node → generate_scan() → GraphNode
  │   ├─ PathPattern::ConnectedPattern → traverse_connected_pattern_with_mode()
  │   │   ├─ Pre-assign aliases for shared Rc nodes
  │   │   ├─ For each connected_pattern:
  │   │   │   ├─ Resolve start/end node labels (AST → PlanCtx fallback)
  │   │   │   ├─ Infer relationship types (explicit, property-filtered, or schema-inferred)
  │   │   │   ├─ Handle multi-type patterns (pattern_combinations for PatternResolver 2.0)
  │   │   │   ├─ generate_scan() / generate_denormalization_aware_scan()
  │   │   │   ├─ generate_relationship_center() → ViewScan for edge table
  │   │   │   ├─ Build GraphRel(left, center, right)
  │   │   │   └─ Register aliases in PlanCtx
  │   │   └─ Handle multi-hop: existing plan becomes left/right child
  │   └─ PathPattern::ShortestPath / AllShortestPaths → same flow + shortest_path_mode
  │
  └─ Multiple disconnected patterns → CartesianProduct
```

### ViewScan Generation (match_clause/view_scan.rs)

```
try_generate_view_scan(alias, label, plan_ctx)
  ├─ Denormalized node → UNION of FROM/TO branches from edge tables
  ├─ Multi-table node → UNION ALL of ViewScans
  └─ Standard node → single ViewScan from node table

try_generate_relationship_view_scan(alias, rel_labels, ...)
  ├─ Multiple types → UNION ALL of relationship ViewScans
  └─ Single type → single ViewScan with from_id/to_id

generate_relationship_center(alias, labels, ...)
  └─ Creates ViewScan for the edge/relationship table
```

### UNION Handling (mod.rs: evaluate_cypher_statement)

```
QUERY1 UNION [ALL] QUERY2 UNION [ALL] QUERY3
  ├─ Build plan for each query independently
  ├─ Filter empty branches (Track C optimization)
  ├─ 0 branches → Empty plan
  ├─ 1 branch → unwrap (no UNION needed)
  └─ N branches → Union { inputs, union_type }
```

## 7. Critical Invariants

### ⚠️ GraphRel Left/Right Convention (MUST READ)

```
left = ALWAYS source node (connects to from_id)
right = ALWAYS target node (connects to to_id)

For (a)-[:R]->(b) (Outgoing): left=a, right=b
For (a)<-[:R]-(b) (Incoming): left=b, right=a  ← NODES ARE SWAPPED!
For (a)-[:R]-(b)  (Either):   left=a, right=b  ← Same as Outgoing for storage

The `direction` field is for DISPLAY ONLY. Never use direction-based branching
for from_id/to_id selection in JOIN logic!

Use:
  left_connection → from_id
  right_connection → to_id
```

### WithClause Scope Isolation

- `WithClause.exported_aliases` defines what's visible downstream
- `process_with_clause_chain()` creates a **child PlanCtx** with `is_with_scope=true`
- This child acts as a **barrier** — parent variables not in exported_aliases are hidden
- Child scope is merged back after subsequent MATCH/WITH processing

### ID Generation

- `generate_id()` → "t1", "t2", "t3"... (global `AtomicU32` counter)
- `generate_cte_id()` → "cte1", "cte2"... (separate global counter)
- `reset_alias_counter()` available for tests (non-deterministic in production)

### Empty Branch Filtering

`is_empty_or_filtered_branch()` detects:
- Explicit: `LogicalPlan::Empty`
- Implicit: `GraphRel { labels: None }` or `GraphRel { labels: Some([]) }` (Track C filtered all types)
- Recursive: checks through wrapper nodes (Projection, Filter, etc.)

### Pattern Comprehension Handling

Pattern comprehensions like `size([(a)--() | 1])` are **rewritten during logical planning**:
- In WITH: `rewrite_with_pattern_comprehensions()` → `PatternComprehensionMeta` attached to WithClause
- In RETURN: `rewrite_pattern_comprehensions()` → `PatternComprehensionMeta` attached to Projection
- Render phase consumes metadata to generate CTE + LEFT JOIN SQL
- This avoids creating GroupBy nodes that break in the VLP path

### rebuild_or_clone Pattern

Every plan node implements `rebuild_or_clone()` using the `Transformed<Arc<LogicalPlan>>` type:
- If child transformation occurred → rebuild with new children
- If no transformation → return original `Arc` (zero-cost)
- `handle_rebuild_or_clone()` helper consolidates the pattern
- `any_transformed()` checks multiple children (used by GraphRel)

## 8. Common Bug Patterns

### 1. Direction-based JOIN logic
**Wrong**: Using `GraphRel.direction` to decide which node connects to `from_id`/`to_id`.
**Right**: Always use `left_connection → from_id`, `right_connection → to_id`.

### 2. Labels not found in subsequent patterns
**Problem**: `MATCH (a:X)-[r]->(b:Y), (b)-[r2]->(c)` — second usage of `b` has no label in AST.
**Fix**: Label resolution falls back to PlanCtx: `if let Some(table_ctx) = plan_ctx.get_table_ctx(alias)`.

### 3. Schema not found → silent Empty
**Problem**: Node label not in schema returns `Empty` instead of error.
**Fix**: `generate_scan()` returns `Err(NodeNotFound)` when label lookup fails.

### 4. Stale alias counter in tests
**Problem**: `generate_id()` uses global counter — tests get non-deterministic aliases.
**Fix**: Call `reset_alias_counter()` at test start.

### 5. WITH scope leaking variables
**Problem**: Accessing parent-scope variable after WITH barrier.
**Fix**: `PlanCtx::with_parent_scope(parent, true)` creates proper isolation.

### 6. UNION branch alias conflicts
**Problem**: UNION branches for untyped patterns suffix aliases (e.g., `o_0`, `o_1`).
**Fix**: `where_clause.rs` has `rewrite_predicate_aliases()` that maps base → branch aliases.

### 7. Pattern comprehension breaking VLP
**Problem**: Creating GroupBy nodes for pattern comprehensions interferes with Variable-Length Path processing.
**Fix**: Extract metadata only (`PatternComprehensionMeta`), defer CTE+JOIN to render phase.

### 8. Multi-type patterns generating excessive UNIONs
**Problem**: Fully untyped `(a)-[r]->(b)` generates N×M combinations.
**Mitigation**: Combination pruning via WHERE constraints; `TooManyInferredTypes` error at configurable limit.

## 9. Public API

### Module-Level Functions

```rust
// Main entry points (mod.rs)
pub fn evaluate_query(ast, schema, ...) → LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)>
pub fn evaluate_cypher_statement(stmt, schema, ...) → LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)>

// ID generation (mod.rs)
pub fn generate_id() → String        // "t1", "t2", ...
pub fn generate_cte_id() → String    // "cte1", "cte2", ...
pub fn reset_alias_counter()         // For tests only
```

### Re-exported from match_clause

```rust
// Pattern evaluation
pub fn evaluate_match_clause(clause, plan, ctx) → LogicalPlanResult<Arc<LogicalPlan>>
pub fn evaluate_match_clause_with_optional(clause, plan, ctx, is_optional) → LogicalPlanResult<Arc<LogicalPlan>>

// ViewScan generation
pub fn try_generate_view_scan(alias, label, ctx) → Result<Option<Arc<LogicalPlan>>>
pub fn try_generate_relationship_view_scan(alias, labels, ...) → Result<Option<Arc<LogicalPlan>>>
pub fn generate_relationship_center(alias, labels, ...) → LogicalPlanResult<Arc<LogicalPlan>>

// Helpers
pub fn generate_scan(alias, label, ctx) → LogicalPlanResult<Arc<LogicalPlan>>
pub fn generate_denormalization_aware_scan(alias, label, ctx) → LogicalPlanResult<(Arc<LogicalPlan>, bool)>
pub fn compute_connection_aliases(direction, start, end) → (String, String)
pub fn compute_rel_node_labels(direction, start_label, end_label) → (Option<String>, Option<String>)
pub fn compute_variable_length(rel, labels) → Option<VariableLengthSpec>
pub fn convert_properties(props, alias) → LogicalPlanResult<Vec<LogicalExpr>>
pub fn register_node_in_context(ctx, alias, label, props, is_named)
pub fn register_relationship_in_context(ctx, alias, labels, props, ...)
pub fn register_path_variable(ctx, path_var, graph_rel, rel_alias, shortest_path_mode)
pub fn is_denormalized_scan(plan) → bool
pub fn is_label_denormalized(label, ctx) → bool
pub fn determine_optional_anchor(ctx, is_optional, left, right) → Option<String>
```

### Public Structs & Enums

All structs documented in Section 5 are public. Key types re-exported:
- `LogicalPlan` (enum)
- `ViewScan` (via `pub use view_scan::ViewScan`)
- `LogicalPlanError` (via `pub use errors::LogicalPlanError`)
- All plan node structs: `GraphNode`, `GraphRel`, `Filter`, `Projection`, `GroupBy`, `OrderBy`, `Skip`, `Limit`, `Cte`, `Union`, `Unwind`, `CartesianProduct`, `WithClause`, `GraphJoins`, `Join`, `PageRank`
- Supporting types: `ProjectionItem`, `OrderByItem`, `VariableLengthSpec`, `ShortestPathMode`, `JoinType`, `UnionType`, `PatternComprehensionMeta`, `AggregationType`

## 10. Testing Guidance

### Unit Tests Location

- `mod.rs` tests: `rebuild_or_clone` correctness, AST→LogicalPlan conversions, complex plan structure
- `match_clause/tests.rs` (1,581 lines): MATCH clause patterns, node/relationship creation, VLP
- `optional_match_clause.rs` tests: OPTIONAL MATCH with/without WHERE
- `unwind_clause.rs` tests: UNWIND with literal lists and property access
- `match_clause/helpers.rs` tests: Property conversion, error cases

### Writing New Tests

```rust
// 1. Create test schema
fn setup_test_graph_schema() -> GraphSchema {
    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

// 2. Create PlanCtx
let mut plan_ctx = PlanCtx::new(Arc::new(schema));

// 3. Reset alias counter for deterministic aliases
reset_alias_counter();

// 4. Build plan from AST constructs
let plan = evaluate_match_clause(&match_clause, Arc::new(LogicalPlan::Empty), &mut plan_ctx)?;

// 5. Assert plan structure via pattern matching
match plan.as_ref() {
    LogicalPlan::GraphRel(rel) => {
        assert_eq!(rel.alias, "r");
        assert_eq!(rel.left_connection, "a");
        assert_eq!(rel.right_connection, "b");
    }
    _ => panic!("Expected GraphRel"),
}
```

### Key Patterns to Test

- Single node: `MATCH (n:Label)` → GraphNode wrapping ViewScan
- Single hop: `MATCH (a)-[r:TYPE]->(b)` → GraphRel with left/center/right
- Multi-hop: `MATCH (a)-[r1]->(b)-[r2]->(c)` → Nested GraphRel
- VLP: `MATCH (a)-[*1..3]->(b)` → GraphRel with `variable_length` set
- Shortest path: `shortestPath((a)-[*]->(b))` → GraphRel with `shortest_path_mode`
- UNION types: `MATCH (a)-[:T1|T2]->(b)` → labels = Some(["T1", "T2"])
- No type: `MATCH (a)-[]->(b)` → inferred or multi-type with `pattern_combinations`
- OPTIONAL MATCH: `is_optional = Some(true)`, `anchor_connection` set
- WITH clause: WithClause with `exported_aliases`, scope isolation
- Denormalized: `is_denormalized = true`, Empty input scan

### Integration Test Considerations

- Schema must be loaded (use benchmark schema for consistency)
- Tests that call `build_logical_plan()` need real `GraphSchema`
- Multi-type tests may produce large UNIONs — use `max_inferred_types` parameter
- WithClause tests must verify scope isolation (child scope variables)

## 11. Relationship to Other Modules

```
┌─────────────────────┐     ┌──────────────────────┐
│ open_cypher_parser/ │────▶│  logical_plan/       │
│  (AST types)        │     │  (this module)       │
└─────────────────────┘     └──────────┬───────────┘
                                       │ produces
                                       ▼
                            ┌──────────────────────┐
                            │ analyzer/ passes     │
                            │  TypeInference       │
                            │  GraphJoinInference  │
                            │  BidirectionalUnion  │
                            │  GroupByBuilding     │
                            │  CteSchemaResolver   │
                            │  FilterPushdown      │
                            └──────────┬───────────┘
                                       │ transforms
                                       ▼
                            ┌──────────────────────┐
                            │ render_plan/         │
                            │  (SQL generation)    │
                            └──────────────────────┘

Dependencies:
  ← graph_catalog/graph_schema.rs (schema lookups during ViewScan creation)
  ← logical_expr/ (LogicalExpr, PropertyAccess, operators)
  ← plan_ctx/ (PlanCtx, TableCtx — planning context)
  ← analyzer/match_type_inference (label/type inference functions)
```

## 12. Key Design Decisions

1. **Arc<LogicalPlan> everywhere**: Plans are immutable trees shared via reference counting. `rebuild_or_clone` creates new nodes only when transformation occurs.

2. **GraphRel normalized convention**: Left=source, right=target regardless of syntactic direction. This simplifies all downstream JOIN logic.

3. **Deferred UNION for multi-type patterns**: Instead of expanding untyped patterns into LogicalPlan Union branches immediately, `pattern_combinations` stores all valid (from, rel, to) tuples. CTE generation creates SQL-level UNIONs, avoiding PlanCtx isolation complexity.

4. **WithClause as separate node**: Unlike Projection (RETURN), WithClause carries ORDER BY/SKIP/LIMIT/WHERE and creates scope isolation. This was designed to prevent analyzers from crossing scope boundaries.

5. **Pattern comprehension metadata**: Extracted as `PatternComprehensionMeta` during planning, NOT as plan nodes. This avoids creating GroupBy nodes that interfere with VLP CTE generation.

6. **ViewScan carries full schema context**: Each ViewScan has property mappings, denormalization info, polymorphic edge metadata, and schema filters — everything the render phase needs to generate correct SQL without additional schema lookups.
