# optimizer Module — Agent Guide

> **Purpose**: Transforms `LogicalPlan` trees to improve query execution efficiency
> before SQL generation. Provides a composable pipeline of optimization passes that
> embed filters, extract join conditions, eliminate redundant operations, and apply
> schema-aware optimizations. Passes run in two phases (initial + final) with some
> passes invoked separately by the analyzer.

## Module Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │          query_planner/analyzer/mod.rs       │
                        │  (orchestrates the full optimization flow)   │
                        └──────────┬──────────────────┬────────────────┘
                                   │                  │
                    ┌──────────────▼──────┐   ┌───────▼──────────────────┐
                    │ initial_optimization │   │  Analyzer-invoked passes │
                    │   (mod.rs)           │   │  (called from analyzer)  │
                    └──────────┬──────────┘   └───────┬──────────────────┘
                               │                      │
              ┌────────────────┼──────────┐    ┌──────┼───────────────┐
              │                │          │    │      │               │
              ▼                ▼          │    ▼      ▼               │
  CartesianJoin    FilterIntoGraphRel     │  CollectUnwind   TrivialWith    │
  Extraction                              │  Elimination     Elimination    │
                                          │                                │
                    ┌─────────────────────▼────────────────────────────────┘
                    │  final_optimization  │
                    │    (mod.rs)           │
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ▼                    ▼                    ▼
  ProjectionPushDown  CleanupViewScan      FilterPushDown
                      Filters                      │
                                                   ▼
                                            ViewOptimizer
```

### Non-Pipeline Components

```
union_pruning.rs  ← Standalone utility (not an OptimizerPass)
                     Called from query_planner/logical_plan/match_clause/traversal.rs
                     Extracts label hints from id() WHERE patterns for UNION branch pruning
```

## Key Files & Line Counts

| File | Lines | Role |
|------|------:|------|
| filter_into_graph_rel.rs | 1,149 | Embed filters into GraphRel/ViewScan nodes |
| cartesian_join_extraction.rs | 766 | Extract cross-pattern filters into JOIN conditions |
| collect_unwind_elimination.rs | 572 | Remove redundant collect()+UNWIND sequences |
| view_optimizer.rs | 349 | Schema-aware ViewScan optimizations (filter simplification) |
| cleanup_viewscan_filters.rs | 340 | Remove duplicate ViewScan filters inside GraphRel |
| trivial_with_elimination.rs | 296 | Remove pass-through WITH clauses |
| union_pruning.rs | 225 | Extract label info from id() patterns for UNION pruning |
| mod.rs | 200 | Pipeline orchestration: initial_optimization + final_optimization |
| filter_push_down.rs | 169 | Push filters toward data sources |
| projection_push_down.rs | 152 | Eliminate unused columns early |
| optimizer_pass.rs | 34 | OptimizerPass trait definition |
| errors.rs | 43 | Error types (OptimizerError enum) |

## Optimization Pipeline — Execution Order

### Phase 1: initial_optimization (mod.rs)

Called by the query planner before analyzer passes. Two passes in strict order:

| Order | Pass | Why This Order |
|:-----:|------|----------------|
| 1 | **CartesianJoinExtraction** | Must run BEFORE FilterIntoGraphRel to prevent cross-pattern filters from being pushed into a single GraphRel |
| 2 | **FilterIntoGraphRel** | Embeds remaining filters into GraphRel.where_predicate and ViewScan.view_filter |

### Phase 2: Analyzer-invoked passes (analyzer/mod.rs)

Called between initial and final optimization, inside the analyzer pipeline:

| Order | Pass | Why Here |
|:-----:|------|----------|
| 3 | **CollectUnwindElimination** | Runs after CTE resolution but before final optimization |
| 4 | **TrivialWithElimination** | Runs after CollectUnwindElimination to clean up remaining trivial WITH clauses |

### Phase 3: final_optimization (mod.rs)

Called after analyzer passes. Four passes in strict order:

| Order | Pass | Why This Order |
|:-----:|------|----------------|
| 5 | **ProjectionPushDown** | Eliminates unused columns before filter processing |
| 6 | **CleanupViewScanFilters** | Must run AFTER FilterIntoGraphRel (from Phase 1) — removes redundant ViewScan.view_filter that was already consolidated into GraphRel.where_predicate |
| 7 | **FilterPushDown** | Pushes remaining filters toward scans |
| 8 | **ViewOptimizer** | Final schema-aware optimizations (filter simplification, property access optimization) |

### Standalone: union_pruning (not a pipeline pass)

Called directly from `query_planner/logical_plan/match_clause/traversal.rs` during MATCH
clause planning. Not an `OptimizerPass` implementation — it's a utility function that
operates on the AST `WhereClause`, not the `LogicalPlan`.

## Each Optimization Pass in Detail

### 1. CartesianJoinExtraction

**File**: `cartesian_join_extraction.rs` (766 lines)
**Phase**: initial_optimization (runs first)
**Depends on**: Nothing (first pass in pipeline)

**What it does**: Finds `Filter → CartesianProduct` patterns where the filter predicate
references aliases from both sides of the CartesianProduct. Extracts those predicates as
`CartesianProduct.join_condition`, enabling `JOIN ... ON` instead of `CROSS JOIN + WHERE`.

**Patterns handled**:
- `Filter → CartesianProduct` (direct)
- `Filter → WithClause(CartesianProduct)` (through WITH)

**Critical behavior**:
- Splits AND expressions: cross-pattern conditions → `join_condition`, single-sided → remain as Filter
- **Correlated subqueries** (NOT PathPattern, EXISTS, size()) MUST stay in WHERE clause — ClickHouse
  doesn't support correlated subqueries in JOIN ON clauses
- Uses `partition_filter_conditions()` for the split logic
- Uses `collect_aliases_from_plan()` to determine which aliases belong to left/right sides
- Also collects `col_alias` from Projection/WithClause items (critical for WITH-defined aliases)

**Helper functions**:
- `collect_aliases_from_expr()` — extract table aliases referenced in an expression
- `collect_aliases_from_plan()` — collect all node aliases defined in a plan subtree
- `partition_filter_conditions()` — split predicates into join conditions vs WHERE filters

### 2. FilterIntoGraphRel

**File**: `filter_into_graph_rel.rs` (1,149 lines)
**Phase**: initial_optimization (runs second)
**Depends on**: CartesianJoinExtraction (cross-pattern filters should be extracted first)

**What it does**: Embeds filter predicates directly into `GraphRel.where_predicate` and
`ViewScan.view_filter` fields, enabling more efficient CTE generation and predicate pushdown.

**Patterns handled** (in match order):
1. `Filter → Projection → GraphRel` — merge filter into GraphRel.where_predicate, remove Filter wrapper
2. `Filter → GraphRel` (direct) — same as above without Projection layer
3. `Filter → Projection → ViewScan` — push filter into ViewScan.view_filter
4. `Filter → ViewScan` (direct) — push filter into ViewScan.view_filter
5. `GraphNode(ViewScan)` — inject filters from PlanCtx for this alias into the ViewScan
6. `Projection(ViewScan)` — match alias labels to ViewScan source tables via schema lookup
7. `Projection(GraphNode(ViewScan))` — inject filters via GraphNode alias, respects optional aliases
8. `GraphRel` (direct visit) — collect filters from PlanCtx for left/right/edge aliases

**Filter sources**:
- Explicit `Filter` nodes in the plan tree
- `PlanCtx` table contexts — filters stored per-alias by the FilterTagging analyzer

**Critical behaviors**:
- Merges with existing `where_predicate` using AND when GraphRel already has one
- Qualifies `Column` expressions with table alias via `qualify_columns_with_alias()`
- Skips left_connection filters when left child is also a GraphRel (multi-hop — inner GraphRel handles it)
- Skips filter injection for optional aliases (filters should be JOIN conditions, not WHERE)
- Tracks `collected_aliases` to avoid duplicate filter injection

**⚠️ MUST NOT run twice** — running FilterIntoGraphRel in both initial and final optimization
causes duplicate filters. The comment in `final_optimization` explicitly warns about this.

### 3. CollectUnwindElimination

**File**: `collect_unwind_elimination.rs` (572 lines)
**Phase**: Analyzer-invoked (analyzer/mod.rs, after CTE resolution)
**Depends on**: CTE resolution must have completed

**What it does**: Detects and removes redundant `collect() + UNWIND` patterns that cancel
each other out. Provides 2-5x performance improvement for affected queries.

**Pattern detected**:
```cypher
-- Before (redundant collect+unwind):
MATCH (a)-[r]->(b) WITH a, collect(b) as bs UNWIND bs as b RETURN b.name
-- After (eliminated):
MATCH (a)-[r]->(b) RETURN b.name
```

**Two elimination modes**:
1. **Simple**: WITH only contains `collect(x) as xs` → eliminate both WITH and UNWIND entirely
2. **Complex**: WITH has other items alongside collect → keep WITH minus the collect item, remove UNWIND

**Alias rewriting**: When eliminating, builds an alias map (`unwound_alias → source_alias`)
and recursively rewrites all references in downstream Projection, Filter, OrderBy, GroupBy nodes
via `rewrite_aliases_in_expr()`.

**Does NOT implement OptimizerPass.optimize() conventionally** — uses `optimize_node()` which
returns `(Arc<LogicalPlan>, HashMap<String, String>)` to propagate alias mappings upward.

### 4. TrivialWithElimination

**File**: `trivial_with_elimination.rs` (296 lines)
**Phase**: Analyzer-invoked (analyzer/mod.rs, after CollectUnwindElimination)
**Depends on**: CollectUnwindElimination (may create trivial WITHs after removing collect)

**What it does**: Removes WITH clauses that are pure pass-throughs adding no value.

**A WITH is trivial if ALL of**:
- No ORDER BY, SKIP, LIMIT, WHERE clause
- Not DISTINCT
- All items are simple `TableAlias` expressions (no aggregations, functions, or operators)

**Patterns handled**:
- `Projection → WithClause(trivial)` → skip WITH, connect Projection to WITH's input
- `WithClause → WithClause(trivial)` → skip inner WITH, keep outer WITH with inner's input

### 5. ProjectionPushDown

**File**: `projection_push_down.rs` (152 lines)
**Phase**: final_optimization (runs first in final phase)
**Depends on**: FilterIntoGraphRel (filters should be embedded before projecting)

**What it does**: Currently a recursive tree walker that propagates through all plan node
types. The actual column elimination logic (reducing ViewScan output schemas to only
referenced columns) is a TODO — the infrastructure is in place but the optimization itself
is primarily structural pass-through.

### 6. CleanupViewScanFilters

**File**: `cleanup_viewscan_filters.rs` (340 lines)
**Phase**: final_optimization (runs after ProjectionPushDown)
**Depends on**: FilterIntoGraphRel (from initial_optimization)

**What it does**: Removes `ViewScan.view_filter` fields that became redundant after
FilterIntoGraphRel consolidated filters into `GraphRel.where_predicate`.

**Context-aware clearing**:
- ViewScans **inside** a GraphRel subtree → clear `view_filter` (redundant with GraphRel.where_predicate)
- ViewScans **outside** a GraphRel (node-only queries) → **keep** `view_filter` (no GraphRel to hold it)

**Implementation**: Uses `optimize_with_context(plan, ctx, inside_graph_rel: bool)` to track
whether current traversal is inside a GraphRel subtree. Sets `inside_graph_rel = true` when
entering a GraphRel node, propagates to all its children (left, center, right).

### 7. FilterPushDown

**File**: `filter_push_down.rs` (169 lines)
**Phase**: final_optimization (runs after CleanupViewScanFilters)
**Depends on**: CleanupViewScanFilters

**What it does**: Currently a recursive tree walker that propagates through all plan node
types using `rebuild_or_clone`. Like ProjectionPushDown, the advanced filter movement logic
(pushing filters through joins, aggregations, etc.) is infrastructure-ready but the deep
pushdown strategy is a TODO.

**Note on ViewScan**: Has a commented TODO for merging additional filters into
`ViewScan.view_filter` respecting view mappings and property transformations.

### 8. ViewOptimizer

**File**: `view_optimizer.rs` (349 lines)
**Phase**: final_optimization (runs last)
**Depends on**: All prior passes

**What it does**: Schema-aware optimizations on ViewScan nodes:
1. **Property access optimization** — calls `ViewScan.optimize_property_access()`
2. **Filter simplification** — flattens nested AND expressions: `(A AND B) AND C → A AND B AND C`
3. **Join order optimization** — placeholder for future selectivity-based reordering

**Configuration flags**:
- `enable_filter_pushdown: bool` (default: true)
- `enable_property_optimization: bool` (default: true)
- `enable_join_optimization: bool` (default: true, but logic is TODO)

### union_pruning (Standalone Utility)

**File**: `union_pruning.rs` (225 lines)
**Phase**: Not a pipeline pass — called from `logical_plan/match_clause/traversal.rs`
**Depends on**: `utils::id_encoding::IdEncoding` for ID decoding

**What it does**: Extracts node label information from WHERE clause `id(var) IN [...]`
patterns to prune unnecessary UNION branches.

**Problem solved**: `MATCH (a)-[r]->(b) WHERE id(a) IN [281474976710657]` would generate
UNION of ALL relationship types. By decoding the ID to extract the label (e.g., User),
only User-related relationship branches are generated.

**Patterns recognized**:
- `id(var) IN [id1, id2, ...]` — extract labels from each ID via `IdEncoding::decode_with_label()`
- `id(var) = id_value` — single ID equality
- Recurses through AND/OR operators
- Respects NOT negation (skips extraction inside NOT)

**Returns**: `HashMap<String, HashSet<String>>` — variable name → set of possible labels

## Critical Invariants

### 1. FilterIntoGraphRel Must Run Exactly Once
Running it in both `initial_optimization` and `final_optimization` causes duplicate
filters in the generated SQL. The comment in `final_optimization` explicitly warns:
```rust
// FilterIntoGraphRel already ran in initial_optimization - don't run it again!
// Running it twice causes duplicate filters.
```

### 2. CartesianJoinExtraction Must Run Before FilterIntoGraphRel
Cross-pattern filters (referencing aliases from both sides of a CartesianProduct) must
be extracted as join conditions BEFORE FilterIntoGraphRel embeds them into a single
GraphRel. Otherwise, the join semantics are lost.

### 3. CleanupViewScanFilters Must Run After FilterIntoGraphRel
This pass clears ViewScan.view_filter only inside GraphRel contexts. If it runs before
FilterIntoGraphRel, it clears filters that haven't been consolidated yet, losing them.

### 4. Correlated Subqueries Cannot Be JOIN Conditions
ClickHouse limitation: correlated subqueries (NOT PathPattern, EXISTS, size()) must stay
in WHERE clause. `partition_filter_conditions()` in CartesianJoinExtraction checks
`contains_not_path_pattern()` and forces such predicates into remaining filters.

### 5. Optional Aliases Must Not Get WHERE Filters
Filters on optional (`OPTIONAL MATCH`) aliases must become JOIN conditions, not WHERE
predicates. FilterIntoGraphRel skips filter injection for aliases in
`plan_ctx.get_optional_aliases()`.

### 6. Multi-Hop GraphRel Left Connection Filters Belong to Inner GraphRel
When a GraphRel's left child is also a GraphRel (multi-hop pattern), the left_connection's
filters should be handled by the inner GraphRel, not the outer one. FilterIntoGraphRel
checks `matches!(graph_rel.left.as_ref(), LogicalPlan::GraphRel(_))` and skips.

### 7. Transformed Enum Correctness
All passes must return `Transformed::Yes(plan)` when the plan was modified and
`Transformed::No(plan)` when unchanged. This is used by `rebuild_or_clone()` helpers
to avoid unnecessary allocations. Getting this wrong causes subtle bugs where changes
are silently dropped.

### 8. CollectUnwindElimination Alias Maps Must Propagate
When eliminating collect+UNWIND, the alias map must propagate upward through all
intervening plan nodes. Missing propagation causes "column not found" errors when
downstream expressions reference the UNWIND alias that no longer exists.

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| Duplicate filters in SQL | Same WHERE condition appears twice | FilterIntoGraphRel ran twice (initial + final) |
| CROSS JOIN instead of JOIN ON | Cartesian product where JOIN expected | CartesianJoinExtraction didn't run or didn't detect cross-pattern filter |
| Filter lost for node-only queries | `MATCH (n:Label) WHERE n.x = 1` returns all rows | FilterIntoGraphRel missing ViewScan handler; CleanupViewScanFilters clearing outside GraphRel |
| Correlated subquery in JOIN ON | ClickHouse error / wrong results | `partition_filter_conditions` not checking `contains_not_path_pattern()` |
| Optional alias filtered in WHERE | LEFT JOIN returns wrong results (like INNER JOIN) | FilterIntoGraphRel not checking `plan_ctx.get_optional_aliases()` |
| Column not found after elimination | "Unknown column" in SQL | CollectUnwindElimination alias map not propagated through all node types |
| Filter on wrong GraphRel | Multi-hop: filter applied at wrong hop level | Left connection filter not skipped when left child is GraphRel |
| ViewScan filter cleared incorrectly | Node-only query filter lost | CleanupViewScanFilters set `inside_graph_rel = true` for non-GraphRel parent |
| Trivial WITH incorrectly eliminated | WITH with aggregation removed | `is_trivial_with()` not checking all expression types |

## Public API

### Pipeline Functions (mod.rs)

```rust
/// Phase 1: CartesianJoinExtraction → FilterIntoGraphRel
pub fn initial_optimization(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> OptimizerResult<Arc<LogicalPlan>>;

/// Phase 3: ProjectionPushDown → CleanupViewScanFilters → FilterPushDown → ViewOptimizer
pub fn final_optimization(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> OptimizerResult<Arc<LogicalPlan>>;
```

### OptimizerPass Trait (optimizer_pass.rs)

```rust
pub type OptimizerResult<T> = Result<T, OptimizerError>;

pub trait OptimizerPass {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>>;
}
```

### Pass Constructors

| Pass | Constructor | Visibility |
|------|-------------|------------|
| `CartesianJoinExtraction` | `::new()` | `pub` (used in mod.rs) |
| `FilterIntoGraphRel` | `::new()` | `pub` (used in mod.rs) |
| `ProjectionPushDown` | `::new()` | `pub(crate)` (used in mod.rs) |
| `FilterPushDown` | `::new()` | `pub(crate)` (used in mod.rs) |
| `ViewOptimizer` | `::new()` / `::default()` | `pub(crate)` (used in mod.rs) |
| `CleanupViewScanFilters` | unit struct | `pub(crate)` (used in mod.rs) |
| `CollectUnwindElimination` | unit struct | `pub` (used in analyzer/mod.rs) |
| `TrivialWithElimination` | unit struct | `pub` (used in analyzer/mod.rs) |

### Standalone Functions

```rust
/// Extract node labels from WHERE clause id() patterns (union_pruning.rs)
pub fn extract_labels_from_id_where(
    where_clause: &ast::WhereClause<'_>,
) -> HashMap<String, HashSet<String>>;
```

### Error Types (errors.rs)

```rust
pub enum OptimizerError {
    CombineFilterPredicate,        // Failed to combine filter predicates
    PlanCtx { pass: Pass, source: PlanCtxError },  // PlanCtx error in a specific pass
}

pub enum Pass {
    ProjectionPushDown,
    FilterPushDown,
}
```

## Dependencies

### Upstream (optimizer imports from)
- `query_planner::logical_plan` — `LogicalPlan`, `GraphRel`, `Filter`, `Projection`, `ViewScan`, etc.
- `query_planner::logical_expr` — `LogicalExpr`, `Operator`, `PropertyAccess`, `TableAlias`, etc.
- `query_planner::plan_ctx::PlanCtx` — filter storage per alias, optional alias tracking, schema access
- `query_planner::transformed::Transformed` — `Yes`/`No` enum for tracking plan modifications
- `graph_catalog::expression_parser::PropertyValue` — for `qualify_columns_with_alias()`
- `open_cypher_parser::ast` — AST types in `union_pruning.rs`
- `utils::id_encoding::IdEncoding` — ID decoding in `union_pruning.rs`

### Downstream (who imports optimizer)
- `query_planner::analyzer::mod.rs` — calls `CollectUnwindElimination`, `TrivialWithElimination`
- `query_planner::logical_plan::match_clause::traversal.rs` — calls `extract_labels_from_id_where()`
- `query_planner/mod.rs` (or equivalent orchestrator) — calls `initial_optimization()`, `final_optimization()`

## Testing Guidance

### Running Tests

```bash
# All optimizer unit tests
cargo test --lib optimizer

# Specific pass tests
cargo test --lib cartesian_join_extraction
cargo test --lib collect_unwind_elimination
cargo test --lib trivial_with_elimination
cargo test --lib view_optimizer
cargo test --lib union_pruning
cargo test --lib filter_into_graph_rel

# Full test suite (includes integration tests that exercise optimization)
cargo test
```

### Test Coverage Notes

| Pass | Unit Tests | Notes |
|------|:----------:|-------|
| CartesianJoinExtraction | 1 test | Tests `collect_aliases_from_expr()` helper. Full coverage via integration tests. |
| FilterIntoGraphRel | 2 tests | Minimal — tests parse + documents PlanCtx pattern. Complex struct setup needed for full unit tests. Relies on integration tests (test_where_simple.py). |
| CollectUnwindElimination | 2 tests | Good coverage: simple + complex elimination patterns with plan construction. |
| TrivialWithElimination | 3 tests | Good coverage: trivial detection, DISTINCT rejection, aggregation rejection. |
| ViewOptimizer | 4 tests | Constructor, filter simplification, AND flattening, ViewScan optimization. |
| CleanupViewScanFilters | 0 tests | No unit tests. Behavior validated by integration tests. |
| FilterPushDown | 0 tests | No unit tests. Structural pass-through validated by integration tests. |
| ProjectionPushDown | 0 tests | No unit tests. Structural pass-through. |
| union_pruning | 3 tests | Smoke tests for `extract_labels_from_id_where()` and negation handling. |

### Key Integration Tests

The optimizer passes are heavily tested via end-to-end integration tests that verify
the generated SQL. Key test files:

- `tests/integration/` — Query-to-SQL pipeline tests
- `test_where_simple.py` — Validates FilterIntoGraphRel for node-only queries
- Benchmark queries in `benchmarks/social_network/queries/` — exercise multi-hop, optional, variable-length paths

### Adding a New Optimization Pass

1. Create `new_pass.rs` implementing `OptimizerPass`
2. Add `mod new_pass;` (or `pub mod`) to `mod.rs`
3. Determine placement: initial_optimization, final_optimization, or analyzer-invoked
4. Add to the appropriate function in `mod.rs` or `analyzer/mod.rs`
5. **Order matters**: document why it must run before/after specific passes
6. Add unit tests that construct plan nodes and verify transformations
7. Run full test suite: `cargo test` to verify no regressions
