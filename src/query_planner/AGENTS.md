# query_planner Module — Agent Guide

> **Purpose**: Transforms parsed Cypher AST into an optimized `LogicalPlan` + `PlanCtx` ready for SQL generation.
> This is the brain of ClickGraph — **schema inference, type resolution, JOIN planning, filter optimization, and variable scoping** all happen here.
> Total: ~51,700 lines across 80+ files.

## Module Architecture

```
Cypher AST (from open_cypher_parser)
    │
    ▼
mod.rs                   ← Entry points: evaluate_read_query(), evaluate_read_statement()
    │                       Orchestrates the 3-phase pipeline: initial → intermediate → final
    │
    ├─ logical_plan/     ← LogicalPlan enum + clause-to-plan translation
    │   plan_builder.rs     builds plan from AST clause-by-clause
    │   match_clause/       MATCH pattern → ViewScan/GraphNode/GraphRel
    │   with_clause.rs      WITH → WithClause (scope boundary + CTE)
    │   return_clause.rs    RETURN → Projection
    │   where_clause.rs     WHERE → Filter
    │   order_by_clause.rs  ORDER BY → OrderBy
    │   ...
    │
    ├─ analyzer/         ← Multi-pass analysis pipeline (schema, types, JOINs, filters)
    │   mod.rs              initial_analyzing() → intermediate_analyzing() → final_analyzing()
    │   graph_join/         Graph pattern → SQL JOIN inference (largest sub-module, 6.7K lines)
    │   filter_tagging.rs   WHERE pushdown and property mapping (3.1K)
    │   type_inference.rs   Infer missing labels/types from schema (1.5K)
    │   schema_inference.rs Label-to-table/ViewScan resolution (1.3K)
    │   ...
    │
    ├─ optimizer/        ← Plan optimization passes (filter pushdown, projection pruning)
    │   mod.rs              initial_optimization() → final_optimization()
    │   filter_into_graph_rel.rs   Embed filters into GraphRel nodes (1.1K)
    │   cartesian_join_extraction.rs  Cross-pattern filter → JOIN condition (765)
    │   ...
    │
    ├─ plan_ctx/         ← Planning context: variables, scoping, table metadata
    │   mod.rs (1.7K)       PlanCtx: scope chain, TableCtx map, CTE tracking, VLP endpoints
    │   table_ctx.rs (333)  Per-alias metadata: labels, properties, CTE references
    │   builder.rs (289)    PlanCtx constructors
    │
    ├─ translator/       ← Graph→SQL property resolution boundary
    │   property_resolver.rs  Unified property mapping (standard/denormalized/polymorphic)
    │
    ├─ logical_expr/     ← Expression IR between AST and SQL
    │   mod.rs (944)        LogicalExpr enum: Literal, Column, Operator, PropertyAccess, etc.
    │   ast_conversion.rs   AST Expression → LogicalExpr conversion
    │   expression_rewriter.rs  Property mapping and transformation
    │   visitors.rs         Visitor pattern for expression traversal
    │   combinators.rs      AND/OR predicate composition
    │
    ├─ ast_transform/    ← Pre-planning AST transformations
    │   mod.rs (943)        id() function rewriting, UNION splitting by labels
    │   id_function.rs      IdFunctionTransformer for Bolt protocol encoded IDs
    │   string_arena.rs     Arena allocator for transformed string lifetimes
    │
    ├─ join_context.rs   ← VLP naming conventions + JOIN state tracking
    ├─ typed_variable.rs ← Unified variable type system (Node/Rel/Scalar/Path/Collection)
    ├─ transformed.rs    ← Transformed<T> enum: Yes(T) | No(T) for pass results
    ├─ types.rs          ← QueryType enum (Read, Call, Procedure, etc.)
    └─ errors.rs         ← Top-level QueryPlannerError
```

## Data Flow: The 3-Phase Pipeline

```
                    ┌─────────────────────────────────────────────┐
                    │           evaluate_read_statement()          │
                    └────────────────────┬────────────────────────┘
                                         │
    ┌────────────────────────────────────┼─────────────────────────────────┐
    │                            Phase 0: Plan Building                    │
    │  logical_plan::evaluate_cypher_statement()                          │
    │    → plan_builder::build_logical_plan()                             │
    │    Processes MATCH → OPTIONAL → UNWIND → WHERE → WITH → RETURN     │
    │    Result: Raw LogicalPlan + PlanCtx                                │
    └────────────────────────────────────┬─────────────────────────────────┘
                                         │
    ┌────────────────────────────────────┼─────────────────────────────────┐
    │                    Phase 1: initial_analyzing()                      │
    │  1. SchemaInference        → label → table/ViewScan                 │
    │  2. TypeInference          → infer missing node/edge types          │
    │  3. PatternResolver        → UNION ALL for ambiguous types          │
    │  4. VlpTransitivityCheck   → validate VLP patterns                  │
    │  5. CteSchemaResolver      → register CTE schemas                   │
    │  6. BidirectionalUnion     → undirected → UNION ALL (both dirs)     │
    │  7. GraphJoinInference     → graph patterns → SQL JOINs             │
    │  8. ProjectedColumnsResolver → pre-compute projected columns        │
    │  9. QueryValidation        → validate query structure               │
    │ 10. FilterTagging          → property mapping + filter pushdown     │
    │ 11. CartesianJoinExtraction → cross-pattern filter → ON clause      │
    │ 12. ProjectionTagging      → expand RETURN *, tag columns           │
    │ 13. GroupByBuilding        → aggregation → GROUP BY                 │
    └────────────────────────────────────┬─────────────────────────────────┘
                                         │
    ┌────────────────────────────────────┼─────────────────────────────────┐
    │                    Phase 1.5: initial_optimization()                 │
    │  1. CartesianJoinExtraction → cross-pattern filters to join_condition│
    │  2. FilterIntoGraphRel      → push filters into GraphRel nodes     │
    └────────────────────────────────────┬─────────────────────────────────┘
                                         │
    ┌────────────────────────────────────┼─────────────────────────────────┐
    │                    Phase 2: intermediate_analyzing()                 │
    │  1. GraphTraversalPlanning → query plan for traversals              │
    │  2. SchemaInference (push) → push table names to scans             │
    │  3. DuplicateScansRemoving → dedup repeated alias scans            │
    │  4. VariableResolver       → resolve property → column references  │
    │  5. CteReferencePopulator  → populate cte_references on WithClause │
    │  6. CteColumnResolver      → resolve CTE column references         │
    │  7. UnwindTupleEnricher    → tuple structure for UNWIND             │
    │  8. CollectUnwindElimination → remove no-op collect+UNWIND         │
    │  9. TrivialWithElimination → remove pass-through WITH clauses      │
    │ 10. PropertyRequirementsAnalyzer → track needed properties         │
    └────────────────────────────────────┬─────────────────────────────────┘
                                         │
    ┌────────────────────────────────────┼─────────────────────────────────┐
    │                    Phase 2.5: final_optimization()                   │
    │  1. ProjectionPushDown      → eliminate unused columns              │
    │  2. CleanupViewScanFilters  → remove duplicate filters on ViewScans│
    │  3. FilterPushDown          → push filters closer to scans         │
    │  4. ViewOptimizer           → schema-aware view optimizations      │
    └────────────────────────────────────┬─────────────────────────────────┘
                                         │
    ┌────────────────────────────────────┼─────────────────────────────────┐
    │                    Phase 3: final_analyzing()                        │
    │  1. PlanSanitization        → final plan cleanup                   │
    │  2. UnwindPropertyRewriter  → tuple index rewriting                │
    └────────────────────────────────────┬─────────────────────────────────┘
                                         │
                                         ▼
                            (LogicalPlan, PlanCtx) → render_plan → SQL
```

## Key Types and Public API

### Entry Points (mod.rs)

| Function | Purpose |
|----------|---------|
| `evaluate_read_query()` | Single query → (LogicalPlan, PlanCtx) |
| `evaluate_read_statement()` | Statement with UNION → (LogicalPlan, PlanCtx) |
| `evaluate_call_query()` | CALL procedure → LogicalPlan (PageRank) |
| `get_query_type()` | Classify query: Read, Call, Delete, Update |
| `get_statement_query_type()` | Classify CypherStatement |

### Core Types

| Type | Location | Purpose |
|------|----------|---------|
| `LogicalPlan` | `logical_plan/mod.rs` | Main plan enum: 16 variants |
| `PlanCtx` | `plan_ctx/mod.rs` | Planning context with scope chain |
| `TableCtx` | `plan_ctx/table_ctx.rs` | Per-alias metadata |
| `LogicalExpr` | `logical_expr/mod.rs` | Expression IR: 20+ variants |
| `TypedVariable` | `typed_variable.rs` | Unified variable type system |
| `VariableRegistry` | `typed_variable.rs` | Variable name → type mapping |
| `JoinContext` | `join_context.rs` | JOIN state during traversal |
| `Transformed<T>` | `transformed.rs` | Yes(T)/No(T) for pass results |
| `PropertyResolver` | `translator/property_resolver.rs` | Graph property → SQL column |

### LogicalPlan Variants

```rust
LogicalPlan::Empty              // No-op (filtered UNION branches)
LogicalPlan::ViewScan(Arc<ViewScan>)  // Table scan with optional predicates
LogicalPlan::GraphNode(GraphNode)     // Node in graph pattern
LogicalPlan::GraphRel(GraphRel)       // Relationship pattern (left/center/right)
LogicalPlan::GraphJoins(GraphJoins)   // Computed JOINs from graph patterns
LogicalPlan::Filter(Filter)           // WHERE predicate
LogicalPlan::Projection(Projection)   // RETURN clause
LogicalPlan::GroupBy(GroupBy)          // GROUP BY + HAVING
LogicalPlan::OrderBy(OrderBy)         // ORDER BY
LogicalPlan::Skip(Skip)               // SKIP n
LogicalPlan::Limit(Limit)             // LIMIT n
LogicalPlan::Cte(Cte)                 // Common Table Expression
LogicalPlan::Union(Union)             // UNION / UNION ALL
LogicalPlan::WithClause(WithClause)   // WITH scope boundary + CTE materialization
LogicalPlan::CartesianProduct(CartesianProduct)  // CROSS/LEFT JOIN for disconnected patterns
LogicalPlan::Unwind(Unwind)           // ARRAY JOIN
LogicalPlan::PageRank(PageRank)       // PageRank algorithm
```

### Pass Traits

```rust
// analyzer/analyzer_pass.rs
trait AnalyzerPass {
    fn analyze(&self, plan: Arc<LogicalPlan>, ctx: &mut PlanCtx)
        -> AnalyzerResult<Transformed<Arc<LogicalPlan>>>;
    fn analyze_with_graph_schema(&self, plan, ctx, schema)
        -> AnalyzerResult<Transformed<Arc<LogicalPlan>>>;
}

// optimizer/optimizer_pass.rs
trait OptimizerPass {
    fn optimize(&self, plan: Arc<LogicalPlan>, ctx: &mut PlanCtx)
        -> OptimizerResult<Transformed<Arc<LogicalPlan>>>;
}
```

## Key Files with Line Counts

### Top-Level (query_planner/)
| File | Lines | Purpose |
|------|------:|---------|
| `mod.rs` | 260 | Entry points, pipeline orchestration, PageRank eval |
| `errors.rs` | 27 | QueryPlannerError enum |
| `join_context.rs` | 387 | VLP naming constants, JoinContext struct, VlpEndpointInfo |
| `typed_variable.rs` | 1,062 | TypedVariable enum, VariableRegistry, 5 variable types |
| `transformed.rs` | 20 | Transformed<T> enum |
| `types.rs` | 11 | QueryType enum |

### logical_plan/ (5,870 lines total)
| File | Lines | Purpose |
|------|------:|---------|
| `mod.rs` | 1,940 | LogicalPlan enum, all node structs, evaluate_cypher_statement |
| `match_clause/traversal.rs` | 1,669 | Core MATCH → plan translation |
| `match_clause/tests.rs` | 1,581 | Unit tests for match clause |
| `match_clause/view_scan.rs` | 974 | ViewScan generation |
| `return_clause.rs` | 745 | RETURN → Projection |
| `match_clause/helpers.rs` | 663 | Scan helpers, property conversion |
| `with_clause.rs` | 579 | WITH → WithClause scope boundary |
| `plan_builder.rs` | 479 | build_logical_plan() orchestrator |
| `view_scan.rs` | 306 | ViewScan struct definition |
| `where_clause.rs` | 260 | WHERE → Filter |
| `optional_match_clause.rs` | 245 | OPTIONAL MATCH → CartesianProduct |
| `unwind_clause.rs` | 171 | UNWIND → Unwind node |
| `match_clause/schema_filter.rs` | 138 | Schema-based filtering |

### analyzer/ (17,900 lines total — largest sub-module)
| File | Lines | Purpose |
|------|------:|---------|
| `graph_join/inference.rs` | 4,048 | **Core JOIN inference** — THE largest file |
| `filter_tagging.rs` | 3,121 | Filter pushdown + property mapping |
| `type_inference.rs` | 1,527 | Infer missing labels/types from schema |
| `graph_join/tests.rs` | 1,373 | JOIN inference tests |
| `variable_resolver.rs` | 1,334 | Property access → column resolution |
| `projection_tagging.rs` | 1,326 | RETURN * expansion, column tagging |
| `schema_inference.rs` | 1,308 | Label → table resolution |
| `bidirectional_union.rs` | 1,235 | Undirected patterns → UNION ALL |
| `pattern_resolver.rs` | 1,228 | Multi-type pattern enumeration |
| `property_requirements_analyzer.rs` | 1,094 | Track needed properties (optimization) |
| `graph_traversal_planning.rs` | 849 | Graph traversal planning |
| `match_type_inference.rs` | 759 | MATCH-specific type inference |
| `graph_join/cross_branch.rs` | 755 | Cross-branch shared node detection |
| `multi_type_vlp_expansion.rs` | 686 | VLP multi-type UNION expansion |
| `mod.rs` | 614 | Pipeline orchestration (3 analysis phases) |
| `group_by_building.rs` | 613 | Aggregation → GROUP BY |
| `cte_column_resolver.rs` | 494 | CTE column reference resolution |
| `graph_join/helpers.rs` | 493 | JOIN inference utilities |
| `property_requirements.rs` | 461 | PropertyRequirements data structure |
| `duplicate_scans_removing.rs` | 458 | Dedup repeated alias scans |
| `graph_context.rs` | 389 | Graph execution context |
| `graph_join/metadata.rs` | 379 | Pattern index: PatternNodeInfo/EdgeInfo |
| `projected_columns_resolver.rs` | 376 | Pre-compute projected columns |
| `vlp_transitivity_check.rs` | 374 | VLP transitivity validation |
| `unwind_property_rewriter.rs` | 374 | Tuple index rewriting |
| `unwind_tuple_enricher.rs` | 363 | Tuple metadata for UNWIND |
| `query_validation.rs` | 342 | Query structure validation |
| `test_multi_type_vlp_auto_inference.rs` | 341 | VLP auto-inference tests |
| `where_property_extractor.rs` | 281 | WHERE-based property extraction |
| `cte_schema_resolver.rs` | 222 | CTE schema registration |
| `view_resolver.rs` | 197 | View resolution |
| `plan_sanitization.rs` | 194 | Plan cleanup |
| `cte_reference_populator.rs` | 192 | Populate cte_references on WithClause |
| `view_resolver_tests.rs` | 138 | View resolver tests |
| `errors.rs` | 108 | AnalyzerError enum |

### optimizer/ (3,440 lines total)
| File | Lines | Purpose |
|------|------:|---------|
| `filter_into_graph_rel.rs` | 1,148 | Embed filters into GraphRel nodes |
| `cartesian_join_extraction.rs` | 765 | Cross-pattern filter → JOIN condition |
| `collect_unwind_elimination.rs` | 571 | Remove no-op collect+UNWIND patterns |
| `cleanup_viewscan_filters.rs` | 339 | Remove duplicate ViewScan filters |
| `view_optimizer.rs` | 348 | Schema-aware view optimizations |
| `trivial_with_elimination.rs` | 295 | Remove pass-through WITH clauses |
| `union_pruning.rs` | 224 | Prune empty UNION branches |
| `mod.rs` | 181 | Pipeline orchestration |
| `filter_push_down.rs` | 168 | Push filters toward scans |
| `projection_push_down.rs` | 151 | Eliminate unused columns |
| `optimizer_pass.rs` | 34 | OptimizerPass trait |
| `errors.rs` | 40 | OptimizerError enum |

### Other Sub-modules
| File | Lines | Purpose |
|------|------:|---------|
| `plan_ctx/mod.rs` | 1,673 | PlanCtx struct + scope chain logic |
| `plan_ctx/table_ctx.rs` | 333 | TableCtx: per-alias metadata |
| `plan_ctx/builder.rs` | 289 | PlanCtx constructors |
| `translator/property_resolver.rs` | 765 | Unified property resolution |
| `ast_transform/mod.rs` | 943 | id() function rewriting |
| `ast_transform/id_function.rs` | 577 | IdFunctionTransformer |
| `logical_expr/mod.rs` | 944 | LogicalExpr enum definitions |
| `logical_expr/ast_conversion.rs` | 524 | AST → LogicalExpr |
| `logical_expr/expression_rewriter.rs` | 441 | Property mapping transforms |
| `logical_expr/visitors.rs` | 408 | Expression visitor pattern |
| `tests/integration_tests.rs` | 192 | Integration tests |

## Critical Invariants

### 1. GraphRel Left/Right Convention
`left` is ALWAYS the **source** node (connects to `from_id`), `right` is ALWAYS the **target** (connects to `to_id`). For incoming arrows `(a)<-[:R]-(b)`, nodes are SWAPPED during parsing so left=b, right=a. **Never** use `direction` field for from_id/to_id selection in JOIN logic.

### 2. WITH Clause = Scope Barrier
WITH clauses create variable scope boundaries. `PlanCtx.is_with_scope=true` means variable lookup stops here. Only `exported_aliases` are visible downstream. Analyzers like BidirectionalUnion must NOT cross WithClause boundaries.

### 3. VLP Naming Conventions (Single Source of Truth)
Constants in `join_context.rs`:
- `VLP_CTE_FROM_ALIAS = "t"` — FROM alias for VLP CTEs
- `VLP_START_ID_COLUMN = "start_id"` — start node ID column
- `VLP_END_ID_COLUMN = "end_id"` — end node ID column

All code generating/referencing VLP CTEs MUST use these constants. VLP CTE names follow: `vlp_{start_alias}_{end_alias}`.

### 4. Analysis Phase Ordering
Many passes depend on prior passes. Key dependencies:
- **SchemaInference** must run before TypeInference (needs table mappings)
- **TypeInference** must run before VlpTransitivityCheck (needs relationship types)
- **BidirectionalUnion** must run before GraphJoinInference (needs expanded patterns)
- **FilterTagging** must run before CartesianJoinExtraction (needs property-mapped predicates)
- **VariableResolver** must run before CteReferencePopulator (needs resolved references)
- **CteReferencePopulator** must run before CteColumnResolver
- **PropertyRequirementsAnalyzer** must run LAST (needs all property references stable)
- **UnwindPropertyRewriter** must run at VERY END (after all transformations)

### 5. Schema Access Pattern
Query-processing code MUST access schema via task-local `QueryContext` or the `PlanCtx.schema()` method, never directly from `GLOBAL_SCHEMAS`. See `server/query_context.rs`.

### 6. Transformed<T> Protocol
Every pass returns `Transformed::Yes(plan)` if it modified the plan, `Transformed::No(plan)` if not. Callers use `.get_plan()` to extract the plan regardless. This enables optimization tracking.

### 7. PlanCtx Dual Registration
`PlanCtx::insert_table_ctx()` registers variables in BOTH the legacy `alias_table_ctx_map` AND the new `VariableRegistry`. Both systems must stay in sync during the migration period.

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| Filter lost across WITH | WHERE clause disappears in SQL | TrivialWithElimination removes WithClause containing filter |
| Variable amnesia after WITH | "alias not found" errors | Variable not exported from WithClause.exported_aliases |
| Wrong property mapping | Column not found in ClickHouse | FilterTagging maps property with wrong schema context |
| Duplicate filters | Same WHERE condition appears twice | FilterIntoGraphRel runs twice, or CleanupViewScanFilters missed |
| VLP endpoint wrong column | JOIN uses `u2.user_id` instead of `t.end_id` | JoinContext not marking VLP endpoint correctly |
| GraphRel left/right swapped | FROM/TO IDs reversed | Using `direction` field instead of left/right convention |
| CTE column resolution fail | `a_name` not found | CteColumnResolver didn't run, or CTE schema not registered |
| Cross-pattern filter in wrong place | Filter pushed into single GraphRel | CartesianJoinExtraction didn't run before FilterIntoGraphRel |
| Type inference explosion | Too many UNION branches | `max_inferred_types` not set, PatternResolver generates N² combinations |
| Scope leak | Variable from parent scope visible | `is_with_scope` not set on new PlanCtx scope |

## Dependencies

### Upstream (inputs to this module)
| Module | What It Provides |
|--------|-----------------|
| `open_cypher_parser` | `OpenCypherQueryAst`, `CypherStatement`, `Expression`, pattern types |
| `graph_catalog` | `GraphSchema`, `NodeSchema`, `RelationshipSchema`, `PatternSchemaContext` |
| `server::query_context` | Task-local schema access |

### Downstream (consumers of this module's output)
| Module | What It Consumes |
|--------|-----------------|
| `render_plan` | `LogicalPlan` + `PlanCtx` → `RenderPlan` (SQL-ready IR) |
| `clickhouse_query_generator` | Indirect — depends on render_plan's output |
| `server::handlers` | Calls `evaluate_read_statement()` |

## Testing Guidance

### Unit Tests (in-module)
```bash
# Run all query_planner tests
cargo test --lib query_planner

# Specific sub-module tests
cargo test --lib graph_join           # JOIN inference (largest test suite)
cargo test --lib match_clause         # MATCH pattern tests
cargo test --lib filter_tagging       # Filter pushdown tests
cargo test --lib type_inference       # Type inference tests
cargo test --lib vlp_transitivity     # VLP validation tests
cargo test --lib view_resolver        # View resolution tests
```

### Integration Tests
```bash
# Tests in src/query_planner/tests/integration_tests.rs
cargo test --lib integration_tests

# Full test suite (includes render_plan + SQL generation)
cargo test
```

### Manual Smoke Testing
```bash
# Set schema and start server
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --bin clickgraph

# Test basic query planning (sql_only skips ClickHouse execution)
curl -X POST localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name LIMIT 5", "sql_only": true}'
```

### Key Test Patterns to Verify
When modifying this module, ensure these patterns work:
- [ ] Simple node: `MATCH (u:User) RETURN u.name`
- [ ] Single-hop: `MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name`
- [ ] Multi-hop: `MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c) RETURN a.name, c.name`
- [ ] VLP: `MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN a.name, b.name`
- [ ] WITH scope: `MATCH (u:User) WITH u MATCH (u)-[:FOLLOWS]->(f) RETURN f.name`
- [ ] OPTIONAL MATCH: `MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f) RETURN u.name, f.name`
- [ ] Aggregation: `MATCH (u:User)-[:FOLLOWS]->(f) RETURN u.name, count(f) AS cnt`
- [ ] Untyped nodes: `MATCH (n) RETURN n LIMIT 10`
- [ ] Undirected: `MATCH (a:User)--(b) RETURN a.name, b.name`

## Sub-modules That Deserve Their Own AGENTS.md

### 1. `analyzer/` (~17,900 lines)
The largest and most complex sub-module. Contains 30+ files spanning schema inference, type inference, JOIN generation, filter optimization, CTE resolution, and variable scoping. The `graph_join/` sub-sub-module alone is 6,700 lines with its own internal architecture (phased approach: metadata → schema context → join generation → cross-branch detection).

### 2. `logical_plan/` (~5,870 lines)
Core data structures + clause processing. The `match_clause/` sub-module (3,500+ lines) handles the most complex AST-to-plan translation including variable-length paths, shortest paths, and view-aware scans. The `mod.rs` alone is 1,940 lines with all plan node structs.

### 3. `plan_ctx/` (~2,300 lines)
The PlanCtx is the most-touched data structure in the codebase. Understanding its scope chain, dual registration (TableCtx + VariableRegistry), CTE tracking, and VLP endpoint management is essential for any query planner work.

### 4. `logical_expr/` (~2,600 lines)
The expression IR layer. While simpler than analyzer, the 20+ LogicalExpr variants and expression rewriting logic are important for anyone working on filter handling or property mapping.

## Schema Variation Awareness

The query planner generates different plan structures based on schema type:

| Schema Type | Plan Differences |
|-------------|-----------------|
| **Standard** | Separate ViewScans for nodes + edges, 3-way GraphRel |
| **FK-edge** | No separate edge table, relationship = FK column on node |
| **Denormalized** | Node properties on edge table, GraphNode.is_denormalized=true |
| **Polymorphic** | Single edge table + type_column, type filters in ViewScan |
| **Composite ID** | Multi-column node identity, affects JOIN conditions |

When modifying the planner, always ask: does this work for all 5 schema variations?
