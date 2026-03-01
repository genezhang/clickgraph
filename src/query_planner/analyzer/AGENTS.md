# query_planner/analyzer Module — Agent Guide

> **Purpose**: The analyzer transforms parsed Cypher AST (`LogicalPlan`) into an optimized,
> fully-resolved logical plan ready for SQL generation. It runs a configurable pipeline of
> ~20 analysis and optimization passes — resolving labels to tables, inferring types,
> mapping properties to columns, generating JOINs, building GROUP BYs, and pruning
> unnecessary data — all producing a plan the renderer can translate "dumbly" to SQL.
>
> **This is the largest sub-module (~28,200 lines)** and where most semantic bugs originate.
> Read this before touching any file here.

## Module Architecture

```
LogicalPlan (from open_cypher_parser + plan_builder)
    │
    ▼
mod.rs                       ← Pipeline orchestrator: initial → intermediate → final
    │                           3 entry points, each runs ordered analysis passes
    │
    ├─ analyzer_pass.rs (29)     ← AnalyzerPass trait definition (2 methods)
    ├─ errors.rs (108)           ← AnalyzerError enum, Pass enum for error attribution
    │
    ├─── Phase 1: Initial Analysis ──────────────────────────────────────────
    │ type_inference.rs (4,238)       ← **UNIFIED TYPE RESOLUTION** (5 phases):
    │                                    Phase 0: Relationship-based label inference
    │                                    Phase 1: WHERE constraint extraction + direction validation
    │                                    Phase 2: Untyped node UNION generation
    │                                    Phase 3: ViewScan resolution
    │                                    Phase 4: Cypher-level UNION handling
    │ [DELETED: schema_inference.rs]  ← Merged into type_inference.rs
    │ [DELETED: pattern_resolver.rs]  ← Merged into type_inference.rs Phase 2
    │ vlp_transitivity_check.rs (375) ← Validate VLP patterns are transitive
    │ cte_schema_resolver.rs (223)    ← Register WITH CTE schemas in PlanCtx
    │ bidirectional_union.rs (1,236)  ← Undirected (a)--(b) → UNION ALL of both directions
    │ union_distribution.rs (310)    ← Distribute Union through GraphRel/CP/Filter/WithClause
    │ graph_join/ (inference)         ← Graph pattern → JOINs (runs early for PatternSchemaContext)
    │ projected_columns_resolver.rs (377) ← Pre-compute GraphNode.projected_columns
    │ query_validation.rs (343)       ← Validate relationship patterns against schema
    │ filter_tagging.rs (3,122)       ← Property mapping, filter extraction, id() decoding
    │ projection_tagging.rs (1,327)   ← RETURN * expansion, column tagging
    │ group_by_building.rs (614)      ← Detect aggregates, create GROUP BY
    │
    ├─── Phase 2: Intermediate Analysis ─────────────────────────────────────
    │ graph_traversal_planning.rs (850) ← Multi-hop & [:TYPE1|TYPE2] → CTE/UNION
    │ duplicate_scans_removing.rs (459) ← Deduplicate same-alias table scans
    │ variable_resolver.rs (1,335)      ← Resolve TableAlias → CTE PropertyAccess
    │ cte_reference_populator.rs (193)  ← Populate GraphRel.cte_references
    │ cte_column_resolver.rs (495)      ← Resolve property names → CTE column names
    │ unwind_tuple_enricher.rs (364)    ← Enrich Unwind with tuple index metadata
    │ property_requirements_analyzer.rs (1,095) ← Determine needed properties per alias
    │ property_requirements.rs (462)    ← PropertyRequirements data structure
    │
    ├─── Phase 3: Final Analysis ────────────────────────────────────────────
    │ plan_sanitization.rs (195)        ← Final plan cleanup
    │ unwind_property_rewriter.rs (375) ← Rewrite user.name → user.5 (tuple index)
    │
    ├─── Supporting Modules ─────────────────────────────────────────────────
    │ graph_context.rs (390)            ← GraphContext struct for pattern analysis
    │ view_resolver.rs (198)            ← Schema lookups for nodes/relationships
    │ view_scan_handling.rs (13)        ← ViewScan leaf node helper
    │ match_type_inference.rs (760)     ← Type inference for MATCH clause processing
    │ multi_type_vlp_expansion.rs (687) ← Enumerate valid multi-type VLP paths
    │ where_property_extractor.rs (282) ← Extract property refs from WHERE (AST-level)
    │
    └─── graph_join/ Sub-Module (7,225 lines total) ─────────────────────────
      mod.rs (57)                ← Public API, re-exports
      inference.rs (2,880)       ← Core JOIN inference — dispatches to join_generation.rs
      join_generation.rs (890)   ← **NEW (PR #117)**: Generic anchor-aware JOIN generation algorithm
      metadata.rs (379)          ← PatternGraphMetadata builder
      helpers.rs (554)           ← Join dedup, column resolution, utilities
      cross_branch.rs (776)      ← Cross-branch JOIN detection, uniqueness constraints
      tests.rs (1,689)           ← Comprehensive unit tests (includes join_generation tests)
```

## Complete File List

| File | Lines | Responsibility |
|------|------:|----------------|
| **type_inference.rs** | **4,238** | **UNIFIED type resolution (5 phases): label inference, UNION generation, direction validation, ViewScan resolution** |
| graph_join/inference.rs | 2,880 | Core JOIN inference — delegates to join_generation.rs for pattern→JOIN conversion |
| graph_join/join_generation.rs | 890 | **NEW (PR #117)**: Generic anchor-aware JOIN generation: `generate_pattern_joins()` handles 4 availability cases (neither/left/right/both nodes available), topological sort, OPTIONAL marking |
| filter_tagging.rs | 3,122 | Property→column mapping, filter extraction, id() decoding |
| graph_join/tests.rs | 1,689 | Unit tests for graph join inference + join generation |
| variable_resolver.rs | 1,335 | Resolve variable references to CTE sources |
| projection_tagging.rs | 1,327 | RETURN * expansion, property tagging, **count(*) semantics** |
| bidirectional_union.rs | 1,236 | Undirected patterns → UNION ALL |
| union_distribution.rs | 310 | Distribute Union through GraphRel/CartesianProduct/Filter/WithClause |
| property_requirements_analyzer.rs | 1,095 | Determine required properties for pruning |
| graph_traversal_planning.rs | 850 | Multi-hop CTE planning, [:A\|B] UNION |
| match_type_inference.rs | 760 | MATCH clause type inference helpers |
| graph_join/cross_branch.rs | 776 | Cross-branch JOIN detection, uniqueness constraints |
| multi_type_vlp_expansion.rs | 687 | Multi-type VLP path enumeration |
| mod.rs | 614 | Pipeline orchestrator (initial/intermediate/final) |
| group_by_building.rs | 614 | GROUP BY clause creation from aggregates |
| cte_column_resolver.rs | 495 | Resolve PropertyAccess → CTE column names |
| graph_join/helpers.rs | 554 | Join deduplication, column resolution |
| property_requirements.rs | 462 | PropertyRequirements data structure |
| duplicate_scans_removing.rs | 459 | Deduplicate same-alias ViewScans |
| graph_context.rs | 390 | GraphContext struct for pattern analysis |
| graph_join/metadata.rs | 380 | PatternGraphMetadata types & builder |
| projected_columns_resolver.rs | 377 | Pre-compute projected_columns for GraphNodes |
| vlp_transitivity_check.rs | 375 | Validate VLP transitivity |
| unwind_property_rewriter.rs | 375 | Rewrite property access → tuple indices |
| unwind_tuple_enricher.rs | 364 | Enrich Unwind nodes with tuple metadata |
| query_validation.rs | 343 | Validate relationship patterns against schema |
| test_multi_type_vlp_auto_inference.rs | 341 | Tests for multi-type VLP auto-inference |
| where_property_extractor.rs | 282 | Extract property refs from WHERE (AST-level) |
| cte_schema_resolver.rs | 223 | Register WITH CTE schemas in PlanCtx |
| view_resolver.rs | 198 | Schema lookups for nodes/relationships |
| plan_sanitization.rs | 195 | Final plan cleanup, PropertyAccess→Column |
| cte_reference_populator.rs | 193 | Populate GraphRel.cte_references |
| view_resolver_tests.rs | 138 | Tests for ViewResolver |
| errors.rs | 108 | AnalyzerError enum |
| pattern_resolver_config.rs | 60 | Max combinations config |
| graph_join/mod.rs | 57 | Public API, re-exports |
| analyzer_pass.rs | 29 | AnalyzerPass trait definition |
| view_scan_handling.rs | 13 | ViewScan leaf helper |

## The 3-Phase Analysis Pipeline

The analyzer is invoked in 3 phases from `mod.rs`. Each phase is a separate function
called sequentially from `query_planner/mod.rs`. **Pass ordering within each phase is critical.**

### Phase 1: `initial_analyzing()` — Schema Resolution & Pattern Analysis

Resolves types, validates patterns, maps properties, and generates JOINs.

```
Step 1:  UnifiedTypeInference    — ALL type resolution in 5 phases:
                                    Phase 0: Relationship-based label inference
                                    Phase 1: WHERE label extraction + direction validation
                                    Phase 2: Untyped node UNION generation (with schema filtering)
                                    Phase 3: ViewScan resolution (GraphNode → ViewScan)
                                    Phase 4: Cypher-level UNION handling
         [CONSOLIDATED: Merged SchemaInference + PatternResolver into TypeInference]
Step 2:  VlpTransitivityCheck    — Validate VLP patterns are transitive
Step 3:  CteSchemaResolver       — Register WITH CTE schemas in PlanCtx
Step 4:  BidirectionalUnion      — (a)--(b) → UNION ALL (MUST be before GraphJoinInference!)
Step 4b: UnionDistribution       — Hoist Union through GraphRel/CartesianProduct/Filter/WithClause
Step 5:  GraphJoinInference      — Graph patterns → JOIN trees + PatternSchemaContext
Step 6:  ProjectedColumnsResolver — Pre-compute GraphNode.projected_columns
Step 7:  QueryValidation         — Validate relationship patterns
Step 8:  FilterTagging           — Property→column mapping, filter extraction
Step 9:  CartesianJoinExtraction — Cross-pattern filters → join_condition (optimizer pass)
Step 10: ProjectionTagging       — RETURN * expansion, column tagging, **count(*) semantics**
Step 11: GroupByBuilding         — Detect aggregates → GROUP BY
```

*Note: Step numbers in code comments are historical and don't match sequential order.*

### Phase 2: `intermediate_analyzing()` — Traversal Planning & Resolution

Plans complex traversals, resolves variables, manages CTEs, optimizes.

```
Step 1:  GraphTraversalPlanning      — Multi-hop/[:A|B] → CTE/UNION
Step 2:  DuplicateScansRemoving      — Deduplicate same-alias scans
Step 3:  VariableResolver            — TableAlias("cnt") → PropertyAccess("cte", "cnt")
Step 4:  CteReferencePopulator       — Populate GraphRel.cte_references
Step 5:  CteColumnResolver           — PropertyAccess("p","firstName") → ("p","p_firstName")
Step 6:  UnwindTupleEnricher         — Enrich Unwind with tuple structure metadata
Step 7:  CollectUnwindElimination    — Remove no-op collect+UNWIND patterns (optimizer)
Step 8:  TrivialWithElimination      — Remove pass-through WITH clauses (optimizer)
Step 9:  PropertyRequirementsAnalyzer — Determine needed properties per alias
```

### Phase 3: `final_analyzing()` — Cleanup & Last-Mile Rewriting

```
Step 1: PlanSanitization           — Final plan cleanup, PropertyAccess→Column
Step 2: UnwindPropertyRewriter     — user.name → user.5 (tuple index access)
```

## The `graph_join/` Sub-Module Architecture

This is a complex sub-module (7,225 lines). It converts Cypher graph patterns into SQL JOINs.

### Core Data Flow

```
GraphRel tree                                     GraphJoins { joins: Vec<Join> }
    │                                                 ▲
    ▼                                                 │
build_pattern_metadata()                         build_graph_joins()
    │  (Phase 0: pre-compute index)                   │
    ▼                                                 │
PatternGraphMetadata                             collected_graph_joins
    │  (nodes, edges, references, appearances)        ▲
    ▼                                                 │
collect_graph_joins()  ──── infer_graph_join() ──────┘
    │                       │
    │                       └─ Dispatches to join_generation.rs
    │                          generate_pattern_joins(strategy, tables, available)
    │
    ├── cross_branch::generate_cross_branch_joins_from_metadata()
    │   (Phase 2: shared nodes in comma-separated patterns → JOINs)
    │
    └── cross_branch::generate_relationship_uniqueness_constraints()
        (Phase 4: r1.id != r2.id for bidirectional patterns)
```

### join_generation.rs — Anchor-Aware Algorithm (PR #117)

**Core insight**: Traditional node-edge-node is the base case requiring 2 JOINs. All
other `JoinStrategy` variants are optimizations that skip some JOINs.

```rust
// Generic loop in inference.rs
for each (a)-[r]->(b):
    already_available = join_ctx.to_hashset()
    joins += generate_pattern_joins(strategy, tables, already_available)
    apply_vlp_rewrites(&mut joins)
    apply_optional_marking(&mut joins)
    collect_with_dedup(&mut all_joins, joins)
anchor = select_anchor(&all_joins)
ordered = topo_sort_joins(all_joins)
```

**4 anchor-availability cases** for Traditional strategy:
| Case | Condition | Generated JOINs |
|------|-----------|-----------------|
| First pattern | Neither node available | `FROM left → JOIN edge ON left → JOIN right ON edge` |
| Left available | Left already joined | `JOIN edge ON left → JOIN right ON edge` |
| Right available | Right already joined | `JOIN edge ON right → JOIN left ON edge` (reversed) |
| Both available | Both already joined | `JOIN edge ON left AND right` (correlation) |

This is critical for OPTIONAL MATCH — the optional side shares a node with the required
MATCH. Without anchor awareness, cartesian products (`ON 1 = 1`) were generated.

**Replaced**: ~1200 lines of per-strategy handler code in inference.rs with a 64-line
generic loop + this clean 890-line module. **Net -374 lines.**

### Key Types

- **`GraphJoinInference`** — Main pass implementing `AnalyzerPass`. Entry point.
- **`JoinContext`** — Tracks which aliases are already joined (prevents duplicates).
  Also tracks VLP endpoints (`VlpEndpointInfo`, `VlpPosition`).
- **`PatternGraphMetadata`** — Pre-computed index over pattern structure:
  - `nodes: HashMap<alias, PatternNodeInfo>` — alias, label, is_referenced, appearance_count
  - `edges: Vec<PatternEdgeInfo>` — alias, rel_types, from/to, is_vlp, direction, is_optional
- **`NodeAppearance`** — Tracks where a node variable appears (for cross-branch detection)

### Phased Execution in `inference.rs`

1. **Phase 0: Metadata Construction** — `build_pattern_metadata()` builds `PatternGraphMetadata`
2. **Phase 1: CTE Reference Registration** — `register_with_cte_references()` discovers WITH exports
3. **Phase 2: Join Collection** — `collect_graph_joins()` recursively traverses plan, calls `infer_graph_join()` for each `GraphRel`
4. **Phase 3: Cross-Branch JOINs** — `generate_cross_branch_joins_from_metadata()` handles shared nodes
5. **Phase 4: Uniqueness Constraints** — `generate_relationship_uniqueness_constraints()` prevents duplicate edge traversal

## The `AnalyzerPass` Trait

```rust
pub trait AnalyzerPass {
    // Simple pass (no schema needed)
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>>;

    // Schema-aware pass
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>>;
}
```

Both methods have default no-op implementations. Each pass overrides whichever variant
it needs. Most passes override `analyze_with_graph_schema` since they need schema access.

**Return type**: `AnalyzerResult<Transformed<Arc<LogicalPlan>>>`
- `Transformed::Yes(plan)` — pass modified the plan
- `Transformed::No(plan)` — pass returned unmodified plan

## Critical Invariants

### 1. Pass Ordering Is Sacred

Many passes have hard dependencies on previous passes:

| Pass | Requires | Reason |
|------|----------|--------|
| UnifiedTypeInference | SchemaInference | Needs table contexts to exist |
| [REMOVED: PatternResolver] | — | Merged into UnifiedTypeInference |
| BidirectionalUnion | — | MUST run BEFORE UnionDistribution and GraphJoinInference |
| UnionDistribution | BidirectionalUnion | MUST run AFTER BidirectionalUnion, BEFORE GraphJoinInference |
| GraphJoinInference | BidirectionalUnion, UnionDistribution, UnifiedTypeInference | GraphRel must be direction-resolved, Union distributed, types inferred |
| FilterTagging | GraphJoinInference | Needs PatternSchemaContext for property mapping |
| CartesianJoinExtraction | FilterTagging | Needs property-mapped predicates |
| VariableResolver | FilterTagging | Needs property-mapped expressions |
| CteReferencePopulator | VariableResolver | Needs resolved variable sources |
| CteColumnResolver | CteReferencePopulator | Needs CTE reference info |
| PropertyRequirementsAnalyzer | ALL above | Must run last (all refs stable) |
| UnwindPropertyRewriter | ALL above | Absolute last pass (tuple index rewrite) |

**If you reorder passes, you WILL break things silently.**

### 2. Parser Direction Normalization

The Cypher parser ALREADY normalizes relationship direction:
- `left_connection` ALWAYS = FROM node (source)
- `right_connection` ALWAYS = TO node (target)
- `direction` field only records original syntax

```cypher
// (a)<-[:REL]-(b) → parser creates: left="b", right="a", direction=Incoming
// DO NOT check direction field for from/to logic — use left/right directly
```

**TypeInference and GraphJoinInference both rely on this invariant.**

### 3. PlanCtx Is the Shared State Bus

`PlanCtx` is the mutable context threaded through ALL passes. It stores:
- `alias_table_ctx_map` — alias → TableCtx (table name, label, properties)
- CTE columns, CTE entity types, CTE references
- Optional aliases, projection aliases
- VLP endpoint info
- Property requirements
- PatternSchemaContext

**Each pass reads from previous passes' PlanCtx writes and writes its own data.**

### 4. GraphJoins Structure and Deprecated Fields ⚠️ CRITICAL

**Key Fact**: `GraphJoins.joins` list is often **EMPTY and DEPRECATED**!

```rust
pub struct GraphJoins {
    pub input: Arc<LogicalPlan>,
    
    /// DEPRECATED: Pre-computed joins, incorrect for multi-hop patterns.
    /// Often EMPTY! Joins are extracted during rendering via input.extract_joins()
    pub joins: Vec<Join>,
    
    pub optional_aliases: HashSet<String>,  // ✅ Used for LEFT JOIN decisions
    
    /// Computed anchor table (FROM clause) - set by reorder_joins_by_dependencies
    /// BUT only if joins list is non-empty!
    pub anchor_table: Option<String>,
    
    pub cte_references: HashMap<String, String>,
    pub correlation_predicates: Vec<LogicalExpr>,
}
```

**When GraphJoins.joins is EMPTY** (common for WITH+MATCH patterns):
- `anchor_table` is None
- Joins extracted during rendering, not analysis
- FROM clause selection happens in `render_plan/from_builder.rs::extract_from()`
- **NOT** in `reorder_joins_by_dependencies()` (which never runs)

**When GraphJoins.joins is NON-EMPTY**:
- Legacy case, still used for some simple patterns
- `reorder_joins_by_dependencies()` runs and sets `anchor_table`
- FROM clause uses `anchor_table` value

**OPTIONAL MATCH Anchor Selection**: GraphRel structure has critical fields:

```rust
pub struct GraphRel {
    // ... other fields ...
    pub is_optional: Option<bool>,  // ✅ Marks this pattern as optional
    pub anchor_connection: Option<String>,  // ✅ CRITICAL: Required node from prior MATCH
    // ... other fields ...
}
```

**Analysis Phase Responsibility** (`graph_join/inference.rs`):
- When processing OPTIONAL MATCH, set `anchor_connection` to the required node
- Example: For `MATCH (tag) OPTIONAL MATCH (m)-[:R]->(tag)`, set `anchor_connection: Some("tag")`

**Rendering Phase Responsibility** (`render_plan/from_builder.rs`):
- `extract_from()` MUST check `anchor_connection` field FIRST
- If set, use that node as FROM (not the left node!)
- See `render_plan/AGENTS.md` section 5 for details

**Investigation Time Waster** (Feb 2026): Don't add logging to `reorder_joins_by_dependencies()`
if `GraphJoins.joins` is empty - it will never run! Check for empty joins list first,
then investigate `extract_from()` logic instead.

### 5. Schema Access via Task-Local QueryContext

Query-processing code MUST access schema via `get_current_schema()` / `get_current_schema_with_fallback()`, never directly from `GLOBAL_SCHEMAS`. See copilot-instructions.md for details.

### 5. Unified Type Inference and UNION Generation

**UnifiedTypeInference** (type_inference.rs) is responsible for:
1. Inferring missing node labels from schema relationships
2. Extracting label constraints from WHERE id() filters
3. Generating UNION branches for multiple valid type combinations
4. **Validating combinations against schema + direction** ← CRITICAL!

**Replaces/merges**:
- Old TypeInference (incremental, incomplete)
- PatternResolver (systematic UNION generation)
- Parts of union_pruning (WHERE constraint extraction)

**Output Structure** for patterns with multiple valid interpretations:

```rust
LogicalPlan::Union {
    inputs: Vec<Arc<LogicalPlan>>,  // Each branch with concrete labels
    union_type: UnionType::All      // UNION ALL
}
```

Each branch is a COMPLETE plan with:
- All nodes typed: `GraphNode { labels: Some(vec!["User"]) }`
- All edges typed: `GraphRel { labels: Some(vec!["AUTHORED"]) }`
- Direction resolved: `Direction::Outgoing` (never `Either`)

**Direction Validation** (CRITICAL):

For directed patterns (`->`), only schema-valid directions allowed:

```cypher
// Schema: AUTHORED from User to Post
✅ Valid:   (User)-[AUTHORED]->(Post)
❌ Invalid: (Post)-[AUTHORED]->(User)  ← MUST BE FILTERED OUT
```

Validation uses: `get_rel_schema_with_direction_check()` ensuring schema direction matches pattern direction.

**Algorithm**:
1. Collect ALL constraints (explicit labels + WHERE id() + schema relationships + direction)
2. Compute possible types for each variable
3. Generate cartesian product of type combinations
4. **Filter by schema validity + direction** ← Prevents invalid branches
5. Create Union if multiple valid combinations, single branch if one

**Key Functions**:
- `extract_labels_from_id_where()` - Extract labels from `WHERE id(x) IN [...]`
- `is_valid_combination_with_direction()` - Validate combo against schema + direction
- `get_rel_schema_with_direction_check()` - Lookup with direction validation

### 6. Aggregation Placement in UNION Generation ⚠️ CRITICAL

**Problem**: When `MATCH (n) RETURN count(n)` has untyped nodes, TypeInference generates UNION branches. Where should the aggregation be placed?

**WRONG** (aggregation INSIDE each branch):
```rust
Union([
    Projection(count(*), GraphJoins(GraphNode(Post))),  // → 50 rows
    Projection(count(*), GraphJoins(GraphNode(User)))   // → 30 rows
])
// Result: 2 rows returned, outer query does count(*) = 1 ❌
```

**CORRECT** (aggregation ABOVE the union):
```rust
Projection(count(*), Union([
    GraphJoins(GraphNode(Post)),  // 50 rows
    GraphJoins(GraphNode(User))   // 30 rows  
]))
// Result: Single row with count = 80 ✅
```

**Implementation** (type_inference.rs):
1. `plan_has_aggregation(plan)` - Detect if plan has GroupBy or aggregate functions
2. `extract_scan_part(plan)` - Extract the scan portion (below aggregation)
3. Clone only scan parts into UNION branches
4. `rewrap_aggregation(original, union)` - Re-wrap aggregation layers above UNION

**When aggregation is detected**:
```rust
if has_aggregation {
    for combo in valid_combinations {
        let scan_branch = clone_plan_with_labels(&extract_scan_part(&plan), &combo);
        union_branches.push(scan_branch);
    }
    let union_arc = Arc::new(LogicalPlan::Union(union_plan));
    rewrap_aggregation(&plan, union_arc)  // Put aggregation back on top
}
```

**Key Insight**: `clone_plan_with_labels` was cloning the ENTIRE plan tree including Projection/GroupBy wrappers. For aggregations, we must split at the aggregation boundary.

### 7. CTE Naming Convention

WITH CTE columns are named `{alias}_{property}`:
- Node alias `a` with property `name` → CTE column `a_name`
- Scalar alias `cnt` → CTE column `cnt` (no prefix)

CTE names are generated by `utils::cte_naming::generate_cte_name()` using exported aliases.

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| Filter lost after TrivialWithElimination | WHERE clause silently dropped | WithClause elimination didn't preserve Filter child |
| Property not found in CTE | `column X cannot be resolved` | CteSchemaResolver didn't register column, or CteColumnResolver didn't resolve it |
| Wrong property mapping | Gets column from wrong table | Denormalized vs standard property source confusion in FilterTagging |
| Cross-branch JOIN missing | Cartesian product instead of JOIN | PatternGraphMetadata didn't detect shared node (appearance_count wrong) |
| Duplicate results with undirected | 2x rows for `(a)--(b)` | BidirectionalUnion not deduplicating, or VLP multi-type mistakenly split |
| VLP non-transitive still recursing | Infinite/wrong recursion | VlpTransitivityCheck didn't convert to fixed-length |
| Variable not resolved | `TableAlias("cnt")` reaches renderer | VariableResolver didn't find CTE source for variable |
| GROUP BY includes aggregate | Invalid SQL | GroupByBuilding `contains_aggregate()` missed nested case |
| Direction-dependent bug | Works outgoing, fails incoming | Code checking `direction` field instead of left/right connections |
| Stale PatternSchemaContext | Wrong join strategy | GraphJoinInference ran but PlanCtx not updated for branch |
| **Invalid UNION branch** | **Wrong data in results** | **UnifiedTypeInference didn't validate direction** |
| **Post→User relationship** | **Schema defines User→Post** | **Direction not checked during type combination validation** |
| **count(n) returns 1 instead of total** | **Aggregation inside UNION branches** | **TypeInference didn't inject UNION below aggregation layer** |

## Dangerous Files — Handle With Extreme Care

### 🔴 graph_join/inference.rs (4,049 lines)
The single most dangerous file. Contains `collect_graph_joins()`, `infer_graph_join()`,
and `build_graph_joins()` — the core join generation logic. Changes here can silently
break any combination of standard/FK-edge/denormalized/VLP/cross-branch patterns.
**Always run full test suite after ANY change.**

### 🔴 filter_tagging.rs (3,122 lines)
Property mapping, filter extraction, and id() decoding. Handles ALL filter placement
decisions. CartesianProduct filter preservation was a regression hotspot. Changes here
affect every query with a WHERE clause.

### 🟡 variable_resolver.rs (1,335 lines)
Resolves all variable references to CTE sources. Scope semantics are complex.
Bugs here cause variables to silently reference wrong CTE or fail to resolve.

### 🔴 type_inference.rs (4,238 lines)
**UNIFIED SYSTEM**: Infers types, extracts WHERE constraints, generates UNION, validates direction, resolves ViewScans — 5 phases in one pass.
Must respect parser direction normalization (invariant #2) AND schema direction constraints.
**CRITICAL**: Aggregation placement logic (Phase 2) — must inject UNION *below* aggregation layer, not inside branches.
Incorrect inference, direction validation, or aggregation placement causes downstream cascading failures.
**Changes here affect EVERY query with multiple possible type interpretations or untyped nodes.**

### 🟡 bidirectional_union.rs (1,236 lines)
Generates 2^n UNION branches for n undirected edges. Must correctly handle VLP
multi-type patterns (skip splitting — CTE handles both directions internally).

**Redundant union collapse** (Feb 2026): `is_redundant_undirected_union()` detects when both endpoints of an undirected edge are already bound by patterns in the left subtree. Collapses to single Outgoing branch to avoid ClickHouse "multiple recursive CTEs" error. Uses `has_alias_in_plan()` helper to check endpoint binding. Called from both the WithClause handler and CartesianProduct handler via `collapse_leaf_unions_in_cp()`.

### 🟡 projection_tagging.rs (1,327 lines)
RETURN * expansion, property tagging, **count() semantics**:
- `count(n)` without DISTINCT → `count(*)` (counting rows = counting nodes/relationships)
- `count(DISTINCT n)` → `count(DISTINCT n.id_column)` (requires label resolution)
- `count(DISTINCT n.prop)` → `count(DISTINCT n.prop)` (direct property)
**Key decision**: Non-DISTINCT count treats nodes like relationships (both are rows in result set).
Changes here affect all aggregation queries.

### union_distribution.rs — Union Distribution Pass (Feb 2026)

Distributes Union nodes through GraphRel/CartesianProduct/Filter/WithClause using algebraic identities. Registered in the analyzer pipeline between BidirectionalUnion and GraphJoinInference.

**Key rule**: When a Union is buried inside a GraphRel or CartesianProduct, the SQL generator can't render it correctly (each Union branch needs its own FROM+JOINs). This pass pulls the Union outward so each branch gets independent rendering.

**WithClause handler** (Mar 2026): Added recursion into `WithClause.input` so Union nodes inside WITH are also distributed. This handles cases like multi-pattern MATCH with undirected edges followed by WITH, where BidirectionalUnion creates a Union inside the WithClause input tree.

### 🟢 Most other files are relatively safe
Single-purpose passes with clear inputs/outputs and limited blast radius.

## Schema Variation Awareness

Every pass may behave differently based on schema type:

| Concept | Standard | FK-edge | Denormalized | Polymorphic |
|---------|:--------:|:-------:|:------------:|:-----------:|
| Node has own table | ✅ | ✅ | ❌ (in edge) | ✅ |
| Edge has own table | ✅ | ❌ (FK on node) | ✅ | ✅ |
| JOIN count per hop | 2 | 1 | 0-1 | 2 |
| Property source | node table | node table | edge table | node table |
| type_column discrimination | — | — | — | ✅ |

**When fixing a bug, always check: does this fix work for ALL schema variations?**

Key files affected by schema variation:
- `graph_join/inference.rs` — different JOIN strategies per schema type
- `filter_tagging.rs` — property mapping varies by source table
- `projected_columns_resolver.rs` — EmbeddedInEdge vs OwnTable vs Virtual
- `view_resolver.rs` — denormalized from/to role-sensitive property resolution

## Public API

### Entry Points (in `mod.rs`)

```rust
pub fn initial_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>>;

pub fn intermediate_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    current_graph_schema: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>>;

pub fn final_analyzing(
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    _: &GraphSchema,
) -> AnalyzerResult<Arc<LogicalPlan>>;
```

### Re-exported from `graph_join/`

```rust
pub use graph_join::GraphJoinInference;
pub use graph_join::{
    PatternGraphMetadata, PatternNodeInfo, PatternEdgeInfo,
    PatternMetadataBuilder, expr_references_alias, is_node_referenced,
    plan_references_alias, JoinContext, VlpEndpointInfo, VlpPosition,
};
```

### Other Public Modules

```rust
pub mod view_resolver;              // ViewResolver for schema lookups
pub mod multi_type_vlp_expansion;   // Path enumeration for multi-type VLPs
pub mod property_requirements;      // PropertyRequirements data structure
pub mod property_requirements_analyzer;
pub mod where_property_extractor;   // WHERE property extraction (AST-level)
pub mod errors;                     // AnalyzerError enum
pub mod graph_join;                 // Full graph_join sub-module
pub mod match_type_inference;       // Type inference helpers
```

## Testing Guidance

### Running Tests

```bash
# All unit tests (fast, no ClickHouse needed)
cargo test --lib

# Tests specifically in analyzer module
cargo test query_planner::analyzer

# Graph join tests
cargo test graph_join

# Multi-type VLP tests
cargo test test_multi_type_vlp

# Full suite (unit + integration, needs ClickHouse)
cargo test
```

### What to Test After Changes

| Changed File | Must Run |
|-------------|----------|
| graph_join/inference.rs | `cargo test graph_join` + full integration suite |
| filter_tagging.rs | `cargo test filter` + any WHERE-related integration tests |
| type_inference.rs | `cargo test type_inference` + patterns with omitted labels |
| variable_resolver.rs | `cargo test variable_resolver` + WITH clause queries |
| bidirectional_union.rs | `cargo test bidirectional` + undirected pattern queries |
| schema_inference.rs | `cargo test schema_inference` + basic MATCH queries |
| Any pass | Full `cargo test` — pass ordering means one change can cascade |

### Manual Smoke Tests

After any analyzer change, test these patterns with the benchmark schema:

```cypher
-- Basic pattern
MATCH (u:User) RETURN u.name

-- Single hop
MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN u1.name, u2.name

-- Undirected
MATCH (u1:User)-[:FOLLOWS]-(u2:User) RETURN u1.name, u2.name

-- WITH clause
MATCH (u:User)-[:FOLLOWS]->(f:User) WITH u, count(f) as cnt RETURN u.name, cnt

-- VLP
MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN u1.name, u2.name

-- OPTIONAL MATCH
MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) RETURN u.name, f.name
```

## Key Relationships to Other Modules

```
open_cypher_parser  ──→  query_planner/logical_plan  ──→  ANALYZER  ──→  render_plan
                              ▲                              │
                              │                              ▼
                         plan_builder                    optimizer/
                              │                        (CartesianJoinExtraction,
                              ▼                         TrivialWithElimination,
                          PlanCtx ◄─────────────────  CollectUnwindElimination)
                              │
                              ▼
                        graph_catalog/ (schema access)
```

The analyzer is the bridge between the parsed AST and the SQL-generating renderer.
It owns the transformation from "Cypher semantics" to "SQL-ready IR".

## Optimizer Passes Called Within Analyzer

Three optimizer passes are called within the analyzer pipeline (not in a separate optimizer phase):

1. **CartesianJoinExtraction** (in initial_analyzing) — Extracts cross-pattern filters into `join_condition`
2. **CollectUnwindElimination** (in intermediate_analyzing) — Removes no-op `collect`+`UNWIND` patterns
3. **TrivialWithElimination** (in intermediate_analyzing) — Removes pass-through WITH clauses

These live in `query_planner/optimizer/` but are invoked via `OptimizerPass::optimize()`.
