# clickhouse_query_generator Module — Agent Guide

> **Purpose**: Converts `RenderPlan` → ClickHouse SQL string.
> Final stage of the Cypher→SQL pipeline. Contains VLP CTE generation, function translation,
> and the most schema-sensitive SQL generation code.

## ⚠️ Fundamental SQL CTE Rules (MUST NOT VIOLATE)

These are basic SQL structural rules. Every generated query must obey them:

1. **CTEs are ALWAYS flat and top-level.** All CTEs form a single linear chain at the
   beginning of the query, separated by commas. CTEs are never nested inside other CTEs
   or inside subqueries. A CTE body can *reference* a previously defined CTE (like a table),
   but cannot *define* one.

2. **One `WITH RECURSIVE` at the beginning.** If any CTE is recursive, the query starts
   with `WITH RECURSIVE` followed by ALL CTEs (both recursive and non-recursive),
   comma-separated. There is never a second `WITH RECURSIVE` anywhere in the query.

3. **Dependency order.** CTEs are listed in dependency order: if CTE B references CTE A,
   then A appears before B. In practice: recursive VLP CTEs first, then non-recursive
   WITH/aggregation CTEs that reference them.

**Correct structure:**
```sql
WITH RECURSIVE
  vlp_a_b AS (...),        -- recursive CTE 1
  vlp_b_a AS (...),        -- recursive CTE 2
  with_x_cte AS (          -- non-recursive, references vlp CTEs
    SELECT ... FROM vlp_a_b AS t
    UNION ALL
    SELECT ... FROM vlp_b_a AS t
  )
SELECT ... FROM with_x_cte
```

**NEVER do this:**
```sql
-- ❌ Nested WITH RECURSIVE inside a CTE body
with_x_cte AS (
  SELECT ... FROM (
    WITH RECURSIVE vlp_b_a AS (...) -- WRONG: nested CTE definition
    SELECT ... FROM vlp_b_a
  ) AS __union
)
```

**Implementation**: `flatten_all_ctes()` in `to_sql_query.rs` enforces this by recursively
extracting all CTEs from the entire RenderPlan tree (including union branches and nested
CTE content) to the top level before rendering. `CteItems::to_sql()` then renders them
as a flat comma-separated list.

## Module Architecture

```
RenderPlan (from render_plan)
    │
    ▼
mod.rs (50)                ← Entry point: generate_sql(plan, max_cte_depth) → String
    │
    ▼
to_sql_query.rs (3,240)    ← MAIN FILE: render_plan_to_sql()
    │                         ToSql impls for RenderPlan, SelectItems, FromTableItem,
    │                         JoinItems, CteItems, FilterItems, GroupByExpressions,
    │                         OrderByItems, UnionItems, RenderExpr, OperatorApplication
    │                         Also: VLP alias rewriting, denormalized ORDER BY resolution,
    │                         multi-type VLP JSON extraction, fixed path rewriting
    │
    ├─ to_sql.rs (374)               ← ToSql trait + impls for LogicalExpr, LogicalPlan
    │                                   Used by view_query.rs and EXISTS/IN subqueries
    │
    ├─ variable_length_cte.rs (3,359) ← VariableLengthCteGenerator: WITH RECURSIVE CTEs
    │                                    for *1..N path patterns. 5 schema variations.
    │                                    Most complex file after to_sql_query.rs.
    │
    ├─ multi_type_vlp_joins.rs (1,337) ← MultiTypeVlpJoinGenerator: UNION ALL of explicit
    │                                     JOINs for multi-type traversals (User→Post via LIKES)
    │
    ├─ function_registry.rs (1,177)   ← Neo4j→ClickHouse function mapping table (73+ functions)
    │                                    lazy_static HashMap, FunctionMapping with arg_transform
    │
    ├─ function_translator.rs (952)   ← translate_scalar_function(), ch./chagg. passthrough,
    │                                    duration(), CH_AGGREGATE_FUNCTIONS registry (200+ aggs)
    │
    ├─ json_builder.rs (331)          ← formatRowNoNewline JSON generation for type-preserving
    │                                    node properties, denormalized properties, UNION ALL
    │
    ├─ pagerank.rs (387)              ← PageRankGenerator: WITH RECURSIVE PageRank iterations
    │
    ├─ view_query.rs (68)             ← ToSql impl for ViewScan (LogicalPlan)
    ├─ view_scan.rs (55)              ← build_view_scan() for standalone ViewScan SQL
    ├─ common.rs (14)                 ← Note about Literal type duplication (no actual code)
    ├─ errors.rs (78)                 ← ClickhouseQueryGeneratorError enum
    │
    ├─ edge_uniqueness_tests.rs (207) ← Tests for edge uniqueness in VLP (path_edges array)
    └─ where_clause_tests.rs (224)    ← Tests for WHERE clause in shortest path queries
```

## Data Flow

```
              ┌─────────────────────────────────────────────────────────┐
              │                  generate_sql()                         │
              │                     mod.rs                              │
              └─────────────┬───────────────────────────────────────────┘
                            │
                            ▼
              ┌─────────────────────────────────────────────────────────┐
              │            render_plan_to_sql()                         │
              │              to_sql_query.rs                            │
              │                                                         │
              │  1. Extract fixed path info                             │
              │  2. Rewrite VLP SELECT aliases (Cypher→CTE columns)     │
              │  3. Rewrite fixed path functions (length(p)→literal)    │
              │  4. Build render contexts (relationship cols, CTE maps) │
              │  5. Set task-local render contexts                      │
              │  6. Generate SQL clause by clause:                      │
              │     CTEs → SELECT → FROM → JOINs → WHERE →             │
              │     GROUP BY → HAVING → ORDER BY → UNION → LIMIT       │
              │  7. Add SETTINGS for recursive CTEs                     │
              │  8. Clear task-local render contexts                    │
              └─────────────────────────────────────────────────────────┘
                            │
              Uses these for CTE content:
              ┌─────────────┼──────────────────┐
              │             │                  │
              ▼             ▼                  ▼
    variable_length    multi_type_vlp      pagerank.rs
       _cte.rs          _joins.rs
    (WITH RECURSIVE)  (UNION ALL JOINs)  (PageRank SQL)
```

## Key Files in Detail

### to_sql_query.rs (3,240 lines) — The Main Renderer

**What it does**: Implements `ToSql` trait for every `RenderPlan` struct component. This is where
the final SQL string is constructed clause by clause.

**Key functions**:
- `render_plan_to_sql()` — Top-level orchestrator. Handles UNION wrapping, VLP rewriting, context setup/teardown.
- `rewrite_vlp_select_aliases()` — Rewrites Cypher aliases (`a.name`) to CTE column names (`t.start_name`).
  Has special handling for OPTIONAL VLP (skip start alias), WITH CTE (skip covered aliases).
- `rewrite_expr_for_vlp()` — Recursive expression rewriter for VLP alias mapping.
- `derive_cypher_property_name()` — **⚠️ TECHNICAL DEBT**: Hardcoded DB→Cypher property name mappings.
- `RenderExpr::to_sql()` — Massive match arm (~700 lines) handling every expression type.
- `RenderExpr::to_sql_without_table_alias()` — For LEFT JOIN subquery filters.

**Critical sections**:
- **CTE flattening** (`flatten_all_ctes()`, `collect_nested_ctes()`): Called at the top of
  `render_plan_to_sql()` to enforce the flat CTE rule (see below).
- **UNION handling** (lines ~1300-1560): Wraps UNION in subquery when ORDER BY/LIMIT/GROUP BY present.
  ClickHouse quirk: bare `UNION ALL` + `LIMIT` only limits last branch.
- **Column heuristic inference** (lines ~2400-2480): **⚠️ TECHNICAL DEBT** — Guesses table alias from
  column name patterns (user_*, post_*, etc.). Covers ~95% of cases but fragile.

### to_sql.rs (374 lines) — LogicalExpr SQL Generation

**What it does**: `ToSql` trait definition and implementations for `LogicalExpr` and `LogicalPlan`.
Used by view_query.rs and for EXISTS/IN subquery generation.

**Key differences from to_sql_query.rs**:
- Operates on `LogicalExpr` (query planner types), not `RenderExpr` (render plan types)
- Simpler — no task-local context, no VLP rewriting
- Operator rendering is duplicated between both files (documented tech debt)

**Notable implementations**:
- `LogicalExpr::List` → `tuple()` (not array) for comparison compatibility
- `LogicalExpr::ReduceExpr` → `arrayFold()` with toInt64() cast for type safety
- `LogicalExpr::MapLiteral` → `map()` with toString() cast (ClickHouse requires homogeneous map values)
- `LogicalExpr::In` with PropertyAccess → `has(array, value)` (array membership)

### variable_length_cte.rs (3,359 lines) — Recursive CTE Generator

**What it does**: Generates `WITH RECURSIVE` CTEs for Cypher `*1..N` path patterns.
The most schema-sensitive code in the codebase.

**The Generator Struct** (`VariableLengthCteGenerator`):
```
Fields: schema, spec (min/max hops), start/end node tables & ID columns,
        relationship table & from/to columns, Cypher aliases, properties,
        shortest_path_mode, start/end/relationship filters, path_variable,
        relationship_types, edge_id, is_denormalized, start_is_denormalized,
        end_is_denormalized, is_fk_edge, type_column, from_label_column,
        to_label_column, from/to_node_label, intermediate_node_* (heterogeneous)
```

**Constructor hierarchy**:
- `new()` → `new_with_polymorphic()` → `new_with_fk_edge()` (each adds parameters)
- `new_denormalized()` — For denormalized edges (node props in edge table)
- `new_mixed()` — For hybrid patterns (one node denorm, one standard)

**Key functions**:
```
generate_cte() → Cte
  └─ generate_recursive_sql() → String
       ├─ is_heterogeneous_polymorphic_path() check
       │   └─ generate_heterogeneous_polymorphic_sql()  // 2-CTE approach
       └─ standard path:
            ├─ generate_base_case()                     // First hop SQL
            ├─ generate_recursive_case_with_cte_name()  // Subsequent hops SQL
            └─ generate_tiered_cte_sql()                // _inner + _to_target pattern
```

**5 Schema Variations × 2 Cases = 10+ Code Paths**:

| Variation | Base Case | Recursive Case |
|-----------|-----------|----------------|
| **Standard** | 3-way JOIN (start→edge→end) | Recursive JOIN on prev end_id |
| **FK-edge** | 2-way JOIN (node→FK target) | Recursive on FK column |
| **Denormalized** | Single-table scan | Recursive single-table |
| **Mixed denorm** | Hybrid JOIN | Hybrid recursive |
| **Polymorphic** | Standard + WHERE type_column = 'X' | Recursive + type filter |

**Critical branching booleans**:
- `is_fk_edge` — No separate edge table; FK column on node table
- `start_is_denormalized` / `end_is_denormalized` — Node props from edge table
- `type_column.is_some()` — Polymorphic: add WHERE type_column = 'REL_TYPE'
- `is_heterogeneous_polymorphic_path()` — Two CTEs for different intermediate types

**Edge uniqueness**: Uses `path_edges` array with `NOT has()` check. Edge ID can be:
- Default: `tuple(from_id, to_id)` when no edge_id configured
- Single: `rel.edge_id_column`
- Composite: `tuple(rel.col1, rel.col2, ...)`

**Path tracking arrays**:
- `path_nodes` — Array of visited node IDs (for cycle prevention)
- `path_edges` — Array of edge tuples (for edge uniqueness)
- `path_relationships` — Array of relationship type strings
- `hop_count` — Integer counter

### multi_type_vlp_joins.rs (1,337 lines) — Multi-Type Path Generator

**What it does**: When a VLP crosses multiple node/relationship types, recursive CTEs
become unsafe (polymorphic IDs). Instead, generates explicit JOIN chains combined via UNION ALL.

**Example**:
```cypher
MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x:User|Post)
```
Generates separate JOIN chains for each valid path combination, UNIONed together.

**Key struct**: `MultiTypeVlpJoinGenerator`
- Enforces 3-hop max (combinatorial explosion)
- `PropertySelectionMode`: IdOnly | Individual | WholeNode (JSON)
- Browser expand pattern uses WholeNode for heterogeneous end nodes

**Key function**: `generate_cte_sql(cte_name) → String`
- Calls `enumerate_vlp_paths()` / `enumerate_vlp_paths_undirected()`
- Generates per-branch SQL via `generate_path_branch_sql()`
- Handles parameterized views (`view_parameter_values`)

### function_registry.rs (1,177 lines) — Function Mapping Table

**What it does**: Maps 73+ Neo4j/Cypher function names to ClickHouse equivalents.
Uses `lazy_static!` HashMap with optional argument transformation functions.

**Categories**: DateTime (7), String (14), Math (14), List (5), Type Conversion (4),
Aggregation (9), Trig (7), Predicate/Null (2), Vector/Similarity (4), Temporal Extraction (10),
Spatial (placeholder).

**Notable transformations**:
- `substring(s, start, len)` → `substring(s, start+1, len)` (0-indexed → 1-indexed)
- `split(str, delim)` → `splitByChar(delim, str)` (args swapped!)
- `rand()` → `rand() / 4294967295.0` (normalize UInt32 to 0.0-1.0)
- `id(node)` → `toInt64(0)` (placeholder — actual ID computed at result transform time)
- `gds.similarity.cosine(v1,v2)` → `1 - cosineDistance(v1, v2)`

### function_translator.rs (952 lines) — Function Translation Logic

**What it does**: `translate_scalar_function()` entry point. Handles:
- Standard Neo4j→ClickHouse mapping via registry lookup
- `ch.functionName()` passthrough (any ClickHouse function directly)
- `chagg.functionName()` explicit aggregate passthrough
- `duration({days: 5})` → `toIntervalDay(5)` special handling

**CH_AGGREGATE_FUNCTIONS**: Registry of 200+ known ClickHouse aggregate function names.
Used to determine GROUP BY behavior when `ch.` prefix is used.

### json_builder.rs (331 lines) — JSON Property Generation

**What it does**: Generates `formatRowNoNewline('JSONEachRow', ...)` SQL for type-preserving
JSON properties. Unlike `toJSONString(map(...))`, this preserves native types.

**Key functions**:
- `generate_json_properties_sql()` — With AS aliases (Cypher property names)
- `generate_json_properties_without_aliases()` — Without aliases (for CTEs with JOINs)
- `generate_json_from_denormalized_properties()` — With key prefixes (`_s_`, `_e_`)
- `generate_multi_type_union_sql()` — UNION ALL across all node types (label-less queries)

### pagerank.rs (387 lines) — PageRank Algorithm

**What it does**: Generates iterative PageRank SQL using WITH RECURSIVE.
Configurable iterations, damping factor, convergence threshold.

**Used by**: `server/handlers.rs` and `server/sql_generation_handler.rs` for `CALL pagerank(...)`.

## Critical Invariants

### 1. Task-Local Render Context
`to_sql_query.rs` sets task-local contexts via `set_all_render_contexts()` before rendering
and clears them via `clear_all_render_contexts()` after. These contexts are:
- **Relationship columns**: alias → (from_id_column, to_id_column) for IS NULL checks
- **CTE property mappings**: cte_alias → (property → column) for property resolution
- **Multi-type VLP aliases**: Cypher alias → CTE name for JSON extraction
- **VariableRegistry** (PR #120): Per-CTE `VariableSource::Cte { property_mapping }` for
  runtime property resolution during `PropertyAccessExp::to_sql()`. Set via
  `set_current_registry()` in `query_context.rs`. Per-CTE save/restore in `Cte::to_sql()`.

**If you skip clearing**: Context leaks to next query on same async task → wrong SQL.

### 2. UNION ALL Must Have Matching Columns
All UNION branches must select identical column sets. `multi_type_vlp_joins.rs` handles this
by padding NULL for missing properties across node types.

### 3. ClickHouse WITH RECURSIVE Limitation
ClickHouse allows only ONE recursive CTE per WITH RECURSIVE block. Additional recursive CTEs
are wrapped in subqueries: `cte_name AS (SELECT * FROM (WITH RECURSIVE inner_cte AS (...) SELECT * FROM inner_cte))`.
See `CteItems::to_sql()` in to_sql_query.rs.

### 4. VLP Alias Rewriting Must Not Overwrite WITH CTE Columns
When VLP and WITH CTEs coexist, the VLP rewriter must skip aliases covered by WITH CTE JOINs.
Detection: check if JOIN table_name starts with `with_`.

### 5. LEFT JOIN Pre-Filter Must Use Subquery Form
For LEFT JOINs with pre-filters, the filter must be inside a subquery:
`LEFT JOIN (SELECT * FROM table WHERE filter) AS alias ON ...`
INNER JOINs add the filter to the ON clause instead.

### 6. String Concatenation Uses concat()
ClickHouse does not support `+` for string concatenation. The `+` operator with any string operand
is automatically converted to `concat()` with flattened operands.

### 7. Cypher 0-Based vs ClickHouse 1-Based Indexing
`substring()` adds +1 to the start index. Array subscripts are 1-based in both systems (no conversion needed).

### 8. Opaque String Expressions Cannot Be Rewritten ⚠️ ARCHITECTURAL DEBT

Three `RenderExpr` variants carry pre-rendered SQL strings instead of structured
sub-expressions. This means CTE scope rewriting **cannot reach inside them**:

| Variant | Carries | Created in | Example content |
|---------|---------|-----------|-----------------|
| `Raw(String)` | Opaque SQL | `render_expr.rs:914-918` | `"NOT EXISTS (SELECT 1 FROM ... WHERE ... = person.id)"` |
| `ExistsSubquery { sql: String }` | Opaque SQL | `render_expr.rs:940-944` | `"EXISTS (SELECT 1 FROM ... WHERE ...)"` |
| `PatternCount { sql: String }` | Opaque SQL | `render_expr.rs:975-978` | `"(SELECT count(*) FROM ... WHERE ... = a.id)"` |

**Impact**: When these expressions appear after a WITH scope barrier, the embedded
variable names (`person.id`, `a.id`) refer to the original table, not the CTE.
All expression rewriting functions (`rewrite_expression_simple`,
`rewrite_expression_with_cte_alias`, `remap_cte_names_in_expr`) skip these
variants via `other => other.clone()`.

**In `to_sql_query.rs`**: `RenderExpr::Raw(raw) => raw.clone()` (line ~2694)
simply passes the string through unchanged. No opportunity to rewrite.

**Correct fix**: These should carry structured `RenderExpr` sub-expressions,
with SQL rendering deferred to `to_sql()`. See `render_plan/AGENTS.md` §10
for the full architecture description and migration plan.

## Common Bug Patterns

| Pattern | Symptom | Where to Fix |
|---------|---------|-------------|
| Type filter missing in recursive case | Traverses wrong relationship types | `variable_length_cte.rs`: polymorphic WHERE in recursive case |
| FK-edge self-JOIN | Redundant JOIN on same table | `variable_length_cte.rs`: `is_fk_edge` + same start/end table |
| Wrong property source | "Column not found" errors | `start_is_denormalized` vs node table in VLP base case |
| Heterogeneous path filter loss | Wrong intermediate nodes included | `generate_heterogeneous_polymorphic_sql()` |
| JSON vs individual columns | Mismatched SELECT in UNION ALL | `PropertySelectionMode` inconsistency across branches |
| VLP rewriting on WITH CTE | Overwrites WITH CTE column references | `rewrite_vlp_select_aliases()` not checking FROM type |
| UNION + ORDER BY on wrong scope | ORDER BY only affects last branch | Missing subquery wrapper in `render_plan_to_sql()` |
| Column heuristic wrong table | Wrong table alias prefix on column | `RenderExpr::Column` heuristic in `to_sql_query.rs` |
| Context leak between queries | Wrong relationship columns in IS NULL | Missing `clear_all_render_contexts()` |
| CTE name collision | Duplicate CTE in WITH clause | CTE deduplication logic in `CteItems::to_sql()` |
| `derive_cypher_property_name()` wrong | VLP property maps to wrong CTE column | Hardcoded mappings don't cover all schemas |

## Schema Variation Awareness

### Standard Schema (e.g., social_benchmark)
- Separate node and edge tables
- 3-way JOINs: node → edge → node
- Properties from node tables

### FK-Edge Schema (e.g., filesystem with parent_id)
- Edge is a FK column on the node table itself
- 2-way JOINs: node.fk_col → node.id
- `is_fk_edge = true` in VLP generator

### Denormalized Schema (e.g., ontime_flights)
- No separate node tables; node properties embedded in edge table
- `from_node_properties` / `to_node_properties` on relationship schema
- Single-table scans in VLP base case
- JSON generation uses `generate_json_from_denormalized_properties()`

### Polymorphic Schema (e.g., social_polymorphic)
- Unified edge table with type discriminator column
- VLP adds `WHERE type_column = 'TYPE'` to filter edges
- `from_label_column` / `to_label_column` for node type filtering

### Composite ID Schema
- `Identifier::Composite(vec![...])` for multi-column node IDs
- Edge uniqueness uses composite tuple: `tuple(col1, col2, ...)`
- JSON builder concatenates columns with pipe separator for `_id`

## Dependencies

### This Module Depends On:
- `render_plan` — RenderPlan, RenderExpr, Cte, Join, SelectItems, etc.
- `query_planner::logical_plan` — LogicalPlan, ViewScan, VariableLengthSpec, ShortestPathMode
- `query_planner::logical_expr` — LogicalExpr, Literal, Operator (for to_sql.rs)
- `query_planner::join_context` — VLP_CTE_FROM_ALIAS, VLP_START_ID_COLUMN, VLP_END_ID_COLUMN
- `query_planner::analyzer::multi_type_vlp_expansion` — enumerate_vlp_paths()
- `query_planner::plan_ctx` — PlanCtx (for property requirements in multi-type VLP)
- `graph_catalog::graph_schema` — GraphSchema, NodeSchema
- `graph_catalog::expression_parser` — PropertyValue
- `graph_catalog::config` — Identifier (Single/Composite)
- `server::query_context` — Task-local render context accessors
- `utils::cte_naming` — is_generated_cte_name()

### What Depends On This Module:
- `server/handlers.rs` — HTTP query handler calls `generate_sql()`
- `server/sql_generation_handler.rs` — SQL generation handler calls `generate_sql()`
- `server/bolt_protocol/handler.rs` — Bolt protocol calls `generate_sql()`
- `render_plan/cte_extraction.rs` — Imports `NodeProperty`, `MultiTypeVlpJoinGenerator`
- `render_plan/cte_generation.rs` — Imports `NodeProperty`
- `render_plan/cte_manager/mod.rs` — Imports `VariableLengthCteGenerator`, `ShortestPathMode`, `NodeProperty`
- `render_plan/render_expr.rs` — Imports `render_plan_to_sql` (for RenderPlan::to_sql trait)
- `query_planner/tests/integration_tests.rs` — Tests call `generate_sql()`
- `render_plan/tests/` — Various test files call `generate_sql()`

## Public API

### From mod.rs:
```rust
pub fn generate_sql(plan: RenderPlan, max_cte_depth: u32) -> String
```

### Re-exported types and functions:
```rust
pub use errors::ClickhouseQueryGeneratorError;
pub use function_translator::{
    get_ch_function_name, get_supported_functions, is_ch_aggregate_function,
    is_ch_passthrough, is_ch_passthrough_aggregate, is_explicit_ch_aggregate,
    is_function_supported, translate_scalar_function,
    CH_AGG_PREFIX, CH_PASSTHROUGH_PREFIX,
};
pub use json_builder::{
    generate_json_properties_from_schema, generate_json_properties_sql,
    generate_multi_type_union_sql,
};
pub use multi_type_vlp_joins::MultiTypeVlpJoinGenerator;
pub use variable_length_cte::{NodeProperty, VariableLengthCteGenerator};
```

### Key traits:
- `ToSql` (to_sql.rs): `fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError>`
  — for LogicalExpr, LogicalPlan
- `ToSql` (to_sql_query.rs / render_plan): `fn to_sql(&self) -> String`
  — for RenderPlan, RenderExpr, and all plan components

## Testing Guidance

### After Changes to to_sql_query.rs:
```bash
cargo test --lib                      # All unit tests (325+)
cargo test variable_length            # VLP-specific tests
cargo test multi_type_vlp             # Multi-type VLP tests
cargo test where_clause               # WHERE clause tests
cargo test edge_uniqueness            # Edge uniqueness tests
cargo test function_translator        # Function translation tests
cargo test function_registry          # Function registry tests
cargo test multiple_relationship      # Multiple relationship type tests
cargo test integration_tests          # Integration tests
```

### After Changes to variable_length_cte.rs:
```bash
cargo test variable_length            # VLP CTE tests
cargo test edge_uniqueness            # Edge uniqueness
cargo test where_clause               # Shortest path WHERE
```

### After Changes to function_registry.rs or function_translator.rs:
```bash
cargo test function_translator        # Translation tests
cargo test function_registry          # Registry tests
```

### Manual Smoke Test (browser expand pattern):
```bash
curl -X POST localhost:8080/query -H "Content-Type: application/json" \
  -d '{"query": "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.user_id = 1 RETURN b.name", "sql_only": true}'
```

## Files You Should NOT Touch Casually

### 🔴 High Risk (affects all queries):
- **to_sql_query.rs** — `RenderExpr::to_sql()` and `render_plan_to_sql()` affect every single
  query. Any change here requires running the full test suite.
- **variable_length_cte.rs** — 5 schema variations × 2 cases. Changes to base/recursive case
  generation can break specific schema types while appearing to work for others.

### 🟡 Medium Risk:
- **multi_type_vlp_joins.rs** — Affects browser expand and multi-type traversals.
  Test with multiple schema types.
- **function_registry.rs** — Adding functions is safe; modifying existing arg_transform
  functions can break queries silently.
- **json_builder.rs** — Changes affect VLP property serialization and label-less queries.

### 🟢 Lower Risk:
- **function_translator.rs** — Well-isolated, good test coverage.
- **pagerank.rs** — Self-contained, only used by CALL pagerank().
- **view_query.rs / view_scan.rs** — Small, rarely changed.
- **errors.rs** — Adding new variants is safe.

## Schema Variation Checklist

When modifying VLP generation, verify SQL output for:
- [ ] Standard: `MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)`
- [ ] FK-edge: `MATCH (o:FsObject)-[:PARENT_OF*1..3]->(p:FsObject)`
- [ ] Denormalized: `MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport)`
- [ ] Polymorphic: `MATCH (u:User)-[:FOLLOWS]->(f:User)` on polymorphic schema
- [ ] Multi-type expand: `MATCH (a:User)--(o)` (browser pattern)
- [ ] Undirected: `MATCH (a:User)-[r]-(b:User)` (UNION ALL both directions)
- [ ] Shortest path: `MATCH p = shortestPath((a)-[*]->(b))`

## Known Technical Debt

1. **Operator rendering duplication**: `to_sql.rs` and `to_sql_query.rs` duplicate ~70 lines
   of operator handling for two different Operator types. Documented in both files.

2. **`derive_cypher_property_name()`**: Hardcoded DB→Cypher mappings (`full_name→name`,
   `email_address→email`). Should use schema context. In `to_sql_query.rs`.

3. **Column heuristic inference**: `RenderExpr::Column` guesses table alias from column name patterns.
   Fragile for non-standard naming. In `to_sql_query.rs`.

4. **Literal type duplication**: `LogicalExpr::Literal` vs `RenderExpr::Literal` are structurally
   similar but different types. Documented in `common.rs`.

5. **`RenderExpr::to_sql()` returns String, not Result**: Some error paths return empty strings
   or log warnings instead of propagating errors. Should eventually return `Result<String, Error>`.
