# render_plan Module — Agent Guide

> **Purpose**: Converts `LogicalPlan` (Cypher AST) → `RenderPlan` (SQL-ready IR) → SQL string.
> This is where most regressions happen. Read this before touching any file here.

## Module Architecture

```
LogicalPlan (from query_planner)
    │
    ▼
plan_builder.rs          ← trait RenderPlanBuilder: LogicalPlan → RenderPlan
    │                       dispatches to build_chained_with_match_cte_plan for WITH+MATCH
    │
    ├─ plan_builder_utils.rs (12K lines) ← the beast: CTE extraction, expression rewriting,
    │                                       WITH→CTE transformation, VLP+WITH JOIN generation
    │
    ├─ plan_builder_helpers.rs (4.6K)   ← schema lookups, property resolution, label fallbacks
    │
    ├─ join_builder.rs (2.5K)           ← JOIN clause extraction (standard, FK-edge, polymorphic)
    │
    ├─ cte_extraction.rs (5.5K)         ← VLP context building, relationship column extraction
    │
    ├─ cte_generation.rs (815)          ← CteGenerationContext: property→column mapping for VLPs
    │
    ├─ cte_manager/mod.rs (3.5K)        ← VLP CTE generation & management
    │
    ├─ render_expr.rs (1.2K)            ← RenderExpr enum (Column, Literal, Operator, etc.)
    │
    ├─ select_builder.rs, from_builder.rs, filter_builder.rs, group_by_builder.rs
    │
    └─ mod.rs (531)                     ← RenderPlan, Join, Cte, SelectItem structs
```

## Critical Invariants

### 0. CTEs Are Flat — No Nesting, Ever

**This is the most important structural rule in the entire codebase.**

CTEs are always a flat, linear chain at the top level of the query. No CTE definition
may appear inside another CTE body, inside a union branch, or inside a subquery.
There is exactly one `WITH RECURSIVE` keyword at the beginning of the SQL (if any CTE
is recursive), followed by all CTEs comma-separated in dependency order.

During plan building, CTEs may temporarily live inside union branches or nested
RenderPlans. That's fine — `flatten_all_ctes()` in `to_sql_query.rs` extracts them all
to the top level before rendering. But plan-building code should still aim to place CTEs
at the highest possible level (via `hoist_nested_ctes()`) to keep the IR clean.

**If you're adding code that creates CTEs**: just attach them to the nearest `RenderPlan.ctes`.
The flattening pass will handle placement. Never try to render CTEs inline.

### 1. CTE Column Naming Convention

WITH CTE columns use the **unambiguous `p{N}` format** (defined in `src/utils/cte_column_naming.rs`):

```
p{N}_{alias}_{property}
```

Where `N` is the character length of the alias (decimal digits). This eliminates
ambiguity when aliases contain underscores (e.g., `person_1_name` — is that
alias=`person_1` property=`name`, or alias=`person` property=`1_name`?).

**Examples**:
- `("u", "name")` → `p1_u_name`
- `("person_1", "user_id")` → `p8_person_1_user_id`
- `("a", "user_id")` → `p1_a_user_id`
- Scalar alias `allNeighboursCount` → `allNeighboursCount` (no prefix, no encoding)

**Three patterns recognized** (in priority order, for backward compatibility):
1. `alias.property` (dotted) — used in VLP CTEs
2. `p{N}_alias_property` (new unambiguous) — primary WITH CTE format
3. `alias_property` (legacy underscore) — fallback

Key functions:
- `cte_column_name(alias, property)` — generate
- `parse_cte_column(col_name)` — parse back to `(alias, property)`

**NEVER** reference VLP internal columns (`start_id`, `end_id`) in WITH CTE schemas.
The WITH CTE uses the node's actual ID column, not the VLP's.

### 2. find_id_column_for_alias() Priority
In `plan_builder.rs`, this function traverses the plan to find a node's ID column.
**Order matters**:
1. First: check GraphNode branches (left/right) for actual node ID (e.g., `user_id`)
2. Fallback: VLP endpoint columns (`start_id`/`end_id`) — only for denormalized schemas
   where no separate node table exists

If you reverse this order, WITH CTE JOINs break (the `a_start_id` regression).

### 3. build_chained_with_match_cte_plan Flow
This is the most complex function (~3500 lines in plan_builder_utils.rs):
```
Input:  LogicalPlan with WITH clause(s) + subsequent MATCH
Output: RenderPlan with CTEs, JOINs, SELECT rewriting

Steps:
1. Extract correlation predicates BEFORE any transformation
2. Iteratively process WITH clauses (innermost first)
3. For each WITH: render SQL, create CTE, store schema in cte_schemas
4. Replace WITH clause in plan with CTE reference
5. After all WITHs processed: render final plan
6. Add CTE JOINs with correct column references
7. Rewrite SELECT/WHERE/ORDER BY to use CTE column names
```

**Key data structure**: `cte_schemas: HashMap<cte_name, (items, props, alias_to_id, property_mapping)>`
- `alias_to_id`: maps node alias → CTE column name holding its ID
- `property_mapping`: maps (alias, cypher_property) → CTE column name

### 4. VLP+WITH CTE JOIN
When FROM is a VLP CTE and a WITH CTE needs to be JOINed:
```sql
-- VLP CTE has: start_id (= toString(user_id))
-- WITH CTE has: a_user_id (the actual column)
-- JOIN must be: t.start_id = toString(a_allNeighboursCount.a_user_id)
-- NOT:          t.start_id = toString(a_allNeighboursCount.a_start_id)  ← BUG
```

The `alias_to_id` map is populated by `compute_cte_id_column_for_alias()` which calls
`find_id_column_for_alias()`. See invariant #2 above.

### 5. FROM Clause & JOIN Order for OPTIONAL MATCH ⚠️ CRITICAL

**The Problem**: When processing `MATCH...OPTIONAL MATCH...WITH`, which node becomes the FROM table, and how should JOINs be ordered?

```cypher
MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass)      -- Required pattern
OPTIONAL MATCH (m:Message)-[:HAS_TAG]->(tag)  -- Optional pattern
WITH tag, count(m) AS cnt
```

**Standard case**: When anchor == left_connection (e.g., `OPTIONAL MATCH (u)-[:KNOWS]->(f)`), standard logic works — left is FROM, joins go left→rel→right.

**Reversed case**: When anchor == right_connection (Bug #3), the FROM table and JOIN order must be reversed.

**Wrong SQL** (Bug #3 - before fix):
```sql
FROM Message AS m            -- ❌ Optional node used as FROM
INNER JOIN ... ON tag.id ... -- ❌ tag doesn't exist yet!
LEFT JOIN Tag AS tag ...     -- ✅ Joined too late
```

**Correct SQL** (after fix):
```sql
FROM Tag AS tag                                                     -- ✅ Required anchor
INNER JOIN Tag_hasType_TagClass AS t2 ON t2.TagId = tag.id          -- ✅ Inner pattern
INNER JOIN TagClass AS t1 ON t1.id = t2.TagClassId                  -- ✅ Inner pattern
LEFT JOIN Message_hasTag_Tag AS t3 ON t3.TagId = tag.id             -- ✅ rel→anchor (reversed)
LEFT JOIN (SELECT * FROM Message WHERE (...)) AS m ON m.id = t3.MessageId  -- ✅ Optional left node
WHERE t1.name = 'MusicalArtist'                                    -- ✅ Anchor filter in WHERE
```

**How This Works — Three-Layer Fix**:

1. **FROM Clause Selection** (`from_builder.rs::extract_from_graph_rel()`, lines 451-506):
   - Checks `anchor_connection` field FIRST
   - Searches for GraphNode with matching alias in left/right/nested positions
   - Returns FROM for the anchor node when found
   - Falls back to left node if anchor_connection is None

2. **JOIN Order Reversal** (`join_builder.rs::extract_joins()`, lines 2075-2176):
   - When `anchor_connection == right_connection` AND right is nested GraphRel:
   - Resolves anchor node's primary key from schema via nested GraphRel inspection
   - **JOIN 1**: `rel.to_id = anchor.id` (connect relationship to FROM/anchor)
   - **JOIN 2**: `left.id = rel.from_id` (connect optional node to relationship)
   - Skips the standard shared-node JOIN (anchor is already FROM)
   - Returns early to avoid duplicate JOINs

3. **Predicate Extraction for Optional Node** (`join_builder.rs`, lines 1943-1990):
   - Standard: extracts user predicates for right_connection (optional)
   - Reversed: also extracts predicates for left_connection (optional when anchor is right)
   - Optional node predicates become `pre_filter` (subquery) for LEFT JOIN semantics
   - Anchor node predicates remain in WHERE clause

**Key Variable: `anchor_on_right`** — computed from `graph_rel.anchor_connection`:
```rust
let anchor_on_right = graph_rel.anchor_connection.as_ref()
    .map(|a| a == &graph_rel.right_connection).unwrap_or(false);
```

**Key Insights**:
- `GraphJoins.joins` list is **EMPTY and DEPRECATED** for OPTIONAL MATCH patterns
- Joins are extracted during rendering via `input.extract_joins()`
- `reorder_joins_by_dependencies()` is NOT called for these cases
- Anchor selection happens in `extract_from()`, not in join reordering
- `end_node_id`/`end_id_col` are UNRELIABLE when right is nested GraphRel (falls back to wrong column)
- Anchor node ID must be resolved from schema via nested GraphRel node label lookup

**Bug History**: Code comment (lines 459-463) described this scenario since initial implementation, stating "This case needs special handling", but the handling was never implemented until Feb 2026. Fixed in three commits: FROM clause, JOIN order reversal, predicate extraction.

### 6. WITH Clause Processing Must Traverse All Plan Node Types ⚠️ CRITICAL

**Background**: `build_chained_with_match_cte_plan` iterates to extract WITH clauses. Each iteration:
1. `has_with_clause_in_tree()` detects if a WITH clause exists in the plan
2. `find_all_with_clauses_grouped()` finds and groups them
3. `replace_with_clause_with_cte_reference_v2()` replaces the WITH with a CTE reference

Inside step 3, there are two additional traversal helpers used within the `GraphRel` match arm:
4. `plan_contains_with_clause()` — checks if a sub-tree contains any WithClause
5. `needs_processing()` — checks if a sub-tree needs CTE replacement for a given alias

**The Trap**: ALL FIVE functions must agree on plan traversal. If any function can't traverse
through a plan node type (e.g., `Unwind`, `CartesianProduct`), WITH clauses nested inside
that node become invisible, causing:
- Detection succeeds (step 1) but replacement skips the sub-tree (step 3) → WITH clause persists
- Next iteration finds same WITH, skips it (already in `processed_cte_aliases`) → no progress
- Loop ends → "Failed to process all WITH clauses" error

**Feb 2026 fix**: `plan_contains_with_clause()` and `needs_processing()` were missing `Unwind`
and `CartesianProduct` variants. The replacement function itself (`replace_with_clause_with_cte_reference_v2`)
already handled them, but the guard checks at the `GraphRel` match arm prevented entry.
This blocked `WITH collect(x) as xs UNWIND xs as x MATCH (x)-[]->(y)` patterns.

**Pattern to watch**: Any time a NEW `LogicalPlan` variant is added, verify ALL FIVE functions handle it:
- `has_with_clause_in_tree()` (plan_builder_utils.rs ~line 3537)
- `plan_contains_with_clause()` (plan_builder_utils.rs ~line 4186)
- `find_all_with_clauses_grouped()` / `find_all_with_clauses_impl()` (plan_builder_utils.rs ~line 10989)
- `needs_processing()` (inside `replace_with_clause_with_cte_reference_v2`, plan_builder_utils.rs ~line 12125)
- `replace_with_clause_with_cte_reference_v2()` (plan_builder_utils.rs ~line 11548)

If even one function misses the new variant, infinite iteration or lost WITH clauses can result.

### 6b. recreate_pattern_schema_context: 3-Tier Label Resolution

**Background**: `recreate_pattern_schema_context()` (cte_extraction.rs) reconstructs a
`PatternSchemaContext` for VLP CTE generation. It needs left/right node labels to look up
node schemas. After WITH→CTE replacement, CTE reference GraphNodes have `label: None`.

**3-tier resolution** (added Feb 2026):
1. **Explicit label** from plan tree (`GraphNode.label` — set by parser)
2. **Inferred label** from `PlanCtx` (`TableCtx.labels` — set by type inference)
3. **Relationship-based inference** from schema's `from_node`/`to_node` fields

**Why tier 2 exists**: Type inference updates `TableCtx.labels` in `PlanCtx` but does NOT
update `GraphNode.label` in the plan tree (plan tree is Arc-wrapped, conceptually immutable
after parsing). The PlanCtx is the correct source for inferred type information.

**Composite key handling**: `GraphRel.labels` can contain composite keys like
`"REPLY_OF::Comment::Post"`. The `infer_node_labels_from_rel()` function extracts the
simple type name (before `::`) for `rel_schemas_for_type()` lookup.

### 7. Stack Overflow with Deep Plan Trees

**Problem**: Bidirectional relationship patterns (`-[:REL]-` without direction) generate UNION plans that double the plan depth. Combined with multiple WITH clauses, UNWIND, and OPTIONAL MATCH, the recursive plan traversal can exceed the default 2MB tokio thread stack.

**Symptoms**:
- Server process silently crashes (no error message, no log output)
- Thread killed by OS signal (SIGSEGV on stack guard page)
- Only occurs with complex queries combining bidirectional + WITH chains + UNWIND

**Mitigation**: `main.rs` configures tokio runtime with `thread_stack_size(128 * 1024 * 1024)` (128 MB default). Configurable via `CLICKGRAPH_THREAD_STACK_MB` environment variable. Debug builds need more stack than release builds due to larger call frames.

**Root cause**: Deep recursive traversal in plan_builder_utils.rs functions (build_chained_with_match_cte_plan, replace_with_clause_with_cte_reference_v2, etc.). Long-term fix: convert recursive traversal to iterative using an explicit stack.

### 8. Projection-Guided UNION Rendering

**Problem**: When TypeInference generates UNION branches for untyped nodes, should we add `__label__` column?

**Answer**: Only when the query returns the whole entity (`RETURN n`), not specific properties (`RETURN n.name`).

**Implementation** (plan_builder.rs ~line 2557):
```rust
fn returns_whole_entity(plan: &LogicalPlan) -> bool {
    // Check if Projection contains TableAlias (whole entity)
    // vs PropertyAccessExp (specific property)
    // Must traverse: Limit, Skip, OrderBy, Filter, GraphJoins
}
```

**UNION normalization always runs**, but `__label__` injection is conditional:
```rust
if returns_whole_entity(&branch.input) {
    // Add: SELECT ... , '<label>' AS "__label__" FROM ...
    normalized_columns.push(SelectItem::column_expr(...));
}
```

**Why**: `RETURN n.name` doesn't need label info, but Neo4j Browser's `RETURN n` needs `__label__` to color nodes.

### 9. Self-Join Alias Handling for Same-Table Relationships

**Problem**: When a relationship connects the same node type (e.g., User→FOLLOWS→User), the SQL generator creates two JOINs to the same table without aliases:

```sql
-- ❌ WRONG (ClickHouse can't distinguish):
INNER JOIN social.users ON ...
INNER JOIN social.users ON ...

-- ✅ CORRECT:
INNER JOIN social.users AS from_node ON ...
INNER JOIN social.users AS to_node ON ...
```

**Detection** (cte_extraction.rs ~line 3055):
```rust
let needs_alias = from_table == to_table;
let from_join_expr = if needs_alias {
    format!("{} AS from_node", from_table)
} else {
    from_table.clone()
};
```

**Impact**: Without aliases, ClickHouse silently returns empty results for same-table relationships (e.g., User→FOLLOWS→User, Person→KNOWS→Person).

### 10. Variable Resolution & WITH Scope Barriers ⚠️ ARCHITECTURAL

#### The Correct Mental Model

In Cypher, every `WITH` clause creates a **scope barrier**:
- Only explicitly listed variables survive past `WITH`
- Downstream clauses can ONLY reference those surviving variables
- Variables with the same name in different scopes are DIFFERENT entities

In SQL, the CTE IS the scope barrier. A CTE is just a table. After a WITH→CTE
translation, downstream SQL reads from the CTE table using CTE column names.
There is no going "behind" the CTE to reach original tables.

```
Cypher scope stack:           SQL equivalent:
┌─────────────────────┐
│ MATCH (a)-[]->(b)   │  →   FROM A JOIN rel JOIN B
│ WITH a, count(b)    │  →   CTE1 = (SELECT a.id AS p1_a_id, count(b.id) AS cnt FROM ...)
├─────────────────────┤       ← SCOPE BARRIER: a and b from above are GONE
│ MATCH (a)-[]->(c)   │  →   FROM CTE1 AS a JOIN C   (a here = CTE1, NOT the original table)
│ RETURN a.name, c.x  │  →   SELECT a.p4_a_name, c.x FROM CTE1 AS a JOIN ...
└─────────────────────┘
```

#### The Architectural Flaw: Premature Resolution + Reverse Mapping

**Current (broken) flow:**
1. Parser creates: `person.name` → `LogicalExpr::PropertyAccess("person", "name")`
2. Schema lookup resolves to DB column: `"name"` → `"full_name"`
3. RenderExpr becomes: `PropertyAccessExp { table_alias: "person", column: "full_name" }`
4. WITH creates CTE: column `p6_person_full_name` (from `person.full_name`)
5. ❌ NOW we need to undo step 3: map `("person", "full_name")` → `"p6_person_full_name"`
6. ❌ This requires `reverse_mapping: HashMap<(alias, db_column), cte_column>`
7. ❌ Walk ALL expressions post-hoc, rewriting matching PropertyAccessExp entries
8. ❌ `Raw(String)`, `ExistsSubquery { sql }`, `PatternCount { sql }` are opaque — SKIPPED

**Why reverse_mapping is a hack:**
- It's a post-hoc fixup: first resolve to DB columns, then try to undo it
- It can't reach inside opaque string expressions (`Raw`, `ExistsSubquery`, etc.)
- It requires fragile heuristics to match aliases across scopes
- It conflates "resolve property to source" with "resolve property to SQL column"

#### The Correct Architecture: Forward Resolution Through CTE Scope

**Principle**: Variable references should resolve to the **nearest enclosing scope's
column names**, not to the original DB columns. The CTE IS the scope.

**Correct flow:**
1. When a CTE is created, record its **forward mapping** (already exists as `property_mapping`):
   `(cypher_alias, cypher_property)` → `cte_column_name`
   e.g., `("person", "name")` → `"p6_person_name"`, `("person", "id")` → `"p6_person_id"`

2. When building downstream expressions that reference a variable from a CTE scope,
   use the forward mapping directly:
   `person.name` → `PropertyAccessExp { table_alias: cte_from_alias, column: "p6_person_name" }`

3. **Never** resolve to DB columns first and then reverse-map. Go straight from
   Cypher property → CTE column.

4. The `property_mapping` in `cte_schemas` already provides this. The problem is that
   expressions are resolved to DB columns BEFORE the CTE scope is applied.

**What changes:**
- `rewrite_expression_simple()` and `rewrite_expression_with_cte_alias()` should use
  the forward mapping `(cypher_alias, cypher_property) → cte_column`, resolving
  property names in Cypher space (not DB column space)
- `build_property_mapping_from_columns()` should map Cypher property names, not DB column names
- NOT EXISTS, EXISTS, size() patterns must remain as structured `RenderExpr` types
  carrying variable references (not pre-rendered SQL strings) so CTE rewriting can reach them

**Impact of eliminating reverse_mapping:**
- All 88+ usages of `reverse_mapping` in `plan_builder_utils.rs` become forward lookups
- No more "also add DB column mapping" hacks in `build_property_mapping_from_columns()`
- `Raw(String)` expressions that embed variable names become fixable
  (because we rewrite at the right level — Cypher variables, not DB columns)

#### NOT EXISTS / EXISTS / size() — The Opaque String Problem

Three expression types bake variable names into SQL strings too early:

| Type | Where created | Problem |
|------|--------------|---------|
| `RenderExpr::Raw(sql)` | `render_expr.rs:914-918` — NOT (PathPattern) | `person.id` baked into string |
| `ExistsSubquery { sql }` | `render_expr.rs:940-944` — ExistsSubquery | Same |
| `PatternCount { sql }` | `render_expr.rs:975-978` — size(pattern) | Same |

All three call SQL-generation functions (`generate_not_exists_from_path_pattern()`,
`generate_exists_sql()`, `generate_pattern_count_sql()`) during `TryFrom<LogicalExpr>
for RenderExpr` conversion — BEFORE any WITH scope processing.

**Why rewriting can't fix them**: Every expression rewriting function
(`rewrite_expression_simple`, `rewrite_expression_with_cte_alias`,
`remap_cte_names_in_expr`) contains `other => other.clone()` for unhandled variants.
`Raw`, `ExistsSubquery`, `PatternCount` all hit this fallback — their internal SQL
strings are never touched.

**The fix**: These should remain as structured `RenderExpr` types carrying
`RenderExpr` sub-expressions (not pre-rendered SQL). Resolution to SQL should
happen in `to_sql_query.rs` where the current scope's variable sources are known.
For example, `ExistsSubquery` should carry the pattern and filter expressions as
`RenderExpr` trees, not a pre-baked SQL string.

#### Migration Strategy

The fix is incremental — no big-bang rewrite:

1. **Phase 1**: Change CTE-downstream expression building to use forward mapping
   (`property_mapping` from `cte_schemas`) instead of reverse_mapping.
   This means: when we know an expression references a variable that comes from a CTE,
   resolve `(alias, cypher_property)` → `cte_column` directly.

2. **Phase 2**: Refactor `ExistsSubquery`, `PatternCount`, and NOT EXISTS `Raw`
   to carry structured sub-expressions. Update their `to_sql()` to render at the end.

3. **Phase 3**: Remove `reverse_mapping` and all the "also add DB column mapping"
   fallback code in `build_property_mapping_from_columns()`.

Each phase is independently testable and improves correctness.

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| `a_start_id` in JOIN | ClickHouse: "cannot be resolved" | `find_id_column_for_alias` VLP shortcut before GraphNode |
| **Forward reference in OPTIONAL MATCH** | **"Unknown identifier `tag.id`"** | **FROM/JOIN order not using `anchor_connection` when anchor is right_connection** |
| **Infinite WITH iteration** | **"Failed to process all WITH clauses after N iterations"** | **`plan_contains_with_clause` or `needs_processing` doesn't traverse a plan node type (see §6)** |
| Wrong property mapping | Properties from wrong table | Denormalized vs standard property source confusion |
| Missing CTE columns | Column not found in subquery | `cte_schemas` not populated for this CTE |
| Duplicate CTEs | Same CTE name generated twice | CTE name collision, missing dedup check |
| Passthrough WITH not collapsed | Extra unnecessary CTE layer | `is_passthrough` detection failure |
| CTE name remapping missed | Old CTE name referenced | `cte_name_remaps` not applied to all expressions |
| **`__label__` always injected** | **Extra column in property queries** | **`returns_whole_entity()` not checking Projection items** |
| **Same-table relationship empty** | **User→FOLLOWS→User returns nothing** | **Missing `AS from_node` / `AS to_node` aliases for self-joins** |
| **Identifier cannot be resolved after WITH** | **"DB::Exception: Identifier 'person.id' cannot be resolved"** | **Premature resolution: variable baked into Raw/ExistsSubquery SQL before CTE scope rewriting (see §10)** |
| **reverse_mapping misses expression type** | **Variable reference not rewritten to CTE column** | **`rewrite_expression_simple` skips Raw, ExistsSubquery, PatternCount, ReduceExpr, etc. (see §10)** |

## Files You Should NOT Touch Casually

- **plan_builder_utils.rs** — 12K lines, hundreds of edge cases. Any change here can break
  any combination of WITH/VLP/UNION/CTE. Always run full test suite.
- **plan_builder.rs** — The `find_id_column_for_alias` traversal order is critical.

## Testing After Changes

```bash
# Must pass ALL of these:
cargo test                                    # 995 unit + 35 integration + 7 + 25 doc
cargo test test_vlp_with_cte_join             # VLP+WITH regression test
cargo test test_with_clause_property_renaming # WITH alias propagation

# Manual browser test (if changing CTE/JOIN logic):
# 1. Start server with schemas/dev/social_dev.yaml
# 2. Connect Neo4j Browser to bolt://localhost:7687
# 3. MATCH (u:User) RETURN u LIMIT 5
# 4. Click a node to expand — must show edges
```

## Schema Variation Awareness

Every function in this module may behave differently based on:
1. **Standard** — separate node + edge tables, 3-way JOIN
2. **FK-edge** — edge is FK column on node table (`is_fk_edge = true`)
3. **Denormalized** — node properties stored in edge table (`is_denormalized = true`)
4. **Polymorphic** — single edge table, `type_column` discriminator
5. **Composite ID** — multi-column node identity

When fixing a bug, always check: does this fix work for ALL 5 variations?

### Schema Compatibility Matrix

| Feature / Area                               | Standard | FK-edge | Denormalized | Polymorphic | Composite ID |
|---------------------------------------------|:--------:|:-------:|:------------:|:-----------:|:------------:|
| Basic `MATCH` / node scans                  |   ✅     |   ✅    |      ⚠️      |     ✅      |      ⚠️      |
| Single-hop relationship traversals          |   ✅     |   ✅    |      ⚠️      |     ✅      |      ⚠️      |
| Multi-hop / variable-length paths (VLP)     |   ✅     |   ⚠️    |      ⚠️      |     ⚠️      |      ⚠️      |
| OPTIONAL MATCH                              |   ✅     |   ✅    |      ⚠️      |     ✅      |      ⚠️      |
| Multiple relationship types (`[:A\|:B]`)     |   ✅     |   ⚠️    |      ⚠️      |     ✅      |      ⚠️      |
| Shortest path functions                      |   ✅     |   ⚠️    |      ⚠️      |     ⚠️      |      ⚠️      |
| Path projection (`nodes(p)`, `rels(p)`)     |   ✅     |   ⚠️    |      ⚠️      |     ⚠️      |      ⚠️      |
| Multi-schema (`USE`, per-request)           |   ✅     |   ✅    |      ✅       |     ✅      |      ✅       |

✅ = supported and covered by tests. ⚠️ = designed to work but less coverage — add tests before relying on it.
