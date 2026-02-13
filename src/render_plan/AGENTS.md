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

### 1. CTE Column Naming Convention
WITH CTE columns are named `{alias}_{property}`:
- Node alias `a` with property `user_id` → CTE column `a_user_id`
- Node alias `a` with property `name` → CTE column `a_name`
- Scalar alias `allNeighboursCount` → CTE column `allNeighboursCount` (no prefix)

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

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| `a_start_id` in JOIN | ClickHouse: "cannot be resolved" | `find_id_column_for_alias` VLP shortcut before GraphNode |
| Wrong property mapping | Properties from wrong table | Denormalized vs standard property source confusion |
| Missing CTE columns | Column not found in subquery | `cte_schemas` not populated for this CTE |
| Duplicate CTEs | Same CTE name generated twice | CTE name collision, missing dedup check |
| Passthrough WITH not collapsed | Extra unnecessary CTE layer | `is_passthrough` detection failure |
| CTE name remapping missed | Old CTE name referenced | `cte_name_remaps` not applied to all expressions |

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
