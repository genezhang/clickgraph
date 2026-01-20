# Known Issues

**Active Issues**: 2 bugs, 4 feature limitations  
**Last Updated**: January 20, 2026

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Current Bugs

### 1. OPTIONAL MATCH + VLP Combination
**Status**: 🐛 BUG  
**Error**: `Identifier 'vlp66.name' cannot be resolved from subquery`  
**Example**:
```cypher
MATCH (a:User) WHERE a.name = 'Alice'
OPTIONAL MATCH (a)-[:FOLLOWS*1..2]->(b:User)
RETURN a.name, COUNT(DISTINCT b) as reachable
```
**Root Cause**: VLP alias rewriting incorrectly maps `a.name` → `vlp66.name` in outer query when VLP is in OPTIONAL MATCH  
**Impact**: Blocks OPTIONAL MATCH combined with variable-length paths  
**Files**: `cte_extraction.rs`, `plan_builder.rs`

### 2. Duplicate CTE Names with Chained WITH and Scalar Aggregates
**Status**: 🐛 BUG  
**Error**: `Syntax error: failed at position 362` (duplicate CTE name in SQL)  
**Example**:
```cypher
MATCH (m:Message)
WITH count(m) AS total
MATCH (m:Message)
WITH total, count(m) AS cnt  -- Generates duplicate CTE name
RETURN total, cnt
```
**Root Cause**: CTE naming logic generates the same name (`with_total_cte_1`) for both the first WITH and second WITH when reusing scalar aggregates. ClickHouse rejects duplicate CTE names.  
**Generated SQL**:
```sql
WITH with_total_cte_1 AS (...),
     with_total_cte_1 AS (...)  -- ERROR: duplicate name
SELECT ...
```
**Impact**: Blocks chained WITHs that reference scalar aggregates from previous WITH clauses  
**Workaround**: Avoid reusing scalar aggregate aliases across multiple WITH clauses  
**Files**: `render_plan/plan_builder.rs` (CTE naming logic around `build_chained_with_match_cte_plan`)  
**Note**: Jan 11 2026 - Previous attempt to fix this by treating scalars differently in TableAlias expansion was incorrect and caused relationship return bugs

---

## Recently Fixed

### ~~1. MULTI_TABLE_LABEL Node Expansion with Dotted Column Names~~ ✅ **FIXED** - January 12, 2026
**Was**: For schemas where the same node label appears in multiple tables (MULTI_TABLE_LABEL, e.g., zeek IP nodes), full node expansion (`RETURN n`) failed with `Identifier 'n.id' cannot be resolved` because column names with multiple dots (like `id.orig_h`) were being truncated to just the first segment.

**Example** (zeek schema with `id.orig_h` column):
```cypher
MATCH (n:IP) RETURN n
-- Generated: SELECT n.id AS "n_ip", ...
-- Should be:  SELECT n."id.orig_h" AS "n_ip", ...
```

**Root Cause**: 
- For MULTI_TABLE_LABEL schemas, nodes are expanded into UNION branches
- Each branch has `projected_columns` like `[("ip", "n.id.orig_h")]`
- When extracting unqualified column names, code used `.split('.').nth(1)` 
- For "n.id.orig_h", this gave ["n", "id", "orig", "h"][1] = "id" ❌
- Should have preserved "id.orig_h" ✅

**Fix**: Changed `plan_builder.rs` line 6337 from:
```rust
.split('.').nth(1)  // Takes only first segment after dot
```
To:
```rust
.splitn(2, '.').nth(1)  // Splits only on FIRST dot, preserves rest
```

**Result**: For "n.id.orig_h", now correctly extracts "id.orig_h"

**Test Results**:
- ✅ `MATCH (n:IP) RETURN n` generates correct SQL with `n."id.orig_h"`
- ✅ Full node expansion works for all zeek schemas
- ✅ Property-specific access (`RETURN n.ip`) still works correctly

**Impact**: Unblocks ~78 comprehensive matrix tests that were skipped for MULTI_TABLE_LABEL schemas

**Files**: `src/render_plan/plan_builder.rs` (line 6337)

### ~~2. WITH Clause Expression Scope Resolution~~ ✅ **FIXED** - January 12, 2026
**Was**: CASE expressions and complex expressions in WITH clauses referencing variables from prior WITH clauses failed with "Unknown expression identifier" errors because table aliases weren't being rewritten to CTE names.

**Example**:
```cypher
MATCH (p:Person)-[:KNOWS]-(f:Person), (f)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)
WITH DISTINCT tag, post
WITH tag, CASE WHEN post.creationDate > 100 THEN 1 ELSE 0 END AS valid
WITH tag, sum(valid) AS postCount
RETURN tag.name, postCount
```

**Root Cause**: 
- `rewrite_expression_simple()` only rewrote column names (e.g., `creationDate` → `post_creationDate`)
- But it didn't rewrite the table alias (e.g., `post` → `with_post_tag_cte_2`)
- Result: Generated SQL had `post.post_creationDate` instead of `with_post_tag_cte_2.post_creationDate`

**Fix**: 
- Added `rewrite_expression_with_cte_alias()` function that rewrites BOTH column name AND table alias
- Added `alias_to_cte` mapping (HashMap<String, String>) from Cypher alias → CTE name
- Modified `rewrite_render_plan_expressions()` to use new function with alias mapping
- Added Case handling to both rewrite functions

**Files**: `render_plan/plan_builder.rs` (lines 3537-3733)

**Test Results**: 
```cypher
// ✅ CASE expressions now correctly reference CTE columns
MATCH (tag:Tag)<-[:HAS_TAG]-(post:Post)
WITH DISTINCT tag, post
WITH tag, CASE WHEN post.creationDate > 100 THEN 1 ELSE 0 END AS valid
RETURN tag.id, valid

// Generated SQL now shows:
// CASE WHEN with_post_tag_cte_2.post_creationDate > 100 THEN 1 ELSE 0 END AS "valid"
```

**Impact**: Fixes IC-4 pattern queries with complex expressions in WITH clauses

### ~~3. CTE Column Reference (Dot vs Underscore)~~ ✅ **FIXED** - January 12, 2026
**Was**: When referencing CTE columns from another CTE, the system used dotted names (`cte_alias."tag.url"`) instead of underscore names (`cte_alias.tag_url`), causing "Unknown expression identifier" errors.

**Root Cause**: 
- UNION subqueries output columns with dotted names as quoted aliases: `tag.url AS "tag.url"`
- The first CTE wrapping the UNION converts these to underscore aliases: `"tag_url"`
- When a subsequent CTE references these columns, it needs to use the underscore version
- But for UNION references directly, it needs the quoted dot version
- The code wasn't distinguishing between these two cases

**Fix**: 
- Added `is_union_reference` flag to distinguish UNION vs CTE column references
- UNION references: keep dotted format (`__union."tag.url"`)
- CTE references: use normalized underscore format (`with_cte.tag_url`)

**Files**: `render_plan/plan_builder.rs` (expand_table_alias_to_select_items)

### ~~4. VLP CTE Column Scoping Issue~~ ✅ **FIXED** - January 12, 2026
**Was**: Queries mixing VLP with additional relationships and aggregations failed with "Unknown expression identifier" errors because aggregate-referenced columns weren't included in UNION SELECT.

**Root Cause**: 
- When VLP patterns are followed by additional relationships with GROUP BY aggregations (e.g., `MATCH (p)-[:KNOWS*1..2]-(f)<-[:CREATOR]-(m) RETURN f.id, COUNT(DISTINCT m)`), the generated SQL creates UNION subqueries for bidirectional VLP patterns
- The UNION SELECT only included non-aggregate columns (base_select_items)
- Aliases referenced in aggregate expressions (like `m` in `COUNT(DISTINCT m)`) weren't in the SELECT list
- Outer query tried to access `m.id` for aggregation but it wasn't in scope → "Unknown expression identifier m.id"

**Fix**: 
- Added `collect_aliases_from_render_expr()` helper to recursively extract table aliases from aggregate function arguments
- Modified `try_build_join_based_plan()` to collect aliases from all aggregate expressions
- For each aggregate alias, looks up its ID column and adds to base_select_items before creating UNION branches
- Result: UNION SELECT now includes columns like `m.id` that are needed for outer aggregation
- **Critical guard**: Only modify `anchor_table` when there ARE CTE references (WITH clauses), preventing join ordering issues

**Test Results**: 
```cypher
// ✅ Core VLP + aggregation pattern
MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message)
RETURN f.id, COUNT(DISTINCT m) AS messageCount

// ✅ Multiple aggregates
MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message)
RETURN f.id, COUNT(DISTINCT m) AS msgCount, COUNT(m) AS totalCount

// ✅ Different hop ranges (all work)
MATCH (p)-[:KNOWS*1]-(f)<-[:HAS_CREATOR]-(m) RETURN f.id, COUNT(DISTINCT m)
MATCH (p)-[:KNOWS*2]-(f)<-[:HAS_CREATOR]-(m) RETURN f.id, COUNT(DISTINCT m)  
MATCH (p)-[:KNOWS*1..3]-(f)<-[:HAS_CREATOR]-(m) RETURN f.id, COUNT(DISTINCT m)
```

**Test Coverage**:
- **12 new unit tests** in `variable_length_tests.rs` (module `vlp_cte_scoping_tests`)
- **11 integration tests** in `test_vlp_aggregation.py`
- All 747 unit tests passing (100%) ✅
- SQL generation tests verify no scoping errors ✅

**Impact**: Unblocks **IC-3, IC-9, BI-2, BI-9** and other VLP + aggregation queries

### ~~5. WITH + MATCH Pattern (CartesianProduct)~~ ✅ **FIXED** - January 12, 2026
**Was**: Queries with `MATCH ... WITH ... MATCH ...` pattern failed with \"Failed to process all WITH clauses after 1 iterations. Remaining aliases: [].\"

**Root Cause**: 
- When MATCH patterns don't share aliases, query planner generates `CartesianProduct` node
- `find_all_with_clauses_impl()` didn't handle CartesianProduct, so it couldn't find WITH clauses inside
- `replace_with_clause_with_cte_reference_v2()` didn't recurse into CartesianProduct, so replacements failed
- Result: Loop detected WITH clause but couldn't process it (infinite loop, 10 iteration limit)

**Fix**: 
- Added CartesianProduct case to `find_all_with_clauses_impl` - recurses into left and right branches
- Added CartesianProduct case to `replace_with_clause_with_cte_reference_v2` - replaces WITH in both branches
- Both functions now handle disconnected MATCH patterns correctly

**Test Results**: 
```cypher
// ✅ IC-4: WITH + property filter  
MATCH (p:Person {id: 933})
WITH p.creationDate AS pcd
MATCH (p2:Person)
WHERE p2.creationDate >= pcd
RETURN p2.id

// ✅ IC-6: VLP + WITH + new MATCH
MATCH (person:Person {id: 933})-[:KNOWS*1..2]-(friend:Person)
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
RETURN friend.id, post.id
```

**Impact**: **+6 LDBC queries** now passing (IC-4, IC-6, BI-5, BI-11, BI-12, BI-19) → 15/41 total (37%)

---

### ~~5. COUNT(r) and COLLECT()~~ ✅ **NOT A BUG** - January 11, 2026
**Status**: ✅ Already working!

**Test Results**:
```cypher
// ✅ All work perfectly
MATCH (u:Person)-[r:KNOWS]->() RETURN COUNT(r)             // Works
MATCH (u:Person)-[r:KNOWS]->() RETURN COUNT(DISTINCT r)    // Works
MATCH (u)-[:FOLLOWS]->(f) RETURN COLLECT(f.name)           // Works
```

**Note**: Was listed as bug but comprehensive testing shows all patterns work correctly. Removed from active issues.

---

### ~~6. Chained WITH CTE Name Remapping~~ ✅ **FIXED** - January 11, 2026
**Was**: 3+ level chained WITHs (e.g., `WITH name WITH name WITH name`) generated SQL with incorrect CTE references, causing `Unknown expression identifier` errors.

**Root Cause**: 
- `collapse_passthrough_with()` matched passthroughs by alias only
- With multiple consecutive WITHs having same alias, it collapsed the **outermost** instead of the target
- This caused CTE name remapping to record wrong mappings
- Final SELECT referenced non-existent CTE names (e.g., `with_name_cte_5` when only `with_name_cte_3` existed)

**Fix**: 
- Modified `collapse_passthrough_with()` to accept `target_cte_name` parameter (analyzer's CTE name)
- Now matches both alias AND analyzer CTE name from `wc.cte_references`
- Ensures the exact passthrough WITH in chain is collapsed, not just any WITH with that alias

**Test Results**: 
```cypher
// ✅ 2-level
MATCH (p:Person) WITH p.firstName AS name WITH name RETURN name

// ✅ 3-level  
MATCH (p:Person) WITH p.lastName AS lnm WITH lnm WITH lnm RETURN lnm

// ✅ 4-level
MATCH (p:Person) WITH p.firstName AS fn WITH fn WITH fn WITH fn RETURN fn

// ✅ Multi-column with CASE
MATCH (p:Person) 
WITH p.firstName AS name, CASE WHEN p.gender = 'male' THEN 1 ELSE 0 END AS isMale 
WITH name, isMale 
WITH name, isMale 
RETURN name, isMale
```

**Impact**: Unlocks **IC-1, IC-2** and other complex LDBC queries with chained WITHs

---

### ~~7. Parameterized Views with Relationships~~ ✅ **FIXED** - January 9, 2026
**Was**: When both node table and edge table are parameterized views, parameters only applied to node tables, not relationship tables in VLP queries.

### ~~8. Parameterized Views with Relationships~~ ✅ **FIXED** - January 9, 2026
**Was**: When both node table and edge table are parameterized views, parameters only applied to node tables, not relationship tables in VLP queries.

**Root Cause**: 
1. `graph_rel.center` is `Empty` for inferred relationship types, causing schema-based lookup without parameterized syntax
2. Backticks from parameterized view syntax weren't stripped before schema lookup

**Fix**: 
- Created `rel_type_to_table_name_with_nodes_and_params()` for parameterized schema lookup
- Added backtick stripping to `rel_table_plain` extraction for correct column lookup
- Updated VLP code to extract view_parameter_values from node ViewScans

**Test Results**: ✅ 6/6 GraphRAG parameterized view tests pass

---

### ~~9. Array Literals Not Supported~~ ✅ **FALSE ALARM** - January 9, 2026
**Status**: ✅ Already working! Tests had bugs.

**Discovery**: Array literals `[1, 2, 3]` and function calls like `cosineDistance([0.1, 0.2], [0.3, 0.4])` work perfectly! The parser already supports `Expression::List` and generates correct SQL.

**Actual Issue**: Test file had bug checking `result.get("sql")` instead of `result.get("generated_sql")`.

**Test Results**: 9/9 vector similarity tests now pass (100%) ✅

---

## Recently Fixed

### Array Subscript Support ✅ **FIXED** - January 7, 2026
**Was**: Array subscript operations on functions and arrays were not implemented

**Now Working**:
```cypher
// ✅ Works on functions
MATCH (u:User) WHERE u.user_id = 1
RETURN labels(u)[1] as first_label  // Returns "User"

// ✅ Works on literal arrays  
RETURN [1, 2, 3][2] as second_element  // Returns 2

// ✅ Works on any expression
MATCH (u)-[:FOLLOWS|AUTHORED*1..2]->(x)
RETURN labels(x)[1] as node_type  // Returns node type
```

**Implementation**:
- Added `ArraySubscript` variant to AST hierarchy (Expression → LogicalExpr → RenderExpr)
- Modified parser to handle `[index]` syntax after any expression
- SQL generation outputs ClickHouse `array[index]` format (1-based indexing)
- Special handling for `labels()` function expansion with subscripts

**Files Modified**: `expression.rs`, `ast.rs`, `logical_expr/mod.rs`, `render_expr.rs`, `projection_tagging.rs`, `to_sql_query.rs`

---

## Current Status

**Bug Status**: ✅ **0 known bugs**  
- Integration test pass rate: **98.5%** (197/200 passing)
- Multi-type VLP test pass rate: **85%** (17/20 passing, 3 have unrelated aggregate query issue)
- All core functionality working correctly
- VLP + WITH clause path functions fixed (Dec 26, 2025)
- VLP cross-functional testing complete (Dec 25, 2025)
- Denormalized VLP fixed (Dec 25, 2025)
- Property pruning complete (Dec 24, 2025)

---

## Recently Fixed

### Path Functions in WITH Clauses (CTEs)
**Status**: ✅ **FIXED** - December 26, 2025

**Problem**: VLP queries with `length(path)` in WITH clauses generated CTEs that used VLP internal aliases (`start_node`/`end_node`) instead of Cypher aliases (`u1`/`u2`) in SELECT items.

**Root Cause**: The `rewrite_vlp_union_branch_aliases` function was incorrectly rewriting WITH CTE bodies. When checking if endpoint aliases had JOINs, it checked the *outer* plan's JOINs, but when rewriting CTE bodies, those nested RenderPlans don't have JOINs yet (they're in the outer plan). This caused it to incorrectly rewrite `u1` → `start_node`.

**Fix**: Modified `rewrite_vlp_union_branch_aliases` to only apply `t` → `vlp_alias` mapping when rewriting CTE bodies, excluding endpoint alias rewrites entirely for CTEs. WITH CTEs have their own JOINs (`JOIN users AS u1`) so SELECT items should use those Cypher aliases.

**Verification**: All VLP + WITH clause tests pass:
- `test_vlp_with_filtering` ✅
- `test_vlp_with_and_aggregation` ✅

---

## Known Limitations

**Documentation**: [docs/development/vlp-cross-functional-testing.md](docs/development/vlp-cross-functional-testing.md)

---

## Feature Limitations

The following Cypher features are **not implemented** (by design - read-only query engine):

### 1. Variable Alias Renaming in WITH Clause
**Status**: ⚠️ LIMITATION  
**Example**: `MATCH (u:User) WITH u AS person RETURN person.name`  
**Error**: `Property 'name' not found on node 'person'`  
**Root Cause**: When a variable is renamed via `WITH u AS person`, the type information (Node/Relationship/Scalar) is not propagated to the new alias. The new alias `person` doesn't have the label information needed to resolve property mappings.  
**Impact**: Blocks queries that use alias renaming patterns  
**Workaround**: Keep the same alias name: `WITH u RETURN u.name`  
**Files**: `query_planner/analyzer/filter_tagging.rs`, `typed_variable.rs`  
**Added**: January 20, 2026

### 2. Procedure Calls (APOC/GDS)
**Status**: ⚠️ NOT IMPLEMENTED (out of scope)  
**Example**: `CALL apoc.algo.pageRank(...)`  
**Reason**: ClickGraph is a SQL query translator, not a procedure runtime  
**Impact**: Blocks 4 LDBC BI queries (bi-10, bi-15, bi-19, bi-20)

### 3. Bidirectional Relationship Patterns  
**Status**: ⚠️ NOT IMPLEMENTED (non-standard syntax)  
**Example**: `(a)<-[:TYPE]->(b)` (both arrows on same relationship)  
**Workaround**: Use undirected pattern `(a)-[:TYPE]-(b)` or two MATCH clauses  
**Impact**: Blocks 1 LDBC BI query (bi-17)

### 4. Write Operations
**Status**: ❌ OUT OF SCOPE (read-only by design)  
**Not Supported**: `CREATE`, `SET`, `DELETE`, `MERGE`, `REMOVE`  
**Reason**: ClickGraph is a read-only analytical query engine for ClickHouse  
**Alternative**: Use native ClickHouse INSERT statements for data loading

---

## Test Suite Status

**Integration Tests**: ✅ **High pass rate** (549+ passed core tests)
- Core queries: **549 passed** ✅
- Security graph: **94 passed, 4 xfailed** ✅  
- Variable-length paths: **24 passed, 1 skipped, 2 xfailed** ✅
- VLP cross-functional: **6/6 passing** ✅ (Dec 25, 2025)
- Pattern comprehensions: **5 passed** ✅
- Property expressions: **28 passed, 3 xfailed** ✅
- Node uniqueness: **4 passed** ✅
- Multiple UNWIND: **7 passed** ✅
- **GraphRAG + Parameterized Views**: **6/6 passing (100%)** ✅ (Jan 9, 2026)
- **GraphRAG + Vector Similarity**: **9/9 passing (100%)** ✅ (Jan 9, 2026)

**LDBC Benchmark**: **29/41 queries passing (70%)**
- All SHORT queries pass ✅
- Remaining 12 blocked by: procedures (4), bidirectional patterns (1), other edge cases (7)

---

## Documentation

For comprehensive feature documentation and examples:
- **User Guide**: [docs/wiki/](docs/wiki/)
- **Getting Started**: [docs/getting-started.md](docs/getting-started.md)
- **Cypher Support**: [docs/features.md](docs/features.md)
- **Schema Configuration**: [docs/schema-reference.md](docs/schema-reference.md)

For developers:
- **Architecture**: [docs/architecture/](docs/architecture/)
- **Development Guide**: [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md)
- **Test Infrastructure**: [tests/README.md](tests/README.md)
- **VLP Cross-Functional Testing**: [docs/development/vlp-cross-functional-testing.md](docs/development/vlp-cross-functional-testing.md) ⭐ NEW
