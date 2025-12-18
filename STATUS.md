# ClickGraph Status

*Updated: December 22, 2025*

## 🔧 In Progress: RelationshipSchema Refactoring (December 22, 2025)

### Objective
Separate graph labels from relational table names in `RelationshipSchema` to fix VLP + WITH label corruption bug and improve code clarity.

### Changes Made
1. **Schema Structure** (✅ Complete):
   - Added `from_node_table` and `to_node_table` fields to `RelationshipSchema`
   - Separated graph concepts (labels) from relational concepts (table names)
   - Updated 45+ test constructors across 14 files

2. **Compilation** (✅ Complete):
   - Fixed 24 compilation errors
   - Updated AST field usage: `match_clause` → `match_clauses`
   - Added `cte_references` field to GraphRel constructors
   - Updated function signatures with `node_alias` parameter

3. **Multi-Relationship Query Fix** (✅ Complete):
   - Fixed `MissingTableInfo` error in `render_plan/plan_builder.rs`
   - Added fallback to lookup table names from relationship schema
   - Queries like `MATCH (c:Customer)-[:PURCHASED|PLACED_ORDER]->(target)` now work

### Test Status: 639/649 passing (98.5%)

**Passing**:
- ✅ Core RelationshipSchema refactoring
- ✅ Multi-relationship type queries
- ✅ All render_plan tests
- ✅ 634/639 query_planner tests

**Remaining Issues** (8 tests):
1. **5 graph_join_inference tests** - Database prefix inconsistency
   - Expected: `"default.FOLLOWS"` 
   - Actual: `"FOLLOWS"`
   - Cause: Test Scan nodes use unqualified names, but JOINs expect qualified names
   
2. **2 match_clause tests**:
   - `test_convert_properties_to_operator_application` - needs PropertyAccessExp expectations
   - `test_traverse_connected_pattern_disconnected_error` - assertion failure
   
3. **1 parser test**:
   - `test_parse_unary_expression_not` - unary expression parsing issue

### Next Steps
1. Fix database qualification in graph_join_inference JOIN creation
2. Update match_clause test expectations for PropertyAccessExp
3. Investigate parser test failure
4. Verify 100% test pass rate
5. Commit and document

---

## 🎉 Recent Fixes (December 18, 2025)

### ✅ CTE Database Prefix Fix for Cross-Branch JOINs

**Critical Bug**: Cross-branch JOINs added database prefixes to CTE names, causing SQL syntax errors

**Issue**: Multi-MATCH queries with WITH clauses generated invalid SQL:
```sql
WITH with_u2_cte AS (SELECT ... FROM ...)
SELECT *
FROM with_u2_cte AS friend
INNER JOIN brahmand.with_u2_cte AS friend  -- ❌ Database prefix on CTE!
```

**Root Cause**: `NodeAppearance` extraction in `graph_join_inference.rs` always used schema database/table names, even when:
1. GraphRel was wrapped in a CTE (for alternate relationships)
2. Multi-variant relationships created UNION CTEs

**Fix (commits 9ae3bc7, ddc7fe7)**: Two-location fix:
1. **graph_context.rs** (lines 206-226): Multi-variant CTE detection
   - Check if `labels.len() > 1` → use `rel_{left}_{right}` format
   - Matches CTE names from `cte_extraction.rs`
   
2. **graph_traversal_planning.rs** (lines 678-696): Consistent CTE wrapping
   - Multi-variant: use `rel_{left}_{right}` format
   - Single: keep `{label}_{alias}` format (backward compatible)

3. **graph_join_inference.rs** (lines 3507-3586): Database prefix removal
   - Detect CTE-wrapped GraphRel → use CTE name, empty database
   - Detect multi-variant labels → construct CTE name, empty database  
   - Conditional prefix: only add database if not empty

**Generated SQL After Fix**:
```sql
rel_friend_city AS (
  SELECT PersonId as from_node_id, CityId as to_node_id FROM Person_isLocatedIn_Place
  UNION ALL ...
)
SELECT *
FROM with_u2_cte AS friend
INNER JOIN rel_friend_city AS t3  -- ✅ No database prefix on CTEs!
```

**Impact**:
- ✅ Multi-variant relationships (IS_LOCATED_IN, HAS_TAG) now work correctly
- ✅ Cross-branch JOINs use correct CTE names without database prefixes
- ⚠️ Known issue: WITH clause queries still fail (node label preservation bug - separate fix needed)

### ✅ VLP Alias Mapping Fix for Undirected ShortestPath

**Critical Bug**: Simple undirected shortestPath queries failed with "Unknown expression identifier"

**Issue**: Query `MATCH path = shortestPath((a)-[:KNOWS*]-(b)) RETURN a.id, b.id` generated:
```sql
SELECT a.id AS "a.id", b.id AS "b.id"  -- ❌ a, b don't exist!
FROM vlp_cte1 AS vlp1
JOIN ldbc.Person AS start_node ON vlp1.start_id = start_node.id
JOIN ldbc.Person AS end_node ON vlp1.end_id = end_node.id
```

**Root Cause**: BidirectionalUnion creates Union branches before CTE extraction. Each branch uses Cypher aliases (`a`, `b`) but VLP CTEs use SQL aliases (`start_node`, `end_node`). SELECT items reference non-existent aliases.

**Fix**: Modified `src/render_plan/plan_builder.rs`:
- Added `rewrite_vlp_union_branch_aliases()` to extract VLP metadata and rewrite SELECT aliases
- Called from `try_build_join_based_plan()` after Union branches render (lines 7748-7764)
- Three helper functions (lines 267-364):
  1. `rewrite_vlp_union_branch_aliases()`: Main entry point
  2. `extract_vlp_alias_mappings()`: Extract Cypher→VLP mappings from CTE metadata
  3. `rewrite_render_expr_for_vlp()`: Recursively rewrite PropertyAccessExp

**Extended Fix** (Dec 17, 2025 - commit 7041692):
- Extended rewriting to WHERE clause expressions (not just SELECT)
- IC1 queries with filters now work: `WHERE friend.firstName = 'Wei'` → `WHERE end_node.firstName = 'Wei'`
- Tested with LDBC database: `MATCH (p:Person {id: 14})-[:KNOWS*1..2]-(friend) WHERE friend.id <> 14` - PASS

**Generated SQL After Fix**:
```sql
SELECT start_node.id AS "a.id", end_node.id AS "b.id"  -- ✅ Correct aliases!
WHERE end_node.firstName = 'Wei'  -- ✅ WHERE clause also rewritten!
```

**Impact**:
- ✅ Simple undirected shortestPath queries now generate valid SQL
- ✅ All Union branches with VLP CTEs properly rewritten (SELECT + WHERE)
- ✅ Enables LDBC IC1 query execution with property filters

### ✅ ShortestPath CTE Wrapping Fix

**Critical Bug**: Duplicate CTE declarations in nested WITH RECURSIVE blocks

**Issue**: Queries with multiple shortestPath patterns generated duplicate CTE names
```sql
vlp_cte2 AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_cte2_inner AS (...),
    vlp_cte2 AS (...)  -- ❌ DUPLICATE!
```

**Root Cause**: VLP CTEs with shortest path generate multi-tier structures (`vlp_inner`, `vlp_to_target`, `vlp`) stored as single `RawSql`. Wrapping logic for 2nd+ recursive CTEs created duplicate declarations.

**Fix**: Modified `src/clickhouse_query_generator/to_sql_query.rs` lines 374-412:
- Detect nested CTE structures (multiple ` AS (` patterns)
- Use raw content directly instead of adding another wrapper
- Prevents duplicate CTE names in nested blocks

**Impact**:
- ✅ IC1 query SQL generation now works (no duplicates)
- ✅ Complex multi-WITH shortestPath queries work
- ✅ All CTE nesting properly structured

**Remaining**: Simple undirected shortestPath queries have alias mapping issue (see KNOWN_ISSUES.md #6)

### ✅ FilterIntoGraphRel Duplicate WHERE Fix

**Critical Bug**: CartesianProduct queries generated duplicate WHERE conditions

**Issue**: Query `MATCH (p:Person {id: X}), (friend:Person) RETURN ...` generated:
```sql
WHERE (p.id = X AND p.id = X)  -- ❌ DUPLICATE!
```

**Root Cause**: FilterIntoGraphRel optimizer matched by table name instead of alias:
- GraphNode('p') had filter `p.id = X` ✓
- GraphNode('friend') ALSO received filter `p.id = X` ❌
- ViewScan handler iterated ALL aliases, injected FIRST filter found

**Fix**: Modified `src/query_planner/optimizer/filter_into_graph_rel.rs`:
- GraphNode handler: Inject filters ONLY for matching alias
- ViewScan handler: Removed filter injection entirely
- Filters now correctly scoped to their specific nodes

**Impact**: All comma patterns with property filters now generate correct SQL

---

## 🎉 Recent Fixes (December 16, 2025)

**Critical Parser Bug**: All binary operators parsed at same precedence level

**Issue**: Expressions like `m.id > 1 + 2` parsed as `(m.id > 1) AND 2`
- Parser treated all binary operators (arithmetic, comparison, logical) equally
- Left-to-right parsing caused incorrect grouping
- Temporal arithmetic broken: `datetime("2011-01-01") + duration({hours: 4})` became `datetime(...) AND duration(...)`

**Fix**: Implemented correct operator precedence hierarchy in `/src/open_cypher_parser/expression.rs`
1. Multiplicative (`*`, `/`, `%`) - highest
2. Additive (`+`, `-`)
3. Comparison (`=`, `<>`, `<`, `>`, `<=`, `>=`, `IN`, `CONTAINS`, etc.)
4. Logical AND
5. Logical OR - lowest

**Impact**: 
- LDBC BI17 now works: temporal arithmetic `datetime() + duration()` generates correct SQL
- All arithmetic expressions in WHERE clauses now parse correctly
- Example: `WHERE m.id > 1 + 2` → `WHERE m.id > 1 + 2` (not `WHERE m.id > 1 AND 2`)

---

## 🎉 Recent Fixes (December 16, 2025)

### ✅ WITH Clause CartesianProduct JOIN Fix

**Pattern**: `MATCH (a), (b) WHERE a.id < b.id WITH a, b, computed_value AS alias RETURN ...`

**Issue**: Missing JOIN ON clause when CartesianProduct is inside WithClause
- Generated: `FROM a INNER JOIN b` (no ON clause)
- Also: Computed columns missing table prefix in SELECT

**Fix**: Special handling in `CartesianJoinExtraction` for `Filter(WithClause(CartesianProduct))`
- Now generates: `FROM a INNER JOIN b ON a.id < b.id`
- Computed columns properly referenced: `cte_alias.computed_value`

**Impact**: LDBC BI-14 base pattern now works, enables comma patterns with WITH clauses

## 🐛 **Correlated Subquery Fix Complete** - December 16, 2025

**ClickHouse correlated subquery compatibility fixed! Anti-join patterns now work.**

### Correlated Subquery Handling (Dec 16, 2025)

- **Anti-Join & Existence Patterns** - Automatic detection and correct placement ✅
  - **Pattern**: `MATCH (a), (b) WHERE a.id < b.id AND NOT (a)-[:REL]-(b)`
  - **Problem**: Correlated subqueries (`NOT EXISTS`, `EXISTS`, `size()`) were placed in JOIN ON clauses
  - **Error**: "Code: 48. DB::Exception: Correlated subqueries in join expression are not supported (NOT_IMPLEMENTED)"
  - **Solution**: 
    - Detect correlated subquery predicates: `NOT (pattern)`, `EXISTS((pattern))`, `size((pattern))`
    - Keep correlated subqueries in WHERE clause
    - Move simple join conditions to JOIN ON clause
  - **Use Case**: LDBC BI-18 Friend Recommendation (mutual friend anti-join)
  - **Example**:
    ```cypher
    MATCH (p1:Person), (p2:Person) 
    WHERE p1.id < p2.id AND NOT (p1)-[:KNOWS]-(p2)
    RETURN p1.id, p2.id
    ```
    Generates correct SQL:
    ```sql
    FROM Person AS p1
    INNER JOIN Person AS p2 ON p1.id < p2.id  -- ✅ Simple condition in JOIN
    WHERE NOT EXISTS (...)                     -- ✅ Correlated subquery in WHERE
    ```
  - **Files Modified**:
    - `src/query_planner/logical_expr/mod.rs`: Added `contains_not_path_pattern()` helper
    - `src/query_planner/optimizer/cartesian_join_extraction.rs`: AND-splitting logic
    - `src/query_planner/analyzer/graph_join_inference.rs`: Skip JOIN creation for correlated predicates
    - `src/render_plan/plan_builder.rs`: Fixed CartesianProduct JOIN rendering

## 🎉 **Cross-Branch JOIN Detection Complete** - December 15, 2025

**Cross-table correlation queries now work! Solves GitHub issue #12**

### Cross-Branch Shared Node JOIN Detection (Dec 15, 2025)

- **Comma-Pattern Cross-Table Queries** - Automatic JOIN generation for branching patterns ✅
  - **Pattern**: `MATCH (n)-[:REL1]->(a), (n)-[:REL2]->(b)` where REL1 and REL2 are in different tables
  - **Problem**: When shared node `n` appears in branches that reference different tables, no JOIN was generated
  - **Example**: DNS lookup followed by connection (GitHub issue #12)
    ```cypher
    MATCH (src:IP)-[:REQUESTED]->(d:Domain), (src)-[:ACCESSED]->(dest:IP)
    RETURN src.ip, d.name, dest.ip
    ```
    - [:REQUESTED] uses `dns_log` table
    - [:ACCESSED] uses `conn_log` table
    - Shared node `src` should trigger JOIN: `FROM conn_log JOIN dns_log ON conn_log.orig_h = dns_log.orig_h`
  
  - **Solution**: Implemented cross-branch shared node detection
    - Track node appearances across GraphRel branches using `HashMap<String, Vec<NodeAppearance>>`
    - When node appears in multiple GraphRels with different tables, generate INNER JOIN
    - Skip JOIN when both GraphRels use same table (coupled edges)
  - **Implementation**:
    - `src/query_planner/analyzer/graph_join_inference.rs`:
      - Added `NodeAppearance` struct: tracks rel_alias, node_label, table_name, database, column_name
      - Added `check_and_generate_cross_branch_joins()`: checks left_connection and right_connection nodes
      - Added `extract_node_appearance()`: extracts node info from GraphRel
      - Added `generate_cross_branch_join()`: creates JOIN with same-table check
    - Updated `deduplicate_joins()`: use (alias, condition) as key instead of just alias
  - **Key Insight**: Deduplication was dropping valid JOINs!
    - Old: HashMap key = table_alias only → dropped second JOIN to same table
    - New: HashMap key = (table_alias, join_condition) → allows multiple JOINs to same table with different conditions
  - **Test Results**: 23/24 Zeek tests passing (95.8%), including 5/5 cross-table correlation tests ✅
    - ✅ `test_comma_pattern_cross_table`: Simple 2-hop branching
    - ✅ `test_comma_pattern_full_dns_path`: 3-hop branching with coupled edges
    - ✅ `test_dns_then_connect_to_resolved_ip`: Full DNS→connection correlation
    - ✅ `test_predicate_correlation`: Predicate-based correlation (srcip1.ip = srcip2.ip)
    - ✅ `test_sequential_match_same_node`: Multiple MATCH clauses
    - ⏳ 1 skipped: WITH...MATCH (future work)
  - Generated SQL example:
    ```sql
    SELECT t1.orig_h AS "src.ip", t1.query AS "d.name", t3.resp_h AS "dest.ip"
    FROM test_zeek.conn_log AS t3
    INNER JOIN test_zeek.dns_log AS t1 ON t3.orig_h = t1.orig_h
    WHERE t1.orig_h = '192.168.1.10'
    ```

- **Predicate-Based Correlation** - Allow disconnected patterns with WHERE clause predicates ✅
  - **Pattern**: `MATCH (n1)-[:R1]->(a), (n2)-[:R2]->(b) WHERE n1.prop = n2.prop` (different variable names)
  - **Solution**: Removed DisconnectedPatternFound error, allow CartesianProduct creation
  - **Example**:
    ```cypher
    MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), (srcip2:IP)-[:ACCESSED]->(dest:IP)
    WHERE srcip1.ip = srcip2.ip
    RETURN srcip1.ip, d.name, dest.ip
    ```
  - **Generated SQL**: Same INNER JOIN as shared variable pattern
  - **Impact**: Enables flexible variable naming in cross-table queries

- **Sequential MATCH Clauses** - Multiple MATCH statements in sequence ✅
  - **Pattern**: `MATCH ... MATCH ... MATCH ...` (no WITH between them)
  - **Semantics**: Each MATCH builds on previous context, no relationship uniqueness across MATCH boundaries
  - **Example**:
    ```cypher
    MATCH (srcip:IP)-[:REQUESTED]->(d:Domain)
    MATCH (srcip)-[:ACCESSED]->(dest:IP)
    WHERE srcip.ip = '192.168.1.10'
    RETURN srcip.ip, d.name, dest.ip
    ```
  - **Implementation**: Parser changed from `match_clause: Option` to `match_clauses: Vec`
  - **Key Difference**: Comma patterns require relationship uniqueness within single MATCH, sequential MATCHes don't
  - **Files**: `src/open_cypher_parser/ast.rs`, `src/open_cypher_parser/mod.rs`, `src/query_planner/logical_plan/plan_builder.rs`

## 🎉 **v0.5.5 Released** - December 10, 2025

**LDBC SNB Benchmark: 29 queries work as-is, 5 with workarounds (34/36 non-blocked = 94%)**

### Test Suite Solidification (Dec 14, 2025)

- **Test Infrastructure Improvements** - Comprehensive fixes → 76.3% pass rate ✅
  - **Before**: 1921/3467 passing (55.2%) - infrastructure broken
  - **After**: 2643/3467 passing (76.3%) - infrastructure solid
  - **Critical Bugs Fixed**:
    - ✅ CLICKGRAPH_URL import missing in test_comprehensive.py (fixed ~400 matrix test failures)
    - ✅ ontime_benchmark schema path incorrect (fixed path to `benchmarks/ontime_flights/schemas/`)
    - ✅ Renamed 12 standalone scripts to `script_test_*.py` (excluded from pytest collection)
    - ✅ Added autouse fixtures for schema loading
  - **Test Data Loaded**:
    - Ran official setup scripts: `setup_all_test_data.sh`, `load_test_schemas.py`
    - Loaded security_graph data from `schemas/examples/security_graph_load.sql`
  - Results by category:
    - **Rust unit tests**: 647/647 (100%)
    - **Security graph**: 91/98 (92.9%)
    - **Core integration** (with data): 452/502 (90%)
    - **Matrix tests**: High quality when data exists
  - Remaining failures: ~95% missing test data, ~5% code bugs
  - **Known Code Bugs** (affecting ~15-20 tests):
    - Multiple independent recursive CTEs in single WITH RECURSIVE clause (affects bidirectional shortest path)
    - Table prefix missing in JOINs within CTEs (affects aggregations with HAVING)
      - Root cause: Join struct doesn't carry database context
      - Workaround needed: Pass database through Join or infer from FROM clause
  - **Key lesson: Always follow README setup instructions first!**
  - See: `TEST_SUITE_STATUS_Dec14_2025.md` for detailed analysis

### Recent Fixes (Dec 14, 2025)

- **Coupled Edge Alias Resolution** - Fixed SQL generation for patterns with multiple edges in same table ✅
  - Problem: `MATCH (src:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)` failed with "Unknown expression identifier"
  - Both REQUESTED and RESOLVED_TO edges use same dns_log table (coupled edges)
  - SQL generated: SELECT used unified alias `t1` but FROM used original alias `t2`
  - Error: "Unknown expression identifier 't1.orig_h' ... Maybe you meant: ['t2.orig_h']"
  - Root cause: `AliasResolverContext.transform_plan()` transformed property access expressions to use unified alias, but didn't transform the GraphRel alias itself
  - Solution: Updated `transform_plan()` to also rewrite GraphRel alias when it appears in `coupled_edge_aliases` HashMap
  - Files: `src/render_plan/alias_resolver.rs` (transform_plan GraphRel case, lines ~150-172)
  - Impact: Zeek tests: 16→18 passing (fixed both coupled DNS path tests)
  - SQL before: `SELECT t1.orig_h FROM test_zeek.dns_log AS t2 WHERE t1.orig_h = ...` ❌
  - SQL after: `SELECT t1.orig_h FROM test_zeek.dns_log AS t1 WHERE t1.orig_h = ...` ✅

- **Multi-Table Node Schema Resolution** - Fixed composite key lookup for same label across tables ✅
  - Problem: `MATCH (s:IP)-[:REQUESTED]->(d:Domain)` used wrong IP schema (conn_log instead of dns_log)
  - Root cause: Schema has TWO `IP` definitions (dns_log and conn_log tables), but `get_node_schema_opt` only used label
  - Solution: Use composite key `"database::table::label"` for table-specific lookup, fallback to label-only
  - Impact: Zeek tests: 17 → 18 passing (fully denormalized patterns now work without unnecessary JOINs)
  - Files: `src/query_planner/analyzer/graph_join_inference.rs` (compute_pattern_context)
  - Schema loader already stored with composite keys, just needed to use them!

- **Denormalized Node ID Property Mapping** - Fixed JOIN conditions for composite node IDs ✅
  - Problem: `MATCH (src:IP)-[:REQUESTED]->(d:Domain)` generated invalid SQL `ON src.ip = r.orig_h`
  - Error: "Identifier 'src.ip' cannot be resolved from table src"
  - Root cause: For denormalized edges, node_id uses Cypher property names ("ip") but JOIN needs DB columns ("orig_h")
  - Property mapping in `from_properties`/`to_properties`, not `property_mappings`
  - Solution: Updated `resolve_id_column()` to check from_properties/to_properties first (with is_from_node flag)
  - Files: `graph_catalog/pattern_schema.rs` (resolve_id_column method + 4 call sites)
  - Impact: Zeek merged schema tests: 15→17 passing (fixed 2 composite ID test failures)
  - Generated SQL: Now correctly uses `ON src.orig_h = r.orig_h`
  - See: Zeek schema uses node_id: ip with from_node_properties: {ip: "id.orig_h"}

- **Inline Property Parameters** - Fixed server crash on parameterized property patterns ✅
  - Problem: `MATCH (n:Person {id: $personId})` caused panic "Property value must be a literal"
  - Root cause: PropertyKVPair.value typed as Literal (didn't support parameters)
  - Solution: Changed PropertyKVPair.value from Literal to LogicalExpr
  - Impact: Official LDBC queries can now use inline parameters (previously required WHERE clause workaround)
  - Files: `query_planner/logical_expr/mod.rs`, `query_planner/logical_plan/match_clause.rs`
  - Tests: 647/647 unit tests passing (no regressions)
  - LDBC: All adapted queries still work, official queries now accessible
  - See: Parameter substitution in server layer handles $param → value replacement

### Recent Fixes (Dec 13, 2025)

- **Lambda Expressions for ClickHouse Functions** - Full support for inline functions ✅
  - Syntax: `ch.arrayFilter(x -> x > 5, array)` or `ch.arrayMap((x,y) -> x+y, arr1, arr2)`
  - Enables all ClickHouse higher-order array functions (arrayFilter, arrayMap, arrayExists, etc.)
  - Lambda parameters treated as local variables (not resolved to table aliases)
  - Dotted function names supported (`ch.*`, `chagg.*`)
  - Implementation: Parser → Logical → Render with proper scoping
  - Tests: 645/645 unit tests passing (including 3 new lambda tests)
  - Example: `MATCH (u:User) RETURN ch.arrayFilter(x -> x > 90, u.scores) AS high_scores`
  - See: `notes/lambda-expressions.md` for complete documentation

- **4-Level WITH Duplicate CTE Bug** - Fixed duplicate CTE generation in multi-level WITH queries ✅
  - Problem: Same CTE (e.g., `with_b_c_cte`) appeared twice in WITH clause declarations
  - Root cause: CTE deduplication checked processed aliases, but same alias could appear in multiple plan nodes
  - Solution: Check if CTE already exists in `all_ctes` by name before creating, still replace WITH clauses
  - Files: `render_plan/plan_builder.rs` (lines 963-982)
  - Impact: 4+ level WITH queries now generate valid SQL without duplicate CTEs
  - Example: `WITH a ... WITH a,b ... WITH b,c ... WITH c,d` now works correctly

- **WITH + WHERE after aggregation → HAVING clause** - Critical bug fix ✅
  - Problem: WHERE clause after WITH with aggregation was completely missing from SQL
  - Should generate: `GROUP BY ... HAVING cnt > 2` but generated: `GROUP BY ...` (no HAVING)
  - Solution: Extract `where_clause` from WithClause and emit as HAVING when GROUP BY present
  - Files: `render_plan/plan_builder.rs` (WHERE→HAVING conversion logic added)
  - Impact: Enables filtering aggregated results (TOP-N, threshold queries)
  - OpenCypher compliance: WHERE-after-WITH-with-aggregation semantics
  - Example: `WITH a, COUNT(b) as cnt WHERE cnt > 2 RETURN a, cnt` now works
  - See: `notes/with-where-having-fix.md` for details

### Major Code Cleanup (Dec 12, 2025)
- **Removed V1 Graph Pattern Handler** - Eliminated 1,568 lines of deprecated code ✅
  - Deleted entire v1 `handle_graph_pattern()` function (1,554 lines)
  - Removed v1 fallback path - v2 now the only implementation
  - File size: `graph_join_inference.rs` reduced from 5,778 → 4,210 lines (27% reduction!)
  - Fixed test infrastructure for v2 compatibility
  - Result: **642/642 tests passing (100%)**
  - See: commit 2bf1bee "refactor: Remove deprecated v1 graph pattern handler"

### Recent Improvements (Dec 11, 2025)
- **WITH Handler Refactoring** - Eliminated ~120 lines of duplicate code ✅
  - Created 3 helper functions for TableAlias/wildcard expansion
  - Refactored 2 of 3 WITH handlers to use shared helpers
  - All 8/8 LDBC queries still passing after refactoring
  - Improved maintainability: Single source of truth for expansion logic
  - See: `notes/with-clause-refactoring-dec2025.md` for details

### Recent Fixes (Dec 11, 2025)
- **WITH TableAlias Aggregation** - Fixed IC-1: `WITH friend, count(*) AS cnt` now works ✅
  - Problem: TableAlias in WITH+aggregation only expanded to ID column
  - Solution: Expand to ALL columns (WITH friend = all properties of friend)
  - Impact: CTE now includes all properties, enabling outer SELECT access
  - Changed: build_chained_with_match_cte_plan() uses flat_map for one-to-many expansion
  - GROUP BY: Now includes all non-aggregated columns (ClickHouse requirement)
  - Benchmark: IC-1 query passes in 37.5ms, 8/8 queries (100%)

### Recent Features (Dec 11, 2025)
- **Composite Node IDs** - Multi-column primary key support for nodes ✅
  - Syntax: `node_id: [bank_id, account_number]` in YAML
  - Generates: `(a.bank_id, a.account_number) = (b.bank_id, b.account_number)` in JOINs
  - Use cases: Banking (multi-bank accounts), multi-tenant (tenant_id + user_id), distributed systems
  - Works with: MATCH, size(), EXISTS, NOT EXISTS, PageRank
  - Infrastructure: New `sql_tuple()` and `sql_equality()` methods on NodeIdSchema
  - Testing: 5 new unit tests, 644 total tests passing (100%)

### Recent Fixes (Dec 11, 2025)
- **size() on Patterns** - Implemented pattern counting with correlated subqueries ✅
  - Syntax: `size((n)-[:REL]->())` generates `(SELECT COUNT(*) FROM rel_table WHERE ...)`
  - Schema-aware: Correctly infers node ID columns from relationship schema
  - Supports: outgoing/incoming/undirected relationships, anonymous end nodes
  - Unlocks: LDBC BI-8, IC-10 queries (pattern comprehension counting)
- **WITH ORDER BY/SKIP/LIMIT** - Full support for modifiers after WITH items
  - Parser now correctly associates ORDER BY, SKIP, LIMIT, WHERE with WITH clause
  - CTE rendering applies modifiers to UNION plans with subquery wrapper
  - Fixed SKIP-only queries (without LIMIT requirement)
  - Updated test cases for new WHERE parsing (now part of WITH clause)

### Recent Fixes (Dec 10, 2025)
- **Undirected VLP with WITH clause** - Fixed CTE hoisting for bidirectional patterns
- **LDBC schema column names** - Corrected all relationship column mappings
- **Added POST_LOCATED_IN relationship** - For IC-3 benchmark query

### Recent Fixes (Dec 9, 2025)
- **Two-level aggregation (WITH + RETURN)** - Fixed bi-12: CTE structure for nested GroupBy patterns
- **OPTIONAL MATCH anchor detection** - Non-optional nodes correctly identified as anchors
- **Multi-hop pattern join ordering** - Fixed bi-18: proper `joined_entities` tracking
- **Undirected relationship join ordering** - Fixed UNION branch generation in `bidirectional_union.rs`
- **NOT pattern (anti-join)** - Implemented `NOT EXISTS` SQL generation for negative path patterns
- **621 unit tests passing** (100%)

### All LDBC BI Queries Now Passing ✅

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for remaining issues (3 active).

---

## 🆕 **v0.5.4 Released** - December 7, 2025

**This is a major release with 20+ new features and bug fixes!**

### Highlights
- **1,378 tests passing** (596 unit + 782 integration)
- **Cross-table query support** - Zeek log correlation and multi-table JOINs
- **Smart type inference** - Automatic node and relationship type inference
- **FK-Edge patterns** - File systems, org charts with variable-length paths
- **Unified schema abstraction** - New PatternSchemaContext for cleaner code
- **Data Security example** - Complete access control graph analysis
- **OnTime Flights benchmark** - 20M row real-world denormalized edge testing

See [CHANGELOG.md](CHANGELOG.md) for full release notes.

---

## 🆕 **Recent Updates** - December 7, 2025

### ✅ Unified Schema Abstraction (Phase 4 Complete)
**New `PatternSchemaContext` provides clean, exhaustive schema pattern handling!**

- **Problem**: 4,800+ lines in `graph_join_inference.rs` with scattered detection functions and nested conditionals for different schema types
- **Solution**: Created unified abstraction that computes schema context ONCE per pattern

- **New Types** (in `src/graph_catalog/pattern_schema.rs`):
  ```rust
  NodeAccessStrategy: OwnTable | EmbeddedInEdge | Virtual
  EdgeAccessStrategy: SeparateTable | Polymorphic | FkEdge
  JoinStrategy: Traditional | SingleTableScan | MixedAccess | EdgeToEdge | CoupledSameRow | FkEdgeJoin
  ```

- **Runtime Toggle**: `USE_PATTERN_SCHEMA_V2=1` enables v2 code path for A/B testing
- **All 588 tests pass** with both v1 and v2 paths
- **Identical SQL output** between v1 and v2 for Traditional pattern

- **Progress**:
  - ✅ Phase 1: PatternSchemaContext types created
  - ✅ Phase 2: Integration helpers
  - ✅ Phase 3: handle_graph_pattern_v2() with exhaustive matching
  - ✅ Phase 3.5: FkEdgeJoin strategy for FK-edge patterns
  - ✅ Phase 4: Wire up v2 with env toggle - tested and working
  - ⏳ Phase 5: Test more schema variations, make v2 default

- **Benefits**:
  - Exhaustive `match` prevents "forgot this case" bugs
  - Single point of schema analysis instead of scattered checks
  - Clear intent, clean code structure
  - Easy to add new schema types (add enum variant, compiler shows all places to update)

- **Files**: `src/graph_catalog/pattern_schema.rs`, `src/query_planner/analyzer/graph_join_inference.rs`
- **Documentation**: `notes/unified-schema-abstraction-proposal.md`

---

### ✅ Single-Node Patterns for Denormalized Schemas Fixed
**Multi-table UNION for nodes appearing in multiple tables now supported!**

- **Problem**: For denormalized schemas where the same label (e.g., IP) appears in multiple tables and positions, standalone `MATCH (ip:IP)` only generated a single ViewScan instead of querying all sources
- **Root Causes**: 
  1. `try_generate_view_scan()` only checked first table for label
  2. `count(node_alias)` generated invalid SQL because inner UNION didn't include ID column
- **Fixes**:
  1. In `match_clause.rs`: Use `ProcessedNodeMetadata.id_sources` to enumerate ALL tables/positions for a label and generate UNION ALL
  2. In `projection_tagging.rs`: Expand `count(node)` to `count(node.id_property)` using schema's `id_column` - matches Neo4j behavior
  3. In `return_clause.rs`: Detect `TableAlias` in aggregate args and include node's ID property in inner UNION projection

- **Working Patterns**:
  ```cypher
  -- All IPs from both tables (generates 3-branch UNION)
  MATCH (ip:IP) RETURN count(ip), count(distinct ip)
  
  -- Explicit property works the same
  MATCH (ip:IP) RETURN count(ip), count(distinct ip.ip)
  
  -- Constrained by relationship (uses specific table)
  MATCH (ip:IP)-[:DNS_REQUESTED]-() RETURN count(ip), count(distinct ip.ip)
  MATCH ()-[:CONNECTED_TO]->(ip:IP) RETURN count(ip), count(distinct ip.ip)
  ```

- **Generated SQL for `MATCH (ip:IP) RETURN count(ip), count(distinct ip)`**:
  ```sql
  SELECT count(ip.ip), count(DISTINCT ip.ip)
  FROM (
      SELECT ip."id.orig_h" AS "ip.ip" FROM zeek.dns_log AS ip
      UNION ALL 
      SELECT ip."id.orig_h" AS "ip.ip" FROM zeek.conn_log AS ip
      UNION ALL 
      SELECT ip."id.resp_h" AS "ip.ip" FROM zeek.conn_log AS ip
  ) AS __union
  ```

- **Neo4j Compatibility**: `count(DISTINCT node)` now counts distinct node identities (via ID column), matching Neo4j's behavior

---

## 🆕 **Recent Updates** - December 5, 2025

### ✅ Cross-Table Query Support - FULLY WORKING (Issue #12)
**Disconnected Patterns with Fully Denormalized Edges Now Supported!**

- **Problem**: Cross-table queries with WITH...MATCH pattern failed with "No FROM clause found"
- **Root Cause**: `build_graph_joins` Projection case didn't recursively process children before wrapping with GraphJoins, so CartesianProduct was never visited
- **Fix**: Three key changes:
  1. Modified Projection case in `build_graph_joins` to recursively process input first
  2. Added cross-table JOIN creation in CartesianProduct handling when both sides are fully denormalized
  3. Added `extract_right_table_from_plan` helper to extract table info from CartesianProduct's right side

- **Working Pattern (Form 1 - Disconnected with WHERE clause)**:
  ```cypher
  MATCH (ip1:IP)-[:DNS_REQUESTED]->(d:Domain) 
  WITH ip1, d 
  MATCH (ip2:IP)-[:CONNECTED_TO]->(dest:IP) 
  WHERE ip1.ip = ip2.ip 
  RETURN ip1.ip, d.name, dest.ip LIMIT 5
  ```
- **Generated SQL** (correct):
  ```sql
  SELECT ip1."id.orig_h" AS "ip1.ip", ip1.query AS "d.name", ip2."id.resp_h" AS "dest.ip"
  FROM zeek.dns_log AS ip1
  INNER JOIN zeek.conn_log AS ip2 ON ip1."id.orig_h" = ip2."id.orig_h"
  LIMIT 5
  ```

- **Working Pattern (Form 3 - Shared Variables)**:
  ```cypher
  MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain), (src)-[:CONNECTED_TO]->(dest:IP)
  RETURN src.ip AS source, d.name AS domain, dest.ip AS dest_ip
  ```
  
- **Key Technical Details**:
  - `CartesianJoinExtraction` optimizer moves WHERE filter → CartesianProduct.join_condition
  - `GraphJoinInference.build_graph_joins` now:
    - Recursively processes Projection children
    - Creates cross-table JOIN when both CartesianProduct sides are fully denormalized
  - Property resolution correctly uses `from_node_properties`/`to_node_properties` from edges

- **Files Modified**:
  - `src/query_planner/analyzer/graph_join_inference.rs` - Projection recursion + CartesianProduct JOIN
  - `src/query_planner/analyzer/filter_tagging.rs` - CartesianProduct property resolution

### Previous Fixes (Earlier December 5)

### ✅ Multi-Hop Patterns with Anonymous Nodes Fixed (Issue #6)
- **Problem**: `MATCH ()-[r1:FLIGHT]->()-[r2:FLIGHT]->()` only generated SQL for one relationship
- **Root Cause**: Parser correctly shares middle node via `Rc::clone()`, but alias generation called `generate_id()` unconditionally for each pattern, creating different aliases for the same node
- **Fix**: Added pre-processing pass in `traverse_connected_pattern_with_mode()`:
  1. Collect all node patterns and assign aliases using pointer-based identity (`as_ptr() as usize`)
  2. Shared nodes (same `Rc<RefCell<NodePattern>>`) get the same alias
- **Result**: Multi-hop anonymous patterns now generate correct SQL with proper JOINs
- **Files**: `src/query_planner/logical_plan/match_clause.rs`

### ✅ OPTIONAL MATCH with Polymorphic Edges Fixed (Issue #3)
- **Problem**: `MATCH (g:Group) OPTIONAL MATCH (g)<-[:MEMBER_OF]-(member:User)` generated invalid SQL
- **Root Cause**: Two issues in `graph_join_inference.rs`:
  1. Anchor detection logic was only in same-type nodes path, missing for different-type nodes
  2. Hardcoded `"to_id"` column name instead of using schema's actual column (`group_id`)
- **Fix**: 
  1. Unified anchor detection at start of `handle_graph_pattern()` 
  2. Use `rel_schema.to_id` instead of hardcoded string
- **Result**: Polymorphic OPTIONAL MATCH now generates correct LEFT JOINs
- **Files**: `src/query_planner/analyzer/graph_join_inference.rs`

---

## 🆕 **Recent Updates** - December 4, 2025

### ✅ Smart Inference System (NEW)
**Relationship Type Inference from Typed Nodes**:
- Query: `(a:Airport)-[r]->()` → infers `r:FLIGHT` if FLIGHT is the only edge from Airport
- Query: `()-[r]->(p:Post)` → infers `r:LIKES` if LIKES is the only edge to Post
- Query: `(u:User)-[r]->(p:Post)` → infers `r:LIKES` from both node types

**Single-Relationship-Schema Inference**:
- Query: `()-[r]->()` → infers r:KNOWS if schema has only one relationship defined
- Great for simple schemas with a single edge type

**Single-Node-Schema Inference** (NEW):
- Query: `MATCH (n) RETURN n` → infers n:User if schema has only one node type defined
- Great for simple schemas with a single node type

**Safety Limit**: Max 4 types can be inferred; more requires explicit type specification

**Unit Tests**: 19 tests for inference scenarios (577 total)
- Covers standard, denormalized, and polymorphic schema variations

### ✅ Label Inference from Relationship Schema
- **Feature**: Unlabeled nodes connected to typed relationships now infer labels from schema
- **Example**: `MATCH (f:Folder)-[:CONTAINS]->(child)` now works on polymorphic schemas
- **How it works**:
  - Query planner looks up the relationship schema
  - Gets `from_label_values`/`to_label_values` based on node position
  - Single type → use as inferred label
  - Multiple types → use first type (warning logged)
- **Issue #5 Fixed**: Polymorphic CONTAINS with untyped target now generates valid SQL
- **Files**: `src/query_planner/logical_plan/match_clause.rs`

### ✅ WITH + Node Reference + Aggregate Fixed
- **Issue #4**: `WITH g, COUNT(u) AS cnt WHERE cnt >= 2 RETURN g.name, cnt` was broken
- **Fix**: Outer query FROM clause now uses correct table for grouping key alias
- **Files**: `src/render_plan/plan_builder_helpers.rs` - made `find_table_name_for_alias()` exhaustive

### ✅ Test Infrastructure Improvements
- **Query Pattern Test Matrix**: Fixed schema-aware query generation
- **STANDARD Schema**: 48/51 tests passing (94%), 3 xfailed (expected)
- **Security Graph Tests**: 98/98 passing (100%)
- **Unit Tests**: 577/577 passing (100%)
- **Key Fixes**:
  - VLP templates now use explicit relationship types (`[:FOLLOWS*2]` not `[*2]`)
  - Shortest path templates use explicit relationship types
  - Query generator prefers cyclic relationships (separate edge tables)
  - Added `RelationshipInfo` dataclass with connectivity metadata

### 🐛 Known Issues
See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details and workarounds.

---

## 🆕 **Recent Fixes** - December 3, 2025

### ✅ Undirected Multi-Hop Patterns Fixed
- **Issue**: `(a)-[r1]-(b)-[r2]-(c)` was generating broken SQL with wrong aliases
- **Fix**: BidirectionalUnion optimizer now generates 2^n UNION branches with:
  - Proper column swapping for denormalized nodes when direction is Incoming
  - Direction-aware `is_from_node` flags in GraphJoinInference
  - Correct JOIN conditions for all direction combinations
- **Files**: `bidirectional_union.rs`, `graph_join_inference.rs`
- **Example**: 2-hop undirected pattern now generates 4 correct UNION branches

### ✅ `collect()` Function Mapping
- **Issue**: `collect()` was generating literal "collect()" in SQL, which ClickHouse doesn't have
- **Fix**: Added function mapping: `collect()` → `groupArray()`

### ✅ Regex Match Operator (`=~`)
- **Feature**: Full support for Neo4j regex match operator
- **Cypher**: `WHERE n.name =~ '^A.*'`
- **SQL**: `WHERE match(n.name, '^A.*')` (ClickHouse native regex)

---

## 🎉 **v0.5.2 Released** - November 30, 2025

**Highlights**:
- ✅ Complete polymorphic edge support (wildcard, multi-hop, bidirectional)
- ✅ Composite edge IDs for polymorphic tables
- ✅ Coupled edge optimization (JOIN elimination)
- ✅ VLP + UNWIND support (path decomposition)
- ✅ OPTIONAL MATCH + VLP fix (anchor node preservation)
- ✅ Denormalized edge tables (edge = node table pattern)
- ✅ 534 library tests passing
- ✅ 73 schema variation tests (Standard, Denormalized, Polymorphic, Coupled)

---

## 🚨 **CRITICAL DOCUMENTATION FIX** - November 22, 2025

**Issue Found**: Cypher Language Reference was missing critical enterprise features:
- ❌ USE clause documentation incomplete
- ❌ Enterprise features (view_parameters, role) not documented
- ❌ Multi-tenancy patterns missing
- ❌ Schema selection methods not explained

**Impact**: Documentation inconsistency led to incorrect assessment of test failures as feature regressions

**Resolution**: ✅ **COMPLETE**
- ✅ Added comprehensive USE clause section (syntax, examples, common errors)
- ✅ Added Enterprise Features section (view_parameters, RBAC, multi-tenancy)
- ✅ Updated Table of Contents
- ✅ Documented schema name vs database name distinction
- ✅ Added production best practices

**Verified Features ARE Implemented**:
- ✅ USE clause (parser, handler, full implementation)
- ✅ Parameters (`$paramName` substitution)
- ✅ view_parameters (multi-tenancy support)
- ✅ role (RBAC passthrough)
- ✅ schema_name (API parameter)

**All enterprise-critical features are working and NOW properly documented**.

---

## ✅ **v0.5.2: Complete** 

**Status**: ✅ **RELEASED**  
**Started**: November 22, 2025  
**Completed**: November 30, 2025  

### 🆕 Polymorphic Edge Filters - COMPLETE (Nov 29, 2025)

**Feature**: Filter polymorphic edge tables by type discriminator columns

**What Works**:
- ✅ **Single type filter**: `MATCH (u:User)-[:FOLLOWS]->(f:User)` → `WHERE r.interaction_type = 'FOLLOWS'`
- ✅ **Node label filters**: `from_label_column`/`to_label_column` for source/target node types
- ✅ **VLP polymorphic filter**: Filters in both base case and recursive case of CTE
- ✅ **$any wildcard**: Skip node label filter when schema uses `$any`
- ✅ **IN clause generation**: `[:FOLLOWS|LIKES]` → `IN ('FOLLOWS', 'LIKES')` (for single-hop direct path)
- ✅ **Single-hop wildcard edges**: `(u:User)-[r]->(target)` with unlabeled targets
- ✅ **Multi-hop polymorphic CTE chaining**: `(u)-[r1]->(m)-[r2]->(t)` with proper CTE chains
- ✅ **Bidirectional (incoming) edges**: `(u:User)<-[r]-(source)` using `to_node_id` JOIN
- ✅ **Mixed edge patterns**: Standard edges + polymorphic edges in same query

**Schema Configuration**:
```yaml
relationships:
  - type: FOLLOWS
    table: interactions
    type_column: interaction_type      # Filter by type
    from_label_column: from_type       # Filter by source node type
    to_label_column: to_type           # Filter by target node type
```

**Example (Multi-hop polymorphic)**:
```cypher
MATCH (u:User)-[r1]->(middle)-[r2]->(target)
WHERE u.user_id = 1
RETURN u.user_id, r1.interaction_type, r2.interaction_type
```
Generates:
```sql
WITH rel_u_middle AS (...), rel_middle_target AS (...)
SELECT u.user_id, r1.interaction_type, r2.interaction_type
FROM brahmand.users AS u
JOIN rel_u_middle AS r1 ON r1.from_node_id = u.user_id
JOIN rel_middle_target AS r2 ON r2.from_node_id = r1.to_node_id  -- CTE chaining!
WHERE u.user_id = 1
```

**Example (Incoming edge)**:
```cypher
MATCH (u:User)<-[r]-(source)
WHERE u.user_id = 4
RETURN u.user_id, r.interaction_type
```
Generates:
```sql
WITH rel_source_u AS (...)
SELECT u.user_id, r.interaction_type
FROM brahmand.users AS u
JOIN rel_source_u AS r ON r.to_node_id = u.user_id  -- Incoming uses to_node_id
WHERE u.user_id = 4
```

**Limitation**: Alternate types `[:FOLLOWS|LIKES]` currently route through UNION CTE path
(designed for separate-table architectures). Works correctly but not optimized for polymorphic tables.

---

### 🆕 OPTIONAL MATCH + Variable-Length Paths - COMPLETE (Nov 30, 2025)

**Feature**: OPTIONAL MATCH with variable-length paths now correctly returns anchor nodes even when no path exists

**What Was Fixed**:
- ✅ **LEFT JOIN for VLP CTEs**: When `OPTIONAL MATCH` + VLP, CTE is LEFT JOINed (not used as FROM)
- ✅ **Anchor node in FROM clause**: Start node is FROM table, ensuring it's always returned
- ✅ **Outer query WHERE clause**: Start node filters are extracted to outer query
- ✅ **End node LEFT JOIN**: End node table is LEFT JOINed through CTE

**Example (Working)**:
```cypher
MATCH (a:User)
WHERE a.name = 'Eve'
OPTIONAL MATCH (a)-[:FOLLOWS*1..3]->(b:User)
RETURN a.name, COUNT(b) as reachable
```
Now correctly generates:
```sql
WITH RECURSIVE variable_path_xxx AS (...)
SELECT a.name AS "a.name", COUNT(b.user_id) AS "reachable"
FROM users AS a                                          -- Anchor in FROM
LEFT JOIN variable_path_xxx AS t ON t.start_id = a.user_id  -- LEFT JOIN CTE
LEFT JOIN users AS b ON t.end_id = b.user_id             -- LEFT JOIN end node
WHERE a.name = 'Eve'                                     -- Filter in outer query
GROUP BY a.name
```

**Before**: Eve (no followers) returned 0 rows
**After**: Eve correctly returns 1 row with `reachable = 0`

**All 27 OPTIONAL MATCH tests now pass (100%)**.

---

### 🆕 Coupled Edges Optimization - COMPLETE (Nov 28, 2025)

**Feature**: Automatic JOIN elimination for multi-hop patterns on same table

When multiple relationships share the same table AND connect through a "coupling node", ClickGraph:
- ✅ **Skips unnecessary JOINs** - No self-join on same table row
- ✅ **Unifies table aliases** - All edges use single alias (e.g., `r1` for both `r1` and `r2`)
- ✅ **Property resolution** - UNWIND correctly maps to SQL columns

**Example (Working)**:
```cypher
MATCH (ip:IP)-[r1:REQUESTED]->(d:Domain)-[r2:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
RETURN ip.ip, d.name, rip.ips
```
Generates (optimized - NO self-join):
```sql
SELECT r1."id.orig_h" AS "ip.ip", r1.query AS "d.name", r1.answers AS "rip.ips"
FROM zeek.dns_log AS r1
WHERE r1."id.orig_h" = '192.168.4.76'
```

**Tested Patterns**: Basic 2-hop, WHERE filters, COUNT/aggregations, ORDER BY, DISTINCT, edge properties, UNWIND with arrays

---

### 🆕 VLP + UNWIND Support - COMPLETE (Nov 28, 2025)

**Feature**: UNWIND `nodes(p)` and `relationships(p)` after variable-length paths

**What Works**:
- ✅ `UNWIND nodes(p) AS n` - Explodes path nodes to rows using ARRAY JOIN
- ✅ `UNWIND relationships(p) AS r` - Explodes path relationships to rows
- ✅ Works with all VLP patterns: `*`, `*2`, `*1..3`, `*..5`, `*2..`

**Example (Working)**:
```cypher
MATCH p = (u:User)-[:FOLLOWS*1..2]->(f:User)
WHERE u.user_id = 1
UNWIND nodes(p) AS n
RETURN n
```
Generates:
```sql
WITH RECURSIVE variable_path_... AS (
    SELECT ..., [start_node.user_id, end_node.user_id] as path_nodes
    FROM brahmand.users_bench start_node
    JOIN brahmand.user_follows_bench rel ON ...
    UNION ALL ...
)
SELECT n AS "n"
FROM variable_path_... AS t
ARRAY JOIN t.path_nodes AS n
```

**Key Implementation Details**:
- VLP CTEs automatically collect `path_nodes` and `path_relationships` arrays
- UNWIND is translated to `ARRAY JOIN` in ClickHouse
- Path function (`nodes()`, `relationships()`) is correctly resolved to CTE column

**Test Results**: 520 tests passing (single-threaded; 1 flaky race condition in parallel mode)

---

### 🎯 v0.5.2 Goals: Schema Variations

**Purpose**: Add support for advanced schema patterns while maintaining existing quality

**Features in Development**:
1. ✅ **Denormalized Edge Tables** (COMPLETE - Nov 27, 2025)
   - ✅ Schema structure complete (node-level properties)
   - ✅ Property resolution function enhanced
   - ✅ Single-hop patterns working
   - ✅ **Multi-hop patterns working** (verified via e2e tests)
   - ✅ **Variable-length paths working** (verified via e2e tests)
   - ✅ Aggregations on denormalized queries working
   - ✅ **shortestPath / allShortestPaths working**
   - ✅ **PageRank working** (named argument syntax)
   
2. ✅ **Coupled Edges** (COMPLETE - Nov 28, 2025)
   - ✅ Automatic JOIN elimination for multi-hop on same table
   - ✅ Alias unification across coupled edges
   - ✅ Works with UNWIND, aggregations, ORDER BY

3. ✅ **Polymorphic edges** (COMPLETE - Nov 29, 2025)
4. ✅ **Composite edge IDs** (COMPLETE - Nov 29, 2025)

#### Denormalized Edge Tables - Implementation Complete ✅

**All Features Working (Verified Nov 27, 2025)**:
- Schema architecture with node-level `from_node_properties` and `to_node_properties`
- YAML schema syntax finalized
- Property mapping function enhanced with role-awareness
- Single-hop pattern SQL generation
- **Multi-hop pattern SQL generation** (2-hop, 3-hop, etc.)
- **Variable-length path SQL generation** (`*1..2`, `*`, etc.)
- Aggregations (COUNT, SUM, AVG) on denormalized patterns
- **Graph algorithms**: shortestPath, allShortestPaths, PageRank

**Example (Working - Single-hop)**:
```cypher
MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)
WHERE a.city = "Seattle"
RETURN a.code, b.code, f.carrier
```
Generates:
```sql
SELECT f.Origin AS "a.code", f.Dest AS "b.code", f.Carrier AS "f.carrier"
FROM flights AS f
WHERE f.OriginCityName = 'Seattle'
```

**Example (Working - Multi-hop)**:
```cypher
MATCH (a:Airport)-[f1:FLIGHT]->(b:Airport)-[f2:FLIGHT]->(c:Airport)
RETURN a.code, b.code, c.code
```
Generates:
```sql
SELECT f1.Origin AS "a.code", f1.Dest AS "b.code", f2.Dest AS "c.code"
FROM flights AS f1
INNER JOIN flights AS f2 ON f2.Origin = f1.Dest
```

**Example (Working - shortestPath)**:
```cypher
MATCH p = shortestPath((a:Airport)-[:FLIGHT*1..5]->(b:Airport))
WHERE a.code = 'SEA' AND b.code = 'LAX'
RETURN p
```

**Example (Working - PageRank)**:
```cypher
CALL pagerank(graph: 'Airport', relationshipTypes: 'FLIGHT', iterations: 10, dampingFactor: 0.85)
YIELD nodeId, score RETURN nodeId, score
```
Note: PageRank requires named argument syntax (not positional).

**Test Results**:
- 20 denormalized-specific unit tests: ✅ All passing
- 487 total library tests: ✅ All passing
- E2E verification: ✅ All patterns working

### Baseline Test Results (Post-v0.5.1)

**Regression Testing Complete**: ✅ Baseline established

| Category | Tests | Pass Rate | Assessment |
|----------|-------|-----------|------------|
| **Core Queries** | 57/57 | **100%** | ✅ Production-ready |
| **Robust Features** | ~88/99 | **~88%** | 🟢 Stable |
| **Partial Features** | ~95/258 | **~37%** | 🟡 Known limitations |
| **Unimplemented** | ~0/100 | **0%** | 🔴 Not supported |
| **Baseline Total** | **240/414** | **57.9%** | ✅ Acceptable |

**Key Finding**: Test failures are **pre-existing issues**, not new regressions
- 57 core tests: All passing ✅
- 160 failing tests: Pre-existing bugs + unimplemented features + test environment issues
- See `tests/REGRESSION_ANALYSIS_CORRECTED.md` for details

**What Works**:
- ✅ Basic MATCH, WHERE, RETURN, ORDER BY, LIMIT
- ✅ Aggregations (COUNT, SUM, MIN, MAX, AVG)
- ✅ Relationships and multi-hop patterns
- ✅ CASE expressions (23/25 tests)
- ✅ Shortest paths
- ✅ Bolt protocol
- ✅ **AI Assistant Integration (MCP)** - Use ClickGraph with Claude via Neo4j MCP server (zero config)
- ✅ Error handling
- ✅ **USE clause (schema selection)**
- ✅ **Parameters (`$paramName` substitution)**
- ✅ **view_parameters (multi-tenancy)**
- ✅ **role (RBAC passthrough)**

**Known Test Issues** (Not Feature Regressions):
- 🐛 USE clause tests use wrong schema names (test bug - database name vs schema name)
- 🐛 Parameter function tests may have similar issues
- 🟡 Variable-length paths (partially implemented, ~50% pass rate)
- 🟡 Complex WITH clauses (~45% pass rate)

---

## 📋 v0.5.2 Development Plan

### Baseline Regression Testing - COMPLETE ✅

**Status**: ✅ **Baseline established** - No new regressions detected

**Findings**:
- ✅ Ran 414 integration tests
- ✅ 240 tests passing (57.9%) - same as pre-v0.5.2
- ✅ 160 failures are **pre-existing** issues (not new regressions)
- ✅ Core features (57 tests): 100% passing
- ✅ No regressions introduced

**Conclusion**: v0.5.1 is stable. Safe to proceed with new features.

**Documentation Created**:
- `tests/REGRESSION_ANALYSIS_CORRECTED.md` - Analysis of pre-existing issues
- `ALPHA_KNOWN_ISSUES.md` - Known limitations (archived as not applicable yet)
- Server management scripts in `scripts/test/`

---

### Schema Variations Implementation - NEXT

**Goal**: Add support for advanced schema patterns

**Features to Implement**:

1. **Polymorphic Edges** ✅ **COMPLETE** (Nov 29, 2025)
   - ✅ Single relationship type per polymorphic table
   - ✅ Type discriminator column support (`type_column`)
   - ✅ Node label columns (`from_label_column`, `to_label_column`)
   - ✅ VLP polymorphic filter (recursive CTE with type filter)
   - ✅ Single-hop polymorphic filter (JOIN ON clause)
   - ✅ IN clause support for multiple types (implementation ready)
   - ✅ **Single-hop wildcard edges** (`(u)-[r]->(target)`)
   - ✅ **Multi-hop polymorphic CTE chaining** (`(u)-[r1]->(m)-[r2]->(t)`)
   - ✅ **Bidirectional (incoming) edges** (`(u)<-[r]-(source)`)
   - ✅ **Mixed edge patterns** (standard + polymorphic in same query)
   - 🟡 Alternate types `[:FOLLOWS|LIKES]` routes through UNION CTE (works, not optimized)
   - Example: Single `interactions` table with `interaction_type` column

2. **Denormalized Properties** ✅ **COMPLETE** (Nov 27, 2025)
   - ✅ Properties stored in both node and edge tables
   - ✅ Automatic property resolution
   - ✅ Example: User name in both `users` and `follows` tables

3. **Coupled Edges** ✅ **COMPLETE** (Nov 28, 2025)
   - ✅ Automatic JOIN elimination for multi-hop patterns on same table
   - ✅ Alias unification (all edges use single alias like `r1`)
   - ✅ Works with denormalized edge tables
   - ✅ Example: Zeek DNS log pattern `(IP)->(Domain)->(ResolvedIP)`

4. **Composite Edge IDs** ✅ **COMPLETE** (Nov 29, 2025)
   - ✅ Single-column edge IDs: `edge_id: uid`
   - ✅ Composite edge IDs: `edge_id: [col1, col2, ...]`
   - ✅ Works with VLP (variable-length paths)
   - ✅ Works with polymorphic edge tables
   - ✅ Proper uniqueness checking with tuples
   - ✅ **`id(r)` returns `tuple(...)` for composite edge IDs** (Dec 1, 2025)
   - ✅ **Round-trip support**: `WHERE id(r) = tuple(a, b, c)` works
   - Example schema: `edge_id: [FlightDate, FlightNum, Origin, Dest]`
   - Example query: `MATCH ()-[r]->() RETURN id(r)` → `tuple(r.FlightDate, r.FlightNum, ...)`

**Success Criteria**:
- ✅ New features work with test cases
- ✅ Don't regress existing 240 passing tests
- ✅ Comprehensive documentation
- ✅ Test coverage for new schema patterns

**Timeline**: 1-2 weeks

---

### Post-Implementation Testing

**After schema variations are complete**:
1. Re-run full regression suite (414 tests)
2. Verify no new regressions (maintain 240+ passing)
3. Add test coverage for new schema patterns
4. Update documentation

**Then**: Ship v0.5.2-alpha with schema variations support!

---

## 🔄 **Previous: Phase 2 Enterprise Readiness**

**Status**: ✅ **Completed November 2025**  
**Target**: v0.5.0 (January-February 2026)

### 🚀 Delivered Features (4.5/5)

#### ✅ 1. **RBAC & Row-Level Security** (Complete)

#### 1. **Parameterized Views for Multi-Tenancy**
- ✅ **Schema Configuration**: `view_parameters: [tenant_id, region, ...]` in YAML
- ✅ **SQL Generation**: `view_name(param=$paramName)` with placeholders
- ✅ **Cache Optimization**: Single template shared across all tenants (99% memory reduction)
- ✅ **HTTP API**: `view_parameters` field in query requests
- ✅ **Bolt Protocol**: Extract from RUN message metadata
- ✅ **Multi-Parameter Support**: Unlimited parameters per view

**Usage Example**:
```yaml
# Schema
nodes:
  - label: User
    table: users_by_tenant
    view_parameters: [tenant_id]
```

```json
// Query
POST /query
{
  "query": "MATCH (u:User) RETURN u.name",
  "view_parameters": {"tenant_id": "acme"}
}
```

```sql
-- Generated SQL (with placeholder)
SELECT name FROM users_by_tenant(tenant_id = $tenant_id)

-- Runtime substitution
-- ACME: tenant_id = 'acme'
-- GLOBEX: tenant_id = 'globex'
```

#### 2. **SET ROLE RBAC Support**
- ✅ **ClickHouse Native RBAC**: `SET ROLE 'viewer'` before queries
- ✅ **HTTP API**: `role` field in requests
- ✅ **Bolt Protocol**: Role extraction from metadata
- ✅ **Column-Level Security**: Combine with row-level (parameterized views)

**Usage**:
```json
{
  "query": "MATCH (u:User) RETURN u",
  "view_parameters": {"tenant_id": "acme"},  // Row-level security
  "role": "viewer"                            // Column-level security
}
```

#### 3. **Comprehensive Documentation**
- ✅ **User Guide**: `docs/multi-tenancy.md` with 5 patterns
- ✅ **Example Schemas**: Simple + encrypted multi-tenancy
- ✅ **Technical Notes**: `notes/parameterized-views.md`
- ✅ **Migration Guide**: Adding multi-tenancy to existing schemas

#### 4. **Test Coverage**
- ✅ **Unit Tests**: 7/7 schema parsing tests passing
- ✅ **Integration Tests**: Comprehensive pytest suite (11 test classes)
- ✅ **E2E Validation**: ACME/GLOBEX tenant isolation verified
- ✅ **Cache Behavior**: Validated template sharing across tenants

#### ✅ 2. **Documentation Consistency & Completeness** (Complete - Nov 18)

**HTTP API & Schema Loading**:
- ✅ **Fixed Endpoint Routing**: Wired `GET /schemas/{name}` to router
- ✅ **Auto-Discovery Tests**: Updated from `/register_schema` to `/schemas/load`
- ✅ **Aspirational Test Marking**: 9 tests properly skipped with explanations
- ✅ **API Documentation**: Fixed parameter names (`config_content` not `config_path`)
- ✅ **Cross-Platform Examples**: Added PowerShell examples throughout

**Wiki Reference Pages** (3 new comprehensive pages):
- ✅ **API-Reference-HTTP.md**: Complete HTTP API reference (450+ lines)
  - All endpoints documented with examples
  - curl, Python, PowerShell examples
  - Multi-tenancy and RBAC usage
  - Performance tips and error handling
  
- ✅ **Cypher-Language-Reference.md**: Complete Cypher syntax guide (600+ lines)
  - All clauses: MATCH, WHERE, RETURN, WITH, ORDER BY, etc.
  - Variable-length paths, OPTIONAL MATCH, path functions
  - Aggregations, functions, operators
  - Real-world query examples
  
- ✅ **Known-Limitations.md**: Comprehensive limitations guide (500+ lines)
  - Feature support matrix (supported/partial/not implemented)
  - ClickHouse-specific constraints
  - Workarounds and best practices
  - Platform-specific issues (Windows)

**Fixed Broken Links**:
- ✅ Home.md reference section fully functional
- ✅ All internal wiki cross-references working
- ✅ No broken links in documentation

**Impact**:
- Professional documentation standards
- Complete API reference for developers
- Clear feature status and limitations
- Better user experience with wiki navigation

### 📊 Performance Metrics

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Cache Entries** | 100 (for 100 tenants) | 1 | **99% reduction** |
| **Memory Usage** | O(n) | O(1) | **Constant** |
| **Cache Hit Rate** | ~30% | ~100% | **3x improvement** |
| **Query Time** | 18ms | 9ms (cached) | **2x faster** |

### 🔐 Security Features

**Row-Level Security** (Parameterized Views):
- ✅ Tenant isolation at database level
- ✅ Per-tenant encryption keys
- ✅ Time-based access control
- ✅ Regional restrictions
- ✅ Hierarchical tenant trees

**Column-Level Security** (SET ROLE):
- ✅ Role-based permissions
- ✅ ClickHouse managed users
- ✅ Dynamic role switching per query

### 📦 Deliverables

**Code**:
- `src/graph_catalog/`: Schema parsing with `view_parameters`
- `src/render_plan/`: SQL generation with placeholders
- `src/server/`: HTTP/Bolt parameter extraction + merging
- `src/query_planner/`: Context propagation through PlanCtx

**Documentation**:
- `docs/multi-tenancy.md` - Complete user guide
- `docs/api.md` - Complete HTTP API reference ✅ Updated Nov 18
- `docs/wiki/API-Reference-HTTP.md` - Wiki API reference ✅ NEW Nov 18
- `docs/wiki/Cypher-Language-Reference.md` - Complete Cypher syntax ✅ NEW Nov 18
- `docs/wiki/Known-Limitations.md` - Limitations & workarounds ✅ NEW Nov 18
- `docs/wiki/Schema-Configuration-Advanced.md` - Updated with working API ✅ Nov 18
- `notes/parameterized-views.md` - Technical implementation
- `notes/phase2-minimal-rbac.md` - Design document
- `AUTO_DISCOVERY_STATUS.md` - HTTP schema loading reference ✅ NEW Nov 18

**Examples**:
- `schemas/examples/multi_tenant_simple.yaml`
- `schemas/examples/multi_tenant_encrypted.yaml`
- `schemas/test/multi_tenant.yaml`

**Tests**:
- `tests/integration/test_multi_tenant_parameterized_views.py`
- `tests/rust/unit/test_view_parameters.rs`

### 🎯 Multi-Tenant Patterns Supported

1. **Simple Isolation**: Filter by `tenant_id`
2. **Multi-Parameter**: tenant + region + date range
3. **Per-Tenant Encryption**: Unique keys per tenant
4. **Hierarchical Tenants**: Parent sees child data
5. **Role-Based + Row-Level**: Combine SET ROLE + parameters

### 📝 Key Commits

- `5a1303d`: Phase 2 documentation complete (Nov 17)
- `805db43`: Cache optimization with SQL placeholders (Nov 17)
- `fa215e3`: Complete parameterized views documentation (Nov 16)
- `7ea4a05`: SQL generation with view parameters (Nov 15)
- `5d0f712`: SET ROLE RBAC support (Nov 15)
- `2d1cb04`: Schema configuration (Nov 15)

---

### 🔄 Remaining Phase 2 Tasks (2/5)

Per ROADMAP.md Phase 2 scope:

#### ✅ 3. **ReplacingMergeTree & FINAL** (Complete)
**Effort**: 1-2 weeks  
**Impact**: 🌟 Medium-High  
**Purpose**: Support mutable data patterns common in production  
**Completed**: November 17, 2025

**Delivered**:
- ✅ Engine detection module (`engine_detection.rs`) - 13 tests passing
- ✅ Schema configuration: `use_final: bool` field in YAML
- ✅ SQL generation: Correct FINAL placement (`FROM table AS alias FINAL`)
- ✅ Schema loading integration: Auto-detect engines via `to_graph_schema_with_client()`
- ✅ Auto-set use_final based on engine type
- ✅ Manual override support

**Usage**:
```yaml
nodes:
  - label: User
    table: users
    use_final: true  # Manual (for any engine)
    
  - label: Post
    table: posts
    auto_discover_columns: true  # Auto-detects engine + sets use_final
```

#### ✅ 4. **Auto-Schema Discovery** (Complete)
**Effort**: 1-2 weeks  
**Impact**: 🌟 Medium  
**Purpose**: Reduce YAML maintenance for wide tables  
**Completed**: November 17, 2025

**Delivered**:
- ✅ Column auto-discovery via `system.columns` query
- ✅ Identity property mappings (column_name → column_name)
- ✅ Selective column exclusion
- ✅ Manual override system
- ✅ Automatic engine detection + FINAL support
- ✅ Example schema: `schemas/examples/auto_discovery_demo.yaml`
- ✅ Integration tests: `tests/integration/test_auto_discovery.py`
- ✅ Documentation: `notes/auto-schema-discovery.md`

**Usage**:
```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    auto_discover_columns: true
    exclude_columns: [_version, _internal]
    property_mappings:
      full_name: name  # Override specific mappings
```

**Benefits**:
- 90% reduction in YAML (50 columns → 5 lines)
- Auto-syncs with schema changes
- Backward compatible

#### ✅ 4.5. **Denormalized Property Access** (Complete)
**Effort**: 2 days  
**Impact**: 🔥 High  
**Purpose**: 10-100x faster queries on denormalized schemas (e.g., OnTime flights)  
**Completed**: November 27, 2025

**Delivered**:
- ✅ Enhanced property mapping with relationship context
- ✅ Direct edge table column access (eliminates JOINs)
- ✅ Automatic fallback to node properties
- ✅ Variable-length path optimization
- ✅ 6 comprehensive unit tests
- ✅ Documentation: `notes/denormalized-property-access.md`

**Schema Configuration**:
```yaml
relationships:
  - type: FLIGHT
    table: flights
    from_id: origin_id
    to_id: dest_id
    property_mappings:
      flight_num: flight_number
    # 🆕 Denormalized node properties
    from_node_properties:
      city: origin_city      # Access Airport.city from flights.origin_city
      state: origin_state
    to_node_properties:
      city: dest_city        # Access Airport.city from flights.dest_city
      state: dest_state
```

**Performance Example** (OnTime 5M flights):
```cypher
MATCH (a:Airport {code: 'LAX'})-[:FLIGHT*1..2]->(b:Airport)
RETURN b.city
```
- **Traditional (with JOINs)**: 450ms
- **Denormalized**: 12ms
- **Speedup**: **37x faster** ⚡

**How It Works**:
1. Property access checks denormalized columns first
2. Falls back to traditional node JOINs if not found
3. Works with variable-length paths, shortest path, OPTIONAL MATCH

#### 🔄 5. **v0.5.0 Wiki Documentation** (Planning Complete)
**Effort**: 3-4 weeks (25 days structured implementation)  
**Impact**: 🔥 High  
**Purpose**: Comprehensive documentation for adoption  
**Status**: Planning complete, ready for implementation (Nov 18, 2025)

**What's Planned** (see `docs/WIKI_DOCUMENTATION_PLAN.md`):
- ✅ Complete content audit (existing docs: 2000+ lines)
- ✅ Identified gaps (10 high-priority topics)
- ✅ 4-phase implementation plan (User Adoption → Production → Advanced → Integration)
- ✅ 50+ planned pages across 11 major sections
- ⏳ Phase 1: Home + Quick Start + Cypher Patterns (Week 1)
- ⏳ Phase 2: Production deployment guides (Week 2)
- ⏳ Phase 3: Advanced features (Week 3)
- ⏳ Phase 4: Use cases & integrations (Week 4)

---

### 🎯 Phase 2 Completion Plan

**Current Progress**: 4.5/5 features complete (90%)  
**Estimated Time Remaining**: 3-4 weeks

**Completed Features**:
1. ✅ **RBAC & Row-Level Security** - Multi-tenant parameterized views
2. ✅ **ReplacingMergeTree & FINAL** - Mutable data support
3. ✅ **Auto-Schema Discovery** - Zero-config column mapping
4. ✅ **Denormalized Property Access** - 10-100x faster queries

**Remaining**:
5. **Week 1-4**: Comprehensive Wiki documentation

**Alternative**: Ship v0.5.0-beta now with items 1-4, complete documentation for v0.5.0 final

---

### 🚀 Next Steps Options

**Option A: Quick Beta Ship** (Recommended)
- Ship v0.5.0-beta with completed features (RBAC + Multi-tenancy)
- Gather user feedback
- Complete remaining items for v0.5.0 final

**Option B: Complete Phase 2**
- Implement ReplacingMergeTree support (1-2 weeks)
- Add auto-schema discovery (1-2 weeks)
- Write comprehensive Wiki (3-4 weeks)
- Ship v0.5.0 final (6-8 weeks total)

---

## 🎉 Major Achievements

- ✅ **423/423 unit tests passing** - 100% pass rate (Nov 19, 2025) - **Including fixed flaky cache test**
- ✅ **236/400 integration tests passing** - 59% real features tested (aspirational tests for unimplemented features)
- ✅ **Bolt Protocol 5.8 complete** - Full Neo4j driver compatibility with all E2E tests passing (4/4) (Nov 12-15, 2025)
- ✅ **All 4 YAML relationship types working** - AUTHORED, FOLLOWS, LIKED, PURCHASED
- ✅ **Multi-hop graph traversals** - Variable-length paths with recursive CTEs
- ✅ **Dual protocol support** - HTTP + Bolt both production-ready
- ✅ **Multi-tenancy & RBAC** - Parameterized views + SET ROLE support
- ✅ **Auto-schema discovery** - Zero-configuration column mapping
- ✅ **Cross-platform** - Linux, macOS, Windows support

---

**For detailed technical information, see feature notes in `notes/` directory.**




