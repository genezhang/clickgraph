# Known Issues

**Active Issues**: 1 bug, 7 feature limitations  
**Last Updated**: February 11, 2026

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Recently Fixed Bugs

### 1. MULTI_TABLE_LABEL Standalone Node Aggregations - FIXED ✅
**Status**: ✅ FIXED (January 22, 2026)  
**Original Error**: `Unknown expression or function identifier 'n.ip'` (missing FROM clause in generated SQL)  
**Example**:
```cypher
USE zeek_merged
MATCH (n:IP) RETURN count(DISTINCT n.ip) as unique_count
```
**Solution**: Fixed three interconnected issues:
1. **Multi-table label detection**: Added `get_all_node_schemas_for_label()` method to find ALL tables with same label instead of just one
2. **Logical planning**: Added MULTI_TABLE_LABEL UNION generation when same label appears in multiple tables
3. **SQL rendering**: Implemented recursive Union extraction in `extract_union()` to unwrap Union nested inside GraphNode → Projection → GroupBy → GraphJoins

**Generated SQL** (After Fix):
```sql
SELECT count(DISTINCT n.ip) AS "unique_count" FROM (
  SELECT n."id.orig_h" AS "n.ip" FROM zeek.conn_log AS n
  UNION ALL
  SELECT n."id.resp_h" AS "n.ip" FROM zeek.conn_log AS n
  UNION ALL
  SELECT n."id.orig_h" AS "n.ip" FROM zeek.dns_log AS n
) AS __union
```

**Files Fixed**:
- `src/graph_catalog/graph_schema.rs` - Added `get_all_node_schemas_for_label()` method
- `src/query_planner/logical_plan/match_clause.rs` - Added MULTI_TABLE_LABEL UNION generation
- `src/render_plan/plan_builder.rs` - Implemented recursive Union extraction
- `src/clickhouse_query_generator/to_sql_query.rs` - Aggregation wrapping for UNION

**Testing**: All 784 unit tests passing, no regressions

---

## Known Limitations

**Documentation**: [docs/development/vlp-cross-functional-testing.md](docs/development/vlp-cross-functional-testing.md)

---

## Feature Limitations

The following Cypher features are **not implemented** (by design - read-only query engine):

### 1. Variable Alias Renaming in WITH Clause - FIXED ✅
**Status**: ✅ FIXED (January 22, 2026)  
**Original Error**: `Property 'name' not found on node 'person'` when using `MATCH (u:User) WITH u AS person RETURN person.name`  
**Root Cause**: When a variable was renamed via `WITH u AS person`, the type information (Node/Relationship/Scalar) was not propagated to the new alias.  
**Solution**: Modified `process_with_clause_chain()` in [plan_builder.rs](src/query_planner/logical_plan/plan_builder.rs) to:
1. Extract source→output alias mappings from `WithClause.items`
2. For simple variable renamings (e.g., `u AS person`), look up the source variable's labels
3. Create a new `TableCtx` with the output alias but preserve all type information

**Now Works**:
```cypher
-- Simple renaming ✅
MATCH (u:User) WITH u AS person RETURN person.name

-- Multiple renames ✅
MATCH (u:User) MATCH (f:User) WITH u AS person, f AS friend RETURN person.name, friend.name

-- Mixed rename and pass-through ✅
MATCH (u:User) MATCH (f:User) WITH u, f AS friend RETURN u.name, friend.name

-- Renamed variable in subsequent MATCH ✅
MATCH (u:User) WITH u AS person MATCH (person)-[:FOLLOWS]->(f) RETURN person.name

-- Chained renaming ✅
MATCH (u:User) WITH u AS a WITH a AS b RETURN b.name
```

**Files Changed**: [src/query_planner/logical_plan/plan_builder.rs](src/query_planner/logical_plan/plan_builder.rs) - Added variable renaming support and `extract_source_alias_from_expr()` helper  
**Tests**: All 784 unit tests passing, no regressions  
**Added**: January 20, 2026 | **Fixed**: January 22, 2026

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

### 5. Shortest Path on Dense Graphs
**Status**: ⚠️ PERFORMANCE LIMITATION  
**Example**:
```cypher
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.user_id = 1 RETURN p
```
**Error**: `MEMORY_LIMIT_EXCEEDED` or query timeout  
**Root Cause**: Recursive CTE-based shortest path algorithms explore all possible paths. On densely connected graphs (e.g., 4 users with 750 FOLLOWS edges = nearly complete graph), this causes exponential path explosion and memory exhaustion.  
**Impact**: shortestPath/allShortestPaths may timeout or OOM on dense graph topologies  
**Workaround**: 
- Add bounded path length: `shortestPath((a)-[:FOLLOWS*1..3]->(b))`
- Use sparser test data with realistic graph density
- For analytics, consider alternative approaches (pagerank, common neighbors)
**Technical Details**: ClickHouse recursive CTEs do not support BFS early termination optimization  
**Added**: January 20, 2026

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

## UNION Column Mismatch with Literal + Aggregate (2026-02-04)

**Issue**: Queries with only literals and aggregates in RETURN fail with UNION column mismatch

**Example**:
```cypher
MATCH (n) WHERE n.user_id IS NOT NULL
RETURN 'user_id' as property, count(*) as count
```

**Error**: 
```
UNION different number of columns in queries
```

**Root Cause**:
- `build_union_with_aggregation()` in `src/query_planner/logical_plan/return_clause.rs:539`
- When no property accesses found (only literals + aggregates)
- Inner UNION branches get `1 AS "__const"` placeholder
- Outer query retains original columns
- Result: Column count mismatch

**Workaround**:
- Include at least one property access: `RETURN n.user_id, count(*)`
- Or use typed pattern: `MATCH (n:User) RETURN 'user_id', count(*)`

**Status**: Pre-existing issue, NOT caused by Track C property optimization

**Priority**: Low (not from Neo4j Browser usage patterns)

## Denormalized Relationships in UNION Queries (2026-02-06) ✅ FIXED

**Issue**: Neo4j Browser relationship fetch queries fail for denormalized (FK-based) relationships

**Example Query** (sent by Neo4j Browser):
```cypher
MATCH (a)-[r]->(b) WHERE id(a) IN $existingNodeIds AND id(b) IN $newNodeIds RETURN r;
```

**Error (before fix)**:
```
Unknown expression or function identifier `r.user_id` in scope SELECT ... FROM posts_bench AS b_1 ...
```

**Root Cause**:
- Untyped relationship patterns `(a)-[r]->(b)` generate UNION branches for each relationship type
- For denormalized relationships (e.g., AUTHORED uses posts_bench as relationship table), the FK columns are on the node table, not a separate `r` table
- SELECT clause incorrectly references `r.user_id`, `r.post_id` which don't exist
- The relationship alias `r` doesn't correspond to a table in denormalized branches

**Solution** (2026-02-06):
- Added `fix_invalid_table_aliases()` in `normalize_union_branches()`
- Collects valid table aliases from FROM/JOINs for each branch
- Detects PropertyAccess expressions using invalid aliases
- Rewrites them to use the FROM table alias (which contains FK columns)
- Example: `r.user_id` → `b_1.user_id`

**Status**: ✅ Fixed in commit 2b9e579

**Affected Files**:
- `src/render_plan/plan_builder_helpers.rs` - Added `fix_invalid_table_aliases()` function


## Pattern Comprehension GROUP BY Property Bug (Feb 9, 2026)

**Issue**: Pattern comprehensions with WITH clause generate incorrect SQL in GROUP BY - references `a.city` instead of `t.start_city` when aggregating from VLP CTE.

**Example Query**:
```cypher
MATCH (a:User) WITH a, size([(a)-[r]->() | 1]) AS count RETURN a.user_id, count
```

**Generated SQL** (incorrect):
```sql
SELECT anyLast(a.city) AS "a_city" ...  -- ❌ Should be t.start_city
FROM vlp_multi_type_a_t1 AS t
GROUP BY t.start_id
```

**Error**: `DB::Exception: Unknown expression identifier 'a.city'`

**Workaround**: Only return aggregated count, not node properties:
```cypher
MATCH (a:User) WHERE a.user_id = 1 
WITH a, size([(a)-[r]->() | 1]) AS count 
RETURN count  -- Works
```

**Impact**: Neo4j Browser click-to-expand partially broken (needs property access fix)

**Related**: CTE name fix (Feb 9) solved the "Table not found" issue, but this property mapping bug remains.

---

## Neo4j Browser Schema Compatibility Limitations

### 7. Denormalized Node Schemas — Browser Expand ✅ FIXED
**Status**: ✅ FIXED (February 12, 2026)

**Description**: Neo4j Browser click-to-expand now works with denormalized/virtual node schemas (e.g., `denormalized_flights.yaml`). VLP CTEs generate JSON property blobs (`start_properties`, `end_properties`) instead of flat columns, matching the 9-field tuple format expected by `transform_vlp_path()`.

**Tested**: All 7 Bolt protocol tests pass — nodes with properties, relationships with all attributes, outbound click-to-expand, string element IDs.

### 8. Multi-Tenant Cross-Session ID Isolation
**Status**: ⚠️ Known Limitation  
**Added**: February 11, 2026

**Description**: When using parameterized views for multi-tenancy, the Bolt protocol's `IdMapper` cross-session lookup can produce cross-tenant ID contamination. Element IDs like `"User:1"` are identical across tenants because `tenant_id` is a view parameter, not part of the node ID.

Two tenants with `User:1` will get the same encoded integer ID. Cross-session reverse lookups (used when a new browser connection queries `id(n) = X`) may return data from the wrong tenant's session cache.

**Impact**: Only affects multi-tenant deployments using parameterized views with the Neo4j Browser Bolt protocol. HTTP API and single-tenant deployments are not affected.

**Workaround**: Include `tenant_id` as part of the composite node ID in the schema definition (e.g., `node_id: [tenant_id, user_id]`), or use separate ClickGraph instances per tenant.
