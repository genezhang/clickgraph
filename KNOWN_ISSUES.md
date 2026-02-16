# Known Issues

**Last Updated**: February 16, 2026

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).

---

## Active Issues

### 1. Neo4j Desktop / NeoDash WebSocket Connection (GitHub #57)
**Status**: Open  
**Error**: `Invalid magic preamble: [71, 69, 84, 32], expected: [96, 96, 176, 23]`  
**Cause**: Neo4j Desktop and NeoDash send HTTP/WebSocket upgrade requests (`GET ...`) instead of raw Bolt TCP. ClickGraph's Bolt listener expects the raw Bolt handshake preamble.  
**Impact**: Cannot connect via Neo4j Desktop or hosted NeoDash. Neo4j Browser (standalone), Python driver, and Cypher Shell work fine.  
**Workaround**: Use Neo4j Browser at `http://localhost:7474` (standalone) or connect via Python/Java drivers directly.

### 2. Shortest Path on Dense Graphs
**Status**: Performance limitation  
**Error**: `MEMORY_LIMIT_EXCEEDED` or query timeout  
**Cause**: Recursive CTE-based shortest path explores all paths. Dense graphs cause exponential explosion.  
**Workaround**: Use bounded path length: `shortestPath((a)-[:FOLLOWS*1..5]->(b))`

### 3. Pattern Comprehensions
**Status**: Parsed but not executed  
**Error**: `PatternComprehensionNotRewritten`  
**Example**: `[(a)-[r]->(b) | b.name]`  
**Impact**: Low-Medium — blocks 1-2 LDBC queries (bi-8)  
**Cause**: AST and LogicalExpr exist, but the rewrite pass to convert to SQL is not implemented

### 4. Aggregations on Empty Results Return Empty Array
**Status**: Semantics mismatch with Neo4j (compatibility issue)  
**Error**: None (behavior mismatch)  
**Impact**: Medium — Breaks Neo4j compatibility, client code must check for empty arrays  
**Cause**: Result handling layer doesn't distinguish between "no rows" vs "aggregation on no rows"

**Expected Neo4j Behavior** (aggregations always return one row):
```cypher
MATCH (p:Post)-[r]->(u:User) RETURN count(*) as result
→ {"results":[{"result": 0}]}

MATCH (p:Post)-[r]->(u:User) RETURN sum(p.post_id), avg(p.post_id), min(p.post_id), max(p.post_id), collect(p.content)
→ {"results":[{"sum": 0, "avg": NULL, "min": NULL, "max": NULL, "collect": []}]}
```

**Actual ClickGraph Behavior** (returns empty array for all aggregations):
```cypher
MATCH (p:Post)-[r]->(u:User) RETURN count(*), sum(...), avg(...), etc.
→ {"results":[]}
```

**Aggregate Function Comparison**:

| Function | Neo4j (Empty) | ClickHouse (Empty) | ClickGraph (Empty) | ✅ Match Neo4j? |
|----------|---------------|--------------------|--------------------|----------------|
| `count(*)` | `0` | `0` | `[]` | ❌ |
| `sum(expr)` | `0` | `0` | `[]` | ❌ |
| `avg(expr)` | `NULL` | `nan` | `[]` | ❌ |
| `min(expr)` | `NULL` | `0` | `[]` | ❌ |
| `max(expr)` | `NULL` | `0` | `[]` | ❌ |
| `collect(expr)` | `[]` | `[]` | `[]` | ❌ (structure wrong) |

**ClickGraph Issue**: Returns `{"results": []}` instead of `{"results": [{"count(*)": 0, ...}]}`

**Root Cause**: In `src/server/handlers.rs` lines 1032-1035, the result handler directly wraps ClickHouse rows without checking if the query contains aggregations. ClickHouse returns 0 rows for empty matches, but SQL aggregations should always return 1 row.

**Fix Location**: Need to detect aggregate queries and ensure at least one result row is returned. Options:
1. Modify SQL generation to use `SELECT ... UNION ALL SELECT 0 WHERE NOT EXISTS(...)`
2. Post-process results in handlers.rs to inject default aggregate row when empty
3. Modify ClickHouse query to use `WITH TOTALS` or similar mechanism

**Workaround**: Client code must check `if (response.results.length === 0)` and supply default aggregate values (0 for count/sum, null for avg/min/max, [] for collect)

### 5. Empty Plans with Column References Generate Invalid SQL
**Status**: Active bug (generates SQL that fails execution)  
**Error**: `Unknown expression identifier 'column_name'`  
**Impact**: High — Queries with impossible patterns + property access fail  
**Cause**: Generated SQL tries to SELECT columns without FROM clause

**Example**:
```cypher
MATCH (p:Post)-[r]->(u:User) WITH r.created_at as ts RETURN ts
-- No Post→User relationships exist, so Empty plan generated
```

**Generated SQL** (FAILS):
```sql
WITH with_ts_cte_1 AS (
    SELECT r.created_at AS "ts"  -- ❌ ERROR: Unknown identifier 'r'
    WHERE false                   -- ← No FROM clause!
)
SELECT ts.ts AS "ts" FROM with_ts_cte_1 AS ts
```

**ClickHouse Error**: `Unknown expression identifier 'r.created_at' in scope`

**Root Cause**: 
- `src/render_plan/plan_builder.rs` lines 2301-2333 generates `SELECT 1 AS "_empty" WHERE false` for Empty plans
- Works for aggregations (`SELECT count(*) WHERE false`) 
- **Fails for column references** (`SELECT r.prop WHERE false`) - no table context for 'r'

**Solution**: Use `FROM system.one WHERE false` + replace column references with typed defaults:
```sql
-- Replace column references with NULL or typed defaults
SELECT 
    NULL AS "ts",              -- Unknown type → NULL
    0 AS "user_id",            -- Known Int → 0
    '' AS "name"               -- Known String → ''
FROM system.one WHERE false    -- ✅ Provides table context
```

**Implementation**: Empty Propagation optimization (see session files) will fix this comprehensively

**Workaround**: None - queries fail. Avoid property access on impossible patterns.

**Related**: This will be fixed by the Empty Propagation optimization (see session files)

### 6. Unlabeled Nodes with Labeled Relationship Create Invalid UNION Branches
**Status**: Active regression from consolidation (Feb 16, 2026)  
**Error**: `Identifier 'table.column' cannot be resolved`  
**Impact**: **CRITICAL** — Common browser pattern fails  
**Discovered**: Neo4j Browser click-to-expand on relationship types

**Example**:
```cypher
MATCH ()-[r:LIKED]->() RETURN r LIMIT 25
-- Browser generates this when clicking a relationship type
```

**Error**:
```
Code: 47. DB::Exception: Identifier 't43.post_id' cannot be resolved from table 
with name t43. In scope SELECT ... FROM social.users AS t42 
INNER JOIN social.post_likes AS r ON r.user_id = t42.user_id 
INNER JOIN social.users AS t43 ON t43.post_id = r.post_id
```

**Root Cause**: 
TypeInference Phase 2 creates invalid UNION branches for unlabeled nodes with labeled relationships:

**Schema**: LIKED is User → Post (from_id: user_id, to_id: post_id)

**Generated SQL** (3 branches, 2 invalid!):
1. ❌ User → User: `FROM users t80 JOIN post_likes r JOIN users t81 ON t81.post_id = r.post_id` (users has no post_id!)
2. ❌ Post → User: `FROM posts t80 JOIN post_likes r ON r.user_id = t80.user_id` (posts has no user_id!)
3. ✅ User → Post: `FROM users t80 JOIN post_likes r JOIN posts t81 ON t81.post_id = r.post_id` (correct!)

**Expected**: Only create branch #3 (the valid one based on schema)

**What Works**:
- ✅ `MATCH (u:User)-[r:LIKED]->(p:Post)` - Both labeled
- ✅ `MATCH (u:User)-[r:LIKED]->()` - One labeled
- ❌ `MATCH ()-[r:LIKED]->()` - Both unlabeled (FAILS!)

**Code Location**: `src/query_planner/analyzer/type_inference.rs` Phase 2 (lines ~2315-2600)
- Should use `check_relationship_exists_with_direction()` to validate BEFORE creating UNION branches
- Currently creates all possible node type combinations, then fails at SQL execution

**Impact**: Breaks Neo4j Browser relationship exploration (critical UX feature)

**Workaround**: Always label at least one node: `MATCH (u:User)-[r:LIKED]->() RETURN r`

**Priority**: **CRITICAL** - Regression from consolidation work, breaks browser

---

### 7. Property Filtering on Unlabeled Nodes Creates Invalid UNION Branches
**Status**: Active regression from consolidation (Feb 16, 2026)  
**Error**: `Identifier 'n.email' cannot be resolved from table posts`  
**Impact**: **CRITICAL** — Neo4j Browser property key discovery broken  
**Discovered**: Browser "Get metadata" queries for property keys

**Example**:
```cypher
MATCH (n) WHERE n.email IS NOT NULL RETURN DISTINCT n.email LIMIT 25
-- Browser uses this to discover available properties
```

**Error**:
```
Code: 47. DB::Exception: Identifier 'n.email' cannot be resolved from table 
with name n. In scope SELECT DISTINCT ... FROM social.posts AS n 
WHERE n.email IS NOT NULL
```

**Root Cause**: 
TypeInference creates UNION branches for ALL node types, including those without the accessed property:

**Schema**: Only User has `email` property, Post does not

**Generated SQL** (2 branches, 1 invalid!):
1. ✅ User: `FROM users WHERE users.email IS NOT NULL` (correct!)
2. ❌ Post: `FROM posts WHERE posts.email IS NOT NULL` (posts has no email column!)

**Expected**: Only create User branch (filter by property schema)

**Code Location**: Same as Issue #6 — `type_inference.rs` Phase 2
- Should filter candidates by properties accessed in WHERE/RETURN/WITH clauses
- Currently assigns all node types without schema validation

**Impact**: Breaks Neo4j Browser metadata discovery and property-based searches

**Workaround**: Always specify node label: `MATCH (u:User) WHERE u.email IS NOT NULL`

**Priority**: **CRITICAL** - Regression from consolidation work, breaks browser

---

### 8. Relationship Property Access Fails with CTE Structure
**Status**: Active regression from consolidation (Feb 16, 2026)  
**Error**: `Identifier 'r.created_at' cannot be resolved from subquery r`  
**Impact**: **CRITICAL** — Cannot access relationship properties in browser  
**Discovered**: Browser relationship property queries

**Example**:
```cypher
MATCH ()-[r]-() WHERE r.created_at IS NOT NULL 
RETURN DISTINCT r.created_at LIMIT 25
```

**Error**:
```
Code: 47. DB::Exception: Identifier 'r.created_at' cannot be resolved 
from subquery with name r. In scope ... 
SELECT DISTINCT r.created_at FROM pattern_union_r AS r
```

**Root Cause**: 
Relationship CTE packs properties into JSON array instead of individual columns:

**Generated SQL**:
```sql
WITH pattern_union_r AS (
    SELECT ... 
        [formatRowNoNewline('JSONEachRow', r.created_at)] AS rel_properties
    ...
)
SELECT r.created_at FROM pattern_union_r AS r  -- ❌ No r.created_at column!
```

**Expected**: 
```sql
WITH pattern_union_r AS (
    SELECT ...
        toString(r.created_at) AS "r.created_at"  -- ✅ Expand to column
    ...
)
SELECT r.created_at FROM pattern_union_r AS r
```

**Code Location**: 
- CTE generation in `clickhouse_query_generator/` (relationship pattern handling)
- May need changes to how relationship properties are surfaced

**Impact**: Cannot filter or return relationship properties when relationship is unlabeled

**Workaround**: Label the relationship: `MATCH ()-[r:LIKED]-() WHERE r.created_at IS NOT NULL`

**Priority**: **CRITICAL** - Regression from consolidation work, breaks browser

---

### 9. Query-Level UNION Fails Plan Context Merge
**Status**: Active regression from consolidation (Feb 16, 2026)  
**Error**: `Failed to merge plan contexts for UNION`  
**Impact**: **CRITICAL** — Browser combined property key queries broken  
**Discovered**: Browser metadata queries that combine node + relationship properties

**Example**:
```cypher
MATCH (n) WHERE n.email IS NOT NULL 
RETURN DISTINCT "node" as entity, n.email AS email LIMIT 25 
UNION ALL 
MATCH ()-[r]-() WHERE r.email IS NOT NULL 
RETURN DISTINCT "relationship" AS entity, r.email AS email LIMIT 25
```

**Error**:
```
Planning error: LogicalPlanError: Query planning error: 
Failed to merge plan contexts for UNION
```

**Root Cause**: Unknown — needs investigation
- Likely related to variable scopes conflicting across UNION branches
- Each branch creates its own plan context with variable 'n' or 'r'
- Merge logic may be failing due to context incompatibility

**Code Location**: 
- `src/query_planner/logical_plan/union.rs` (UNION planning)
- `src/query_planner/plan_ctx/mod.rs` (context merging)

**Impact**: Browser cannot fetch combined metadata (nodes + relationships in one query)

**Workaround**: Run two separate queries instead of UNION

**Priority**: **HIGH** - Regression from consolidation work, browser workaround available

---

## Out of Scope (by design)

ClickGraph is a **read-only** analytical query engine:
- ❌ Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`)
- ❌ Schema DDL (`CREATE INDEX`, `CREATE CONSTRAINT`)
- ❌ Transaction management (`BEGIN`, `COMMIT`, `ROLLBACK`)
- ❌ Stored procedures (APOC/GDS) — only built-in `db.*` procedures

---

## Recently Fixed (February 2026)

| Issue | Fix | PR |
|---|---|---|
| UNWIND crash with collect(DISTINCT) | Fixed infinite WITH iteration + DISTINCT handling | #91 |
| Cross-session ID leakage between tenants | IdMapper scoped by schema + tenant | #85 |
| Query cache ignores tenant_id | Cache key includes tenant_id + view_parameters | main |
| PackStream arrays/objects not encoded | Recursive PackStream encoding | #83 |
| UNION column mismatch (literal + aggregate) | Extracted helper, fixed branch construction | #84 |
| Browser click-to-expand failures (5 schema types) | CTE naming, JOIN fixes, VLP rendering | #70–#82 |
| Browser EXPLAIN probe noise | EXPLAIN handler returns empty SUCCESS | #85 |
| Session commands not working in browser | ConnectionState::Streaming fix | #85 |
