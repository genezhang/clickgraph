# Known Issues

**Active Issues**: 2  
**Test Results**: 588/588 unit tests passing (100%)  
**Integration Tests**: 110/118 core tests passing (93%) - improved with WITH clause fix  
**Security Graph Tests**: 98/98 passing (100%)  
**Last Updated**: December 7, 2025

For recently fixed issues, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## ~~Polymorphic Edge Queries Fail with "Traditional strategy requires OwnTable"~~ ✅ FIXED
**Fixed**: December 7, 2025  
**Root Cause**: When a relationship schema defined polymorphic endpoints (`from_node: $any` or `to_node: $any`), the code incorrectly created `NodeAccessStrategy::Virtual` nodes even when the actual query specified concrete node labels (e.g., `User`, `Group`) that have their own tables.

**Fix**: Modified `build_node_strategies()` in `src/graph_catalog/pattern_schema.rs` to always use the concrete node schema for building node access strategies, regardless of whether the edge schema is polymorphic. The polymorphic flags are still used for edge type filtering.

**Impact**: All polymorphic edge queries now work:
- `MATCH (u:User)-[:MEMBER_OF]->(g:Group)` - from-side polymorphic
- `MATCH (f:Folder)-[:CONTAINS]->(c:File)` - to-side polymorphic  
- `MATCH (u:User)-[:HAS_ACCESS]->(f:File)` - both-sides polymorphic
- Multi-hop traversals through polymorphic edges

---

## TODO: Denormalized Schema Query Patterns

Issues discovered during `zeek_merged.yaml` testing (December 5, 2025):

### ~~TODO-1: Cross-table OPTIONAL MATCH uses INNER JOIN instead of LEFT JOIN~~ ✅ FIXED
**Fixed**: December 5, 2025  
**Root Cause**: Multi-hop denormalized edge-to-edge JOIN was hardcoded to `JoinType::Inner`  
**Fix**: Changed to use `Self::determine_join_type(rel_is_optional)` in `graph_join_inference.rs:2552`

---

### ~~TODO-2: Chained WITH clauses include unexpected fields~~ ✅ FIXED
**Fixed**: December 5, 2025  
**Root Cause**: When `TableAlias("src")` was converted to `PropertyAccessExp("src", "*")`, the `col_alias` was not set. Later when the alias was remapped to the edge alias, the original node context was lost.

**Fix**: 
1. In `projection_tagging.rs`: Set `col_alias = "src.*"` when converting TableAlias to wildcard PropertyAccessExp
2. In `plan_builder.rs`: Use `col_alias` to recover original node name and lookup correct properties from `from_node_properties`

**Query**: `MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain) WITH src, COUNT(d) AS dns_count RETURN src.ip, dns_count`  
**Result**: Correctly generates SELECT with only src.ip (1 property) instead of all edge properties (6)

---

### ~~TODO-3: Cross-table WITH correlation missing JOIN~~ ✅ FIXED
**Fixed**: December 6, 2025  
**Root Cause**: Cross-table correlations (WHERE clause referencing WITH aliases across tables) were not being converted to JOIN conditions. The CartesianJoinExtraction pass was extracting the condition, but GraphJoinInference wasn't creating the proper JOIN for the second table.

**Fix** (in `graph_join_inference.rs`):
1. Added `deduplicate_joins()` function to remove duplicate joins for the same table alias, preferring joins that reference TableAlias (cross-table correlation)
2. Added FROM marker join detection in `reorder_joins_by_dependencies()` to recognize empty `joining_on` as anchor tables
3. Modified CartesianProduct handling to create both a FROM marker for the left table and a proper JOIN for the right table with the correlation condition

**Query**:
```cypher
MATCH (src:IP)-[r:DNS_REQUESTED]->(d:Domain) 
WITH src.ip AS source_ip, d.name AS domain 
MATCH (src2:IP)-[c:CONNECTED_TO]->(dest:IP) 
WHERE src2.ip = source_ip 
RETURN source_ip, domain, dest.ip
```
**Now Generates Correct SQL**:
```sql
SELECT source_ip AS "source_ip", domain AS "domain", c."id.resp_h" AS "dest.ip"
FROM zeek.dns_log AS r
INNER JOIN zeek.conn_log AS c ON c."id.orig_h" = source_ip
```

---

### ~~TODO-4: Node wildcard only expands one node~~ ✅ FIXED
**Fixed**: December 5, 2025  
**Root Cause**: Two issues:
1. Parser's `parse_alphanumeric_with_underscore` rejected `*` as property name
2. Wildcard expansion used edge alias lookup, losing distinction between `src.*` and `dest.*`

**Fix**: 
1. Added `parse_property_name()` in `expression.rs` that accepts `*`
2. In `plan_builder.rs`, use `col_alias` ("src.*", "dest.*") to determine node role
3. Added `get_properties_for_node_role()` helper to lookup from/to node properties
4. Prefixed column aliases with original node name for clarity

**Query**: `MATCH (src:IP)-[:CONNECTED_TO]->(dest:IP) RETURN src.*, dest.*`  
**Result**: Correctly generates 4 columns: src.ip, src.port, dest.ip, dest.port

---

### ~~TODO-5: Single-node patterns for denormalized nodes generate invalid SQL~~ ✅ FIXED
**Fixed**: December 7, 2025  
**Root Cause**: Two issues:
1. For denormalized nodes appearing in multiple tables (e.g., IP in both conn_log and dns_log), standalone `MATCH (ip:IP)` only generated a single ViewScan instead of a UNION across all tables
2. `count(node_alias)` generated invalid SQL because the inner UNION subquery didn't include the node's ID property column

**Fix**:
1. In `match_clause.rs`: Added multi-table UNION support for standalone node queries using `ProcessedNodeMetadata.id_sources` to enumerate all tables/positions where a label appears
2. In `projection_tagging.rs`: Expanded `count(denormalized_node)` to `count(node.id_property)` using schema's `id_column` - consistent with Neo4j behavior (count by node identity)
3. In `return_clause.rs`: Added logic to detect `TableAlias` in aggregate args and include the node's ID property in the inner UNION projection

**Queries Now Working**:
```cypher
-- All IPs from both tables (3-branch UNION: orig/resp from conn_log, orig from dns_log)
MATCH (ip:IP) RETURN count(ip), count(distinct ip)

-- Explicit property also works
MATCH (ip:IP) RETURN count(ip), count(distinct ip.ip)

-- Constrained by relationship
MATCH (ip:IP)-[:DNS_REQUESTED]-() RETURN count(ip), count(distinct ip.ip)
MATCH ()-[:CONNECTED_TO]->(ip:IP) RETURN count(ip), count(distinct ip.ip)
```

**Generated SQL for `MATCH (ip:IP) RETURN count(ip), count(distinct ip)`**:
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

**Neo4j Compatibility**: `count(DISTINCT node)` now counts distinct **node identities** (via ID column), matching Neo4j's behavior where nodes are compared by their internal element ID.

---

### ~~TODO-6: Node properties incorrectly remapped to edge alias in standard schemas~~ ✅ FIXED
**Fixed**: December 7, 2025  
**Root Cause**: The `get_denormalized_node_id_reference` function in `plan_builder_helpers.rs` was remapping ALL node aliases to edge aliases based on whether the edge had `from_id`/`to_id` columns. However, ALL edges have these FK columns - the function should only remap when the node is **actually denormalized** (properties stored on edge table).

**Symptoms**:
- `MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name` generated `SELECT edge.name, edge.name FROM ...` 
- WHERE clause `a.name = 'Alice'` became `edge.name = 'Alice'` (follows table has no `name` column)
- All relationship queries with property access were broken for standard schemas

**Fix**: Added check for `from_node_properties`/`to_node_properties` existence before remapping. Only schemas that define node properties on the edge (denormalized) should remap node aliases to edge aliases.

**Queries Now Working**:
```cypher
-- Standard schema (nodes have their own tables)
MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN a.name, b.name
-- Correctly generates: SELECT a.name, b.name FROM users AS a JOIN follows JOIN users AS b WHERE a.name = 'Alice'

-- Denormalized schema (nodes are columns on edge tables)
MATCH (src:IP)-[:CONNECTED_TO]->(dest:IP) RETURN src.ip, dest.ip
-- Correctly generates: SELECT edge.orig_h, edge.resp_h FROM conn_log AS edge
```

---

### ~~TODO-7: Undirected patterns generated identical UNION branches~~ ✅ FIXED

### ~~TODO-8: WITH clause alias not rewritten in nested aggregations~~ ✅ FIXED
**Fixed**: December 7, 2025  
**Root Cause**: Three issues:
1. WITH alias collection didn't look inside `GraphJoins` wrapper (only checked direct `Projection` child of `GroupBy`)
2. Expression rewriting for CTE references only handled top-level `TableAlias`, not nested ones inside aggregate functions like `AVG(follows)`
3. GROUP BY for wildcards only converted `a.*` to `a.user_id` but SELECT expanded it to all properties, causing ClickHouse GROUP BY error

**Fix**: 
1. Added recursive `extract_with_aliases()` helper to look inside `GraphJoins` wrapper
2. Created `rewrite_with_aliases_to_cte()` helper in `plan_builder_helpers.rs` that recursively rewrites `TableAlias` references inside `AggregateFnCall`, `ScalarFnCall`, `OperatorApplicationExp`, `Case` expressions, etc.
3. Expanded GROUP BY wildcards to all properties using `get_properties_with_table_alias()`

**Query Now Working**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
RETURN AVG(follows) as avg_follows_per_user
```

**Generated SQL**:
```sql
WITH grouped_data AS (
  SELECT a.age AS "a.age", a.name AS "a.name", a.user_id AS "a.user_id", 
         COUNT(b.user_id) AS "follows"
  FROM users AS a
  INNER JOIN follows ON ...
  GROUP BY a.age, a.name, a.user_id
)
SELECT AVG(grouped_data.follows) AS "avg_follows_per_user"
FROM grouped_data AS grouped_data
```

---

### TODO-9: CTE column aliasing for mixed RETURN (WITH alias + node property)
**Status**: 🔴 Active  
**Severity**: MEDIUM  
**Found**: December 7, 2025

**Symptom**: When RETURN references both WITH aliases AND node properties, the JOIN condition uses wrong column names.

**Example Failing Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
ORDER BY a.name
```

**Generated SQL** (incorrect):
```sql
WITH grouped_data AS (
  SELECT a.age AS "a.age", a.name AS "a.name", a.user_id AS "a.user_id", 
         COUNT(b.user_id) AS "follows"
  ...
)
SELECT a.name AS "a.name", grouped_data.follows AS "follows"
FROM test_integration.users AS a
INNER JOIN grouped_data AS grouped_data ON a.age = grouped_data.age  -- BUG: should be grouped_data."a.age"
ORDER BY a.name ASC
```

**Root Cause**: CTE column aliases include the table prefix (e.g., `"a.age"`) but the outer query JOIN tries to reference `grouped_data.age` (without prefix).

**Workaround**: For queries that only need WITH aliases in RETURN (no additional node properties), the optimization correctly skips the JOIN and selects directly from CTE. Ensure RETURN only references WITH clause output.

**Affected Tests**:
- `test_having_count`, `test_having_avg`, `test_having_multiple_conditions`
- `test_where_on_grouped_result`
- `test_case_on_relationship_count`

---

**Fixed**: December 7, 2025  
**Root Cause**: The `BidirectionalUnion` analyzer transforms undirected patterns `(a)-[r]-(b)` into UNION ALL of two directed branches. However, when creating the `Incoming` direction branch, it wasn't swapping `left_connection` and `right_connection` to match the parser's normalization convention (left=FROM, right=TO).

**Symptoms**:
- `MATCH (a:User)-[r:FOLLOWS]-(b:User) WHERE a.name = 'Bob' RETURN b.name` returned 4 rows instead of expected 3
- Both UNION branches generated identical JOIN conditions (both using Outgoing semantics)
- The second branch should have swapped the join direction to find people who follow Bob

**Fix**: In `bidirectional_union.rs`, added logic to swap `left_connection` and `right_connection` when creating the `Incoming` branch from an `Either` pattern. This maintains the invariant that left_connection is always the FROM node and right_connection is always the TO node, consistent with how the parser handles explicitly-written incoming patterns like `(a)<-[r]-(b)`.

**Queries Now Working**:
```cypher
-- Undirected relationship query
MATCH (a:User)-[r:FOLLOWS]-(b:User) WHERE a.name = 'Bob' RETURN b.name ORDER BY b.name
-- Branch 1: Bob follows others (a=from, b=to)
-- Branch 2: Others follow Bob (b=from, a=to)
-- Correctly returns: Alice (follows Bob), Charlie, Diana (Bob follows them)
```

---

## Active Issues

### 1. Anonymous Nodes Without Labels (Partial Support)

**Status**: ✅ Significantly Improved  
**Updated**: December 5, 2025

**Supported Inference Scenarios**:

1. **Label inference from relationship type**:
   - Query: \`()-[r:FLIGHT]->()\`
   - Schema: FLIGHT has from_node=Airport, to_node=Airport
   - Result: Both nodes inferred as Airport ✅

2. **Relationship type inference from typed nodes**:
   - Query: \`(a:Airport)-[r]->()\`
   - Schema: FLIGHT is the only edge with from_node=Airport
   - Result: r inferred as :FLIGHT ✅

3. **Single-schema inference**:
   - Query: \`()-[r]->()\`
   - Schema: Only one relationship defined
   - Result: r inferred automatically ✅

4. **Single-node-schema inference**:
   - Query: \`MATCH (n) RETURN n\`
   - Schema: Only one node type defined
   - Result: n inferred automatically ✅

5. **Multi-hop anonymous patterns** (December 5, 2025):
   - Query: \`()-[r1]->()-[r2]->()-[r3]->()\`
   - Schema: Only one relationship type
   - Result: All 3 relationships inferred ✅

**Remaining Limitations**:
- \`MATCH (n)\` with **multiple** node types in schema still doesn't work (needs UNION)
- Polymorphic edges use first matching type (future: UNION ALL for all types)
- Safety limit: max 4 types inferred before requiring explicit specification

**Workaround**: Specify at least one label when multiple types exist:
\`\`\`cypher
MATCH (a:User)-[r]->(b:User) RETURN r  -- ✅ Works
\`\`\`

---

### 2. Cross-Table Patterns with Disconnected Variables (GitHub Issue #12)

**Status**: ✅ FIXED for shared variable patterns (December 2025)  
**Severity**: LOW (workaround available)  
**Updated**: December 2025

**✅ Working Pattern (Shared Variable - RECOMMENDED)**:
```cypher
-- Use shared node variable to connect patterns
MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain), (src)-[:CONNECTED_TO]->(dest:IP)
RETURN src.ip AS source, d.name AS domain, dest.ip AS dest_ip LIMIT 5
```

Generated SQL (correct):
```sql
SELECT conn."id.orig_h" AS source, dns.query AS domain, conn."id.resp_h" AS dest_ip
FROM zeek.dns_log AS dns
INNER JOIN zeek.conn_log AS conn ON conn."id.orig_h" = dns."id.orig_h"
LIMIT 5
```

**Key Fix**: 
- Edge-defined `from_node_properties` and `to_node_properties` now correctly determine column mappings
- Node aliases are correctly remapped to edge table aliases in SELECT clause
- Property mapping now uses plan traversal to find owning edge BEFORE graph_join_inference

**✅ FIXED (December 5, 2025) - Disconnected Patterns with Fully Denormalized Edges**:
```cypher
-- Pattern 1: WITH ... MATCH with disconnected patterns
MATCH (ip1:IP)-[:DNS_REQUESTED]->(d:Domain) 
WITH ip1, d 
MATCH (ip2:IP)-[:CONNECTED_TO]->(dest:IP) 
WHERE ip1.ip = ip2.ip 
RETURN ip1.ip, d.name, dest.ip
```

Generated SQL (now correct):
```sql
SELECT ip1."id.orig_h" AS "ip1.ip", ip1.query AS "d.name", ip2."id.resp_h" AS "dest.ip"
FROM zeek.dns_log AS ip1
INNER JOIN zeek.conn_log AS ip2 ON ip1."id.orig_h" = ip2."id.orig_h"
LIMIT 5
```

**Key Fixes Applied**:
1. **Projection recursion in `build_graph_joins`**: Now recursively processes children before wrapping with GraphJoins
2. **Cross-table JOIN for fully denormalized CartesianProduct**: When both sides are fully denormalized (0 JOINs each), we create a JOIN using the `join_condition` extracted by `CartesianJoinExtraction`
3. **`extract_right_table_from_plan` helper**: Extracts table name and alias from CartesianProduct's right side for JOIN creation

**Files Modified**:
- `src/query_planner/analyzer/graph_join_inference.rs` - Projection recursion + CartesianProduct cross-table JOIN logic
- `src/query_planner/analyzer/filter_tagging.rs` - Added CartesianProduct support to 5 property resolution functions

**✅ Simple CONNECTED_TO pattern now works** (verified December 7, 2025):
```cypher
MATCH (src:IP)-[:CONNECTED_TO]->(dest:IP) RETURN src.ip, dest.ip
```
Correctly generates: `SELECT ... FROM zeek.conn_log AS ...`

---

## Previously Documented Issues (for historical reference)
2. **Unified join collection**: Modify `extract_joins()` to recursively collect all joins from CartesianProduct children
3. **Two-phase rendering**: Render each side separately, then combine in final SQL

**Recommendation**: Use the shared variable pattern (Form 3) which correctly generates edge-to-edge JOINs. This is semantically equivalent and more explicit about the relationship between patterns.

**Example Schema** (`schemas/examples/zeek_merged.yaml`)

---

## Recently Fixed (v0.5.4 - December 5, 2025)

| Issue | Description | Fix |
|-------|-------------|-----|
| OPTIONAL MATCH polymorphic edges | Invalid SQL with undefined aliases | Unified anchor detection in \`graph_join_inference.rs\` |
| WITH + node ref + aggregate | Wrong FROM table | Exhaustive match in \`find_table_name_for_alias()\` |
| Polymorphic CONTAINS edge | Untyped target node failed | Label inference from relationship schema |
| Multi-hop anonymous nodes | Missing JOINs in denormalized schemas | Pre-assigned consistent aliases for shared nodes |
| STARTS WITH/ENDS WITH/CONTAINS | String predicates not parsed | Added operators and SQL generation |
| Anonymous VLP wrong table | CTE used node table instead of edge | Relationship type filtering in CTE extraction |
| Denormalized edge JOIN | Swapped from_id/to_id columns | Fixed FK-edge JOIN generation |
| Duplicate relationship types | \`[:A|A]\` generated invalid SQL | Deduplication in multiple locations |
| VLP + chained patterns | Missing table JOINs | Recursive VLP detection in nested GraphRels |
| VLP endpoint filters | WHERE on chained nodes not applied | Extract all filters for final WHERE clause |

---

## Test Statistics

| Category | Passing | Total | Rate |
|----------|---------|-------|------|
| Unit Tests | 577 | 577 | 100% |
| Integration (Standard) | 48 | 51 | 94% |
| Security Graph | 98 | 98 | 100% |

**Note**: 3 integration test failures are expected (benchmark-specific tests requiring specific datasets).
