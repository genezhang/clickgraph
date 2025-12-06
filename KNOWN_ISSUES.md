# Known Issues

**Active Issues**: 3  
**Test Results**: 578/578 unit tests passing (100%)  
**Integration Tests**: 48/51 passing for STANDARD schema (94%)  
**Security Graph Tests**: 98/98 passing (100%)  
**Last Updated**: December 5, 2025

For recently fixed issues, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

> **Note**: Issue #2 (Cross-Table Patterns) is a fundamental limitation that affects cross-table analytics use cases. See workaround below.

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

### TODO-3: Cross-table WITH correlation missing JOIN
**Priority**: HIGH  
**Status**: INVESTIGATED - Architectural Change Needed

**Query**:
```cypher
MATCH (src:IP)-[r:DNS_REQUESTED]->(d:Domain) 
WITH src.ip AS source_ip, d.name AS domain 
MATCH (src2:IP)-[c:CONNECTED_TO]->(dest:IP) 
WHERE src2.ip = source_ip 
RETURN source_ip, domain, dest.ip
```
**Expected**: JOIN dns_log and conn_log with correlation  
**Actual Generated**:
```sql
SELECT source_ip, domain, c."id.resp_h"
FROM zeek.dns_log AS r
WHERE c."id.orig_h" = source_ip
```
**Problem**: The `c` alias (conn_log table) is referenced in SELECT and WHERE but not JOINed.

**Root Cause Analysis** (Dec 5, 2025):
- Two independent graph patterns connected only by WHERE clause correlation
- CartesianProduct logic bubbles up joins from both branches but doesn't JOIN them to each other
- The correlation (`WHERE src2.ip = source_ip`) is treated as a filter, not a join condition
- The second table (conn_log) needs to be explicitly JOINed or used in a subquery

**Potential Fixes**:
1. **Subquery approach**: Render first WITH clause as a subquery, JOIN second pattern to it
2. **CROSS JOIN approach**: Generate explicit CROSS JOIN between tables with correlation in WHERE
3. **CTE approach**: Use WITH clause (SQL) to materialize first result, join to it

**Workaround**: Use shared variable pattern instead of WITH + correlation:
```cypher
-- This works because src is shared between patterns
MATCH (src:IP)-[:DNS_REQUESTED]->(d:Domain), (src)-[:CONNECTED_TO]->(dest:IP)
RETURN src.ip, d.name, dest.ip
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

**⚠️ Remaining Issue (Separate Problem)**:
```cypher
-- Single CONNECTED_TO pattern uses wrong table (dns_log instead of conn_log)
MATCH (src:IP)-[:CONNECTED_TO]->(dest:IP) RETURN src.ip, dest.ip
```
This is a pre-existing node table resolution issue where the IP node's default table (dns_log) is used instead of the edge's table (conn_log). Not related to the cross-table fix.

---

## Previously Documented Issues (for historical reference)
2. **Unified join collection**: Modify `extract_joins()` to recursively collect all joins from CartesianProduct children
3. **Two-phase rendering**: Render each side separately, then combine in final SQL

**Recommendation**: Use the shared variable pattern (Form 3) which correctly generates edge-to-edge JOINs. This is semantically equivalent and more explicit about the relationship between patterns.

**Example Schema** (`schemas/examples/zeek_unified.yaml`):
```yaml
nodes:
  - label: IP
    primary_table: zeek.dns_log
    primary_key: "id.orig_h"
    properties:
      ip: "id.orig_h"
  - label: Domain
    primary_table: zeek.dns_log
    primary_key: query
    properties:
      name: query

relationships:
  - type: DNS_REQUESTED
    table: zeek.dns_log
    from_node: IP
    to_node: Domain
    from_field: "id.orig_h"
    to_field: query
    from_node_properties:
      ip: "id.orig_h"
    to_node_properties:
      name: query
  - type: CONNECTED_TO
    table: zeek.conn_log
    from_node: IP
    to_node: IP
    from_field: "id.orig_h"
    to_field: "id.resp_h"
    from_node_properties:
      ip: "id.orig_h"
    to_node_properties:
      ip: "id.resp_h"
```

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
