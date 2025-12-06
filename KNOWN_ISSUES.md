# Known Issues

**Active Issues**: 2  
**Test Results**: 577/577 unit tests passing (100%)  
**Integration Tests**: 48/51 passing for STANDARD schema (94%)  
**Security Graph Tests**: 98/98 passing (100%)  
**Last Updated**: December 5, 2025

For recently fixed issues, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

> **Note**: Issue #2 (Cross-Table Patterns) is a fundamental limitation that affects cross-table analytics use cases. See workaround below.

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
