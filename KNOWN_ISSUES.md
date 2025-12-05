# Known Issues

**Active Issues**: 2  
**Test Results**: 577/577 unit tests passing (100%)  
**Integration Tests**: 48/51 passing for STANDARD schema (94%)  
**Security Graph Tests**: 98/98 passing (100%)  
**Last Updated**: December 5, 2025

For recently fixed issues, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

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

### 2. Disconnected Patterns Generate Invalid SQL

**Status**: 🐛 Bug  
**Severity**: MEDIUM  
**Difficulty**: Easy  
**Estimated Fix Time**: 1-2 days

**Problem**: Comma-separated patterns without shared nodes generate invalid SQL:
\`\`\`cypher
MATCH (user:User), (other:User) WHERE user.user_id = 1 RETURN other.user_id
\`\`\`

**Current**: Generates SQL referencing \`user\` not in FROM clause → ClickHouse error  
**Expected**: Either throw \`DisconnectedPatternFound\` error OR generate CROSS JOIN

**Location**: \`src/query_planner/logical_plan/match_clause.rs\` - disconnection check not triggering

**Workaround**: Use connected patterns or explicit joins:
\`\`\`cypher
-- Option 1: Use connected pattern
MATCH (user:User)-[:KNOWS]->(other:User) WHERE user.user_id = 1 RETURN other.user_id

-- Option 2: Use subquery (if supported)
MATCH (user:User) WHERE user.user_id = 1
WITH user
MATCH (other:User) WHERE other.user_id <> user.user_id
RETURN other.user_id
\`\`\`

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
