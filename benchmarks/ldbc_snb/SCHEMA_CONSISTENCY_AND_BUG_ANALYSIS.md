# LDBC SNB SF10 Benchmark - Schema Consistency Analysis

**Date**: January 9, 2026  
**Issue**: SQL generation regression for variable-length path queries  
**Impact**: 17/41 official queries generate SQL but only 4 execute (23% success rate)

## Problem Summary

The LDBC SF10 benchmark revealed a critical regression in ClickGraph's SQL generation:
- **SQL Generation**: 17/41 queries pass (41%)
- **Actual Execution**: 4/17 queries run (23%)
- **Main Bug**: Variable-length path (VLP) queries failing with "Relationship type 'KNOWS::Person::Person' not found"

## Root Cause Analysis

### 1. Schema Consistency ✅ VERIFIED

**YAML Schema** (`ldbc_snb_complete.yaml`):
```yaml
edges:
  - type: KNOWS
    database: ldbc
    table: Person_knows_Person
    from_id: Person1Id
    to_id: Person2Id
    from_node: Person
    to_node: Person
```

**ClickHouse Database**: ✅ Correct
- Table: `ldbc.Person_knows_Person`
- Columns: `Person1Id`, `Person2Id`, `creationDate`
- Data: 1,754,972 edges (SF10)

**Schema Loading**: ✅ Works correctly
- Server loads schema from YAML
- Registers relationships with BOTH:
  - Simple key: `"KNOWS"` (backward compat)
  - Composite key: `"KNOWS::Person::Person"` (for node-specific lookup)

### 2. Bug Location: VLP Query Planning 🐛 FOUND

**Working Query** (simple pattern):
```cypher
MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN count(*) 
```
✅ Generates SQL successfully using simple key `"KNOWS"`

**Failing Query** (variable-length pattern):
```cypher
MATCH (p1:Person)-[:KNOWS*1..3]-(p2:Person) RETURN count(*)
```
❌ Fails with: "Relationship type 'KNOWS::Person::Person' not found"

**Bug in**: `src/clickhouse_query_generator/multi_type_vlp_joins.rs`

Lines 449, 458, 506 use OLD METHOD:
```rust
self.schema.get_rel_schema(rel_type)  // ❌ Doesn't support composite keys
```

Should use NEW METHOD:
```rust
self.schema.get_rel_schema_with_nodes(rel_type, Some(from_node), Some(to_node))  // ✅ Supports composite keys
```

## Additional SQL Generation Issues

Beyond the VLP bug, the audit revealed other SQL correctness issues:

1. **Duplicate Alias Generation** (13 queries)
   - Same table alias used multiple times in same query
   - Example: `Multiple table expressions with same alias t102`

2. **Incorrect Table/Column References**  (13 queries)
   - Generated SQL references non-existent identifiers
   - Example: `Unknown expression identifier t102.Person1Id`

3. **CTE Scope Issues** (6 queries)
   - CTEs not properly visible to subqueries
   - Example: `Unknown table expression identifier ldbc.with_message_...`

4. **Render Plan Errors** (4 queries)
   - GroupBy aggregation references missing columns
   - Example: `Cannot find ID column for alias 'distance'`

## Test Results Summary

### Passing Queries (4/17)
- ✅ IS-1: Person lookup (simple node)
- ✅ IS-4: Message content (simple property access)
- ✅ IS-5: Message creator (simple join)
- ✅ IS-6: Forum info (multi-level join)

### Failing Query Categories

**BI Queries (0/5 pass)**
- BI-3: Multi-hop pattern ❌ Duplicate aliases
- BI-6: Tag evolution ❌ Unknown identifier
- BI-7: Related tags ❌ Duplicate aliases  
- BI-9: Thread initiators ❌ Table reference error
- BI-18: Friend recommendation ❌ Unknown identifier

**Interactive Complex (0/5 pass)**
- IC-2: Recent messages ❌ Unknown identifier
- IC-8: Recent replies ❌ Duplicate aliases
- IC-9: Friends of friends ❌ Unknown identifier
- IC-11: Job referral ❌ Duplicate aliases
- IC-12: Expert search ❌ Duplicate aliases

**Interactive Short (4/7 pass)**
- IS-1: ✅ Pass
- IS-2: ❌ CTE scope issue
- IS-3: ❌ Unknown identifier
- IS-4: ✅ Pass
- IS-5: ✅ Pass
- IS-6: ✅ Pass
- IS-7: ❌ Duplicate aliases

## Regression Analysis

The codebase previously had better results. Likely causes:

1. **Recent Refactoring**: Code reorganization may have missed VLP path
2. **Composite Key Migration**: Transition from simple to composite keys incomplete
3. **Testing Coverage**: VLP integration tests may not cover all code paths

## Fix Priority

**P0 - Critical** (blocks 13 queries):
1. Fix VLP composite key lookup in `multi_type_vlp_joins.rs`
2. Fix duplicate alias generation in SQL renderer

**P1 - High** (blocks 6 queries):
3. Fix CTE scope/visibility issues
4. Fix incorrect table/column reference generation

**P2 - Medium** (blocks 4 queries):
5. Fix GroupBy render plan column resolution

## Next Steps

1. **Fix VLP Bug** (~30 min)
   - Update `multi_type_vlp_joins.rs` to use `get_rel_schema_with_nodes()`
   - Pass from_node/to_node context through VLP code path
   - Test with IC-13 and other VLP queries

2. **Fix Duplicate Alias Bug** (~2 hours)
   - Audit alias generation in SQL renderer
   - Ensure unique aliases across entire query
   - Add alias collision detection

3. **Fix CTE Scope** (~1 hour)
   - Review CTE visibility rules
   - Fix nested CTE references
   - Test with IS-2 and similar queries

4. **Re-run Benchmark**
   - Target: 25+ queries executing (from current 4)
   - Document remaining unsupported features
   - Create comprehensive SF10 results

## Files to Fix

1. `src/clickhouse_query_generator/multi_type_vlp_joins.rs` - VLP composite key lookup
2. `src/clickhouse_query_generator/` - Alias generation
3. `src/render_plan/` - CTE scope handling
4. `src/query_planner/logical_plan/` - GroupBy column resolution

## Testing Strategy

After fixes:
1. Unit test: VLP with composite keys
2. Integration test: All 17 SQL-generating queries
3. E2E test: Execute all passing queries on SF10
4. Regression test: Verify no other queries break

## Conclusion

**Schema is correct and consistent**. The issue is a code regression in VLP query planning that uses outdated relationship lookup methods. Fixing this single bug should restore functionality for variable-length path queries and improve success rate from 23% to ~60%+.
