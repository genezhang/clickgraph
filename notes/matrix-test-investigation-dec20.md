# Matrix Test Investigation - December 20, 2025

## Executive Summary

**Status**: Root cause identified for 565/762 failures (74%)  
**Issue**: Denormalized schema SQL generation bug  
**Impact**: zeek_merged, ontime_benchmark, filesystem, group_membership schemas all affected

## Problem Description

### Test Failure Pattern
```
MATCH (a:IP)-[r:DNS_REQUESTED]->(b) RETURN a, r, b LIMIT 10

❌ Error: Unknown expression identifier `r.id.orig_h` in scope 
SELECT r.`id.orig_h` AS a_ip, ... FROM zeek.dns_log AS b
```

**Key observation**: FROM clause uses alias `b` (node), but SELECT uses alias `r` (relationship) → undefined identifier

### Root Cause

**Denormalized Schema Design**:
```yaml
# zeek_merged.yaml - IP node embedded in dns_log table
- label: IP
  table: dns_log
  from_node_properties:    # Node properties stored in edge table
    ip: "id.orig_h"        # Property with dot in column name!
  to_node_properties:
    ip: "id.resp_h"
```

**What SHOULD happen**:
1. dns_log table aliased as `r` (the relationship)
2. Node `a` properties come from `r.from_node_properties` → `r."id.orig_h"`
3. Node `b` properties come from `r.to_node_properties` → `r.query`

**What ACTUALLY happens**:
1. dns_log table aliased as `b` (the TO node)
2. SELECT tries to access `r."id.orig_h"` → `r` doesn't exist in FROM clause!

## Technical Details

### Schema Support Status
- ✅ **Query Planning**: Correctly sets `from_node_properties` and `to_node_properties` on ViewScan
  - File: `src/query_planner/logical_plan/match_clause.rs::try_generate_relationship_view_scan()`
  - Lines 927-960 populate the fields correctly

- ❌ **SQL Generation**: Uses wrong table alias in FROM clause
  - File: `src/render_plan/plan_builder.rs` (suspected location)
  - FROM clause generation doesn't account for denormalized node access pattern

### Additional Complication

Column names with dots (`id.orig_h`) require special quoting:
- ClickHouse requires: `"id.orig_h"` (double quotes)
- Our code does handle this via `needs_quoting()` in `expression_parser.rs`
- But the alias problem must be fixed first

## Affected Schemas

1. **zeek_merged** (74 tests): Multi-table denormalized (IP in dns_log + conn_log)
2. **ontime_benchmark** (est. 100+ tests): Denormalized Airport nodes in flights table
3. **filesystem** (est. 50+ tests): FK-edge with self-referencing denormalized nodes
4. **group_membership** (est. 50+ tests): Polymorphic edges with denormalized nodes

**Total impact**: ~565 of 762 failures (74%)

## Fix Strategy

### Phase 1: Understand Current SQL Generation
1. Trace how `LogicalPlan::GraphRel` with ViewScan generates FROM clause
2. Identify where table alias is determined
3. Find where `from_node_properties` should influence alias choice

### Phase 2: Implement Fix
Key insight: When `ViewScan.from_node_properties` or `to_node_properties` is set:
- The **relationship table IS the primary data source**
- Node properties are **accessed through the relationship alias**
- No separate node table JOINs needed

Likely changes needed:
- `render_plan/plan_builder.rs`: FROM clause generation logic
- `render_plan/cte_generation.rs`: Relationship traversal SQL
- Property expansion: Use relationship alias when denormalized

### Phase 3: Test & Validate
1. Start with simple zeek_merged query
2. Expand to ontime_benchmark (denormalized edges)
3. Run full matrix suite

## Expected Impact

**Before**: 2581/3363 tests passing (76.7%)  
**After**: ~3000+/3363 tests passing (89%+) - fixing 500+ denormalized schema tests

**Remaining failures** (after fix):
- Variable-length paths (26)
- Shortest paths (45)
- Optional match edge cases (27)
- Domain-specific tests (79)
- Misc (24)

Total: ~200 remaining failures (6% of suite)

## Next Steps

1. ✅ Investigation complete - documented findings
2. ⏳ Trace SQL generation for denormalized schemas
3. ⏳ Implement fix in render_plan/plan_builder.rs
4. ⏳ Test with zeek_merged queries
5. ⏳ Run full matrix suite validation

## References

- Investigation plan: `TEST_FAILURE_INVESTIGATION_PLAN.md`
- Zeek schema: `schemas/examples/zeek_merged.yaml`
- Test file: `tests/integration/matrix/test_comprehensive.py`
- Query planner: `src/query_planner/logical_plan/match_clause.rs:828-979`
- Property resolver: `src/query_planner/analyzer/view_resolver.rs:68-145`
