# Denormalized Edge Table - Integration Test Findings

**Date**: November 23, 2025  
**Status**: ⚠️ Feature Partially Implemented

## Summary

Integration testing for denormalized edge tables revealed that while **property mapping works correctly**, **SQL generation still creates unnecessary JOINs** when node and edge share the same table.

## Test Results

### ✅ Unit Tests (Property Mapping Level)
- **Status**: 7/7 tests passing (including new comprehensive test)
- **Location**: `src/render_plan/tests/denormalized_property_tests.rs`
- **What Works**: `map_property_to_column_with_relationship_context()` correctly:
  - Maps node properties through `from_node_properties`/`to_node_properties`
  - Falls back to node property mappings when denormalized version doesn't exist
  - Handles same table for node and edge

### ❌ SQL Generation (End-to-End)
- **Status**: 3/18 integration tests passing (16.7%)
- **Location**: `tests/integration/test_denormalized_edges.py`
- **What Fails**: SQL generator creates JOINs even when unnecessary

## The Problem

**Query**:
```cypher
MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
RETURN f.flight_num, f.carrier
```

**Schema** (denormalized edge table pattern):
- Airport node: uses `flights` table
- FLIGHT edge: uses `flights` table (SAME table)
- Only edge properties accessed in RETURN clause

**Expected SQL** (no JOINs needed):
```sql
SELECT 
  f.flight_number AS "f.flight_num", 
  f.airline AS "f.carrier"
FROM test_integration.flights AS f
```

**Actual SQL** (incorrect - creates self-JOINs):
```sql
SELECT 
  f.flight_number AS "f.flight_num", 
  f.airline AS "f.carrier"
FROM test_integration.flights AS origin
INNER JOIN test_integration.flights AS f ON f.origin_code = origin.code  -- ❌ origin.code doesn't exist
INNER JOIN test_integration.flights AS dest ON dest.code = f.dest_code   -- ❌ dest.code doesn't exist
```

**Issues**:
1. Creates unnecessary JOINs to the same table
2. Tries to access `origin.code` and `dest.code` which don't exist (should be `origin_code`, `dest_code`)
3. Doesn't recognize that when node and edge share the same table, the pattern can be resolved without JOINs

## Root Cause

The SQL generator (`query_planner/` and `clickhouse_query_generator/`) doesn't have logic to detect and optimize the denormalized edge table pattern. It generates JOINs based on the MATCH pattern without checking:
1. Whether node and edge use the same table
2. Whether the query only accesses edge properties (no node properties)
3. Whether denormalized properties make JOINs unnecessary

## What's Needed

### Short Term (Fix SQL Generation)
1. Add detection in query planner: check if node table == edge table
2. When detected, skip JOIN generation if only edge properties are accessed
3. When node properties ARE accessed, use denormalized columns (already works via property mapping)

### Medium Term (Comprehensive Fix)
1. Add `is_denormalized_edge_table` flag to RelationshipSchema
2. Update SQL generator to handle this pattern explicitly
3. Add optimizer pass to eliminate unnecessary JOINs for denormalized patterns

## Test Infrastructure

✅ **Integration test infrastructure is complete**:
- Schema: `schemas/tests/denormalized_flights.yaml`
- Setup: `scripts/test/setup_denormalized_test_data.sql`
- Tests: `tests/integration/test_denormalized_edges.py` (18 test cases)
- All ready to validate once SQL generation is fixed

## Files

**Test Files**:
- `schemas/tests/denormalized_flights.yaml` - Correct denormalized schema
- `scripts/test/setup_denormalized_test_data.sql` - Test data (single flights table)
- `tests/integration/test_denormalized_edges.py` - 18 integration tests
- `src/render_plan/tests/denormalized_property_tests.rs` - 7 unit tests (all passing)

**Key Implementation Files**:
- `src/render_plan/cte_generation.rs` - Property mapping (✅ works)
- `src/query_planner/` - Query planning (❌ needs JOIN optimization logic)
- `src/clickhouse_query_generator/` - SQL generation (❌ needs denormalized table detection)

## Recommendation

Before investing in fixing the SQL generation:
1. ✅ **Verify** unit tests pass (property mapping works) - DONE
2. ✅ **Confirm** the pattern is correctly understood - DONE
3. ⏭️ **Prioritize** - Is denormalized edge table pattern critical for v1.0?
4. ⏭️ **Scope** - How much refactoring is needed in query planner/SQL generator?

If this feature is critical, the fix requires changes in the query planning phase to recognize and optimize the denormalized pattern. If not critical, mark as "known limitation" and defer to future release.
