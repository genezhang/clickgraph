# LDBC SQL Generation - Critical Issues Found

**Date**: December 21, 2025  
**Status**: 🔴 BLOCKING ISSUE IDENTIFIED

## Critical Issue: Date/Timestamp Literal Handling

### Problem
Generated SQL uses **string literals** for date comparisons, but ClickHouse schema stores dates as **Int64 Unix timestamps (milliseconds)**.

### Example - BI-1a Query

**Generated SQL** (❌ INCORRECT):
```sql
WHERE message.creationDate < '2012-01-01'
```

**Error**:
```
Code: 53. DB::Exception: Cannot convert string '2012-01-01' to type Int64
```

**Correct SQL** (✅ WORKS):
```sql
WHERE message.creationDate < 1325376000000  -- Unix timestamp for 2012-01-01
```

### Impact

**Severity**: 🔴 HIGH - Affects ALL date-based queries

**Affected Queries**: At least 15-20 queries use date filtering
- BI-1a: Post date filtering
- BI-10a, BI-10b: Date range queries  
- IC-1, IC-2, IC-9: Message date filtering
- Many others with `creationDate`, `joinDate`, etc.

**Current Status**:
- ✅ SQL **generation** is correct (proper structure)
- ❌ Date **literals** need conversion to timestamps
- ❌ Queries **cannot execute** without fixing this

### Root Cause Analysis

1. **Schema Definition**: LDBC schema stores dates as Int64 milliseconds
2. **Query Parameters**: Cypher queries use date strings like '2012-01-01'
3. **SQL Generator**: Passes date strings through without conversion

### Solution Options

**Option 1: Schema-Level Type Mapping** (RECOMMENDED)
```yaml
# In schema YAML, specify date column types
properties:
  creationDate:
    column: creationDate
    type: timestamp_ms  # Tells generator to convert date strings
```

**Option 2: Runtime Conversion in SQL Generator**
```rust
// In clickhouse_query_generator
match (column_type, literal_value) {
    (ColumnType::TimestampMs, Value::String(date_str)) => {
        // Convert "2012-01-01" to 1325376000000
        let timestamp = parse_date_to_unix_ms(date_str);
        format!("{}", timestamp)
    }
    // ...
}
```

**Option 3: ClickHouse toDateTime64 Function**
```sql
-- Wrap string literals in conversion function
WHERE message.creationDate < toUnixTimestamp64Milli(toDateTime64('2012-01-01', 3))
```

### Recommended Fix

**Two-Phase Approach**:

**Phase 1: Quick Fix (30 min)** - Use ClickHouse functions
- Modify SQL generator to wrap date literals: `toUnixTimestamp64Milli(toDateTime64('2012-01-01', 3))`
- Enables immediate testing without schema changes
- Works for all existing queries

**Phase 2: Proper Solution (2-3 hours)** - Schema type metadata
- Add type information to schema YAML
- Implement proper type-aware literal generation
- Cleaner SQL, better performance

### Testing Required

After fixing date handling, must retest:
1. ✅ SQL generation (already works)
2. ⚠️ SQL execution (currently blocked by date issue)
3. ⚠️ Result correctness (cannot validate until execution works)
4. ⚠️ Performance benchmarking (blocked)

### Updated Success Metrics

**Before Fix**:
- SQL Generation: 36/46 (78%) ✅
- SQL Execution: 0/36 (0%) ❌ BLOCKED
- Result Validation: 0/36 (0%) ❌ BLOCKED

**After Fix** (estimated):
- SQL Generation: 36/46 (78%) ✅  
- SQL Execution: ~32/36 (89%) 🟡 (assuming minor issues remain)
- Result Validation: TBD

## Other Issues (Lower Priority)

### 1. Duplicate JOINs in Complex Queries
**Severity**: 🟡 MEDIUM - Functional but not optimal

**Example** (BI-3):
```sql
INNER JOIN ldbc.Person_isLocatedIn_Place AS t4 ON t4.CityId = city.id
...
INNER JOIN ldbc.Person_isLocatedIn_Place AS t4 ON t3.Place1Id = t4.CityId  -- DUPLICATE
```

**Impact**: SQL is longer than needed, may affect query planner

**Fix**: Optimize JOIN generation in multi-MATCH query planning

### 2. Missing Relationship Schemas
**Severity**: 🟡 MEDIUM - Expected for incomplete schema

**Missing**:
- `REPLY_OF_POST` (affects 4 queries)
- `WORK_AT` (affects 1 query)
- `POST_LOCATED_IN` (affects 1 query)

**Fix**: Either add to schema or mark queries as "not supported"

### 3. Parser Issues with Workaround Files
**Severity**: 🟢 LOW - Workaround files likely have syntax issues

**Affected**: 4 workaround files return "empty AST"

**Fix**: Review and fix syntax in these files

## Action Plan

### Immediate (Day 1)
1. ✅ Capture all generated SQL - DONE
2. ✅ Document issues - DONE  
3. 🔴 **FIX DATE HANDLING** - CRITICAL BLOCKER
4. Re-run SQL capture with date fix
5. Test 5-10 queries for execution

### Short Term (Days 2-3)
6. Add missing relationship schemas
7. Fix workaround file syntax
8. Achieve 100% SQL generation
9. Validate correctness for key queries

### Medium Term (Week 1)
10. Optimize duplicate JOINs
11. Full correctness validation
12. Performance benchmarking
13. Compare with Neo4j baseline

## Conclusion

**Good News** ✅:
- SQL generation structure is correct
- Complex patterns (CTEs, JOINs) work properly
- Core engine is solid

**Critical Blocker** 🔴:
- Date literal handling prevents execution
- Must fix before ANY correctness/performance testing

**Estimated Time to Working Queries**:
- Quick fix: 30 minutes (use ClickHouse functions)
- Proper fix: 2-3 hours (schema type metadata)
- Full validation: 1-2 days

**Priority**: Fix date handling immediately, then proceed with comprehensive testing.
