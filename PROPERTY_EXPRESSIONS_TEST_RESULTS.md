# Property Expressions Test Results

**Date**: November 22, 2025  
**Version**: v0.5.2-dev  
**Test Suite**: `tests/integration/test_property_expressions.py`  
**Total Tests**: 28  
**Passed**: 7 (25%)  
**Failed**: 21 (75%)

## Executive Summary

Property expression mappings feature is **documented but non-functional** for most use cases. Testing reveals a critical SQL generation bug that prefixes ClickHouse function names with table aliases, making them invalid SQL.

## Test Results Breakdown

### ✅ PASSING (7 tests - 25%)

**Mathematical Expressions** (2/2):
- Division normalization: `score / 1000.0` ✓
- Addition with constant: `score + 100` ✓

**Boolean Expressions** (2/2):
- Boolean conversion: `score >= 1000` ✓  
- Boolean NULL check: `metadata_json != ''` ✓

**Edge Property Expressions** (3/4):
- Recent follow check: `follow_date >= today() - INTERVAL 7 DAY` ✓
- Relationship strength calculation: `interaction_count` ✓
- Strength tier CASE WHEN on edges ✓

### ❌ FAILING (21 tests - 75%)

**Root Cause**: SQL generation bug - ClickGraph incorrectly prefixes ClickHouse function names with table aliases.

**String Expressions** (3/3 failed):
- `concat()` → Generated: `u.concat()` ❌ Should be: `concat()`
- `splitByChar()` → Generated: `u.splitByChar()` ❌
- `length()` → Generated: `u.length()` ❌

**Date Expressions** (3/3 failed):
- `dateDiff()` → Generated: `u.dateDiff()` ❌
- `toDate()` → Generated: `u.toDate()` ❌

**Type Conversions** (2/2 failed):
- `toUInt8()` → Generated: `u.toUInt8()` ❌
- `toFloat64()` → Generated: `u.toFloat64()` ❌

**CASE WHEN Expressions** (4/4 failed):
- All CASE WHEN expressions fail with syntax errors
- ClickHouse rejects: `WHERE u.CASE WHEN ...`
- CASE WHEN not treated as expression, gets table prefix

**multiIf() Expressions** (4/4 failed):
- `multiIf()` → Generated: `u.multiIf()` ❌

**JSON Expressions** (1/1 failed):
- `JSONExtractString()` → Generated: `u.JSONExtractString()` ❌

**Edge Date Expression** (1/1 failed):
- `f.dateDiff()` on relationships ❌

**Complex Queries** (3/3 failed):
- All complex queries fail due to CASE WHEN and function name prefixing

## Technical Analysis

### Bug Location
**Component**: SQL generation for property access expressions  
**Likely File**: `clickhouse_query_generator/` or `render_plan/`  
**Issue**: When generating SQL for properties defined via expressions in `property_mappings`, ClickGraph treats function calls as column references and prefixes them with table aliases.

### Example Error

**Schema**:
```yaml
property_mappings:
  full_name: "concat(first_name, ' ', last_name)"
```

**Generated SQL** (WRONG):
```sql
SELECT `u.concat`(first_name, ' ', last_name) AS `u.full_name`
FROM brahmand.users_expressions_test AS u
```

**Expected SQL** (CORRECT):
```sql
SELECT concat(first_name, ' ', last_name) AS `u.full_name`
FROM brahmand.users_expressions_test AS u  
```

**ClickHouse Error**:
```
Code: 46. DB::Exception: Function with name `u.concat` does not exist.
Maybe you meant: ['concat','mapConcat'].
```

### What Works

**Simple expressions WITHOUT function calls**:
- Arithmetic: `score / 1000.0`, `score + 100` ✓
- Column comparisons: `score >= 1000` ✓
- NULL checks: `column != ''` ✓
- Direct column references ✓

**What Doesn't Work**:
- Any ClickHouse function: `concat()`, `dateDiff()`, `toDate()`, `splitByChar()`, etc. ❌
- CASE WHEN expressions ❌
- multiIf() expressions ❌
- JSON functions ❌

## Impact Assessment

**Severity**: **CRITICAL**  
**User Impact**: **HIGH**

- Documented feature is ~75% non-functional
- Only trivial expressions (arithmetic, comparisons) work
- All documented examples in `docs/wiki/Schema-Configuration-Advanced.md` that use functions would fail
- Feature appears comprehensive in docs but is severely limited in practice

## Recommendations

### Immediate Actions

1. **Update KNOWN_ISSUES.md**: Document this as a critical known limitation
2. **Update Documentation**: 
   - Add warning in `docs/wiki/Schema-Configuration-Advanced.md`
   - Clarify which expressions work vs. don't work
   - Mark function-based examples as "not yet supported"
3. **Create GitHub Issue**: Track bug fix with test suite reference

### For v0.5.2 Release

**Option A: Delay v0.5.2** (Recommended)
- Fix property expressions bug before release
- v0.5.2 becomes "fully functional schema features release"
- Estimated fix time: 1-3 days

**Option B: Release with Known Limitation**
- Document limitation prominently in release notes
- Mark property expressions as "experimental/limited"
- Release focuses on other schema features (polymorphic edges, denormalized properties)

### Fix Strategy

**Likely fixes**:
1. Detect when `property_mappings` value contains function calls
2. Don't prefix function names with table aliases
3. Parse expressions to identify functions vs columns
4. Apply alias only to bare column references within expressions

**Test validation**:
- Re-run `tests/integration/test_property_expressions.py`
- Target: 28/28 tests passing before release

## Test Artifacts

**Schema**: `tests/fixtures/schemas/test_property_expressions.yaml`  
**Data**: `tests/fixtures/data/setup_property_expressions.sql` (12 users, 10 follows)  
**Tests**: `tests/integration/test_property_expressions.py` (28 tests)

## Conclusion

Property expression mappings are a **documented but critically broken feature**. The 28-test suite successfully validates the approach and identifies the bug. Fix is required before claiming v0.5.2 as a "schema features release" with property expressions support.

**Next Steps**:
1. Document findings in KNOWN_ISSUES.md
2. Create GitHub issue with test results
3. Decide: fix before release or document limitation
4. Update STATUS.md with accurate feature status
