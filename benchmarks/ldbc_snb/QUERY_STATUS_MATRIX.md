# LDBC SNB Query Status Matrix

**Goal**: Categorize all 41 LDBC queries for v0.6.1 release

**Updated**: January 11, 2026

## Classification System

- **Class 1** ✅ - Official query works as-is (benchmarkable)
- **Class 2** ⚠️ - Equivalent adaptation needed (workaround for known bug)
- **Class 3** 🔧 - Functional variation (cannot implement exact semantics)
- **Class X** ❌ - Not working (blocked by missing features)

## Interactive Short (IS) - 7 queries

| Query | Status | Class | Notes |
|-------|--------|-------|-------|
| IS-1 | ✅ Pass | 1 | Person profile |
| IS-2 | ✅ Pass | 1 | Recent messages |
| IS-3 | ✅ Pass | 1 | Friends |
| IS-4 | ⚠️ Missing | - | No file in dataset |
| IS-5 | ✅ Pass | 1 | Message creator |
| IS-6 | ⚠️ Missing | - | No file in dataset |
| IS-7 | ⚠️ Adapted | 2 | OPTIONAL MATCH + inline property bug |

**Summary**: 4/5 Class 1, 1 Class 2, 2 missing files

## Interactive Complex (IC) - 14 queries

| Query | Status | Class | Notes |
|-------|--------|-------|-------|
| IC-1 | 🔄 Test | - | Need to test |
| IC-2 | ✅ Pass | 1 | Recent messages by friends |
| IC-3 | 🔄 Test | - | Friends in X-Y countries |
| IC-4 | 🔄 Test | - | Need to test |
| IC-5 | 🔄 Test | - | New groups |
| IC-6 | ✅ Pass | 1 | Tag co-occurrence |
| IC-7 | 🔄 Test | - | Need to test |
| IC-8 | 🔄 Test | - | Recent replies |
| IC-9 | ❌ Fail | - | CTE column naming bug (WITH DISTINCT) |
| IC-10 | 🔄 Test | - | Need to test |
| IC-11 | 🔄 Test | - | Job referral |
| IC-12 | ✅ Pass | 1 | Expert search |
| IC-13 | 🔄 Test | - | Shortest paths |
| IC-14 | 🔄 Test | - | Weighted paths |

**Summary**: 3 Class 1 confirmed, 1 known failure, 10 need testing

## Business Intelligence (BI) - 20 queries

| Query | Status | Class | Notes |
|-------|--------|-------|-------|
| BI-1 | 🔄 Test | - | Now parses (// comment fix) |
| BI-2 | 🔄 Test | - | Need to test |
| BI-3 | 🔄 Test | - | Popular topics in country |
| BI-4 | 🔄 Test | - | Top message creators |
| BI-5 | 🔄 Test | - | Need to test |
| BI-6 | 🔄 Test | - | Most active posters |
| BI-7 | 🔄 Test | - | Related topics |
| BI-8 | 🔄 Test | - | Need to test |
| BI-9 | 🔄 Test | - | Top thread initiators |
| BI-10 | 🔄 Test | - | Need to test |
| BI-11 | 🔄 Test | - | Friend triangles |
| BI-12 | 🔄 Test | - | Trending posts |
| BI-13 | 🔄 Test | - | Now parses (// comment fix) |
| BI-14 | 🔄 Test | - | Need to test |
| BI-15 | 🔄 Test | - | Need to test |
| BI-16 | 🔄 Test | - | Need to test |
| BI-17 | 🔄 Test | - | Need to test |
| BI-18 | 🔄 Test | - | Friend recommendation |
| BI-19 | 🔄 Test | - | Need to test |
| BI-20 | 🔄 Test | - | Need to test |

**Summary**: All need systematic testing (BI-1, BI-13 should improve with parser fixes)

## Overall Status

**Total Queries**: 41
- **Tested**: 7/41 (17%)
- **Class 1 (Official)**: 4
- **Class 2 (Adapted)**: 1
- **Class 3 (Variation)**: 0
- **Class X (Blocked)**: 1 (IC-9)
- **Untested**: 34

## Priority Order for Testing

### Phase 1: Quick Wins (Fix → Re-test)
1. **Fix IC-9** - CTE column naming
2. **Fix IS-7** - OPTIONAL MATCH + inline property
3. **Re-test** IS-7, IC-9 → move to Class 1

### Phase 2: Systematic IC Testing
Test all IC queries (IC-1, IC-3, IC-4, IC-5, IC-7, IC-8, IC-10, IC-11, IC-13, IC-14)
- Likely candidates for Class 1: IC-5, IC-8, IC-11, IC-13, IC-14
- Monitor for patterns in failures

### Phase 3: Systematic BI Testing
Test all BI queries (BI-1 through BI-20)
- BI-1, BI-13 should work after parser fixes
- Monitor for WITH clause patterns
- Expect some Class 3 variations needed

### Phase 4: Classification & Documentation
1. Finalize Class 1/2/3/X for each query
2. Create adaptations for Class 2
3. Create variations for Class 3
4. Document all differences

## Known Blockers

### Parser/Generator Issues
- **IC-9**: CTE column naming (WITH DISTINCT uses underscores)
- **IS-7**: OPTIONAL MATCH + inline property
- **Scalar aggregates**: WITH + GROUP BY architecture limitation

### Expected Challenges
- Complex WITH clause patterns (multiple BI queries)
- Nested subqueries
- CASE expressions (not yet implemented)
- Some path aggregations

## Target for v0.6.1 Release

**Realistic Goal**: 
- 20-25/41 Class 1+2 (50-60% official/equivalent pass rate)
- 5-10 Class 3 (variations)
- 5-10 Class X (documented limitations)

**Stretch Goal**:
- 30/41 Class 1+2 (70% pass rate)
- 5 Class 3
- 6 Class X

## Testing Script

```bash
#!/bin/bash
# Test all LDBC queries and update matrix

for query in benchmarks/ldbc_snb/queries/official/interactive/short-*.cypher; do
    echo "Testing $query..."
    # Run query, capture result
done

for query in benchmarks/ldbc_snb/queries/official/interactive/complex-*.cypher; do
    echo "Testing $query..."
    # Run query, capture result
done

for query in benchmarks/ldbc_snb/queries/official/bi/*.cypher; do
    echo "Testing $query..."
    # Run query, capture result
done
```

## Next Actions

1. ✅ Create this tracking matrix
2. 🔄 Fix IC-9 CTE column naming issue
3. 🔄 Fix IS-7 OPTIONAL MATCH + inline property bug
4. 🔄 Create systematic test script
5. 🔄 Run all 41 queries and update matrix
6. 🔄 Classify into Class 1/2/3/X
7. 🔄 Create adaptations/variations as needed
8. 🔄 Generate benchmark results
9. 🔄 Release v0.6.1
