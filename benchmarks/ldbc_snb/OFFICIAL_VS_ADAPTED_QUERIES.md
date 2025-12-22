# LDBC Official vs Adapted Queries - Analysis

**Date**: December 21, 2025  
**Purpose**: Identify which queries should be used for official benchmarking

## Summary

**Official LDBC Queries**: 41 total (20 BI + 21 Interactive)  
**SQL Generation Success**: 25/41 (61%)  
**Ready for Benchmarking**: 25 queries

**Adapted/Custom Queries**: ~42 (from bi-queries-adapted.cypher)  
**Purpose**: Testing, development, not for official benchmarks

## Official Queries - Passing (Use These for Benchmarking)

### Business Intelligence (BI): 9/20 ✅

**Passing**:
- ✅ BI-1: Posting summary (324 chars)
- ✅ BI-3: Popular topics in country (2451 chars)
- ✅ BI-4: Top message creators (1670 chars)
- ✅ BI-6: Most active posters (1223 chars)
- ✅ BI-7: Related topics (792 chars)
- ✅ BI-9: Top thread initiators (2017 chars)
- ✅ BI-11: Friend triangles (10010 chars!)
- ✅ BI-12: Trending posts (2146 chars)
- ✅ BI-18: Friend recommendation (3573 chars)

**Failing** (11 queries):
- ❌ BI-2, BI-5: WITH clause aggregation issues
- ❌ BI-8, BI-10, BI-15, BI-17, BI-19, BI-20: Invalid SQL (likely unsupported features)
- ❌ BI-13, BI-16: Property not found (schema mismatch)
- ❌ BI-14: WITH clause validation error

### Interactive Complex (IC): 9/14 ✅

**Passing**:
- ✅ IC-2: Recent messages by friends (1611 chars)
- ✅ IC-3: Friends in X-Y countries (2107 chars)
- ✅ IC-5: New groups (6587 chars)
- ✅ IC-6: Tag co-occurrence (2976 chars)
- ✅ IC-8: Recent replies (962 chars)
- ✅ IC-11: Job referral (5764 chars)
- ✅ IC-12: Expert search (929 chars)
- ✅ IC-13: Shortest paths (3331 chars)
- ✅ IC-14: Weighted paths (5969 chars)

**Failing** (5 queries):
- ❌ IC-1, IC-4, IC-10: WITH clause aggregation issues
- ❌ IC-7: Property 'likeTime' not found
- ❌ IC-9: Property 'id' not found

### Interactive Short (IS): 7/7 ✅

**All Passing**:
- ✅ IS-1: Person profile (467 chars)
- ✅ IS-2: Recent messages (2520 chars)
- ✅ IS-3: Friends (749 chars)
- ✅ IS-4: Message content (162 chars)
- ✅ IS-5: Message creator (275 chars)
- ✅ IS-6: Forum (1677 chars)
- ✅ IS-7: Replies (2395 chars)

## Adapted Queries - NOT for Benchmarking

These are simplified/custom queries from `bi-queries-adapted.cypher`:

**Custom Aggregation Queries** (not in LDBC spec):
- AGG-1, AGG-2, AGG-3, AGG-4, AGG-5

**Simplified BI Queries** (too different from official):
- BI-1a, BI-1b (simplified versions of BI-1)
- BI-2a, BI-2b (simplified versions of BI-2)
- BI-4a, BI-4b (simplified versions of BI-4)
- BI-8a, BI-8b (simplified versions of BI-8)
- BI-10a, BI-10b (simplified versions of BI-10)

**Custom Complex Queries** (not in LDBC spec):
- COMPLEX-1, COMPLEX-2, COMPLEX-3, COMPLEX-4, COMPLEX-5

**Purpose of Adapted Queries**:
- ✅ Testing specific Cypher features
- ✅ Development and debugging
- ✅ Demonstrating capabilities
- ❌ NOT for official LDBC benchmarking

## Benchmarking Strategy

### Phase 1: Validate 25 Passing Official Queries

**Priority HIGH** - These match LDBC spec exactly:

1. **Interactive Short (7 queries)** - Simple, should all work
   - IS-1 through IS-7

2. **Core BI Queries (9 queries)** - Medium complexity
   - BI-1, BI-3, BI-4, BI-6, BI-7, BI-9, BI-11, BI-12, BI-18

3. **Core IC Queries (9 queries)** - Complex analytics
   - IC-2, IC-3, IC-5, IC-6, IC-8, IC-11, IC-12, IC-13, IC-14

**Actions**:
- [ ] Test execution of all 25 queries
- [ ] Verify results match expected output
- [ ] Measure performance (latency, throughput)
- [ ] Compare with Neo4j baseline

### Phase 2: Fix Failing Official Queries

**Priority MEDIUM** - Expand coverage to more official queries:

**WITH Clause Issues** (5 queries):
- IC-1, IC-4, IC-10, BI-2, BI-5
- Root cause: Aggregation in WITH clause not properly registered
- Fix in query planner

**Schema Mismatches** (3 queries):
- BI-13, BI-16, IC-7, IC-9
- Fix: Update schema or adapt property names

**Unsupported Features** (8 queries):
- BI-8, BI-10, BI-14, BI-15, BI-17, BI-19, BI-20
- Investigate which Cypher features are missing

### Phase 3: Optional - Extended Testing

**Priority LOW** - For development testing only:

Use adapted queries to test specific features, but don't report as LDBC results.

## Recommended Benchmark Queries

For **official LDBC SNB benchmark results** that can be compared with other systems:

### Minimum Set (Core Functionality) - 15 queries
- All IS queries (7): IS-1 to IS-7
- Core BI queries (5): BI-1, BI-3, BI-4, BI-7, BI-9
- Core IC queries (3): IC-2, IC-3, IC-8

### Extended Set (Full Analytics) - 25 queries
- All passing queries from Phase 1

### Target Set (After Fixes) - 35+ queries
- Add fixed WITH clause queries (5)
- Add fixed schema mismatch queries (4)
- Aim for 80%+ coverage of official LDBC queries

## Key Issues to Address

### 1. WITH Clause Aggregation (5 queries blocked)
```cypher
# Pattern causing issues:
WITH person, count(post) AS postCount  # postCount not accessible later
RETURN person.id, postCount
```

**Fix needed**: Register WITH clause aggregations in render plan

### 2. Schema Property Mismatches (4 queries blocked)
- `likeTime` property not found
- `letter` property not found
- Some properties on zombie/param nodes

**Fix needed**: Review official schema requirements

### 3. Unsupported Cypher Features (8 queries)
Need to investigate which specific features are missing.

## Execution Plan

**Today (Dec 21)**:
1. ✅ Identify official vs adapted queries - DONE
2. ✅ Run official query audit - DONE (25/41 passing)
3. ⏭️ Test execution of 15 core queries
4. ⏭️ Document which queries are benchmark-ready

**Next Session**:
1. Fix WITH clause aggregation bug (highest impact)
2. Fix schema mismatches
3. Expand to 30+ working official queries

## Files

**Official SQL**: `benchmarks/ldbc_snb/results/official_sql/`
- 25 SQL files for passing queries
- `official_audit_report.md` - Detailed results

**Adapted SQL**: `benchmarks/ldbc_snb/results/generated_sql/`
- 42 SQL files (includes custom queries)
- For development/testing only

**Scripts**:
- `scripts/audit_official_queries.py` - Audit official queries only
- `scripts/capture_sql.py` - Audit all adapted queries

## Conclusion

✅ **For Official Benchmarking**: Use 25 passing official queries  
❌ **Not for Benchmarking**: Adapted/custom queries (AGG-*, COMPLEX-*, simplified versions)

**Next Priority**: Test execution and validate correctness of the 25 passing official queries.
