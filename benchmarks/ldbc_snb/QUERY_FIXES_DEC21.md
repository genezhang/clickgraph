# LDBC Query Fixes - December 21, 2025

## Summary of Fixes

**Before**: 36/46 queries passing (78%)  
**After**: 42/46 queries passing (91%)  
**Improvement**: +6 queries fixed

## Changes Made

### 1. Relationship Name Corrections

Fixed 5 queries with incorrect relationship names:

**BI-7**: `REPLY_OF_POST` → `REPLY_OF`
```cypher
# Before
MATCH (post:Post)<-[:REPLY_OF_POST]-(comment:Comment)

# After
MATCH (post:Post)<-[:REPLY_OF]-(comment:Comment)
```

**BI-9**: `REPLY_OF_POST` → `REPLY_OF`
```cypher
# Before
OPTIONAL MATCH (post)<-[:REPLY_OF_POST]-(reply:Comment)

# After
OPTIONAL MATCH (post)<-[:REPLY_OF]-(reply:Comment)
```

**BI-17**: `REPLY_OF_POST` → `REPLY_OF`
```cypher
# Before
MATCH (comment:Comment)-[:REPLY_OF_POST]->(post2:Post)

# After
MATCH (comment:Comment)-[:REPLY_OF]->(post2:Post)
```

**COMPLEX-3**: `REPLY_OF_POST` → `REPLY_OF`, `REPLY_OF_COMMENT` → `REPLY_OF`
```cypher
# Before
MATCH (post:Post)<-[:REPLY_OF_POST]-(c1:Comment)
OPTIONAL MATCH (c1)<-[:REPLY_OF_COMMENT*1..5]-(cn:Comment)

# After
MATCH (post:Post)<-[:REPLY_OF]-(c1:Comment)
OPTIONAL MATCH (c1)<-[:REPLY_OF*1..5]-(cn:Comment)
```

**COMPLEX-5**: `Organisation` → `Company` (label mismatch)
```cypher
# Before
MATCH (company:Organisation)<-[:WORK_AT]-(employee:Person)

# After
MATCH (company:Company)<-[:WORK_AT]-(employee:Person)
```

**IC-3**: `POST_LOCATED_IN` → `IS_LOCATED_IN`
```cypher
# Before
MATCH (post:Post)-[:POST_LOCATED_IN]->(country:Country)

# After
MATCH (post:Post)-[:IS_LOCATED_IN]->(country:Country)
```

### 2. Date Type Handling

Fixed date literal to work with ClickHouse Int64 timestamp columns:

**BI-1a**: Convert date string to Unix timestamp (milliseconds)
```cypher
# Before
WHERE message.creationDate < '2012-01-01'

# After
WHERE message.creationDate < 1325376000000  # 2012-01-01 in Unix ms
```

**Note**: IC-3 still has date parameters (`$startDate`, `$endDate`) that need to be provided as Unix timestamps when calling the query.

## Verification

### SQL Generation Tests
- ✅ All 6 fixed queries now generate valid SQL
- ✅ BI-1a executes successfully in ClickHouse
- ✅ BI-7, BI-9, BI-17, COMPLEX-3, COMPLEX-5, IC-3 all pass SQL generation

### Execution Test Results

**BI-1a** (tested successfully):
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (message:Post) WHERE message.creationDate < 1325376000000 RETURN message.creationDate, count(*) AS messageCount ORDER BY messageCount DESC LIMIT 5","database":"ldbc"}'

# Returns 5 rows with correct aggregation
```

**COMPLEX-5** (SQL generation works, but execution reveals duplicate alias bug):
```
Error: Multiple table expressions with same alias t110
```
This is a known issue with duplicate JOINs in the SQL generator (not specific to our fixes).

## Remaining Issues

### 1. Parser Issues (4 queries)
These files fail with "empty query AST" - likely syntax/formatting issues:
- bi-4-workaround.cypher
- bi-8-workaround.cypher
- interactive-complex-10-workaround.cypher
- interactive-short-7.cypher

**Action needed**: Review file format, ensure proper Cypher syntax

### 2. Duplicate JOIN Aliases (SQL Generator Bug)
The SQL generator sometimes creates duplicate table aliases (e.g., `t110` used twice), causing ClickHouse execution errors.

**Affected**: Multi-MATCH queries with shared patterns (COMPLEX-5, possibly BI-3)

**Action needed**: Fix JOIN alias generation in `clickhouse_query_generator`

### 3. Date Parameter Handling
While we fixed hardcoded date literals, queries with date parameters (e.g., `$startDate`) still need Unix timestamps passed in:

```javascript
// When calling IC-3
{
  "query": "MATCH (person:Person {id: $personId})...",
  "parameters": {
    "personId": 933,
    "startDate": 1325376000000,  // Must be Unix ms, not "2012-01-01"
    "endDate": 1356998400000     // Must be Unix ms, not "2013-01-01"
  }
}
```

## Schema Learnings

### Relationship Name Standards
- Use `REPLY_OF` for all reply relationships (Comment→Post, Comment→Comment)
- Unified relationship names work across different node type combinations
- Check schema YAML for exact relationship type names

### Label Hierarchy
- `Organisation` is base label for all organizations
- `Company` and `University` are filtered views (type = 'Company' / 'University')
- Relationships reference the specific filtered label (e.g., `WORK_AT` uses `Company`)

### Date Column Types
- LDBC schema stores dates as `Int64` (Unix timestamp in milliseconds)
- No automatic string→timestamp conversion in ClickHouse for Int64 columns
- Must provide numeric timestamps in queries

## Files Modified

1. `benchmarks/ldbc_snb/queries/adapted/bi-queries-adapted.cypher`
   - Fixed BI-1a, BI-7, BI-9, BI-17, COMPLEX-3, COMPLEX-5

2. `benchmarks/ldbc_snb/queries/adapted/interactive-complex-3.cypher`
   - Fixed IC-3

## Next Steps

### Immediate (High Priority)
1. ✅ Fix relationship names - DONE
2. ✅ Fix date literals - DONE  
3. ✅ Verify SQL generation - DONE (42/46 passing)
4. ⚠️ Fix duplicate JOIN alias bug in SQL generator
5. 🔲 Review/fix 4 workaround files

### Short Term (This Week)
6. Test all 42 passing queries for execution
7. Document which queries work end-to-end
8. Create date conversion helper for parameters
9. Run performance benchmark on working queries

### Medium Term
10. Optimize JOIN generation to eliminate duplicates
11. Add schema type metadata for automatic date conversion
12. Complete full LDBC validation suite

## Impact

**Query Coverage**: 91% of adapted queries now generate valid SQL (up from 78%)

**Blocking Issues Resolved**: 
- ✅ Relationship name mismatches
- ✅ Date literal type mismatches  
- ✅ Label hierarchy confusion

**Remaining Blockers**:
- 🔴 Duplicate JOIN aliases (affects ~5-10 complex queries)
- 🟡 4 workaround files need syntax fixes

**Estimated Time to 100% Working**:
- Fix JOIN alias bug: 2-3 hours
- Fix workaround files: 1 hour
- Full validation: 1-2 days
- **Total**: 1-2 days to full working benchmark suite
