# LDBC Baseline Test Results - January 11, 2026

## Summary

**Total**: 41 queries  
**Passing**: 7/41 (17.1%)  
**Failing**: 33/41  
**Error**: 1/41

## Failure Categories

### Category A: Parser/Syntax Errors (10 queries) - Easy Wins
**Root Cause**: Unsupported syntax, CALL procedures, list comprehensions

- **IC-10**: List comprehension in WHERE `[p IN posts WHERE ...]`  
- **IC-14**: CALL gds.graph.project.cypher (Neo4j GDS library)
- **BI-4**, **BI-10**, **BI-15**, **BI-16**, **BI-19**, **BI-20**: CALL apoc.* (Neo4j APOC library)
- **BI-17**: Parser error on pattern

**Fix Strategy**: 
1. Add list comprehension support (IC-10) - **High Value**
2. CALL procedures - Create Class 3 variations (no equivalent in ClickHouse)

---

### Category B: Schema/Property Issues (5 queries) - Schema Fix
**Root Cause**: Property name mismatches between query and schema

- **IC-3**: Property 'creationDate' not found on node 'message'
- **IC-7**: Property 'likeTime' not found on node 'latestLike'  
- **BI-14**: Property 'person1Id' not found on node 'top'

**Fix Strategy**: Verify schema property mappings in `ldbc_snb_complete.yaml`

---

### Category C: Missing FROM Clause (6 queries) - Planner Bug
**Root Cause**: Query planner fails to generate FROM clause for complex WITH patterns

- **IC-4**, **IC-6**: No FROM clause found
- **BI-5**, **BI-11**, **BI-12**: No FROM clause found

**Fix Strategy**: Improve FROM clause propagation in WITH clauses - **Medium Priority**

---

### Category D: GROUP BY Issues (2 queries) - Known Issue
**Root Cause**: Cannot find ID column for GROUP BY alias

- **IC-5**: Cannot find ID column for alias 'forum'
- **BI-13**: Cannot find ID column for alias 'zombie'

**Fix Strategy**: Fix GROUP BY ID resolution - **Medium Priority**

---

### Category E: ClickHouse SQL Errors (9 queries) - SQL Generation Bugs
**Root Cause**: Generated SQL is invalid

- **IS-2**: Unknown table expression identifier
- **IC-1**: Cannot modify 'max_recursive...' (likely max_recursion_depth setting issue)
- **IC-8**, **IC-9**, **IC-11**, **IC-12**, **IC-13**: Unknown expression/identifier in generated SQL
- **BI-2**, **BI-9**: Unknown identifier in generated SQL

**Fix Strategy**: 
1. Fix IC-9 CTE column naming (affects multiple queries) - **HIGHEST PRIORITY**
2. Debug individual SQL generation issues

---

### Category F: ClickHouse Type Errors (2 queries) - Type Casting
**Root Cause**: Type mismatch in generated SQL

- **BI-1**: Illegal type Int64 of argument (likely aggregation type issue)
- **BI-3**: Multiple expressions for identifier (likely column aliasing issue)

**Fix Strategy**: Improve type casting in aggregations

---

### Category G: Complex Queries (7 queries) - Various Issues
**Root Cause**: Multiple issues, need individual investigation

- **BI-6**, **BI-7**, **BI-18**: Unknown expression/function
- **BI-8**: Server crash (connection closed)

**Fix Strategy**: Individual investigation needed

---

## Passing Queries (7) ✅

### Interactive Short (6/7)
- ✅ IS-1: Person profile
- ✅ IS-3: Friends  
- ✅ IS-4: Message content
- ✅ IS-5: Message creator
- ✅ IS-6: Forum
- ✅ IS-7: Replies

### Interactive Complex (1/14)
- ✅ IC-2: Recent messages by friends

---

## Priority Action Plan

### Phase 1: Quick Wins (1-2 days) - Target +10 queries
1. **Fix IC-9 CTE column naming** → Fixes IC-9, possibly IC-8, IC-11, IC-12, IC-13, BI-2, BI-9 (**~7 queries**)
2. **Fix schema property mappings** → Fixes IC-3, IC-7, BI-14 (**+3 queries**)
3. **Add list comprehension support** → Fixes IC-10 (**+1 query**)

**Expected Result**: 17-18/41 passing (41-44%)

### Phase 2: Medium Fixes (2-3 days) - Target +5 queries  
1. **FROM clause propagation** → Fixes IC-4, IC-6, BI-5, BI-11, BI-12 (**+5 queries**)
2. **GROUP BY ID resolution** → Fixes IC-5, BI-13 (**+2 queries**)

**Expected Result**: 24-25/41 passing (59-61%)

### Phase 3: Create Adaptations (1 day) - Target +3-5 queries
1. **IS-2**: Fix table identifier issue
2. **IC-1**: Fix max_recursion_depth setting
3. **BI-1**, **BI-3**: Type casting fixes

**Expected Result**: 27-30/41 passing (66-73%)

### Phase 4: Class 3 Variations (1 day) - Document limitations
1. **CALL procedures** (IC-14, BI-4, BI-10, BI-15, BI-16, BI-19, BI-20): Create simplified variations
2. **BI-8**: Investigate server crash
3. **BI-6**, **BI-7**, **BI-18**: Individual fixes or variations

---

## Realistic v0.6.1 Target

**Conservative**: 25/41 (61%) Class 1+2  
**Stretch**: 30/41 (73%) Class 1+2  
**Class 3**: 5-8 queries (functional variations)  
**Class X**: 3-8 queries (blocked by missing features)

---

## Next Immediate Steps

1. ✅ Baseline test complete - 7/41 passing
2. 🔄 Fix IC-9 CTE column naming (HIGHEST IMPACT)
3. 🔄 Fix schema property mappings  
4. 🔄 Re-test and measure improvement
5. 🔄 Continue with Phase 2 fixes
