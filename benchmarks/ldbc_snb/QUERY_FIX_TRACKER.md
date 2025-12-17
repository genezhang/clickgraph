# LDBC Query Fix Tracker

**Last Updated**: December 16, 2025

## Current Status: 5/33 queries working (15%)

### ✅ Working Queries (5)
- **IS1**: Profile of a person - 8-10ms
- **IS3**: Friends of a person - 25-28ms  
- **BI1**: Posting summary - 12.4s (needs optimization but works)
- **BI8**: Central person for tag - ✅ (Dec 16: Verified working)
- **BI13**: Zombies in a country - ✅ (Dec 16: WITH literal alias works)

### 🎯 Recent Fixes (Dec 16, 2025)
- **BI-18**: ✅ FIXED - Correlated subquery in JOIN ON issue resolved
  - `NOT (pattern)`, `EXISTS()`, `size()` now stay in WHERE clause
  - CartesianProduct JOIN rendering fixed
  - See CHANGELOG.md for details

- **Comma Pattern WITH Bug**: ✅ FIXED - Missing JOIN ON in CartesianProduct inside WITH
  - Pattern: `MATCH (a), (b) WHERE a.id < b.id WITH a, b, 0 AS score`
  - Now generates proper `INNER JOIN b ON a.id < b.id`
  - Computed columns now properly prefixed with table alias

### 🔧 Priority 1: Quick Fixes (Target: +6 queries = 27% total)

#### A. Parsing Fixes (2 queries)
- [ ] **IC13**: `path = shortestPath(...)` - Assignment in MATCH not supported
- [ ] **BI17**: Missing `$delta` parameter (already added, needs testing)

#### B. WITH Clause Fixes (STATUS: ALL PATTERNS WORK!)  
- [x] **BI13**: ✅ WORKS - `WITH country, 12 AS monthNum` (Dec 16)
- [x] **BI14 BASE**: ✅ FIXED - Cartesian WITH pattern now works (Dec 16)
  - Pattern: `MATCH (a), (b) WHERE a.id < b.id WITH a, b, 0 AS score`
  - Fix: Added Filter(WithClause(CartesianProduct)) handling
  - JOIN ON now properly generated in CTE
  - Full BI-14 query may need additional work for OPTIONAL MATCH chains
- [x] **BI8**: ✅ WORKS - `100 * size` arithmetic expression (Dec 16)

#### C. Schema Fixes (1 query)
- [ ] **IC4**: Tag.name property (already added to schema, needs verification)

### 🔨 Priority 2: Feature Gaps (11 queries)

#### ANALYZER_RELATION Errors (needs investigation)
- [ ] **BI11**: Invalid relation query - t117
- [ ] **BI5**: Invalid relation query - t155  
- [ ] **BI6**: Invalid relation query - t159
- [ ] **IC1**: Invalid relation query - t170
- [ ] **IC11**: Invalid relation query - t173
- [ ] **IC2**: Invalid relation query - t183
- [ ] **IC3**: Invalid relation query - t184
- [ ] **IC7**: Invalid relation query - t188
- [ ] **IC8**: Invalid relation query - t192
- [ ] **IC9**: Invalid relation query - t194
- [ ] **IS5**: Invalid relation query - t195

**Common Pattern**: These likely involve complex patterns like:
- Variable-length paths with filters
- OPTIONAL MATCH with multiple hops
- Subqueries or complex WITH clauses
- Need to analyze one by one

### 🏗️ Priority 3: SQL Generation Issues (8 queries)

#### Duplicate Alias Errors (3 queries)
- [ ] **BI12**: Multiple table expression with same alias
- [ ] **BI18**: Multiple table expression with same alias
- [ ] **IS6**: Multiple table expression with same alias

**Root Cause**: Variable-length path `[:REPLY_OF*0..]` generates CTEs with duplicate aliases

#### Unknown Identifier Errors (5 queries)
- [ ] **BI9**: Unknown table expression identifier
- [ ] **IS2**: Unknown table expression identifier  
- [ ] **IS4**: Unknown expression `t.id`
- [ ] **IC5**: Unknown identifier `otherPerson.person_id`
- [ ] **IC6**: Unknown expression `friend.id`
- [ ] **IC12**: Unknown identifier `rel.from_id`

**Root Cause**: CTE column aliasing or join predicate issues

### 📋 Priority 4: Missing Relationships (4 queries)

- [ ] **BI2**: No HAS_TYPE relationship from Tag to TagClass
- [ ] **BI3**: Missing relationship (analyze query)  
- [ ] **BI7**: No HAS_TAG relationship (analyze query)
- [ ] **IS7**: No REPLY_OF relationship (need generic type)

**Action**: Need to add these to ldbc_snb_complete.yaml or use alternate types

---

## Next Steps

1. **Test IC4** - verify Tag.name fix works
2. **Fix WITH clause aliases** - BI13, BI14, BI8
3. **Investigate ANALYZER_RELATION** - pick one query (IC1) and debug thoroughly
4. **Fix duplicate alias in variable paths** - Critical SQL generation bug
5. **Add missing relationship types** - Schema configuration

---

## Test Command

```bash
cd /home/gz/clickgraph/benchmarks/ldbc_snb/scripts
python3 audit_queries_individual.py
```

Or for specific query:
```bash
curl -s http://localhost:8080/query -H "Content-Type: application/json" \
  -d '{"query":"..."}' | jq .
```
