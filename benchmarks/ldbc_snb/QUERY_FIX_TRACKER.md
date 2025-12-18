# LDBC Query Fix Tracker

**Last Updated**: December 19, 2025

## Current Status: 18/41 queries generating valid SQL (44%)

### ✅ Working Queries (18 total)

**Interactive Short (7/7 = 100%)**
- **IS1**: Profile of a person
- **IS2**: Recent messages by person
- **IS3**: Friends of a person
- **IS4**: Content of a message
- **IS5**: Creator of a message
- **IS6**: Forum of a message
- **IS7**: Replies to a message

**Interactive Complex (5/14 = 36%)**
- **IC2**: Recent messages by friends
- **IC5**: New groups of a person's friends
- **IC6**: Tag co-occurrence
- **IC9**: Recent messages by friends or friends of friends
- **IC14**: Weighted paths

**Business Intelligence (6/20 = 30%)**
- **BI1**: Posting summary
- **BI2**: Tag evolution
- **BI3**: Popular topics in a country
- **BI6**: Active posters in a month
- **BI12**: Trending posts
- **BI18**: Friend recommendation

### 🎯 Recent Fixes (Dec 16-19, 2025)

- **CTE Column Aliasing**: ✅ FIXED (Dec 19) - Underscore convention now enforced
  - Fixed dot notation bug in CTE column names (`"a.name"` → `"a_name"`)
  - Affects queries with `WITH alias RETURN alias.property` pattern
  - See KNOWN_ISSUES.md Issue #1, CHANGELOG.md for details

- **Database Prefix**: ✅ FIXED (Dec 19) - Base table JOINs after WITH clause
  - Missing database qualifiers now added (e.g., `ldbc.Place`)
  - Prevents "Unknown table" errors in non-default databases
  - See KNOWN_ISSUES.md Issue #2, CHANGELOG.md for details

- **BI-18**: ✅ FIXED (Dec 16) - Correlated subquery in JOIN ON issue resolved
  - `NOT (pattern)`, `EXISTS()`, `size()` now stay in WHERE clause
  - CartesianProduct JOIN rendering fixed
  - See CHANGELOG.md for details

- **Comma Pattern WITH Bug**: ✅ FIXED (Dec 16) - Missing JOIN ON in CartesianProduct inside WITH
  - Pattern: `MATCH (a), (b) WHERE a.id < b.id WITH a, b, 0 AS score`
  - Now generates proper `INNER JOIN b ON a.id < b.id`
  - Computed columns now properly prefixed with table alias

### 🔧 Priority 1: Quick Fixes (STATUS: 2 working, 1 blocked)
- ✅ **IC4**: Schema fix verified working
- ✅ **BI17**: Operator precedence bug fixed (Dec 17)
- ❌ **IC13**: Blocked by parser limitation (path assignment syntax)

#### A. Parsing Fixes (2 queries) - 1 FIXED, 1 BLOCKED
- [ ] **IC13**: `path = shortestPath(...)` - ❌ Parser doesn't support variable assignment in MATCH clause
  - Error: "Unable to parse path = shortestPath(...)"
  - Needs: Parser enhancement to support Cypher path assignment syntax
- [x] **BI17**: Temporal arithmetic - ✅ FIXED (Dec 17)
  - Pattern: `datetime("2011-01-01") + duration({hours: 4})`
  - Root Cause: Operator precedence bug - all binary operators parsed at same level
  - Fix: Implemented proper precedence: multiplicative > additive > comparison > logical
  - Now generates: `WHERE m.creationDate > parseDateTime64BestEffort(...) + toIntervalHour(4)`

#### B. WITH Clause Fixes (STATUS: ✅ ALL COMPLETE!)  
- [x] **BI13**: ✅ WORKS - `WITH country, 12 AS monthNum` (Dec 16)
- [x] **BI14 BASE**: ✅ FIXED - Cartesian WITH pattern now works (Dec 16)
  - Pattern: `MATCH (a), (b) WHERE a.id < b.id WITH a, b, 0 AS score`
  - Fix: Added Filter(WithClause(CartesianProduct)) handling
  - JOIN ON now properly generated in CTE
  - Full BI-14 query may need additional work for OPTIONAL MATCH chains
- [x] **BI8**: ✅ WORKS - `100 * size` arithmetic expression (Dec 16)

#### C. Schema Fixes (1 query) - ✅ COMPLETE
- [x] **IC4**: ✅ WORKS - Tag.name property verified (Dec 17)
  - Query: `MATCH (tag:Tag {name: "Che_Guevara"}) RETURN tag.name, tag.url`
  - Returns: `{"tag.name": "Che_Guevara", "tag.url": "http://dbpedia.org/resource/..."}`

### 🔨 Priority 2: Planning/Rendering Failures (23 queries)

**These queries fail to generate SQL and return "SQL doesn't contain SELECT" errors.**

#### Interactive Complex (9/14 failed)
- [ ] **IC1**: Friends with a given name (planning failure)
- [ ] **IC3**: Friends within N hops from 2 countries (planning failure)
- [ ] **IC4**: Tag co-occurrence (planning failure)
- [ ] **IC7**: Recent likers (planning failure)
- [ ] **IC8**: Recent replies (planning failure)
- [ ] **IC10**: Friend recommendation (planning failure)
- [ ] **IC11**: Job referral (planning failure)
- [ ] **IC12**: Expert search (planning failure)
- [ ] **IC13**: Shortest path between people - ❌ Parser doesn't support `path = shortestPath(...)`

#### Business Intelligence (14/20 failed)
- [ ] **BI4**: Popular topics in a country (planning failure)
- [ ] **BI5**: Active posters (planning failure)
- [ ] **BI7**: Related topics (planning failure)
- [ ] **BI8**: Central person for tag (planning failure)
- [ ] **BI9**: Forum with related tags (planning failure)
- [ ] **BI10**: Experts in social circle (planning failure)
- [ ] **BI11**: Friend triangles (planning failure)
- [ ] **BI13**: Zombies (planning failure)
- [ ] **BI14**: International dialog (planning failure)
- [ ] **BI15**: Weighted paths (planning failure)
- [ ] **BI16**: Fake news pattern (planning failure)
- [ ] **BI17**: Information propagation - ✅ FIXED (Dec 17) but needs retest
- [ ] **BI19**: Interaction path between cities (planning failure)
- [ ] **BI20**: Recruitment recommendation (planning failure)

**Root Cause Analysis Needed**: These queries likely involve:
- Complex OPTIONAL MATCH chains
- Multiple aggregations
- Subqueries in WHERE/WITH clauses
- Variable-length paths with complex filters
- Missing analyzer logic for specific patterns

### 📊 Audit Results Summary

**Audit Run**: December 17, 2025 using `audit_sql_generation.py`  
**Note**: Re-audit recommended after Dec 19 fixes (CTE aliasing, database prefix)

**Results**:
- Total queries tested: 41
- ✅ Valid SQL generated: 18 (44%)
- ❌ Planning failures: 23 (56%)

**By Category**:
- Interactive Short: 7/7 (100%) ✅ Perfect!
- Interactive Complex: 5/14 (36%)
- Business Intelligence: 6/20 (30%)

**Key Finding**: The old tracker showed 5/33 working (15%) but actual audit shows 18/41 generating valid SQL (44%). The discrepancy was due to:
1. Using interactive audit script that waited for user input (appeared to hang)
2. Not all queries were being tested
3. Recent fixes (BI17, IC4, BI13, BI14, BI18) improved coverage

---

## Next Steps

1. **Investigate Planning Failures** - Pick 2-3 failed queries and identify root causes
   - Start with IC1 (friends with given name) - simpler pattern
   - Then IC3 (multi-hop with filters) - more complex
   - Analyze error messages with DEBUG logging

2. **Verify BI17 Fix** - Was recently fixed, should now pass audit

3. **Test Query Execution** - Run working queries against actual data
   - Check if SQL generation ✓ means actual results ✓
   - Identify queries that generate SQL but fail at execution

4. **Performance Testing** - Benchmark the 18 working queries
   - Identify slow queries needing optimization
   - Create execution time baseline

---

## Test Commands

**Batch SQL Generation Audit** (non-interactive, recommended):
```bash
cd /home/gz/clickgraph/benchmarks/ldbc_snb/scripts
python3 audit_sql_generation.py
```

**Individual Query Testing** (interactive, waits for input):
```bash
cd /home/gz/clickgraph/benchmarks/ldbc_snb/scripts
python3 audit_queries_individual.py
```

**Manual Query Test**:
```bash
curl -s http://localhost:8080/query -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n:Person {id: 933}) RETURN n.firstName","database":"ldbc"}' | jq .
```

**Debug Specific Query**:
```bash
export RUST_LOG=debug
# Restart server, then test query and check /tmp/server.log
```
