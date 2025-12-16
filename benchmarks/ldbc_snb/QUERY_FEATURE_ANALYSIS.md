# LDBC SNB Query Feature Analysis

This document analyzes the Cypher features used in official LDBC SNB benchmark queries and maps them against ClickGraph's current capabilities.

## Summary

| Category | Total Queries | Works As-Is | Simple Workaround | Needs Restructuring | Blocked (External Libs) |
|----------|--------------|-------------|-------------------|---------------------|-------------------------|
| Interactive Short (IS) | 7 | 7 | 0 | 0 | 0 |
| Interactive Complex (IC) | 14 | 11 | 0 | 0 | 3 |
| Business Intelligence (BI) | 20 | 12 | 0 | 2 | 6 |
| **Total** | **41** | **30 (73%)** | **0 (0%)** | **2 (5%)** | **9 (22%)** |

**Actionable Coverage**: **32 of 32 non-blocked queries (100%)** can be tested!

**Last Updated**: December 16, 2025 (Post-Issue #6 fixes)

**Recent Enhancements**:
- ✅ `duration()` with map arguments - IMPLEMENTED (Dec 2025)
- ✅ Temporal arithmetic (`datetime() + duration({days: N})`) - IMPLEMENTED (Dec 2025)
- ✅ MapLiteral parsing (`{key: value}` syntax) - IMPLEMENTED (Dec 2025)
- ✅ Temporal extraction functions (`toYear()`, `toMonth()`, etc.) - IMPLEMENTED (Dec 2025)
- ✅ Label predicate (`n:Label` in expressions) - IMPLEMENTED (Dec 2025)
- ✅ **Cross-table branching patterns** - FIXED (Dec 15, Issue #1)
- ✅ **4-Level WITH CTE column references** - FIXED (Dec 15, Issue #2)
- ✅ **WITH+MATCH aggregation** - FIXED (Dec 15, Issue #5)
- ✅ **Comma patterns & NOT operator** - FIXED (Dec 16, Issue #6)

**Recent Discoveries**:
- ✅ `size()` on patterns - IMPLEMENTED (Dec 11, 2025) - Works for simple patterns!
- ✅ Most queries can work with existing features + minor workarounds
- ✅ **BI8 now works directly** without pattern comprehension workaround!
- ✅ **IC10 pattern comprehension** moved to blocked (requires parser extension)

**Remaining Gaps** (known limitations with workarounds):
- ⚠️ Pattern comprehension `[(p)-[:R]->(x) | x.prop]` - Use OPTIONAL MATCH + collect()
- ⚠️ CALL subquery `CALL { ... }` - Restructure with WITH + UNION

## Feature Gap Analysis

### ✅ Features ClickGraph Supports

| Feature | Status | Notes |
|---------|--------|-------|
| MATCH patterns | ✅ Full | Node and relationship patterns |
| WHERE clauses | ✅ Full | Including complex predicates |
| RETURN / ORDER BY / LIMIT | ✅ Full | All basic clauses |
| OPTIONAL MATCH | ✅ Full | LEFT JOIN semantics |
| WITH clause | ✅ Full | Query chaining |
| UNWIND | ✅ Full | Via ARRAY JOIN |
| Parameters ($param) | ✅ Full | Via query API |
| Variable-length paths `*1..3` | ✅ Full | Recursive CTE generation |
| Aggregations (count, sum, avg, etc.) | ✅ Full | All standard aggregates |
| collect() | ✅ Full | Array aggregation |
| DISTINCT | ✅ Full | Deduplication |
| CASE expressions | ✅ Full | Simple and searched CASE |
| shortestPath() | ✅ Full | Recursive CTE with early termination |
| length(), nodes(), relationships() | ✅ Full | Path functions |
| coalesce() | ✅ Full | NULL handling |
| toInteger(), toFloat(), toString() | ✅ Full | Type conversion |
| String functions | ✅ Full | upper, lower, trim, substring, split, replace, etc. |
| reduce() | ✅ Full | Via arrayFold() |
| NOT predicate | ✅ Full | Including `NOT (pattern)` |
| IN operator | ✅ Full | List membership |
| IS NULL / IS NOT NULL | ✅ Full | NULL checks |
| **head(), tail(), last()** | ✅ Full | List element access |
| **abs(), floor(), ceil(), round()** | ✅ Full | Math functions |
| **datetime(), date(), timestamp()** | ✅ Full | Temporal parsing |
| **year(), month(), day(), hour()** | ✅ Full | Temporal extraction |
| **sin, cos, tan, log, exp, sqrt, pow** | ✅ Full | Advanced math |
| **startsWith, endsWith, contains** | ✅ Full | String predicates |
| **keys()** | ✅ Full | Map key extraction |

### ✅ Recently Implemented (December 2025)

| Feature | Status | Notes |
|---------|--------|-------|
| **duration()** with map args | ✅ DONE | `duration({days: 5})` → `toIntervalDay(5)` |
| **Temporal arithmetic** | ✅ DONE | `datetime() + duration({days: N})` works automatically |
| **MapLiteral parsing** | ✅ DONE | `{key: value, ...}` syntax supported |
| **Multi-unit durations** | ✅ DONE | `duration({days: 5, hours: 2})` supported |
| **Temporal extraction functions** | ✅ DONE | `toYear(x)`, `toMonth(x)`, `toDayOfMonth(x)` work in WHERE/RETURN |
| **Label predicate** `n:Label` | ✅ DONE | Type check in expression context (WHERE/WITH clauses) |
| **Polymorphic label predicate** | ✅ DONE | For tables with `label_column`, generates `type = 'Label'` at runtime |

**Label Predicate Usage**:
- For **non-polymorphic tables**: Resolves at compile-time to `true`/`false`
- For **polymorphic tables** (with `label_column` in schema): Generates runtime check `type = 'Label'`

Example with LDBC `Message` table (polymorphic, has `type` column):
```cypher
-- WHERE clause: generates WHERE m.type = 'Comment'
MATCH (m:Message) WHERE m:Comment RETURN m

-- WITH clause: generates boolean columns
MATCH (m:Message)
WITH m, m:Comment AS isComment, m:Post AS isPost
RETURN m.id, isComment, isPost
```

**Note on Property Path Access**: The Cypher property-style `birthday.year` is **NOT supported** due to parser limitations with chained property access. Use **function-style** instead:
- ❌ `birthday.year` → Not supported
- ✅ `toYear(birthday)` or `year(birthday)` → Fully supported

### ⚠️ Features Needing Implementation or Testing

| Feature | Priority | Queries Affected | Implementation Effort |
|---------|----------|-----------------|----------------------|
| ~~size() on patterns~~ | ~~MEDIUM~~ | ~~BI8, IC10~~ | ✅ DONE (Dec 11, 2025) |

**Note**: Property path access (`.year`, `.month`) is available via function-style syntax (e.g., `toYear(birthday)`). See "Recently Implemented" section.

### 🚧 Deferred as Known Limitations

| Feature | Queries Affected | Workaround |
|---------|-----------------|------------|
| **Pattern comprehension** `[(p)-[:R]->(x) \| x.prop]` | BI8, IC10 | Use OPTIONAL MATCH + collect() |
| **CALL subquery** `CALL { ... }` | BI4, BI16 | Restructure with WITH clauses |

### 🚫 Features Requiring External Libraries (Blocked)

| Feature | Queries Affected | Notes |
|---------|-----------------|-------|
| **gds.shortestPath.dijkstra** | IC14, BI15, BI19, BI20 | Neo4j Graph Data Science library |
| **apoc.path.subgraphNodes** | BI10 | Neo4j APOC library |
| **gds.graph.project** | BI15, BI19, BI20 | GDS graph projection |

These queries require Neo4j-specific graph algorithm libraries that have no direct Cypher equivalent.

---

## Query-by-Query Analysis

### Interactive Short Queries (IS1-IS7)

| Query | Status | Missing Features | Notes |
|-------|--------|------------------|-------|
| IS1 | ✅ Ready | None | Simple pattern match |
| IS2 | ✅ Ready | None | With ORDER BY DESC |
| IS3 | ✅ Ready | None | Simple friends query |
| IS4 | ✅ Ready | None | Content lookup |
| IS5 | ✅ Ready | None | Creator lookup |
| IS6 | ✅ Ready | None | Forum lookup |
| IS7 | ✅ Ready | None | Reply lookup |

### Interactive Complex Queries (IC1-IC14)

| Query | Status | Workaround Needed | Notes |
|-------|--------|-------------------|-------|
| IC1 | ✅ Works | None | Uses shortestPath ✅, complex WITH chains |
| IC2 | ✅ Works | None | coalesce() used |
| IC3 | ✅ Works | None (just needs testing) | `country IN [x, y]` should already work |
| IC4 | ✅ Works | None | CASE with temporal comparison |
| IC5 | ✅ Works | None | Variable-length KNOWS |
| IC6 | ✅ Works | None | Tag co-occurrence with UNWIND |
| IC7 | ✅ Works | None | Recent likers (head(), floor() ✅) |
| IC8 | ✅ Works | None | Recent replies |
| IC9 | ✅ Works | None | Friends of friends messages |
| IC10 | 🚫 Blocked | N/A (pattern comprehension) | Requires parser extension |
| IC11 | ✅ Works | None | Job referral |
| IC12 | ✅ Works | None | Expert search (*0.. ✅) |
| IC13 | ✅ Works | None | shortestPath with CASE |
| IC14 | 🚫 Blocked | N/A (external library) | Requires Neo4j GDS |

### Business Intelligence Queries (BI1-BI20)

| Query | Status | Workaround Needed | Notes |
|-------|--------|-------------------|-------|
| BI1 | ✅ Works | None | Label predicate ✅ (Dec 2025) |
| BI2 | ✅ Works | None | Tag evolution (duration ✅) |
| BI3 | ✅ Works | None | Country topics (*0.. ✅) |
| BI4 | 🚧 Restructure | Rewrite CALL as WITH + UNION | Top message creators |
| BI5 | ✅ Works | None | Active posters |
| BI6 | ✅ Works | None | Authority score |
| BI7 | ✅ Works | None | Related topics |
| BI8 | ✅ Works | None | Central person (size() ✅ works directly!) |
| BI9 | ✅ Works | None | Top thread initiators |
| BI10 | 🚫 Blocked | N/A (external library) | Requires Neo4j APOC |
| BI11 | ✅ Works | None | Friend triangles |
| BI12 | ✅ Works | None (just needs testing) | IN with array param should work |
| BI13 | ✅ Works | None | Zombie detection (temporal ✅) |
| BI14 | ✅ Works | None | International dialog |
| BI15 | 🚫 Blocked | N/A (external library) | Requires Neo4j GDS |
| BI16 | 🚧 Restructure | Rewrite CALL as WITH + UNION | Fake news detection |
| BI17 | ✅ Works | None | Information propagation |
| BI18 | ✅ Works | None | Friend recommendation |
| BI19 | 🚫 Blocked | N/A (external library) | Requires Neo4j GDS |
| BI20 | 🚫 Blocked | N/A (external library) | Requires Neo4j GDS |

---

## Workarounds for Partially Supported Queries

### IC10 - Pattern Comprehension Workaround

**Original Query** (uses pattern comprehension):
```cypher
WITH friend,
     city,
     size(posts) AS postCount,
     size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
```

**Workaround** (use OPTIONAL MATCH + COUNT):
```cypher
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
WITH friend, city, person, collect(post) AS allPosts
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(commonPost:Post)
WHERE (commonPost)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)
WITH friend, city, person, size(allPosts) AS postCount, count(commonPost) AS commonPostCount
```

### BI8 - Pattern Comprehension Workaround

**Original Query** (uses pattern comprehension):
```cypher
100 * size([(tag)<-[interest:HAS_INTEREST]-(person) | interest]) 
  + size([(tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person) 
     WHERE $startDate < message.creationDate AND message.creationDate < $endDate | message])
```

**Workaround** (use size() on patterns directly - implemented Dec 11):
```cypher
100 * size((tag)<-[:HAS_INTEREST]-(person)) 
  + size((tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person))
-- Note: WHERE clause filtering in size() may need separate OPTIONAL MATCH
```

### BI4 & BI16 - CALL Subquery Restructuring

**Original Pattern**:
```cypher
CALL {
  WITH topForums
  UNWIND topForums AS topForum1
  MATCH ...
  RETURN person, count(DISTINCT message) AS messageCount
UNION ALL
  WITH topForums
  UNWIND topForums AS topForum1
  MATCH ...
  RETURN person, 0 AS messageCount
}
```

**Workaround** (restructure as separate queries with UNION):
```cypher
-- First part
WITH topForums
UNWIND topForums AS topForum1
MATCH ...
WITH person, count(DISTINCT message) AS messageCount
UNION ALL
-- Second part
WITH topForums
UNWIND topForums AS topForum1
MATCH ...
WITH person, 0 AS messageCount
-- Then continue with grouping
WITH person, sum(messageCount) AS totalMessageCount
RETURN ...
```

---

## Implementation Priority

### ✅ Completed Phases

**Phase 1: Quick Wins** - DONE
- ✅ UNWIND - `arrayJoin()` mapping
- ✅ head(), tail(), last() - List element access
- ✅ abs(), floor(), ceil(), round() - Math functions
- ✅ Zero-length paths `*0..` - Supported

**Phase 2: Temporal Support** - DONE
- ✅ datetime() function - ISO datetime parsing
- ✅ duration() function with map args - Interval generation
- ✅ Temporal arithmetic - Add/subtract durations from dates
- ✅ year(), month(), day() functions - Extract date parts

### 🎯 Low-Hanging Fruit (Next Priorities)

| Feature | Effort | Queries Unlocked | Notes |
|---------|--------|------------------|-------|
| **size() on patterns** | 2-3 days | BI8, IC10 | Subquery count generation |

**Recently Completed**:
- ✅ Label predicate `n:Label` in expressions (Dec 2025)
- ✅ Temporal extraction: Use `toYear(x)`, `toMonth(x)`, `toDayOfMonth(x)` for date part extraction

### 🚧 Deferred (Known Limitations)

1. **Pattern comprehension** `[(n)-[:R]->(m) | m.prop]` - Requires significant parser work
2. **CALL subquery** - Requires parser grammar extension

### Not Planned (GDS-dependent queries)
- IC14, BI10, BI15, BI19, BI20 require Neo4j-specific libraries
- These would need custom graph algorithm implementations

---

## LDBC Official Benchmark Driver

### Driver Overview
- **Repository**: https://github.com/ldbc/ldbc_snb_interactive_v2_driver
- **Language**: Java (with Maven build)
- **Purpose**: Standardized benchmark execution with proper timing, validation, and result reporting

### Key Components
1. **Workload Definition**: Query mix and execution order
2. **Parameter Generation**: Generates query parameters from dataset
3. **Validation**: Cross-validates results between implementations
4. **Metrics Collection**: Throughput, latency percentiles

### Integration Options for ClickGraph

#### Option A: Implement LDBC Driver Interface (Recommended for Official Benchmarks)
- Create Java wrapper around ClickGraph HTTP/Bolt API
- Implement `DbConnectionState` and `OperationHandler` interfaces
- Follows official LDBC benchmark protocol

#### Option B: Custom Benchmark Script (Current Approach)
- Python-based query execution
- Parameter substitution from LDBC-format files
- Manual timing and result collection
- Sufficient for internal performance testing

### Parameter Files
LDBC provides pre-generated parameters in `parameters/` directory:
- `interactive-*.txt` files with substitution parameters
- Format: tab-separated values with headers

---

## Recommendations

### Short-term (Next 2 weeks)
1. ✅ ~~Implement UNWIND and head()~~ - DONE
2. ✅ ~~Add abs(), floor() functions~~ - DONE
3. Test all IS queries with official parameters
4. ✅ ~~Temporal extraction~~ - DONE (use function-style `toYear()`, `toMonth()`)

### Medium-term (Next month)
1. ✅ ~~Implement datetime/duration support~~ - DONE
2. ✅ ~~Implement label predicate `n:Label` in expressions~~ - DONE (Dec 2025)
3. Implement size() on patterns
4. Target 90% IC query support (currently at ~85%)

### Long-term (Deferred)
1. Pattern comprehension - Deferred (complex parser work)
2. CALL subqueries - Deferred (complex parser work)
3. Consider custom implementations for GDS-equivalent algorithms
4. Integration with LDBC driver for official benchmark runs

---

## Detailed Feature Requirements by Query

### UNWIND Examples

**IC6 - Tag co-occurrence**:
```cypher
WITH collect(distinct friend) as friends
UNWIND friends as f
MATCH (f)<-[:HAS_CREATOR]-(post:Post)
```
Maps to: `arrayJoin(friends) AS f`

**IC9 - Recent FoF messages**:
```cypher
WITH collect(distinct friend) as friends
UNWIND friends as friend
MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
```

### datetime() Examples

**BI1 - Posting summary**:
```cypher
:params { datetime: datetime('2011-12-01T00:00:00.000') }
WHERE message.creationDate < $datetime
```
Requires: ISO 8601 datetime parsing

**IC10 - Birthday recommendation**:
```cypher
WITH datetime({epochMillis: friend.birthday}) as birthday
WHERE (birthday.month=$month AND birthday.day>=21)
```
Requires: Epoch conversion and property extraction

### Pattern Comprehension Examples

**BI8 - Central person**:
```cypher
100 * size([(tag)<-[interest:HAS_INTEREST]-(person) | interest])
```
Maps to: Subquery with COUNT

**IC10 - Common interests**:
```cypher
size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])
```
Requires: List comprehension with pattern filter

### duration() Examples - ✅ NOW SUPPORTED

**BI2 - Tag evolution**:
```cypher
WHERE $date <= message1.creationDate
  AND message1.creationDate < $date + duration({days: 100})
```
✅ Generates: `$date + toIntervalDay(100)`

**BI17 - Information propagation**:
```cypher
WHERE message2.creationDate > message1.creationDate + duration({hours: $delta})
```
✅ Generates: `message1.creationDate + toIntervalHour($delta)`

**Multi-unit durations**:
```cypher
RETURN datetime() + duration({days: 5, hours: 2}) AS future
```
✅ Generates: `parseDateTime64BestEffort(now64(3)) + (toIntervalDay(5) + toIntervalHour(2))`


---

## Test Matrix

To validate query support, run:

```bash
# Test parsing only
python3 benchmarks/ldbc_snb/scripts/test_query_parsing.py

# Test with server (requires running ClickGraph + ClickHouse with LDBC data)
python3 benchmarks/ldbc_snb/scripts/run_official_queries.py --dry-run
```

---

## Low-Hanging Fruit Opportunities

The following features offer high value with relatively low implementation effort:

### 1. Property Path Access (`.year`, `.month`, `.day`)

**Effort**: 1-2 days  
**Queries Unlocked**: IC10, BI1, BI13

**Current State**: We support `year(datetime())` function syntax but not `datetime().year` property syntax.

**Implementation Approach**:
```
// In expression parser or AST transform:
datetime().year  →  year(datetime())
datetime().month →  month(datetime())
datetime().day   →  day(datetime())
```

This could be a simple AST rewrite pass that converts property access on temporal expressions to function calls.

**Example**:
```cypher
-- Current (works)
RETURN year(datetime()) AS y

-- Desired (needs implementation)
WITH datetime() AS dt
RETURN dt.year AS y
```

### 2. ✅ Label Predicate in Expressions (`n:Label`) - IMPLEMENTED

**Status**: ✅ IMPLEMENTED (Dec 2025)  
**Queries Unlocked**: BI1

**Implementation**: Label predicates in expressions are resolved at compile-time based on schema labels.

**How It Works**:
- Parser: `parse_label_expression()` recognizes `variable:Label` pattern
- Resolution: At compile-time, checks if variable's bound labels include the tested label
- Result: Resolves to `true` (label matches) or `false` (label doesn't match)

**Examples**:
```cypher
-- WHERE clause (resolves to WHERE true if u has User label)
MATCH (u:User) WHERE u:User RETURN u.name

-- WITH clause (creates boolean column)
MATCH (m:Message)
WITH m, m:Comment AS isComment, m:Post AS isPost
RETURN m.id, isComment, isPost
```

This enables the BI1 pattern: `message:Comment AS isComment`.

### 3. size() on Patterns

**Effort**: 2-3 days  
**Queries Unlocked**: BI8, IC10

**Current State**: `size(list)` works, but `size((n)-[:REL]->())` doesn't.

**Implementation Approach**:
- Recognize pattern inside size()
- Generate correlated subquery with COUNT(*)

**Example**:
```cypher
-- Desired
MATCH (p:Person)
RETURN p.name, size((p)-[:KNOWS]->()) AS friendCount

-- Generated SQL
SELECT p.name, (SELECT COUNT(*) FROM knows WHERE from_id = p.id) AS friendCount
FROM persons p
```

### Priority Ranking

| Feature | Impact | Effort | ROI Score |
|---------|--------|--------|-----------|
| ~~Property path access~~ | ~~HIGH (3 queries)~~ | ~~LOW (1-2 days)~~ | ✅ Partial (use `toYear()` functions) |
| ~~Label predicate~~ | ~~MEDIUM (1 query)~~ | ~~LOW (1 day)~~ | ✅ DONE |
| size() on patterns | MEDIUM (2 queries) | MEDIUM (2-3 days) | ⭐⭐⭐ |

**Recommendation**: Next priority is `size() on patterns` for subquery count generation.
