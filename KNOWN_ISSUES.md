# Known Issues

**Active Issues**: 3 (30 LDBC queries remaining)  
**Last Updated**: December 19, 2025

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Recently Fixed

### ✅ Polymorphic Relationship Lookup (December 19, 2025)
**Fixed**: Relationships with same type but different node pairs (e.g., `IS_LOCATED_IN::Person::City` vs `IS_LOCATED_IN::Post::Place`)
- **Solution**: Thread node labels through relationship lookup pipeline
- **Impact**: LDBC audit improved from 7/41 (17%) → 11/41 (27%) queries passing
- **Commit**: 3b2c781
- **Details**: See STATUS.md and CHANGELOG.md

---

## Active Issues

### 1. LDBC Query Failures - Investigation Complete (December 19, 2025)

**Status**: 📋 ANALYZED  
**Severity**: MEDIUM  
**Current**: 29/41 (70%) LDBC queries passing ✅ ALL SHORT QUERIES PASS!

**Failure Analysis (12 remaining)**:

#### 1.1 UNWIND After WITH - Wrong FROM Table (3 queries - 25%)
**Status**: ✅ **PARTIALLY FIXED** (Dec 20, 2025) - RefCell panic prevented, core UNWIND bug remains  
**Queries**: complex-7, complex-9, bi-16  
**Error**: Empty results or incorrect SQL (uses `system.one` instead of CTE)

**Problem**:
When UNWIND appears after WITH clause, generated SQL uses `FROM system.one` instead of the CTE:
```cypher
MATCH (p:Person)-[:KNOWS]-(friend:Person) 
WITH collect(friend) as friends
UNWIND friends as friend         # Should SELECT from CTE, not system.one!
RETURN friend.firstName
```

**Generated SQL (WRONG)**:
```sql
WITH with_friends_cte AS (SELECT friend.* FROM Person...)
SELECT friend.firstName 
FROM system.one AS _dummy        -- ❌ Wrong! Should be: FROM with_friends_cte
ARRAY JOIN friends AS friend     -- ❌ 'friends' doesn't exist in system.one!
```

**Root Causes**:
1. ✅ **FIXED (Dec 20)**: RefCell panic when populating relationship columns from nested CTEs
   - `populate_relationship_columns_from_plan()` had illegal reentrancy
   - Called recursively while holding `borrow_mut()` → panic at runtime
   - Fixed by collecting CTE plans first, then processing after releasing borrow
   
2. **REMAINING**: UNWIND FROM table resolution bug
   - Code at `plan_builder.rs:9741-9750` assumes no FROM → use `system.one`
   - Should detect CTE context and use CTE as FROM table
   - ARRAY JOIN references non-existent `friends` array in `system.one`

**Location**: `src/render_plan/plan_builder.rs` lines 9741-9750  
**Complexity**: MEDIUM - Need to detect CTE context and set correct FROM table

#### 1.2 Pattern Comprehensions Not Supported (2 queries - 17%)
**Queries**: bi-8, bi-14  
**Status**: ⚠️ NOT IMPLEMENTED (clear error messages added Dec 19, 2025)  
**Error Message**: `Pattern comprehensions [(pattern) | projection] are not yet supported. Use MATCH with collect() instead.`

**What are Pattern Comprehensions?**
Pattern comprehensions are Cypher syntax for creating lists by matching graph patterns:
```cypher
[(start)-[rel:TYPE]-(end) | end.property]
```

This is like list comprehensions `[x IN list | expression]` but for graph patterns.

**bi-8 Example**:
```cypher
WITH
  person,
  100 * size([(tag)<-[interest:HAS_INTEREST]-(person) | interest])
  + size([(tag)<-[:HAS_TAG]-(message)-[:HAS_CREATOR]->(person) | message])
  AS score
```

**Parser Enhancement (Dec 19, 2025)**:

Previously, pattern comprehensions caused confusing error messages like:
- ❌ "No table context for alias `size`"  
- ❌ Server crashes with cascading parse failures

Now the parser detects pattern comprehensions early and provides:
- ✅ Clear error: "Pattern comprehensions... not yet supported"
- ✅ Helpful guidance: "Use MATCH with collect() instead"
- ✅ No more confusing fallback behavior or crashes

**Implementation Requirements**:
- New AST nodes for pattern comprehension expressions
- Parser rules for `[(pattern) | projection]` syntax  
- Query planning to convert patterns to SQL subqueries
- Projection logic for the `| projection` part

**Workaround**: Rewrite queries using explicit MATCH + collect() instead of pattern comprehensions

**Complexity**: HIGH - Requires significant parser and planner work  
**Priority**: MEDIUM - Blocks 2 LDBC BI queries, but workarounds exist

#### 1.3 Bidirectional Relationship Patterns Not Supported (1 query - 8%)
**Queries**: bi-17  
**Status**: ⚠️ NOT IMPLEMENTED (clear error messages added Dec 19, 2025)  
**Error Message**: `Bidirectional relationship patterns <-[:TYPE]-> are not supported. Use two separate MATCH clauses or the undirected pattern -[:TYPE]-.`

**What are Bidirectional Patterns?**
Bidirectional patterns use both incoming (`<-`) and outgoing (`->`) arrows on the same relationship:
```cypher
(forum)<-[:HAS_MEMBER]->(person)
```

This is non-standard Cypher syntax. Standard Cypher requires either:
- Directed: `(forum)<-[:HAS_MEMBER]-(person)` or `(forum)-[:HAS_MEMBER]->(person)`
- Undirected: `(forum)-[:HAS_MEMBER]-(person)` (matches either direction)

**bi-17 Example**:
```cypher
MATCH
  (forum1)<-[:HAS_MEMBER]->(person2:Person),  # Bidirectional pattern
  (forum1)<-[:HAS_MEMBER]->(person3:Person)   # Ensures different persons
RETURN forum1, person2, person3
```

The query uses this to ensure edge-isomorphic matching (person2 ≠ person3).

**Parser Enhancement (Dec 19, 2025)**:

Previously, bidirectional patterns caused parse errors with unclear messages.

Now the parser detects bidirectional patterns early and provides:
- ✅ Clear error: "Bidirectional relationship patterns... not supported"
- ✅ Helpful guidance: "Use two separate MATCH clauses or -[:TYPE]-"
- ✅ No more confusing parser failures

**Workaround**: Use two separate MATCH clauses or undirected patterns:
```cypher
MATCH (forum)-[:HAS_MEMBER]-(person2:Person),
      (forum)-[:HAS_MEMBER]-(person3:Person)
WHERE person2 <> person3  # Explicit inequality check
```

**Complexity**: MEDIUM - Non-standard syntax, low priority  
**Priority**: LOW - Single LDBC query, has simple workarounds

#### 1.4 Procedure Calls Not Supported (4 queries - 33%)
**Queries**: bi-10, bi-15, bi-19, bi-20  
**Error**: "SQL doesn't contain SELECT" or "No select items found"  

**Known Issues**:
- ~~complex-13: Path variable assignment in comma-separated patterns~~ ✅ **FIXED Dec 19, 2025** - Parsing now works! (SQL generation has minor bug with `t.hop_count`)
- bi-10, bi-15, bi-19, bi-20: APOC/GDS procedures (clear errors already)
- ~~bi-13: DateTime property accessors (`.year`, `.month`)~~ ✅ **FIXED Dec 19, 2025**

**Path Variable Support (Dec 19, 2025)** ✅:
Path variables in comma-separated patterns now fully parse:
- ✅ `MATCH (a:Person), (b:Person), path = shortestPath((a)-[:KNOWS*]-(b)) RETURN path`
- ✅ AST refactored: `path_patterns: Vec<(Option<&str>, PathPattern)>` (each pattern can have path variable)
- ✅ Parser updated: `parse_pattern_with_optional_variable()` function
- ⚠️ SQL generation has minor alias bug (`t.hop_count` should be `vlp1.hop_count`)

**CASE Expression Status (Dec 19, 2025)** ✅:
All CASE variants fully implemented and working:
- Simple CASE: `CASE x WHEN val1 THEN res1 WHEN val2 THEN res2 ELSE default END`
- Searched CASE: `CASE WHEN condition1 THEN res1 WHEN condition2 THEN res2 ELSE default END`  
- CASE with IS NULL: `CASE x IS NULL WHEN true THEN 'null' ELSE 'not null' END`
- SQL generation: ClickHouse `caseWithExpression()` for simple CASE, standard CASE...END for searched

**bi-13 Fix (Dec 19, 2025)**:
Temporal property accessors now converted to function calls at parser level:
- `$endDate.year` → `year($endDate)` → SQL: `toYear(?)`
- `zombie.creationDate.month` → `month(zombie.creationDate)` → SQL: `toMonth(Person.creationDate)`
- Supports chained properties and parameters
- Transparent to query planner and SQL generator

**Blocker**: LDBC schema requires `ldbc` database (not available in current setup)

**Next Steps**:
1. **UNWIND variable tracking** - Architectural improvement needed
2. **LDBC database setup** - Required for remaining 9 queries
3. Focus on other high-value features until LDBC environment available

**Progress Tracking**: `benchmarks/ldbc_snb/scripts/audit_sql_generation.py`

---

### 2. Multi-Variant CTE Column Name Mismatch After WITH Clause

**Status**: ✅ FIXED (December 18, 2025)  
**Severity**: HIGH  
**Affects**: Multi-variant relationships (e.g., `[:IS_LOCATED_IN]` mapping to multiple tables) after WITH clause  

**Problem**:
When a multi-variant relationship appears after a WITH clause, the generated JOIN conditions reference the first table's schema-specific column names instead of the CTE's standardized column names (`from_node_id`, `to_node_id`).

**Example Query**:
```cypher
MATCH (p:Person)-[:KNOWS*1..2]-(friend:Person) WHERE p.id = 14 
WITH friend LIMIT 3 
MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:Place) 
RETURN friend.firstName, friendCity.name
```

**Incorrect SQL Generated (Before Fix)**:
```sql
WITH RECURSIVE ...,
rel_friend_friendCity AS (
  SELECT PersonId AS from_node_id, CityId AS to_node_id FROM Person_isLocatedIn_Place 
  UNION ALL 
  SELECT CommentId AS from_node_id, CountryId AS to_node_id FROM Comment_isLocatedIn_Place 
  ...
)
SELECT ...
FROM with_friend_cte_1 AS friend
INNER JOIN rel_friend_friendCity AS t2 ON t2.CommentId = friend.id  -- ❌ Wrong! Should be t2.from_node_id
INNER JOIN Place AS friendCity ON friendCity.id = t2.CountryId        -- ❌ Wrong! Should be t2.to_node_id
```

**Root Cause**:
- GraphJoinInference analyzer builds Traditional JOINs using relationship schema column names
- `resolve_column()` function attempts to map schema columns to CTE columns
- No column mappings were registered when multi-variant CTE names were created
- Lookup failed → fell back to using schema column names as-is → Wrong JOIN conditions

**Fix Implementation** (December 18, 2025):
1. **Early Registration** (`src/query_planner/analyzer/graph_context.rs` lines 79-105):
   - Check `graph_rel.labels.len() > 1` BEFORE borrowing from plan_ctx
   - Collect all schema column names from all relationship schemas in the union
   - Register mappings for each: `schema_column → "from_node_id"/"to_node_id"`
   - Ensures mappings exist before GraphJoinInference analyzer runs

2. **Simple Lookup** (`src/query_planner/analyzer/graph_join_inference.rs` lines 273-295):
   - Simplified `resolve_column()` from 90+ lines to 20 lines
   - Removed unreliable GLOBAL_SCHEMAS heuristic fallback
   - Now relies entirely on registered mappings from step 1
   - Clean separation: early registration, late lookup

3. **Helper Method** (`src/query_planner/plan_ctx/mod.rs`):
   - Added `register_cte_column()` method for individual column mapping registration
   - Complements existing `register_cte_columns()` which takes ProjectionItems

**Fixed SQL** (After Fix):
```sql
INNER JOIN rel_friend_friendCity AS t2 ON t2.from_node_id = friend.id  -- ✅ Correct!
INNER JOIN Place AS friendCity ON friendCity.id = t2.to_node_id        -- ✅ Correct!
```

**Verification**:
- Build successful with new code
- Column mapping registration logs confirmed:
  ```
  🔧 Registering 4 column mappings for multi-variant CTE 'rel_u2_target'
  🔧 Mapping follower_id → from_node_id
  🔧 Mapping followed_id → to_node_id
  🔧 Mapping user_id → from_node_id
  🔧 Mapping post_id → to_node_id
  ```
- Note: Full end-to-end testing blocked by separate bug (node label lookup error)

**Files Modified**:
- `src/query_planner/analyzer/graph_context.rs` - Register mappings before table context borrows
- `src/query_planner/analyzer/graph_join_inference.rs` - Simplified resolve_column()
- `src/query_planner/plan_ctx/mod.rs` - Added register_cte_column() helper method

**Commit**: (pending)

**Test Case**: Create integration test with VLP + WITH + multi-variant relationship (blocked by label lookup bug)

---

### 2. Missing Database Prefixes After WITH Clause

**Status**: ✅ FIXED (December 19, 2025)  
**Severity**: MEDIUM  
**Affects**: Table references in JOINs after WITH clause  

**Problem** (resolved):
Tables referenced in JOINs after a WITH clause were missing database prefixes, causing "Unknown table" errors.

**Example Query**:
```cypher
MATCH (p:Person)-[:KNOWS*1..2]-(friend:Person) WHERE p.id = 14 
WITH friend LIMIT 3 
MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:Place) 
RETURN friend.firstName, friendCity.name
```

**Incorrect SQL (Before Fix)**:
```sql
-- Generated (incorrect):
INNER JOIN Place AS friendCity ON friendCity.id = t2.CountryId

-- Should be:
INNER JOIN ldbc.Place AS friendCity ON friendCity.id = t2.to_node_id
```

**Error Message**:
```
Unknown table expression identifier 'Place'
```

**Root Cause**:
- Traditional and MixedAccess JOIN strategies were using bare table names without database prefixes
- The code couldn't distinguish between CTEs (which cannot have prefixes) and base tables (which need prefixes)
- Example: Both `left_cte_name` from CTEs and base table names were treated identically

**Fix Implementation** (December 19, 2025):

1. **Added Helper Functions** (`src/query_planner/analyzer/graph_join_inference.rs`):
   - `get_table_name_with_prefix()`: For node tables (checks if CTE or base table)
   - `get_rel_table_name_with_prefix()`: For relationship tables
   - Both check `plan_ctx.get_table_ctx_from_alias_opt()` to determine table type

2. **Updated Function Signature**:
   - `handle_graph_pattern_v2()` now accepts `left_node_schema`, `right_node_schema`, `rel_schema`
   - Provides database names for base table prefix generation

3. **Fixed JOIN Creation Sites**:
   - **Traditional strategy**: Lines ~2463, ~2508, ~2547 (left/rel/right node JOINs)
   - **MixedAccess strategy**: Lines ~2752, ~2798 (node/rel JOINs)
   - All now use helper functions to add prefix only for base tables

**Logic**:
```rust
// Helper function checks:
if table_ctx.get_cte_name().is_some() {
    // CTE from WITH clause → no prefix
    return cte_name.to_string();
} else {
    // Base table → add database prefix
    return format!("{}.{}", schema.database, cte_name);
}
```

**Result**:
- CTEs (from WITH clause) remain unprefixed: `with_friend_cte_1`
- Base tables get database qualification: `ldbc.Place`, `ldbc.Person_isLocatedIn_Place`
- All 650/650 tests passing
- Query generation now produces valid ClickHouse SQL

---

### 3. Cross-Table Branching Patterns - ✅ FIXED

**Status**: ✅ FIXED (December 15, 2025)  
**Severity**: HIGH  
**Affects**: Comma patterns with shared nodes across different tables  
**Fixed By**: Commits `8e4482c` (cross-branch JOIN detection) and `b015cf0` (predicate correlation)

**Problem** (resolved):
Branching patterns with shared nodes in different tables now correctly generate JOINs:
```cypher
MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP)
WHERE srcip.ip = '192.168.1.10'
RETURN srcip.ip, d.name, dest.ip
```

**Solution**:
- Implemented cross-branch shared node detection in `GraphJoinInference::infer_graph_join()`
- Detects when `left_connection` appears in sibling branches
- Generates INNER JOIN between branches on shared node ID columns

**Generated SQL** (✅ correct):
```sql
SELECT t3.orig_h, t3.query, t4.resp_h
FROM test_zeek.conn_log AS t4
INNER JOIN test_zeek.dns_log AS t3 ON t4.orig_h = t3.orig_h
WHERE t3.orig_h = '...'
```

**Testing**: All 6 tests now passing (100%)
- ✅ test_comma_pattern_cross_table
- ✅ test_comma_pattern_full_dns_path
- ✅ test_sequential_match_same_node
- ✅ test_with_match_correlation
- ✅ test_predicate_correlation
- ✅ test_dns_then_connect_to_resolved_ip

**Files Changed**:
- `src/query_planner/analyzer/graph_join_inference.rs` - Cross-branch JOIN detection
- `src/query_planner/logical_plan/match_clause.rs` - Predicate-based correlation support

---

### 2. 4-Level WITH CTE Column References - ✅ FIXED

**Status**: ✅ FIXED (December 15, 2025)  
**Severity**: HIGH  
**Affects**: Multi-level WITH queries (4+ levels)

**Fixed Issues**:
1. ✅ Duplicate CTE generation (Dec 13, 2025)
2. ✅ Invalid JOIN conditions with out-of-scope variables (Dec 15, 2025)
3. ✅ Expression rewriting for intermediate CTEs (Dec 15, 2025)

**Example (now works)**:
```cypher
MATCH (a:User) WHERE a.user_id = 1 WITH a 
MATCH (a)-[:FOLLOWS]->(b:User) WITH a, b 
MATCH (b)-[:FOLLOWS]->(c:User) WITH b, c 
MATCH (c)-[:FOLLOWS]->(d:User) RETURN b.name, c.name, d.name
```

**Generated SQL** (✅ correct):
```sql
WITH with_b_c_cte AS (
    SELECT b.*, c.*  -- ✅ Only exported aliases
    FROM with_a_b_cte AS a_b
    JOIN user_follows_bench AS t2 ON t2.follower_id = a_b.b_user_id  -- ✅ CTE column ref
    JOIN users_bench AS c ON c.user_id = t2.followed_id
)
```

**Solution**: Added expression rewriting with reverse_mapping for generic IDs, prefixed IDs, and composite aliases.

**Testing**: 2-level, 4-level, 5-level, and N-level WITH queries all work correctly.

**Remaining**: Column selection still includes all previous aliases instead of only exported ones (minor optimization issue, doesn't affect correctness).

---

### 3. WITH Aggregation (count, collect, etc.) - ✅ FIXED

**Status**: ✅ FIXED (December 15, 2025)  
**Severity**: MEDIUM  
**Affects**: Queries using aggregation in WITH clause items

**Example (now works)**:
```cypher
MATCH (p:Person)-[:KNOWS]-(friend:Person)
WITH count(friend) as cnt
RETURN cnt
```

**Solution**: CTEs now correctly perform aggregation with proper SQL generation.

---

### 4. WITH Expression Aliases - ✅ FIXED

**Status**: ✅ FIXED (December 15, 2025)  
**Severity**: MEDIUM  
**Affects**: Queries aliasing expressions in WITH clause

**Example (now works)**:
```cypher
MATCH (p:Person)-[:KNOWS]-(friend:Person)
WITH friend.firstName AS name
RETURN name
```

**Solution**: CTEs now correctly project expression aliases with proper column names.

---

### 6. ShortestPath Undirected Alias Mapping - ✅ FIXED

**Status**: ✅ FIXED (December 17, 2025)  
**Severity**: MEDIUM  
**Affects**: Simple shortestPath queries with undirected patterns

**Problem** (resolved):
Simple shortestPath queries with undirected patterns failed with "Unknown expression identifier":

```cypher
MATCH path = shortestPath((a:Person)-[:KNOWS*1..2]-(b:Person)) 
RETURN a.id, b.id LIMIT 5
```

**Error** (before):
```
Code: 47. DB::Exception: Unknown expression identifier `a.id` in scope 
SELECT a.id AS "a.id", b.id AS "b.id"  -- ❌ a, b don't exist!
FROM vlp_cte1 AS vlp1 
INNER JOIN ldbc.Person AS start_node ON vlp1.start_id = start_node.id 
INNER JOIN ldbc.Person AS end_node ON vlp1.end_id = end_node.id
```

**Root Cause**:
- SELECT used Cypher aliases (`a`, `b`)
- FROM used VLP table aliases (`start_node`, `end_node`)
- Union branches rendered independently without VLP context
- VLP metadata existed but wasn't used during Union rendering

**Fix**: Modified `src/render_plan/plan_builder.rs`:
- Added `rewrite_vlp_union_branch_aliases()` to extract VLP metadata and rewrite SELECT aliases
- Called from `try_build_join_based_plan()` after Union branches render
- Three helper functions for mapping extraction and recursive expression rewriting

**Generated SQL** (after):
```sql
SELECT start_node.id AS "a.id", end_node.id AS "b.id"  -- ✅ Correct aliases!
```

**Impact**:
- ✅ Simple undirected shortestPath queries now work
- ✅ All Union branches with VLP CTEs properly rewritten
- ✅ LDBC IC1 query execution enabled

**Testing**: Verified with LDBC schema, SQL generation shows correct alias rewriting.

**Related**: ✅ Fixed duplicate CTE declarations (see `notes/shortestpath-cte-wrapping-fix.md`)

---

### 5. WITH Expression Aliases - ✅ FIXED

**Status**: ✅ FIXED (December 15, 2025)  
**Severity**: MEDIUM  
**Affects**: Queries with aggregation after WITH+MATCH

**Problem** (resolved):
WITH+MATCH patterns with aggregation on second MATCH variables now work correctly:

**Example (now works)**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(friend:User)
WITH friend
MATCH (friend)-[:FOLLOWS]->(b:User)
RETURN friend.name, count(b) AS msgCount
```

**Generated SQL** (✅ correct):
```sql
WITH with_friend_cte_1 AS (
  SELECT friend.* FROM users_bench AS a
  INNER JOIN user_follows_bench ON ...
  INNER JOIN users_bench AS friend ON ...
)
SELECT friend.friend_name, count(b.user_id) AS msgCount
FROM with_friend_cte_1 AS friend
INNER JOIN user_follows_bench ON friend.friend_user_id = ...
INNER JOIN users_bench AS b ON ...
GROUP BY friend.friend_name
```

**Solution**: CTEs correctly export node columns, and final query properly JOINs the CTE with second MATCH pattern.

**Testing**: Query generates valid SQL and executes successfully.

---

### 6. Anti-Join Pattern (NOT relationship) - ✅ FIXED

**Status**: ✅ FIXED (December 16, 2025)  
**Severity**: HIGH  
**Affects**: LDBC BI-18, queries with NOT patterns and comma patterns  
**Fixed By**: Commit `4e39636` (Comma Pattern fixes in logical plan, FROM extraction, and JOIN extraction)

**Problem** (resolved):
Comma patterns and NOT operators now work correctly. Both underlying bugs have been fixed.

**Example (now works)**:
```cypher
MATCH (person1:User), (person2:User)
WHERE person1.user_id < person2.user_id
  AND NOT (person1.name = person2.name)
RETURN person1.user_id, person2.user_id
LIMIT 5
```

**Generated SQL** (✅ VALID):
```sql
SELECT 
  person1.user_id AS "person1.user_id", 
  person2.user_id AS "person2.user_id"
FROM brahmand.users_bench AS person1
INNER JOIN brahmand.users_bench AS person2 
  ON person1.user_id < person2.user_id 
  AND NOT person1.full_name = person2.full_name
LIMIT 5
```

**Root Causes Fixed**:

1. **Comma Pattern Bug** - FIXED ✅
   - **Problem**: `MATCH (a:Type1), (b:Type2)` only included ONE table in FROM clause
   - **Root Cause 1**: `traverse_node_pattern()` didn't combine standalone nodes with CartesianProduct
   - **Root Cause 2**: `GraphJoins.extract_from()` treated all CartesianProducts as WITH...MATCH patterns
   - **Root Cause 3**: `GraphJoins.extract_joins()` never delegated to input when joins array is empty
   - **Fix 1**: Added CartesianProduct creation when `has_existing_plan=true` in `match_clause.rs:2070-2111`
   - **Fix 2**: Added `is_cte_reference()` to distinguish comma patterns from WITH...MATCH in `plan_builder.rs:5739-5780`
   - **Fix 3**: Added delegation to `input.extract_joins()` when joins is empty and input is CartesianProduct in `plan_builder.rs:6340-6360`

2. **NOT Boolean Operator** - Already Working ✅
   - **Status**: No fix needed, was already implemented correctly
   - **Generates**: `ON person1.user_id < person2.user_id AND NOT person1.full_name = person2.full_name`

**Testing**: All 3 test patterns passing (100%)
- ✅ Comma pattern: `MATCH (a:User), (b:User)`
- ✅ NOT operator: `WHERE NOT (a.name = b.name)`
- ✅ Combined anti-join: Both comma pattern + NOT operator

**Files Changed**:
- `src/query_planner/logical_plan/match_clause.rs` - CartesianProduct creation for standalone nodes
- `src/render_plan/plan_builder.rs` - FROM/JOIN extraction for comma patterns

---

---

### 6. CTE Column Aliasing for Mixed RETURN (WITH alias + node property) - ✅ FIXED

**Status**: ✅ FIXED (December 15, 2025)  
**Severity**: MEDIUM

**Problem** (resolved):
Mixed RETURN with both WITH aliases and node properties now works correctly:

**Example (now works)**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
ORDER BY a.name
```

**Generated SQL** (✅ correct):
```sql
WITH with_a_follows_cte_1 AS (
  SELECT anyLast(a.full_name) AS "a_name",
         a.user_id AS "a_user_id",
         count(*) AS "follows"
  FROM users_bench AS a
  INNER JOIN user_follows_bench ON ...
  GROUP BY a.user_id
  HAVING follows > 1
)
SELECT a_follows.a_name AS "a.name",
       a_follows.follows AS "follows"
FROM with_a_follows_cte_1 AS a_follows
ORDER BY a_follows.a_name
```

**Solution**: CTEs correctly export node properties with prefixed aliases (e.g., `a_name`), and outer query references them properly (e.g., `a_follows.a_name`).

**Testing**: Query generates valid SQL and executes successfully.

---

### 7. Pattern Comprehension - NOT IMPLEMENTED

**Status**: 🔴 Not Implemented  
**Severity**: LOW  
**Affects**: LDBC BI queries using `[(pattern) | expression]` syntax

**Symptom**: Parser does not recognize pattern comprehension syntax, resulting in parse errors.

**Example (fails)**:
```cypher
MATCH (p:Person)
RETURN p.name, [(p)-[:KNOWS]->(f) | f.name] AS friendNames
```

**Root Cause**: Pattern comprehension (`[pattern | expression]`) is a distinct syntactic construct that requires:
1. Parser support for the bracket-pattern-pipe syntax
2. Query planner support to convert to correlated subqueries
3. SQL generation for array aggregation with subqueries

**Workaround**: Use explicit COLLECT with OPTIONAL MATCH or separate queries:
```cypher
-- ✅ Works: Using OPTIONAL MATCH + COLLECT
MATCH (p:Person)
OPTIONAL MATCH (p)-[:KNOWS]->(f)
WITH p, collect(f.name) AS friendNames
RETURN p.name, friendNames
```

**Future**: Implementing pattern comprehension would require significant parser and planner changes. Consider priority based on user demand.

---

### 8. CALL Subquery - NOT IMPLEMENTED

**Status**: 🔴 Not Implemented  
**Severity**: LOW  
**Affects**: Queries using `CALL { ... }` subquery blocks

**Symptom**: CALL subquery blocks are ignored by the parser. The query executes using only the outer MATCH, silently omitting the CALL block.

**Example (partial execution)**:
```cypher
MATCH (p:Person)
CALL {
  WITH p
  MATCH (p)-[:KNOWS]->(f)
  RETURN count(f) AS friendCount
}
RETURN p.name, friendCount
```
The above query executes as if it were just `MATCH (p:Person) RETURN p.name`, with `friendCount` undefined.

**Root Cause**: The parser does not implement CALL subquery grammar. This is a Neo4j 4.x+ feature that allows:
- Correlated subqueries with WITH import
- UNION within subqueries
- Isolated variable scoping

**Workaround**: Restructure using WITH clauses or multiple queries:
```cypher
-- ✅ Works: Using WITH + OPTIONAL MATCH
MATCH (p:Person)
OPTIONAL MATCH (p)-[:KNOWS]->(f)
WITH p, count(f) AS friendCount
RETURN p.name, friendCount
```

**Future**: CALL subquery implementation would require parser grammar extension and planner support for correlated subquery execution. Consider priority based on user demand.

---

### 9. Anonymous Nodes Without Labels (Partial Support)

**Status**: 🟡 Partial Support  
**Severity**: LOW

**What Works** ✅:
- Label inference from relationship type: `()-[r:FLIGHT]->()` infers Airport
- Relationship type inference from typed nodes: `(a:Airport)-[r]->()` infers r:FLIGHT  
- Single-schema inference: `()-[r]->()` when only one relationship defined

**Limitations**:
- `MATCH (n)` with multiple node types requires explicit label
- Safety limit: max 4 types inferred before requiring explicit specification

**Workaround**: Specify at least one label when multiple types exist.

---

### 10. Edge Type Predicate in Expressions - NOT IMPLEMENTED

**Status**: 🟡 Not Implemented (Deferred)  
**Severity**: LOW  
**Affects**: Queries using `r:TYPE` as expression in WHERE/WITH clauses

**What Works** ✅:
- **Node label predicate** `n:Label` in expressions (WHERE/WITH) - fully implemented
- **Node polymorphic support**: Tables with `label_column` generate runtime checks
- **Edge type in MATCH**: `MATCH (a)-[r:FOLLOWS]->(b)` works correctly

**Limitation**:
Edge type predicates as expressions (`r:TYPE`) are not supported:
```cypher
-- ❌ Not supported: Edge type as expression
MATCH (a)-[r:INTERACTION]->(b)
WITH r, r:FOLLOWS AS isFollow, r:LIKES AS isLike
RETURN ...
```

**Root Cause**: Label predicates were implemented for nodes only. Edge type predicates would require:
1. Extending `LabelExpression` handling to check if variable is a relationship
2. Looking up `type_column` from `RelationshipSchema` (already exists)
3. Generating `type_column = 'TYPE'` comparison

**Workaround**: Use separate MATCH patterns or filter on edge properties:
```cypher
-- ✅ Works: Filter on edge property directly
MATCH (a)-[r]->(b)
WHERE r.type = 'FOLLOWS'
RETURN r
```

**Future**: Implementation is straightforward (mirrors node label predicate) but deferred due to low demand. Can be added if needed for specific use cases.

---

## Fixed Issues (December 2025)

### size() on Patterns - FIXED

**Status**: ✅ Fixed (Dec 11, 2025)
**Feature**: Pattern counting with `size((n)-[:REL]->())` 

Successfully implemented correlated COUNT(*) subquery generation for pattern counting. The implementation correctly infers node ID columns from relationship schema when labels aren't specified in the pattern.

**Example that works**:
```cypher
MATCH (u:User)
RETURN u.name, size((u)-[:FOLLOWS]->()) AS followerCount
```

**Generated SQL**:
```sql
SELECT u.full_name AS "u.name",
  (SELECT COUNT(*) FROM user_follows_bench 
   WHERE user_follows_bench.follower_id = u.user_id) AS "followerCount"
FROM users_bench AS u
```

**Key Features**:
- Parser: `size((n)-[:REL]->())` parses as FunctionCall with PathPattern argument
- Planner: Automatic conversion to PatternCount LogicalExpr variant
- SQL Gen: Correlated subquery with proper node ID column lookup from relationship schema
- Schema-aware: Falls back to relationship's from_node/to_node types when pattern doesn't specify labels

**Technical Details**: When the pattern `(u)-[:FOLLOWS]->()` doesn't have explicit labels, the code looks up the FOLLOWS relationship schema to determine that the from_node is "User", then looks up the User node schema to get the correct ID column (`user_id` instead of defaulting to `id`).

### Undirected VLP with WITH Clause - FIXED

**Status**: ✅ Fixed in commit (Dec 10, 2025)  
**Previously Affected**: LDBC IC-1

The issue with undirected variable-length path patterns combined with WITH clause and aggregation has been fixed. The SQL generator now correctly hoists CTE definitions from UNION branches to the outer query.

**Example that now works**:
```cypher
MATCH (p:Person {id: 933})-[:KNOWS*1..3]-(friend:Person)
WHERE friend.firstName = 'John' AND friend.id <> 933
WITH friend, count(*) AS cnt
RETURN friend.id AS friendId
```

**Technical Details**: Fixed in `plan_builder.rs`:
1. When wrapping UNION branches with outer aggregation (GROUP BY), CTEs were being lost
2. Added code to collect CTEs from all UNION branches using `flat_map()`
3. CTEs are now included in the outer RenderPlan that wraps the UNION
4. Both return paths (with and without aggregation) now preserve CTEs

### Two-Level Aggregation (WITH + RETURN) - FIXED

**Status**: ✅ Fixed in commit (Dec 9, 2025)  
**Previously Affected**: LDBC BI-12

The issue with two-level aggregation (WITH clause aggregation followed by RETURN aggregation) has been fixed. The SQL generator now correctly creates a CTE structure for the inner aggregation and an outer query for the final aggregation.

**Example that now works**:
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
WITH person, count(post) AS postCount
RETURN postCount AS messageCount, count(person) AS personCount
ORDER BY messageCount DESC
```

**Technical Details**: Fixed in `plan_builder.rs`:
1. Detects nested GroupBy pattern (GroupBy wrapping another GroupBy)
2. Creates CTE for inner query (WITH clause aggregation)
3. Outer query references CTE and performs its own GROUP BY
4. GROUP BY expressions derived from SELECT items (properly expands wildcards)

### Multi-Hop Pattern Join Ordering - FIXED

**Status**: ✅ Fixed in commit (Dec 9, 2025)  
**Previously Affected**: LDBC BI-18

The join ordering bug for multi-hop patterns like `(a)-[:REL]->(b)<-[:REL]-(c)` has been fixed. The Traditional JoinStrategy now correctly detects which node is already available and connects the edge to that node first, rather than always assuming left-to-right ordering.

**Example that now works**:
```cypher
MATCH (person1:Person)-[:KNOWS]->(mutual:Person)<-[:KNOWS]-(person2:Person)
WHERE person1 <> person2
RETURN person1.firstName, person2.firstName, count(mutual) AS mutualFriendCount
```

**Technical Details**: Fixed in `graph_join_inference.rs` - the Traditional strategy now checks `joined_entities.contains()` to determine connect order:
- If left node available: `LEFT → EDGE (via from_id) → RIGHT`
- If right node available: `RIGHT → EDGE (via to_id) → LEFT`

Also improved alias generation from UUID hex strings (`a300df5f72`) to simple counters (`t1`, `t2`) for better readability.

### OPTIONAL MATCH Anchor Detection - FIXED

**Status**: ✅ Fixed in commit (Dec 9, 2025)  
**Previously Affected**: LDBC BI-6, BI-9, BI-12

The anchor detection for OPTIONAL MATCH patterns has been improved. When one node is from a prior non-optional MATCH and the other is from the OPTIONAL MATCH, the non-optional node is correctly identified as the anchor.

**Example that now works**:
```cypher
MATCH (person:Person) 
OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(post:Post)
RETURN person.id, count(post) as posts
```

**Technical Details**: Fixed in `graph_join_inference.rs` - the Traditional strategy now considers optionality:
- If left is optional and right is not: right is anchor
- If right is optional and left is not: left is anchor  
- Otherwise: default semantic ordering

### Undirected Relationship Pattern Join Ordering - FIXED

**Status**: ✅ Fixed in commit (Dec 9, 2025)  
**Previously Affected**: LDBC BI-14

The join ordering bug for undirected relationship patterns `(a)-[:REL]-(b)` has been fixed. When generating the UNION ALL for undirected patterns, the second branch (Incoming direction) now correctly swaps both the left/right plan structures AND the connection strings, ensuring proper table reference ordering.

**Example that now works**:
```cypher
MATCH (person1:Person)-[:KNOWS]-(person2:Person)
MATCH (person1)-[:IS_LOCATED_IN]->(city1:Place)
RETURN person1.id, person2.id, city1.name
```

**Technical Details**: Fixed in `bidirectional_union.rs` - when creating Incoming branch for Either (undirected) patterns, both `left`/`right` GraphNode plans and `left_connection`/`right_connection` strings are now swapped together.

---

## Active Issues

### 1. CTE Column Aliasing for Mixed RETURN (WITH alias + node property) - ✅ FIXED

**Status**: ✅ FIXED (December 19, 2025)  
**Severity**: MEDIUM (resolved)

**Problem** (resolved):
CTE column aliases incorrectly used dot notation (e.g., `"a.age"`) instead of underscore convention (e.g., `"a_age"`), causing JOIN issues in outer queries.

**Example (now works)**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
ORDER BY a.name
```

**Root Cause**:
Two locations in `plan_builder.rs` used `format!("{}.{}", alias, property)` to create CTE column aliases, violating the established convention:
- **Inside CTE**: Use underscore (`a_name`, `a_user_id`)
- **Outer SELECT**: Use AS to map to dot notation (`SELECT a_name AS "a.name"`)

**Fix** (December 19, 2025):
Changed both format! calls in `src/render_plan/plan_builder.rs`:
1. **Line 5151** (TableAlias expansion): Changed `format!("{}.{}")` → `format!("{}_{}")`
2. **Line 5219** (Wildcard expansion): Changed `format!("{}.{}")` → `format!("{}_{}")`

**Generated SQL** (✅ now correct):
```sql
WITH with_a_follows_cte_1 AS (
  SELECT anyLast(a.full_name) AS "a_name",      -- ✅ underscore in CTE
         a.user_id AS "a_user_id",
         count(*) AS "follows"
  FROM users_bench AS a
  INNER JOIN user_follows_bench ON ...
  GROUP BY a.user_id
  HAVING follows > 1
)
SELECT a_follows.a_name AS "a.name",             -- ✅ AS maps to dot notation
       a_follows.follows AS "follows"
FROM with_a_follows_cte_1 AS a_follows
ORDER BY a_follows.a_name
```

**Testing**: 
- All 650 existing unit tests pass
- Added comprehensive test: `tests/rust/integration/cte_column_aliasing_tests.rs` (2 tests)
- Verified underscore convention in CTEs and proper AS mapping in outer SELECT

---

### 2. Anonymous Nodes Without Labels (Partial Support)

**Status**: 🟡 Partial Support  
**Severity**: LOW

**What Works** ✅:
- Label inference from relationship type: `()-[r:FLIGHT]->()` infers Airport
- Relationship type inference from typed nodes: `(a:Airport)-[r]->()` infers r:FLIGHT  
- Single-schema inference: `()-[r]->()` when only one relationship defined
- Single-node-schema inference: `MATCH (n) RETURN n` when only one node type
- Multi-hop anonymous patterns with single relationship type

**Limitations**:
- `MATCH (n)` with multiple node types requires explicit label
- Safety limit: max 4 types inferred before requiring explicit specification

**Workaround**: Specify at least one label when multiple types exist:
```cypher
MATCH (a:User)-[r]->(b:User) RETURN r  -- ✅ Works
```

---

## LDBC SNB Benchmark Status

### BI Queries (Business Intelligence)

**Original Queries**: 25/26 (96.2%)  
**Workaround Queries**: 26/26 (100%)

| Query | Original | Workaround | Issue |
|-------|----------|------------|-------|
| bi-1a | ✅ | ✅ | |
| bi-1b | ✅ | ✅ | |
| bi-2a | ✅ | ✅ | |
| bi-2b | ✅ | ✅ | |
| bi-3 | ✅ | ✅ | |
| bi-4a | ✅ | ✅ | |
| bi-4b | ✅ | ✅ | |
| bi-5 | ✅ | ✅ | |
| bi-6 | ✅ | ✅ | |
| bi-7 | ✅ | ✅ | |
| bi-8 | ✅ | ✅ | |
| bi-9 | ✅ | ✅ | |
| bi-10 | ✅ | ✅ | |
| bi-11 | ✅ | ✅ | |
| bi-12 | ✅ | ✅ | |
| bi-14 | ✅ | ✅ | |
| bi-18 | ❌ | ✅ | Anti-join pattern `NOT (a)-[:KNOWS]-(b)` not supported |
| agg-* | ✅ | ✅ | All 6 aggregation queries |
| geo-dist | ✅ | ✅ | |
| forum-activity | ✅ | ✅ | |
| tag-class | ✅ | ✅ | |

### Interactive Short (IS) Queries

**Status**: 4/4 (100%)

| Query | Status | Notes |
|-------|--------|-------|
| is1 | ✅ | Person lookup |
| is2 | ✅ | Recent messages |
| is3 | ✅ | Friends |
| is5 | ✅ | Creator of message |

### Interactive Complex (IC) Queries

**Original Queries**: 3/4 (75%)

| Query | Status | Issue |
|-------|--------|-------|
| ic1 | ✅ | Fixed: Undirected VLP + WITH now generates correct CTE SQL |
| ic2 | ✅ | |
| ic3 | ❌ | WITH+MATCH with nested relationships (3+ hops after WITH) |
| ic9 | ✅ | |

---

## Test Statistics

| Category | Passing | Total | Rate |
|----------|---------|-------|------|
| Unit Tests | 621 | 621 | 100% |
| Integration (social_benchmark) | 391 | 391 | 100% |
| Integration (security_graph) | 391 | 391 | 100% |
| LDBC BI (workaround) | 26 | 26 | 100% |
| LDBC BI (original) | 25 | 26 | 96.2% |
| LDBC IS | 4 | 4 | 100% |
| LDBC IC (original) | 3 | 4 | 75% |
