# Known Issues

**Active Issues**: 12  
**Last Updated**: December 14, 2025

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Active Issues

### 1. Cross-Table Branching Patterns - JOIN Generation Broken

**Status**: 🔴 BROKEN (regression from refactor)  
**Severity**: HIGH  
**Affects**: Comma patterns with shared nodes across different tables  
**Introduced**: Recent refactor  
**Tests**: 6 skipped tests in `TestCrossTableCorrelation`

**Problem**:
Branching patterns with shared nodes in different tables don't generate JOINs:
```cypher
MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP)
WHERE srcip.ip = '192.168.1.10'
RETURN srcip.ip, d.name, dest.ip
```

**Expected**: JOIN dns_log and conn_log on shared `srcip` node (orig_h column)
**Actual**: Only one table in FROM clause, missing JOIN, alias mismatch errors

**Generated SQL** (❌ broken):
```sql
SELECT t3.orig_h AS "srcip.ip", t3.query AS "d.name", t4.resp_h AS "dest.ip"
FROM test_zeek.conn_log AS t4  -- Missing dns_log!
WHERE t3.orig_h = '...'         -- t3 not defined!
```

**Needed SQL** (✅ correct):
```sql
SELECT t3.orig_h, t3.query, t4.resp_h
FROM test_zeek.dns_log AS t3
JOIN test_zeek.conn_log AS t4 ON t3.orig_h = t4.orig_h
WHERE t3.orig_h = '...'
```

**Root Cause**:
- `GraphJoinInference::infer_graph_join()` designed for linear patterns (node-edge-node)
- Doesn't detect cross-branch node sharing (srcip in both branches)
- Nested GraphRel structure: outer GraphRel has LEFT=inner GraphRel, both share `srcip`
- Need to detect shared nodes between sibling branches and generate appropriate JOINs

**Analysis**:
- Both GraphRels have `left_connection: "srcip"`
- Both are denormalized (no node tables, properties in edge tables)
- Should JOIN on srcip.ip (orig_h column) between dns_log (t3) and conn_log (t4)
- `collect_graph_joins` processes both branches but doesn't recognize shared anchor

**Impact**: 6/24 Zeek tests failing (all cross-table correlation patterns)

**Skipped Tests**:
- test_comma_pattern_cross_table
- test_comma_pattern_full_dns_path
- test_sequential_match_same_node
- test_with_match_correlation
- test_predicate_correlation
- test_dns_then_connect_to_resolved_ip

**Files to Fix**:
- `src/query_planner/analyzer/graph_join_inference.rs` - `infer_graph_join()` method
- Need to detect when left_connection appears in sibling branches
- Generate JOIN between branches on shared node ID columns

**Related**: This was working before - likely regression from major refactor

---

### 2. 4-Level WITH CTE Column References - INCOMPLETE

**Status**: 🟡 Partial Fix (duplicate CTEs resolved, column refs remain broken)  
**Severity**: HIGH  
**Affects**: Multi-level WITH queries (4+ levels)

**Fixed**: ✅ Duplicate CTE generation (Dec 13, 2025)
- Same CTE no longer appears twice in WITH clause
- Deduplication checks CTE name in `all_ctes` before creation

**Remaining Issues**:
1. **Invalid JOIN conditions**: CTE uses out-of-scope variables in joins
2. **Incorrect column selection**: CTE selects all previous aliases instead of only exported ones

**Example (partially works)**:
```cypher
MATCH (a:User) WHERE a.user_id = 1 WITH a 
MATCH (a)-[:FOLLOWS]->(b:User) WITH a, b 
MATCH (b)-[:FOLLOWS]->(c:User) WITH b, c 
MATCH (c)-[:FOLLOWS]->(d:User) RETURN b.name, c.name, d.name
```

**Current Generated SQL** (broken):
```sql
WITH with_b_c_cte AS (
    SELECT a.*, b.*, c.*  -- ❌ Should only select b.*, c.*
    FROM with_a_b_cte AS a_b
    JOIN user_follows_bench AS t2 ON t2.follower_id = b.id  -- ❌ b.id out of scope
    JOIN users_bench AS c ON c.user_id = t2.followed_id
)
```

**Expected SQL**:
```sql
WITH with_b_c_cte AS (
    SELECT b.*, c.*  -- ✅ Only exported aliases
    FROM with_a_b_cte AS a_b
    JOIN user_follows_bench AS t2 ON t2.follower_id = a_b.b_user_id  -- ✅ CTE column ref
    JOIN users_bench AS c ON c.user_id = t2.followed_id
)
```

**Root Cause**: CTE content generation doesn't filter columns based on `exported_aliases` from WithClause.

**Workaround**: Avoid 4+ level WITH queries until fixed.

---

### 2. WITH Aggregation (count, collect, etc.) - INCOMPLETE

**Status**: 🟡 Partial Implementation  
**Severity**: MEDIUM  
**Affects**: Queries using aggregation in WITH clause items

**Symptom**: `WITH count(x) AS cnt` generates CTE with raw columns instead of performing aggregation.

**Example (fails)**:
```cypher
MATCH (p:Person)-[:KNOWS]-(friend:Person)
WITH count(friend) as cnt
RETURN cnt
```

**Current Behavior**: CTE selects all columns, then tries to use `cnt` as table alias.

**Expected Behavior**: CTE should perform `SELECT count(*) as cnt FROM ...`.

**Workaround**: Use aggregation in RETURN clause instead:
```cypher
MATCH (p:Person)-[:KNOWS]-(friend:Person)
RETURN count(friend) as cnt
```

---

### 2. WITH Expression Aliases - INCOMPLETE

**Status**: 🟡 Partial Implementation  
**Severity**: MEDIUM  
**Affects**: Queries aliasing expressions in WITH clause

**Symptom**: `WITH x.prop AS alias` generates CTE with raw columns instead of projected alias.

**Example (fails)**:
```cypher
MATCH (p:Person)-[:KNOWS]-(friend:Person)
WITH friend.firstName AS name
RETURN name
```

**Workaround**: Keep the full reference through:
```cypher
MATCH (p:Person)-[:KNOWS]-(friend:Person)
WITH friend
RETURN friend.firstName AS name
```

---

### 5. WITH+MATCH with Aggregation on Second MATCH Variables (LDBC IC-3)

**Status**: 🟡 Partial Implementation  
**Severity**: MEDIUM  
**Affects**: LDBC IC-3, queries with aggregation after WITH+MATCH

**Symptom**: WITH+MATCH patterns where the aggregation references variables from the second MATCH can fail with "Unknown identifier" errors. The CTE incorrectly includes joins from the second MATCH.

**Example (fails - IC-3 pattern)**:
```cypher
MATCH (p:Person)-[:KNOWS*1..2]-(friend:Person)
WITH friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:LOCATED_IN]->(country:Country)
RETURN friend.id, count(post) AS msgCount  // <-- aggregation on post
```

**Example (works - IC-9 pattern)**:
```cypher
MATCH (p:Person)-[:KNOWS*1..2]-(friend:Person)  
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
RETURN friend.id, post.id, post.content  // <-- no aggregation, just projections
```

**Root Cause**: When bidirectional patterns (UNION) combine with WITH+MATCH CTEs, the second MATCH's joins incorrectly leak into the CTE definition. The CTE references `post.id` before `post` is defined.

**Workaround**: Avoid aggregations on second MATCH variables, or restructure query to avoid WITH clause with bidirectional patterns.

---

### 6. Anti-Join Pattern (NOT relationship) - NOT IMPLEMENTED

**Status**: 🔴 Not Implemented  
**Severity**: HIGH  
**Affects**: LDBC BI-18 (original query)

**Symptom**: Queries using `NOT (a)-[:REL]-(b)` to exclude relationships fail with parsing or generation errors.

**Example (fails)**:
```cypher
MATCH (person1:Person)-[:KNOWS]-(mutual:Person)-[:KNOWS]-(person2:Person)
WHERE person1.id <> person2.id AND NOT (person1)-[:KNOWS]-(person2)
RETURN person1.id, person2.id, count(DISTINCT mutual) AS mutualFriendCount
```

**Root Cause**: Anti-join patterns require generating `NOT EXISTS` or `LEFT JOIN ... WHERE ... IS NULL` SQL, which is not yet implemented.

**Workaround**: Use directed patterns without the NOT clause:
```cypher
-- ✅ Works: Directed pattern, no anti-join
MATCH (person1:Person)-[:KNOWS]->(mutual:Person)-[:KNOWS]->(person2:Person)
WHERE person1.id <> person2.id
RETURN person1.id, person2.id, count(DISTINCT mutual) AS mutualFriendCount
```

---

---

### 6. CTE Column Aliasing for Mixed RETURN (WITH alias + node property)

**Status**: 🟡 Partial  
**Severity**: MEDIUM

**Symptom**: When RETURN references both WITH aliases AND node properties, the JOIN condition may use incorrect column names.

**Example**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
ORDER BY a.name
```

**Root Cause**: CTE column aliases include the table prefix (e.g., `"a.age"`) but the outer query JOIN tries to reference `grouped_data.age` (without prefix).

**Workaround**: Ensure RETURN only references WITH clause output:
```cypher
-- ✅ Works: RETURN only references WITH output
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a.name as name, COUNT(b) as follows
WHERE follows > 1
RETURN name, follows
```

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

### 1. CTE Column Aliasing for Mixed RETURN (WITH alias + node property)

**Status**: 🔴 Active  
**Severity**: MEDIUM

**Symptom**: When RETURN references both WITH aliases AND node properties, the JOIN condition may use incorrect column names.

**Example**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
ORDER BY a.name
```

**Root Cause**: CTE column aliases include the table prefix (e.g., `"a.age"`) but the outer query JOIN tries to reference `grouped_data.age` (without prefix).

**Workaround**: For queries that only need WITH aliases in RETURN (no additional node properties), the optimization correctly skips the JOIN and selects directly from CTE. Ensure RETURN only references WITH clause output:
```cypher
-- ✅ Works: RETURN only references WITH output
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a.name as name, COUNT(b) as follows
WHERE follows > 1
RETURN name, follows
```

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
