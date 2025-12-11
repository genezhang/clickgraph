# Known Issues

**Active Issues**: 4  
**Last Updated**: December 10, 2025

For fixed issues and release history, see [CHANGELOG.md](CHANGELOG.md).  
For usage patterns and feature documentation, see [docs/wiki/](docs/wiki/).

---

## Active Issues

### 1. WITH+MATCH with Aggregation on Second MATCH Variables (LDBC IC-3)

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

### 2. Anti-Join Pattern (NOT relationship) - NOT IMPLEMENTED

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

### 4. CTE Column Aliasing for Mixed RETURN (WITH alias + node property)

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

### 4. Anonymous Nodes Without Labels (Partial Support)

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

## Fixed Issues (December 2025)

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
