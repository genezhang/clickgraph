# LDBC Remaining Issues Analysis
**Date**: December 18, 2025  
**Status**: 23/41 (56%) queries passing after parameter fix

## Recent Progress

**December 18, 2025**: Added missing parameters to audit script
- **Impact**: 14/41 (34%) → 23/41 (56%) (+9 queries, +64%)
- **Fixed queries**: short-4, short-5, short-6, complex-2, complex-3, complex-5, complex-6, complex-14, bi-12
- **Commit**: 6d55dbf

**December 15, 2025**: Filtered view support
- **Impact**: 11/41 (27%) → 14/41 (34%) before params fix
- **Fixed**: Company→Organisation, City/Country→Place filtered views

## Issue Categorization

### Out of Scope (7 queries - APOC/GDS procedures)
These queries use Neo4j stored procedures that aren't part of standard Cypher:

| Query | Issue | Procedure Used |
|-------|-------|----------------|
| bi-10 | APOC path traversal | `apoc.path.subgraphNodes()` |
| bi-13 | GDS graph projection | `gds.graph.project.cypher()` |
| bi-15 | GDS graph projection | `gds.graph.project.cypher()` |
| bi-19 | GDS shortest path | `gds.shortestPath.dijkstra.stream()` |
| bi-20 | GDS shortest path | `gds.shortestPath.dijkstra.stream()` |
| bi-17 | Unknown CALL | (need to verify) |
| complex-13 | Unknown CALL | (need to verify) |

**Decision**: Document as known limitation. These require APOC/GDS library support or query rewrites.

---

### Test Harness Issues (9 queries - missing parameters)
These fail due to missing query parameters in test harness, not code bugs:

| Query | Missing Parameter |
|-------|-------------------|
| short-4, short-5, short-6 | messageId |
| complex-2 | maxDate |
| complex-3 | countryXName |
| complex-5 | minDate |
| complex-6 | tagName |
| complex-14 | person1Id |
| bi-12 | languages |

**Decision**: Fix test harness parameter passing. These queries likely work when parameters are provided.

---

### Code Bugs (11 queries)

#### 1. Polymorphic Node Relationships (6 queries) 🎯 **HIGH IMPACT**

**Root Cause**: Relationships are defined on concrete types (Post, Comment), but queries use polymorphic type (Message).

**Examples**:
- `MATCH (p:Person)-[:LIKES]->(m:Message)` generates `Person_likes_Message` (doesn't exist)
- Schema has: `Person_likes_Post` and `Person_likes_Comment`
- Message is union view with `label_column: type`

**Affected Queries**:
- complex-7: `Person LIKES Message`
- complex-8: `Message REPLY_OF ...` (Message as source)
- complex-11: `Person WORK_AT Message` (?)
- short-7: `Message REPLY_OF Comment`
- complex-10: `Message HAS_CREATOR Person`
- bi-5: `HAS_CREATOR::Message::Person` pattern

**Solution Approach**:
When relationship involves polymorphic node, expand to UNION:
```sql
-- Query: Person LIKES Message
SELECT ... FROM Person JOIN Person_likes_Post
UNION ALL
SELECT ... FROM Person JOIN Person_likes_Comment
```

**Implementation**:
1. Detect polymorphic nodes (has `label_column` field)
2. Find concrete types (iterate schema to find nodes with same table)
3. Resolve relationships for each concrete type
4. Generate UNION SQL

**Complexity**: Medium-High (1-2 days)
- Changes: graph_schema.rs, query_validation.rs, clickhouse_query_generator/
- Requires UNION query support in SQL generation

**Impact**: Fixes 6 queries → 29/41 (71%) passing (excluding APOC, assuming params work)

---

#### 2. Multi-Pattern MATCH with Variable Reuse (3 queries)

**Root Cause**: CTE scoping issue when variable is used across multiple MATCH clauses separated by WITH.

**Example** (short-2):
```cypher
MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(message)
WITH message, message.id AS messageId
ORDER BY creationDate DESC
LIMIT 10
MATCH (message)-[:REPLY_OF]->(p:Post), (p)-[:HAS_CREATOR]->(person)
-- Error: "Property 'id' not found on node 'person'"
-- Also: CTE 'rel_p_person' not visible in second MATCH
```

**Affected Queries**:
- short-2: Property 'id' not found on node 'person'
- complex-9: Property 'id' not found on node 'friend'
- bi-16: Property 'letter' not found on node 'param'

**Solution Approach**:
- Fix CTE scoping to make relationship CTEs visible across WITH boundaries
- Ensure variable bindings propagate correctly through WITH clauses
- Handle comma-separated patterns in MATCH correctly

**Complexity**: Medium (2-3 days)
- Changes: query_planner/plan_ctx/, logical_plan/, clickhouse_query_generator/
- Requires tracking variable bindings across WITH boundaries

**Impact**: Fixes 3 queries → 26/41 (63%) passing

---

#### 3. WITH Clause Validation (2 queries)

**Root Cause**: Expressions in WITH must have explicit aliases.

**Examples**:
- bi-8: `WITH collect(...) * 100.0` (multiply operator without alias)
- bi-14: `WITH collect(post)` (aggregate without alias)

**Error**: "WITH clause validation error: Expression without alias"

**Affected Queries**:
- bi-8: Operator application without alias
- bi-14: Aggregate function without alias

**Solution Approach**:
Either:
- Auto-generate aliases for expressions in WITH clause, OR
- Improve error message to suggest adding alias

**Complexity**: Low (1-2 hours)
- Changes: query_planner/analyzer/query_validation.rs

**Impact**: Fixes 2 queries → 25/41 (61%) passing

---

## Priority Recommendations

### Option A: Polymorphic Node Relationships (HIGHEST IMPACT)
- **Fixes**: 6 queries (complex-7, complex-8, complex-11, short-7, complex-10, bi-5)
- **Impact**: 23/41 → 29/41 (71%) passing
- **Complexity**: Medium-High (1-2 days)
- **Value**: Enables Message abstract type queries (common pattern)

### Option B: WITH Clause Validation (QUICKEST WIN)
- **Fixes**: 2 queries (bi-8, bi-14)
- **Impact**: 23/41 → 25/41 (61%) passing
- **Complexity**: Low (1-2 hours)
- **Value**: Low-hanging fruit, good for momentum

### Option C: Multi-Pattern MATCH (MEDIUM IMPACT)
- **Fixes**: 3 queries (short-2, complex-9, bi-16)
- **Impact**: 23/41 → 26/41 (63%) passing
- **Complexity**: Medium (2-3 days)
- **Value**: Important Cypher feature

### Option D: Test Harness Parameters (EASY WINS)
- **Fixes**: 9 queries (all parameter errors)
- **Impact**: 23/41 → 32/41 (78%) passing
- **Complexity**: Low (1 hour - fix test harness)
- **Value**: Quick win, but not testing new code

---

## Recommended Sequence

1. **Test harness parameters** (1 hour) → 32/41 (78%)
2. **WITH clause validation** (2 hours) → 34/41 (83%)
3. **Polymorphic node relationships** (1-2 days) → 40/41 (98%)
4. **Multi-pattern MATCH** (2-3 days) → 43/41 (but need to subtract APOC)

**Excluding APOC** (7 queries), theoretical max is **34/34 (100%)** fixable queries.

**After sequence above**: 27/34 (79%) of fixable queries passing.

---

## APOC/GDS Queries - Long-term Strategy

For bi-10, bi-13, bi-15, bi-17, bi-19, bi-20, complex-13:

**Options**:
1. Implement APOC/GDS equivalents (huge effort)
2. Provide query rewrites using standard Cypher (bi-19, bi-20 could use `shortestPath()`)
3. Document as known limitations
4. Create "LDBC-compatible" query set without APOC

**Recommendation**: Document as limitation. Focus on standard Cypher compliance.
