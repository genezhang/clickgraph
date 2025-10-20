# OPTIONAL MATCH Implementation Design

**Date**: October 17, 2025  
**Feature**: OPTIONAL MATCH support for null-safe pattern matching  
**Status**: Planning Phase

## Overview

Implement `OPTIONAL MATCH` clause to support left-join semantics in Cypher queries. This allows pattern matching where unmatched patterns return null values instead of filtering out rows.

## OpenCypher Semantics

### Basic Behavior
```cypher
MATCH (person:Person)
OPTIONAL MATCH (person)-[:KNOWS]->(friend:Person)
RETURN person.name, friend.name
```

**Expected behavior**:
- Returns all persons
- If person has friends, returns their names
- If person has no friends, returns NULL for friend.name
- Similar to SQL LEFT JOIN

### Key Characteristics
1. **Null handling**: Unmatched patterns produce null values
2. **Multiple optional matches**: Each is independent
3. **Chaining**: Can chain MATCH and OPTIONAL MATCH
4. **WHERE clauses**: Filters apply before optional matching
5. **Property access**: Properties from optional patterns can be null

## SQL Mapping Strategy

### Current MATCH Translation
```cypher
MATCH (a:User)-[r:FRIEND]->(b:User)
RETURN a.name, b.name
```

Becomes:
```sql
SELECT a.name, b.name
FROM user AS a
INNER JOIN friendship AS r ON a.user_id = r.from_id
INNER JOIN user AS b ON b.user_id = r.to_id
```

### OPTIONAL MATCH Translation
```cypher
MATCH (a:User)
OPTIONAL MATCH (a)-[r:FRIEND]->(b:User)
RETURN a.name, b.name
```

Should become:
```sql
SELECT a.name, b.name
FROM user AS a
LEFT JOIN friendship AS r ON a.user_id = r.from_id
LEFT JOIN user AS b ON b.user_id = r.to_id
```

## Implementation Plan

### Phase 1: AST Extension ✅
**Files to modify**: `brahmand/src/open_cypher_parser/ast.rs`

Add `OptionalMatchClause` to AST:
```rust
#[derive(Debug, PartialEq, Clone)]
pub struct OpenCypherQueryAst<'a> {
    pub match_clause: Option<MatchClause<'a>>,
    pub optional_match_clauses: Vec<OptionalMatchClause<'a>>,  // NEW
    // ... rest of fields
}

#[derive(Debug, PartialEq, Clone)]
pub struct OptionalMatchClause<'a> {
    pub path_patterns: Vec<PathPattern<'a>>,
    pub where_clause: Option<WhereClause<'a>>,  // Optional WHERE per clause
}
```

**Why Vec instead of Option**: Multiple OPTIONAL MATCH clauses are allowed in sequence.

### Phase 2: Parser Extension ✅
**Files to modify**: `brahmand/src/open_cypher_parser/mod.rs`

Update parser to recognize OPTIONAL MATCH:
```rust
// Parse MATCH clause
let (input, match_clause) = opt(match_clause::parse_match_clause).parse(input)?;

// Parse zero or more OPTIONAL MATCH clauses
let (input, optional_match_clauses) = many0(optional_match_clause::parse_optional_match_clause).parse(input)?;
```

**New file**: `brahmand/src/open_cypher_parser/optional_match_clause.rs`
- Similar structure to `match_clause.rs`
- Parse "OPTIONAL MATCH" keyword (two words!)
- Reuse `path_pattern` parser
- Support optional WHERE clause per OPTIONAL MATCH

### Phase 3: Logical Plan ✅
**Files to modify**: `brahmand/src/query_planner/logical_plan/`

Add `OptionalMatch` plan node:
```rust
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan { ... },
    Filter { ... },
    Project { ... },
    Join { ... },
    OptionalMatch {  // NEW
        input: Box<LogicalPlan>,
        pattern: PathPattern,
        join_type: JoinType::LeftOuter,
    },
    // ... rest
}
```

### Phase 4: Query Planning ✅
**Files to modify**: `brahmand/src/query_planner/analyzer/`

Update analyzer to handle optional patterns:
- Process OPTIONAL MATCH after MATCH
- Build left-join logical plans
- Handle null propagation in projections
- Validate that OPTIONAL MATCH references bound variables

### Phase 5: SQL Generation ✅
**Files to modify**: `brahmand/src/clickhouse_query_generator/`

Generate LEFT JOIN SQL:
```rust
fn generate_optional_match_sql(
    plan: &OptionalMatchPlan,
    context: &mut GeneratorContext,
) -> Result<String> {
    // Generate LEFT JOIN instead of INNER JOIN
    // Handle nullable columns in SELECT
    // Preserve null values in result set
}
```

**Key considerations**:
- ClickHouse LEFT JOIN syntax
- Nullable column handling
- CTEs for complex patterns
- Performance with large optional branches

### Phase 6: Testing ✅
**Test coverage needed**:

1. **Simple optional relationship**
   ```cypher
   MATCH (a:User)
   OPTIONAL MATCH (a)-[:FRIEND]->(b:User)
   RETURN a.name, b.name
   ```

2. **Optional with WHERE filter**
   ```cypher
   MATCH (a:User)
   OPTIONAL MATCH (a)-[:FRIEND]->(b:User)
   WHERE b.age > 25
   RETURN a.name, b.name
   ```

3. **Multiple optional matches**
   ```cypher
   MATCH (a:User)
   OPTIONAL MATCH (a)-[:FRIEND]->(b:User)
   OPTIONAL MATCH (a)-[:AUTHORED]->(p:Post)
   RETURN a.name, b.name, p.title
   ```

4. **Chained optional patterns**
   ```cypher
   MATCH (a:User)
   OPTIONAL MATCH (a)-[:FRIEND]->(b:User)-[:AUTHORED]->(p:Post)
   RETURN a.name, b.name, p.title
   ```

5. **Property access on optional nodes**
   ```cypher
   MATCH (a:User)
   OPTIONAL MATCH (a)-[:FRIEND]->(b:User)
   RETURN a.name, b.name, b.age  -- b.age can be null
   ```

6. **Aggregations with optional matches**
   ```cypher
   MATCH (a:User)
   OPTIONAL MATCH (a)-[:FRIEND]->(b:User)
   RETURN a.name, COUNT(b) AS friend_count
   ```

## Edge Cases & Challenges

### 1. Variable Scoping
- OPTIONAL MATCH can only reference variables from previous MATCH
- Cannot introduce new standalone patterns

### 2. Null Handling in WHERE
```cypher
MATCH (a:User)
OPTIONAL MATCH (a)-[:FRIEND]->(b:User)
WHERE b.age > 25  -- Filter applied to optional pattern
RETURN a.name, b.name
```

WHERE on optional pattern should be part of JOIN condition, not final filter.

### 3. Multiple Optional Matches
Each OPTIONAL MATCH is independent - needs separate LEFT JOINs:
```sql
FROM user AS a
LEFT JOIN friendship AS r1 ON a.user_id = r1.from_id
LEFT JOIN user AS b ON b.user_id = r1.to_id
LEFT JOIN authored AS r2 ON a.user_id = r2.from_id
LEFT JOIN post AS p ON p.post_id = r2.to_id
```

### 4. Variable-Length Optional Paths
```cypher
MATCH (a:User)
OPTIONAL MATCH (a)-[:FRIEND*1..3]->(b:User)
RETURN a.name, b.name
```

Combining recursive CTEs with LEFT JOIN - complex!

## Implementation Order

1. ✅ **Research & Design** (this document)
2. ⏭️ **Simple optional relationship** - Basic LEFT JOIN for single relationship
3. **Optional with properties** - Handle nullable property access
4. **Multiple optional matches** - Independent LEFT JOINs
5. **Optional with WHERE** - Join condition vs final filter
6. **Chained optional patterns** - Multiple hops in optional branch
7. **Aggregations** - COUNT, SUM with nulls
8. **Variable-length optional** - Future enhancement

## Success Criteria

- ✅ Parser accepts OPTIONAL MATCH syntax
- ✅ Simple optional relationships generate correct LEFT JOIN SQL
- ✅ Null values preserved in results for unmatched patterns
- ✅ Multiple optional matches work independently
- ✅ 20+ tests covering common use cases
- ✅ Documentation with examples
- ✅ Compatible with existing MATCH queries

## References

- OpenCypher Specification: https://opencypher.org/
- Neo4j OPTIONAL MATCH docs: https://neo4j.com/docs/cypher-manual/current/clauses/optional-match/
- ClickHouse LEFT JOIN docs: https://clickhouse.com/docs/en/sql-reference/statements/select/join

## Next Steps

1. Mark task #1 complete - research done ✅
2. Start with AST extension (task #2)
3. Implement parser (task #3)
4. Build logical plan (task #4)
5. Generate SQL (task #5)
6. Add tests (task #6)
7. Document feature (task #7)
