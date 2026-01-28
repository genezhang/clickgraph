# Pattern Comprehensions

**Status**: ✅ Complete (Dec 25, 2025)  
**Version**: v0.6.0  
**Test Coverage**: 5/5 integration tests passing (100%)

## Summary

Pattern comprehensions provide a concise syntax for collecting values from graph patterns, combining pattern matching and projection into a single expression that returns a list.

**Syntax**: `[(pattern) WHERE condition | projection]`

## How It Works

### Architecture

Pattern comprehensions are implemented through a **query rewriting** approach:

1. **Parser** (`open_cypher_parser/expression.rs`):
   - Parses pattern comprehension syntax into AST
   - Supports optional WHERE clause
   - Handles arbitrary projection expressions

2. **Rewriter** (`query_planner/pattern_comprehension_rewriter.rs`):
   - Transforms pattern comprehensions to OPTIONAL MATCH + collect()
   - Preserves WHERE conditions
   - Maintains variable scoping

3. **SQL Generation** (`clickhouse_query_generator/`):
   - OPTIONAL MATCH → LEFT JOIN
   - collect() → groupArray()
   - WHERE → SQL WHERE clause

### Transformation Example

**Input Cypher:**
```cypher
MATCH (u:User)
RETURN u.name, [(u)-[:FOLLOWS]->(f) WHERE f.country = 'USA' | f.name] AS friends
```

**Rewritten to:**
```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FOLLOWS]->(f)
WHERE f.country = 'USA'
WITH u, collect(f.name) AS friends
RETURN u.name, friends
```

**Generated SQL:**
```sql
SELECT 
  u.full_name AS `u.name`,
  groupArray(f.full_name) AS friends
FROM brahmand.users u
LEFT JOIN brahmand.user_follows rel 
  ON u.user_id = rel.follower_id
LEFT JOIN brahmand.users f 
  ON rel.followed_id = f.user_id AND f.country = 'USA'
GROUP BY u.user_id, u.full_name
```

## Key Files

### Parser
- `src/open_cypher_parser/expression.rs`:
  - `parse_pattern_comprehension()` - Main parser function
  - `PatternComprehension` AST node
  - Handles `[(pattern) WHERE condition | projection]` syntax

### Rewriter
- `src/query_planner/pattern_comprehension_rewriter.rs`:
  - `rewrite_pattern_comprehensions()` - Entry point
  - Transforms Return/With clause expressions
  - Generates synthetic OPTIONAL MATCH
  - Creates collect() aggregations

### SQL Generation
- Pattern comprehensions use existing OPTIONAL MATCH and aggregation infrastructure
- No special SQL generation code needed (reuses existing functionality)

### Tests
- `tests/integration/test_pattern_comprehensions.py`:
  - 5 comprehensive integration tests
  - Covers all syntax variants
  - Tests empty results, filtering, expressions

## Design Decisions

### 1. Query Rewriting vs Direct SQL Generation

**Decision**: Rewrite to OPTIONAL MATCH + collect()

**Rationale**:
- ✅ Reuses existing, well-tested LEFT JOIN logic
- ✅ Simpler implementation (no new SQL generation)
- ✅ Consistent behavior with explicit OPTIONAL MATCH
- ✅ Easier to maintain and debug

**Alternative Considered**: Direct SQL generation from pattern comprehension AST
- ❌ Would duplicate existing LEFT JOIN logic
- ❌ More complex code paths
- ❌ Higher risk of inconsistencies

### 2. Empty List Handling

**Decision**: Return empty list `[]` when no matches found

**Rationale**:
- ✅ Matches Neo4j behavior
- ✅ Allows using size() to check for matches
- ✅ Consistent with collect() semantics

### 3. Variable Scoping

**Decision**: Pattern comprehension variables are local to the comprehension

**Rationale**:
- ✅ Prevents variable pollution in outer scope
- ✅ Allows reusing variable names (e.g., multiple comprehensions with `f`)
- ✅ Matches Neo4j and OpenCypher spec

## Gotchas

### 1. Empty Results

Pattern comprehensions always return a list, even if empty:

```cypher
MATCH (u:User) WHERE u.user_id = 999  -- User with no friends
RETURN [(u)-[:FOLLOWS]->(f) | f.name] AS friends
-- Returns: [] (not null)
```

**Workaround**: Check with `size(friends) = 0` or `friends = []`

### 2. WHERE Clause Placement

The WHERE clause goes BEFORE the projection:

```cypher
-- ✅ Correct
[(u)-[:FOLLOWS]->(f) WHERE f.country = 'USA' | f.name]

-- ❌ Wrong
[(u)-[:FOLLOWS]->(f) | f.name WHERE f.country = 'USA']
```

### 3. Multiple Patterns Not Supported

Cannot combine multiple patterns in one comprehension:

```cypher
-- ❌ Not supported
[(u)-[:FOLLOWS]->(f), (f)-[:POSTED]->(p) | p.title]

-- ✅ Use separate comprehensions or explicit WITH
MATCH (u:User)
OPTIONAL MATCH (u)-[:FOLLOWS]->(f)-[:POSTED]->(p)
WITH u, collect(p.title) AS posts
RETURN u.name, posts
```

### 4. Variable-Length Paths

Variable-length paths are not yet supported in pattern comprehensions:

```cypher
-- ❌ Not supported
[(u)-[:FOLLOWS*1..3]->(f) | f.name]

-- ✅ Use explicit MATCH
MATCH (u:User)-[:FOLLOWS*1..3]->(f:User)
RETURN u.name, collect(f.name) AS reachable
```

## Limitations

1. **No variable-length paths**: `*` syntax not supported
2. **Single pattern only**: Cannot chain multiple patterns
3. **No nested comprehensions**: Comprehensions cannot be nested (yet)
4. **No path variables**: Cannot assign pattern to variable inside comprehension

## Future Work

### Potential Enhancements

1. **Variable-Length Path Support**:
   ```cypher
   [(u)-[:FOLLOWS*1..3]->(f) | f.name] AS transitive_friends
   ```

2. **Multiple Patterns**:
   ```cypher
   [(u)-[:FOLLOWS]->(f)-[:POSTED]->(p) WHERE p.views > 1000 | p.title]
   ```

3. **Nested Comprehensions**:
   ```cypher
   [(u)-[:FOLLOWS]->(f) | [f.name, [(f)-[:POSTED]->(p) | p.title]]]
   ```

4. **Performance Optimizations**:
   - Predicate pushdown into LEFT JOIN ON clause
   - Avoid groupArray() when not needed
   - Index hints for filtered properties

## Related Features

- **OPTIONAL MATCH**: Pattern comprehensions rewrite to this
- **collect()**: Aggregation function used internally
- **Variable-Length Paths**: Future integration opportunity
- **List Functions**: size(), head(), last() work with comprehension results

## References

- [OpenCypher Specification - Pattern Comprehensions](https://github.com/opencypher/openCypher/blob/master/cip/1.accepted/CIP2016-06-22-nested-updating-and-chained-subqueries.adoc)
- [Neo4j Documentation - Pattern Comprehensions](https://neo4j.com/docs/cypher-manual/current/syntax/lists/#cypher-pattern-comprehension)
- ClickGraph Implementation: Days 1-5 development process (Dec 21-25, 2025)

## Testing

**Integration Tests**: `tests/integration/test_pattern_comprehensions.py`

1. **test_simple_pattern_comprehension**: Basic collection without WHERE
2. **test_pattern_comprehension_with_where**: Filtering with WHERE clause
3. **test_multiple_pattern_comprehensions**: Multiple comprehensions in one query
4. **test_pattern_comprehension_empty_result**: Empty list handling
5. **test_pattern_comprehension_with_expression**: Expression projections

**Test Data**:
- Database: `brahmand`
- Tables: `pattern_comp_users`, `pattern_comp_follows`
- Schema: `unified_test_schema.yaml`

**Coverage**: 100% of implemented features tested
