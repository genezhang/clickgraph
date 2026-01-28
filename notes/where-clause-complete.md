# WHERE Clause Handling for Variable-Length Paths

*Completed: October 22, 2025*

## Summary

Full implementation of WHERE clause filtering for variable-length path queries and shortest path functions, including proper handling of end node filters like `WHERE b.name = "David Lee"`.

## How It Works

### Filter Categorization
WHERE predicates are categorized by node reference:
- **Start filters**: Applied to the starting node of the path
- **End filters**: Applied to the ending node of the path
- **Path filters**: Applied to properties of the path itself

### Context Storage
End filters are stored in `CteGenerationContext` during GraphRel processing:
```rust
pub struct CteGenerationContext {
    pub end_filters_for_outer_query: Option<RenderExpr>,
    // ... other fields
}
```

### Expression Rewriting
The `rewrite_expr_for_var_len_cte` function maps Cypher expressions to CTE columns:
- `a.user_id` → `start_user_id` (start node properties)
- `b.name` → `end_name` (end node properties)
- Handles both single-quoted and double-quoted string literals

### SQL Generation
End filters are applied in the outer query scope using the CTE table alias 't':
```sql
WITH RECURSIVE variable_path_cte AS (
  -- CTE definition with end_ prefixed columns
  SELECT ..., end_name FROM ...
)
SELECT * FROM variable_path_cte t
WHERE t.end_name = 'David Lee'  -- End filter applied here
```

## Key Files

### Core Implementation
- `src/render_plan/plan_builder.rs`: Main SQL generation and filter processing
- `src/open_cypher_parser/expression.rs`: Parser support for double-quoted strings
- `src/clickhouse_query_generator/variable_length_cte.rs`: CTE generation with property selection

### Test Coverage
- `src/render_plan/tests/where_clause_filter_tests.rs`: Comprehensive test suite
- 298 total tests passing (100%)

## Design Decisions

### Parser-Level Fix
**Decision**: Fix double-quoted string parsing at the parser level rather than using runtime workarounds.

**Rationale**: Proper AST representation prevents downstream issues and ensures consistent handling across all query types.

**Implementation**: Added `parse_double_quoted_string_literal()` function to the expression parser.

### Context-Based Storage
**Decision**: Store end filters in `CteGenerationContext` rather than passing them through function parameters.

**Rationale**: Simplifies the API and allows filters to be accumulated across multiple processing steps.

### CTE Column Prefixing
**Decision**: Use `end_` prefix for end node properties in CTEs (e.g., `end_name`, `end_user_id`).

**Rationale**: Clear naming convention that prevents conflicts with start node properties and path metadata.

## Gotchas

### String Literal Handling
Double-quoted strings were initially parsed as `TableAlias` containing JSON-encoded values:
```rust
// Before fix: Incorrect parsing
TableAlias("\"David Lee\"")  // JSON-encoded string

// After fix: Correct parsing
Literal(String("David Lee"))  // Proper string literal
```

### Filter Scope
End filters must be applied in the outer query, not within the recursive CTE:
```sql
-- ❌ Wrong: Filter inside CTE
WITH RECURSIVE cte AS (
  SELECT * FROM ... WHERE end_name = 'value'  -- Won't work
)

-- ✅ Correct: Filter in outer query
WITH RECURSIVE cte AS (SELECT * FROM ...)
SELECT * FROM cte t WHERE t.end_name = 'value'
```

### Property Mapping
YAML schema configuration must correctly map Cypher properties to database columns:
```yaml
nodes:
  Person:
    table: users
    properties:
      name: full_name  # Cypher 'name' maps to DB 'full_name'
```

## Limitations

### Complex Expressions
Currently supports simple property comparisons. Complex expressions like:
- `WHERE b.age > 25 AND b.city = "NYC"`
- `WHERE NOT b.active`
- `WHERE b.score IN [80, 90, 100]`

May require additional implementation for full expression support.

### Path Filters
Filters on path properties (not just end node properties) are not yet implemented:
```cypher
MATCH p = (a)-[:KNOWS*]-(b)
WHERE length(p) > 3  -- Path filter, not implemented
```

## Future Work

### Expression Expansion
- Support for complex logical expressions (AND, OR, NOT)
- IN clauses and range comparisons
- Function calls in WHERE conditions

### Path Property Filters
- Filters on path length: `WHERE length(p) BETWEEN 2 AND 5`
- Filters on path properties when available

### Performance Optimization
- Push down filters into CTE where possible
- Index utilization for filtered queries

## Testing

### Test Categories
- **Basic filters**: Single property comparisons
- **Multiple filters**: AND/OR combinations
- **String literals**: Both single and double quotes
- **Numeric filters**: Age, ID comparisons
- **Shortest path**: Filters with `shortestPath()` and `allShortestPaths()`
- **Edge cases**: Null values, missing properties

### Validation
All tests pass end-to-end with real ClickHouse data, confirming:
- Correct SQL generation
- Proper filter application
- Expected result filtering

## Related Features

- **Variable-length paths**: Core functionality this filtering extends
- **Shortest path queries**: Also supports WHERE clause filtering
- **Path variables**: Foundation for path-based filtering
- **Schema mapping**: YAML configuration for property mapping


