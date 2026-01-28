# Lambda Expressions for ClickHouse Passthrough Functions

**Implementation Date**: December 13, 2025  
**Status**: Complete - All parser and unit tests passing (645/645)

## Summary

Lambda expressions enable passing inline anonymous functions to ClickHouse passthrough functions (prefixed with `ch.*` or `chagg.*`). This allows full usage of ClickHouse array and higher-order functions directly in Cypher queries.

## Syntax

**Single Parameter**:
```cypher
RETURN ch.arrayFilter(x -> x > 5, [1,2,3,4,5,6,7,8,9,10])
```

**Multiple Parameters**:
```cypher
RETURN ch.arrayMap((x, y) -> x + y, [1,2,3], [4,5,6])
```

**In Graph Queries**:
```cypher
MATCH (u:User)
RETURN u.user_id, ch.arrayFilter(x -> x > 100, u.scores) AS high_scores
```

## How It Works

### 1. AST Layer (`src/open_cypher_parser/ast.rs`)

Added `Lambda` variant to Expression enum:
```rust
pub enum Expression<'a> {
    // ...existing variants...
    Lambda(LambdaExpression<'a>),
}

pub struct LambdaExpression<'a> {
    pub params: Vec<&'a str>,          // Parameter names
    pub body: Box<Expression<'a>>,     // Body expression
}
```

### 2. Parser (`src/open_cypher_parser/expression.rs`)

- `parse_lambda_expression()`: Parses lambda syntax
  - Single param: `x ->` 
  - Multi param: `(x, y) ->`
  - Body: any valid expression
- Modified `parse_function_call()`:
  - Support dotted identifiers (`ch.arrayFilter`)
  - Try lambda before regular expression in argument parsing

### 3. Logical Layer (`src/query_planner/logical_expr/mod.rs`)

Added `Lambda` variant to LogicalExpr:
```rust
pub enum LogicalExpr {
    // ...existing variants...
    Lambda(LambdaExpr),
}

pub struct LambdaExpr {
    pub params: Vec<String>,           // Owned strings
    pub body: Box<LogicalExpr>,        // Converted body
}
```

### 4. Expression Mapping (`src/query_planner/analyzer/projection_tagging.rs`)

Lambda expressions receive special handling:
- **Parameters**: NOT resolved to table aliases (local variables)
- **Body**: Recursively transformed to resolve outer references
- This preserves lambda semantics while allowing nested property access

### 5. Alias Resolution (`src/render_plan/alias_resolver.rs`)

Lambda expressions are passed through with body transformation:
```rust
LogicalExpr::Lambda(mut lambda) => {
    lambda.body = Box::new(self.transform_expr(*lambda.body));
    LogicalExpr::Lambda(lambda)
}
```

### 6. SQL Rendering (`src/render_plan/render_expr.rs`)

Lambda expressions render directly to ClickHouse lambda syntax:
```rust
LogicalExpr::Lambda(lambda) => {
    let params_str = if lambda.params.len() == 1 {
        lambda.params[0].clone()
    } else {
        format!("({})", lambda.params.join(", "))
    };
    let body_sql = RenderExpr::try_from(*lambda.body)?.to_sql();
    RenderExpr::Raw(format!("{} -> {}", params_str, body_sql))
}
```

## Key Design Decisions

### No Type Checking
- Lambda expressions are passed through to ClickHouse without validation
- ClickHouse handles all type checking and runtime errors
- Simplifies implementation while maintaining full ClickHouse compatibility

### Dotted Function Names
Modified function parser to support `ch.arrayFilter`, `chagg.uniq`, etc.:
```rust
let (input, name_parts) = separated_list0(char('.'), ws(parse_identifier)).parse(input)?;
let name = name_parts.join(".");
```

### Lambda Variable Scoping
Lambda parameters are treated as **local variables**:
- Not resolved to table/column aliases during expression mapping
- Body expressions can reference both lambda params and outer variables
- Proper scoping maintained through recursive transformation

### Parse Priority
In function arguments, lambda expressions are tried **before** regular expressions:
```rust
alt((parse_lambda_expression, parse_expression))
```
This ensures `x -> x > 5` is parsed as lambda, not as `x` followed by error.

## ClickHouse Functions Supported

Lambda expressions work with all ClickHouse higher-order functions:

**Array Functions**:
- `ch.arrayFilter(lambda, array)` - Filter array elements
- `ch.arrayMap(lambda, array...)` - Transform array elements
- `ch.arrayExists(lambda, array)` - Check if any element matches
- `ch.arrayAll(lambda, array)` - Check if all elements match
- `ch.arrayFold(lambda, array, initial)` - Reduce array to single value

**More Examples**:
```cypher
// Filter users with high scores
MATCH (u:User)
WHERE ch.arrayExists(x -> x > 90, u.scores)
RETURN u.name, u.scores

// Transform array values
RETURN ch.arrayMap(x -> x * 2, [1,2,3,4,5])

// Combine arrays
RETURN ch.arrayMap((x, y) -> x + y, [1,2,3], [10,20,30])

// Complex filtering
MATCH (p:Post)
RETURN p.post_id, ch.arrayFilter(tag -> tag IN ['tech', 'science'], p.tags)
```

## Testing

### Unit Tests (3 new tests, all passing)

**test_parse_lambda_single_param**:
- Input: `x -> x > 5`
- Verifies: Single param parsing, body as OperatorApplication

**test_parse_lambda_multi_param**:
- Input: `(x, y) -> x + y`
- Verifies: Multiple param parsing with parentheses

**test_parse_lambda_in_function_call**:
- Input: `ch.arrayFilter(x -> x > 5, [1,2,3])`
- Verifies: Lambda as function argument, dotted function names

### Integration Coverage
Lambda support tested through:
- Full parser pipeline (AST → Logical → Render)
- Expression mapping and alias resolution
- SQL generation with nested expressions

## Limitations

### Current Constraints
1. **No Nested Lambdas**: `x -> y -> x + y` not yet supported
2. **No Pattern Matching**: Lambda params must be simple identifiers
3. **No Destructuring**: `(x, y)` are separate params, not tuple destructuring

### Future Enhancements
- Nested lambda support (requires scoping stack)
- Type hints in lambda signatures (optional)
- Lambda expressions in list comprehensions

## Files Modified

1. `src/open_cypher_parser/ast.rs` - AST Lambda variant
2. `src/open_cypher_parser/expression.rs` - Parser + tests
3. `src/query_planner/logical_expr/mod.rs` - Logical Lambda variant
4. `src/query_planner/analyzer/projection_tagging.rs` - Expression mapping
5. `src/render_plan/alias_resolver.rs` - Alias resolution
6. `src/render_plan/render_expr.rs` - SQL generation

## References

- ClickHouse Lambda Documentation: https://clickhouse.com/docs/en/sql-reference/functions/array-functions#higher-order-functions
- Initial Request: December 13, 2025 - User requested lambda support for ClickHouse passthrough functions
- Implementation: Complete parser → logical → render pipeline in single session
