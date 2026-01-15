# Parser Error Handling in ClickGraph

## Overview

ClickGraph's Cypher parser uses **nom** (parser combinators) with comprehensive error handling to prevent silent failures and provide clear error messages to users.

## Key Error Handling Mechanisms

### 1. Complete Input Consumption Validation

**Location**: [`src/open_cypher_parser/mod.rs:182-199`](../../src/open_cypher_parser/mod.rs#L182-L199)

The top-level `parse_query()` function validates that **all input is consumed** after parsing:

```rust
pub fn parse_query(input: &'_ str) -> Result<OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>> {
    match parse_statement(input) {
        Ok((remainder, query_ast)) => {
            // Check that all input was consumed (remainder should be empty or whitespace only)
            let trimmed = remainder.trim();
            if !trimmed.is_empty() {
                return Err(OpenCypherParsingError {
                    errors: vec![
                        (remainder, "Unexpected tokens after query"),
                        (trimmed, "Unparsed input"),
                    ],
                });
            }
            Ok(query_ast)
        }
        // ... error handling
    }
}
```

**What This Prevents**:
- ❌ Silent failures where parser stops mid-query
- ❌ Confusing downstream errors from unparsed text
- ❌ Partial query execution with unexpected semantics

**Example**:
```cypher
MATCH (u:User) WHERE u.user_id = 1 RETURN u.name } invalid
                                                  ^^^^^^^^^^
                                                  Caught as unparsed!
```

**Error Response**:
```json
{
  "cypher_query": "MATCH (u:User) ... } invalid",
  "error": "Unexpected tokens after query:  } invalid\nUnparsed input: } invalid\n",
  "error_type": "ParseError",
  "error_details": {
    "hint": "Check Cypher syntax. See docs/wiki/Cypher-Language-Reference.md"
  }
}
```

### 2. Context-Rich Error Messages

**Location**: [`src/open_cypher_parser/errors.rs`](../../src/open_cypher_parser/errors.rs)

The `OpenCypherParsingError` type provides:
- Multiple error context entries (input position + error message)
- Custom Display implementation showing all error contexts
- Integration with nom's `context()` combinator for adding parse context

```rust
pub struct OpenCypherParsingError<'a> {
    pub errors: Vec<(&'a str, &'static str)>,
}

impl<'a> ContextError<&'a str> for OpenCypherParsingError<'a> {
    fn add_context(input: &'a str, ctx: &'static str, mut other: Self) -> Self {
        other.errors.push((input, ctx));
        other
    }
}
```

### 3. User-Facing Error Presentation

**Location**: [`src/server/sql_generation_handler.rs:145-172`](../../src/server/sql_generation_handler.rs#L145-L172)

Parse errors are caught and converted to user-friendly HTTP responses:

```rust
let cypher_ast = match open_cypher_parser::parse_query(clean_query) {
    Ok(ast) => ast,
    Err(e) => {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(SqlGenerationError {
                cypher_query: payload.query.clone(),
                error: format!("{}", e),  // Uses Display trait
                error_type: "ParseError".to_string(),
                error_details: Some(ErrorDetails {
                    hint: Some(
                        "Check Cypher syntax. See docs/wiki/Cypher-Language-Reference.md"
                    ),
                    // ... position info could be added
                }),
            }),
        ));
    }
};
```

## How nom Prevents Silent Failures

### Parser Combinator Approach

nom uses **parser combinators** which compose small parsers into larger ones. Each combinator:
1. Returns `IResult<Input, Output, Error>` with remaining input
2. Explicitly handles success vs. failure
3. Allows adding context at any level

### Example: Two-Word Keyword Parsing

For keywords like `OPTIONAL MATCH`:

```rust
pub fn parse_optional_match_clause(...) -> IResult<...> {
    let (input, _) = context(
        "OPTIONAL",
        ws(tag_no_case("OPTIONAL"))
    ).parse(input)?;
    
    let (input, _) = context(
        "MATCH after OPTIONAL",
        ws(tag_no_case("MATCH"))
    ).parse(input)?;
    
    // ... rest of parsing
}
```

If parsing stops after "OPTIONAL", the `parse_query()` function will catch the remaining "MATCH ..." as unparsed input.

## Current Limitations & Future Improvements

### ✅ What Works Well

1. **Complete input validation**: All queries are checked for complete consumption
2. **Clear error messages**: Users see what couldn't be parsed
3. **Helpful hints**: Error responses point to documentation
4. **Consistent error handling**: Same pattern throughout codebase

### ⚠️ Potential Improvements

1. **Line/Column Numbers**: Add position information to errors
   ```rust
   // Future enhancement:
   error_details: Some(ErrorDetails {
       position: Some(42),
       line: Some(1),
       column: Some(43),
       hint: Some("...")
   })
   ```

2. **Better Error Context**: Use nom's `context()` more extensively
   ```rust
   // Add context at parse boundaries:
   context("parsing WHERE clause", parse_where_clause)
   context("parsing expression", parse_expression)
   ```

3. **Suggestions for Common Mistakes**: Detect patterns and suggest fixes
   ```rust
   // Example: If user writes "MATCH (n) WERE n.id = 1"
   Error: "Unexpected token 'WERE'. Did you mean 'WHERE'?"
   ```

4. **Better Handling of Incomplete Input**:
   ```rust
   Err(nom::Err::Incomplete(_)) => {
       // Currently: generic "incomplete" error
       // Future: specific "expected X, found end of input"
   }
   ```

## Best Practices for Adding New Features

When adding new Cypher syntax:

### ✅ DO:

1. **Always return IResult**: Never consume input without returning remainder
   ```rust
   fn parse_my_feature(input: &str) -> IResult<&str, MyAst, OpenCypherParsingError> {
       // ...
   }
   ```

2. **Use context() for clarity**:
   ```rust
   context("parsing my feature", my_parser).parse(input)?
   ```

3. **Handle all AST cases in match statements**:
   ```rust
   match expr {
       LogicalExpr::Property(_) => { /* ... */ },
       LogicalExpr::ArraySubscript { .. } => { /* ... */ },
       // Don't use _ => catch-all unless truly exhaustive
   }
   ```

4. **Test unparsable variants**:
   ```rust
   #[test]
   fn test_invalid_syntax() {
       let result = parse_query("MATCH (n) } invalid");
       assert!(result.is_err());
       assert!(result.unwrap_err().to_string().contains("Unexpected tokens"));
   }
   ```

### ❌ DON'T:

1. **Don't use `unwrap()` on parse results**: Always propagate errors with `?`
2. **Don't silently ignore remaining input**: Let `parse_query()` validate
3. **Don't use overly broad `alt()` branches**: Specific parsers before general ones
4. **Don't skip error testing**: Always test invalid syntax variants

## Testing Parser Errors

### Manual Testing

```bash
# Test unparsed tokens
curl -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n) } invalid", "schema_name": "social_benchmark"}'

# Expected: "Unexpected tokens after query: } invalid"
```

### Unit Testing

```rust
#[test]
fn test_parse_error_on_trailing_tokens() {
    let query = "MATCH (n) RETURN n } extra";
    let result = parse_query(query);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Unexpected tokens"));
}
```

### Integration Testing

```python
def test_parser_error_handling():
    response = requests.post(
        "http://localhost:8080/query/sql",
        json={"query": "MATCH (n) } invalid", "schema_name": "test"}
    )
    assert response.status_code == 400
    data = response.json()
    assert data["error_type"] == "ParseError"
    assert "Unexpected tokens" in data["error"]
```

## Summary

**ClickGraph's parser error handling prevents silent failures** through:

1. ✅ **Complete input validation** - Detects unparsed trailing tokens
2. ✅ **Clear error messages** - Shows what couldn't be parsed
3. ✅ **User-friendly presentation** - HTTP 400 with helpful hints
4. ✅ **Consistent patterns** - Same approach throughout codebase
5. ✅ **Parse-first validation** - Syntax checked before schema lookup (prevents misleading "Schema not found" errors)

**The concern you raised is already addressed** - when the parser can't handle syntax, it immediately emits an error with accurate context, rather than silently failing or causing random downstream errors.

### Critical Fix (Jan 7, 2026): Parse Before Schema Lookup

**Problem**: Previously, if a query had syntax errors AND no schema was specified, users would see:
```
Error: Schema 'default' not found
```

Instead of the actual parse error, because:
1. Pre-parse for USE clause failed → fell back to "default"
2. Schema lookup for "default" failed → returned error
3. Real parse (with proper error handling) never executed

**Fix** ([handlers.rs:173-196](../../src/server/handlers.rs#L173-L196)):
```rust
// Validate query syntax FIRST before schema lookup
let schema_name = match open_cypher_parser::parse_query(clean_query) {
    Ok(ast) => {
        // Extract schema from USE clause or use default
        ast.use_clause.map(|u| u.database_name)
            .or(payload.schema_name.as_deref())
            .unwrap_or("default")
    }
    Err(e) => {
        // Return parse error immediately - don't proceed to schema lookup
        return Err((StatusCode::BAD_REQUEST, 
            format!("Query syntax error: {}", e)));
    }
};
```

**Result**: Parse errors are now reported correctly, before any schema operations.

## References

- nom Documentation: https://docs.rs/nom/
- OpenCypher Grammar: `/open_cypher_specs/`
- Parser Implementation: [`src/open_cypher_parser/mod.rs`](../../src/open_cypher_parser/mod.rs)
- Error Types: [`src/open_cypher_parser/errors.rs`](../../src/open_cypher_parser/errors.rs)
- Error Handling: [`src/server/sql_generation_handler.rs`](../../src/server/sql_generation_handler.rs)
