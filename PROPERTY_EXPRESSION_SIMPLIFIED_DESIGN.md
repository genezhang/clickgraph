# Property Expression Parsing - Simplified Design with Nom

**Date**: November 23, 2025  
**Decision**: Simplified expressions only, reuse nom parser, no view filters

---

## Design Constraints (Simplified)

### What's Supported ✅
1. **Simple column references**: `user_id`, `full_name`
2. **Function calls**: `concat(first_name, ' ', last_name)`
3. **Type conversions**: `toUInt8(age_str)`, `toDate(birth_date)`
4. **Math operations**: `score / 1000.0`, `price * quantity`
5. **Date functions**: `dateDiff('day', start, end)`, `today()`
6. **String functions**: `concat()`, `upper()`, `lower()`, `substring()`
7. **JSON extraction**: `JSONExtractString(json_col, 'key')`
8. **Quoted identifiers**: `"First Name"`, `` `User-ID` ``

### What's NOT Supported ❌
1. **Conditionals**: `CASE WHEN`, `multiIf()`, `IF()` - use at query time
2. **View filters**: `filter:` field removed - use ClickHouse views if needed
3. **Comparisons in mappings**: `age >= 18` - use WHERE clause instead
4. **Boolean expressions**: `is_active AND is_verified` - query time only

### Why This Simplification?

**Property mappings are for convenience, not business logic**:
```yaml
# ✅ GOOD: Simple transformations
property_mappings:
  full_name: "concat(first_name, ' ', last_name)"
  age_days: "dateDiff('day', birth_date, today())"
  score_pct: "score / 100.0"

# ❌ BAD: Complex logic belongs in queries
property_mappings:
  tier: "CASE WHEN score >= 1000 THEN 'gold' ELSE 'bronze' END"
  #     ^^^ Use this in RETURN clause instead!
```

**Use case separation**:
- **Schema-time**: Simple column aliasing, basic transformations
- **Query-time**: Business logic, conditionals, complex expressions

---

## Architecture: Reuse Nom Parser

### Why Nom?

1. **Already in project**: Used for Cypher parsing
2. **Proven**: Handles complex expressions correctly
3. **Composable**: Can parse ClickHouse scalar expressions
4. **Type-safe**: Generates proper AST

### Strategy

**Parse ClickHouse scalar expressions using subset of Cypher expression parser**:

```rust
// In src/graph_catalog/expression_parser.rs

use nom::{IResult, Parser};
use crate::open_cypher_parser::expression::{
    parse_literal_or_variable_expression,
    parse_function_call,
    parse_operator_symbols,
};

/// Parse ClickHouse scalar expression (subset of Cypher)
/// Supports: literals, identifiers, function calls, operators
/// Does NOT support: CASE, path patterns, list comprehensions
pub fn parse_clickhouse_scalar_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        map(parse_function_call, ClickHouseExpr::FunctionCall),
        map(parse_binary_expr, ClickHouseExpr::BinaryOp),
        map(parse_identifier, ClickHouseExpr::Column),
        map(parse_quoted_identifier, ClickHouseExpr::QuotedColumn),
        map(parse_literal, ClickHouseExpr::Literal),
    ))(input)
}

/// ClickHouse expression AST (simplified)
#[derive(Debug, Clone)]
pub enum ClickHouseExpr {
    /// Column reference: user_id
    Column(String),
    
    /// Quoted column: "First Name" or `User-ID`
    QuotedColumn(String),
    
    /// Function call: concat(a, b)
    FunctionCall {
        name: String,
        args: Vec<ClickHouseExpr>,
    },
    
    /// Binary operation: a + b, score / 100.0
    BinaryOp {
        op: Operator,
        left: Box<ClickHouseExpr>,
        right: Box<ClickHouseExpr>,
    },
    
    /// Literal: 'string', 123, 45.67
    Literal(Literal),
}

impl ClickHouseExpr {
    /// Extract all column references from expression
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            ClickHouseExpr::Column(col) => vec![col.clone()],
            ClickHouseExpr::QuotedColumn(col) => vec![col.clone()],
            ClickHouseExpr::FunctionCall { args, .. } => {
                args.iter().flat_map(|e| e.get_columns()).collect()
            }
            ClickHouseExpr::BinaryOp { left, right, .. } => {
                let mut cols = left.get_columns();
                cols.extend(right.get_columns());
                cols
            }
            ClickHouseExpr::Literal(_) => vec![],
        }
    }
    
    /// Generate SQL with table alias prefix
    pub fn to_sql(&self, table_alias: &str) -> String {
        match self {
            ClickHouseExpr::Column(col) => {
                format!("{}.{}", table_alias, col)
            }
            ClickHouseExpr::QuotedColumn(col) => {
                format!("{}.\"{}\"", table_alias, col)
            }
            ClickHouseExpr::FunctionCall { name, args } => {
                let args_sql: Vec<String> = args.iter()
                    .map(|a| a.to_sql(table_alias))
                    .collect();
                format!("{}({})", name, args_sql.join(", "))
            }
            ClickHouseExpr::BinaryOp { op, left, right } => {
                format!("({} {} {})", 
                    left.to_sql(table_alias),
                    op.to_string(),
                    right.to_sql(table_alias)
                )
            }
            ClickHouseExpr::Literal(lit) => {
                lit.to_string()
            }
        }
    }
}
```

---

## Data Structures

```rust
// In src/graph_catalog/config.rs

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PropertyValue {
    /// Simple column reference
    Column(String),
    
    /// Parsed expression
    Expression {
        /// Original expression string
        raw: String,
        
        /// Parsed AST
        #[serde(skip)] // Don't serialize, parse on load
        ast: ClickHouseExpr,
    },
}

impl PropertyValue {
    /// Apply table prefix to generate SQL
    pub fn to_sql(&self, table_alias: &str) -> String {
        match self {
            PropertyValue::Column(col) => {
                if needs_quoting(col) {
                    format!("{}.\"{}\"", table_alias, col)
                } else {
                    format!("{}.{}", table_alias, col)
                }
            }
            PropertyValue::Expression { ast, .. } => {
                ast.to_sql(table_alias)
            }
        }
    }
    
    /// Get raw value (for debugging)
    pub fn raw(&self) -> &str {
        match self {
            PropertyValue::Column(col) => col,
            PropertyValue::Expression { raw, .. } => raw,
        }
    }
}
```

---

## Parser Implementation

```rust
// In src/graph_catalog/expression_parser.rs

use nom::{
    IResult,
    branch::alt,
    bytes::complete::tag,
    character::complete::{alphanumeric1, char, multispace0},
    combinator::{map, opt, recognize},
    multi::separated_list0,
    sequence::{delimited, preceded, tuple},
};

/// Parse property value (entry point)
pub fn parse_property_value(value: &str) -> Result<PropertyValue, String> {
    let value = value.trim();
    
    // Check for simple column name
    if is_simple_column(value) {
        return Ok(PropertyValue::Column(value.to_string()));
    }
    
    // Parse as expression
    match parse_clickhouse_scalar_expr(value) {
        Ok((remaining, ast)) => {
            if !remaining.trim().is_empty() {
                return Err(format!("Unexpected trailing content: {}", remaining));
            }
            Ok(PropertyValue::Expression {
                raw: value.to_string(),
                ast,
            })
        }
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
}

fn is_simple_column(s: &str) -> bool {
    !s.is_empty() 
        && s.chars().next().unwrap().is_alphabetic()
        && s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Parse ClickHouse scalar expression
fn parse_clickhouse_scalar_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    parse_binary_expr(input)
}

/// Parse binary operations (lowest precedence)
fn parse_binary_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    let (input, left) = parse_term(input)?;
    
    // Try to parse operator and right side
    let result = tuple((
        delimited(multispace0, parse_additive_op, multispace0),
        parse_term,
    ))(input);
    
    match result {
        Ok((input, (op, right))) => {
            Ok((input, ClickHouseExpr::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        Err(_) => Ok((input, left)),
    }
}

fn parse_additive_op(input: &str) -> IResult<&str, Operator> {
    alt((
        map(tag("+"), |_| Operator::Addition),
        map(tag("-"), |_| Operator::Subtraction),
        map(tag("*"), |_| Operator::Multiplication),
        map(tag("/"), |_| Operator::Division),
    ))(input)
}

/// Parse term (higher precedence)
fn parse_term(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        parse_function_call_expr,
        parse_quoted_identifier,
        parse_identifier_expr,
        parse_literal_expr,
        delimited(
            char('('),
            delimited(multispace0, parse_clickhouse_scalar_expr, multispace0),
            char(')'),
        ),
    ))(input)
}

/// Parse function call: concat(a, b)
fn parse_function_call_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    let (input, name) = parse_identifier_str(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, args) = separated_list0(
        delimited(multispace0, char(','), multispace0),
        parse_clickhouse_scalar_expr,
    )(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, ClickHouseExpr::FunctionCall {
        name: name.to_string(),
        args,
    }))
}

/// Parse quoted identifier: "First Name" or `User-ID`
fn parse_quoted_identifier(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        // Double quotes
        map(
            delimited(char('"'), recognize(take_until("\"")), char('"')),
            |s: &str| ClickHouseExpr::QuotedColumn(s.to_string()),
        ),
        // Backticks
        map(
            delimited(char('`'), recognize(take_until("`")), char('`')),
            |s: &str| ClickHouseExpr::QuotedColumn(s.to_string()),
        ),
    ))(input)
}

/// Parse bare identifier
fn parse_identifier_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    map(parse_identifier_str, |s| {
        ClickHouseExpr::Column(s.to_string())
    })(input)
}

fn parse_identifier_str(input: &str) -> IResult<&str, &str> {
    recognize(tuple((
        alt((alphanumeric1, tag("_"))),
        opt(recognize(many0(alt((alphanumeric1, tag("_")))))),
    )))(input)
}

/// Parse literal: 'string', 123, 45.67
fn parse_literal_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        // String literal: 'hello'
        map(
            delimited(char('\''), recognize(take_until("'")), char('\'')),
            |s: &str| ClickHouseExpr::Literal(Literal::String(s.to_string())),
        ),
        // Numeric literal
        map(recognize_number, |s: &str| {
            if s.contains('.') {
                ClickHouseExpr::Literal(Literal::Float(s.parse().unwrap()))
            } else {
                ClickHouseExpr::Literal(Literal::Integer(s.parse().unwrap()))
            }
        }),
    ))(input)
}

fn recognize_number(input: &str) -> IResult<&str, &str> {
    recognize(tuple((
        opt(char('-')),
        digit1,
        opt(tuple((char('.'), digit1))),
    )))(input)
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
}

impl Literal {
    pub fn to_string(&self) -> String {
        match self {
            Literal::String(s) => format!("'{}'", s),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Operator {
    Addition,
    Subtraction,
    Multiplication,
    Division,
}

impl Operator {
    pub fn to_string(&self) -> &str {
        match self {
            Operator::Addition => "+",
            Operator::Subtraction => "-",
            Operator::Multiplication => "*",
            Operator::Division => "/",
        }
    }
}
```

---

## Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_column() {
        let pv = parse_property_value("user_id").unwrap();
        assert!(matches!(pv, PropertyValue::Column(_)));
        assert_eq!(pv.to_sql("u"), "u.user_id");
    }

    #[test]
    fn test_concat_expression() {
        let pv = parse_property_value("concat(first_name, ' ', last_name)").unwrap();
        assert_eq!(pv.to_sql("u"), "concat(u.first_name, ' ', u.last_name)");
    }

    #[test]
    fn test_math_expression() {
        let pv = parse_property_value("score / 100.0").unwrap();
        assert_eq!(pv.to_sql("u"), "(u.score / 100.0)");
    }

    #[test]
    fn test_nested_functions() {
        let pv = parse_property_value("upper(concat(first_name, last_name))").unwrap();
        assert_eq!(pv.to_sql("u"), "upper(concat(u.first_name, u.last_name))");
    }

    #[test]
    fn test_quoted_columns() {
        let pv = parse_property_value(r#"concat("First Name", " ", "Last Name")"#).unwrap();
        assert_eq!(pv.to_sql("u"), r#"concat(u."First Name", " ", u."Last Name")"#);
    }

    #[test]
    fn test_reject_case_when() {
        let result = parse_property_value("CASE WHEN score > 1000 THEN 'gold' END");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not supported"));
    }

    #[test]
    fn test_reject_multiif() {
        let result = parse_property_value("multiIf(is_active = 1, 'active', 'inactive')");
        // multiIf looks like function call but has comparison operators
        // Parser may accept it but we can validate in a separate check
        assert!(result.is_err() || 
                result.unwrap().raw().contains("comparison not supported"));
    }
}
```

---

## Benefits of This Approach

### 1. **Reuses Existing Infrastructure**
- ✅ Nom parser already in project
- ✅ Expression AST similar to Cypher AST
- ✅ Type-safe parsing

### 2. **Simple & Correct**
- ✅ Handles ~90% of real use cases
- ✅ Proper parsing (not regex hacks)
- ✅ Clear error messages

### 3. **Performance**
- ✅ Parse once at schema load
- ✅ AST traversal for SQL generation
- ✅ No runtime parsing overhead

### 4. **Extensible**
- ✅ Easy to add more operators
- ✅ Can add more functions
- ✅ AST structure allows future optimizations

---

## Implementation Plan

### Phase 1: Parser (4 hours)
1. Create `src/graph_catalog/expression_parser.rs`
2. Implement nom-based parser
3. Add comprehensive tests (20+ test cases)

### Phase 2: Schema Integration (2 hours)
1. Add `PropertyValue` enum to `config.rs`
2. Update schema loading to parse expressions
3. Handle backward compatibility

### Phase 3: SQL Generation (2 hours)
1. Update `to_sql()` implementations
2. Handle PropertyValue in SQL generation
3. Test with benchmark schema

### Phase 4: Documentation (2 hours)
1. Update schema docs with supported expressions
2. Add examples and limitations
3. Migration guide

**Total**: ~10 hours (~1.5 days)

---

## Migration from Current Code

### Before (HashMap<String, String>)
```rust
pub struct NodeSchema {
    pub property_mappings: HashMap<String, String>,
}
```

### After (HashMap<String, PropertyValue>)
```rust
pub struct NodeSchema {
    pub property_mappings: HashMap<String, PropertyValue>,
}
```

### Schema Loading
```rust
for (key, value) in config.property_mappings {
    let property_value = parse_property_value(&value)
        .map_err(|e| format!("Property '{}': {}", key, e))?;
    mappings.insert(key, property_value);
}
```

---

## Decision: No View Filters

**Rationale**: View filters are essentially row-level security, which should be handled by:
1. **ClickHouse views**: Create filtered views in ClickHouse
2. **Row policies**: Use ClickHouse row-level security
3. **Query-time filtering**: Use WHERE clauses in Cypher

**Example** (wrong approach - would have been in schema):
```yaml
# ❌ DON'T DO THIS (would add complexity)
nodes:
  - label: User
    table: users
    filter: "is_deleted = 0 AND tenant_id = {tenant}"  # NO!
```

**Example** (correct approach - use ClickHouse):
```sql
-- Create filtered view in ClickHouse
CREATE VIEW active_users AS
SELECT * FROM users WHERE is_deleted = 0;
```

```yaml
# ✅ Reference the filtered view
nodes:
  - label: User
    table: active_users  # Points to filtered view
```

---

## Summary

**Simplified approach**:
- ✅ Simple scalar expressions only
- ✅ Reuse nom parser (already in project)
- ✅ No conditionals in schema (use query time)
- ✅ No view filters (use ClickHouse views)
- ✅ Clear separation: schema for structure, queries for logic

**Timeline**: ~1.5 days to implement and test

Ready to start when you are!
