# Expression Parsing Implementation Plan

**Date**: November 22, 2024  
**Problem**: Property expressions contain column references that need table alias prefixing, but function names must not be prefixed.

---

## The Correct Understanding

### Current Flow
```yaml
# Schema
property_mappings:
  full_name: "concat(first_name, ' ', last_name)"
```

```
Schema Load → property_mappings["full_name"] = "concat(first_name, ' ', last_name)"
     ↓
Query Planning → PropertyAccess { table_alias: "u", column: "concat(...)" }
     ↓
SQL Generation → format!("{}.{}", "u", "concat(...)") 
     ↓
Result: "u.concat(first_name, ' ', last_name)" ❌ WRONG
```

### What We Need
```sql
concat(u.first_name, ' ', u.last_name)
-- ^^^^^^ function name - NO prefix
--      ^^ table alias prefix added
```

### Why Simple Solutions Won't Work

❌ **Option 1: Unqualified column names**
```sql
-- Won't work with multiple table aliases:
SELECT concat(first_name, ' ', last_name)  -- Which table's first_name??
FROM users_table AS u
JOIN users_table AS u2 ON ...
```

❌ **Option 2: Simple regex replacement**
```rust
// Too naive - will break on:
"multiIf(is_deleted = 1, 'deleted', ...)"
//       ^^^^^^^^^^  This is a column, but inside function call!
```

---

## The Solution: Parse at Schema Load

### Architecture

```
Schema Load Phase:
  Parse YAML → Extract property_mappings
      ↓
  For each expression:
      Parse into AST → Identify column refs vs functions
      ↓
  Store: PropertyValue enum (Column | Expression with metadata)
      ↓
  Save in GraphSchema

Query Planning Phase:
  Look up property → Get PropertyValue
      ↓
  If Expression: Create PropertyAccess with template + column list
      ↓
  Store in LogicalPlan

SQL Generation Phase:
  PropertyAccess → Check if expression
      ↓
  If expression: Apply table alias to column refs only
      ↓
  Generate correct SQL
```

### Data Structures

**New enum in `src/graph_catalog/config.rs`:**

```rust
/// Property value - either a simple column or a computed expression
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PropertyValue {
    /// Simple column reference
    Column(String),
    
    /// Computed expression with metadata for SQL generation
    Expression {
        /// Original expression string
        raw: String,
        
        /// List of column identifiers referenced in the expression
        /// Example: ["first_name", "last_name", "score"]
        columns: Vec<String>,
        
        /// Parsed expression tokens for SQL generation
        /// This allows efficient prefix injection
        tokens: Vec<ExpressionToken>,
    },
}

/// Token in a parsed expression
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExpressionToken {
    /// Column identifier that needs table prefix
    ColumnRef(String),
    
    /// Function name (no prefix)
    Function(String),
    
    /// Operator (+, -, *, /, =, etc.)
    Operator(String),
    
    /// String literal ('...')
    StringLiteral(String),
    
    /// Numeric literal (123, 45.67)
    NumericLiteral(String),
    
    /// Keyword (CASE, WHEN, THEN, END, AND, OR, etc.)
    Keyword(String),
    
    /// Punctuation ((, ), ,)
    Punctuation(char),
    
    /// Whitespace
    Whitespace,
}

/// Node schema with PropertyValue instead of String
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeSchema {
    pub label: String,
    pub database: String,
    pub view_name: String,
    pub primary_keys: Identifier,
    
    /// Changed from HashMap<String, String> to HashMap<String, PropertyValue>
    pub property_mappings: HashMap<String, PropertyValue>,
    
    // ... rest of fields
}
```

### Expression Parser

**New module: `src/graph_catalog/expression_parser.rs`**

```rust
//! Parse ClickHouse expressions to identify column references

use super::config::{ExpressionToken, PropertyValue};

/// Parse a property value into PropertyValue enum
pub fn parse_property_value(value: &str) -> Result<PropertyValue, String> {
    // Check if it's a simple column name (alphanumeric + underscore only)
    if is_simple_column_name(value) {
        return Ok(PropertyValue::Column(value.to_string()));
    }
    
    // It's an expression - parse it
    let tokens = tokenize_expression(value)?;
    let columns = extract_column_references(&tokens);
    
    Ok(PropertyValue::Expression {
        raw: value.to_string(),
        columns,
        tokens,
    })
}

fn is_simple_column_name(s: &str) -> bool {
    // Simple column: starts with letter/underscore, contains only alphanumeric/underscore
    if s.is_empty() {
        return false;
    }
    
    let first_char = s.chars().next().unwrap();
    if !first_char.is_alphabetic() && first_char != '_' {
        return false;
    }
    
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

fn tokenize_expression(expr: &str) -> Result<Vec<ExpressionToken>, String> {
    let mut tokens = Vec::new();
    let mut chars = expr.chars().peekable();
    
    while let Some(ch) = chars.next() {
        match ch {
            // Whitespace
            ' ' | '\t' | '\n' | '\r' => {
                tokens.push(ExpressionToken::Whitespace);
            }
            
            // String literals
            '\'' => {
                let mut literal = String::new();
                let mut escaped = false;
                
                while let Some(&next_ch) = chars.peek() {
                    chars.next();
                    if escaped {
                        literal.push(next_ch);
                        escaped = false;
                    } else if next_ch == '\\' {
                        escaped = true;
                    } else if next_ch == '\'' {
                        break;
                    } else {
                        literal.push(next_ch);
                    }
                }
                
                tokens.push(ExpressionToken::StringLiteral(literal));
            }
            
            // Numeric literals
            '0'..='9' => {
                let mut number = String::from(ch);
                let mut has_dot = false;
                
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_numeric() {
                        number.push(next_ch);
                        chars.next();
                    } else if next_ch == '.' && !has_dot {
                        has_dot = true;
                        number.push(next_ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                
                tokens.push(ExpressionToken::NumericLiteral(number));
            }
            
            // Identifiers (functions, columns, keywords)
            'a'..='z' | 'A'..='Z' | '_' => {
                let mut ident = String::from(ch);
                
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_alphanumeric() || next_ch == '_' {
                        ident.push(next_ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                
                // Check if next non-whitespace char is '(' → function
                let mut temp_chars = chars.clone();
                while let Some(&next_ch) = temp_chars.peek() {
                    if next_ch.is_whitespace() {
                        temp_chars.next();
                    } else {
                        break;
                    }
                }
                
                let is_function = temp_chars.peek() == Some(&'(');
                let ident_lower = ident.to_lowercase();
                
                if is_function {
                    tokens.push(ExpressionToken::Function(ident));
                } else if is_sql_keyword(&ident_lower) {
                    tokens.push(ExpressionToken::Keyword(ident));
                } else {
                    // Column reference
                    tokens.push(ExpressionToken::ColumnRef(ident));
                }
            }
            
            // Operators and punctuation
            '(' | ')' | ',' => {
                tokens.push(ExpressionToken::Punctuation(ch));
            }
            
            '+' | '-' | '*' | '/' | '%' | '=' | '<' | '>' | '!' => {
                let mut op = String::from(ch);
                
                // Handle multi-char operators: <=, >=, !=, <>
                if let Some(&next_ch) = chars.peek() {
                    if (ch == '<' || ch == '>' || ch == '!' || ch == '=') 
                       && (next_ch == '=' || next_ch == '>') {
                        op.push(next_ch);
                        chars.next();
                    }
                }
                
                tokens.push(ExpressionToken::Operator(op));
            }
            
            _ => {
                return Err(format!("Unexpected character: {}", ch));
            }
        }
    }
    
    Ok(tokens)
}

fn extract_column_references(tokens: &[ExpressionToken]) -> Vec<String> {
    let mut columns = Vec::new();
    
    for token in tokens {
        if let ExpressionToken::ColumnRef(col) = token {
            if !columns.contains(col) {
                columns.push(col.clone());
            }
        }
    }
    
    columns
}

fn is_sql_keyword(word: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "case", "when", "then", "else", "end",
        "and", "or", "not", "null",
        "interval", "day", "month", "year", "hour", "minute", "second",
        "true", "false",
        "in", "between", "like", "is",
    ];
    
    KEYWORDS.contains(&word)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_column() {
        let result = parse_property_value("user_id").unwrap();
        assert_eq!(result, PropertyValue::Column("user_id".to_string()));
    }

    #[test]
    fn test_concat_expression() {
        let result = parse_property_value("concat(first_name, ' ', last_name)").unwrap();
        
        match result {
            PropertyValue::Expression { columns, .. } => {
                assert_eq!(columns, vec!["first_name", "last_name"]);
            }
            _ => panic!("Expected Expression"),
        }
    }

    #[test]
    fn test_case_expression() {
        let expr = "CASE WHEN score >= 1000 THEN 'gold' WHEN score >= 500 THEN 'silver' ELSE 'bronze' END";
        let result = parse_property_value(expr).unwrap();
        
        match result {
            PropertyValue::Expression { columns, .. } => {
                assert_eq!(columns, vec!["score"]);
            }
            _ => panic!("Expected Expression"),
        }
    }

    #[test]
    fn test_multiif_expression() {
        let expr = "multiIf(is_deleted = 1, 'deleted', is_banned = 1, 'banned', 'active')";
        let result = parse_property_value(expr).unwrap();
        
        match result {
            PropertyValue::Expression { columns, .. } => {
                assert!(columns.contains(&"is_deleted".to_string()));
                assert!(columns.contains(&"is_banned".to_string()));
            }
            _ => panic!("Expected Expression"),
        }
    }
}
```

### SQL Generation with Prefixing

**Modify `src/clickhouse_query_generator/to_sql.rs`:**

```rust
impl ToSql for LogicalExpr {
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        match self {
            // ... other cases ...
            
            LogicalExpr::PropertyAccessExp(prop) => {
                // Check if this is an expression or simple column
                if is_expression(&prop.column.0) {
                    // Expression: apply table prefix to column references only
                    apply_table_prefix_to_expression(&prop.column.0, &prop.table_alias.0)
                } else {
                    // Simple column: prefix as before
                    Ok(format!("{}.{}", prop.table_alias.0, prop.column.0))
                }
            }
            
            // ... rest ...
        }
    }
}

/// Check if a column value is actually an expression
fn is_expression(value: &str) -> bool {
    // Simple heuristic: contains parentheses or operators
    value.contains('(') || value.contains('+') || value.contains('-')
        || value.contains('*') || value.contains('/') || value.contains("CASE")
}

/// Apply table alias prefix to column references in an expression
fn apply_table_prefix_to_expression(expr: &str, table_alias: &str) 
    -> Result<String, ClickhouseQueryGeneratorError> {
    
    // For now, use the pre-parsed tokens from schema if available
    // Otherwise, fall back to runtime parsing
    
    use crate::graph_catalog::expression_parser::tokenize_expression;
    use crate::graph_catalog::config::ExpressionToken;
    
    let tokens = tokenize_expression(expr)
        .map_err(|e| ClickhouseQueryGeneratorError::SchemaError(
            format!("Failed to parse expression: {}", e)
        ))?;
    
    let mut result = String::new();
    
    for token in tokens {
        match token {
            ExpressionToken::ColumnRef(col) => {
                // Add table prefix
                result.push_str(&format!("{}.{}", table_alias, col));
            }
            ExpressionToken::Function(func) => {
                result.push_str(&func);
            }
            ExpressionToken::Operator(op) => {
                result.push_str(&op);
            }
            ExpressionToken::StringLiteral(lit) => {
                result.push_str(&format!("'{}'", lit));
            }
            ExpressionToken::NumericLiteral(num) => {
                result.push_str(&num);
            }
            ExpressionToken::Keyword(kw) => {
                result.push_str(&kw);
            }
            ExpressionToken::Punctuation(p) => {
                result.push(p);
            }
            ExpressionToken::Whitespace => {
                result.push(' ');
            }
        }
    }
    
    Ok(result)
}
```

---

## Implementation Plan

### Phase 1: Expression Parser (6 hours)

1. Create `src/graph_catalog/expression_parser.rs`
2. Implement tokenizer
3. Add comprehensive tests for all expression types:
   - Simple functions: `concat(a, b)`
   - Nested functions: `upper(concat(a, b))`
   - CASE expressions
   - multiIf expressions
   - Math operators: `a + b`, `a / 1000.0`
   - Comparisons: `a >= 100`
   - String literals with escaping
   - Numeric literals

### Phase 2: Schema Changes (4 hours)

1. Add `PropertyValue` enum to `config.rs`
2. Modify `NodeSchema` to use `HashMap<String, PropertyValue>`
3. Update schema loading in `load_graph_schema()` to parse expressions
4. Update schema serialization/deserialization
5. Migrate existing schemas (backward compatibility)

### Phase 3: SQL Generation (3 hours)

1. Modify `to_sql.rs` to detect expressions
2. Implement `apply_table_prefix_to_expression()`
3. Handle edge cases (nested expressions, complex operators)

### Phase 4: Testing (3 hours)

1. Re-run property expression tests (28 tests)
2. Add tests for:
   - Expressions with JOINs (multiple aliases)
   - Nested function calls
   - Complex CASE expressions
   - Mathematical expressions
3. Performance testing (parse once, use many times)

### Phase 5: Documentation (2 hours)

1. Update schema configuration docs
2. Add expression syntax guide
3. Document limitations and supported patterns

**Total: ~18 hours (~2-3 days)**

---

## Backward Compatibility

To maintain backward compatibility, during schema load:

```rust
pub fn load_graph_schema(config: &str) -> Result<GraphSchema, Error> {
    // Parse YAML
    let config: GraphConfig = serde_yaml::from_str(config)?;
    
    // For each node schema
    for node in config.nodes {
        // Convert old String-based property_mappings to PropertyValue
        let property_mappings = node.property_mappings
            .into_iter()
            .map(|(k, v)| {
                let value = parse_property_value(&v)
                    .unwrap_or_else(|_| PropertyValue::Column(v));
                (k, value)
            })
            .collect();
        
        // Create NodeSchema with PropertyValue
        let node_schema = NodeSchema {
            label: node.label,
            property_mappings,
            // ... rest
        };
    }
    
    // ...
}
```

---

## Alternative: Lazy Parsing

Instead of parsing at schema load, parse expressions on first use and cache:

```rust
use std::sync::RwLock;
use std::collections::HashMap;

lazy_static! {
    static ref EXPRESSION_CACHE: RwLock<HashMap<String, Vec<ExpressionToken>>> 
        = RwLock::new(HashMap::new());
}

fn apply_table_prefix_to_expression(expr: &str, table_alias: &str) 
    -> Result<String, Error> {
    
    // Check cache first
    let tokens = {
        let cache = EXPRESSION_CACHE.read().unwrap();
        if let Some(cached) = cache.get(expr) {
            cached.clone()
        } else {
            // Parse and cache
            drop(cache);
            let parsed = tokenize_expression(expr)?;
            let mut cache = EXPRESSION_CACHE.write().unwrap();
            cache.insert(expr.to_string(), parsed.clone());
            parsed
        }
    };
    
    // Apply prefix
    generate_sql_from_tokens(&tokens, table_alias)
}
```

**Pros**: 
- Simpler schema structure (no schema changes)
- Parse only expressions that are actually used

**Cons**:
- Runtime overhead on first use
- Can't validate expressions at schema load time
- Less efficient (parse during query execution)

---

## Recommendation

**Use Phase 1-5 approach** (parse at schema load):

1. ✅ Validates expressions early (catch errors at schema load)
2. ✅ Zero runtime overhead (parse once, use many times)
3. ✅ Better debugging (can inspect parsed tokens)
4. ✅ Enables optimizations (pre-compute column lists)
5. ✅ Clean architecture (schema owns parsing logic)

**Timeline**: 2-3 days for complete implementation

**Risk**: Medium - requires schema structure changes, but well-tested

**Benefit**: Robust, efficient solution that handles all expression types correctly

