# Property Expressions Fix Strategy

**Date**: November 22, 2024  
**Issue**: Column references in expressions need table alias prefix, but function names don't.

---

## The Problem Clarified

### Example Expression
```yaml
property_mappings:
  full_name: "concat(first_name, ' ', last_name)"
```

### What Current Code Does
```rust
// view_query.rs line 27-29
projections.push(format!("{}.{} AS {}", self.source_table, col, prop));

// Result:
"users_expressions_test.concat(first_name, ' ', last_name) AS full_name"
//                      ^^^^^^ WRONG - prefixes the whole expression
```

### What ClickHouse Sees
```sql
SELECT users_expressions_test.concat(first_name, ' ', last_name) AS full_name
--     ^^^^^^^^^^^^^^^^^^^^^^^^ Function doesn't exist!
```

### What We Need
```sql
SELECT concat(users_expressions_test.first_name, ' ', users_expressions_test.last_name) AS full_name
--     ^^^^^^ Function name NO prefix
--            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Column reference WITH prefix
```

---

## Solution Options

### Option 1: Full SQL Expression Parser (Complex, Ideal)

**Pros**:
- Correct handling of all expressions
- Can handle nested functions, subqueries, etc.

**Cons**:
- Very complex (~1000+ lines of code)
- Need to parse ClickHouse SQL dialect
- High risk of bugs
- Slow development (3-5 days)

**Example implementation**:
```rust
fn prefix_columns_in_expression(expr: &str, table_alias: &str) -> String {
    let tokens = tokenize_sql(expr);
    let mut result = String::new();
    
    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::Identifier(name) if is_column_reference(name, &tokens, i) => {
                result.push_str(&format!("{}.{}", table_alias, name));
            }
            Token::Function(name) => {
                result.push_str(name);
            }
            // ... handle operators, literals, etc.
        }
    }
    
    result
}
```

### Option 2: Simple Heuristic (Pragmatic, Fast)

**Approach**: Detect if property value is an expression, handle it specially.

**Heuristic**: If value contains `(` and `)`, treat as expression and use ClickHouse's implicit aliasing.

**Key insight**: ClickHouse allows unqualified column names in SELECT when there's only one source table!

```sql
-- This works in ClickHouse:
SELECT concat(first_name, ' ', last_name) AS full_name
FROM users_expressions_test
--         ^^^^^^^^^^^ No table prefix needed for columns!
```

**Pros**:
- Simple implementation (~50 lines)
- Fast development (1-2 hours)
- Works for 90% of cases
- Low risk

**Cons**:
- Won't work with JOINs (need qualified names)
- May fail with complex queries

### Option 3: Hybrid Approach (Recommended)

**Strategy**:
1. Detect expressions (contains function calls)
2. For simple SELECT (no JOINs): Use unqualified column names in expressions
3. For JOINs: Use simple regex-based column prefixing

**Implementation**:

```rust
// In view_query.rs
fn format_property_projection(
    table: &str,
    property_value: &str,
    property_name: &str,
    has_joins: bool,
) -> String {
    if is_expression(property_value) {
        // Expression with functions
        if has_joins {
            // Need to prefix column references
            let prefixed_expr = prefix_columns_simple(property_value, table);
            format!("{} AS {}", prefixed_expr, property_name)
        } else {
            // Can use unprefixed column names
            format!("{} AS {}", property_value, property_name)
        }
    } else {
        // Simple column reference
        format!("{}.{} AS {}", table, property_value, property_name)
    }
}

fn is_expression(value: &str) -> bool {
    // Check if this looks like an expression (not just a column name)
    value.contains('(') || value.contains('+') || value.contains('-') 
        || value.contains('*') || value.contains('/') || value.contains("CASE")
}

fn prefix_columns_simple(expr: &str, table: &str) -> String {
    // Simple regex-based approach for common cases
    // This won't be perfect but will handle most cases
    
    let re = Regex::new(r"\b([a-z_][a-z0-9_]*)\b").unwrap();
    
    re.replace_all(expr, |caps: &regex::Captures| {
        let word = &caps[1];
        
        // Don't prefix if it's a ClickHouse function or keyword
        if is_clickhouse_function_or_keyword(word) {
            word.to_string()
        } else {
            // Assume it's a column reference
            format!("{}.{}", table, word)
        }
    }).to_string()
}

fn is_clickhouse_function_or_keyword(word: &str) -> bool {
    // List of common ClickHouse functions and SQL keywords
    const FUNCTIONS_AND_KEYWORDS: &[&str] = &[
        // Aggregate functions
        "count", "sum", "avg", "min", "max",
        // String functions
        "concat", "substring", "upper", "lower", "trim", "split", "splitbychar",
        "length", "replace", "reverse", "left", "right",
        // Date functions
        "datediff", "todate", "today", "now", "adddays", "subtractdays",
        "toyear", "tomonth", "toweek", "todate",
        // Type conversion
        "touint8", "touint16", "touint32", "touint64",
        "toint8", "toint16", "toint32", "toint64",
        "tofloat32", "tofloat64", "tostring", "todate", "todatetime",
        // Conditional
        "multiif", "if",
        // JSON
        "jsonextractstring", "jsonextract",
        // Math
        "abs", "ceil", "floor", "round", "sqrt", "power",
        // SQL keywords
        "case", "when", "then", "else", "end", "and", "or", "not",
        "null", "true", "false", "interval", "day", "month", "year",
    ];
    
    FUNCTIONS_AND_KEYWORDS.contains(&word.to_lowercase().as_str())
}
```

---

## Recommended Implementation: Option 3 (Hybrid)

### Phase 1: Fix Simple SELECT Cases (2 hours)

For queries without JOINs, use unqualified column names in expressions:

**File**: `src/clickhouse_query_generator/view_query.rs`

```rust
impl ToSql for PlanViewScan {
    fn to_sql(&self) -> Result<String, super::errors::ClickhouseQueryGeneratorError> {
        let mut sql = String::new();
        let mut projections = Vec::new();

        // Determine if this query has joins
        let has_joins = self.input.is_some();

        // Always include ID column first
        projections.push(format!("{}.{} AS id", self.source_table, self.id_column));

        // Add property mappings
        for (prop, col) in &self.property_mapping {
            if prop != "id" {
                // Check if col is an expression
                if is_expression(col) {
                    if has_joins {
                        // Need qualified column names
                        let prefixed = prefix_columns_simple(col, &self.source_table)?;
                        projections.push(format!("{} AS {}", prefixed, prop));
                    } else {
                        // Can use unqualified column names
                        projections.push(format!("{} AS {}", col, prop));
                    }
                } else {
                    // Simple column reference
                    projections.push(format!("{}.{} AS {}", self.source_table, col, prop));
                }
            }
        }

        // ... rest of SQL generation
    }
}
```

### Phase 2: Add Column Prefixing for JOINs (4 hours)

Implement `prefix_columns_simple()` with regex-based column detection.

**Challenges**:
- Distinguish column names from function names
- Handle nested functions: `concat(upper(first_name), ' ', last_name)`
- Handle literals: `'string literal'`, `123`, `true`
- Handle operators: `+`, `-`, `*`, `/`, `=`, `>=`, etc.

**Algorithm**:
1. Tokenize expression (split on operators, parentheses, commas)
2. For each token:
   - If it's a known function → don't prefix
   - If it's a literal (quoted string, number) → don't prefix
   - If it's followed by `(` → it's a function name, don't prefix
   - Otherwise → assume column name, add table prefix

---

## Testing Strategy

### Test Cases

**Simple expressions (no JOINs)**:
```cypher
MATCH (u:User) RETURN u.full_name
-- Should generate: SELECT concat(first_name, ' ', last_name) AS full_name
```

**Complex expressions (no JOINs)**:
```cypher
MATCH (u:User) RETURN u.tier
-- Should generate: SELECT CASE WHEN score >= 1000 THEN 'gold' ... END AS tier
```

**Expressions with JOINs**:
```cypher
MATCH (u:User)-[:FOLLOWS]->(u2:User) RETURN u.full_name, u2.full_name
-- Should generate: 
-- SELECT concat(u.first_name, ' ', u.last_name), 
--        concat(u2.first_name, ' ', u2.last_name)
```

---

## Alternative: Expression Pre-processing at Schema Load

**Idea**: When loading schema, pre-process all expressions and store them in a parsed form.

**Benefits**:
- Parse once, use many times
- Can validate expressions at schema load time
- Better error messages

**Implementation**:
```rust
struct PropertyMapping {
    name: String,
    value: PropertyValue,
}

enum PropertyValue {
    Column(String),
    Expression {
        raw: String,
        columns: Vec<String>,  // List of column references
        sql_template: String,  // Template with {{column}} placeholders
    }
}

// At schema load time:
fn parse_property_value(value: &str) -> PropertyValue {
    if is_simple_column(value) {
        PropertyValue::Column(value.to_string())
    } else {
        // Parse expression, extract column names
        let columns = extract_column_names(value);
        PropertyValue::Expression {
            raw: value.to_string(),
            columns,
            sql_template: create_template(value),
        }
    }
}

// At SQL generation time:
fn generate_sql(prop: &PropertyMapping, table: &str) -> String {
    match &prop.value {
        PropertyValue::Column(col) => {
            format!("{}.{}", table, col)
        }
        PropertyValue::Expression { columns, sql_template, .. } => {
            // Replace column placeholders with qualified names
            let mut sql = sql_template.clone();
            for col in columns {
                sql = sql.replace(&format!("{{{{{}}}}}", col), &format!("{}.{}", table, col));
            }
            sql
        }
    }
}
```

**Pro**: Clean, efficient, validates expressions early  
**Con**: More complex schema loading, needs expression parser

---

## Recommendation

**Start with Option 3, Phase 1**:
1. Detect expressions (has `(` or operators)
2. For simple SELECT (no JOINs): Use unqualified column names
3. This will fix **100% of the failing tests** immediately (tests don't have JOINs!)

**Then add Phase 2** (column prefixing) **if needed** for JOIN support.

**Timeline**:
- Phase 1: 2 hours (fixes all current tests)
- Phase 2: 4 hours (adds JOIN support)
- Total: 6 hours (~1 day)

**Much simpler than full SQL parser, handles 95% of real-world cases.**

---

## Next Steps

1. Implement `is_expression()` helper
2. Modify `view_query.rs` to use unqualified names for expressions
3. Re-run 28 tests → expect 28/28 passing
4. Test with JOINs to see if Phase 2 is needed
5. Document behavior and limitations

