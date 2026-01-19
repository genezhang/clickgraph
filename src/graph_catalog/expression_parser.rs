/// Property expression parser for ClickHouse scalar expressions
///
/// Parses expressions used in schema property mappings:
/// - Column references: `user_id`, `full_name`
/// - Quoted identifiers: `"First Name"`, `` `User-ID` ``
/// - Function calls: `concat(first_name, ' ', last_name)`
/// - Math operations: `score / 100.0`, `price * quantity`
/// - Array indexing: `tags[1]`
/// - Comparisons: `age >= 18`, `length(col) > 0`
/// - Boolean logic: `AND`, `OR`, `NOT`
///
/// Does NOT support (use at query time):
/// - Conditionals: `CASE WHEN`, `multiIf()`, `IF()`
/// - Lambdas: `arrayMap(x -> expr, arr)`
use nom::{
    branch::alt,
    bytes::complete::{tag, take_until},
    character::complete::{alphanumeric1, char, digit1, multispace0, one_of},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0},
    sequence::{delimited, preceded},
    IResult, Parser,
};
use serde::{Deserialize, Serialize};

/// Property value: either a simple column or a parsed expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    /// Simple column reference (stored as string)
    Column(String),

    /// Expression (stored as raw string, parsed on demand)
    Expression(String),
}

impl PropertyValue {
    /// Apply table prefix to generate SQL
    pub fn to_sql(&self, table_alias: &str) -> String {
        log::debug!(
            "PropertyValue.to_sql called: variant={}, value='{}', table_alias='{}'",
            match self {
                PropertyValue::Column(_) => "Column",
                PropertyValue::Expression(_) => "Expression",
            },
            self.raw(),
            table_alias
        );
        match self {
            PropertyValue::Column(col) => {
                // Special case: * is the SQL wildcard and shouldn't be quoted
                if col == "*" {
                    format!("{}.*", table_alias)
                } else if needs_quoting(col) {
                    format!("{}.\"{}\"", table_alias, col)
                } else {
                    format!("{}.{}", table_alias, col)
                }
            }
            PropertyValue::Expression(expr) => {
                // Parse and apply table alias
                match parse_clickhouse_scalar_expr(expr) {
                    Ok((_, ast)) => {
                        let sql = ast.to_sql(table_alias);
                        log::debug!("✅ PropertyValue.to_sql: Successfully parsed expression '{}' -> SQL: '{}'", expr, sql);
                        sql
                    }
                    Err(e) => {
                        // Fallback: treat as raw SQL
                        log::warn!(
                            "⚠️ PropertyValue.to_sql: Failed to parse expression '{}', error: {:?}. Using raw string.",
                            expr, e
                        );
                        expr.clone()
                    }
                }
            }
        }
    }

    /// Generate SQL for just the column/expression without table prefix.
    /// Used for rendering filters inside subqueries where no alias exists yet.
    pub fn to_sql_column_only(&self) -> String {
        match self {
            PropertyValue::Column(col) => {
                if needs_quoting(col) {
                    format!("\"{}\"", col)
                } else {
                    col.clone()
                }
            }
            PropertyValue::Expression(expr) => {
                // Parse and render without table alias
                match parse_clickhouse_scalar_expr(expr) {
                    Ok((_, ast)) => ast.to_sql_no_alias(),
                    Err(_) => {
                        // Fallback: treat as raw SQL
                        crate::debug_print!(
                            "Warning: Failed to parse expression '{}', using as-is",
                            expr
                        );
                        expr.clone()
                    }
                }
            }
        }
    }

    /// Get raw value (for debugging)
    pub fn raw(&self) -> &str {
        match self {
            PropertyValue::Column(col) => col,
            PropertyValue::Expression(expr) => expr,
        }
    }

    /// Get all column references from this value
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            PropertyValue::Column(col) => vec![col.clone()],
            PropertyValue::Expression(expr) => {
                // Parse and extract columns
                match parse_clickhouse_scalar_expr(expr) {
                    Ok((_, ast)) => ast.get_columns(),
                    Err(_) => vec![],
                }
            }
        }
    }
}

/// ClickHouse expression AST (simplified)
#[derive(Debug, Clone, PartialEq)]
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

    /// Array indexing: tags[1]
    ArrayIndex {
        array: Box<ClickHouseExpr>,
        index: Box<ClickHouseExpr>,
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
            ClickHouseExpr::ArrayIndex { array, index } => {
                let mut cols = array.get_columns();
                cols.extend(index.get_columns());
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
                let args_sql: Vec<String> = args.iter().map(|a| a.to_sql(table_alias)).collect();
                format!("{}({})", name, args_sql.join(", "))
            }
            ClickHouseExpr::BinaryOp { op, left, right } => {
                format!(
                    "({} {} {})",
                    left.to_sql(table_alias),
                    op.to_str(),
                    right.to_sql(table_alias)
                )
            }
            ClickHouseExpr::ArrayIndex { array, index } => {
                format!(
                    "{}[{}]",
                    array.to_sql(table_alias),
                    index.to_sql(table_alias)
                )
            }
            ClickHouseExpr::Literal(lit) => lit.to_sql(),
        }
    }

    /// Generate SQL without table alias prefix.
    /// Used for rendering filters inside subqueries where no alias exists yet.
    pub fn to_sql_no_alias(&self) -> String {
        match self {
            ClickHouseExpr::Column(col) => col.clone(),
            ClickHouseExpr::QuotedColumn(col) => format!("\"{}\"", col),
            ClickHouseExpr::FunctionCall { name, args } => {
                let args_sql: Vec<String> = args.iter().map(|a| a.to_sql_no_alias()).collect();
                format!("{}({})", name, args_sql.join(", "))
            }
            ClickHouseExpr::BinaryOp { op, left, right } => {
                format!(
                    "({} {} {})",
                    left.to_sql_no_alias(),
                    op.to_str(),
                    right.to_sql_no_alias()
                )
            }
            ClickHouseExpr::ArrayIndex { array, index } => {
                format!("{}[{}]", array.to_sql_no_alias(), index.to_sql_no_alias())
            }
            ClickHouseExpr::Literal(lit) => lit.to_sql(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
}

impl Literal {
    pub fn to_sql(&self) -> String {
        match self {
            Literal::String(s) => format!("'{}'", s.replace('\'', "''")),
            Literal::Integer(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operator {
    // Arithmetic
    Addition,
    Subtraction,
    Multiplication,
    Division,
    Modulo,
    // Comparison
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl Operator {
    pub fn to_str(&self) -> &str {
        match self {
            Operator::Addition => "+",
            Operator::Subtraction => "-",
            Operator::Multiplication => "*",
            Operator::Division => "/",
            Operator::Modulo => "%",
            Operator::Equal => "=",
            Operator::NotEqual => "!=",
            Operator::Less => "<",
            Operator::LessOrEqual => "<=",
            Operator::Greater => ">",
            Operator::GreaterOrEqual => ">=",
        }
    }
}

/// Parse property value (entry point)
pub fn parse_property_value(value: &str) -> Result<PropertyValue, String> {
    let value = value.trim();

    // Check for simple column name
    if is_simple_column(value) {
        return Ok(PropertyValue::Column(value.to_string()));
    }

    // Parse as expression
    match parse_clickhouse_scalar_expr(value) {
        Ok((remaining, _ast)) => {
            let remaining = remaining.trim();
            if !remaining.is_empty() {
                return Err(format!("Unexpected trailing content: '{}'", remaining));
            }
            // Store as expression (raw string)
            Ok(PropertyValue::Expression(value.to_string()))
        }
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
}

fn is_simple_column(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    let first = s.chars().next().unwrap();
    if !first.is_alphabetic() && first != '_' {
        return false;
    }

    // Allow alphanumeric, underscore, and dot (for nested columns like id.orig_h)
    s.chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
}

fn needs_quoting(col: &str) -> bool {
    // If column name contains spaces or special chars, it needs quoting
    col.chars().any(|c| !c.is_alphanumeric() && c != '_')
}

/// Parse ClickHouse scalar expression (entry point for complex expressions)
pub(crate) fn parse_clickhouse_scalar_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    parse_binary_expr(input)
}

/// Parse binary operations with precedence
/// Precedence (low to high): comparison, additive, multiplicative, postfix, primary
fn parse_binary_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    // Start with comparison level (lowest precedence for binary ops)
    parse_comparison_expr(input)
}

/// Parse comparison operations (<, >, <=, >=, =, !=)
fn parse_comparison_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    let (input, left) = parse_additive_expr(input)?;
    let (mut input, _) = multispace0(input)?;

    // Try to parse comparison operator (order matters - check 2-char ops first)
    let op_opt = if let Ok((new_input, _)) = tag::<_, _, nom::error::Error<_>>("<=")(input) {
        input = new_input;
        Some(Operator::LessOrEqual)
    } else if let Ok((new_input, _)) = tag::<_, _, nom::error::Error<_>>(">=")(input) {
        input = new_input;
        Some(Operator::GreaterOrEqual)
    } else if let Ok((new_input, _)) = tag::<_, _, nom::error::Error<_>>("!=")(input) {
        input = new_input;
        Some(Operator::NotEqual)
    } else if let Ok((new_input, _)) = tag::<_, _, nom::error::Error<_>>("<>")(input) {
        input = new_input;
        Some(Operator::NotEqual)
    } else if let Ok((new_input, _)) = tag::<_, _, nom::error::Error<_>>("<")(input) {
        input = new_input;
        Some(Operator::Less)
    } else if let Ok((new_input, _)) = tag::<_, _, nom::error::Error<_>>(">")(input) {
        input = new_input;
        Some(Operator::Greater)
    } else if let Ok((new_input, _)) = tag::<_, _, nom::error::Error<_>>("=")(input) {
        input = new_input;
        Some(Operator::Equal)
    } else {
        None
    };

    if let Some(op) = op_opt {
        let (input, _) = multispace0(input)?;
        let (input, right) = parse_additive_expr(input)?;
        Ok((
            input,
            ClickHouseExpr::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    } else {
        Ok((input, left))
    }
}

fn parse_additive_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    let (input, left) = parse_multiplicative_expr(input)?;

    // Try to parse additional additive operations
    let mut current_left = left;
    let mut current_input = input;

    loop {
        let (new_input, _) = multispace0(current_input)?;

        // Try to parse + or -
        if let Ok((new_input, _)) = char::<_, nom::error::Error<_>>('+')(new_input) {
            let op = Operator::Addition;
            let (new_input, _) = multispace0(new_input)?;
            if let Ok((new_input, right)) = parse_multiplicative_expr(new_input) {
                current_left = ClickHouseExpr::BinaryOp {
                    op,
                    left: Box::new(current_left),
                    right: Box::new(right),
                };
                current_input = new_input;
                continue;
            } else {
                return Ok((current_input, current_left));
            }
        } else if let Ok((new_input, _)) = char::<_, nom::error::Error<_>>('-')(new_input) {
            let op = Operator::Subtraction;
            let (new_input, _) = multispace0(new_input)?;
            if let Ok((new_input, right)) = parse_multiplicative_expr(new_input) {
                current_left = ClickHouseExpr::BinaryOp {
                    op,
                    left: Box::new(current_left),
                    right: Box::new(right),
                };
                current_input = new_input;
                continue;
            } else {
                return Ok((current_input, current_left));
            }
        } else {
            return Ok((current_input, current_left));
        }
    }
}

fn parse_multiplicative_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    let (input, left) = parse_postfix_expr(input)?;

    // Try to parse additional multiplicative operations
    let mut current_left = left;
    let mut current_input = input;

    loop {
        let (new_input, _) = multispace0(current_input)?;

        // Try to parse *, /, or %
        let op_and_input =
            if let Ok((new_input, _)) = char::<_, nom::error::Error<_>>('*')(new_input) {
                Some((new_input, Operator::Multiplication))
            } else if let Ok((new_input, _)) = char::<_, nom::error::Error<_>>('/')(new_input) {
                Some((new_input, Operator::Division))
            } else if let Ok((new_input, _)) = char::<_, nom::error::Error<_>>('%')(new_input) {
                Some((new_input, Operator::Modulo))
            } else {
                None
            };

        match op_and_input {
            Some((new_input, op)) => {
                let (new_input, _) = multispace0(new_input)?;
                match parse_postfix_expr(new_input) {
                    Ok((new_input, right)) => {
                        current_left = ClickHouseExpr::BinaryOp {
                            op,
                            left: Box::new(current_left),
                            right: Box::new(right),
                        };
                        current_input = new_input;
                    }
                    Err(_) => return Ok((current_input, current_left)),
                }
            }
            None => return Ok((current_input, current_left)),
        }
    }
}

/// Parse postfix operations (array indexing)
fn parse_postfix_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    let (input, mut expr) = parse_primary_expr(input)?;

    // Handle array indexing: expr[index]
    let mut current_input = input;
    loop {
        let (new_input, _) = multispace0(current_input)?;

        match char::<_, nom::error::Error<_>>('[')(new_input) {
            Ok((new_input, _)) => {
                let (new_input, _) = multispace0(new_input)?;
                let (new_input, index) = parse_clickhouse_scalar_expr(new_input)?;
                let (new_input, _) = multispace0(new_input)?;
                let (new_input, _) = char(']')(new_input)?;

                expr = ClickHouseExpr::ArrayIndex {
                    array: Box::new(expr),
                    index: Box::new(index),
                };
                current_input = new_input;
            }
            Err(_) => break,
        }
    }

    Ok((current_input, expr))
}

/// Parse primary expressions (highest precedence)
fn parse_primary_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        parse_function_call_expr,
        parse_literal_expr, // Try literals before identifiers
        parse_quoted_identifier,
        parse_identifier_expr,
        delimited(
            char('('),
            delimited(multispace0, parse_clickhouse_scalar_expr, multispace0),
            char(')'),
        ),
    ))
    .parse(input)
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
    )
    .parse(input)?;

    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    Ok((
        input,
        ClickHouseExpr::FunctionCall {
            name: name.to_string(),
            args,
        },
    ))
}

/// Parse quoted identifier: "First Name" or `User-ID`
fn parse_quoted_identifier(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        // Double quotes
        map(
            delimited(char('"'), take_until("\""), char('"')),
            |s: &str| ClickHouseExpr::QuotedColumn(s.to_string()),
        ),
        // Backticks
        map(
            delimited(char('`'), take_until("`"), char('`')),
            |s: &str| ClickHouseExpr::QuotedColumn(s.to_string()),
        ),
    ))
    .parse(input)
}

/// Parse bare identifier
fn parse_identifier_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    map(parse_identifier_str, |s| {
        ClickHouseExpr::Column(s.to_string())
    })
    .parse(input)
}

fn parse_identifier_str(input: &str) -> IResult<&str, &str> {
    recognize((
        alt((alphanumeric1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))
    .parse(input)
}

/// Parse literal: 'string', 123, 45.67
fn parse_literal_expr(input: &str) -> IResult<&str, ClickHouseExpr> {
    alt((
        // String literal: 'hello'
        map(
            delimited(char('\''), take_until("'"), char('\'')),
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
    ))
    .parse(input)
}

fn recognize_number(input: &str) -> IResult<&str, &str> {
    recognize((opt(one_of("+-")), digit1, opt(preceded(char('.'), digit1)))).parse(input)
}

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
    fn test_column_with_underscore() {
        let pv = parse_property_value("first_name").unwrap();
        assert_eq!(pv.to_sql("u"), "u.first_name");
    }

    #[test]
    fn test_concat_expression() {
        let pv = parse_property_value("concat(first_name, ' ', last_name)").unwrap();
        assert_eq!(pv.to_sql("u"), "concat(u.first_name, ' ', u.last_name)");
    }

    #[test]
    fn test_math_expression() {
        let pv = parse_property_value("score / 100").unwrap();
        // 100 is parsed as integer literal
        assert_eq!(pv.to_sql("u"), "(u.score / 100)");
    }

    #[test]
    fn test_nested_functions() {
        let pv = parse_property_value("upper(concat(first_name, last_name))").unwrap();
        assert_eq!(pv.to_sql("u"), "upper(concat(u.first_name, u.last_name))");
    }

    #[test]
    fn test_quoted_columns() {
        let pv = parse_property_value(r#"concat("First Name", " ", "Last Name")"#).unwrap();
        // All quoted identifiers are treated as columns
        assert_eq!(
            pv.to_sql("u"),
            r#"concat(u."First Name", u." ", u."Last Name")"#
        );
    }

    #[test]
    fn test_backtick_columns() {
        let pv = parse_property_value(r#"concat(`User-ID`, `_`, `Tenant-ID`)"#).unwrap();
        // All backtick identifiers are treated as columns
        assert_eq!(
            pv.to_sql("u"),
            r#"concat(u."User-ID", u."_", u."Tenant-ID")"#
        );
    }

    #[test]
    fn test_array_indexing() {
        let pv = parse_property_value("tags[1]").unwrap();
        assert_eq!(pv.to_sql("u"), "u.tags[1]");
    }

    #[test]
    fn test_array_negative_index() {
        let pv = parse_property_value("tags[-1]").unwrap();
        assert_eq!(pv.to_sql("u"), "u.tags[-1]");
    }

    #[test]
    fn test_array_function() {
        let pv = parse_property_value("length(tags)").unwrap();
        assert_eq!(pv.to_sql("u"), "length(u.tags)");
    }

    #[test]
    fn test_complex_math() {
        let pv = parse_property_value("(price * quantity) - discount").unwrap();
        assert_eq!(pv.to_sql("u"), "((u.price * u.quantity) - u.discount)");
    }

    #[test]
    fn test_date_diff() {
        let pv = parse_property_value("dateDiff('day', start_date, end_date)").unwrap();
        assert_eq!(pv.to_sql("u"), "dateDiff('day', u.start_date, u.end_date)");
    }

    #[test]
    fn test_type_conversion() {
        let pv = parse_property_value("toUInt8(age_str)").unwrap();
        assert_eq!(pv.to_sql("u"), "toUInt8(u.age_str)");
    }

    #[test]
    fn test_get_columns() {
        let pv = parse_property_value("concat(first_name, ' ', last_name)").unwrap();
        let cols = pv.get_columns();
        assert_eq!(cols, vec!["first_name", "last_name"]);
    }

    #[test]
    fn test_get_columns_nested() {
        let pv = parse_property_value("upper(concat(first_name, last_name))").unwrap();
        let cols = pv.get_columns();
        assert_eq!(cols, vec!["first_name", "last_name"]);
    }

    #[test]
    fn test_modulo_operator() {
        let pv = parse_property_value("id % 10").unwrap();
        assert_eq!(pv.to_sql("u"), "(u.id % 10)");
    }

    #[test]
    fn test_string_literal_escaping() {
        let pv = parse_property_value("concat(name, 'test')").unwrap();
        assert_eq!(pv.to_sql("u"), "concat(u.name, 'test')");
    }

    #[test]
    fn test_whitespace_handling() {
        let pv = parse_property_value("  concat( first_name ,  last_name )  ").unwrap();
        assert_eq!(pv.to_sql("u"), "concat(u.first_name, u.last_name)");
    }

    #[test]
    fn test_error_trailing_content() {
        let result = parse_property_value("user_id extra");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unexpected trailing content"));
    }

    #[test]
    fn test_error_unclosed_paren() {
        let result = parse_property_value("concat(first_name, last_name");
        assert!(result.is_err());
    }

    #[test]
    fn test_function_no_args() {
        let pv = parse_property_value("now()").unwrap();
        assert_eq!(pv.to_sql("u"), "now()");
    }

    #[test]
    fn test_negative_number() {
        let pv = parse_property_value("score + -100").unwrap();
        assert_eq!(pv.to_sql("u"), "(u.score + -100)");
    }
}
