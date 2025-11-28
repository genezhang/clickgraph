/// Filter predicate parser for ClickHouse WHERE clause expressions
///
/// Extends the scalar expression parser to support:
/// - Comparison operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
/// - Boolean operators: `AND`, `OR`, `NOT`
/// - Special predicates: `IN`, `LIKE`, `BETWEEN`, `IS NULL`, `IS NOT NULL`
/// - INTERVAL expressions: `INTERVAL 7 DAY`
///
/// Examples:
/// - `ts >= now() - INTERVAL 7 DAY`
/// - `proto = 'tcp' AND port IN (80, 443, 8080)`
/// - `status IS NOT NULL AND active = true`

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until},
    character::complete::{char, multispace0, multispace1, alphanumeric1, digit1, one_of},
    combinator::{map, opt, recognize, value},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, tuple},
};
use serde::{Deserialize, Serialize};

use super::expression_parser::{ClickHouseExpr, parse_clickhouse_scalar_expr};

/// Filter predicate AST
#[derive(Debug, Clone, PartialEq)]
pub enum FilterPredicate {
    /// Comparison: a = b, x > 5
    Comparison {
        left: ClickHouseExpr,
        op: ComparisonOp,
        right: ClickHouseExpr,
    },

    /// Boolean AND: a AND b
    And(Box<FilterPredicate>, Box<FilterPredicate>),

    /// Boolean OR: a OR b
    Or(Box<FilterPredicate>, Box<FilterPredicate>),

    /// Boolean NOT: NOT a
    Not(Box<FilterPredicate>),

    /// IN list: x IN (1, 2, 3)
    In {
        expr: ClickHouseExpr,
        values: Vec<ClickHouseExpr>,
        negated: bool,
    },

    /// LIKE pattern: name LIKE 'foo%'
    Like {
        expr: ClickHouseExpr,
        pattern: String,
        negated: bool,
    },

    /// BETWEEN range: x BETWEEN 1 AND 10
    Between {
        expr: ClickHouseExpr,
        low: ClickHouseExpr,
        high: ClickHouseExpr,
        negated: bool,
    },

    /// IS NULL / IS NOT NULL
    IsNull {
        expr: ClickHouseExpr,
        negated: bool,
    },

    /// Parenthesized expression
    Parenthesized(Box<FilterPredicate>),

    /// Raw scalar expression (for boolean columns like `active`)
    Scalar(ClickHouseExpr),
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ComparisonOp {
    Equal,          // =
    NotEqual,       // != or <>
    Less,           // <
    LessOrEqual,    // <=
    Greater,        // >
    GreaterOrEqual, // >=
}

impl ComparisonOp {
    pub fn to_sql(&self) -> &'static str {
        match self {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::Less => "<",
            ComparisonOp::LessOrEqual => "<=",
            ComparisonOp::Greater => ">",
            ComparisonOp::GreaterOrEqual => ">=",
        }
    }
}

/// Stored filter value with parsed AST
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFilter {
    /// Original filter string
    pub raw: String,
}

impl SchemaFilter {
    /// Create a new schema filter from a string
    pub fn new(filter: &str) -> Result<Self, String> {
        // Validate by parsing
        parse_filter_predicate(filter)?;
        Ok(SchemaFilter {
            raw: filter.to_string(),
        })
    }

    /// Generate SQL with table alias prefix
    pub fn to_sql(&self, table_alias: &str) -> Result<String, String> {
        let (remaining, ast) = parse_filter_predicate(&self.raw)?;
        if !remaining.trim().is_empty() {
            return Err(format!("Unexpected trailing content: '{}'", remaining));
        }
        Ok(ast.to_sql(table_alias))
    }

    /// Get all column references from the filter
    pub fn get_columns(&self) -> Vec<String> {
        match parse_filter_predicate(&self.raw) {
            Ok((_, ast)) => ast.get_columns(),
            Err(_) => vec![],
        }
    }
}

impl FilterPredicate {
    /// Generate SQL with table alias prefix
    pub fn to_sql(&self, table_alias: &str) -> String {
        match self {
            FilterPredicate::Comparison { left, op, right } => {
                format!(
                    "{} {} {}",
                    left.to_sql(table_alias),
                    op.to_sql(),
                    right.to_sql(table_alias)
                )
            }

            FilterPredicate::And(left, right) => {
                format!(
                    "({} AND {})",
                    left.to_sql(table_alias),
                    right.to_sql(table_alias)
                )
            }

            FilterPredicate::Or(left, right) => {
                format!(
                    "({} OR {})",
                    left.to_sql(table_alias),
                    right.to_sql(table_alias)
                )
            }

            FilterPredicate::Not(inner) => {
                format!("NOT {}", inner.to_sql(table_alias))
            }

            FilterPredicate::In { expr, values, negated } => {
                let values_sql: Vec<String> = values
                    .iter()
                    .map(|v| v.to_sql(table_alias))
                    .collect();
                let not_str = if *negated { "NOT " } else { "" };
                format!(
                    "{} {}IN ({})",
                    expr.to_sql(table_alias),
                    not_str,
                    values_sql.join(", ")
                )
            }

            FilterPredicate::Like { expr, pattern, negated } => {
                let not_str = if *negated { "NOT " } else { "" };
                format!(
                    "{} {}LIKE '{}'",
                    expr.to_sql(table_alias),
                    not_str,
                    pattern.replace('\'', "''")
                )
            }

            FilterPredicate::Between { expr, low, high, negated } => {
                let not_str = if *negated { "NOT " } else { "" };
                format!(
                    "{} {}BETWEEN {} AND {}",
                    expr.to_sql(table_alias),
                    not_str,
                    low.to_sql(table_alias),
                    high.to_sql(table_alias)
                )
            }

            FilterPredicate::IsNull { expr, negated } => {
                let null_str = if *negated { "IS NOT NULL" } else { "IS NULL" };
                format!("{} {}", expr.to_sql(table_alias), null_str)
            }

            FilterPredicate::Parenthesized(inner) => {
                format!("({})", inner.to_sql(table_alias))
            }

            FilterPredicate::Scalar(expr) => expr.to_sql(table_alias),
        }
    }

    /// Extract all column references from the filter
    pub fn get_columns(&self) -> Vec<String> {
        match self {
            FilterPredicate::Comparison { left, right, .. } => {
                let mut cols = left.get_columns();
                cols.extend(right.get_columns());
                cols
            }
            FilterPredicate::And(left, right) | FilterPredicate::Or(left, right) => {
                let mut cols = left.get_columns();
                cols.extend(right.get_columns());
                cols
            }
            FilterPredicate::Not(inner) | FilterPredicate::Parenthesized(inner) => {
                inner.get_columns()
            }
            FilterPredicate::In { expr, values, .. } => {
                let mut cols = expr.get_columns();
                for v in values {
                    cols.extend(v.get_columns());
                }
                cols
            }
            FilterPredicate::Like { expr, .. } => expr.get_columns(),
            FilterPredicate::Between { expr, low, high, .. } => {
                let mut cols = expr.get_columns();
                cols.extend(low.get_columns());
                cols.extend(high.get_columns());
                cols
            }
            FilterPredicate::IsNull { expr, .. } => expr.get_columns(),
            FilterPredicate::Scalar(expr) => expr.get_columns(),
        }
    }
}

/// Parse a filter predicate string
pub fn parse_filter_predicate(input: &str) -> Result<(&str, FilterPredicate), String> {
    let input = input.trim();
    parse_or_expr(input).map_err(|e| format!("Filter parse error: {:?}", e))
}

/// Parse OR expressions (lowest precedence for boolean)
fn parse_or_expr(input: &str) -> IResult<&str, FilterPredicate> {
    let (input, left) = parse_and_expr(input)?;
    
    let mut current = left;
    let mut current_input = input;
    
    loop {
        let (new_input, _) = multispace0(current_input)?;
        
        // Try to parse OR keyword
        if let Ok((new_input, _)) = tag_no_case::<_, _, nom::error::Error<_>>("OR")(new_input) {
            // Must have whitespace after OR
            if let Ok((new_input, _)) = multispace1::<_, nom::error::Error<_>>(new_input) {
                if let Ok((new_input, right)) = parse_and_expr(new_input) {
                    current = FilterPredicate::Or(Box::new(current), Box::new(right));
                    current_input = new_input;
                    continue;
                }
            }
        }
        break;
    }
    
    Ok((current_input, current))
}

/// Parse AND expressions
fn parse_and_expr(input: &str) -> IResult<&str, FilterPredicate> {
    let (input, left) = parse_not_expr(input)?;
    
    let mut current = left;
    let mut current_input = input;
    
    loop {
        let (new_input, _) = multispace0(current_input)?;
        
        // Try to parse AND keyword
        if let Ok((new_input, _)) = tag_no_case::<_, _, nom::error::Error<_>>("AND")(new_input) {
            // Must have whitespace after AND
            if let Ok((new_input, _)) = multispace1::<_, nom::error::Error<_>>(new_input) {
                if let Ok((new_input, right)) = parse_not_expr(new_input) {
                    current = FilterPredicate::And(Box::new(current), Box::new(right));
                    current_input = new_input;
                    continue;
                }
            }
        }
        break;
    }
    
    Ok((current_input, current))
}

/// Parse NOT expressions
fn parse_not_expr(input: &str) -> IResult<&str, FilterPredicate> {
    let (input, _) = multispace0(input)?;
    
    // Try to parse NOT keyword
    if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<_>>("NOT")(input) {
        if let Ok((input, _)) = multispace1::<_, nom::error::Error<_>>(input) {
            let (input, inner) = parse_not_expr(input)?;
            return Ok((input, FilterPredicate::Not(Box::new(inner))));
        }
    }
    
    parse_comparison_expr(input)
}

/// Parse comparison expressions
fn parse_comparison_expr(input: &str) -> IResult<&str, FilterPredicate> {
    let (input, _) = multispace0(input)?;
    
    // Try parenthesized expression first
    if let Ok((input, _)) = char::<_, nom::error::Error<_>>('(')(input) {
        let (input, _) = multispace0(input)?;
        let (input, inner) = parse_or_expr(input)?;
        let (input, _) = multispace0(input)?;
        let (input, _) = char(')')(input)?;
        return Ok((input, FilterPredicate::Parenthesized(Box::new(inner))));
    }
    
    // Parse left-hand side scalar expression
    let (input, left) = parse_clickhouse_scalar_expr(input)?;
    let (input, _) = multispace0(input)?;
    
    // Try IS NULL / IS NOT NULL
    if let Ok((remaining, pred)) = parse_is_null(&left, input) {
        return Ok((remaining, pred));
    }
    
    // Try IN / NOT IN
    if let Ok((remaining, pred)) = parse_in_expr(&left, input) {
        return Ok((remaining, pred));
    }
    
    // Try LIKE / NOT LIKE
    if let Ok((remaining, pred)) = parse_like_expr(&left, input) {
        return Ok((remaining, pred));
    }
    
    // Try BETWEEN / NOT BETWEEN
    if let Ok((remaining, pred)) = parse_between_expr(&left, input) {
        return Ok((remaining, pred));
    }
    
    // Try comparison operators
    if let Ok((remaining, pred)) = parse_comparison_op(&left, input) {
        return Ok((remaining, pred));
    }
    
    // Fall back to scalar expression (for boolean columns)
    Ok((input, FilterPredicate::Scalar(left)))
}

/// Parse comparison operator and right-hand side
fn parse_comparison_op<'a>(left: &ClickHouseExpr, input: &'a str) -> IResult<&'a str, FilterPredicate> {
    let (input, op) = alt((
        value(ComparisonOp::NotEqual, tag("<>")),
        value(ComparisonOp::NotEqual, tag("!=")),
        value(ComparisonOp::LessOrEqual, tag("<=")),
        value(ComparisonOp::GreaterOrEqual, tag(">=")),
        value(ComparisonOp::Less, tag("<")),
        value(ComparisonOp::Greater, tag(">")),
        value(ComparisonOp::Equal, tag("=")),
    )).parse(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, right) = parse_clickhouse_scalar_expr(input)?;
    
    Ok((input, FilterPredicate::Comparison {
        left: left.clone(),
        op,
        right,
    }))
}

/// Parse IS NULL / IS NOT NULL
fn parse_is_null<'a>(left: &ClickHouseExpr, input: &'a str) -> IResult<&'a str, FilterPredicate> {
    let (input, _) = tag_no_case("IS")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Check for NOT
    let (input, negated) = if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<_>>("NOT")(input) {
        let (input, _) = multispace1(input)?;
        (input, true)
    } else {
        (input, false)
    };
    
    let (input, _) = tag_no_case("NULL")(input)?;
    
    Ok((input, FilterPredicate::IsNull {
        expr: left.clone(),
        negated,
    }))
}

/// Parse IN / NOT IN
fn parse_in_expr<'a>(left: &ClickHouseExpr, input: &'a str) -> IResult<&'a str, FilterPredicate> {
    // Check for NOT
    let (input, negated) = if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<_>>("NOT")(input) {
        let (input, _) = multispace1(input)?;
        (input, true)
    } else {
        (input, false)
    };
    
    let (input, _) = tag_no_case("IN")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, values) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_clickhouse_scalar_expr,
    ).parse(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, FilterPredicate::In {
        expr: left.clone(),
        values,
        negated,
    }))
}

/// Parse LIKE / NOT LIKE
fn parse_like_expr<'a>(left: &ClickHouseExpr, input: &'a str) -> IResult<&'a str, FilterPredicate> {
    // Check for NOT
    let (input, negated) = if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<_>>("NOT")(input) {
        let (input, _) = multispace1(input)?;
        (input, true)
    } else {
        (input, false)
    };
    
    let (input, _) = tag_no_case("LIKE")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse string pattern
    let (input, _) = char('\'')(input)?;
    let (input, pattern) = take_until("'")(input)?;
    let (input, _) = char('\'')(input)?;
    
    Ok((input, FilterPredicate::Like {
        expr: left.clone(),
        pattern: pattern.to_string(),
        negated,
    }))
}

/// Parse BETWEEN / NOT BETWEEN
fn parse_between_expr<'a>(left: &ClickHouseExpr, input: &'a str) -> IResult<&'a str, FilterPredicate> {
    // Check for NOT
    let (input, negated) = if let Ok((input, _)) = tag_no_case::<_, _, nom::error::Error<_>>("NOT")(input) {
        let (input, _) = multispace1(input)?;
        (input, true)
    } else {
        (input, false)
    };
    
    let (input, _) = tag_no_case("BETWEEN")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, low) = parse_clickhouse_scalar_expr(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("AND")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, high) = parse_clickhouse_scalar_expr(input)?;
    
    Ok((input, FilterPredicate::Between {
        expr: left.clone(),
        low,
        high,
        negated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_comparison() {
        let filter = SchemaFilter::new("ts >= now()").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.ts >= now()");
    }

    #[test]
    fn test_comparison_with_interval() {
        // INTERVAL is a function call in our parser
        let filter = SchemaFilter::new("ts >= now() - toIntervalDay(7)").unwrap();
        let sql = filter.to_sql("t").unwrap();
        assert!(sql.contains("t.ts >= (now() - toIntervalDay(7))"));
    }

    #[test]
    fn test_equality() {
        let filter = SchemaFilter::new("proto = 'tcp'").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.proto = 'tcp'");
    }

    #[test]
    fn test_not_equal() {
        let filter = SchemaFilter::new("status != 'deleted'").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.status != 'deleted'");
    }

    #[test]
    fn test_and_expression() {
        let filter = SchemaFilter::new("proto = 'tcp' AND port = 80").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "(t.proto = 'tcp' AND t.port = 80)");
    }

    #[test]
    fn test_or_expression() {
        let filter = SchemaFilter::new("port = 80 OR port = 443").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "(t.port = 80 OR t.port = 443)");
    }

    #[test]
    fn test_complex_boolean() {
        let filter = SchemaFilter::new("proto = 'tcp' AND (port = 80 OR port = 443)").unwrap();
        let sql = filter.to_sql("t").unwrap();
        assert!(sql.contains("t.proto = 'tcp'"));
        assert!(sql.contains("t.port = 80"));
        assert!(sql.contains("t.port = 443"));
    }

    #[test]
    fn test_not_expression() {
        let filter = SchemaFilter::new("NOT deleted").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "NOT t.deleted");
    }

    #[test]
    fn test_in_expression() {
        let filter = SchemaFilter::new("port IN (80, 443, 8080)").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.port IN (80, 443, 8080)");
    }

    #[test]
    fn test_not_in_expression() {
        let filter = SchemaFilter::new("status NOT IN ('deleted', 'archived')").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.status NOT IN ('deleted', 'archived')");
    }

    #[test]
    fn test_like_expression() {
        let filter = SchemaFilter::new("name LIKE 'foo%'").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.name LIKE 'foo%'");
    }

    #[test]
    fn test_not_like_expression() {
        let filter = SchemaFilter::new("name NOT LIKE '%test%'").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.name NOT LIKE '%test%'");
    }

    #[test]
    fn test_between_expression() {
        let filter = SchemaFilter::new("age BETWEEN 18 AND 65").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.age BETWEEN 18 AND 65");
    }

    #[test]
    fn test_is_null() {
        let filter = SchemaFilter::new("deleted_at IS NULL").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.deleted_at IS NULL");
    }

    #[test]
    fn test_is_not_null() {
        let filter = SchemaFilter::new("email IS NOT NULL").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.email IS NOT NULL");
    }

    #[test]
    fn test_get_columns() {
        let filter = SchemaFilter::new("proto = 'tcp' AND port IN (80, 443)").unwrap();
        let cols = filter.get_columns();
        assert!(cols.contains(&"proto".to_string()));
        assert!(cols.contains(&"port".to_string()));
    }

    #[test]
    fn test_zeek_time_filter() {
        // Common Zeek log filter pattern
        let filter = SchemaFilter::new("ts >= now() - toIntervalDay(7) AND proto = 'tcp'").unwrap();
        let sql = filter.to_sql("conn").unwrap();
        assert!(sql.contains("conn.ts"));
        assert!(sql.contains("conn.proto"));
    }

    #[test]
    fn test_function_comparison() {
        let filter = SchemaFilter::new("length(name) > 0").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "length(t.name) > 0");
    }

    #[test]
    fn test_boolean_column() {
        // Just a column name as filter (for boolean columns)
        let filter = SchemaFilter::new("active").unwrap();
        assert_eq!(filter.to_sql("t").unwrap(), "t.active");
    }

    #[test]
    fn test_complex_nested() {
        let filter = SchemaFilter::new("(a = 1 OR b = 2) AND (c = 3 OR d = 4)").unwrap();
        let sql = filter.to_sql("t").unwrap();
        assert!(sql.contains("t.a = 1"));
        assert!(sql.contains("t.b = 2"));
        assert!(sql.contains("t.c = 3"));
        assert!(sql.contains("t.d = 4"));
    }
}
