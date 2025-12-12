use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, take_while1},
    character::complete::{alphanumeric1, multispace0},
    combinator::{map, not, opt, peek},
    error::{Error, ErrorKind},
    multi::separated_list0,
    sequence::{delimited, preceded, separated_pair, terminated},
};

use nom::character::complete::char;

use crate::open_cypher_parser::common::{self, ws};

use super::{
    ast::{Expression, ExistsSubquery, FunctionCall, Literal, Operator, OperatorApplication, PropertyAccess, ReduceExpression},
    path_pattern,
    where_clause,
};

pub fn parse_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, expression) = parse_logical_or.parse(input)?;
    Ok((input, expression))
}

pub fn parse_path_pattern_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, path_pattern) = path_pattern::parse_path_pattern(input)?;
    Ok((input, Expression::PathPattern(path_pattern)))
}

// parse_postfix_expression
fn parse_postfix_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // First, parse a basic primary expression: literal, variable, or parenthesized expression.
    let (input, expr) = alt((
        parse_parameter,
        parse_property_access,
        parse_literal_or_variable_expression,
        delimited(ws(char('(')), parse_expression, ws(char(')'))),
    ))
    .parse(input)?;

    // Then, optionally, parse the postfix operator "IS NULL" or "IS NOT NULL".
    let (input, opt_op) = nom::combinator::opt(preceded(
        ws(tag_no_case("IS")),
        alt((
            map(
                preceded(ws(tag_no_case("NOT")), ws(tag_no_case("NULL"))),
                |_| Operator::IsNotNull,
            ),
            map(ws(tag_no_case("NULL")), |_| Operator::IsNull),
        )),
    ))
    .parse(input)?;

    if let Some(op) = opt_op {
        Ok((
            input,
            Expression::OperatorApplicationExp(OperatorApplication {
                operator: op,
                operands: vec![expr],
            }),
        ))
    } else {
        Ok((input, expr))
    }
}

/// Parse EXISTS subquery expression
/// Syntax: EXISTS { pattern } or EXISTS { MATCH pattern WHERE condition }
/// Examples:
///   EXISTS { (u)-[:FOLLOWS]->(:User) }
///   EXISTS { MATCH (u)-[:FOLLOWS]->(f) WHERE f.active = true }
fn parse_exists_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse EXISTS keyword
    let (input, _) = ws(tag_no_case("EXISTS")).parse(input)?;
    
    // Parse opening brace
    let (input, _) = ws(char('{')).parse(input)?;
    
    // Optionally skip MATCH keyword if present
    let (input, _) = opt(ws(tag_no_case("MATCH"))).parse(input)?;
    
    // Parse the pattern
    let (input, pattern) = ws(path_pattern::parse_path_pattern).parse(input)?;
    
    // Parse optional WHERE clause - convert the error type
    let (input, where_clause) = match opt(where_clause::parse_where_clause).parse(input) {
        Ok((rest, wc)) => (rest, wc),
        Err(nom::Err::Error(_)) | Err(nom::Err::Failure(_)) => (input, None),
        Err(nom::Err::Incomplete(n)) => return Err(nom::Err::Incomplete(n)),
    };
    
    // Parse closing brace
    let (input, _) = ws(char('}')).parse(input)?;
    
    Ok((
        input,
        Expression::ExistsExpression(Box::new(ExistsSubquery {
            pattern,
            where_clause: where_clause.map(Box::new),
        })),
    ))
}

fn parse_case_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, _) = ws(tag_no_case("CASE")).parse(input)?;

    // Check if this is a searched CASE (starts with WHEN) or simple CASE (has an expression)
    // Try to peek ahead to see if the next non-whitespace token is "WHEN"
    let (input_after_ws, _) = multispace0.parse(input)?;
    let is_searched = input_after_ws.starts_with("WHEN") || input_after_ws.starts_with("when");

    let (input, case_expr) = if is_searched {
        // Searched CASE - no case_expr
        (input, None)
    } else {
        // Try to parse simple CASE expression
        match opt(parse_expression).parse(input) {
            Ok((input, expr)) => (input, expr),
            Err(_) => (input, None), // If parsing fails, assume searched CASE
        }
    };

    // Parse WHEN/THEN pairs
    let mut when_then = Vec::new();
    let mut remaining_input = input;

    loop {
        let res = preceded(
            ws(tag_no_case("WHEN")),
            separated_pair(parse_expression, ws(tag_no_case("THEN")), parse_expression),
        )
        .parse(remaining_input);

        match res {
            Ok((new_input, (when_expr, then_expr))) => {
                when_then.push((when_expr, then_expr));
                remaining_input = new_input;
            }
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }

    // Optional ELSE clause
    let (input, else_expr) =
        opt(preceded(ws(tag_no_case("ELSE")), parse_expression)).parse(remaining_input)?;

    // END keyword
    let (input, _) = ws(tag_no_case("END")).parse(input)?;

    Ok((
        input,
        Expression::Case(crate::open_cypher_parser::ast::Case {
            expr: case_expr.map(Box::new),
            when_then,
            else_expr: else_expr.map(Box::new),
        }),
    ))
}

/// Parse reduce expression
/// Syntax: reduce(accumulator = initial, variable IN list | expression)
/// Examples:
///   reduce(total = 0, x IN [1, 2, 3] | total + x)
///   reduce(s = '', name IN names | s + name)
fn parse_reduce_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse "reduce" keyword (case-insensitive)
    let (input, _) = ws(tag_no_case("reduce")).parse(input)?;
    
    // Parse opening parenthesis
    let (input, _) = ws(char('(')).parse(input)?;
    
    // Parse accumulator = initial_value
    let (input, accumulator) = ws(parse_identifier).parse(input)?;
    let (input, _) = ws(char('=')).parse(input)?;
    let (input, initial_value) = ws(parse_expression).parse(input)?;
    
    // Parse comma separator
    let (input, _) = ws(char(',')).parse(input)?;
    
    // Parse variable IN list
    let (input, variable) = ws(parse_identifier).parse(input)?;
    let (input, _) = ws(tag_no_case("IN")).parse(input)?;
    
    // Parse the list expression - need to be careful to stop at '|'
    // We can't just use parse_expression because it would consume the '|'
    let (input, list) = parse_reduce_list_expression(input)?;
    
    // Parse '|' separator
    let (input, _) = ws(char('|')).parse(input)?;
    
    // Parse the expression (the body of the reduce)
    let (input, expression) = ws(parse_reduce_body_expression).parse(input)?;
    
    // Parse closing parenthesis
    let (input, _) = ws(char(')')).parse(input)?;
    
    Ok((
        input,
        Expression::ReduceExp(ReduceExpression {
            accumulator,
            initial_value: Box::new(initial_value),
            variable,
            list: Box::new(list),
            expression: Box::new(expression),
        }),
    ))
}

/// Parse the list expression in reduce, stopping at '|'
fn parse_reduce_list_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse a simple expression that doesn't cross the '|' boundary
    // This handles: variable, list literal, function call, property access
    let (input, _) = multispace0.parse(input)?;
    
    // Try to parse a list literal first
    if input.starts_with('[') {
        return parse_list_literal(input);
    }
    
    // Try function call (e.g., nodes(path))
    let func_result = parse_function_call(input);
    if func_result.is_ok() {
        return func_result;
    }
    
    // Try property access (e.g., u.friends)
    let prop_result = parse_property_access(input);
    if prop_result.is_ok() {
        return prop_result;
    }
    
    // Fall back to simple variable
    let (input, var) = parse_identifier(input)?;
    Ok((input, Expression::Variable(var)))
}

/// Parse the body expression in reduce, stopping at ')'
fn parse_reduce_body_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse expression but be careful with parentheses
    // We need to track depth to handle nested expressions
    let mut depth = 0;
    let mut end_pos = 0;
    let chars: Vec<char> = input.chars().collect();
    
    for (i, &c) in chars.iter().enumerate() {
        match c {
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    end_pos = i;
                    break;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    
    if end_pos == 0 && depth == 0 {
        // No closing paren found at depth 0, use whole remaining input
        end_pos = input.len();
    }
    
    let expr_str = &input[..end_pos];
    let remaining = &input[end_pos..];
    
    // Now parse the expression substring
    let (leftover, expr) = parse_expression(expr_str.trim())?;
    
    // Make sure we consumed the whole expression
    if !leftover.trim().is_empty() {
        return Err(nom::Err::Error(Error::new(input, ErrorKind::TakeWhile1)));
    }
    
    Ok((remaining, expr))
}

fn parse_primary(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        parse_exists_expression,  // Must be before parse_function_call to catch EXISTS { }
        parse_case_expression,
        parse_reduce_expression,  // Must be before parse_function_call to catch reduce(...)
        parse_path_pattern_expression,
        parse_function_call,
        parse_postfix_expression,
        parse_property_access,
        parse_map_literal,        // Must be before list_literal (different brackets anyway)
        parse_list_literal,
        parse_parameter,
        parse_literal_or_variable_expression,
        delimited(ws(char('(')), parse_expression, ws(char(')'))),
    ))
    .parse(input)
}

pub fn parse_operator_symbols(input: &str) -> IResult<&str, Operator> {
    alt((
        map(tag_no_case(">="), |_| Operator::GreaterThanEqual),
        map(tag_no_case("<="), |_| Operator::LessThanEqual),
        map(tag_no_case("<>"), |_| Operator::NotEqual),
        map(tag_no_case("=~"), |_| Operator::RegexMatch),  // Must be before "=" to match first
        map(tag_no_case(">"), |_| Operator::GreaterThan),
        map(tag_no_case("<"), |_| Operator::LessThan),
        map(tag_no_case("="), |_| Operator::Equal),
        map(tag_no_case("+"), |_| Operator::Addition),
        map(tag_no_case("-"), |_| Operator::Subtraction),
        map(tag_no_case("*"), |_| Operator::Multiplication),
        map(tag_no_case("/"), |_| Operator::Division),
        map(tag_no_case("%"), |_| Operator::ModuloDivision),
        map(tag_no_case("^"), |_| Operator::Exponentiation),
        // String predicates - must be before IN to avoid partial match
        map(preceded(ws(tag_no_case("STARTS")), ws(tag_no_case("WITH"))), |_| Operator::StartsWith),
        map(preceded(ws(tag_no_case("ENDS")), ws(tag_no_case("WITH"))), |_| Operator::EndsWith),
        map(tag_no_case("CONTAINS"), |_| Operator::Contains),
        map(tag_no_case("NOT IN"), |_| Operator::NotIn),
        map(tag_no_case("IN"), |_| Operator::In),
    ))
    .parse(input)
}

fn parse_unary_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        map(
            preceded(ws(tag_no_case("NOT")), parse_unary_expression),
            |expr| {
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Not,
                    operands: vec![expr],
                })
            },
        ),
        map(
            preceded(ws(tag_no_case("DISTINCT")), parse_unary_expression),
            |expr| {
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Distinct,
                    operands: vec![expr],
                })
            },
        ),
        parse_primary, // fallback to a primary expression
    ))
    .parse(input)
}

fn parse_binary_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // parse the left-hand side.
    let (input, lhs) = parse_unary_expression(input)?;

    let mut remaining_input = input;
    let mut final_expression = lhs;

    loop {
        // Try to parse an operator and a right-hand side.
        let res = (ws(parse_operator_symbols), parse_unary_expression).parse(remaining_input);
        match res {
            Ok((new_input, (op, rhs))) => {
                // Build a new expression by moving the previous result into the operator application.
                final_expression = Expression::OperatorApplicationExp(OperatorApplication {
                    operator: op,
                    operands: vec![final_expression, rhs],
                });
                remaining_input = new_input;
            }
            // If no more operator/unary expression pair is found, break out.
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }
    Ok((remaining_input, final_expression))
}

fn parse_logical_and(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, lhs) = parse_binary_expression(input)?;

    let mut remaining_input = input;
    let mut final_expression = lhs;

    loop {
        // Try to parse an "AND" followed by a binary expression.
        let res = preceded(ws(tag_no_case("AND")), parse_binary_expression).parse(remaining_input);
        match res {
            Ok((new_input, rhs)) => {
                // Build a new expression by moving `expr` and `rhs` into a new operator application.
                final_expression = Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    // final_expression here is lhs
                    operands: vec![final_expression, rhs],
                });
                remaining_input = new_input;
            }
            // If we don't match "AND", exit the loop.
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }
    Ok((remaining_input, final_expression))
}

// fn parse_logical_or(input: &str) -> IResult<&str, Expression> {
//     let (input, left) = parse_logical_and(input)?;
//     fold_many0(
//         // parse only "OR" and not "ORDER"
//         preceded(ws(terminated(tag_no_case("OR"), not(peek(alphanumeric1)))), parse_logical_and),
//         move || left.clone(),
//         |acc, rhs| Expression::OperatorApplicationExp(OperatorApplication {
//             operator: Operator::Or,
//             operands: vec![acc, rhs],
//         }),
//     ).parse(input)
// }

fn parse_logical_or(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, lhs) = parse_logical_and(input)?;

    let mut remaining_input = input;
    let mut final_expression = lhs;

    loop {
        let res = preceded(
            // parse only "OR" and not "ORDER"
            ws(terminated(tag_no_case("OR"), not(peek(alphanumeric1)))),
            parse_logical_and,
        )
        .parse(remaining_input);

        match res {
            Ok((new_input, rhs)) => {
                final_expression = Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Or,
                    // final_expression here is lhs
                    operands: vec![final_expression, rhs],
                });
                remaining_input = new_input;
            }
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }

    Ok((remaining_input, final_expression))
}

fn is_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

// Parse an identifier and return it as a String.
pub fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(is_identifier_char).parse(input)
}

pub fn parse_function_call(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // First, parse the function name.
    let (input, name) = ws(parse_identifier).parse(input)?;
    // Then parse the comma-separated arguments within parentheses.
    let (input, args) = delimited(
        ws(char('(')),
        separated_list0(ws(char(',')), parse_expression),
        ws(char(')')),
    )
    .parse(input)?;

    Ok((
        input,
        Expression::FunctionCallExp(FunctionCall {
            name: name.to_string(),
            args,
        }),
    ))
}

/// Parse a map literal: {key1: value1, key2: value2}
/// Used in duration({days: 5}), point({x: 1, y: 2}), etc.
pub fn parse_map_literal(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse content within { ... } as comma-separated key:value pairs
    let (input, entries) = delimited(
        // Opening brace with optional whitespace
        delimited(multispace0, char('{'), multispace0),
        // Zero or more key:value pairs separated by commas
        separated_list0(
            delimited(multispace0, char(','), multispace0),
            // Each entry is: key : value (using native tuple parser)
            (
                parse_identifier,  // key (identifier)
                delimited(multispace0, char(':'), multispace0),  // colon
                parse_expression,  // value (any expression)
            ),
        ),
        // Closing brace with optional whitespace
        delimited(multispace0, char('}'), multispace0),
    )
    .parse(input)?;

    // Transform (key, _, value) tuples into (key, value) pairs
    let pairs: Vec<(&str, Expression)> = entries
        .into_iter()
        .map(|(key, _, value)| (key, value))
        .collect();

    Ok((input, Expression::MapLiteral(pairs)))
}

pub fn parse_list_literal(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse content within [ ... ] as a comma-separated list of expressions.
    let (input, exprs) = delimited(
        // Opening bracket with optional whitespace afterwards
        delimited(multispace0, char('['), multispace0),
        // Zero or more expressions separated by commas (with optional whitespace)
        separated_list0(
            delimited(multispace0, char(','), multispace0),
            parse_expression,
        ),
        // Closing bracket with optional whitespace preceding it
        delimited(multispace0, char(']'), multispace0),
    )
    .parse(input)?;

    Ok((input, Expression::List(exprs)))
}

/// Parse a property name which can be either an identifier or a wildcard (*)
fn parse_property_name(input: &str) -> IResult<&str, &str> {
    nom::branch::alt((
        nom::bytes::complete::tag("*"),
        common::parse_alphanumeric_with_underscore,
    ))
    .parse(input)
}

pub fn parse_property_access(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // First part: the base (e.g., "src")
    let (input, base_str) = common::parse_alphanumeric_with_underscore(input)?;
    
    // Then: a dot
    let (input, _) = char('.')(input)?;
    
    // Then: the property name (can be identifier or *)
    let (input, key_str) = parse_property_name(input)?;
    
    let base = match parse_literal_or_variable_expression(base_str) {
        Ok((_, Expression::Variable(base))) => base,
        _ => return Err(nom::Err::Error(Error::new(input, ErrorKind::Float))),
    };

    // For wildcard (*), key is just "*"
    let key = if key_str == "*" {
        "*"
    } else {
        match parse_literal_or_variable_expression(key_str) {
            Ok((_, Expression::Variable(key))) => key,
            _ => return Err(nom::Err::Error(Error::new(input, ErrorKind::Float))),
        }
    };

    let property_access = Expression::PropertyAccessExp(PropertyAccess { base, key });

    Ok((input, property_access))
}

/// Helper function to determine if a character is valid in a parameter name.
fn is_param_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

pub fn parse_parameter(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // println!("Input in parse_parameter {:?}", input);

    let (input, param) = preceded(tag("$"), take_while1(is_param_char)).parse(input)?;

    Ok((input, Expression::Parameter(param)))
}

/// Reserved for future use when order-specific expression parsing is needed
#[allow(dead_code)]
pub fn parse_parameter_property_access_literal_variable_expression(
    input: &'_ str,
) -> IResult<&'_ str, Expression<'_>> {
    // println!("Input in parse_literal_variable_parameter_expression {:?}", input);

    let (input, expression) = alt((
        parse_parameter,
        parse_property_access,
        parse_literal_or_variable_expression,
    ))
    .parse(input)?;
    Ok((input, expression))
}

/// Check if a string is a reserved Cypher keyword that cannot be used as a variable
/// at the start of an expression. This catches cases like "WHERE AND ..." where AND
/// is incorrectly treated as a variable name.
/// 
/// We only block binary operators that require a left operand:
/// - Logical: AND, OR, XOR
/// - Note: NOT is a unary prefix operator, so it IS valid at expression start
fn is_binary_operator_keyword(s: &str) -> bool {
    let upper = s.to_uppercase();
    matches!(upper.as_str(), "AND" | "OR" | "XOR")
}

/// Parse a label expression: variable:Label
/// This checks if a variable has a specific label
/// Example: message:Comment, n:Person
fn parse_label_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse variable name (identifier)
    let (input, variable) = ws(parse_identifier).parse(input)?;
    // Parse colon
    let (input, _) = char(':').parse(input)?;
    // Parse label name (identifier)
    let (input, label) = parse_identifier(input)?;
    
    Ok((input, Expression::LabelExpression { variable, label }))
}

pub fn parse_literal_or_variable_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        map(ws(parse_string_literal), Expression::Literal),
        map(ws(parse_double_quoted_string_literal), Expression::Literal),
        // Try label expression first (variable:Label)
        parse_label_expression,
        // Parse alphanumeric values but reject binary operators as standalone expressions
        |input| {
            let (remaining, s) = ws(common::parse_alphanumeric_with_underscore_dot_star).parse(input)?;
            
            if s.eq_ignore_ascii_case("null") {
                Ok((remaining, Expression::Literal(Literal::Null)))
            } else if s.eq_ignore_ascii_case("true") {
                Ok((remaining, Expression::Literal(Literal::Boolean(true))))
            } else if s.eq_ignore_ascii_case("false") {
                Ok((remaining, Expression::Literal(Literal::Boolean(false))))
            } else if let Ok(i) = s.parse::<i64>() {
                Ok((remaining, Expression::Literal(Literal::Integer(i))))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok((remaining, Expression::Literal(Literal::Float(f))))
            } else if is_binary_operator_keyword(s) {
                // Reject binary operators as standalone expressions
                // This catches "WHERE AND ..." patterns
                Err(nom::Err::Error(Error::new(input, ErrorKind::Tag)))
            } else {
                // string literal is covered already in parse_string_literal fn. Any other string is variable now.
                Ok((remaining, Expression::Variable(s)))
            }
        },
    ))
    .parse(input)
}

pub fn parse_string_literal(input: &'_ str) -> IResult<&'_ str, Literal<'_>> {
    let (input, s) = delimited(char('\''), take_until("\'"), char('\'')).parse(input)?;

    Ok((input, Literal::String(s)))
}

pub fn parse_double_quoted_string_literal(input: &'_ str) -> IResult<&'_ str, Literal<'_>> {
    let (input, s) = delimited(char('"'), take_until("\""), char('"')).parse(input)?;

    Ok((input, Literal::String(s)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_operator_symbols() {
        let (rem, op) = parse_operator_symbols(">=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::GreaterThanEqual);

        let (rem, op) = parse_operator_symbols("<=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::LessThanEqual);

        let (rem, op) = parse_operator_symbols("<>").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::NotEqual);

        let (rem, op) = parse_operator_symbols(">").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::GreaterThan);

        let (rem, op) = parse_operator_symbols("<").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::LessThan);

        let (rem, op) = parse_operator_symbols("=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Equal);

        let (rem, op) = parse_operator_symbols("+").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Addition);

        let (rem, op) = parse_operator_symbols("-").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Subtraction);

        let (rem, op) = parse_operator_symbols("*").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Multiplication);

        let (rem, op) = parse_operator_symbols("/").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Division);

        let (rem, op) = parse_operator_symbols("%").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::ModuloDivision);

        let (rem, op) = parse_operator_symbols("^").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Exponentiation);

        let (rem, op) = parse_operator_symbols("IN").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::In);
    }

    // postfix
    #[test]
    fn test_parse_postfix_expression_is_null() {
        let (rem, expr) = parse_postfix_expression("a IS NULL").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNull,
            operands: vec![Expression::Variable("a")],
        });
        assert_eq!(&expr, &expected);
    }

    #[test]
    fn test_parse_postfix_expression_is_not_null() {
        let (rem, expr) = parse_postfix_expression("a IS NOT NULL").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNotNull,
            operands: vec![Expression::Variable("a")],
        });
        assert_eq!(&expr, &expected);
    }

    // unary
    #[test]
    fn test_parse_unary_expression_not() {
        let (rem, expr) = parse_unary_expression("NOT a").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Not,
            operands: vec![Expression::Variable("a")],
        });
        assert_eq!(&expr, &expected);
    }

    // binary
    #[test]
    fn test_parse_binary_expression_addition() {
        let (rem, expr) = parse_binary_expression("a + b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    // and
    #[test]
    fn test_parse_logical_and() {
        let (rem, expr) = parse_logical_and("a AND b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    // or
    #[test]
    fn test_parse_logical_or() {
        let (rem, expr) = parse_logical_or("a OR b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Or,
            operands: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    // fn call
    #[test]
    fn test_parse_function_call() {
        // Testing a function call: foo(a, b)
        let (rem, expr) = parse_function_call("foo(a, b)").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::FunctionCallExp(FunctionCall {
            name: "foo".to_string(),
            args: vec![Expression::Variable("a"), Expression::Variable("b")],
        });
        assert_eq!(&expr, &expected);
    }

    #[test]
    fn test_parse_function_cal_count() {
        // Testing a function call: foo(a, b)
        let (rem, expr) = parse_function_call("count(*)").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::FunctionCallExp(FunctionCall {
            name: "count".to_string(),
            args: vec![Expression::Variable("*")],
        });
        assert_eq!(&expr, &expected);
    }

    // list
    #[test]
    fn test_parse_list_literal() {
        let (rem, expr) = parse_list_literal("[a, b]").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::List(vec![Expression::Variable("a"), Expression::Variable("b")]);
        assert_eq!(&expr, &expected);
    }

    //  property access
    #[test]
    fn test_parse_property_access() {
        let (rem, expr) = parse_property_access("user.name").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::PropertyAccessExp(PropertyAccess {
            base: "user",
            key: "name",
        });
        assert_eq!(&expr, &expected);
    }

    // fn_call + operator
    #[test]
    fn test_parse_expression_fn_call_expression() {
        // Example: foo(a, b) + c
        let (rem, expr) = parse_expression("foo(a, b) + c").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                Expression::FunctionCallExp(FunctionCall {
                    name: "foo".to_string(),
                    args: vec![Expression::Variable("a"), Expression::Variable("b")],
                }),
                Expression::Variable("c"),
            ],
        });
        assert_eq!(&expr, &expected);
    }

    // reduce expression
    #[test]
    fn test_parse_reduce_expression_simple() {
        let (rem, expr) = parse_expression("reduce(total = 0, x IN [1, 2, 3] | total + x)").unwrap();
        assert_eq!(rem, "");
        
        if let Expression::ReduceExp(reduce) = expr {
            assert_eq!(reduce.accumulator, "total");
            assert_eq!(reduce.variable, "x");
            // Check initial value is 0
            if let Expression::Literal(Literal::Integer(n)) = *reduce.initial_value {
                assert_eq!(n, 0);
            } else {
                panic!("Expected integer literal for initial value");
            }
            // Check list has 3 elements
            if let Expression::List(items) = *reduce.list {
                assert_eq!(items.len(), 3);
            } else {
                panic!("Expected list for list expression");
            }
            // Check expression is addition
            if let Expression::OperatorApplicationExp(op) = *reduce.expression {
                assert_eq!(op.operator, Operator::Addition);
            } else {
                panic!("Expected operator application for expression");
            }
        } else {
            panic!("Expected ReduceExp variant");
        }
    }

    #[test]
    fn test_parse_reduce_expression_with_variable_list() {
        let (rem, expr) = parse_expression("reduce(s = '', name IN names | s + name)").unwrap();
        assert_eq!(rem, "");
        
        if let Expression::ReduceExp(reduce) = expr {
            assert_eq!(reduce.accumulator, "s");
            assert_eq!(reduce.variable, "name");
            // Check list is a variable reference
            if let Expression::Variable(var) = *reduce.list {
                assert_eq!(var, "names");
            } else {
                panic!("Expected variable for list expression");
            }
        } else {
            panic!("Expected ReduceExp variant");
        }
    }

    #[test]
    fn test_parse_map_literal_single_entry() {
        let (rem, expr) = parse_map_literal("{days: 5}").unwrap();
        assert_eq!(rem, "");
        
        if let Expression::MapLiteral(entries) = expr {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "days");
            if let Expression::Literal(Literal::Integer(n)) = entries[0].1 {
                assert_eq!(n, 5);
            } else {
                panic!("Expected integer literal for value");
            }
        } else {
            panic!("Expected MapLiteral variant");
        }
    }

    #[test]
    fn test_parse_map_literal_multiple_entries() {
        let (rem, expr) = parse_map_literal("{days: 5, hours: 2}").unwrap();
        assert_eq!(rem, "");
        
        if let Expression::MapLiteral(entries) = expr {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].0, "days");
            assert_eq!(entries[1].0, "hours");
        } else {
            panic!("Expected MapLiteral variant");
        }
    }

    #[test]
    fn test_parse_map_literal_with_expression_value() {
        let (rem, expr) = parse_map_literal("{offset: x + 1}").unwrap();
        assert_eq!(rem, "");
        
        if let Expression::MapLiteral(entries) = expr {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "offset");
            // Value should be an operator application (x + 1)
            if let Expression::OperatorApplicationExp(op) = &entries[0].1 {
                assert_eq!(op.operator, Operator::Addition);
            } else {
                panic!("Expected operator application for value");
            }
        } else {
            panic!("Expected MapLiteral variant");
        }
    }

    #[test]
    fn test_parse_duration_with_map_arg() {
        let (rem, expr) = parse_expression("duration({days: 5})").unwrap();
        assert_eq!(rem, "");
        
        if let Expression::FunctionCallExp(fc) = expr {
            assert_eq!(fc.name, "duration");
            assert_eq!(fc.args.len(), 1);
            if let Expression::MapLiteral(entries) = &fc.args[0] {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].0, "days");
            } else {
                panic!("Expected MapLiteral argument");
            }
        } else {
            panic!("Expected FunctionCallExp variant");
        }
    }

    #[test]
    fn test_parse_label_expression() {
        // Test basic label expression
        let (rem, expr) = parse_label_expression("u:User").unwrap();
        assert_eq!(rem, "");
        if let Expression::LabelExpression { variable, label } = expr {
            assert_eq!(variable, "u");
            assert_eq!(label, "User");
        } else {
            panic!("Expected LabelExpression, got {:?}", expr);
        }

        // Test with different casing
        let (rem, expr) = parse_label_expression("message:Comment").unwrap();
        assert_eq!(rem, "");
        if let Expression::LabelExpression { variable, label } = expr {
            assert_eq!(variable, "message");
            assert_eq!(label, "Comment");
        } else {
            panic!("Expected LabelExpression, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_label_expression_in_full_expression() {
        // Test label expression through parse_expression
        let (rem, expr) = parse_expression("u:User").unwrap();
        assert_eq!(rem, "");
        if let Expression::LabelExpression { variable, label } = expr {
            assert_eq!(variable, "u");
            assert_eq!(label, "User");
        } else {
            panic!("Expected LabelExpression, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_size_with_pattern() {
        // Test size() with a simple path pattern: size((n)-[:KNOWS]->())
        let (rem, expr) = parse_expression("size((n)-[:KNOWS]->())").unwrap();
        assert_eq!(rem, "");
        if let Expression::FunctionCallExp(fc) = expr {
            assert_eq!(fc.name, "size");
            assert_eq!(fc.args.len(), 1);
            if let Expression::PathPattern(_) = &fc.args[0] {
                // Good - the argument is a path pattern
            } else {
                panic!("Expected PathPattern argument, got {:?}", fc.args[0]);
            }
        } else {
            panic!("Expected FunctionCallExp, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_size_with_bidirectional_pattern() {
        // Test size() with anonymous bidirectional pattern: size((p)-[:KNOWS]-())
        let (rem, expr) = parse_expression("size((p)-[:KNOWS]-())").unwrap();
        assert_eq!(rem, "");
        if let Expression::FunctionCallExp(fc) = expr {
            assert_eq!(fc.name, "size");
            assert_eq!(fc.args.len(), 1);
            if let Expression::PathPattern(_) = &fc.args[0] {
                // Good - the argument is a path pattern
            } else {
                panic!("Expected PathPattern argument, got {:?}", fc.args[0]);
            }
        } else {
            panic!("Expected FunctionCallExp, got {:?}", expr);
        }
    }
}
