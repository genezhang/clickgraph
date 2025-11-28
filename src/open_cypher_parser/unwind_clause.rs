use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    combinator::cut,
    error::context,
};

use super::{
    ast::{Expression, UnwindClause},
    common::ws,
    errors::OpenCypherParsingError,
    expression::{parse_expression, parse_identifier},
};

/// Wrapper to convert parse_expression errors to OpenCypherParsingError
fn expression_parser(input: &str) -> IResult<&str, Expression<'_>, OpenCypherParsingError<'_>> {
    parse_expression(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

/// Wrapper to convert parse_identifier errors to OpenCypherParsingError
fn identifier_parser(input: &str) -> IResult<&str, &str, OpenCypherParsingError<'_>> {
    parse_identifier(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

/// Parse an UNWIND clause: UNWIND <expression> AS <alias>
/// 
/// Examples:
/// - UNWIND [1, 2, 3] AS x
/// - UNWIND r.items AS item
/// - UNWIND range(1, 10) AS num
pub fn parse_unwind_clause(
    input: &'_ str,
) -> IResult<&'_ str, UnwindClause<'_>, OpenCypherParsingError<'_>> {
    // Parse the UNWIND keyword
    let (input, _) = ws(tag_no_case("UNWIND")).parse(input)?;

    // Parse the expression to unwind (e.g., [1,2,3] or r.items)
    let (input, expression) = context(
        "Error parsing UNWIND expression",
        cut(ws(expression_parser)),
    )
    .parse(input)?;

    // Parse AS keyword
    let (input, _) = context(
        "Expected AS keyword after UNWIND expression",
        cut(ws(tag_no_case("AS"))),
    )
    .parse(input)?;

    // Parse the alias identifier
    let (input, alias) = context(
        "Expected alias after AS in UNWIND clause",
        cut(ws(identifier_parser)),
    )
    .parse(input)?;

    let unwind_clause = UnwindClause { expression, alias };

    Ok((input, unwind_clause))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast::Expression;

    #[test]
    fn test_parse_unwind_literal_list() {
        let input = "UNWIND [1, 2, 3] AS x";
        let result = parse_unwind_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert!(remaining.trim().is_empty());
        assert_eq!(clause.alias, "x");
        // The expression should be a list
        match clause.expression {
            Expression::List(_) => (),
            _ => panic!("Expected list expression, got {:?}", clause.expression),
        }
    }

    #[test]
    fn test_parse_unwind_property_access() {
        let input = "UNWIND r.items AS item";
        let result = parse_unwind_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert!(remaining.trim().is_empty());
        assert_eq!(clause.alias, "item");
        // The expression should be a property access
        match clause.expression {
            Expression::PropertyAccessExp(_) => (),
            _ => panic!("Expected property access expression, got {:?}", clause.expression),
        }
    }

    #[test]
    fn test_parse_unwind_with_function() {
        let input = "UNWIND range(1, 10) AS num";
        let result = parse_unwind_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert!(remaining.trim().is_empty());
        assert_eq!(clause.alias, "num");
    }

    #[test]
    fn test_parse_unwind_case_insensitive() {
        let input = "unwind [1] as x";
        let result = parse_unwind_clause(input);
        assert!(result.is_ok());
        let (_, clause) = result.unwrap();
        assert_eq!(clause.alias, "x");
    }

    #[test]
    fn test_parse_unwind_missing_as() {
        let input = "UNWIND [1, 2, 3] x";
        let result = parse_unwind_clause(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unwind_missing_alias() {
        let input = "UNWIND [1, 2, 3] AS";
        let result = parse_unwind_clause(input);
        assert!(result.is_err());
    }
}
