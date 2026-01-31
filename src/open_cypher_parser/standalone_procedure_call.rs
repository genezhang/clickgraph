use crate::open_cypher_parser::ast::{Expression, StandaloneProcedureCall};
use crate::open_cypher_parser::common::ws;
use crate::open_cypher_parser::errors::OpenCypherParsingError;
use crate::open_cypher_parser::expression;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric1, char};
use nom::combinator::{opt, recognize};
use nom::multi::{many1, separated_list0};
use nom::sequence::delimited;
use nom::{IResult, Parser};

/// Parse a standalone CALL procedure statement
/// Examples:
/// - CALL db.labels()
/// - CALL db.relationshipTypes()
/// - CALL dbms.components()
/// - CALL db.propertyKeys()
/// - CALL db.labels() YIELD label
pub fn parse_standalone_procedure_call<'a>(
    input: &'a str,
) -> IResult<&'a str, StandaloneProcedureCall<'a>, OpenCypherParsingError<'a>> {
    // Parse CALL keyword
    let (input, _) = ws(tag("CALL")).parse(input)?;

    // Parse procedure name (can include dots, e.g., "db.labels")
    let (input, procedure_name) = parse_procedure_name.parse(input)?;

    // Parse required parentheses with optional arguments
    let (input, arguments) = delimited(
        ws(char('(')),
        separated_list0(ws(char(',')), expression_parser),
        ws(char(')')),
    )
    .parse(input)?;

    // Parse optional YIELD clause
    let (input, yield_items) = opt(parse_yield_clause).parse(input)?;

    Ok((
        input,
        StandaloneProcedureCall {
            procedure_name,
            arguments,
            yield_items,
        },
    ))
}

/// Helper to convert expression parser error type
fn expression_parser(input: &str) -> IResult<&str, Expression<'_>, OpenCypherParsingError<'_>> {
    expression::parse_expression(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Error(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

/// Parse procedure name that can contain alphanumeric characters and dots
/// Examples: "db.labels", "dbms.components", "apoc.meta.graph"
fn parse_procedure_name<'a>(
    input: &'a str,
) -> IResult<&'a str, &'a str, OpenCypherParsingError<'a>> {
    ws(recognize(many1(alt((
        alphanumeric1,
        recognize(char('.')),
        recognize(char('_')),
    )))))
    .parse(input)
}

/// Parse YIELD clause
/// Example: YIELD label, name, age
fn parse_yield_clause<'a>(
    input: &'a str,
) -> IResult<&'a str, Vec<&'a str>, OpenCypherParsingError<'a>> {
    let (input, _) = ws(tag("YIELD")).parse(input)?;
    separated_list0(ws(char(',')), ws(alphanumeric1)).parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_db_labels() {
        let input = "CALL db.labels()";
        let result = parse_standalone_procedure_call(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "db.labels");
        assert!(call.arguments.is_empty());
        assert!(call.yield_items.is_none());
    }

    #[test]
    fn test_parse_db_relationship_types() {
        let input = "CALL db.relationshipTypes()";
        let result = parse_standalone_procedure_call(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "db.relationshipTypes");
        assert!(call.arguments.is_empty());
    }

    #[test]
    fn test_parse_dbms_components() {
        let input = "CALL dbms.components()";
        let result = parse_standalone_procedure_call(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "dbms.components");
        assert!(call.arguments.is_empty());
    }

    #[test]
    fn test_parse_db_property_keys() {
        let input = "CALL db.propertyKeys()";
        let result = parse_standalone_procedure_call(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "db.propertyKeys");
        assert!(call.arguments.is_empty());
    }

    #[test]
    fn test_parse_with_yield() {
        let input = "CALL db.labels() YIELD label";
        let result = parse_standalone_procedure_call(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "db.labels");
        assert!(call.yield_items.is_some());
        let yield_items = call.yield_items.unwrap();
        assert_eq!(yield_items.len(), 1);
        assert_eq!(yield_items[0], "label");
    }

    #[test]
    fn test_parse_with_multiple_yield_items() {
        let input = "CALL dbms.components() YIELD name, versions, edition";
        let result = parse_standalone_procedure_call(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "dbms.components");
        assert!(call.yield_items.is_some());
        let yield_items = call.yield_items.unwrap();
        assert_eq!(yield_items.len(), 3);
        assert_eq!(yield_items[0], "name");
        assert_eq!(yield_items[1], "versions");
        assert_eq!(yield_items[2], "edition");
    }
}
