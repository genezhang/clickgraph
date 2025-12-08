use crate::open_cypher_parser::ast::{CallArgument, CallClause, Expression};
use crate::open_cypher_parser::common::ws;
use crate::open_cypher_parser::errors::OpenCypherParsingError;
use crate::open_cypher_parser::expression;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric1, char};
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::sequence::delimited;
use nom::{IResult, Parser};

pub fn parse_call_clause<'a>(
    input: &'a str,
) -> IResult<&'a str, CallClause<'a>, OpenCypherParsingError<'a>> {
    let (input, _) = ws(tag("CALL")).parse(input)?;

    let (input, procedure_name) = parse_procedure_name.parse(input)?;

    let (input, arguments) = opt(delimited(
        ws(char('(')),
        separated_list0(ws(char(',')), parse_call_argument),
        ws(char(')')),
    ))
    .parse(input)?;

    let arguments = arguments.unwrap_or_default();

    Ok((
        input,
        CallClause {
            procedure_name,
            arguments,
        },
    ))
}

fn parse_procedure_name<'a>(
    input: &'a str,
) -> IResult<&'a str, &'a str, OpenCypherParsingError<'a>> {
    use nom::character::complete::{alphanumeric1, char};
    use nom::combinator::recognize;
    use nom::multi::many1;

    // Parse procedure name that can contain alphanumeric characters and dots
    ws(recognize(many1(nom::branch::alt((
        alphanumeric1,
        nom::combinator::recognize(char('.')),
    )))))
    .parse(input)
}

fn parse_call_argument<'a>(
    input: &'a str,
) -> IResult<&'a str, CallArgument<'a>, OpenCypherParsingError<'a>> {
    let (input, name) = ws(alphanumeric1).parse(input)?;

    // Support both => (GDS style) and : (traditional) syntax
    let (input, _) = ws(alt((tag("=>"), tag(":")))).parse(input)?;

    let (input, value) = expression_parser(input)?;

    Ok((input, CallArgument { name, value }))
}

fn expression_parser(input: &str) -> IResult<&str, Expression<'_>, OpenCypherParsingError<'_>> {
    expression::parse_expression(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::open_cypher_parser::ast::{Expression, Literal};

    #[test]
    fn test_parse_call_clause_simple() {
        let input = "CALL pagerank";
        let result = parse_call_clause(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "pagerank");
        assert!(call.arguments.is_empty());
    }

    #[test]
    fn test_parse_call_clause_with_args() {
        let input = "CALL pagerank(iterations: 10, damping: 0.85)";
        let result = parse_call_clause(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "pagerank");
        assert_eq!(call.arguments.len(), 2);
        assert_eq!(call.arguments[0].name, "iterations");
        assert_eq!(call.arguments[1].name, "damping");
    }

    #[test]
    fn test_parse_call_clause_with_arrow_syntax() {
        let input = "CALL pagerank.graph(nodeLabels => 'User', maxIterations => 5)";
        let result = parse_call_clause(input);
        assert!(result.is_ok());
        let (_, call) = result.unwrap();
        assert_eq!(call.procedure_name, "pagerank.graph");
        assert_eq!(call.arguments.len(), 2);
        assert_eq!(call.arguments[0].name, "nodeLabels");
        assert_eq!(call.arguments[1].name, "maxIterations");
    }
}
