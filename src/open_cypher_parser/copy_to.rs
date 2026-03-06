//! Parser for `COPY (<cypher>) TO '<destination>' [FORMAT <fmt>] [(options)]` statements.
//!
//! Compatible with Kuzu/DuckDB COPY TO syntax.

use crate::open_cypher_parser::ast::{CopyToStatement, Expression, Literal};
use crate::open_cypher_parser::common::ws;
use crate::open_cypher_parser::errors::OpenCypherParsingError;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::character::complete::{alphanumeric1, char, multispace0};
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::sequence::delimited;
use nom::{IResult, Parser};

/// Parse a COPY TO statement.
///
/// Syntax:
/// ```text
/// COPY (<cypher-query>) TO '<destination>' [FORMAT <fmt>] [(options)];
/// ```
pub fn parse_copy_to<'a>(
    input: &'a str,
) -> IResult<&'a str, CopyToStatement<'a>, OpenCypherParsingError<'a>> {
    // COPY keyword
    let (input, _) = ws(tag_no_case("COPY")).parse(input)?;

    // ( <cypher-query> ) — extract the inner query as a raw string slice
    let (input, _) = ws(char('(')).parse(input)?;
    let query_start = input;
    let (input, _) = take_balanced_parens(input)?;
    let query_end = input;
    // The query is everything between the outer parens
    let query_len = query_start.len() - query_end.len() - 1; // -1 for closing paren
    let query = query_start[..query_len].trim();
    let (input, _) = multispace0.parse(input)?;

    // TO keyword
    let (input, _) = ws(tag_no_case("TO")).parse(input)?;

    // '<destination>' — single-quoted string
    let (input, destination) =
        ws(delimited(char('\''), take_until("'"), char('\''))).parse(input)?;

    // Optional FORMAT keyword
    let (input, format) = opt(parse_format_clause).parse(input)?;

    // Optional (key value, ...) options
    let (input, options) = opt(parse_options_clause).parse(input)?;
    let options = options.unwrap_or_default();

    // Optional trailing semicolon
    let (input, _) = opt(ws(tag(";"))).parse(input)?;

    Ok((
        input,
        CopyToStatement {
            query,
            destination,
            format,
            options,
        },
    ))
}

/// Parse `FORMAT <identifier>` clause.
fn parse_format_clause<'a>(
    input: &'a str,
) -> IResult<&'a str, &'a str, OpenCypherParsingError<'a>> {
    let (input, _) = ws(tag_no_case("FORMAT")).parse(input)?;
    ws(alphanumeric1).parse(input)
}

/// Parse `(key value, key value, ...)` options.
fn parse_options_clause<'a>(
    input: &'a str,
) -> IResult<&'a str, Vec<(&'a str, Expression<'a>)>, OpenCypherParsingError<'a>> {
    delimited(
        ws(char('(')),
        separated_list0(ws(char(',')), parse_option_pair),
        ws(char(')')),
    )
    .parse(input)
}

/// Parse a single `key value` option pair.
fn parse_option_pair<'a>(
    input: &'a str,
) -> IResult<&'a str, (&'a str, Expression<'a>), OpenCypherParsingError<'a>> {
    let (input, key) = ws(alphanumeric1).parse(input)?;
    let (input, value) = parse_option_value(input)?;
    Ok((input, (key, value)))
}

/// Parse an option value — string literal, boolean, or number.
fn parse_option_value<'a>(
    input: &'a str,
) -> IResult<&'a str, Expression<'a>, OpenCypherParsingError<'a>> {
    // Try single-quoted string
    if let Ok((rest, s)) = ws(delimited(char('\''), take_until("'"), char('\''))).parse(input)
        as IResult<&str, &str, OpenCypherParsingError>
    {
        return Ok((rest, Expression::Literal(Literal::String(s))));
    }
    // Try TRUE/FALSE
    if let Ok((rest, _)) =
        ws(tag_no_case("TRUE")).parse(input) as IResult<&str, &str, OpenCypherParsingError>
    {
        return Ok((rest, Expression::Literal(Literal::Boolean(true))));
    }
    if let Ok((rest, _)) =
        ws(tag_no_case("FALSE")).parse(input) as IResult<&str, &str, OpenCypherParsingError>
    {
        return Ok((rest, Expression::Literal(Literal::Boolean(false))));
    }
    // Try integer
    if let Ok((rest, digits)) = ws(nom::character::complete::digit1).parse(input)
        as IResult<&str, &str, OpenCypherParsingError>
    {
        let n: i64 = digits.parse().unwrap_or(0);
        return Ok((rest, Expression::Literal(Literal::Integer(n))));
    }
    // Try unquoted identifier (e.g., format names)
    let (rest, ident) = ws(alphanumeric1).parse(input)?;
    Ok((rest, Expression::Literal(Literal::String(ident))))
}

/// Consume input until we find the matching closing ')'.
/// Handles nested parentheses within the inner query.
fn take_balanced_parens<'a>(
    input: &'a str,
) -> IResult<&'a str, &'a str, OpenCypherParsingError<'a>> {
    let mut depth = 1u32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut prev_char = '\0';

    for (i, ch) in input.char_indices() {
        match ch {
            '\'' if !in_double_quote && prev_char != '\\' => in_single_quote = !in_single_quote,
            '"' if !in_single_quote && prev_char != '\\' => in_double_quote = !in_double_quote,
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote => {
                depth -= 1;
                if depth == 0 {
                    let matched = &input[..i];
                    let remaining = &input[i + 1..];
                    return Ok((remaining, matched));
                }
            }
            _ => {}
        }
        prev_char = ch;
    }

    Err(nom::Err::Error(OpenCypherParsingError {
        errors: vec![(input, "Unbalanced parentheses in COPY TO query")],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_copy_to() {
        let input = "COPY (MATCH (u:User) RETURN u.name) TO '/tmp/users.csv'";
        let (rem, stmt) = parse_copy_to(input).unwrap();
        assert!(rem.trim().is_empty());
        assert_eq!(stmt.query, "MATCH (u:User) RETURN u.name");
        assert_eq!(stmt.destination, "/tmp/users.csv");
        assert!(stmt.format.is_none());
        assert!(stmt.options.is_empty());
    }

    #[test]
    fn test_copy_to_with_format() {
        let input = "COPY (MATCH (u:User) RETURN u.name) TO '/tmp/users.parquet' FORMAT PARQUET";
        let (_, stmt) = parse_copy_to(input).unwrap();
        assert_eq!(stmt.query, "MATCH (u:User) RETURN u.name");
        assert_eq!(stmt.destination, "/tmp/users.parquet");
        assert_eq!(stmt.format, Some("PARQUET"));
    }

    #[test]
    fn test_copy_to_with_format_and_options() {
        let input =
            "COPY (MATCH (u:User) RETURN u.name) TO '/tmp/users.csv' FORMAT CSV (HEADER TRUE, DELIMITER ';')";
        let (_, stmt) = parse_copy_to(input).unwrap();
        assert_eq!(stmt.format, Some("CSV"));
        assert_eq!(stmt.options.len(), 2);
        assert_eq!(stmt.options[0].0, "HEADER");
        assert_eq!(stmt.options[1].0, "DELIMITER");
    }

    #[test]
    fn test_copy_to_s3_destination() {
        let input =
            "COPY (MATCH (u:User) RETURN u.name, u.email) TO 's3://bucket/users.parquet' FORMAT PARQUET";
        let (_, stmt) = parse_copy_to(input).unwrap();
        assert_eq!(stmt.destination, "s3://bucket/users.parquet");
        assert_eq!(stmt.format, Some("PARQUET"));
    }

    #[test]
    fn test_copy_to_nested_parens() {
        let input = "COPY (MATCH (u:User) WHERE u.age > 18 RETURN count(u)) TO '/tmp/count.csv'";
        let (_, stmt) = parse_copy_to(input).unwrap();
        assert_eq!(
            stmt.query,
            "MATCH (u:User) WHERE u.age > 18 RETURN count(u)"
        );
    }

    #[test]
    fn test_copy_to_with_semicolon() {
        let input = "COPY (MATCH (n) RETURN n) TO '/tmp/all.json' FORMAT JSON;";
        let (rem, stmt) = parse_copy_to(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(stmt.format, Some("JSON"));
    }

    #[test]
    fn test_copy_to_options_only() {
        let input = "COPY (MATCH (n) RETURN n) TO '/tmp/out.csv' (HEADER TRUE)";
        let (_, stmt) = parse_copy_to(input).unwrap();
        assert!(stmt.format.is_none());
        assert_eq!(stmt.options.len(), 1);
        assert_eq!(stmt.options[0].0, "HEADER");
    }

    #[test]
    fn test_copy_to_quoted_inner_query() {
        let input = r#"COPY (MATCH (u:User) WHERE u.name = 'Alice' RETURN u) TO '/tmp/alice.csv'"#;
        let (_, stmt) = parse_copy_to(input).unwrap();
        assert_eq!(stmt.query, "MATCH (u:User) WHERE u.name = 'Alice' RETURN u");
    }
}
