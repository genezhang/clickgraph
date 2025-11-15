use nom::{
    IResult, Parser,
    bytes::complete::{tag_no_case, take_while1},
    error::context,
};

use super::{ast::UseClause, common::ws, errors::OpenCypherParsingError};

/// Parse a USE clause: USE database_name
/// Examples:
///   USE social_network
///   USE ecommerce
///   USE mydb
pub fn parse_use_clause<'a>(
    input: &'a str,
) -> IResult<&'a str, UseClause<'a>, OpenCypherParsingError<'a>> {
    let (input, _) = ws(tag_no_case("USE")).parse(input)?;

    let (input, database_name) = context(
        "Error parsing database name in USE clause",
        ws(take_while1(|c: char| {
            c.is_alphanumeric() || c == '_' || c == '.'
        })),
    )
    .parse(input)?;

    let use_clause = UseClause { database_name };

    Ok((input, use_clause))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_use_clause_simple() {
        let input = "USE social_network";
        let res = parse_use_clause(input);
        match res {
            Ok((remaining, use_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(use_clause.database_name, "social_network");
            }
            Err(e) => panic!("Failed to parse USE clause: {:?}", e),
        }
    }

    #[test]
    fn test_parse_use_clause_with_dot() {
        let input = "USE neo4j.social_network";
        let res = parse_use_clause(input);
        match res {
            Ok((remaining, use_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(use_clause.database_name, "neo4j.social_network");
            }
            Err(e) => panic!("Failed to parse USE clause with qualified name: {:?}", e),
        }
    }

    #[test]
    fn test_parse_use_clause_case_insensitive() {
        let input = "use mydb";
        let res = parse_use_clause(input);
        match res {
            Ok((remaining, use_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(use_clause.database_name, "mydb");
            }
            Err(e) => panic!("Failed to parse lowercase USE clause: {:?}", e),
        }
    }

    #[test]
    fn test_parse_use_clause_with_trailing() {
        let input = "USE social_network MATCH (n) RETURN n";
        let res = parse_use_clause(input);
        match res {
            Ok((remaining, use_clause)) => {
                assert_eq!(remaining, "MATCH (n) RETURN n"); // ws() consumes trailing whitespace
                assert_eq!(use_clause.database_name, "social_network");
            }
            Err(e) => panic!("Failed to parse USE clause with trailing: {:?}", e),
        }
    }

    #[test]
    fn test_parse_use_clause_missing_name() {
        let input = "USE ";
        let res = parse_use_clause(input);
        assert!(res.is_err(), "Should fail when database name is missing");
    }

    #[test]
    fn test_parse_use_clause_numeric_start() {
        let input = "USE 123db";
        let res = parse_use_clause(input);
        match res {
            Ok((remaining, use_clause)) => {
                assert_eq!(use_clause.database_name, "123db");
                assert_eq!(remaining, "");
            }
            Err(e) => panic!("Should allow numeric characters in database name: {:?}", e),
        }
    }
}
