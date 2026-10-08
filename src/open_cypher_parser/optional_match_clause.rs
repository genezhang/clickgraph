use nom::character::complete::char;
use nom::combinator::{cut, opt};
use nom::error::context;
use nom::{
    bytes::complete::tag_no_case,
    character::complete::{multispace0, multispace1},
    multi::separated_list1,
    sequence::delimited,
    IResult, Parser,
};

use super::ast::OptionalMatchClause;
use super::errors::OpenCypherParsingError;
use super::where_clause;

/// Parse an OPTIONAL MATCH clause
///
/// Syntax: OPTIONAL MATCH <pattern> [WHERE <condition>]
///
/// Examples:
/// - OPTIONAL MATCH (a)-[:FRIEND]->(b)
/// - OPTIONAL MATCH (a)-[:FRIEND]->(b) WHERE b.age > 25
/// - OPTIONAL MATCH (a)-[:FRIEND*1..3]->(b)
/// - OPTIONAL MATCH p = (a)-[:FRIEND*1..3]->(b)
pub fn parse_optional_match_clause(
    input: &'_ str,
) -> IResult<&'_ str, OptionalMatchClause<'_>, OpenCypherParsingError<'_>> {
    // Parse "OPTIONAL MATCH" as two separate keywords
    // multispace0 handles leading whitespace (e.g., after WITH ... WHERE ... expression)
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("OPTIONAL").parse(input)?;
    let (input, _) = multispace1.parse(input)?; // Require whitespace between OPTIONAL and MATCH
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace0(input)?;

    // Parse path patterns (comma-separated list)
    let (input, pattern_parts) = context(
        "Error in optional match clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(super::match_clause::parse_pattern_with_optional_variable),
        ),
    )
    .parse(input)?;

    // Parse optional WHERE clause (specific to this OPTIONAL MATCH)
    let (input, where_clause_opt) = opt(where_clause::parse_where_clause).parse(input)?;

    let optional_match_clause = OptionalMatchClause {
        path_patterns: pattern_parts,
        where_clause: where_clause_opt,
    };

    Ok((input, optional_match_clause))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast::PathPattern;

    // Test that a simple OPTIONAL MATCH clause with one node pattern is parsed correctly.
    #[test]
    fn test_parse_optional_match_clause_single_pattern() {
        let input = "OPTIONAL MATCH ()";
        let result = parse_optional_match_clause(input);
        match result {
            Ok((remaining, optional_match_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(optional_match_clause.path_patterns.len(), 1);
                assert!(optional_match_clause.where_clause.is_none());
            }
            Err(e) => {
                panic!("Expected Ok, got Err: {:?}", e);
            }
        }
    }

    // Test OPTIONAL MATCH with a named node
    #[test]
    fn test_parse_optional_match_clause_named_node() {
        let input = "OPTIONAL MATCH (a)";
        let result = parse_optional_match_clause(input);
        assert!(result.is_ok());
        let (remaining, optional_match_clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(optional_match_clause.path_patterns.len(), 1);

        if let PathPattern::Node(node) = &optional_match_clause.path_patterns[0].1 {
            assert_eq!(node.name, Some("a"));
        } else {
            panic!("Expected OPTIONAL MATCH clause to contain a Node pattern");
        }
    }

    // Test OPTIONAL MATCH with relationship pattern
    #[test]
    fn test_parse_optional_match_clause_relationship() {
        let input = "OPTIONAL MATCH (a)-[:FRIEND]->(b)";
        let result = parse_optional_match_clause(input);
        assert!(result.is_ok());
        let (remaining, optional_match_clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(optional_match_clause.path_patterns.len(), 1);

        // Just verify we got a path pattern (Node, ConnectedPattern, or shortest path variants)
        match &optional_match_clause.path_patterns[0].1 {
            PathPattern::Node(_)
            | PathPattern::ConnectedPattern(_)
            | PathPattern::ShortestPath(_)
            | PathPattern::AllShortestPaths(_) => {
                // Success - valid pattern parsed
            }
        }
    }

    // Test OPTIONAL MATCH with WHERE clause
    #[test]
    fn test_parse_optional_match_clause_with_where() {
        let input = "OPTIONAL MATCH (a)-[:FRIEND]->(b) WHERE b.age > 25";
        let result = parse_optional_match_clause(input);
        assert!(result.is_ok());
        let (remaining, optional_match_clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(optional_match_clause.path_patterns.len(), 1);
        assert!(optional_match_clause.where_clause.is_some());
    }

    // Test multiple path patterns in OPTIONAL MATCH
    #[test]
    fn test_parse_optional_match_clause_multiple_patterns() {
        let input = "OPTIONAL MATCH (a), (b)";
        let result = parse_optional_match_clause(input);
        assert!(result.is_ok());
        let (remaining, optional_match_clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(optional_match_clause.path_patterns.len(), 2);
    }

    // Test that regular MATCH doesn't parse as OPTIONAL MATCH
    #[test]
    fn test_optional_match_requires_optional_keyword() {
        let input = "MATCH (a)";
        let result = parse_optional_match_clause(input);
        assert!(result.is_err());
    }

    // Test case-insensitive parsing
    #[test]
    fn test_optional_match_case_insensitive() {
        let input = "optional match (a)";
        let result = parse_optional_match_clause(input);
        assert!(result.is_ok());
    }

    // Test that whitespace between OPTIONAL and MATCH is required
    #[test]
    fn test_optional_match_requires_whitespace() {
        let input = "OPTIONALMATCH (a)";
        let result = parse_optional_match_clause(input);
        assert!(result.is_err());
    }

    // Test variable-length pattern in OPTIONAL MATCH
    #[test]
    fn test_optional_match_variable_length_path() {
        let input = "OPTIONAL MATCH (a)-[:FRIEND*1..3]->(b)";
        let result = parse_optional_match_clause(input);
        assert!(result.is_ok());
        let (remaining, optional_match_clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(optional_match_clause.path_patterns.len(), 1);
    }
}
