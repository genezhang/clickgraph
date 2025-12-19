use nom::character::complete::char;
use nom::combinator::cut;
use nom::error::context;
use nom::{
    bytes::complete::tag_no_case, character::complete::multispace0, multi::separated_list1,
    sequence::delimited, IResult, Parser,
};

use super::ast::{MatchClause, PathPattern};
use super::errors::OpenCypherParsingError;
use super::expression::parse_identifier;
use super::path_pattern;

pub fn parse_match_clause(
    input: &'_ str,
) -> IResult<&'_ str, MatchClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace0(input)?;

    // Parse comma-separated list of (optional path_variable, pattern)
    let (input, pattern_parts) = context(
        "Error in match clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(parse_pattern_with_optional_variable),
        ),
    )
    .parse(input)?;

    let match_clause = MatchClause {
        path_patterns: pattern_parts,
    };

    Ok((input, match_clause))
}

/// Parse optional "varname = " followed by pattern
fn parse_pattern_with_optional_variable(
    input: &str,
) -> IResult<&str, (Option<&str>, PathPattern<'_>), OpenCypherParsingError<'_>> {
    let mut input_after_var = input;
    let mut path_variable = None;
    
    // Try to parse "varname = "
    if let Ok((rest, name)) = parse_identifier(input) {
        let (rest, _) = multispace0(rest)?;
        if let Ok((rest, _)) = char::<_, nom::error::Error<_>>('=')(rest) {
            let (rest, _) = multispace0(rest)?;
            path_variable = Some(name);
            input_after_var = rest;
        }
    }
    
    // Parse the pattern
    let (input, pattern) = path_parser(input_after_var)?;
    Ok((input, (path_variable, pattern)))
}

fn path_parser(input: &str) -> IResult<&str, PathPattern<'_>, OpenCypherParsingError<'_>> {
    path_pattern::parse_path_pattern(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => {
            // Check if error is due to bidirectional pattern
            if input.contains("<-[") && input.contains("]->") {
                let trimmed = input.trim_start();
                if let Some(incoming_pos) = trimmed.find("<-[") {
                    let after_incoming = &trimmed[incoming_pos..];
                    if after_incoming.contains("]->") {
                        return nom::Err::Failure(OpenCypherParsingError {
                            errors: vec![(
                                input,
                                "Bidirectional relationship patterns <-[:TYPE]-> are not supported. Use two separate MATCH clauses or the undirected pattern -[:TYPE]-.",
                            )],
                        });
                    }
                }
            }
            nom::Err::Failure(OpenCypherParsingError::from(err))
        }
        nom::Err::Failure(err) => {
            // Check if error is due to bidirectional pattern
            if input.contains("<-[") && input.contains("]->") {
                let trimmed = input.trim_start();
                if let Some(incoming_pos) = trimmed.find("<-[") {
                    let after_incoming = &trimmed[incoming_pos..];
                    if after_incoming.contains("]->") {
                        return nom::Err::Failure(OpenCypherParsingError {
                            errors: vec![(
                                input,
                                "Bidirectional relationship patterns <-[:TYPE]-> are not supported. Use two separate MATCH clauses or the undirected pattern -[:TYPE]-.",
                            )],
                        });
                    }
                }
            }
            nom::Err::Failure(OpenCypherParsingError::from(err))
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::NodePattern;

    use super::*;
    use nom::Err;

    // Test that a simple MATCH clause with one node pattern is parsed correctly.
    #[test]
    fn test_parse_match_clause_single_pattern() {
        let input = "MATCH ()";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, match_clause)) => {
                assert_eq!(remaining, "");
                // We expect one path pattern.
                assert_eq!(match_clause.path_patterns.len(), 1);
                match &match_clause.path_patterns[0] {
                    PathPattern::Node(node) => {
                        // Expected empty node: no name, no label, no properties.
                        let expected = NodePattern {
                            name: None,
                            label: None,
                            properties: None,
                        };
                        assert_eq!(node, &expected);
                    }
                    other => {
                        panic!("Expected PathPattern::Node, got {:?}", other);
                    }
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_multiple_patterns() {
        let input = "MATCH () , ()";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, match_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(match_clause.path_patterns.len(), 2);
                for pattern in &match_clause.path_patterns {
                    match pattern {
                        PathPattern::Node(node) => {
                            let expected = NodePattern {
                                name: None,
                                label: None,
                                properties: None,
                            };
                            assert_eq!(node, &expected);
                        }
                        other => {
                            panic!("Expected PathPattern::Node, got {:?}", other);
                        }
                    }
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_missing_match_keyword() {
        let input = "MERGE ()";
        let result = parse_match_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because the input does not start with "MATCH".
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for missing MATCH keyword, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_match_clause_invalid_pattern() {
        let input = "MATCH xyz";
        let result = parse_match_clause(input);
        match result {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected error due to an invalid path pattern.
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure for invalid path pattern, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_match_clause_with_path_variable() {
        let input = "MATCH p = (a:Person)";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, clause)) => {
                assert_eq!(remaining.trim(), "");
                assert_eq!(clause.path_variable, Some("p"));
                assert_eq!(clause.path_patterns.len(), 1);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_with_path_variable_shortestpath() {
        let input = "MATCH p = shortestPath((a)-[*]-(b))";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, clause)) => {
                assert_eq!(remaining.trim(), "");
                assert_eq!(clause.path_variable, Some("p"));
                assert_eq!(clause.path_patterns.len(), 1);
                match &clause.path_patterns[0] {
                    PathPattern::ShortestPath(_) => {
                        // Expected
                    }
                    other => {
                        panic!("Expected PathPattern::ShortestPath, got {:?}", other);
                    }
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_without_path_variable() {
        let input = "MATCH (a:Person)";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, clause)) => {
                assert_eq!(remaining.trim(), "");
                assert_eq!(clause.path_variable, None);
                assert_eq!(clause.path_patterns.len(), 1);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_with_numeric_property() {
        let input = "MATCH (u:User {user_id: 1})";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, clause)) => {
                assert_eq!(
                    remaining.trim(),
                    "",
                    "Should consume entire input, remaining: '{}'",
                    remaining
                );
                assert_eq!(clause.path_patterns.len(), 1);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_with_edge_inline_properties() {
        // Test relationship/edge inline properties with numeric value
        let input = "MATCH (a:User)-[r:FOLLOWS {since: 2024}]->(b:User)";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, clause)) => {
                assert_eq!(
                    remaining.trim(),
                    "",
                    "Should consume entire input, remaining: '{}'",
                    remaining
                );
                assert_eq!(clause.path_patterns.len(), 1);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_match_clause_with_edge_multiple_properties() {
        // Test relationship with multiple inline properties
        let input = "MATCH (a)-[r:KNOWS {weight: 0.5, since: 2020}]->(b)";
        let result = parse_match_clause(input);
        match result {
            Ok((remaining, clause)) => {
                assert_eq!(
                    remaining.trim(),
                    "",
                    "Should consume entire input, remaining: '{}'",
                    remaining
                );
                assert_eq!(clause.path_patterns.len(), 1);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }
}
