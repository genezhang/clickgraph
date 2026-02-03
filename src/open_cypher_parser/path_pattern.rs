use std::cell::RefCell;
use std::rc::Rc;
use std::vec;

use nom::character::complete::char;
use nom::combinator::peek;
use nom::error::ErrorKind;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, space0},
    combinator::{map, opt},
    error::Error,
    multi::separated_list0,
    sequence::{delimited, separated_pair},
    IResult, Parser,
};

use super::ast::{
    ConnectedPattern, Direction, Expression, NodePattern, PathPattern, Property, PropertyKVPair,
    RelationshipPattern, VariableLengthSpec,
};
use super::common::ws;
use super::expression::parse_parameter;
use super::{common, expression};
use nom::character::complete::digit1;

/// Type alias for node label/property parsing result to reduce complexity
type NodeLabelPropertyResult<'a> = (Option<Vec<&'a str>>, Option<Vec<Property<'a>>>);

/// Type alias for relationship internals parsing result to reduce complexity
type RelInternalsResult<'a> = (
    Option<&'a str>,
    Option<Vec<&'a str>>,
    Option<Vec<Property<'a>>>,
    Option<VariableLengthSpec>,
);

/// Maximum depth for parsing consecutive relationships in a single path pattern.
/// This prevents stack overflow on adversarial inputs like (a)-[]->(b)-[]->(c)...(repeated 50x)
/// Real-world queries rarely exceed 10 hops; 50 is extremely generous while protecting against DoS.
const MAX_RELATIONSHIP_CHAIN_DEPTH: usize = 50;

/// Try to parse shortestPath() or allShortestPaths() wrapper
fn parse_shortest_path_function(input: &'_ str) -> IResult<&'_ str, PathPattern<'_>> {
    use nom::combinator::map;
    use nom::sequence::delimited;

    // Parse shortestPath() - consume leading whitespace first!
    let parse_shortest = map(
        (
            multispace0, // <-- Add this to consume leading whitespace!
            tag_no_case::<_, _, Error<&str>>("shortestPath"),
            multispace0,
            delimited(
                char('('),
                delimited(multispace0, parse_path_pattern_inner, multispace0),
                char(')'),
            ),
        ),
        |(_, _, _, pattern)| PathPattern::ShortestPath(Box::new(pattern)),
    );

    // Parse allShortestPaths() - consume leading whitespace first!
    let parse_all_shortest = map(
        (
            multispace0, // <-- Add this to consume leading whitespace!
            tag_no_case::<_, _, Error<&str>>("allShortestPaths"),
            multispace0,
            delimited(
                char('('),
                delimited(multispace0, parse_path_pattern_inner, multispace0),
                char(')'),
            ),
        ),
        |(_, _, _, pattern)| PathPattern::AllShortestPaths(Box::new(pattern)),
    );

    // Try both parsers
    alt((parse_shortest, parse_all_shortest)).parse(input)
}

/// Main entry point for parsing path patterns
pub fn parse_path_pattern(input: &'_ str) -> IResult<&'_ str, PathPattern<'_>> {
    // Try shortest path functions first, if that fails try regular pattern
    alt((parse_shortest_path_function, parse_path_pattern_inner)).parse(input)
}

/// Internal parser for path patterns (without shortest path wrapper)
fn parse_path_pattern_inner(input: &'_ str) -> IResult<&'_ str, PathPattern<'_>> {
    let (input, start_node_pattern) = parse_node_pattern.parse(input)?;

    let (_, is_start_of_relation) = is_start_of_a_relationship.parse(input)?;

    if is_start_of_relation {
        let (input, relationship_end_node_pair_inner_option) =
            parse_relationship_and_connected_node.parse(input)?;

        match relationship_end_node_pair_inner_option {
            Some((first_relationship, end_node_pattern)) => {
                let first_connected_pattern = ConnectedPattern {
                    start_node: Rc::new(RefCell::new(start_node_pattern)),
                    relationship: first_relationship,
                    end_node: Rc::new(RefCell::new(end_node_pattern)),
                };

                let mut connected_nodes_pattern: Vec<ConnectedPattern> = vec![];
                connected_nodes_pattern.push(first_connected_pattern);

                // let mut last_end_node = end_node_pattern;
                let (input, consecutive_relations_end_nodes_vec) =
                    parse_consecutive_relationships_with_depth(input, 1)?;

                for (consecutive_relationship, consecutive_end_node_pattern) in
                    consecutive_relations_end_nodes_vec
                {
                    // Safe: connected_nodes_pattern is guaranteed to have at least one element
                    let last_pushed = connected_nodes_pattern
                        .last()
                        .expect("connected_nodes_pattern must not be empty at this point");
                    let connected_pattern = ConnectedPattern {
                        start_node: last_pushed.end_node.clone(),
                        relationship: consecutive_relationship,
                        end_node: Rc::new(RefCell::new(consecutive_end_node_pattern)),
                    };
                    connected_nodes_pattern.push(connected_pattern);
                    // last_end_node = consecutive_end_node_pattern;
                }

                Ok((
                    input,
                    PathPattern::ConnectedPattern(connected_nodes_pattern),
                ))
            }
            // This is only a placeholder error. Replace it with actual custom error later.
            None => Err(nom::Err::Failure(Error::new(input, ErrorKind::Satisfy))),
        }
    } else {
        Ok((input, PathPattern::Node(start_node_pattern)))
    }
}

fn parse_relationship_and_connected_node(
    input: &'_ str,
) -> IResult<&'_ str, Option<(RelationshipPattern<'_>, NodePattern<'_>)>> {
    let (input, relationship_pattern) = parse_relationship_pattern(input)?;

    match relationship_pattern {
        Some(rel_pattern) => {
            let (input, end_node_pattern) = parse_node_pattern.parse(input)?;
            Ok((input, Some((rel_pattern, end_node_pattern))))
        }
        None => Ok((input, None)),
    }
}

// Parses a single `-` (dash) followed by `[` (for relationships with brackets like `-[r:TYPE]->`)
fn parse_single_dash(input: &str) -> IResult<&str, bool> {
    map((char('-'), multispace0, char('[')), |_| true).parse(input)
}

// Parses `--` pattern (undirected/either relationship)
fn parse_double_dash(input: &str) -> IResult<&str, bool> {
    map((char('-'), multispace0, char('-')), |_| true).parse(input)
}

// Parses `<-` or `<--` with spaces allowed in between
// Matches both:
// - `<-` (arrow followed by single dash, used for relationships with properties)
// - `<--` (arrow followed by double dash, used for empty relationships)
fn parse_incoming(input: &str) -> IResult<&str, bool> {
    alt((
        // Try `<--` first (empty relationship pattern)
        map(
            (char('<'), multispace0, char('-'), multispace0, char('-')),
            |_| true,
        ),
        // Fall back to `<-` (relationship with properties)
        map((char('<'), multispace0, char('-')), |_| true),
    ))
    .parse(input)
}

// Parses `->` or `-->` with spaces allowed in between
// Matches both:
// - `->` (single dash followed by arrow)
// - `-->` (double dash followed by arrow, used for empty relationships)
fn parse_outgoing(input: &str) -> IResult<&str, bool> {
    alt((
        // Try `-->` first (empty relationship pattern)
        map(
            (char('-'), multispace0, char('-'), multispace0, char('>')),
            |_| true,
        ),
        // Fall back to `->` (relationship with properties)
        map((char('-'), multispace0, char('>')), |_| true),
    ))
    .parse(input)
}

// Main parser that checks for `<-`, `<--`, `->`, `-->`, `--`, or `-[`
fn is_start_of_a_relationship(input: &str) -> IResult<&str, bool> {
    let (input, _) = multispace0(input)?;

    let (_, found_relationship_start) = opt(peek(alt((
        parse_incoming,    // `<-` or `<--`
        parse_outgoing,    // `->` or `-->`
        parse_double_dash, // `--` (must come before parse_single_dash to avoid false match)
        parse_single_dash, // `-[`
    ))))
    .parse(input)?;
    let is_start = found_relationship_start.is_some();
    Ok((input, is_start))
}

fn get_relation_node(
    input: &'_ str,
) -> IResult<&'_ str, Option<(RelationshipPattern<'_>, NodePattern<'_>)>> {
    // Try to detect the start of a relationship pattern.
    let (_, is_start_of_relation) = is_start_of_a_relationship(input)?;
    if is_start_of_relation {
        parse_relationship_and_connected_node(input)
    } else {
        Ok((input, None))
    }
}

/// Parse consecutive relationships with depth tracking to prevent stack overflow.
///
/// Depth limit protects against adversarial inputs like:
/// `(a)-[]->(b)-[]->(c)-[]->...(repeated thousands of times)`
///
/// # Arguments
/// * `input` - Input string to parse
/// * `depth` - Current recursion depth (starts at 1 for first relationship after initial node)
///
/// # Returns
/// Vector of (relationship, node) pairs representing the chain
fn parse_consecutive_relationships_with_depth(
    input: &'_ str,
    depth: usize,
) -> IResult<&'_ str, Vec<(RelationshipPattern<'_>, NodePattern<'_>)>> {
    // Check depth limit before attempting to parse
    if depth > MAX_RELATIONSHIP_CHAIN_DEPTH {
        return Err(nom::Err::Failure(Error::new(input, ErrorKind::TooLarge)));
    }

    let (input, maybe_relation_node) = get_relation_node(input)?;

    // If we got a relation-node, accumulate it and continue recursively.
    if let Some(relation_node) = maybe_relation_node {
        let mut result = vec![relation_node];
        let (input, mut rest) = parse_consecutive_relationships_with_depth(input, depth + 1)?;
        result.append(&mut rest);
        Ok((input, result))
    } else {
        // No more relation-nodes found, so return an empty vector.
        Ok((input, Vec::new()))
    }
}

/// Legacy wrapper for backward compatibility - delegates to depth-tracked version
#[allow(dead_code)]
fn parse_consecutive_relationships(
    input: &'_ str,
) -> IResult<&'_ str, Vec<(RelationshipPattern<'_>, NodePattern<'_>)>> {
    parse_consecutive_relationships_with_depth(input, 1)
}

// {name: 'Oliver Stone', age: 52, tags: ['actor', 'director'], created: date('2024-01-01')}
pub fn parse_properties(input: &'_ str) -> IResult<&'_ str, Vec<Property<'_>>> {
    alt((
        // Property map: requires curly braces and key-value pairs.
        delimited(
            delimited(space0, char('{'), space0),
            separated_list0(
                delimited(space0, char(','), space0),
                map(
                    separated_pair(
                        // Property key: alphanumeric with underscores (e.g., user_id)
                        delimited(space0, common::parse_alphanumeric_with_underscore, space0),
                        delimited(space0, char(':'), space0),
                        // Use the full expression parser for values - supports:
                        // - Strings: 'hello', "world"
                        // - Numbers: 42, 3.14, -5
                        // - Booleans: true, false
                        // - Null: null
                        // - Lists: [1, 2, 3]
                        // - Function calls: date('2024-01-01'), datetime(...)
                        // - Parameters: $param
                        expression::parse_expression,
                    ),
                    |(key, value_expression)| {
                        Property::PropertyKV(PropertyKVPair {
                            key,
                            value: value_expression,
                        })
                    },
                ),
            ),
            delimited(space0, char('}'), space0),
        ),
        // Parameter variant: no curly braces are expected.
        map(ws(parse_parameter), |expr| {
            match expr {
                Expression::Parameter(s) => vec![Property::Param(s)],
                Expression::FunctionCallExp(_) => {
                    // Parameters with temporal accessors ($param.year) are converted to function calls
                    // These shouldn't be used as node properties, but handle gracefully
                    vec![]
                }
                _ => unreachable!("parse_parameter returned unexpected expression type"),
            }
        }),
    ))
    .parse(input)
}

fn parse_name_or_label_with_properties(
    input: &'_ str,
) -> IResult<&'_ str, (Option<&'_ str>, Option<Vec<Property<'_>>>)> {
    let (remainder, node_label) =
        ws(opt(common::parse_alphanumeric_with_underscore)).parse(input)?;
    let (remainder, node_properties) = opt(parse_properties).parse(remainder)?;
    Ok((remainder, (node_label, node_properties)))
}

// Parse node name or labels (multi-label support) with properties
fn parse_name_or_labels_with_properties(
    input: &'_ str,
) -> IResult<&'_ str, NodeLabelPropertyResult<'_>> {
    let (remainder, node_labels) = parse_node_labels(input)?;
    let (remainder, node_properties) = opt(parse_properties).parse(remainder)?;
    Ok((remainder, (node_labels, node_properties)))
}

/// Parse multiple labels/types separated by | (e.g., User|Person or FOLLOWS|LIKES)
/// This is the common parser for both node labels and relationship types since
/// they share identical syntax: `Label1|Label2|Label3`
///
/// Returns None if no labels are found, Some(vec![...]) otherwise
fn parse_multi_labels_or_types(input: &'_ str) -> IResult<&'_ str, Option<Vec<&'_ str>>> {
    let (remainder, first_label) =
        ws(opt(common::parse_alphanumeric_with_underscore)).parse(input)?;

    if first_label.is_none() {
        return Ok((remainder, None));
    }

    // Safe: first_label is guaranteed to be Some at this point (checked above)
    let mut labels = vec![first_label.expect("first_label must be Some after is_none check")];

    // Parse additional labels separated by |
    let mut current_input = remainder;
    loop {
        let (new_input, pipe) = opt(ws(char('|'))).parse(current_input)?;
        if pipe.is_none() {
            break;
        }

        let (new_input, additional_label) =
            ws(common::parse_alphanumeric_with_underscore).parse(new_input)?;
        labels.push(additional_label);
        current_input = new_input;
    }

    Ok((current_input, Some(labels)))
}

/// Parse relationship types (e.g., :FOLLOWS, :FOLLOWS|LIKES)
/// Thin wrapper around parse_multi_labels_or_types for semantic clarity
fn parse_relationship_labels(input: &'_ str) -> IResult<&'_ str, Option<Vec<&'_ str>>> {
    parse_multi_labels_or_types(input)
}

/// Parse node labels (e.g., :User, :User|Person)
/// Thin wrapper around parse_multi_labels_or_types for semantic clarity
fn parse_node_labels(input: &'_ str) -> IResult<&'_ str, Option<Vec<&'_ str>>> {
    parse_multi_labels_or_types(input)
}

type NameOrLabelWithProperties<'a> = (Option<&'a str>, Option<Vec<Property<'a>>>);
type NameOrLabelsWithProperties<'a> = (Option<Vec<&'a str>>, Option<Vec<Property<'a>>>);

// Parse node name and labels (with multi-label support)
fn parse_name_labels(
    input: &'_ str,
) -> IResult<
    &'_ str,
    (
        NameOrLabelWithProperties<'_>,
        NameOrLabelsWithProperties<'_>,
    ),
> {
    let (input, _) = multispace0(input)?;

    separated_pair(
        parse_name_or_label_with_properties,
        opt(char(':')),
        parse_name_or_labels_with_properties,
    )
    .parse(input)
}

// fn parse_comma(input: &str) -> IResult<&str, Option<&str>> {
//     opt(tag_no_case(",")).parse(input)
// }

fn parse_node_pattern(input: &'_ str) -> IResult<&'_ str, NodePattern<'_>> {
    let (input, _) = multispace0(input)?;

    let empty_node_parser = map(delimited(ws(char('(')), space0, ws(char(')'))), |_| {
        NodePattern {
            name: None,
            labels: None,
            properties: None,
        }
    });

    let node_parser = map(
        delimited(ws(char('(')), parse_name_labels, ws(char(')'))),
        |((node_name, properties_with_node_name), (node_labels, properties_with_node_label))| {
            NodePattern {
                name: node_name,
                labels: node_labels, // Now supports multi-labels directly
                properties: properties_with_node_name.map_or(properties_with_node_label, Some),
            }
        },
    );

    alt((empty_node_parser, node_parser)).parse(input)
}

// Parse relationship internals with support for multiple labels
fn parse_relationship_internals_with_multiple_labels(
    input: &'_ str,
) -> IResult<&'_ str, RelInternalsResult<'_>> {
    let (input, _) = ws(char('[')).parse(input)?;
    let (input, _) = multispace0(input)?;

    // Parse relationship name (optional)
    let (input, rel_name) = ws(opt(common::parse_alphanumeric_with_underscore)).parse(input)?;

    // Parse : separator
    let (input, _) = opt(ws(char(':'))).parse(input)?;

    // Parse relationship labels (can be multiple separated by |)
    let (input, rel_labels) = parse_relationship_labels(input)?;

    // Parse variable length spec
    let (input, var_len) = parse_variable_length_spec(input)?;

    // Parse properties
    let (input, rel_properties) = opt(parse_properties).parse(input)?;

    let (input, _) = ws(char(']')).parse(input)?;
    Ok((input, (rel_name, rel_labels, rel_properties, var_len)))
}

// Parse variable-length specification: *, *2, *1..3, *..5
// Returns Some(VariableLengthSpec) if parsed, None if not present
fn parse_variable_length_spec(input: &'_ str) -> IResult<&'_ str, Option<VariableLengthSpec>> {
    let (input, _) = multispace0(input)?;

    // Check if there's a * character
    let (input, asterisk_opt) = opt(char('*')).parse(input)?;
    if asterisk_opt.is_none() {
        // No *, so no variable-length spec
        return Ok((input, None));
    }

    let (input, _) = multispace0(input)?;

    // Try to parse range specifications
    // *N..M (range with both bounds)
    let range_parser = map(
        separated_pair(
            map(digit1, |s: &str| s.parse::<u32>().ok()),
            tag(".."),
            map(digit1, |s: &str| s.parse::<u32>().ok()),
        ),
        |(min, max)| VariableLengthSpec {
            min_hops: min,
            max_hops: max,
        },
    );

    // *..M (upper bound only, min defaults to 1)
    let upper_bound_parser = map(
        nom::sequence::preceded(tag(".."), map(digit1, |s: &str| s.parse::<u32>().ok())),
        |max| VariableLengthSpec {
            min_hops: Some(1),
            max_hops: max,
        },
    );

    // *N.. (lower bound only, max unbounded)
    let lower_bound_parser = map(
        nom::sequence::terminated(map(digit1, |s: &str| s.parse::<u32>().ok()), tag("..")),
        |min| VariableLengthSpec {
            min_hops: min,
            max_hops: None, // Unbounded
        },
    );

    // *N (fixed length)
    let fixed_length_parser = map(map(digit1, |s: &str| s.parse::<u32>().ok()), |n| {
        VariableLengthSpec {
            min_hops: n,
            max_hops: n,
        }
    });

    // * (unbounded, equivalent to *1..)
    let unbounded_parser = map(
        nom::combinator::peek(nom::branch::alt((
            nom::character::complete::char(']'),
            nom::character::complete::char('-'),
        ))),
        |_| VariableLengthSpec {
            min_hops: Some(1),
            max_hops: None,
        },
    );

    let (input, spec_opt) = alt((
        range_parser,
        upper_bound_parser,
        lower_bound_parser, // Must come before fixed_length_parser
        fixed_length_parser,
        unbounded_parser,
    ))
    .map(Some)
    .parse(input)?;

    // Validate the parsed specification
    if let Some(ref spec) = spec_opt {
        if let Err(_validation_error) = spec.validate() {
            // Convert validation error to nom error
            // Note: We use Failure (not Error) to indicate this is a semantic error, not a parsing error
            crate::debug_print!(
                "Variable-length path validation error: {}",
                _validation_error
            );
            return Err(nom::Err::Failure(Error::new(input, ErrorKind::Verify)));
        }
    }

    Ok((input, spec_opt))
}

// Parse relationships - e.g -
//  '<-[ name:KIND ]-' , '-[ name:KIND ]->' '-[ name:KIND ]-',
// '<-[name]-', '-[name]->', '-[name]-'
// '<-[]', '-[]->', '-[]-'
//  '<-[*1..3]-', '-[*2]->', '-[r:KNOWS*]- '
fn parse_relationship_pattern(input: &'_ str) -> IResult<&'_ str, Option<RelationshipPattern<'_>>> {
    // Note: Removed bidirectional check - mixed-direction chains like
    // (a)<-[:]-(b)-[:]->(c) are valid Cypher patterns

    let empty_incoming_relationship_parser =
        map(delimited(ws(tag("<-")), space0, ws(tag("-"))), |_| {
            RelationshipPattern {
                direction: Direction::Incoming,
                name: None,
                labels: None,
                properties: None,
                variable_length: None,
            }
        });

    let incoming_relationship_with_props_parser = map(
        delimited(
            tag("<-"),
            parse_relationship_internals_with_multiple_labels,
            tag("-"),
        ),
        |(rel_name, rel_labels, rel_properties, var_len)| RelationshipPattern {
            direction: Direction::Incoming,
            name: rel_name,
            labels: rel_labels,
            properties: rel_properties,
            variable_length: var_len,
        },
    );

    let empty_outgoing_relationship_parser =
        map(delimited(ws(tag("-")), space0, ws(tag("->"))), |_| {
            RelationshipPattern {
                direction: Direction::Outgoing,
                name: None,
                labels: None,
                properties: None,
                variable_length: None,
            }
        });

    let outgoing_relationship_with_props_parser = map(
        delimited(
            tag("-"),
            parse_relationship_internals_with_multiple_labels,
            tag("->"),
        ),
        |(rel_name, rel_labels, rel_properties, var_len)| RelationshipPattern {
            direction: Direction::Outgoing,
            name: rel_name,
            labels: rel_labels,
            properties: rel_properties,
            variable_length: var_len,
        },
    );

    let empty_either_relationship_parser =
        map(delimited(ws(tag("-")), space0, ws(tag("-"))), |_| {
            RelationshipPattern {
                direction: Direction::Either,
                name: None,
                labels: None,
                properties: None,
                variable_length: None,
            }
        });

    let either_relationship_with_props_parser = map(
        delimited(
            tag("-"),
            parse_relationship_internals_with_multiple_labels,
            tag("-"),
        ),
        |(rel_name, rel_labels, rel_properties, var_len)| RelationshipPattern {
            direction: Direction::Either,
            name: rel_name,
            labels: rel_labels,
            properties: rel_properties,
            variable_length: var_len,
        },
    );

    opt(alt((
        empty_incoming_relationship_parser,
        empty_outgoing_relationship_parser,
        empty_either_relationship_parser,
        incoming_relationship_with_props_parser,
        outgoing_relationship_with_props_parser,
        either_relationship_with_props_parser,
    )))
    .parse(input)
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::Literal;

    use super::*;
    use nom::{
        error::{Error, ErrorKind},
        Err,
    };
    use std::rc::Rc;

    #[test]
    fn test_parse_properties_with_numeric_literal() {
        // Test inline properties with numeric values
        let input = "{id: 1}";
        let result = parse_properties(input);
        assert!(result.is_ok(), "Failed to parse {{id: 1}}: {:?}", result);
        let (remaining, props) = result.unwrap();
        assert_eq!(
            remaining, "",
            "Should consume entire input, got: '{}'",
            remaining
        );
        assert_eq!(props.len(), 1, "Should have one property");

        match &props[0] {
            Property::PropertyKV(kv) => {
                assert_eq!(kv.key, "id");
                match &kv.value {
                    Expression::Literal(Literal::Integer(i)) => assert_eq!(*i, 1),
                    other => panic!("Expected Integer literal, got: {:?}", other),
                }
            }
            other => panic!("Expected PropertyKV, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_properties_with_float_literal() {
        let input = "{price: 3.14}";
        let result = parse_properties(input);
        assert!(
            result.is_ok(),
            "Failed to parse {{price: 3.14}}: {:?}",
            result
        );
        let (remaining, props) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match &props[0] {
            Property::PropertyKV(kv) => {
                assert_eq!(kv.key, "price");
                match &kv.value {
                    Expression::Literal(Literal::Float(f)) => assert!((f - 3.14).abs() < 0.001),
                    other => panic!("Expected Float literal, got: {:?}", other),
                }
            }
            _ => panic!("Expected PropertyKV"),
        }
    }

    #[test]
    fn test_parse_node_with_numeric_property() {
        // Full pattern test: (n:User {id: 1})
        let input = "(n:User {id: 1})";
        let result = parse_path_pattern(input);
        assert!(
            result.is_ok(),
            "Failed to parse (n:User {{id: 1}}): {:?}",
            result
        );
        let (remaining, pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match pattern {
            PathPattern::Node(node) => {
                assert_eq!(node.name, Some("n"));
                assert_eq!(node.labels, Some(vec!["User"]));
                assert!(node.properties.is_some(), "Should have properties");
            }
            _ => panic!("Expected Node pattern"),
        }
    }

    #[test]
    fn test_parse_node_with_underscore_property_key() {
        // Test with underscore in property key
        let input = "(u:User {user_id: 1})";
        let result = parse_path_pattern(input);
        assert!(
            result.is_ok(),
            "Failed to parse (u:User {{user_id: 1}}): {:?}",
            result
        );
        let (remaining, pattern) = result.unwrap();
        assert_eq!(
            remaining, "",
            "Should consume entire input, remaining: '{}'",
            remaining
        );

        match pattern {
            PathPattern::Node(node) => {
                assert_eq!(node.name, Some("u"));
                assert_eq!(node.labels, Some(vec!["User"]));
                assert!(node.properties.is_some(), "Should have properties");
            }
            _ => panic!("Expected Node pattern"),
        }
    }

    #[test]
    fn test_parse_path_pattern_single_node() {
        let input = "()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::Node(node))) => {
                assert_eq!(remaining, "");
                let expected = NodePattern {
                    name: None,
                    labels: None,
                    properties: None,
                };
                assert_eq!(&node, &expected);
            }
            Ok((_, other)) => {
                panic!("Expected a Node variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_connected_single_relationship() {
        let input = "()- [ ] -> ()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::ConnectedPattern(connected_patterns))) => {
                assert_eq!(remaining, "");
                assert_eq!(connected_patterns.len(), 1);
                let connected_pattern: &ConnectedPattern<'_> = &connected_patterns[0];
                // The start and end nodes are parsed as empty nodes.
                let expected_node = Rc::new(RefCell::new(NodePattern {
                    name: None,
                    labels: None,
                    properties: None,
                }));
                // For this test, we expect an outgoing relationship without properties.
                let expected_relationship = RelationshipPattern {
                    direction: Direction::Outgoing,
                    name: None,
                    labels: None,
                    properties: None,
                    variable_length: None,
                };
                // Compare start node.
                assert_eq!(
                    format!("{:?}", connected_pattern.start_node),
                    format!("{:?}", expected_node)
                );
                // Compare relationship.
                assert_eq!(&connected_pattern.relationship, &expected_relationship);
                // Compare end node.
                assert_eq!(
                    format!("{:?}", connected_pattern.end_node),
                    format!("{:?}", Rc::new(expected_node))
                );
            }
            Ok((_, other)) => {
                panic!("Expected a ConnectedPattern variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_multiple_patterns() {
        let input = "()- [ ] -> (), ()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, path_pattern)) => {
                match path_pattern {
                    PathPattern::Node(node_pattern) => {
                        assert_eq!(remaining, "");
                        let expected_node = Rc::new(RefCell::new(NodePattern {
                            name: None,
                            labels: None,
                            properties: None,
                        }));
                        assert_eq!(
                            format!("{:?}", node_pattern),
                            format!("{:?}", expected_node)
                        );
                    }
                    PathPattern::ConnectedPattern(connected_patterns) => {
                        assert_eq!(connected_patterns.len(), 1);
                        let connected_pattern: &ConnectedPattern<'_> = &connected_patterns[0];
                        // The start and end nodes are parsed as empty nodes.
                        let expected_node = Rc::new(RefCell::new(NodePattern {
                            name: None,
                            labels: None,
                            properties: None,
                        }));
                        // For this test, we expect an outgoing relationship without properties.
                        let expected_relationship = RelationshipPattern {
                            direction: Direction::Outgoing,
                            name: None,
                            labels: None,
                            properties: None,
                            variable_length: None,
                        };
                        // Compare start node.
                        assert_eq!(
                            format!("{:?}", connected_pattern.start_node),
                            format!("{:?}", expected_node)
                        );
                        // Compare relationship.
                        assert_eq!(&connected_pattern.relationship, &expected_relationship);
                        // Compare end node.
                        assert_eq!(
                            format!("{:?}", connected_pattern.end_node),
                            format!("{:?}", Rc::new(expected_node))
                        );
                    }
                    PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
                        panic!("Unexpected shortest path pattern in this test");
                    }
                }
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_connected_multiple_relationships() {
        let input = "()-[]->()<-[]-()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::ConnectedPattern(connected_patterns))) => {
                assert_eq!(remaining, "");
                // Expect two connected patterns.
                assert_eq!(connected_patterns.len(), 2);
                let expected_node = Rc::new(RefCell::new(NodePattern {
                    name: None,
                    labels: None,
                    properties: None,
                }));
                let expected_relationship_1 = RelationshipPattern {
                    direction: Direction::Outgoing,
                    name: None,
                    labels: None,
                    properties: None,
                    variable_length: None,
                };
                // First connected pattern: from node1 to node2.
                let connected_pattern_1: &ConnectedPattern<'_> = &connected_patterns[0];
                assert_eq!(
                    format!("{:?}", connected_pattern_1.start_node),
                    format!("{:?}", expected_node)
                );
                assert_eq!(&connected_pattern_1.relationship, &expected_relationship_1);
                let start_node_2nd = connected_pattern_1.end_node.clone();
                // Second connected pattern: from node2 to node3.
                let connected_pattern_2 = &connected_patterns[1];
                assert_eq!(
                    format!("{:?}", connected_pattern_2.start_node),
                    format!("{:?}", start_node_2nd)
                );
                let expected_relationship_2 = RelationshipPattern {
                    direction: Direction::Incoming,
                    name: None,
                    labels: None,
                    properties: None,
                    variable_length: None,
                };
                assert_eq!(&connected_pattern_2.relationship, &expected_relationship_2);
                assert_eq!(
                    format!("{:?}", connected_pattern_2.end_node),
                    format!("{:?}", Rc::new(expected_node))
                );
            }
            Ok((_, other)) => {
                panic!("Expected a ConnectedPattern variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_connected_multiple_relationships_props_and_labels() {
        let input =
            "(a:IamA {name: 'IamA'} )-[:Pointing]->(b)<-[pointing {what: $dontKnow}]-(:IamC)";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::ConnectedPattern(connected_patterns))) => {
                assert_eq!(remaining, "");
                // Expect two connected patterns.
                assert_eq!(connected_patterns.len(), 2);

                let expected_node_a = Rc::new(RefCell::new(NodePattern {
                    name: Some("a"),
                    labels: Some(vec!["IamA"]),
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "name",
                        value: Expression::Literal(Literal::String("IamA")),
                    })]),
                }));

                let expected_node_b = Rc::new(RefCell::new(NodePattern {
                    name: Some("b"),
                    labels: None,
                    properties: None,
                }));

                let expected_node_c = Rc::new(RefCell::new(NodePattern {
                    name: None,
                    labels: Some(vec!["IamC"]),
                    properties: None,
                }));

                let expected_relationship_1 = RelationshipPattern {
                    direction: Direction::Outgoing,
                    name: None,
                    labels: Some(vec!["Pointing"]),
                    properties: None,
                    variable_length: None,
                };

                let expected_relationship_2 = RelationshipPattern {
                    direction: Direction::Incoming,
                    name: Some("pointing"),
                    labels: None,
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "what",
                        value: Expression::Parameter("dontKnow"),
                    })]),
                    variable_length: None,
                };
                // First connected pattern: from a to b.
                let connected_pattern_1: &ConnectedPattern<'_> = &connected_patterns[0];
                assert_eq!(
                    format!("{:?}", connected_pattern_1.start_node),
                    format!("{:?}", expected_node_a)
                );
                assert_eq!(
                    format!("{:?}", connected_pattern_1.end_node),
                    format!("{:?}", expected_node_b)
                );
                assert_eq!(&connected_pattern_1.relationship, &expected_relationship_1);
                // Second connected pattern: from b to c.
                let connected_pattern_2 = &connected_patterns[1];
                assert_eq!(
                    format!("{:?}", connected_pattern_2.start_node),
                    format!("{:?}", connected_pattern_1.end_node)
                );
                assert_eq!(
                    format!("{:?}", connected_pattern_2.end_node),
                    format!("{:?}", expected_node_c)
                );
                assert_eq!(&connected_pattern_2.relationship, &expected_relationship_2);
            }
            Ok((_, other)) => {
                panic!("Expected a ConnectedPattern variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parsing failed with error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_path_pattern_placeholder_error() {
        let input = "()-[";
        let result = parse_path_pattern(input);
        match result {
            Err(Err::Failure(Error { code, .. })) => {
                // Change this later with actual custom error.s
                assert_eq!(code, ErrorKind::Satisfy);
            }
            _ => {
                panic!("Expected failure error for incomplete relationship pattern");
            }
        }
    }

    // ===== Validation Tests for Variable-Length Paths =====

    #[test]
    fn test_invalid_range_min_greater_than_max() {
        // *5..2 should fail validation (min > max)
        let input = "()-[*5..2]->()";
        let result = parse_path_pattern(input);
        match result {
            Err(Err::Failure(Error { code, .. })) => {
                assert_eq!(code, ErrorKind::Verify); // Validation error
            }
            Ok(_) => {
                panic!("Expected validation error for *5..2 (min > max)");
            }
            Err(e) => {
                panic!("Expected Failure with Verify, got: {:?}", e);
            }
        }
    }

    #[test]
    fn test_invalid_range_with_zero_min() {
        // *0..5 is now allowed (for shortest path self-loops) with a warning
        let input = "()-[*0..5]->()";
        let result = parse_path_pattern(input);
        assert!(
            result.is_ok(),
            "Expected *0..5 to be allowed (with warning), but got: {:?}",
            result
        );
    }

    #[test]
    fn test_invalid_range_with_zero_max() {
        // *0 is now allowed (for shortest path self-loops) with a warning
        let input = "()-[*0]->()";
        let result = parse_path_pattern(input);
        assert!(
            result.is_ok(),
            "Expected *0 to be allowed (with warning), but got: {:?}",
            result
        );
    }

    #[test]
    fn test_valid_variable_length_patterns() {
        // Test various valid patterns
        let valid_inputs = vec![
            "()-[*1..3]->()",    // Normal range
            "()-[*2]->()",       // Fixed length
            "()-[*..5]->()",     // Upper bound only
            "()-[*]->()",        // Unbounded
            "()-[*1..100]->(),", // Large but valid range
        ];

        for input in valid_inputs {
            let result = parse_path_pattern(input);
            assert!(
                result.is_ok(),
                "Expected {} to parse successfully, but got: {:?}",
                input,
                result
            );
        }
    }

    #[test]
    fn test_variable_length_spec_validation_direct() {
        // Test the validation method directly

        // Valid cases
        assert!(VariableLengthSpec::range(1, 3).validate().is_ok());
        assert!(VariableLengthSpec::fixed(5).validate().is_ok());
        assert!(VariableLengthSpec::unbounded().validate().is_ok());
        assert!(VariableLengthSpec::max_only(10).validate().is_ok());

        // Invalid case: min > max
        let invalid_spec = VariableLengthSpec {
            min_hops: Some(5),
            max_hops: Some(2),
        };
        assert!(invalid_spec.validate().is_err());
        let err_msg = invalid_spec.validate().unwrap_err();
        assert!(err_msg.contains("minimum hops (5) cannot be greater than maximum hops (2)"));

        // Zero hops is now allowed (for shortest path self-loops) - prints warning
        let zero_spec = VariableLengthSpec {
            min_hops: Some(0),
            max_hops: Some(5),
        };
        assert!(
            zero_spec.validate().is_ok(),
            "Zero hops should be allowed with warning"
        );
    }

    #[test]
    fn test_parse_shortest_path_simple() {
        let input = "shortestPath((a:Person)-[*]-(b:Person))";
        let result = parse_path_pattern(input);

        assert!(result.is_ok(), "Failed to parse shortestPath: {:?}", result);
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        // Verify it's a ShortestPath variant
        match path_pattern {
            PathPattern::ShortestPath(inner) => {
                // Verify inner pattern is a ConnectedPattern
                match inner.as_ref() {
                    PathPattern::ConnectedPattern(connected) => {
                        assert_eq!(connected.len(), 1, "Should have one connected pattern");
                    }
                    _ => panic!("Expected ConnectedPattern inside ShortestPath"),
                }
            }
            _ => panic!("Expected ShortestPath variant, got: {:?}", path_pattern),
        }
    }

    #[test]
    fn test_parse_all_shortest_paths() {
        let input = "allShortestPaths((a:Person)-[*]-(b:Person))";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse allShortestPaths: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        // Verify it's an AllShortestPaths variant
        match path_pattern {
            PathPattern::AllShortestPaths(inner) => {
                // Verify inner pattern is a ConnectedPattern
                match inner.as_ref() {
                    PathPattern::ConnectedPattern(connected) => {
                        assert_eq!(connected.len(), 1, "Should have one connected pattern");
                    }
                    _ => panic!("Expected ConnectedPattern inside AllShortestPaths"),
                }
            }
            _ => panic!("Expected AllShortestPaths variant, got: {:?}", path_pattern),
        }
    }

    #[test]
    fn test_parse_shortest_path_with_relationship_type() {
        let input = "shortestPath((a:Person)-[:KNOWS*]-(b:Person))";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse shortestPath with relationship type: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match path_pattern {
            PathPattern::ShortestPath(inner) => {
                match inner.as_ref() {
                    PathPattern::ConnectedPattern(connected) => {
                        assert_eq!(connected.len(), 1);
                        // Verify relationship has KNOWS label
                        assert_eq!(connected[0].relationship.labels, Some(vec!["KNOWS"]));
                    }
                    _ => panic!("Expected ConnectedPattern inside ShortestPath"),
                }
            }
            _ => panic!("Expected ShortestPath variant"),
        }
    }

    #[test]
    fn test_parse_shortest_path_with_whitespace() {
        let input = "shortestPath( ( a : Person ) - [ * ] - ( b : Person ) )";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse shortestPath with whitespace: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match path_pattern {
            PathPattern::ShortestPath(_) => {
                // Success - whitespace handled correctly
            }
            _ => panic!("Expected ShortestPath variant"),
        }
    }

    #[test]
    fn test_parse_regular_pattern_not_shortest_path() {
        let input = "(a:Person)-[*]-(b:Person)";
        let result = parse_path_pattern(input);

        assert!(result.is_ok());
        let (_, path_pattern) = result.unwrap();

        // Should NOT be wrapped in ShortestPath
        match path_pattern {
            PathPattern::ConnectedPattern(_) => {
                // Correct - regular pattern without shortest path wrapper
            }
            PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
                panic!("Should not wrap regular pattern in shortest path");
            }
            _ => {}
        }
    }

    #[test]
    fn test_parse_shortest_path_directed() {
        let input = "shortestPath((a:Person)-[*]->(b:Person))";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse directed shortestPath: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match path_pattern {
            PathPattern::ShortestPath(inner) => {
                match inner.as_ref() {
                    PathPattern::ConnectedPattern(connected) => {
                        assert_eq!(connected.len(), 1);
                        // Verify direction is outgoing
                        assert_eq!(connected[0].relationship.direction, Direction::Outgoing);
                    }
                    _ => panic!("Expected ConnectedPattern"),
                }
            }
            _ => panic!("Expected ShortestPath variant"),
        }
    }

    #[test]
    fn test_parse_path_pattern_multiple_relationship_labels() {
        let input = "()- [:TYPE1|TYPE2] -> ()";
        let result = parse_path_pattern(input);
        match result {
            Ok((remaining, PathPattern::ConnectedPattern(connected_patterns))) => {
                assert_eq!(remaining, "");
                assert_eq!(connected_patterns.len(), 1);
                let connected_pattern: &ConnectedPattern<'_> = &connected_patterns[0];
                // The start and end nodes are parsed as empty nodes.
                let expected_node = Rc::new(RefCell::new(NodePattern {
                    name: None,
                    labels: None,
                    properties: None,
                }));
                // For this test, we expect an outgoing relationship with multiple labels.
                let expected_relationship = RelationshipPattern {
                    direction: Direction::Outgoing,
                    name: None,
                    labels: Some(vec!["TYPE1", "TYPE2"]),
                    properties: None,
                    variable_length: None,
                };
                // Compare start node.
                assert_eq!(
                    format!("{:?}", connected_pattern.start_node),
                    format!("{:?}", expected_node)
                );
                // Compare relationship.
                assert_eq!(&connected_pattern.relationship, &expected_relationship);
                // Compare end node.
                assert_eq!(
                    format!("{:?}", connected_pattern.end_node),
                    format!("{:?}", Rc::new(expected_node))
                );
            }
            Ok((_, other)) => {
                panic!("Expected a ConnectedPattern variant, got: {:?}", other);
            }
            Err(e) => {
                panic!("Parse error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_node_pattern_multiple_labels() {
        // Test: (x:User|Post) should parse with both labels
        let input = "(x:User|Post)";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse multi-label node: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match path_pattern {
            PathPattern::Node(node) => {
                assert_eq!(node.name, Some("x"), "Node name should be 'x'");
                assert_eq!(
                    node.labels,
                    Some(vec!["User", "Post"]),
                    "Should have both User and Post labels"
                );
                assert!(node.properties.is_none(), "Should have no properties");
            }
            _ => panic!("Expected Node pattern, got: {:?}", path_pattern),
        }
    }

    #[test]
    fn test_parse_node_pattern_multiple_labels_with_properties() {
        // Test: (x:User|Post {id: 1}) with properties
        let input = "(x:User|Post {id: 1})";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse multi-label node with properties: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match path_pattern {
            PathPattern::Node(node) => {
                assert_eq!(node.name, Some("x"));
                assert_eq!(node.labels, Some(vec!["User", "Post"]));
                assert!(node.properties.is_some(), "Should have properties");
            }
            _ => panic!("Expected Node pattern"),
        }
    }

    #[test]
    fn test_parse_node_pattern_triple_labels() {
        // Test: (x:Person|User|Admin) with three labels
        let input = "(x:Person|User|Admin)";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse triple-label node: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match path_pattern {
            PathPattern::Node(node) => {
                assert_eq!(node.name, Some("x"));
                assert_eq!(
                    node.labels,
                    Some(vec!["Person", "User", "Admin"]),
                    "Should have all three labels"
                );
            }
            _ => panic!("Expected Node pattern"),
        }
    }

    #[test]
    fn test_parse_multi_label_in_connected_pattern() {
        // Test: (u:User)-[:FOLLOWS]->(x:User|Post) - multi-label as end node
        let input = "(u:User)-[:FOLLOWS]->(x:User|Post)";
        let result = parse_path_pattern(input);

        assert!(
            result.is_ok(),
            "Failed to parse connected pattern with multi-label end node: {:?}",
            result
        );
        let (remaining, path_pattern) = result.unwrap();
        assert_eq!(remaining, "", "Should consume entire input");

        match path_pattern {
            PathPattern::ConnectedPattern(connected) => {
                assert_eq!(connected.len(), 1);

                // Check start node
                let start_node = connected[0].start_node.borrow();
                assert_eq!(start_node.name, Some("u"));
                assert_eq!(start_node.labels, Some(vec!["User"]));

                // Check end node has multiple labels
                let end_node = connected[0].end_node.borrow();
                assert_eq!(end_node.name, Some("x"));
                assert_eq!(
                    end_node.labels,
                    Some(vec!["User", "Post"]),
                    "End node should have both User and Post labels"
                );
            }
            _ => panic!("Expected ConnectedPattern"),
        }
    }

    // ===== Depth Limit Tests =====

    #[test]
    fn test_reasonable_relationship_chain_depth() {
        // Test a long but reasonable chain (10 relationships)
        let mut query = String::from("(a)");
        for i in 0..10 {
            query.push_str(&format!("-[:REL{}]->(n{})", i, i));
        }

        let result = parse_path_pattern(&query);
        assert!(
            result.is_ok(),
            "Should parse 10 consecutive relationships without hitting depth limit"
        );

        if let Ok((_, PathPattern::ConnectedPattern(connected))) = result {
            assert_eq!(connected.len(), 10, "Should have 10 relationship patterns");
        } else {
            panic!("Expected ConnectedPattern");
        }
    }

    #[test]
    fn test_maximum_relationship_chain_depth() {
        // Test exactly at the limit (50 relationships)
        let mut query = String::from("(a)");
        for i in 0..50 {
            query.push_str(&format!("-[]->(n{})", i));
        }

        let result = parse_path_pattern(&query);
        assert!(
            result.is_ok(),
            "Should parse exactly 50 consecutive relationships (at limit)"
        );

        if let Ok((_, PathPattern::ConnectedPattern(connected))) = result {
            assert_eq!(
                connected.len(),
                50,
                "Should have exactly 50 relationship patterns"
            );
        } else {
            panic!("Expected ConnectedPattern");
        }
    }

    #[test]
    fn test_exceeds_maximum_relationship_chain_depth() {
        // Test exceeding the limit (51 relationships)
        let mut query = String::from("(a)");
        for i in 0..51 {
            query.push_str(&format!("-[]->(n{})", i));
        }

        let result = parse_path_pattern(&query);
        match result {
            Err(nom::Err::Failure(Error { code, .. })) => {
                assert_eq!(
                    code,
                    ErrorKind::TooLarge,
                    "Should fail with TooLarge error when exceeding depth limit"
                );
            }
            Ok(_) => {
                panic!("Should have failed with depth limit error for 51 relationships");
            }
            Err(e) => {
                panic!("Expected Failure with TooLarge, got: {:?}", e);
            }
        }
    }

    #[test]
    fn test_depth_limit_error_message_clarity() {
        // Verify error occurs at a predictable point
        let mut query = String::from("(start)");
        for i in 0..100 {
            query.push_str(&format!("-[:T{}]->(n{})", i, i));
        }

        let result = parse_path_pattern(&query);
        assert!(
            result.is_err(),
            "Should error on deeply nested relationship chains (100 > 50 limit)"
        );
    }
}
