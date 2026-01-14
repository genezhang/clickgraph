use nom::{
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0},
    combinator::{cut, opt, recognize},
    error::context,
    multi::separated_list1,
    sequence::{delimited, preceded},
    IResult, Parser,
};

use super::{
    ast::{ReturnClause, ReturnItem},
    common::ws,
    errors::OpenCypherParsingError,
    expression::{parse_expression, parse_identifier},
};

fn parse_return_item(input: &'_ str) -> IResult<&'_ str, ReturnItem<'_>> {
    // Capture the original text of the expression using recognize
    let (input, expr_text) = recognize(parse_expression).parse(input)?;

    // Parse the expression again to get the AST (recognize consumes but doesn't parse)
    let (_, expression) = parse_expression.parse(expr_text)?;

    let (input, alias) = opt(preceded(ws(tag_no_case("AS")), ws(parse_identifier))).parse(input)?;

    // Only store original_text when no explicit alias is provided
    let original_text = if alias.is_none() {
        Some(expr_text.trim())
    } else {
        None
    };

    let return_item = ReturnItem {
        expression,
        alias,
        original_text,
    };
    Ok((input, return_item))
}

pub fn parse_return_clause(
    input: &'_ str,
) -> IResult<&'_ str, ReturnClause<'_>, OpenCypherParsingError<'_>> {
    // Parse the RETURN statement

    let (input, _) = ws(tag_no_case("RETURN")).parse(input)?;

    // Check for optional DISTINCT keyword
    let (input, distinct) = opt(ws(tag_no_case("DISTINCT"))).parse(input)?;
    let distinct = distinct.is_some();

    let (input, return_items) = context(
        "Error in return clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(return_item_parser),
        ),
    )
    .parse(input)?;

    let return_clause = ReturnClause {
        distinct,
        return_items,
    };

    Ok((input, return_clause))
}

fn return_item_parser(input: &str) -> IResult<&str, ReturnItem<'_>, OpenCypherParsingError<'_>> {
    parse_return_item(input).map_err(|e| match e {
        nom::Err::Incomplete(needed) => nom::Err::Incomplete(needed),
        nom::Err::Error(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
        nom::Err::Failure(err) => nom::Err::Failure(OpenCypherParsingError::from(err)),
    })
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::Expression;

    use super::*;
    use nom::Err;

    #[test]
    fn test_parse_return_item_no_alias() {
        let input = "a";
        let res = parse_return_item(input);
        match res {
            Ok((remaining, return_item)) => {
                assert_eq!(remaining, "");
                let expected = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                    original_text: Some("a"),
                };
                assert_eq!(&return_item, &expected);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_item_with_alias() {
        let input = "a AS alias";
        let res = parse_return_item(input);
        match res {
            Ok((remaining, return_item)) => {
                assert_eq!(remaining, "");
                let expected = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: Some("alias"),
                    original_text: None, // No original_text when explicit alias is provided
                };
                assert_eq!(&return_item, &expected);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_with_distinct() {
        let input = "RETURN DISTINCT a.name";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert!(return_clause.distinct, "DISTINCT flag should be true");
                assert_eq!(return_clause.return_items.len(), 1);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_without_distinct() {
        let input = "RETURN a.name";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert!(!return_clause.distinct, "DISTINCT flag should be false");
                assert_eq!(return_clause.return_items.len(), 1);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_single_item() {
        let input = "RETURN a";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 1);
                let expected_item = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                    original_text: Some("a"),
                };
                assert_eq!(&return_clause.return_items[0], &expected_item);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_multiple_items() {
        let input = "RETURN a, b AS aliasB, c";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 3);
                let expected_item1 = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                    original_text: Some("a"),
                };
                let expected_item2 = ReturnItem {
                    expression: Expression::Variable("b"),
                    alias: Some("aliasB"),
                    original_text: None,
                };
                let expected_item3 = ReturnItem {
                    expression: Expression::Variable("c"),
                    alias: None,
                    original_text: Some("c"),
                };
                assert_eq!(&return_clause.return_items[0], &expected_item1);
                assert_eq!(&return_clause.return_items[1], &expected_item2);
                assert_eq!(&return_clause.return_items[2], &expected_item3);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_extra_whitespace() {
        let input = "   RETURN   a   AS  a_alias  ,   b   ,   c  AS c_alias  ";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 3);
                let expected_item1 = ReturnItem {
                    expression: Expression::Variable("a"),
                    alias: Some("a_alias"),
                    original_text: None,
                };
                let expected_item2 = ReturnItem {
                    expression: Expression::Variable("b"),
                    alias: None,
                    original_text: Some("b"),
                };
                let expected_item3 = ReturnItem {
                    expression: Expression::Variable("c"),
                    alias: Some("c_alias"),
                    original_text: None,
                };
                assert_eq!(&return_clause.return_items[0], &expected_item1);
                assert_eq!(&return_clause.return_items[1], &expected_item2);
                assert_eq!(&return_clause.return_items[2], &expected_item3);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_missing_return_keyword() {
        let input = "Match a, b AS aliasB";
        let res = parse_return_clause(input);
        match res {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure because the input does not start with "RETURN".
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure due to missing RETURN keyword, but got remaining: {:?} and clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_return_clause_map_literal() {
        use crate::open_cypher_parser::ast::Literal;

        let input = "RETURN {days: 5} AS d";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                assert_eq!(item.alias, Some("d"));
                if let Expression::MapLiteral(entries) = &item.expression {
                    assert_eq!(entries.len(), 1);
                    assert_eq!(entries[0].0, "days");
                    if let Expression::Literal(Literal::Integer(n)) = entries[0].1 {
                        assert_eq!(n, 5);
                    } else {
                        panic!("Expected integer literal for value");
                    }
                } else {
                    panic!("Expected MapLiteral, got {:?}", item.expression);
                }
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_parse_return_clause_with_pattern_comprehension() {
        let input = "RETURN [(p)-[:KNOWS]->(f) | f.firstName]";
        let res = parse_return_clause(input);
        match res {
            Ok((remaining, return_clause)) => {
                println!("Parsed! Remaining: {:?}", remaining);
                assert_eq!(remaining, "");
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                println!("Return item expression: {:?}", item.expression);
                if let Expression::PatternComprehension(_) = &item.expression {
                    // Success - pattern comprehension was parsed correctly
                } else {
                    panic!("Expected PatternComprehension, got {:?}", item.expression);
                }
            }
            Err(e) => panic!("Return clause parsing failed: {:?}", e),
        }
    }

    // Tests for Neo4j-compatible alias behavior

    #[test]
    fn test_original_text_preserves_spacing_arithmetic() {
        let input = "RETURN 1  +  1";
        let res = parse_return_clause(input);
        match res {
            Ok((_, return_clause)) => {
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                assert_eq!(item.original_text, Some("1  +  1"));
                assert_eq!(item.alias, None);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_original_text_function_call() {
        let input = "RETURN substring('hello', 1, 3)";
        let res = parse_return_clause(input);
        match res {
            Ok((_, return_clause)) => {
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                assert_eq!(item.original_text, Some("substring('hello', 1, 3)"));
                assert_eq!(item.alias, None);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_original_text_function_with_spacing() {
        let input = "RETURN substring( 'hello' , 1 , 3 )";
        let res = parse_return_clause(input);
        match res {
            Ok((_, return_clause)) => {
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                // trim() removes leading/trailing whitespace but preserves internal spacing
                assert_eq!(item.original_text, Some("substring( 'hello' , 1 , 3 )"));
                assert_eq!(item.alias, None);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_original_text_property_access() {
        let input = "RETURN a.code";
        let res = parse_return_clause(input);
        match res {
            Ok((_, return_clause)) => {
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                assert_eq!(item.original_text, Some("a.code"));
                assert_eq!(item.alias, None);
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_original_text_not_set_with_explicit_alias() {
        let input = "RETURN a.code AS airport_code";
        let res = parse_return_clause(input);
        match res {
            Ok((_, return_clause)) => {
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                assert_eq!(item.original_text, None); // No original_text when explicit alias provided
                assert_eq!(item.alias, Some("airport_code"));
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }

    #[test]
    fn test_original_text_multiple_items_mixed() {
        let input = "RETURN a.code, substring(a.code, 1, 3), a.name AS airport_name";
        let res = parse_return_clause(input);
        match res {
            Ok((_, return_clause)) => {
                assert_eq!(return_clause.return_items.len(), 3);

                // First item: no alias, has original_text
                assert_eq!(return_clause.return_items[0].original_text, Some("a.code"));
                assert_eq!(return_clause.return_items[0].alias, None);

                // Second item: no alias, has original_text
                assert_eq!(
                    return_clause.return_items[1].original_text,
                    Some("substring(a.code, 1, 3)")
                );
                assert_eq!(return_clause.return_items[1].alias, None);

                // Third item: explicit alias, no original_text
                assert_eq!(return_clause.return_items[2].original_text, None);
                assert_eq!(return_clause.return_items[2].alias, Some("airport_name"));
            }
            Err(e) => panic!("Parsing failed unexpectedly: {:?}", e),
        }
    }
}
