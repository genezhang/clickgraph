use nom::{
    bytes::complete::tag_no_case,
    character::complete::{char, multispace0},
    combinator::{cut, opt},
    error::context,
    multi::{many0, separated_list1},
    sequence::{delimited, preceded},
    IResult, Parser,
};

use super::{
    ast::{WithClause, WithItem},
    common::ws,
    errors::OpenCypherParsingError,
    expression::{parse_expression, parse_identifier},
    limit_clause::parse_limit_clause,
    match_clause::parse_match_clause,
    optional_match_clause::parse_optional_match_clause,
    order_by_clause::parse_order_by_clause,
    skip_clause::parse_skip_clause,
    unwind_clause::parse_unwind_clause,
    where_clause::parse_where_clause,
};

fn parse_with_item(input: &'_ str) -> IResult<&'_ str, WithItem<'_>> {
    let expr_result = parse_expression.parse(input);
    
    // Check for pattern comprehension and provide helpful error
    let (input, expression) = match expr_result {
        Ok(result) => result,
        Err(nom::Err::Failure(e)) => {
            // Check if this looks like pattern comprehension syntax
            if input.contains("[(") && input.contains("|") && input.contains("->") {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Tag,
                )));
            }
            return Err(nom::Err::Failure(e));
        }
        Err(e) => return Err(e),
    };
    
    let (input, alias) = opt(preceded(ws(tag_no_case("AS")), ws(parse_identifier))).parse(input)?;

    let with_item = WithItem { expression, alias };
    Ok((input, with_item))
}

pub fn parse_with_clause(
    input: &'_ str,
) -> IResult<&'_ str, WithClause<'_>, OpenCypherParsingError<'_>> {
    // Parse the WITH keyword
    let (input, _) = ws(tag_no_case("WITH")).parse(input)?;

    // Parse optional DISTINCT modifier
    let (input, distinct) = opt(ws(tag_no_case("DISTINCT"))).parse(input)?;
    let distinct = distinct.is_some();

    let (input, with_items) = context(
        "Error in with clause",
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            cut(with_item_parser),
        ),
    )
    .parse(input)?;

    // Parse optional ORDER BY clause (part of WITH syntax per OpenCypher spec)
    let (input, order_by) = opt(parse_order_by_clause).parse(input)?;

    // Parse optional SKIP clause (part of WITH syntax per OpenCypher spec)
    let (input, skip) = opt(parse_skip_clause).parse(input)?;

    // Parse optional LIMIT clause (part of WITH syntax per OpenCypher spec)
    let (input, limit) = opt(parse_limit_clause).parse(input)?;

    // Parse optional WHERE clause (part of WITH syntax per OpenCypher spec)
    let (input, where_clause) = opt(parse_where_clause).parse(input)?;

    // Parse optional subsequent UNWIND clause after WITH
    // This handles: WITH d, rip UNWIND rip.ips AS ip ...
    let (input, subsequent_unwind) = opt(parse_unwind_clause).parse(input)?;

    // Parse optional subsequent MATCH clause after WITH
    // This handles: WITH u MATCH (u)-[:FOLLOWS]->(f) ...
    let (input, subsequent_match) = opt(parse_match_clause).parse(input)?;

    // Parse optional subsequent OPTIONAL MATCH clauses after WITH
    // This handles: WITH u OPTIONAL MATCH (u)-[:FOLLOWS]->(f) ...
    let (input, subsequent_optional_matches) = many0(parse_optional_match_clause).parse(input)?;

    // Parse optional subsequent WITH clause for chained WITH...MATCH...WITH patterns
    // This handles: WITH a MATCH ... WITH a, b MATCH ... RETURN ...
    // Using Box to handle recursion
    let (input, subsequent_with) = opt(parse_with_clause).parse(input)?;

    let with_clause = WithClause {
        with_items,
        distinct,
        order_by,
        skip,
        limit,
        where_clause,
        subsequent_unwind,
        subsequent_match: subsequent_match.map(Box::new),
        subsequent_optional_matches,
        subsequent_with: subsequent_with.map(Box::new),
    };

    Ok((input, with_clause))
}

fn with_item_parser(input: &str) -> IResult<&str, WithItem<'_>, OpenCypherParsingError<'_>> {
    parse_with_item(input).map_err(|e| match e {
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
    fn test_parse_with_item_no_alias() {
        let input = "a";
        let res = parse_with_item(input);
        match res {
            Ok((remaining, with_item)) => {
                assert_eq!(remaining, "");
                let expected = WithItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                };
                assert_eq!(&with_item, &expected);
            }
            Err(e) => panic!("Expected successful parse, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_with_item_with_alias() {
        let input = "a AS alias";
        let res = parse_with_item(input);
        match res {
            Ok((remaining, with_item)) => {
                assert_eq!(remaining, "");
                let expected = WithItem {
                    expression: Expression::Variable("a"),
                    alias: Some("alias"),
                };
                assert_eq!(&with_item, &expected);
            }
            Err(e) => panic!("Expected successful parse with alias, got error: {:?}", e),
        }
    }

    #[test]
    fn test_parse_with_clause_multiple_items() {
        let input = "WITH a, b AS aliasB, c";
        let res = parse_with_clause(input);
        match res {
            Ok((remaining, with_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(with_clause.with_items.len(), 3);
                let expected_item1 = WithItem {
                    expression: Expression::Variable("a"),
                    alias: None,
                };
                let expected_item2 = WithItem {
                    expression: Expression::Variable("b"),
                    alias: Some("aliasB"),
                };
                let expected_item3 = WithItem {
                    expression: Expression::Variable("c"),
                    alias: None,
                };
                assert_eq!(&with_clause.with_items[0], &expected_item1);
                assert_eq!(&with_clause.with_items[1], &expected_item2);
                assert_eq!(&with_clause.with_items[2], &expected_item3);
            }
            Err(e) => panic!(
                "Expected successful parse for multiple items, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_with_clause_extra_whitespace() {
        let input = "   WITH   a   AS  aliasA  ,   b  ,  c AS aliasC   ";
        let res = parse_with_clause(input);
        match res {
            Ok((remaining, with_clause)) => {
                assert_eq!(remaining, "");
                assert_eq!(with_clause.with_items.len(), 3);
                let expected_item1 = WithItem {
                    expression: Expression::Variable("a"),
                    alias: Some("aliasA"),
                };
                let expected_item2 = WithItem {
                    expression: Expression::Variable("b"),
                    alias: None,
                };
                let expected_item3 = WithItem {
                    expression: Expression::Variable("c"),
                    alias: Some("aliasC"),
                };
                assert_eq!(&with_clause.with_items[0], &expected_item1);
                assert_eq!(&with_clause.with_items[1], &expected_item2);
                assert_eq!(&with_clause.with_items[2], &expected_item3);
            }
            Err(e) => panic!(
                "Expected successful parse with extra whitespace, got error: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_parse_with_clause_missing_keyword() {
        let input = "MATCH a, b AS aliasB";
        let res = parse_with_clause(input);
        match res {
            Err(Err::Error(_)) | Err(Err::Failure(_)) => {
                // Expected failure since the input does not begin with "WITH".
            }
            Ok((remaining, clause)) => {
                panic!(
                    "Expected failure due to missing WITH keyword, but got remaining: {:?} clause: {:?}",
                    remaining, clause
                );
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
