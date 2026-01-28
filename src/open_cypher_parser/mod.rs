use ast::{
    CallClause, CreateClause, CypherStatement, DeleteClause, LimitClause, MatchClause,
    OpenCypherQueryAst, OptionalMatchClause, OrderByClause, ReadingClause, RemoveClause,
    ReturnClause, SetClause, SkipClause, UnionClause, UnionType, UnwindClause, UseClause,
    WhereClause, WithClause,
};
pub use common::strip_comments;
use common::ws;
use errors::OpenCypherParsingError;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::multispace0;
use nom::combinator::opt;
use nom::multi::many0;
use nom::{IResult, Parser};

pub mod ast;
mod call_clause;
mod common;
mod create_clause;
mod delete_clause;
pub(crate) mod errors;
mod expression;
mod limit_clause;
mod match_clause;
mod optional_match_clause;
mod order_by_and_page_clause;
mod order_by_clause;
mod path_pattern;
mod remove_clause;
mod return_clause;
mod set_clause;
mod skip_clause;
mod unwind_clause;
mod use_clause;
mod where_clause;
mod with_clause;

/// Parse a complete Cypher statement, potentially with UNION clauses
pub fn parse_cypher_statement(
    input: &'_ str,
) -> IResult<&'_ str, CypherStatement<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = multispace0.parse(input)?;

    // Parse the first query
    let (input, first_query) = parse_query_with_nom.parse(input)?;

    // Parse zero or more UNION clauses
    let (input, union_clauses) = many0(parse_union_clause).parse(input)?;

    // Optional trailing semicolon
    let (input, _) = opt(ws(tag(";"))).parse(input)?;

    Ok((
        input,
        CypherStatement {
            query: first_query,
            union_clauses,
        },
    ))
}

/// Parse a UNION clause: UNION [ALL] followed by a query
fn parse_union_clause(
    input: &'_ str,
) -> IResult<&'_ str, UnionClause<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = multispace0.parse(input)?;

    // Parse UNION keyword (case-insensitive)
    let (input, _) = tag_no_case("UNION").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse optional ALL keyword to determine union type
    let (input, union_type) = alt((
        |i| {
            let (i, _) = tag_no_case("ALL").parse(i)?;
            Ok((i, UnionType::All))
        },
        |i| Ok((i, UnionType::Distinct)),
    ))
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;

    // Parse the subsequent query
    let (input, query) = parse_query_with_nom.parse(input)?;

    Ok((input, UnionClause { union_type, query }))
}

/// Legacy function for backward compatibility - parses single query
pub fn parse_statement(
    input: &'_ str,
) -> IResult<&'_ str, OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>> {
    // Make semicolon optional - parse query with optional trailing semicolon
    let (input, query) = parse_query_with_nom.parse(input)?;
    let (input, _) = opt(ws(tag(";"))).parse(input)?;
    Ok((input, query))
}

/// Parse either a MATCH or OPTIONAL MATCH clause
fn parse_reading_clause(input: &str) -> IResult<&str, ReadingClause<'_>, OpenCypherParsingError<'_>> {
    // Try OPTIONAL MATCH first (since it starts with a longer keyword)
    if let Ok((remaining, optional_match)) =
        optional_match_clause::parse_optional_match_clause(input)
    {
        return Ok((remaining, ReadingClause::OptionalMatch(optional_match)));
    }

    // Try regular MATCH
    if let Ok((remaining, match_clause)) = match_clause::parse_match_clause(input) {
        return Ok((remaining, ReadingClause::Match(match_clause)));
    }

    // Neither worked - return an error that doesn't match
    Err(nom::Err::Error(OpenCypherParsingError {
        errors: vec![(input, "Expected MATCH or OPTIONAL MATCH clause")],
    }))
}

pub fn parse_query_with_nom(
    input: &'_ str,
) -> IResult<&'_ str, OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = multispace0.parse(input)?;

    // Parse USE clause first (must come before any other clauses)
    let (input, use_clause): (&str, Option<UseClause>) =
        opt(use_clause::parse_use_clause).parse(input)?;

    // Parse reading clauses (MATCH and OPTIONAL MATCH can appear in any order)
    let (input, reading_clauses): (&str, Vec<ReadingClause>) =
        many0(parse_reading_clause).parse(input)?;

    // Separate into match_clauses and optional_match_clauses for backward compatibility
    let mut match_clauses: Vec<MatchClause> = Vec::new();
    let mut optional_match_clauses: Vec<OptionalMatchClause> = Vec::new();

    for clause in &reading_clauses {
        match clause {
            ReadingClause::Match(m) => match_clauses.push(m.clone()),
            ReadingClause::OptionalMatch(o) => optional_match_clauses.push(o.clone()),
        }
    }

    // Parse WHERE clause that comes after all MATCH clauses
    // (Note: Per-MATCH WHERE clauses are handled within each MatchClause)
    let (input, where_clause): (&str, Option<WhereClause>) =
        opt(where_clause::parse_where_clause).parse(input)?;

    let (input, call_clause): (&str, Option<CallClause>) =
        opt(call_clause::parse_call_clause).parse(input)?;

    // Parse UNWIND clauses (can appear after MATCH/OPTIONAL MATCH, before WITH/RETURN)
    // Supports multiple consecutive UNWIND for cartesian product
    // Example: MATCH (n) UNWIND n.items AS item RETURN item
    // Example: UNWIND [1,2] AS x UNWIND [10,20] AS y RETURN x, y
    let (input, unwind_clauses): (&str, Vec<UnwindClause>) =
        many0(unwind_clause::parse_unwind_clause).parse(input)?;

    let (input, with_clause): (&str, Option<WithClause>) =
        opt(with_clause::parse_with_clause).parse(input)?;

    // Parse WHERE clause again after WITH (can filter WITH results)
    // If present, this will override any earlier WHERE clause
    let (input, where_clause_after_with): (&str, Option<WhereClause>) =
        opt(where_clause::parse_where_clause).parse(input)?;
    let where_clause = if where_clause_after_with.is_some() {
        where_clause_after_with
    } else {
        where_clause
    };

    let (input, create_clause): (&str, Option<CreateClause>) =
        opt(create_clause::parse_create_clause).parse(input)?;
    let (input, set_clause): (&str, Option<SetClause>) =
        opt(set_clause::parse_set_clause).parse(input)?;
    let (input, remove_clause): (&str, Option<RemoveClause>) =
        opt(remove_clause::parse_remove_clause).parse(input)?;
    let (input, delete_clause): (&str, Option<DeleteClause>) =
        opt(delete_clause::parse_delete_clause).parse(input)?;
    let (input, return_clause): (&str, Option<ReturnClause>) =
        opt(return_clause::parse_return_clause).parse(input)?;

    // Parse ORDER BY and pagination clauses (unified to support flexible ordering)
    let (input, page_clause) =
        opt(order_by_and_page_clause::parse_order_by_and_page_clause).parse(input)?;
    let (order_by_clause, skip_clause, limit_clause) = if let Some(clause) = page_clause {
        (clause.order_by, clause.skip, clause.limit)
    } else {
        (None, None, None)
    };

    let cypher_query = OpenCypherQueryAst {
        use_clause,
        match_clauses,
        optional_match_clauses,
        reading_clauses,
        call_clause,
        unwind_clauses,
        with_clause,
        where_clause,
        create_clause,
        set_clause,
        remove_clause,
        delete_clause,
        return_clause,
        order_by_clause,
        skip_clause,
        limit_clause,
    };

    Ok((input, cypher_query))
}

pub fn parse_query(input: &'_ str) -> Result<OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>> {
    match parse_statement(input) {
        Ok((remainder, query_ast)) => {
            // Check that all input was consumed (remainder should be empty or whitespace only)
            let trimmed = remainder.trim();
            if !trimmed.is_empty() {
                return Err(OpenCypherParsingError {
                    errors: vec![
                        (remainder, "Unexpected tokens after query"),
                        (trimmed, "Unparsed input"),
                    ],
                });
            }
            Ok(query_ast)
        }
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(e),
        Err(nom::Err::Incomplete(_)) => Err(OpenCypherParsingError {
            errors: vec![("", "")],
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::open_cypher_parser::ast::{
        ConnectedPattern, Direction, Expression, FunctionCall, Literal, NodePattern, Operator,
        OperatorApplication, OrderByItem, OrerByOrder, PathPattern, Property, PropertyAccess,
        PropertyKVPair, RelationshipPattern, ReturnItem, WithItem,
    };

    use super::*;

    #[test]
    fn test_parse_full_query() {
        let query = "
            MATCH (a)
            WITH a
            WHERE a = 1
            CREATE (b)
            SET b.name = 'John', b.age = 30
            REMOVE b.temp
            DELETE a
            RETURN a, b.name AS name
            ORDER BY a ASC, b DESC
            SKIP 5
            LIMIT 10 ;";
        let parsed = parse_query(query);
        match parsed {
            Ok(ast) => {
                // Ensure each clause is present.
                assert!(!ast.match_clauses.is_empty(), "Expected MATCH clause");
                assert!(ast.with_clause.is_some(), "Expected WITH clause");
                // WHERE after WITH is now part of WITH clause, not query-level
                assert!(
                    ast.where_clause.is_none(),
                    "WHERE should be part of WITH clause, not query level"
                );
                assert!(ast.create_clause.is_some(), "Expected CREATE clause");
                assert!(ast.set_clause.is_some(), "Expected SET clause");
                assert!(ast.remove_clause.is_some(), "Expected REMOVE clause");
                assert!(ast.delete_clause.is_some(), "Expected DELETE clause");
                assert!(ast.return_clause.is_some(), "Expected RETURN clause");
                assert!(ast.order_by_clause.is_some(), "Expected ORDER BY clause");
                assert!(ast.skip_clause.is_some(), "Expected SKIP clause");
                assert!(ast.limit_clause.is_some(), "Expected LIMIT clause");

                let match_clause = &ast.match_clauses[0];

                if let (_, PathPattern::Node(node)) = &match_clause.path_patterns[0] {
                    assert_eq!(node.name, Some("a"));
                } else {
                    panic!("Expected MATCH clause to contain a Node pattern");
                }

                let with_clause = ast.with_clause.unwrap();
                assert_eq!(with_clause.with_items.len(), 1);
                let with_item = &with_clause.with_items[0];
                assert_eq!(with_item.expression, Expression::Variable("a"));
                assert_eq!(with_item.alias, None);

                // Check WHERE is now part of WITH clause
                assert!(
                    with_clause.where_clause.is_some(),
                    "Expected WHERE clause inside WITH"
                );
                let where_clause = with_clause.where_clause.unwrap();

                if let Expression::OperatorApplicationExp(operator_application) =
                    where_clause.conditions
                {
                    assert_eq!(operator_application.operator, Operator::Equal);
                    assert_eq!(operator_application.operands.len(), 2);
                    assert_eq!(operator_application.operands[0], Expression::Variable("a"));
                    assert_eq!(
                        operator_application.operands[1],
                        Expression::Literal(Literal::Integer(1))
                    );
                } else {
                    panic!("Expected Where clause to contain a Expression::OperatorApplicationExp");
                }

                let create_clause = ast.create_clause.unwrap();
                if let PathPattern::Node(node) = &create_clause.path_patterns[0] {
                    assert_eq!(node.name, Some("b"));
                } else {
                    panic!("Expected CREATE clause to contain a Node pattern");
                }

                let set_clause = ast.set_clause.unwrap();
                assert_eq!(set_clause.set_items.len(), 2);

                assert_eq!(set_clause.set_items[0].operator, Operator::Equal);
                if let Expression::PropertyAccessExp(prop) = &set_clause.set_items[0].operands[0] {
                    assert_eq!(prop.base, "b");
                    assert_eq!(prop.key, "name");
                } else {
                    panic!("Expected first operand of SET item to be a property access");
                }
                assert_eq!(
                    set_clause.set_items[0].operands[1],
                    Expression::Literal(Literal::String("John"))
                );

                assert_eq!(set_clause.set_items[1].operator, Operator::Equal);
                if let Expression::PropertyAccessExp(prop) = &set_clause.set_items[1].operands[0] {
                    assert_eq!(prop.base, "b");
                    assert_eq!(prop.key, "age");
                } else {
                    panic!("Expected first operand of second SET item to be a property access");
                }
                assert_eq!(
                    set_clause.set_items[1].operands[1],
                    Expression::Literal(Literal::Integer(30))
                );

                let remove_clause = ast.remove_clause.unwrap();
                assert_eq!(remove_clause.remove_items.len(), 1);
                let remove_item = &remove_clause.remove_items[0];
                assert_eq!(remove_item.base, "b");
                assert_eq!(remove_item.key, "temp");

                let delete_clause = ast.delete_clause.unwrap();
                assert_eq!(delete_clause.is_detach, false);
                assert_eq!(delete_clause.delete_items.len(), 1);
                assert_eq!(delete_clause.delete_items[0], Expression::Variable("a"));

                let return_clause = ast.return_clause.unwrap();
                assert_eq!(return_clause.return_items.len(), 2);
                let return_item1 = &return_clause.return_items[0];
                assert_eq!(return_item1.expression, Expression::Variable("a"));
                assert_eq!(return_item1.alias, None);
                let return_item2 = &return_clause.return_items[1];
                if let Expression::PropertyAccessExp(prop) = &return_item2.expression {
                    assert_eq!(prop.base, "b");
                    assert_eq!(prop.key, "name");
                } else {
                    panic!("Expected second RETURN item to be a property access");
                }
                assert_eq!(return_item2.alias, Some("name"));

                let order_by_clause = ast.order_by_clause.unwrap();
                assert_eq!(order_by_clause.order_by_items.len(), 2);
                let order_item1 = &order_by_clause.order_by_items[0];
                assert_eq!(order_item1.expression, Expression::Variable("a"));
                assert_eq!(order_item1.order, OrerByOrder::Asc);
                let order_item2 = &order_by_clause.order_by_items[1];
                assert_eq!(order_item2.expression, Expression::Variable("b"));
                assert_eq!(order_item2.order, OrerByOrder::Desc);

                let skip_clause = ast.skip_clause.unwrap();
                assert_eq!(skip_clause.skip_item, 5);

                let limit_clause = ast.limit_clause.unwrap();
                assert_eq!(limit_clause.limit_item, 10);
            }
            Err(e) => panic!("Full query parsing failed: {:?}", e),
        }
    }

    #[test]
    fn test_parse_partial_query() {
        let query = "MATCH (a) WHERE a = 1 RETURN a;";
        let parsed = parse_query(query);
        match parsed {
            Ok(ast) => {
                // These clauses should be present.
                assert!(!ast.match_clauses.is_empty(), "Expected MATCH clause");
                // WHERE clause is now part of MATCH clause per OpenCypher spec
                assert!(
                    ast.match_clauses[0].where_clause.is_some(),
                    "Expected WHERE clause in MATCH clause"
                );
                assert!(ast.return_clause.is_some(), "Expected RETURN clause");
                // The rest should be None.
                assert!(ast.with_clause.is_none(), "Expected WITH clause to be None");
                assert!(
                    ast.create_clause.is_none(),
                    "Expected CREATE clause to be None"
                );
                assert!(ast.set_clause.is_none(), "Expected SET clause to be None");
                assert!(
                    ast.remove_clause.is_none(),
                    "Expected REMOVE clause to be None"
                );
                assert!(
                    ast.delete_clause.is_none(),
                    "Expected DELETE clause to be None"
                );
                assert!(
                    ast.order_by_clause.is_none(),
                    "Expected ORDER BY clause to be None"
                );
                assert!(ast.skip_clause.is_none(), "Expected SKIP clause to be None");
                assert!(
                    ast.limit_clause.is_none(),
                    "Expected LIMIT clause to be None"
                );
            }
            Err(e) => panic!("Partial query parsing failed: {:?}", e),
        }
    }

    #[test]
    fn test_parse_where_with_pattern_comprehension_return() {
        // This test case specifically tests WHERE followed by RETURN with pattern comprehension
        let query =
            "MATCH (p:Person) WHERE true RETURN [(p)-[:KNOWS]->(f) | f.firstName] AS friends";
        let parsed = parse_query(query);
        match parsed {
            Ok(ast) => {
                assert!(!ast.match_clauses.is_empty(), "Expected MATCH clause");
                // WHERE clause is now part of MATCH clause per OpenCypher spec
                assert!(
                    ast.match_clauses[0].where_clause.is_some(),
                    "Expected WHERE clause in MATCH clause"
                );
                assert!(ast.return_clause.is_some(), "Expected RETURN clause");
            }
            Err(e) => panic!("Pattern comprehension query parsing failed: {:?}", e),
        }
    }

    #[test]
    fn test_parse_full_read_query() {
        let input = "
        MATCH (david {name: 'David'})-[]-(otherPerson)-[]->(b)
        WITH otherPerson, count(*) AS foaf
        WHERE foaf > 1 and (fof is not null or a + b)
        RETURN otherPerson.name AS otherName
        ORDER BY otherPerson.name DESC
        Skip 10
        LIMIT 20;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];

        let expected_match_clause = MatchClause {
            path_patterns: vec![(
                None,
                PathPattern::ConnectedPattern(vec![
                    ConnectedPattern {
                        start_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("david"),
                            labels: None,
                            properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                                key: "name",
                                value: Expression::Literal(Literal::String("David")),
                            })]),
                        })),
                        relationship: RelationshipPattern {
                            name: None,
                            direction: Direction::Either,
                            labels: None,
                            properties: None,
                            variable_length: None,
                        },
                        end_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("otherPerson"),
                            labels: None,
                            properties: None,
                        })),
                    },
                    ConnectedPattern {
                        start_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("otherPerson"),
                            labels: None,
                            properties: None,
                        })),
                        relationship: RelationshipPattern {
                            name: None,
                            direction: Direction::Outgoing,
                            variable_length: None,
                            labels: None,
                            properties: None,
                        },
                        end_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("b"),
                            labels: None,
                            properties: None,
                        })),
                    },
                ]),
            )],
            where_clause: None,
        };

        assert_eq!(*match_clause, expected_match_clause);

        assert!(query_ast.with_clause.is_some(), "Expected WITH clause");
        let with_clause = query_ast.with_clause.unwrap();

        let expected_with_clause = WithClause {
            with_items: vec![
                WithItem {
                    expression: Expression::Variable("otherPerson"),
                    alias: None,
                },
                WithItem {
                    expression: Expression::FunctionCallExp(FunctionCall {
                        name: "count".to_string(),
                        args: vec![Expression::Variable("*")],
                    }),
                    alias: Some("foaf"),
                },
            ],
            subsequent_unwind: None,
            subsequent_match: None,
            subsequent_optional_matches: vec![],
            subsequent_with: None,
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            // WHERE after WITH items is now part of WITH clause per OpenCypher spec
            where_clause: Some(WhereClause {
                conditions: Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::GreaterThan,
                            operands: vec![
                                Expression::Variable("foaf"),
                                Expression::Literal(Literal::Integer(1)),
                            ],
                        }),
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Or,
                            operands: vec![
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::IsNotNull,
                                    operands: vec![Expression::Variable("fof")],
                                }),
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::Addition,
                                    operands: vec![
                                        Expression::Variable("a"),
                                        Expression::Variable("b"),
                                    ],
                                }),
                            ],
                        }),
                    ],
                }),
            }),
        };
        assert_eq!(with_clause, expected_with_clause);

        // WHERE is now parsed as part of WITH clause, so query-level where_clause should be None
        assert!(
            query_ast.where_clause.is_none(),
            "WHERE should be part of WITH clause, not query level"
        );

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            distinct: false,
            return_items: vec![ReturnItem {
                expression: Expression::PropertyAccessExp(PropertyAccess {
                    base: "otherPerson",
                    key: "name",
                }),
                alias: Some("otherName"),
                original_text: None,
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.order_by_clause.is_some(),
            "Expected ORDER BY clause"
        );
        let order_by_clause = query_ast.order_by_clause.unwrap();
        let expected_order_by_clause = OrderByClause {
            order_by_items: vec![OrderByItem {
                expression: Expression::PropertyAccessExp(PropertyAccess {
                    base: "otherPerson",
                    key: "name",
                }),
                order: OrerByOrder::Desc,
            }],
        };
        assert_eq!(order_by_clause, expected_order_by_clause);

        assert!(query_ast.skip_clause.is_some(), "Expected SKIP clause");
        let skip_clause = query_ast.skip_clause.unwrap();
        let expected_skip_clause = SkipClause { skip_item: 10 };
        assert_eq!(skip_clause, expected_skip_clause);

        assert!(query_ast.limit_clause.is_some(), "Expected LIMIT clause");
        let limit_clause = query_ast.limit_clause.unwrap();
        let expected_limit_clause = LimitClause { limit_item: 20 };
        assert_eq!(limit_clause, expected_limit_clause);

        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
    }

    #[test]
    fn test_parse_full_read_query_person_movie() {
        let input = "
            MATCH (p:Person {name: 'Tom Hardy' })-[r:ACTED_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person)
            WHERE p Is not null and movie.name = 'Batman'
            RETURN p as tom_hardy, movie.name AS movieName, (a)-[]->(c)
        ;";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];
        let expected_match_clause = MatchClause {
            path_patterns: vec![(
                None,
                PathPattern::ConnectedPattern(vec![
                    // (p:Person {name: 'Tom Hardy'})-[r:ACTED_IN]->(movie:Movie)
                    ConnectedPattern {
                        start_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("p"),
                            labels: Some(vec!["Person"]),
                            properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                                key: "name",
                                value: Expression::Literal(Literal::String("Tom Hardy")),
                            })]),
                        })),
                        relationship: RelationshipPattern {
                            name: Some("r"),
                            direction: Direction::Outgoing,
                            variable_length: None,
                            labels: Some(vec!["ACTED_IN"]),
                            properties: None,
                        },
                        end_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("movie"),
                            labels: Some(vec!["Movie"]),
                            properties: None,
                        })),
                    },
                    // (movie:Movie)<-[:DIRECTED]-(director:Person)
                    ConnectedPattern {
                        start_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("movie"),
                            labels: Some(vec!["Movie"]),
                            properties: None,
                        })),
                        relationship: RelationshipPattern {
                            name: None,
                            direction: Direction::Incoming,
                            variable_length: None,
                            labels: Some(vec!["DIRECTED"]),
                            properties: None,
                        },
                        end_node: Rc::new(RefCell::new(NodePattern {
                            name: Some("director"),
                            labels: Some(vec!["Person"]),
                            properties: None,
                        })),
                    },
                ]),
            )],
            where_clause: Some(WhereClause {
                conditions: Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![
                        // p IS NOT NULL
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::IsNotNull,
                            operands: vec![Expression::Variable("p")],
                        }),
                        // movie.name = 'Batman'
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                Expression::PropertyAccessExp(PropertyAccess {
                                    base: "movie",
                                    key: "name",
                                }),
                                Expression::Literal(Literal::String("Batman")),
                            ],
                        }),
                    ],
                }),
            }),
        };
        assert_eq!(*match_clause, expected_match_clause);

        // WHERE clause is now part of MATCH clause per OpenCypher grammar
        // assert!(query_ast.where_clause.is_some(), "Expected WHERE clause");

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            distinct: false,
            return_items: vec![
                // p as tom_hardy
                ReturnItem {
                    expression: Expression::Variable("p"),
                    alias: Some("tom_hardy"),
                    original_text: None,
                },
                // movie.name AS movieName
                ReturnItem {
                    expression: Expression::PropertyAccessExp(PropertyAccess {
                        base: "movie",
                        key: "name",
                    }),
                    alias: Some("movieName"),
                    original_text: None,
                },
                // (a)-[]->(c)
                ReturnItem {
                    expression: Expression::PathPattern(PathPattern::ConnectedPattern(vec![
                        ConnectedPattern {
                            start_node: Rc::new(RefCell::new(NodePattern {
                                name: Some("a"),
                                labels: None,
                                properties: None,
                            })),
                            relationship: RelationshipPattern {
                                name: None,
                                direction: Direction::Outgoing,
                                variable_length: None,
                                labels: None,
                                properties: None,
                            },
                            end_node: Rc::new(RefCell::new(NodePattern {
                                name: Some("c"),
                                labels: None,
                                properties: None,
                            })),
                        },
                    ])),
                    alias: None,
                    original_text: Some("(a)-[]->(c)"),
                },
            ],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_create_query() {
        let input = "
            MATCH (a:Person), (b:Person)
            WHERE a.name = 'Node A' AND b.name = 'Node B'
            CREATE (a)-[r:RELTYPE {name: a.name }]->(b)
            RETURN r;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];
        let expected_match_clause = MatchClause {
            path_patterns: vec![
                (
                    None,
                    PathPattern::Node(NodePattern {
                        name: Some("a"),
                        labels: Some(vec!["Person"]),
                        properties: None,
                    }),
                ),
                (
                    None,
                    PathPattern::Node(NodePattern {
                        name: Some("b"),
                        labels: Some(vec!["Person"]),
                        properties: None,
                    }),
                ),
            ],
            where_clause: Some(WhereClause {
                conditions: Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                Expression::PropertyAccessExp(PropertyAccess {
                                    base: "a",
                                    key: "name",
                                }),
                                Expression::Literal(Literal::String("Node A")),
                            ],
                        }),
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                Expression::PropertyAccessExp(PropertyAccess {
                                    base: "b",
                                    key: "name",
                                }),
                                Expression::Literal(Literal::String("Node B")),
                            ],
                        }),
                    ],
                }),
            }),
        };
        assert_eq!(*match_clause, expected_match_clause);

        // WHERE clause is now part of MATCH clause per OpenCypher spec
        // assert!(query_ast.where_clause.is_some(), "Expected WHERE clause");

        assert!(query_ast.create_clause.is_some(), "Expected CREATE clause");
        let create_clause = query_ast.create_clause.unwrap();
        let expected_create_clause = CreateClause {
            path_patterns: vec![PathPattern::ConnectedPattern(vec![ConnectedPattern {
                start_node: Rc::new(RefCell::new(NodePattern {
                    name: Some("a"),
                    labels: None,
                    properties: None,
                })),
                relationship: RelationshipPattern {
                    name: Some("r"),
                    direction: Direction::Outgoing,
                    variable_length: None,
                    labels: Some(vec!["RELTYPE"]),
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "name",
                        value: Expression::PropertyAccessExp(PropertyAccess {
                            base: "a",
                            key: "name",
                        }),
                    })]),
                },
                end_node: Rc::new(RefCell::new(NodePattern {
                    name: Some("b"),
                    labels: None,
                    properties: None,
                })),
            }])],
        };
        assert_eq!(create_clause, expected_create_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            distinct: false,
            return_items: vec![ReturnItem {
                expression: Expression::Variable("r"),
                alias: None,
                original_text: Some("r"),
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_set_query() {
        let input = "
            MATCH (n {name: 'Andres'})
            SET n = $props, n.rage = 'blah'
            RETURN n;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];
        let expected_match_clause = MatchClause {
            path_patterns: vec![(
                None,
                PathPattern::Node(NodePattern {
                    name: Some("n"),
                    labels: None,
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "name",
                        value: Expression::Literal(Literal::String("Andres")),
                    })]),
                }),
            )],
            where_clause: None,
        };
        assert_eq!(*match_clause, expected_match_clause);

        assert!(query_ast.set_clause.is_some(), "Expected SET clause");
        let set_clause = query_ast.set_clause.unwrap();
        assert_eq!(set_clause.set_items.len(), 2, "Expected two SET items");

        let expected_set_item1 = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![Expression::Variable("n"), Expression::Parameter("props")],
        };

        let expected_set_item2 = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "n",
                    key: "rage",
                }),
                Expression::Literal(Literal::String("blah")),
            ],
        };
        assert_eq!(set_clause.set_items[0], expected_set_item1);
        assert_eq!(set_clause.set_items[1], expected_set_item2);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            distinct: false,
            return_items: vec![ReturnItem {
                expression: Expression::Variable("n"),
                alias: None,
                original_text: Some("n"),
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.where_clause.is_none(),
            "Expected WHERE clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_delete_query() {
        let input = "
            MATCH (n {name: 'Andres'})
            DETACH DELETE n;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];
        let expected_match_clause = MatchClause {
            path_patterns: vec![(
                None,
                PathPattern::Node(NodePattern {
                    name: Some("n"),
                    labels: None,
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "name",
                        value: Expression::Literal(Literal::String("Andres")),
                    })]),
                }),
            )],
            where_clause: None,
        };
        assert_eq!(*match_clause, expected_match_clause);

        assert!(query_ast.delete_clause.is_some(), "Expected DELETE clause");
        let delete_clause = query_ast.delete_clause.unwrap();
        let expected_delete_clause = DeleteClause {
            is_detach: true,
            delete_items: vec![Expression::Variable("n")],
        };
        assert_eq!(delete_clause, expected_delete_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.where_clause.is_none(),
            "Expected WHERE clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.return_clause.is_none(),
            "Expected RETURN clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_remove_query() {
        let input = "
            MATCH (andres {name: 'Andres'})
            REMOVE andres.age, andres.address
            RETURN andres;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];
        let expected_match_clause = MatchClause {
            path_patterns: vec![(
                None,
                PathPattern::Node(NodePattern {
                    name: Some("andres"),
                    labels: None,
                    properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                        key: "name",
                        value: Expression::Literal(Literal::String("Andres")),
                    })]),
                }),
            )],
            where_clause: None,
        };
        assert_eq!(*match_clause, expected_match_clause);

        assert!(query_ast.remove_clause.is_some(), "Expected REMOVE clause");
        let remove_clause = query_ast.remove_clause.unwrap();
        let expected_remove_clause = RemoveClause {
            remove_items: vec![
                PropertyAccess {
                    base: "andres",
                    key: "age",
                },
                PropertyAccess {
                    base: "andres",
                    key: "address",
                },
            ],
        };
        assert_eq!(remove_clause, expected_remove_clause);

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            distinct: false,
            return_items: vec![ReturnItem {
                expression: Expression::Variable("andres"),
                alias: None,
                original_text: Some("andres"),
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.where_clause.is_none(),
            "Expected WHERE clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_logical_operators_query() {
        let input = "
            MATCH (p:Person)
            WHERE p.name IN ['Alice', 'Bob'] AND (p.age > 30 OR p.age < 20)
            RETURN p;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];
        let expected_match_clause = MatchClause {
            path_patterns: vec![(
                None,
                PathPattern::Node(NodePattern {
                    name: Some("p"),
                    labels: Some(vec!["Person"]),
                    properties: None,
                }),
            )],
            where_clause: Some(WhereClause {
                conditions: Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![
                        // Left operand: p.name IN ['Alice', 'Bob']
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::In,
                            operands: vec![
                                Expression::PropertyAccessExp(PropertyAccess {
                                    base: "p",
                                    key: "name",
                                }),
                                Expression::List(vec![
                                    Expression::Literal(Literal::String("Alice")),
                                    Expression::Literal(Literal::String("Bob")),
                                ]),
                            ],
                        }),
                        // Right operand: (p.age > 30 OR p.age < 20)
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Or,
                            operands: vec![
                                // p.age > 30
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::GreaterThan,
                                    operands: vec![
                                        Expression::PropertyAccessExp(PropertyAccess {
                                            base: "p",
                                            key: "age",
                                        }),
                                        Expression::Literal(Literal::Integer(30)),
                                    ],
                                }),
                                // p.age < 20
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::LessThan,
                                    operands: vec![
                                        Expression::PropertyAccessExp(PropertyAccess {
                                            base: "p",
                                            key: "age",
                                        }),
                                        Expression::Literal(Literal::Integer(20)),
                                    ],
                                }),
                            ],
                        }),
                    ],
                }),
            }),
        };
        assert_eq!(*match_clause, expected_match_clause);

        // WHERE clause is now part of MATCH clause per OpenCypher spec

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            distinct: false,
            return_items: vec![ReturnItem {
                expression: Expression::Variable("p"),
                alias: None,
                original_text: Some("p"),
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    #[test]
    fn test_parse_full_query_with_and_or_in_not_in() {
        let input = "
            MATCH (p:Person)
            WHERE p.name IN ['Alice', 'Bob'] AND
                  p.city NOT IN ['Chicago', 'Miami'] AND
                  (p.age > 30 OR p.age < 20)
            RETURN p;
        ";

        let query_ast = parse_query(input).expect("Query parsing failed");

        // --- MATCH clause ---
        assert!(!query_ast.match_clauses.is_empty(), "Expected MATCH clause");
        let match_clause = &query_ast.match_clauses[0];
        let expected_match_clause = MatchClause {
            path_patterns: vec![(
                None,
                PathPattern::Node(NodePattern {
                    name: Some("p"),
                    labels: Some(vec!["Person"]),
                    properties: None,
                }),
            )],
            where_clause: Some(WhereClause {
                conditions: Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::In,
                                    operands: vec![
                                        Expression::PropertyAccessExp(PropertyAccess {
                                            base: "p",
                                            key: "name",
                                        }),
                                        Expression::List(vec![
                                            Expression::Literal(Literal::String("Alice")),
                                            Expression::Literal(Literal::String("Bob")),
                                        ]),
                                    ],
                                }),
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::NotIn,
                                    operands: vec![
                                        Expression::PropertyAccessExp(PropertyAccess {
                                            base: "p",
                                            key: "city",
                                        }),
                                        Expression::List(vec![
                                            Expression::Literal(Literal::String("Chicago")),
                                            Expression::Literal(Literal::String("Miami")),
                                        ]),
                                    ],
                                }),
                            ],
                        }),
                        Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Or,
                            operands: vec![
                                // p.age > 30
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::GreaterThan,
                                    operands: vec![
                                        Expression::PropertyAccessExp(PropertyAccess {
                                            base: "p",
                                            key: "age",
                                        }),
                                        Expression::Literal(Literal::Integer(30)),
                                    ],
                                }),
                                // p.age < 20
                                Expression::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::LessThan,
                                    operands: vec![
                                        Expression::PropertyAccessExp(PropertyAccess {
                                            base: "p",
                                            key: "age",
                                        }),
                                        Expression::Literal(Literal::Integer(20)),
                                    ],
                                }),
                            ],
                        }),
                    ],
                }),
            }),
        };
        assert_eq!(*match_clause, expected_match_clause);

        // WHERE clause is now part of MATCH clause per OpenCypher spec

        assert!(query_ast.return_clause.is_some(), "Expected RETURN clause");
        let return_clause = query_ast.return_clause.unwrap();
        let expected_return_clause = ReturnClause {
            distinct: false,
            return_items: vec![ReturnItem {
                expression: Expression::Variable("p"),
                alias: None,
                original_text: Some("p"),
            }],
        };
        assert_eq!(return_clause, expected_return_clause);

        assert!(
            query_ast.with_clause.is_none(),
            "Expected WITH clause to be None"
        );
        assert!(
            query_ast.create_clause.is_none(),
            "Expected CREATE clause to be None"
        );
        assert!(
            query_ast.set_clause.is_none(),
            "Expected SET clause to be None"
        );
        assert!(
            query_ast.remove_clause.is_none(),
            "Expected REMOVE clause to be None"
        );
        assert!(
            query_ast.delete_clause.is_none(),
            "Expected DELETE clause to be None"
        );
        assert!(
            query_ast.order_by_clause.is_none(),
            "Expected ORDER BY clause to be None"
        );
        assert!(
            query_ast.skip_clause.is_none(),
            "Expected SKIP clause to be None"
        );
        assert!(
            query_ast.limit_clause.is_none(),
            "Expected LIMIT clause to be None"
        );
    }

    // ==================== UNION PARSING TESTS ====================

    #[test]
    fn test_parse_cypher_statement_single_query() {
        // A single query without UNION should have empty union_clauses
        let query = "MATCH (n:Person) RETURN n.name";
        let result = parse_cypher_statement(query);
        assert!(
            result.is_ok(),
            "Failed to parse single query: {:?}",
            result.err()
        );

        let (remaining, stmt) = result.unwrap();
        assert!(
            remaining.trim().is_empty(),
            "Expected empty remaining, got: '{}'",
            remaining
        );
        assert!(
            stmt.union_clauses.is_empty(),
            "Expected no UNION clauses for single query"
        );
        assert!(
            !stmt.query.match_clauses.is_empty(),
            "Expected MATCH clause"
        );
    }

    #[test]
    fn test_parse_cypher_statement_union() {
        // UNION (distinct) combines results removing duplicates
        let query = "MATCH (a:Person) RETURN a.name UNION MATCH (b:Company) RETURN b.name";
        let result = parse_cypher_statement(query);
        assert!(
            result.is_ok(),
            "Failed to parse UNION query: {:?}",
            result.err()
        );

        let (remaining, stmt) = result.unwrap();
        assert!(
            remaining.trim().is_empty(),
            "Expected empty remaining, got: '{}'",
            remaining
        );
        assert_eq!(stmt.union_clauses.len(), 1, "Expected 1 UNION clause");
        assert_eq!(stmt.union_clauses[0].union_type, UnionType::Distinct);

        // Verify first query
        assert!(!stmt.query.match_clauses.is_empty());
        // Verify union query
        assert!(!stmt.union_clauses[0].query.match_clauses.is_empty());
    }

    #[test]
    fn test_parse_cypher_statement_union_all() {
        // UNION ALL combines results keeping duplicates
        let query = "MATCH (a:Person) RETURN a.name UNION ALL MATCH (b:Company) RETURN b.name";
        let result = parse_cypher_statement(query);
        assert!(
            result.is_ok(),
            "Failed to parse UNION ALL query: {:?}",
            result.err()
        );

        let (remaining, stmt) = result.unwrap();
        assert!(
            remaining.trim().is_empty(),
            "Expected empty remaining, got: '{}'",
            remaining
        );
        assert_eq!(stmt.union_clauses.len(), 1, "Expected 1 UNION clause");
        assert_eq!(stmt.union_clauses[0].union_type, UnionType::All);
    }

    #[test]
    fn test_parse_cypher_statement_multiple_unions() {
        // Multiple UNION clauses
        let query = "MATCH (a:Person) RETURN a.name UNION MATCH (b:Company) RETURN b.name UNION ALL MATCH (c:City) RETURN c.name";
        let result = parse_cypher_statement(query);
        assert!(
            result.is_ok(),
            "Failed to parse multiple UNION query: {:?}",
            result.err()
        );

        let (remaining, stmt) = result.unwrap();
        assert!(
            remaining.trim().is_empty(),
            "Expected empty remaining, got: '{}'",
            remaining
        );
        assert_eq!(stmt.union_clauses.len(), 2, "Expected 2 UNION clauses");
        assert_eq!(stmt.union_clauses[0].union_type, UnionType::Distinct);
        assert_eq!(stmt.union_clauses[1].union_type, UnionType::All);
    }

    #[test]
    fn test_parse_cypher_statement_case_insensitive() {
        // UNION keywords should be case insensitive
        let query = "MATCH (a) RETURN a union all MATCH (b) RETURN b";
        let result = parse_cypher_statement(query);
        assert!(
            result.is_ok(),
            "Failed to parse lowercase UNION: {:?}",
            result.err()
        );

        let (_, stmt) = result.unwrap();
        assert_eq!(stmt.union_clauses.len(), 1);
        assert_eq!(stmt.union_clauses[0].union_type, UnionType::All);
    }

    #[test]
    fn test_parse_cypher_statement_with_semicolon() {
        // Trailing semicolon should be handled
        let query = "MATCH (a) RETURN a UNION MATCH (b) RETURN b;";
        let result = parse_cypher_statement(query);
        assert!(
            result.is_ok(),
            "Failed to parse UNION with semicolon: {:?}",
            result.err()
        );

        let (remaining, stmt) = result.unwrap();
        assert!(
            remaining.trim().is_empty(),
            "Expected empty remaining after semicolon"
        );
        assert_eq!(stmt.union_clauses.len(), 1);
    }

    #[test]
    fn test_parse_full_query_with_pattern_comprehension() {
        let query = "MATCH (p:Person) WHERE p.id = 1 RETURN [(p)-[:KNOWS]->(f) | f.firstName]";
        let result = parse_query(query);
        match result {
            Ok(ast) => {
                println!("Full query AST parsed successfully!");
                println!("Match clauses: {:?}", ast.match_clauses.len());
                println!("Return clause: {:?}", ast.return_clause.is_some());

                assert_eq!(ast.match_clauses.len(), 1, "Expected 1 MATCH clause");
                // WHERE clause is now part of MATCH clause per OpenCypher spec
                assert!(
                    ast.match_clauses[0].where_clause.is_some(),
                    "Expected WHERE clause in MATCH clause"
                );
                assert!(ast.return_clause.is_some(), "Expected RETURN clause");

                let return_clause = ast.return_clause.as_ref().unwrap();
                assert_eq!(return_clause.return_items.len(), 1);
                let item = &return_clause.return_items[0];
                if let Expression::PatternComprehension(_) = &item.expression {
                    println!("Pattern comprehension parsed correctly!");
                } else {
                    panic!(
                        "Expected PatternComprehension in RETURN, got {:?}",
                        item.expression
                    );
                }
            }
            Err(e) => {
                panic!(
                    "Failed to parse full query with pattern comprehension: {:?}",
                    e
                );
            }
        }
    }
}
