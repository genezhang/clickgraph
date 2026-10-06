//! The clause-list form of a Cypher query (P-4c S2, `docs/design/EXPLICIT_SCOPE.md` §4.2).
//!
//! The legacy query AST ([`super::ast::OpenCypherQueryAst`]) has a fixed slot
//! per clause kind, and after a WITH at most one UNWIND, one MATCH, a list of
//! OPTIONAL MATCHes and one next WITH (`WithClause::subsequent_*`). Clause
//! order is implied by that layout, and anything it cannot hold does not parse
//! (`WITH a MATCH .. MATCH ..`, two UNWINDs after a WITH).
//!
//! This form is a plain sequence of clauses in source order, which is what the
//! P-4c binder consumes: every clause is bound against the scope the previous
//! clause produced. It reuses the clause and expression parsers of the legacy
//! grammar; only the sequencing differs. The legacy pipeline still parses with
//! [`super::parse_cypher_statement`]; `tests/rust/integration/
//! clause_list_parity.rs` checks that both parsers agree on every corpus query
//! the legacy parser accepts.
//!
//! The grammar follows Neo4j 5.26 (each rule checked against it):
//! * WITH takes its own modifiers in the fixed order ORDER BY, SKIP/OFFSET,
//!   LIMIT, WHERE; they may see the pre-projection variables, and the WHERE
//!   filters after the LIMIT (#1311).
//! * An ORDER BY / SKIP / OFFSET / LIMIT written anywhere else is a clause of
//!   its own (GQL style): it may follow any clause, may repeat, and sees only
//!   the variables the previous clause produced (`WITH x AS y LIMIT 2 ORDER BY
//!   x` is "Variable `x` not defined").
//! * There is no free-standing WHERE: a WHERE belongs to its MATCH, OPTIONAL
//!   MATCH or WITH.
//! * RETURN takes ORDER BY, SKIP/OFFSET, LIMIT in that order and ends the
//!   query; a query ends with RETURN, an updating clause or CALL.

use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::multispace0;
use nom::combinator::opt;
use nom::multi::many0;
use nom::{IResult, Parser};

use super::ast::{
    CallClause, CreateClause, DeleteClause, LimitClause, MatchClause, OptionalMatchClause,
    OrderByClause, RemoveClause, ReturnClause, SetClause, SkipClause, UnionType, UnwindClause,
    UseClause, WhereClause, WithItem,
};
use super::common::ws;
use super::errors::OpenCypherParsingError;
use super::{
    call_clause, create_clause, delete_clause, limit_clause, match_clause, optional_match_clause,
    order_by_clause, remove_clause, return_clause, set_clause, skip_clause, unwind_clause,
    use_clause, where_clause, with_clause,
};

/// A whole statement: one query, or several joined by UNION.
#[derive(Debug, PartialEq, Clone)]
pub struct ClauseStatement<'a> {
    pub first: ClauseQuery<'a>,
    /// Further arms, each with the UNION kind that precedes it.
    pub unions: Vec<(UnionType, ClauseQuery<'a>)>,
}

/// One query (one UNION arm): its clauses in source order.
#[derive(Debug, PartialEq, Clone)]
pub struct ClauseQuery<'a> {
    pub use_clause: Option<UseClause<'a>>,
    pub clauses: Vec<Clause<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Clause<'a> {
    /// MATCH, with its own WHERE.
    Match(MatchClause<'a>),
    /// OPTIONAL MATCH, with its own WHERE.
    OptionalMatch(OptionalMatchClause<'a>),
    Unwind(UnwindClause<'a>),
    Call(CallClause<'a>),
    With(WithProjection<'a>),
    /// A free-standing ORDER BY (not part of a WITH or RETURN).
    OrderBy(OrderByClause<'a>),
    /// A free-standing SKIP / OFFSET.
    Skip(SkipClause),
    /// A free-standing LIMIT.
    Limit(LimitClause),
    Return(ReturnProjection<'a>),
    Create(CreateClause<'a>),
    Set(SetClause<'a>),
    Remove(RemoveClause<'a>),
    Delete(DeleteClause<'a>),
}

/// `WITH [DISTINCT] items | * [, items] [ORDER BY] [SKIP] [LIMIT] [WHERE]`.
/// Evaluated as: project, sort, skip, limit, then filter (#1311). The
/// modifiers may refer to pre-projection variables (subject to the
/// aggregation / DISTINCT rules the binder enforces).
#[derive(Debug, PartialEq, Clone)]
pub struct WithProjection<'a> {
    pub distinct: bool,
    pub is_star: bool,
    pub items: Vec<WithItem<'a>>,
    pub order_by: Option<OrderByClause<'a>>,
    pub skip: Option<SkipClause>,
    pub limit: Option<LimitClause>,
    pub where_clause: Option<WhereClause<'a>>,
}

/// `RETURN [DISTINCT] items [ORDER BY] [SKIP] [LIMIT]`.
#[derive(Debug, PartialEq, Clone)]
pub struct ReturnProjection<'a> {
    pub clause: ReturnClause<'a>,
    pub order_by: Option<OrderByClause<'a>>,
    pub skip: Option<SkipClause>,
    pub limit: Option<LimitClause>,
}

/// Parse a statement into clause-list form. Like
/// [`super::parse_cypher_statement`], it is all-consuming: anything but
/// trailing whitespace and semicolons after the statement is an error.
/// `COPY TO` is not a query and is not accepted here; a standalone procedure
/// call parses as a single CALL clause.
pub fn parse_clause_statement(
    input: &'_ str,
) -> IResult<&'_ str, ClauseStatement<'_>, OpenCypherParsingError<'_>> {
    let (input, first) = parse_clause_query(input)?;
    let (input, unions) = many0(parse_union_arm).parse(input)?;
    let (rest, _) = opt(ws(tag(";"))).parse(input)?;
    let (rest, _) = opt(ws(tag(";"))).parse(rest)?;
    if !rest.trim().is_empty() {
        return Err(nom::Err::Failure(OpenCypherParsingError {
            errors: vec![
                (input, "Unexpected tokens after query"),
                (rest.trim(), "Unparsed input"),
            ],
        }));
    }
    Ok((rest, ClauseStatement { first, unions }))
}

fn parse_union_arm(
    input: &'_ str,
) -> IResult<&'_ str, (UnionType, ClauseQuery<'_>), OpenCypherParsingError<'_>> {
    let (input, _) = ws(tag_no_case("UNION")).parse(input)?;
    let (input, all) = opt(ws(tag_no_case("ALL"))).parse(input)?;
    let union_type = if all.is_some() {
        UnionType::All
    } else {
        UnionType::Distinct
    };
    let (input, query) = parse_clause_query(input)?;
    Ok((input, (union_type, query)))
}

/// One query: `USE` clauses, then clauses up to and including a RETURN.
pub fn parse_clause_query(
    input: &'_ str,
) -> IResult<&'_ str, ClauseQuery<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (mut input, use_clauses) = many0(use_clause::parse_use_clause).parse(input)?;
    let mut clauses: Vec<Clause<'_>> = Vec::new();
    loop {
        match parse_clause(input) {
            Ok((rest, clause)) => {
                let is_return = matches!(clause, Clause::Return(_));
                clauses.push(clause);
                input = rest;
                if is_return {
                    break; // RETURN ends the query (or this UNION arm)
                }
            }
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }
    match clauses.last() {
        None => {
            return Err(nom::Err::Error(OpenCypherParsingError {
                errors: vec![(input, "Expected a clause")],
            }))
        }
        Some(
            Clause::Return(_)
            | Clause::Call(_)
            | Clause::Create(_)
            | Clause::Set(_)
            | Clause::Remove(_)
            | Clause::Delete(_),
        ) => {}
        Some(_) => {
            return Err(nom::Err::Failure(OpenCypherParsingError {
                errors: vec![(
                input,
                "Query cannot conclude with this clause (must be RETURN, an update clause or CALL)",
            )],
            }))
        }
    }
    Ok((
        input,
        ClauseQuery {
            // Several USE clauses: the last takes effect (as in the legacy parser).
            use_clause: use_clauses.into_iter().last(),
            clauses,
        },
    ))
}

type ClauseResult<'a> = IResult<&'a str, Clause<'a>, OpenCypherParsingError<'a>>;

/// One clause, tried by its leading keyword. A hard failure inside a clause
/// parser (`nom::Err::Failure`, from `cut`) propagates; a plain `Error` means
/// "not this clause" and the next one is tried.
fn parse_clause(input: &'_ str) -> ClauseResult<'_> {
    let parsers: [fn(&str) -> ClauseResult<'_>; 13] = [
        // OPTIONAL MATCH before MATCH, as in the legacy parser.
        |i| {
            optional_match_clause::parse_optional_match_clause(i)
                .map(|(r, c)| (r, Clause::OptionalMatch(c)))
        },
        |i| match_clause::parse_match_clause(i).map(|(r, c)| (r, Clause::Match(c))),
        |i| unwind_clause::parse_unwind_clause(i).map(|(r, c)| (r, Clause::Unwind(c))),
        |i| call_clause::parse_call_clause(i).map(|(r, c)| (r, Clause::Call(c))),
        parse_with_projection,
        |i| order_by_clause::parse_order_by_clause(i).map(|(r, c)| (r, Clause::OrderBy(c))),
        |i| skip_clause::parse_skip_or_offset_clause(i).map(|(r, c)| (r, Clause::Skip(c))),
        |i| limit_clause::parse_limit_clause(i).map(|(r, c)| (r, Clause::Limit(c))),
        parse_return_projection,
        |i| create_clause::parse_create_clause(i).map(|(r, c)| (r, Clause::Create(c))),
        |i| set_clause::parse_set_clause(i).map(|(r, c)| (r, Clause::Set(c))),
        |i| remove_clause::parse_remove_clause(i).map(|(r, c)| (r, Clause::Remove(c))),
        |i| delete_clause::parse_delete_clause(i).map(|(r, c)| (r, Clause::Delete(c))),
    ];
    for parser in parsers {
        match parser(input) {
            Err(nom::Err::Error(_)) => continue,
            other => return other,
        }
    }
    Err(nom::Err::Error(OpenCypherParsingError {
        errors: vec![(input, "Expected a clause")],
    }))
}

fn parse_with_projection(input: &'_ str) -> ClauseResult<'_> {
    let (input, (distinct, items, is_star)) = with_clause::parse_with_head(input)?;
    let (input, order_by) = opt(order_by_clause::parse_order_by_clause).parse(input)?;
    let (input, skip) = opt(skip_clause::parse_skip_or_offset_clause).parse(input)?;
    let (input, limit) = opt(limit_clause::parse_limit_clause).parse(input)?;
    let (input, where_clause) = opt(where_clause::parse_where_clause).parse(input)?;
    Ok((
        input,
        Clause::With(WithProjection {
            distinct,
            is_star,
            items,
            order_by,
            skip,
            limit,
            where_clause,
        }),
    ))
}

fn parse_return_projection(input: &'_ str) -> ClauseResult<'_> {
    let (input, clause) = return_clause::parse_return_clause(input)?;
    let (input, order_by) = opt(order_by_clause::parse_order_by_clause).parse(input)?;
    let (input, skip) = opt(skip_clause::parse_skip_or_offset_clause).parse(input)?;
    let (input, limit) = opt(limit_clause::parse_limit_clause).parse(input)?;
    Ok((
        input,
        Clause::Return(ReturnProjection {
            clause,
            order_by,
            skip,
            limit,
        }),
    ))
}
