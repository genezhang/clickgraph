//! The clause-list form of a Cypher query (P-4c S2, `docs/design/EXPLICIT_SCOPE.md` §4.2).
//!
//! The legacy query AST ([`super::ast::OpenCypherQueryAst`]) has a fixed slot
//! per clause kind, and after a WITH at most one UNWIND, one MATCH, a list of
//! OPTIONAL MATCHes and one next WITH (`WithClause::subsequent_*`). Clause
//! order is implied by that layout, and anything it cannot hold does not parse
//! (`WITH a MATCH .. MATCH ..`, two UNWINDs after a WITH). It also loses where
//! a WITH's WHERE stood relative to its ORDER BY / SKIP / LIMIT (#1311).
//!
//! This form is a plain sequence of clauses in source order, which is what the
//! P-4c binder consumes: every clause is bound against the scope the previous
//! clauses produced. It reuses the same clause and expression parsers as the
//! legacy grammar; only the sequencing differs. The legacy pipeline still
//! parses with [`super::parse_cypher_statement`]; `tests/rust/integration/
//! clause_list_parity.rs` checks that both parsers agree on every corpus
//! query the legacy parser accepts.

use nom::branch::alt;
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
    order_by_and_page_clause, order_by_clause, remove_clause, return_clause, set_clause,
    skip_clause, unwind_clause, use_clause, where_clause, with_clause,
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
    /// A WHERE that follows a clause which does not take one (the legacy
    /// grammar accepts one after the reading clauses); the binder decides
    /// what it may attach to.
    Where(WhereClause<'a>),
    Call(CallClause<'a>),
    With(WithProjection<'a>),
    Return(ReturnProjection<'a>),
    Create(CreateClause<'a>),
    Set(SetClause<'a>),
    Remove(RemoveClause<'a>),
    Delete(DeleteClause<'a>),
}

/// `WITH [DISTINCT] items | * [, items]` and its modifiers.
#[derive(Debug, PartialEq, Clone)]
pub struct WithProjection<'a> {
    pub distinct: bool,
    pub is_star: bool,
    pub items: Vec<WithItem<'a>>,
    /// ORDER BY / SKIP / LIMIT / WHERE in the order written. Neo4j 5.26
    /// applies them one after another in that order (verified: `WITH x
    /// LIMIT 2 ORDER BY x` limits, then sorts; `WITH x ORDER BY x LIMIT 5
    /// WHERE p` filters the limited rows, #1311), so the order is meaning.
    pub modifiers: Vec<Modifier<'a>>,
}

/// `RETURN [DISTINCT] items` and its ORDER BY / SKIP / LIMIT, which (unlike a
/// WITH's) must be written in that order.
#[derive(Debug, PartialEq, Clone)]
pub struct ReturnProjection<'a> {
    pub clause: ReturnClause<'a>,
    pub modifiers: Vec<Modifier<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Modifier<'a> {
    OrderBy(OrderByClause<'a>),
    Skip(SkipClause),
    Limit(LimitClause),
    Where(WhereClause<'a>),
}

/// Parse a statement into clause-list form. Like
/// [`super::parse_cypher_statement`], it is all-consuming: anything but
/// trailing whitespace and semicolons after the statement is an error.
/// Standalone procedure calls and `COPY TO` are not queries and are not
/// accepted here.
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

/// One query: `USE` clauses, then clauses until none parses.
pub fn parse_clause_query(
    input: &'_ str,
) -> IResult<&'_ str, ClauseQuery<'_>, OpenCypherParsingError<'_>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, use_clauses) = many0(use_clause::parse_use_clause).parse(input)?;
    let (input, clauses) = many0(parse_clause).parse(input)?;
    if clauses.is_empty() {
        return Err(nom::Err::Error(OpenCypherParsingError {
            errors: vec![(input, "Expected a clause")],
        }));
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

/// One clause. Each alternative is a clause parser of the legacy grammar,
/// tried by its leading keyword; a hard failure inside one (`cut`) propagates.
fn parse_clause(input: &'_ str) -> IResult<&'_ str, Clause<'_>, OpenCypherParsingError<'_>> {
    alt((
        // OPTIONAL MATCH before MATCH: both start with a keyword the other lacks,
        // but trying the longer one first keeps the order of the legacy parser.
        |i| {
            optional_match_clause::parse_optional_match_clause(i)
                .map(|(r, c)| (r, Clause::OptionalMatch(c)))
        },
        |i| match_clause::parse_match_clause(i).map(|(r, c)| (r, Clause::Match(c))),
        |i| unwind_clause::parse_unwind_clause(i).map(|(r, c)| (r, Clause::Unwind(c))),
        |i| where_clause::parse_where_clause(i).map(|(r, c)| (r, Clause::Where(c))),
        |i| call_clause::parse_call_clause(i).map(|(r, c)| (r, Clause::Call(c))),
        parse_with_projection,
        parse_return_projection,
        |i| create_clause::parse_create_clause(i).map(|(r, c)| (r, Clause::Create(c))),
        |i| set_clause::parse_set_clause(i).map(|(r, c)| (r, Clause::Set(c))),
        |i| remove_clause::parse_remove_clause(i).map(|(r, c)| (r, Clause::Remove(c))),
        |i| delete_clause::parse_delete_clause(i).map(|(r, c)| (r, Clause::Delete(c))),
    ))
    .parse(input)
}

fn parse_with_projection(
    input: &'_ str,
) -> IResult<&'_ str, Clause<'_>, OpenCypherParsingError<'_>> {
    let (input, (distinct, items, is_star)) = with_clause::parse_with_head(input)?;
    let (input, modifiers) = parse_with_modifiers(input)?;
    Ok((
        input,
        Clause::With(WithProjection {
            distinct,
            is_star,
            items,
            modifiers,
        }),
    ))
}

fn parse_return_projection(
    input: &'_ str,
) -> IResult<&'_ str, Clause<'_>, OpenCypherParsingError<'_>> {
    let (input, clause) = return_clause::parse_return_clause(input)?;
    // ORDER BY, then SKIP, then LIMIT (the legacy page-clause grammar; Neo4j
    // rejects any other order after RETURN).
    let (input, page) =
        opt(order_by_and_page_clause::parse_order_by_and_page_clause).parse(input)?;
    let mut modifiers = Vec::new();
    if let Some(page) = page {
        modifiers.extend(page.order_by.map(Modifier::OrderBy));
        modifiers.extend(page.skip.map(Modifier::Skip));
        modifiers.extend(page.limit.map(Modifier::Limit));
    }
    Ok((
        input,
        Clause::Return(ReturnProjection { clause, modifiers }),
    ))
}

/// A WITH's ORDER BY / SKIP / LIMIT / WHERE in any order, each at most once,
/// recorded in the order written.
fn parse_with_modifiers(
    mut input: &'_ str,
) -> IResult<&'_ str, Vec<Modifier<'_>>, OpenCypherParsingError<'_>> {
    let mut modifiers: Vec<Modifier<'_>> = Vec::new();
    loop {
        let seen = |pred: fn(&Modifier<'_>) -> bool| modifiers.iter().any(pred);
        if !seen(|m| matches!(m, Modifier::OrderBy(_))) {
            if let Ok((rest, o)) = order_by_clause::parse_order_by_clause(input) {
                modifiers.push(Modifier::OrderBy(o));
                input = rest;
                continue;
            }
        }
        if !seen(|m| matches!(m, Modifier::Skip(_))) {
            if let Ok((rest, s)) = skip_clause::parse_skip_clause(input) {
                modifiers.push(Modifier::Skip(s));
                input = rest;
                continue;
            }
        }
        if !seen(|m| matches!(m, Modifier::Limit(_))) {
            if let Ok((rest, l)) = limit_clause::parse_limit_clause(input) {
                modifiers.push(Modifier::Limit(l));
                input = rest;
                continue;
            }
        }
        if !seen(|m| matches!(m, Modifier::Where(_))) {
            match where_clause::parse_where_clause(input) {
                Ok((rest, w)) => {
                    modifiers.push(Modifier::Where(w));
                    input = rest;
                    continue;
                }
                Err(nom::Err::Failure(e)) => return Err(nom::Err::Failure(e)),
                Err(_) => {}
            }
        }
        return Ok((input, modifiers));
    }
}
