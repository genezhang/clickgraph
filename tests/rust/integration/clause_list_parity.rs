//! P-4c S2: the clause-list parser (`open_cypher_parser::clause_list`) agrees
//! with the legacy parser on every corpus query the legacy parser accepts, and
//! accepts the clause orders the legacy layout cannot hold.
//!
//! Both parses are reduced to one canonical form: per query part (a part ends
//! at a WITH or RETURN), the reading clauses in order, the UNWINDs in order,
//! a dangling WHERE, CALL, write clauses, and the projection with its
//! modifiers. The canonical form forgets exactly what the legacy AST cannot
//! record: where an UNWIND stood relative to the reading clauses of its part
//! (the legacy parser merges leading and trailing UNWINDs) and the written
//! order of a WITH's ORDER BY / SKIP / LIMIT / WHERE.

use clickgraph::open_cypher_parser::ast::{
    CallClause, CreateClause, CypherStatement, DeleteClause, LimitClause, OpenCypherQueryAst,
    OrderByClause, ReadingClause, RemoveClause, ReturnClause, SetClause, SkipClause, UnionType,
    UnwindClause, UseClause, WhereClause, WithClause, WithItem,
};
use clickgraph::open_cypher_parser::clause_list::{
    parse_clause_statement, Clause, ClauseQuery, Modifier,
};
use clickgraph::open_cypher_parser::{parse_cypher_statement, strip_comments};

#[derive(Debug, PartialEq, Clone, Default)]
struct Part<'a> {
    reading: Vec<ReadingClause<'a>>,
    unwinds: Vec<UnwindClause<'a>>,
    dangling_where: Option<WhereClause<'a>>,
    call: Option<CallClause<'a>>,
    creates: Vec<CreateClause<'a>>,
    sets: Vec<SetClause<'a>>,
    removes: Vec<RemoveClause<'a>>,
    deletes: Vec<DeleteClause<'a>>,
    projection: Option<Projection<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
enum Projection<'a> {
    With {
        distinct: bool,
        is_star: bool,
        items: Vec<WithItem<'a>>,
        mods: Mods<'a>,
    },
    Return {
        clause: ReturnClause<'a>,
        mods: Mods<'a>,
    },
}

#[derive(Debug, PartialEq, Clone, Default)]
struct Mods<'a> {
    order_by: Option<OrderByClause<'a>>,
    skip: Option<SkipClause>,
    limit: Option<LimitClause>,
    where_: Option<WhereClause<'a>>,
}

type Canon<'a> = (Option<UseClause<'a>>, Vec<Part<'a>>);

fn legacy_canon<'a>(q: &OpenCypherQueryAst<'a>) -> Canon<'a> {
    let mut parts = Vec::new();
    let mut part = Part {
        reading: q.reading_clauses.clone(),
        unwinds: q.unwind_clauses.clone(),
        dangling_where: q.where_clause.clone(),
        call: q.call_clause.clone(),
        ..Part::default()
    };
    let mut with = q.with_clause.as_ref();
    while let Some(w) = with {
        part.projection = Some(legacy_with(w));
        parts.push(std::mem::take(&mut part));
        if let Some(u) = &w.subsequent_unwind {
            part.unwinds.push(u.clone());
        }
        if let Some(m) = &w.subsequent_match {
            part.reading.push(ReadingClause::Match((**m).clone()));
        }
        for o in &w.subsequent_optional_matches {
            part.reading.push(ReadingClause::OptionalMatch(o.clone()));
        }
        with = w.subsequent_with.as_deref();
    }
    part.creates.extend(q.create_clause.clone());
    part.sets.extend(q.set_clause.clone());
    part.removes.extend(q.remove_clause.clone());
    part.deletes.extend(q.delete_clause.clone());
    if let Some(r) = &q.return_clause {
        part.projection = Some(Projection::Return {
            clause: r.clone(),
            mods: Mods {
                order_by: q.order_by_clause.clone(),
                skip: q.skip_clause.clone(),
                limit: q.limit_clause.clone(),
                where_: None,
            },
        });
    }
    parts.push(part);
    (q.use_clause.clone(), parts)
}

fn legacy_with<'a>(w: &WithClause<'a>) -> Projection<'a> {
    Projection::With {
        distinct: w.distinct,
        is_star: w.is_star,
        items: w.with_items.clone(),
        mods: Mods {
            order_by: w.order_by.clone(),
            skip: w.skip.clone(),
            limit: w.limit.clone(),
            where_: w.where_clause.clone(),
        },
    }
}

fn mods<'a>(ms: &[Modifier<'a>]) -> Mods<'a> {
    let mut out = Mods::default();
    for m in ms {
        match m {
            Modifier::OrderBy(o) => out.order_by = Some(o.clone()),
            Modifier::Skip(s) => out.skip = Some(s.clone()),
            Modifier::Limit(l) => out.limit = Some(l.clone()),
            Modifier::Where(w) => out.where_ = Some(w.clone()),
        }
    }
    out
}

fn clause_canon<'a>(q: &ClauseQuery<'a>) -> Canon<'a> {
    let mut parts = Vec::new();
    let mut part = Part::default();
    for c in &q.clauses {
        match c {
            Clause::Match(m) => part.reading.push(ReadingClause::Match(m.clone())),
            Clause::OptionalMatch(o) => part.reading.push(ReadingClause::OptionalMatch(o.clone())),
            Clause::Unwind(u) => part.unwinds.push(u.clone()),
            Clause::Where(w) => part.dangling_where = Some(w.clone()),
            Clause::Call(c) => part.call = Some(c.clone()),
            Clause::Create(c) => part.creates.push(c.clone()),
            Clause::Set(s) => part.sets.push(s.clone()),
            Clause::Remove(r) => part.removes.push(r.clone()),
            Clause::Delete(d) => part.deletes.push(d.clone()),
            Clause::With(w) => {
                part.projection = Some(Projection::With {
                    distinct: w.distinct,
                    is_star: w.is_star,
                    items: w.items.clone(),
                    mods: mods(&w.modifiers),
                });
                parts.push(std::mem::take(&mut part));
            }
            Clause::Return(r) => {
                part.projection = Some(Projection::Return {
                    clause: r.clause.clone(),
                    mods: mods(&r.modifiers),
                });
            }
        }
    }
    parts.push(part);
    (q.use_clause.clone(), parts)
}

type Statement<'a> = (Canon<'a>, Vec<(UnionType, Canon<'a>)>);

fn legacy_statement<'a>(s: &CypherStatement<'a>) -> Option<Statement<'a>> {
    match s {
        CypherStatement::Query {
            query,
            union_clauses,
        } => Some((
            legacy_canon(query),
            union_clauses
                .iter()
                .map(|u| (u.union_type.clone(), legacy_canon(&u.query)))
                .collect(),
        )),
        _ => None,
    }
}

#[test]
fn clause_list_parser_agrees_with_the_legacy_parser_on_the_corpus() {
    let corpus = include_str!("../../corpus/queries.jsonl");
    let (mut compared, mut legacy_rejects, mut not_queries) = (0, 0, 0);
    let mut disagreements = Vec::new();
    for line in corpus.lines().filter(|l| !l.trim().is_empty()) {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        let cypher = entry["cypher"].as_str().unwrap().to_string();
        let cleaned = strip_comments(&cypher);
        let legacy = match parse_cypher_statement(&cleaned) {
            Ok((_, s)) => s,
            Err(_) => {
                legacy_rejects += 1;
                continue;
            }
        };
        let Some(legacy) = legacy_statement(&legacy) else {
            not_queries += 1;
            continue;
        };
        compared += 1;
        match parse_clause_statement(&cleaned) {
            Ok((_, s)) => {
                let new = (
                    clause_canon(&s.first),
                    s.unions
                        .iter()
                        .map(|(t, q)| (t.clone(), clause_canon(q)))
                        .collect(),
                );
                if new != legacy {
                    disagreements.push(format!("{}: canonical forms differ", entry["name"]));
                }
            }
            Err(e) => disagreements.push(format!(
                "{}: clause-list parse failed: {e:?}",
                entry["name"]
            )),
        }
    }
    assert!(compared > 1000, "corpus too small: {compared}");
    assert!(
        disagreements.is_empty(),
        "{} of {compared} corpus queries disagree (legacy rejects {legacy_rejects}, \
         non-queries {not_queries}):\n{}",
        disagreements.len(),
        disagreements.join("\n")
    );
}

fn clauses(cypher: &str) -> Vec<&'static str> {
    let leaked: &'static str = Box::leak(cypher.to_string().into_boxed_str());
    let (_, s) = parse_clause_statement(leaked).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    s.first
        .clauses
        .iter()
        .map(|c| match c {
            Clause::Match(_) => "MATCH",
            Clause::OptionalMatch(_) => "OPTIONAL MATCH",
            Clause::Unwind(_) => "UNWIND",
            Clause::Where(_) => "WHERE",
            Clause::Call(_) => "CALL",
            Clause::With(_) => "WITH",
            Clause::Return(_) => "RETURN",
            Clause::Create(_) => "CREATE",
            Clause::Set(_) => "SET",
            Clause::Remove(_) => "REMOVE",
            Clause::Delete(_) => "DELETE",
        })
        .collect()
}

/// Clause orders the legacy layout cannot hold parse, in source order.
#[test]
fn clause_orders_the_legacy_layout_cannot_hold() {
    let q = "MATCH (a:User) WITH a MATCH (a)-[:FOLLOWS]->(b:User) MATCH (b)-[:FOLLOWS]->(c:User) RETURN count(*)";
    assert!(
        parse_cypher_statement(q).is_err(),
        "legacy parser accepts it now; update this test"
    );
    assert_eq!(clauses(q), ["MATCH", "WITH", "MATCH", "MATCH", "RETURN"]);

    let q = "MATCH (a:User) WITH a UNWIND [1,2] AS x UNWIND [3] AS y RETURN count(*)";
    assert!(parse_cypher_statement(q).is_err());
    assert_eq!(clauses(q), ["MATCH", "WITH", "UNWIND", "UNWIND", "RETURN"]);

    let q = "MATCH (a:User) WITH a OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) MATCH (b)-[:FOLLOWS]->(c:User) RETURN count(*)";
    assert!(parse_cypher_statement(q).is_err());
    assert_eq!(
        clauses(q),
        ["MATCH", "WITH", "OPTIONAL MATCH", "MATCH", "RETURN"]
    );

    // UNWIND between two MATCH clauses keeps its place.
    let q = "MATCH (a:User) UNWIND [1,2] AS x MATCH (b:User) RETURN count(*)";
    assert_eq!(clauses(q), ["MATCH", "UNWIND", "MATCH", "RETURN"]);
}

/// A WITH's WHERE written after LIMIT is recorded after it (#1311): it filters
/// the limited rows, so the order is part of the meaning.
#[test]
fn with_modifiers_keep_their_written_order() {
    let q = "MATCH (u:User) WITH u ORDER BY u.age LIMIT 5 WHERE u.age > 30 RETURN count(*)";
    let (_, s) = parse_clause_statement(q).unwrap();
    let Clause::With(w) = &s.first.clauses[1] else {
        panic!("expected WITH")
    };
    let order: Vec<&str> = w
        .modifiers
        .iter()
        .map(|m| match m {
            Modifier::OrderBy(_) => "ORDER BY",
            Modifier::Skip(_) => "SKIP",
            Modifier::Limit(_) => "LIMIT",
            Modifier::Where(_) => "WHERE",
        })
        .collect();
    assert_eq!(order, ["ORDER BY", "LIMIT", "WHERE"]);

    let q = "MATCH (u:User) WITH u WHERE u.age > 30 ORDER BY u.age LIMIT 5 RETURN count(*)";
    let (_, s) = parse_clause_statement(q).unwrap();
    let Clause::With(w) = &s.first.clauses[1] else {
        panic!("expected WITH")
    };
    assert!(matches!(w.modifiers[0], Modifier::Where(_)));
}

/// Neo4j 5.26 applies a WITH's modifiers in written order (`WITH x LIMIT 2
/// ORDER BY x` limits, then sorts), but a RETURN's must be ORDER BY, SKIP,
/// LIMIT in that order (`RETURN 1 LIMIT 5 SKIP 0` is an error).
#[test]
fn return_modifiers_have_a_fixed_order_with_modifiers_do_not() {
    assert!(parse_clause_statement("RETURN 1 AS num LIMIT 5 SKIP 0").is_err());
    assert!(parse_clause_statement("RETURN 1 AS num SKIP 0 LIMIT 5").is_ok());
    let (_, s) =
        parse_clause_statement("UNWIND [3,1,2] AS x WITH x LIMIT 2 ORDER BY x RETURN x").unwrap();
    let Clause::With(w) = &s.first.clauses[1] else {
        panic!("expected WITH")
    };
    assert!(matches!(
        w.modifiers[..],
        [Modifier::Limit(_), Modifier::OrderBy(_)]
    ));
}

#[test]
fn trailing_garbage_and_errors_are_rejected() {
    assert!(parse_clause_statement("MATCH (n) RETURN n garbage").is_err());
    assert!(parse_clause_statement("MATCH (n) RETURN n;").is_ok());
    assert!(parse_clause_statement("MATCH (n RETURN n").is_err());
    assert!(parse_clause_statement("").is_err());
}

#[test]
fn union_arms_are_separate_queries() {
    let q = "MATCH (a:User) RETURN a.name AS n UNION ALL MATCH (b:Post) RETURN b.title AS n";
    let (_, s) = parse_clause_statement(q).unwrap();
    assert_eq!(s.unions.len(), 1);
    assert_eq!(s.unions[0].0, UnionType::All);
    assert_eq!(s.unions[0].1.clauses.len(), 2);
}

/// Report (not assert) the corpus queries the legacy parser rejects and what
/// the clause-list parser does with them: run with `--nocapture` to see.
#[test]
#[ignore]
fn report_legacy_rejects() {
    for line in include_str!("../../corpus/queries.jsonl")
        .lines()
        .filter(|l| !l.trim().is_empty())
    {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        let cleaned = strip_comments(entry["cypher"].as_str().unwrap());
        if parse_cypher_statement(&cleaned).is_err() {
            let new = parse_clause_statement(&cleaned).is_ok();
            println!(
                "legacy-reject new-{} | {}",
                if new { "ACCEPTS" } else { "rejects" },
                cleaned.split_whitespace().collect::<Vec<_>>().join(" ")
            );
        }
    }
}
