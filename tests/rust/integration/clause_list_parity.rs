//! P-4c S2: the clause-list parser (`open_cypher_parser::clause_list`).
//!
//! 1. It agrees with the legacy parser on every corpus query the legacy
//!    parser accepts. Both parses are reduced to one canonical form: per query
//!    part (a part ends at WITH or RETURN), the reading clauses in order, the
//!    UNWINDs in order, CALLs, write clauses, and the projection with its
//!    modifiers. The canonical form forgets only what the legacy AST cannot
//!    record: where an UNWIND stood relative to the reading clauses of its part
//!    (the legacy parser merges leading and trailing UNWINDs).
//! 2. Its grammar follows Neo4j 5.26 for clause order: every positive and
//!    negative case below was checked against Neo4j 5.26.31.

use clickgraph::open_cypher_parser::ast::{
    CallClause, CreateClause, CypherStatement, DeleteClause, LimitClause, OpenCypherQueryAst,
    OrderByClause, ReadingClause, RemoveClause, ReturnClause, SetClause, SkipClause, UnionType,
    UnwindClause, UseClause, WhereClause, WithClause, WithItem,
};
use clickgraph::open_cypher_parser::clause_list::{parse_clause_statement, Clause, ClauseQuery};
use clickgraph::open_cypher_parser::{parse_cypher_statement, strip_comments};

#[derive(Debug, PartialEq, Clone, Default)]
struct Part<'a> {
    reading: Vec<ReadingClause<'a>>,
    unwinds: Vec<UnwindClause<'a>>,
    /// Only the legacy AST can hold a WHERE outside MATCH/WITH; the clause
    /// list cannot, so a legacy query with one shows up as a disagreement.
    dangling_where: Option<WhereClause<'a>>,
    calls: Vec<CallClause<'a>>,
    creates: Vec<CreateClause<'a>>,
    sets: Vec<SetClause<'a>>,
    removes: Vec<RemoveClause<'a>>,
    deletes: Vec<DeleteClause<'a>>,
    /// Free-standing ORDER BY / SKIP / LIMIT clauses (the legacy grammar has
    /// none, so any here is a disagreement).
    standalone_modifiers: usize,
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
        calls: q.call_clause.clone().into_iter().collect(),
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

fn clause_canon<'a>(q: &ClauseQuery<'a>) -> Canon<'a> {
    let mut parts = Vec::new();
    let mut part = Part::default();
    for c in &q.clauses {
        match c {
            Clause::Match(m) => part.reading.push(ReadingClause::Match(m.clone())),
            Clause::OptionalMatch(o) => part.reading.push(ReadingClause::OptionalMatch(o.clone())),
            Clause::Unwind(u) => part.unwinds.push(u.clone()),
            Clause::Call(c) => part.calls.push(c.clone()),
            Clause::Create(c) => part.creates.push(c.clone()),
            Clause::Set(s) => part.sets.push(s.clone()),
            Clause::Remove(r) => part.removes.push(r.clone()),
            Clause::Delete(d) => part.deletes.push(d.clone()),
            Clause::OrderBy(_) | Clause::Skip(_) | Clause::Limit(_) => {
                part.standalone_modifiers += 1
            }
            Clause::With(w) => {
                part.projection = Some(Projection::With {
                    distinct: w.distinct,
                    is_star: w.is_star,
                    items: w.items.clone(),
                    mods: Mods {
                        order_by: w.order_by.clone(),
                        skip: w.skip.clone(),
                        limit: w.limit.clone(),
                        where_: w.where_clause.clone(),
                    },
                });
                parts.push(std::mem::take(&mut part));
            }
            Clause::Return(r) => {
                part.projection = Some(Projection::Return {
                    clause: r.clause.clone(),
                    mods: Mods {
                        order_by: r.order_by.clone(),
                        skip: r.skip.clone(),
                        limit: r.limit.clone(),
                        where_: None,
                    },
                });
                parts.push(std::mem::take(&mut part));
            }
        }
    }
    if part != Part::default() {
        parts.push(part);
    }
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
    let (mut compared, mut legacy_rejects, mut not_queries, mut incomplete) = (0, 0, 0, 0);
    let (mut with_parts, mut unions) = (0, 0);
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
        // A query without RETURN / update clause / CALL is accepted by the
        // legacy parser (the planner rejects it later); the clause-list
        // parser rejects it, as Neo4j does ("Query cannot conclude with ...").
        let last = legacy.0 .1.last().expect("at least one part");
        let concludes = matches!(last.projection, Some(Projection::Return { .. }))
            || !last.calls.is_empty()
            || !(last.creates.is_empty()
                && last.sets.is_empty()
                && last.removes.is_empty()
                && last.deletes.is_empty());
        if !concludes {
            incomplete += 1;
            if parse_clause_statement(&cleaned).is_ok() {
                disagreements.push(format!("{}: accepted without RETURN", entry["name"]));
            }
            continue;
        }
        compared += 1;
        with_parts += usize::from(legacy.0 .1.len() > 1);
        unions += usize::from(!legacy.1.is_empty());
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
        with_parts > 200 && unions > 5,
        "corpus lacks WITH/UNION coverage"
    );
    assert!(
        disagreements.is_empty(),
        "{} of {compared} corpus queries disagree (legacy rejects {legacy_rejects}, \
         non-queries {not_queries}, no RETURN {incomplete}):\n{}",
        disagreements.len(),
        disagreements.join("\n")
    );
}

fn kinds(cypher: &str) -> Result<Vec<&'static str>, String> {
    let leaked: &'static str = Box::leak(cypher.to_string().into_boxed_str());
    let (_, s) = parse_clause_statement(leaked).map_err(|e| format!("{e:?}"))?;
    Ok(s.first
        .clauses
        .iter()
        .map(|c| match c {
            Clause::Match(_) => "MATCH",
            Clause::OptionalMatch(_) => "OPTIONAL MATCH",
            Clause::Unwind(_) => "UNWIND",
            Clause::Call(_) => "CALL",
            Clause::With(w) => {
                if w.order_by.is_none()
                    && w.skip.is_none()
                    && w.limit.is_none()
                    && w.where_clause.is_none()
                {
                    "WITH"
                } else {
                    "WITH+mods"
                }
            }
            Clause::OrderBy(_) => "ORDER BY",
            Clause::Skip(_) => "SKIP",
            Clause::Limit(_) => "LIMIT",
            Clause::Return(_) => "RETURN",
            Clause::Create(_) => "CREATE",
            Clause::Set(_) => "SET",
            Clause::Remove(_) => "REMOVE",
            Clause::Delete(_) => "DELETE",
        })
        .collect())
}

/// Accepted by Neo4j 5.26; the clause sequence the parser must produce.
#[test]
fn neo4j_accepted_clause_orders() {
    let cases: &[(&str, &[&str])] = &[
        // shapes the legacy layout cannot hold
        (
            "MATCH (a:User) WITH a MATCH (a)-[:FOLLOWS]->(b:User) MATCH (b)-[:FOLLOWS]->(c:User) RETURN count(*)",
            &["MATCH", "WITH", "MATCH", "MATCH", "RETURN"],
        ),
        (
            "MATCH (a:User) WITH a UNWIND [1,2] AS x UNWIND [3] AS y RETURN count(*)",
            &["MATCH", "WITH", "UNWIND", "UNWIND", "RETURN"],
        ),
        (
            "MATCH (a:User) WITH a OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) MATCH (b)-[:FOLLOWS]->(c:User) RETURN count(*)",
            &["MATCH", "WITH", "OPTIONAL MATCH", "MATCH", "RETURN"],
        ),
        (
            "MATCH (a:User) UNWIND [1,2] AS x MATCH (b:User) RETURN count(*)",
            &["MATCH", "UNWIND", "MATCH", "RETURN"],
        ),
        // a WITH's own modifiers, in the fixed order, WHERE last (#1311)
        (
            "MATCH (u:User) WITH u ORDER BY u.age LIMIT 5 WHERE u.age > 30 RETURN count(*)",
            &["MATCH", "WITH+mods", "RETURN"],
        ),
        // out-of-order modifiers are free-standing clauses (Neo4j: [[1],[3]])
        (
            "UNWIND [3,1,2] AS x WITH x LIMIT 2 ORDER BY x RETURN x",
            &["UNWIND", "WITH+mods", "ORDER BY", "RETURN"],
        ),
        (
            "UNWIND [3,1,2] AS x WITH x SKIP 1 ORDER BY x RETURN x",
            &["UNWIND", "WITH+mods", "ORDER BY", "RETURN"],
        ),
        (
            "UNWIND [3,1,2] AS x WITH x SKIP 1 SKIP 1 RETURN x",
            &["UNWIND", "WITH+mods", "SKIP", "RETURN"],
        ),
        (
            "UNWIND [3,1,2] AS x WITH x ORDER BY x WHERE x > 1 ORDER BY x DESC RETURN x",
            &["UNWIND", "WITH+mods", "ORDER BY", "RETURN"],
        ),
        (
            "UNWIND [3,1,2] AS x WITH x WHERE x > 1 ORDER BY x LIMIT 1 RETURN x",
            &["UNWIND", "WITH+mods", "ORDER BY", "LIMIT", "RETURN"],
        ),
        // after a clause other than WITH (Neo4j: [[3],[2]])
        (
            "UNWIND [1,2,3] AS x ORDER BY x DESC LIMIT 2 RETURN x",
            &["UNWIND", "ORDER BY", "LIMIT", "RETURN"],
        ),
        (
            "UNWIND [3,1,2] AS x WITH x OFFSET 1 RETURN x",
            &["UNWIND", "WITH+mods", "RETURN"],
        ),
        (
            "UNWIND [3,1,2] AS x RETURN x ORDER BY x OFFSET 1",
            &["UNWIND", "RETURN"],
        ),
        ("RETURN 1 AS num SKIP 0 LIMIT 5", &["RETURN"]),
    ];
    for (q, expected) in cases {
        assert_eq!(kinds(q).as_deref(), Ok(*expected), "{q}");
    }
}

/// Rejected by Neo4j 5.26; the parser must reject them too.
#[test]
fn neo4j_rejected_clause_orders() {
    for q in [
        // nothing may follow RETURN
        "RETURN 1 AS a MATCH (n) RETURN n",
        "RETURN 1 AS x RETURN 2 AS y",
        "UNWIND [1,2,3] AS x RETURN x WHERE x > 1",
        "RETURN 1 AS num LIMIT 5 SKIP 0",
        // a query must not end with WITH / MATCH / UNWIND
        "UNWIND [1,2] AS x WITH x",
        "MATCH (n)",
        "WITH 1 AS x UNION RETURN 2 AS x",
        // there is no free-standing WHERE
        "UNWIND [3,1,2] AS x WITH x WHERE x > 1 WHERE x > 2 RETURN x",
        "UNWIND [1] AS x WHERE x > 0 RETURN x",
        "UNWIND [3,1,2] AS x WITH x AS y SKIP 0 ORDER BY y LIMIT 1 WHERE y > 0 RETURN y",
        "UNWIND [3,1,2] AS x WITH x AS y LIMIT 3 SKIP 0 WHERE y > 0 RETURN y",
        // syntax errors and trailing garbage
        "MATCH (n) RETURN n garbage",
        "MATCH (n RETURN n",
        "",
    ] {
        assert!(kinds(q).is_err(), "must be rejected: {q}");
    }
    assert!(kinds("MATCH (n) RETURN n;").is_ok());
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
/// the clause-list parser does with them: run with `--ignored --nocapture`.
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
