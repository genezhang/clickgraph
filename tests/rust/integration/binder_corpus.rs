//! P-4c S3: bind the whole corpus with the clause-list parser + binder.
//!
//! * No corpus query panics the binder.
//! * Every query Neo4j answers (the S1 result goldens' scorecard) binds, or
//!   is reported `Unsupported` (falls back to the legacy pipeline). A semantic
//!   bind error ("Variable `x` not defined", ...) on a query Neo4j accepts
//!   would be a binder bug.
//!
//! Run with `--nocapture` for the coverage summary.

use std::collections::{BTreeMap, HashMap};

use clickgraph::bound_plan::{bind_statement, BindError};
use clickgraph::graph_catalog::graph_schema::GraphSchema;
use clickgraph::open_cypher_parser::clause_list::parse_clause_statement;
use clickgraph::open_cypher_parser::strip_comments;

use crate::corpus_sweep::{corpus_root, load_schema_entry, load_schema_map};

#[test]
fn the_corpus_binds() {
    let schema_map = load_schema_map();
    let mut schemas: HashMap<String, GraphSchema> = HashMap::new();
    let scorecard: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{}/expected/scorecard.json", corpus_root())).unwrap(),
    )
    .unwrap();
    let neo4j_answers =
        |schema: &str, name: &str| scorecard.get(format!("{schema}/{name}")).is_some();

    let mut outcome: BTreeMap<String, usize> = BTreeMap::new();
    let mut unsupported: BTreeMap<String, usize> = BTreeMap::new();
    let mut failures = Vec::new();
    let corpus = std::fs::read_to_string(format!("{}/queries.jsonl", corpus_root())).unwrap();
    for line in corpus.lines().filter(|l| !l.trim().is_empty()) {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        let (cypher, name, schema_name) = (
            entry["cypher"].as_str().unwrap(),
            entry["name"].as_str().unwrap(),
            entry["schema"].as_str().unwrap(),
        );
        let Some(map_entry) = schema_map.get(schema_name) else {
            continue;
        };
        let schema = schemas
            .entry(schema_name.to_string())
            .or_insert_with(|| load_schema_entry(map_entry));
        let cleaned = strip_comments(cypher);
        let Ok((_, stmt)) = parse_clause_statement(&cleaned) else {
            *outcome.entry("parse error".into()).or_default() += 1;
            if std::env::var("BIND_TRACE").is_ok() {
                println!("parse error {schema_name}/{name}: {cypher}");
            }
            continue;
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bind_statement(&stmt, schema)
        }));
        match result {
            Err(_) => failures.push(format!("PANIC {schema_name}/{name}: {cypher}")),
            Ok(Ok(_)) => *outcome.entry("bound".into()).or_default() += 1,
            Ok(Err(BindError::Unsupported(why))) => {
                *outcome.entry("unsupported".into()).or_default() += 1;
                if std::env::var("BIND_TRACE").is_ok() {
                    println!("unsupported {schema_name}/{name}: {why}: {cypher}");
                }
                let key = why.split(':').next().unwrap_or(&why).to_string();
                *unsupported.entry(key).or_default() += 1;
            }
            Ok(Err(e)) => {
                *outcome.entry("bind error".into()).or_default() += 1;
                println!("bind error {schema_name}/{name}: {e}: {cypher}");
                if neo4j_answers(schema_name, name) {
                    failures.push(format!(
                        "{schema_name}/{name}: Neo4j answers it, binder says `{e}`: {cypher}"
                    ));
                }
            }
        }
    }
    println!("binder over the corpus: {outcome:?}");
    println!("unsupported by reason: {unsupported:?}");
    assert!(
        outcome.get("bound").copied().unwrap_or(0) > 1000,
        "binder coverage collapsed: {outcome:?}"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// P-4c S4: lower every corpus query the bound-plan path can lower, through
/// the same seam the server uses. No panic, and every lowered query yields
/// SQL. With `LOWERED_LIST=<path>`, writes `schema/name` of each lowered
/// query (the oracle comparison uses it to score the new path alone); with
/// `LOWER_TRACE=<text>`, prints why each query containing `<text>` is not.
#[test]
fn the_corpus_lowers() {
    let schema_map = load_schema_map();
    let mut schemas: HashMap<String, GraphSchema> = HashMap::new();
    let mut lowered = Vec::new();
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    let mut failures = Vec::new();
    let corpus = std::fs::read_to_string(format!("{}/queries.jsonl", corpus_root())).unwrap();
    for line in corpus.lines().filter(|l| !l.trim().is_empty()) {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        let (cypher, name, schema_name) = (
            entry["cypher"].as_str().unwrap(),
            entry["name"].as_str().unwrap(),
            entry["schema"].as_str().unwrap(),
        );
        let Some(map_entry) = schema_map.get(schema_name) else {
            continue;
        };
        let schema = schemas
            .entry(schema_name.to_string())
            .or_insert_with(|| load_schema_entry(map_entry));
        let cleaned = strip_comments(cypher);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            clickgraph::translate::translate_bound_plan(
                &cleaned,
                schema,
                &clickgraph::translate::ReadOptions::default(),
            )
        }));
        match result {
            Err(_) => failures.push(format!("PANIC {schema_name}/{name}: {cypher}")),
            Ok(Ok(t)) => {
                let sql = t.sql;
                assert!(
                    sql.starts_with("SELECT") || sql.starts_with("WITH "),
                    "{schema_name}/{name}: {sql}"
                );
                lowered.push(format!("{schema_name}/{name}"));
            }
            Ok(Err(why)) => {
                if std::env::var("LOWER_TRACE").is_ok_and(|t| cypher.contains(&t)) {
                    println!("not lowered {schema_name}/{name}: {why}: {cypher}");
                }
                let key = why.split(':').take(2).collect::<Vec<_>>().join(":");
                *reasons.entry(key).or_default() += 1;
            }
        }
    }
    println!("lowered: {}", lowered.len());
    let mut by_count: Vec<_> = reasons.into_iter().collect();
    by_count.sort_by(|a, b| b.1.cmp(&a.1));
    for (why, n) in by_count.iter().take(25) {
        println!("  {n:5}  {why}");
    }
    if let Ok(path) = std::env::var("LOWERED_LIST") {
        std::fs::write(path, lowered.join("\n") + "\n").unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
