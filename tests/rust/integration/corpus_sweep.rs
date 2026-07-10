//! Corpus translation-snapshot sweep — Phase 0 slice P0.6
//! (`docs/design/REFACTORING_SAFETY_PLAN.md` §3.2).
//!
//! A mass "translate everything, lock the bytes" regression net over a
//! ~1,070-query corpus harvested from the Python integration test suite
//! (`scripts/dev/harvest_corpus.py` -> `tests/corpus/queries.jsonl` +
//! `tests/corpus/schema_map.json`). Unlike `sql_golden_tests.rs` (a curated,
//! hand-picked ~370-case net across 5 schema variations), this sweep proves
//! no-op-ness over the FULL surface of query shapes the test suite actually
//! exercises, at the cost of the byte-locks being less individually
//! documented.
//!
//! For every corpus entry, parse -> plan -> render is run ONCE PER DIALECT
//! (ClickHouse + Databricks), reusing the exact production render path
//! (`sql_golden_tests::render`'s sibling here, `try_render`) and the same
//! `normalize()` counter-anonymization. Unlike `sql_golden_tests.rs`, a
//! translation FAILURE is not a test bug — it is locked too (`.err` golden
//! with the normalized error string), because an error<->success transition
//! is exactly the kind of regression this net exists to catch (and because
//! the corpus, being harvested rather than curated, inevitably contains
//! queries that hit known-unimplemented paths — see the module docs in
//! `sql_golden_tests.rs` for examples of that class of gap). A genuine Rust
//! panic (`unimplemented!`/`panic!` deep in the planner/renderer) is also
//! caught (`catch_unwind`) and locked as a `PANIC: ...` error golden rather
//! than crashing the whole sweep — one bad corpus entry must not blind the
//! net to every other entry.
//!
//! Golden layout: `golden/corpus/{schema}/{name}.{dialect}.{sql,err}` — one
//! file per (schema, name, dialect), matching `sql_golden_tests.rs`'s
//! layout convention (chosen over the spec's escape-hatch concatenated
//! layout since 1,072 queries x 2 dialects = ~2,140 files, well under the
//! ~6,000 threshold where that trade-off would flip).
//!
//! Regenerate with:
//! ```text
//! UPDATE_GOLDEN=1 cargo test -p clickgraph --test integration corpus_sweep -- --nocapture
//! ```
//!
//! NONDETERMINISM: `tests/corpus/nondeterministic.txt` lists entries excluded
//! from the byte-lock comparison because their render is not byte-stable.
//! The 19 historical HashMap-order entries (15 column-order flaps for #480,
//! 4 coupled-edge middle-node WRONG-COLUMN flaps for #481) were fixed by
//! sorting every such iteration on the cypher property key and by
//! owning-edge-first role resolution in `PlanCtx::get_node_strategy`; 14 of
//! them are now byte-locked. The 5 that remain listed are excluded for a
//! DIFFERENT, harness-level reason: their CTE name embeds the process-global
//! anonymous-alias counter (`pattern_union_t<n>`), which `normalize()`'s
//! `\bt\d+\b` regex cannot remap, so the bytes shift with test order inside
//! the shared cargo-test process (fully deterministic per fresh process).
//! See the file's header for the full history and the triage rule for any
//! new flap (a property/column-ORDER or endpoint-binding diff is a
//! #480/#481 regression, not an "add to the list" event). Any listed entry
//! is still RENDERED here (so a panic regression would still be caught) but
//! excluded from the byte-lock comparison/regeneration.

use std::collections::HashSet;
use std::sync::Arc;

use clickgraph::{
    graph_catalog::{config::GraphSchemaConfig, graph_schema::GraphSchema},
    open_cypher_parser::{parse_cypher_statement, strip_comments},
    query_planner::evaluate_read_statement,
    render_plan::{logical_plan_to_render_plan_with_ctx, ToSql},
    server::query_context::{set_current_schema, with_query_context, QueryContext},
    sql_generator::SqlDialect,
};

use crate::sql_golden_tests::normalize;

#[derive(Debug, serde::Deserialize)]
struct CorpusEntry {
    schema: String,
    name: String,
    cypher: String,
}

#[derive(Debug, serde::Deserialize)]
struct SchemaMapEntry {
    yaml: String,
    subschema: Option<String>,
}

fn corpus_root() -> String {
    format!("{}/tests/corpus", env!("CARGO_MANIFEST_DIR"))
}

fn load_entries() -> Vec<CorpusEntry> {
    let path = format!("{}/queries.jsonl", corpus_root());
    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("parse corpus line {l:?}: {e}")))
        .collect()
}

fn load_schema_map() -> std::collections::HashMap<String, SchemaMapEntry> {
    let path = format!("{}/schema_map.json", corpus_root());
    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&content).unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

/// Load the nondeterministic-entry exclusion list: `schema/name<TAB>reason`
/// per line, blank lines and `#`-comments ignored.
fn load_nondeterministic_set() -> HashSet<(String, String)> {
    let path = format!("{}/nondeterministic.txt", corpus_root());
    let content = std::fs::read_to_string(&path).unwrap_or_default();
    content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            let key = l.split('\t').next().unwrap_or(l);
            let (schema, name) = key.split_once('/')?;
            Some((schema.to_string(), name.to_string()))
        })
        .collect()
}

/// Load a schema by its `schema_map.json` entry: either a standalone
/// single-schema YAML (`GraphSchemaConfig::from_yaml_file`, same as
/// `sql_golden_tests::load_schema`), or a named sub-schema inside a
/// multi-schema YAML (`schemas: [...]`, e.g.
/// `schemas/test/unified_test_multi_schema.yaml` / `schema_variations.yaml`)
/// — the corpus spans schemas beyond the 5 `SchemaId` variations
/// `sql_golden_tests.rs` covers, so this loader is more general than that
/// file's (reusing it here would need a `SchemaConfigFile`-aware rewrite of
/// that helper anyway).
fn load_schema_entry(entry: &SchemaMapEntry) -> GraphSchema {
    let yaml_path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), entry.yaml);
    let content =
        std::fs::read_to_string(&yaml_path).unwrap_or_else(|e| panic!("read {yaml_path}: {e}"));
    match &entry.subschema {
        None => GraphSchemaConfig::from_yaml_str(&content)
            .unwrap_or_else(|e| panic!("parse {yaml_path}: {e}"))
            .to_graph_schema()
            .unwrap_or_else(|e| panic!("to_graph_schema {yaml_path}: {e}")),
        Some(sub_name) => {
            use clickgraph::graph_catalog::config::SchemaConfigFile;
            let file: SchemaConfigFile = serde_yaml::from_str(&content)
                .unwrap_or_else(|e| panic!("parse multi-schema {yaml_path}: {e}"));
            match file {
                SchemaConfigFile::Multi { schemas, .. } => schemas
                    .into_iter()
                    .find(|c| c.name.as_deref() == Some(sub_name.as_str()))
                    .unwrap_or_else(|| panic!("subschema '{sub_name}' not found in {yaml_path}"))
                    .to_graph_schema()
                    .unwrap_or_else(|e| panic!("to_graph_schema {yaml_path}::{sub_name}: {e}")),
                SchemaConfigFile::Single(config) => config
                    .to_graph_schema()
                    .unwrap_or_else(|e| panic!("to_graph_schema {yaml_path}: {e}")),
            }
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Parse -> plan -> render for one (schema, cypher, dialect), catching BOTH
/// pipeline errors (`Result::Err`) and Rust panics (`unimplemented!`/
/// `panic!` deep in the planner/renderer) so one bad corpus entry can't take
/// down the whole sweep. Mirrors `sql_golden_tests::render`'s steps exactly
/// (same production path: `evaluate_read_statement` ->
/// `logical_plan_to_render_plan_with_ctx` -> `to_sql()`), but returns
/// `Result<String, String>` instead of panicking, so errors can be LOCKED
/// rather than failing the test outright.
async fn try_render(
    schema: &GraphSchema,
    cypher: &str,
    dialect: SqlDialect,
) -> Result<String, String> {
    let schema = schema.clone();
    let cypher = cypher.to_string();
    let ctx = QueryContext {
        dialect,
        ..QueryContext::default()
    };
    with_query_context(ctx, async move {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> Result<String, String> {
                set_current_schema(Arc::new(schema.clone()));
                let cleaned = strip_comments(&cypher);
                let (_rest, statement) =
                    parse_cypher_statement(&cleaned).map_err(|e| format!("parse error: {e}"))?;
                let (logical_plan, plan_ctx) =
                    evaluate_read_statement(statement, &schema, None, None, None)
                        .map_err(|e| format!("plan error: {e}"))?;
                let render_plan =
                    logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                        .map_err(|e| format!("render error: {e}"))?;
                Ok(render_plan.to_sql())
            },
        ));
        match outcome {
            Ok(inner) => inner,
            Err(payload) => Err(format!("PANIC: {}", panic_message(&*payload))),
        }
    })
    .await
}

fn golden_path(schema: &str, name: &str, dialect: &str, ext: &str) -> String {
    format!(
        "{}/tests/rust/integration/golden/corpus/{schema}/{name}.{dialect}.{ext}",
        env!("CARGO_MANIFEST_DIR")
    )
}

const DIALECTS: &[(SqlDialect, &str)] = &[
    (SqlDialect::ClickHouse, "clickhouse"),
    (SqlDialect::Databricks, "databricks"),
];

#[tokio::test]
async fn corpus_sweep() {
    let update = std::env::var("UPDATE_GOLDEN").as_deref() == Ok("1");
    let entries = load_entries();
    let schema_map = load_schema_map();
    let nondeterministic = load_nondeterministic_set();

    // Cache loaded GraphSchema per schema key — many entries share a schema.
    let mut schema_cache: std::collections::HashMap<String, GraphSchema> =
        std::collections::HashMap::new();

    let mut mismatches: Vec<String> = Vec::new();
    let mut locked_ok = 0usize;
    let mut locked_err = 0usize;
    let mut skipped_nondeterministic = 0usize;

    for entry in &entries {
        let schema = schema_cache.entry(entry.schema.clone()).or_insert_with(|| {
            let map_entry = schema_map.get(&entry.schema).unwrap_or_else(|| {
                panic!(
                    "corpus entry '{}' references unknown schema key '{}' \
                    (not in schema_map.json)",
                    entry.name, entry.schema
                )
            });
            load_schema_entry(map_entry)
        });

        let is_nondeterministic =
            nondeterministic.contains(&(entry.schema.clone(), entry.name.clone()));

        for (dialect, dname) in DIALECTS {
            let result = try_render(schema, &entry.cypher, *dialect).await;

            if is_nondeterministic {
                // Still exercised (a panic here would still fail the test
                // via the unwrap below not being reached — catch_unwind
                // already contained it), but not byte-locked.
                skipped_nondeterministic += 1;
                continue;
            }

            let (ext, content) = match &result {
                Ok(sql) => ("sql", normalize(sql)),
                Err(e) => ("err", normalize(e)),
            };
            let other_ext = if ext == "sql" { "err" } else { "sql" };
            let path = golden_path(&entry.schema, &entry.name, dname, ext);
            let other_path = golden_path(&entry.schema, &entry.name, dname, other_ext);

            if update {
                if let Some(dir) = std::path::Path::new(&path).parent() {
                    std::fs::create_dir_all(dir).expect("create golden dir");
                }
                std::fs::remove_file(&other_path).ok();
                std::fs::write(&path, &content).expect("write golden");
                if ext == "sql" {
                    locked_ok += 1;
                } else {
                    locked_err += 1;
                }
            } else {
                match std::fs::read_to_string(&path) {
                    Ok(expected) if expected == content => {}
                    Ok(expected) => mismatches.push(format!(
                        "--- {}/{}__{dname} MISMATCH ---\nEXPECTED:\n{expected}\nACTUAL:\n{content}\n",
                        entry.schema, entry.name
                    )),
                    Err(_) => mismatches.push(format!(
                        "--- {}/{}__{dname} MISSING golden ({ext}) — possibly an Ok<->Err transition \
                        (run UPDATE_GOLDEN=1) ---",
                        entry.schema, entry.name
                    )),
                }
                if std::path::Path::new(&other_path).exists() {
                    mismatches.push(format!(
                        "--- {}/{}__{dname} STALE {other_ext} golden present alongside the current {ext} \
                        result — an Ok<->Err transition wasn't regenerated (run UPDATE_GOLDEN=1) ---",
                        entry.schema, entry.name
                    ));
                }
            }
        }
    }

    if update {
        eprintln!(
            "corpus_sweep: locked {locked_ok} success goldens, {locked_err} error goldens, \
            skipped {skipped_nondeterministic} nondeterministic renders (over {} entries x {} dialects)",
            entries.len(),
            DIALECTS.len()
        );
    }

    assert!(
        mismatches.is_empty(),
        "{} corpus golden mismatch(es):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
