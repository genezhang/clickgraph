//! Corpus translation-snapshot sweep — Phase 0 slice P0.6
//! (`docs/design/REFACTORING_SAFETY_PLAN.md` §3.2), on the bound plan since
//! P-4c S14c0.
//!
//! A "translate everything, lock the bytes" regression net over the corpus
//! (`tests/corpus/queries.jsonl` + `schema_map.json`: the queries harvested
//! from the Python integration suite by `scripts/dev/harvest_corpus.py`, and
//! the official LDBC queries and curated client queries added by
//! `scripts/dev/corpus_add_ldbc.py`; 1548 entries). It is the net that keeps
//! the legacy pipeline's deletion (S14c…) from changing what the bound plan
//! emits; until S14c0 it locked the legacy pipeline's SQL (it translated
//! without the query text, which took the legacy route).
//!
//! Each entry is translated through the production read seam
//! (`translate_read` with the query text, as every entry point) on each
//! dialect (ClickHouse, Databricks), with the default options and again with
//! the server's (Neo4j-compat mode, Bolt's id mapper). Locked, per (schema,
//! name), in `golden/corpus/{schema}/{name}.{dialect}.{sql,err}`:
//! - the SQL, then the result's shape (each RETURN item's kind and columns,
//!   which the HTTP and Bolt rows are read by) as `-- column` lines; or the
//!   error (an error<->success transition is a regression too);
//! - the server options' result, as `{dialect}_compat`, where it differs.
//!
//! `normalize()` anonymizes process-global counters (the bound plan's SQL has
//! none it changes). A panic fails the sweep. A golden of no corpus entry
//! fails it too (`UPDATE_GOLDEN=1` deletes it). The census
//! (`binder_corpus.rs`) asserts how many entries lower.
//!
//! Regenerate with:
//! ```text
//! UPDATE_GOLDEN=1 cargo test -p clickgraph --test integration corpus_sweep -- --nocapture
//! ```
//!
//! NONDETERMINISM: `tests/corpus/nondeterministic.txt` lists entries
//! excluded from the byte-lock comparison because their translation is not
//! byte-stable (rendered, not compared). It is empty: the bound plan's SQL
//! is stable across fresh processes and the whole test binary (S14c0). A new
//! flap is a bug to fix (see the file's header), not an entry to add.

use std::collections::HashSet;
use std::sync::Arc;

use clickgraph::{
    graph_catalog::{config::GraphSchemaConfig, graph_schema::GraphSchema},
    open_cypher_parser::{parse_cypher_statement, strip_comments},
    server::bolt_protocol::id_mapper::IdMapper,
    server::query_context::{set_current_schema, with_query_context, QueryContext},
    sql_generator::SqlDialect,
    translate::{translate_read, ReadOptions, TranslateError},
};

#[derive(Debug, serde::Deserialize)]
struct CorpusEntry {
    schema: String,
    name: String,
    cypher: String,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct SchemaMapEntry {
    yaml: String,
    subschema: Option<String>,
}

pub(crate) fn corpus_root() -> String {
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

pub(crate) fn load_schema_map() -> std::collections::HashMap<String, SchemaMapEntry> {
    let path = format!("{}/schema_map.json", corpus_root());
    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&content).unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

/// Load the nondeterministic-entry exclusion list: `schema/name<TAB>reason`
/// per line, blank lines and `#`-comments ignored.
fn load_nondeterministic_set() -> HashSet<(String, String)> {
    load_entry_list("nondeterministic.txt")
}

/// A `schema/name<TAB>…` list of corpus entries in the corpus directory.
fn load_entry_list(file: &str) -> HashSet<(String, String)> {
    let path = format!("{}/{file}", corpus_root());
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
pub(crate) fn load_schema_entry(entry: &SchemaMapEntry) -> GraphSchema {
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
/// down the whole sweep. Uses the production read seam
/// (`clickgraph::translate::translate_read`, which every entry point calls), but returns
/// `Result<String, String>` instead of panicking, so errors can be LOCKED
/// rather than failing the test outright.
async fn try_render(
    schema: &GraphSchema,
    cypher: &str,
    dialect: SqlDialect,
    compat: bool,
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
                // The production read seam (every entry point translates
                // here, with the query text: the bound plan).
                translate_read(
                    statement,
                    &schema,
                    ReadOptions {
                        cypher: cleaned.to_string(),
                        // The server's options (Neo4j-compat, Bolt's ids).
                        neo4j_compat: Some(compat),
                        id_mapper: compat.then(IdMapper::new),
                        ..Default::default()
                    },
                )
                .map(|t| {
                    // The result's shape (how the rows are read: HTTP,
                    // Bolt), locked with its SQL as comment lines.
                    let shape: String = t
                        .shape
                        .iter()
                        .map(|c| format!("\n-- column {}: {:?} {:?}", c.name, c.kind, c.columns))
                        .collect();
                    format!("{}{shape}", t.sql)
                })
                .map_err(|e| match e {
                    TranslateError::Invalid(e) => format!("query error: {e}"),
                    TranslateError::Unsupported(e) => format!("not supported: {e}"),
                    TranslateError::Internal(e) => format!("internal error: {e}"),
                })
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

/// Every file under `dir`, recursively.
fn collect_files(dir: &std::path::Path, out: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {dir:?}: {e}")) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path.to_string_lossy().into_owned());
        }
    }
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
    // The golden files the entries have.
    let mut expected: HashSet<String> = HashSet::new();

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
            let result = try_render(schema, &entry.cypher, *dialect, false).await;
            // In Neo4j-compat mode with Bolt's ids (the server's options):
            // locked where it differs.
            let compat = try_render(schema, &entry.cypher, *dialect, true).await;

            if is_nondeterministic {
                // Still exercised (a panic here would still fail the test
                // via the unwrap below not being reached — catch_unwind
                // already contained it), but not byte-locked.
                skipped_nondeterministic += 1;
                continue;
            }
            let compat_name = format!("{dname}_compat");
            let compat_differs = compat != result;
            for (dname, result, locked) in [
                (*dname, &result, true),
                (compat_name.as_str(), &compat, compat_differs),
            ] {
                let content = |r: &Result<String, String>| match r {
                    Ok(sql) => ("sql", normalize(sql)),
                    Err(e) => ("err", normalize(e)),
                };
                let (ext, content) = content(result);
                if content.starts_with("PANIC") {
                    mismatches.push(format!(
                        "--- {}/{}__{dname} PANIC: {content}",
                        entry.schema, entry.name
                    ));
                }
                let other_ext = if ext == "sql" { "err" } else { "sql" };
                let path = golden_path(&entry.schema, &entry.name, dname, ext);
                let other_path = golden_path(&entry.schema, &entry.name, dname, other_ext);
                expected.insert(path.clone());
                if !locked {
                    // Equal to the default options' result: no golden.
                    expected.remove(&path);
                    continue;
                }

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
                        Ok(expected) if normalize_golden_text(&expected) == content => {}
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
    }

    // Every golden is one of an entry's (a stale one: deleted on update).
    let mut files = Vec::new();
    collect_files(
        std::path::Path::new(&format!(
            "{}/tests/rust/integration/golden/corpus",
            env!("CARGO_MANIFEST_DIR")
        )),
        &mut files,
    );
    for f in files.iter().filter(|f| !expected.contains(*f)) {
        if update {
            std::fs::remove_file(f).expect("remove a stale golden");
        } else {
            mismatches.push(format!(
                "--- STALE golden of no corpus entry: {f} (run UPDATE_GOLDEN=1) ---"
            ));
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

/// Anonymize the two process-global counters whose values vary with test
/// ordering/concurrency: `ALIAS_COUNTER` (anonymous rel aliases `t{n}`) and
/// `CTE_COUNTER` (`cte{n}`). Each is remapped by first appearance, so goldens
/// are deterministic while structure (which alias joins where) is preserved.
///
/// CAUTION: this is text-blind — it rewrites any `t<digits>`/`cte<digits>`
/// token, including ones inside string literals or a schema column literally
/// named `t5`. Today the corpus contains no such token (verified), but if you
/// add a query whose SQL contains a non-counter `t<n>`/`cte<n>`, tighten this
/// (e.g. scope to the alias-defining position) so a real regression in that
/// token can't be silently normalized away.
///
/// `pub(crate)` (not private): the P0.6 corpus sweep (`corpus_sweep.rs`, same
/// `integration` test binary) reuses this exact anonymization for its own,
/// much larger corpus — see docs/design/REFACTORING_SAFETY_PLAN.md §3.2.
pub(crate) fn normalize(sql: &str) -> String {
    fn remap(input: &str, pattern: &str, prefix: &str) -> String {
        let re = regex::Regex::new(pattern).unwrap();
        let mut seen: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut next = 0usize;
        for m in re.find_iter(input) {
            seen.entry(m.as_str().to_string()).or_insert_with(|| {
                let p = format!("{prefix}{next}");
                next += 1;
                p
            });
        }
        re.replace_all(input, |c: &regex::Captures| seen[&c[0].to_string()].clone())
            .into_owned()
    }
    let s = remap(sql, r"\bt\d+\b", "t");
    let s = remap(&s, r"\bcte\d+\b", "cte");
    // The generic `\bt\d+\b` remap above cannot reach the `t<n>` render counter
    // inside a `pattern_union_t<n>` CTE name: the `t` is preceded by `_` (a word
    // char), so there is no word boundary before it. Left un-anonymized, that
    // global counter leaks into the golden and makes the render order-dependent
    // (byte-stable alone, but not across a full sweep). Anonymize it explicitly.
    remap(&s, r"pattern_union_t\d+", "pattern_union_t")
}

/// Normalize checked-in snapshot text so Git checkout line endings do not
/// change the meaning of a golden comparison on Windows.
pub(crate) fn normalize_golden_text(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

#[test]
fn normalizes_golden_line_endings_without_changing_content() {
    assert_eq!(
        normalize_golden_text("SELECT 1\nSELECT 2\n"),
        "SELECT 1\nSELECT 2\n"
    );
    assert_eq!(
        normalize_golden_text("SELECT 1\r\nSELECT 2\r\n"),
        "SELECT 1\nSELECT 2\n"
    );
    assert_eq!(
        normalize_golden_text("SELECT 1\rSELECT 2\r"),
        "SELECT 1\nSELECT 2\n"
    );
}
