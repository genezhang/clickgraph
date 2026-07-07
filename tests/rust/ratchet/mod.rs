//! Ratchet test — guardrail G.1 of `docs/design/REFACTORING_SAFETY_PLAN.md` §2.1.
//!
//! While the schema-pattern/dialect-dispatch refactor (see that doc) is in
//! flight, nothing stops *new* inline branching on the raw axis tokens from
//! accreting faster than old branches are migrated to the `PatternSchemaContext`/
//! `Dialect` dispatch points. This test makes that debt visible instead of
//! silent: it counts, per source file, occurrences of the raw axis tokens and
//! compares against a committed baseline (`tests/rust/ratchet/baseline.txt`).
//!
//! Two axes are tracked:
//! - **Schema-pattern axis** (counted only OUTSIDE `src/graph_catalog/`, which
//!   is the canonical home for these predicates): `is_denormalized`,
//!   `is_fk_edge`, `type_column`, `from_label_column`, `to_label_column`,
//!   `from_node_properties`, `to_node_properties`.
//! - **Dialect axis** (counted only OUTSIDE `src/sql_generator/` and
//!   `src/executor/`, the canonical dispatch layer): case-insensitive
//!   `databricks`, plus `Dialect::`.
//!
//! This is a ratchet, not a parser: matching is plain substring counting, so
//! false positives (comments, string literals, doc examples) are expected and
//! fine — what matters is that the same file produces the same count run to
//! run, so any *change* is a signal.
//!
//! Known evasion (accepted): the dialect axis is detected via the
//! `databricks`/`Dialect::` substrings, so a dialect branch written without
//! either (e.g. a boolean helper with a neutral name, or matching variants
//! through a `use SqlDialect::*` glob) would not be counted. Today every
//! dialect branch in the tree uses `SqlDialect::` syntax; reviewers should
//! keep it that way.
//!
//! - A file whose count for a token **increases** vs. baseline (including a
//!   brand-new file, treated as an increase from 0) fails the test: route the
//!   new code through `PatternSchemaContext`/schema-catalog APIs or `Dialect`/
//!   `FunctionMapper` per §2.1, or if genuinely justified, regenerate the
//!   baseline (see below) and justify the bump in your PR description.
//! - A file whose count **decreases** (or a token disappears entirely) also
//!   fails the test, but with a friendly message — this is progress, and the
//!   baseline must be ratcheted down in the same PR so the improvement is
//!   locked in and can't silently regress later.
//!
//! Regenerate the baseline after an intentional count change with:
//!
//! ```text
//! UPDATE_RATCHET=1 cargo test --test ratchet -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Schema-pattern axis raw tokens (see module docs).
const SCHEMA_PATTERN_TOKENS: &[&str] = &[
    "is_denormalized",
    "is_fk_edge",
    "type_column",
    "from_label_column",
    "to_label_column",
    "from_node_properties",
    "to_node_properties",
];

/// Dialect axis raw tokens. `"databricks"` is matched case-insensitively and
/// `"Dialect::"` case-sensitively — see `count_token`.
const DIALECT_TOKENS: &[&str] = &["databricks", "Dialect::"];

/// A single axis being ratcheted.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Axis {
    SchemaPattern,
    Dialect,
}

impl Axis {
    fn label(self) -> &'static str {
        match self {
            Axis::SchemaPattern => "schema",
            Axis::Dialect => "dialect",
        }
    }

    fn from_label(s: &str) -> Option<Axis> {
        match s {
            "schema" => Some(Axis::SchemaPattern),
            "dialect" => Some(Axis::Dialect),
            _ => None,
        }
    }

    /// Path prefix (relative to `CARGO_MANIFEST_DIR`) this axis's tokens are
    /// canonically dispatched through, and therefore excluded from counting.
    fn excluded_prefixes(self) -> &'static [&'static str] {
        match self {
            Axis::SchemaPattern => &["src/graph_catalog/"],
            Axis::Dialect => &["src/sql_generator/", "src/executor/"],
        }
    }

    fn tokens(self) -> &'static [&'static str] {
        match self {
            Axis::SchemaPattern => SCHEMA_PATTERN_TOKENS,
            Axis::Dialect => DIALECT_TOKENS,
        }
    }
}

/// Key identifying one (file, axis, token) count in the baseline.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Key {
    axis: Axis,
    token: String,
    file: String,
}

fn src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn baseline_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/rust/ratchet/baseline.txt")
}

/// Should this source file be scanned at all? Excludes test code in all four
/// conventions used in this codebase: a `tests/` directory component, a
/// `*_tests.rs` suffix, a file literally named `tests.rs` (`mod tests;`), and
/// a `test_*.rs` prefix. Test fixtures aren't production branching debt; the
/// ratchet polices production code only.
fn is_scannable(rel_path: &str) -> bool {
    if rel_path.contains("/tests/") || rel_path.ends_with("_tests.rs") {
        return false;
    }
    let basename = rel_path.rsplit('/').next().unwrap_or(rel_path);
    basename != "tests.rs" && !basename.starts_with("test_")
}

/// Count occurrences of `token` in `content`. The literal token
/// `"databricks"` is matched case-insensitively (covers `Databricks`,
/// `DATABRICKS`, etc. in code, comments, and string literals alike);
/// everything else (incl. `Dialect::`) is matched case-sensitively.
fn count_token(content: &str, content_lower: &str, token: &str) -> usize {
    if token.eq_ignore_ascii_case("databricks") {
        content_lower.matches("databricks").count()
    } else {
        content.matches(token).count()
    }
}

/// Recursively collect all `.rs` files under `dir`.
fn walk_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Scan `src/**/*.rs` and produce per-(file, axis, token) counts, keyed by
/// path relative to `CARGO_MANIFEST_DIR` (POSIX-separated for a
/// platform-stable, human-diffable baseline).
fn scan_current() -> BTreeMap<Key, usize> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    walk_rs_files(&src_root(), &mut files);

    let mut counts = BTreeMap::new();
    for path in files {
        let rel_path = path
            .strip_prefix(manifest_dir)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");

        if !is_scannable(&rel_path) {
            continue;
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue, // non-UTF8 or unreadable; nothing to count
        };
        let content_lower = content.to_ascii_lowercase();

        for &axis in &[Axis::SchemaPattern, Axis::Dialect] {
            if axis
                .excluded_prefixes()
                .iter()
                .any(|p| rel_path.starts_with(p))
            {
                continue;
            }
            for &token in axis.tokens() {
                let n = count_token(&content, &content_lower, token);
                if n > 0 {
                    counts.insert(
                        Key {
                            axis,
                            token: token.to_string(),
                            file: rel_path.clone(),
                        },
                        n,
                    );
                }
            }
        }
    }
    counts
}

/// Parse the committed baseline file. Lines starting with `#` (and blank
/// lines) are comments. Format: `<axis>\t<token>\t<file>\t<count>`.
fn load_baseline() -> BTreeMap<Key, usize> {
    let path = baseline_path();
    let content = std::fs::read_to_string(&path).unwrap_or_default();
    let mut map = BTreeMap::new();
    for line in content.lines() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        assert!(
            parts.len() == 4,
            "malformed ratchet baseline line (expected 4 tab-separated fields): {line:?}"
        );
        let axis = Axis::from_label(parts[0])
            .unwrap_or_else(|| panic!("unknown ratchet axis label: {:?}", parts[0]));
        let token = parts[1].to_string();
        let file = parts[2].to_string();
        let count: usize = parts[3]
            .parse()
            .unwrap_or_else(|_| panic!("non-numeric ratchet count: {:?}", parts[3]));
        map.insert(Key { axis, token, file }, count);
    }
    map
}

fn render_baseline(counts: &BTreeMap<Key, usize>) -> String {
    let mut out = String::new();
    out.push_str("# Ratchet baseline for docs/design/REFACTORING_SAFETY_PLAN.md §2.1.\n");
    out.push_str("# Per-(file, axis, token) occurrence counts of raw schema-pattern/dialect\n");
    out.push_str("# axis tokens outside their canonical dispatch modules. Generated by\n");
    out.push_str("# tests/rust/ratchet/mod.rs; regenerate with:\n");
    out.push_str("#   UPDATE_RATCHET=1 cargo test --test ratchet -- --nocapture\n");
    out.push_str("# Any count increase (incl. new files) fails the test; any decrease must\n");
    out.push_str("# be ratcheted down (regenerated) in the same PR.\n");
    out.push_str("# Format: <axis>\\t<token>\\t<file>\\t<count>\n");
    for (key, count) in counts {
        out.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            key.axis.label(),
            key.token,
            key.file,
            count
        ));
    }
    out
}

#[test]
fn ratchet_schema_and_dialect_axis_counts() {
    let current = scan_current();

    if std::env::var("UPDATE_RATCHET").as_deref() == Ok("1") {
        std::fs::write(baseline_path(), render_baseline(&current)).expect("write ratchet baseline");
        eprintln!(
            "Ratchet baseline regenerated at {} ({} entries). Review the diff before committing.",
            baseline_path().display(),
            current.len()
        );
        return;
    }

    let baseline = load_baseline();

    let mut increases: Vec<String> = Vec::new();
    let mut decreases: Vec<String> = Vec::new();

    let mut all_keys: Vec<&Key> = current.keys().chain(baseline.keys()).collect();
    all_keys.sort();
    all_keys.dedup();

    for key in all_keys {
        let old = baseline.get(key).copied().unwrap_or(0);
        let new = current.get(key).copied().unwrap_or(0);
        if new > old {
            increases.push(format!(
                "  {} axis, token `{}`, file `{}`: {} -> {}",
                key.axis.label(),
                key.token,
                key.file,
                old,
                new
            ));
        } else if new < old {
            decreases.push(format!(
                "  {} axis, token `{}`, file `{}`: {} -> {}",
                key.axis.label(),
                key.token,
                key.file,
                old,
                new
            ));
        }
    }

    if !increases.is_empty() {
        // Report any coincident decreases too, so a mixed run surfaces the
        // full picture in one failure instead of hiding the decrease until
        // the increase is fixed and the test rerun.
        let decrease_note = if decreases.is_empty() {
            String::new()
        } else {
            format!(
                "\n\nAdditionally, {} count decrease(s) were detected in the same run \
                 (ratchet these down when regenerating):\n{}",
                decreases.len(),
                decreases.join("\n")
            )
        };
        panic!(
            "Ratchet FAILED: {} raw axis-token count increase(s) vs. baseline \
             (tests/rust/ratchet/baseline.txt):\n{}\n\n\
             New/increased raw branching on a schema-pattern or dialect axis token was \
             detected outside its canonical dispatch module. Route the change through \
             PatternSchemaContext/schema-catalog APIs (schema-pattern axis) or Dialect/\
             FunctionMapper (dialect axis) per docs/design/REFACTORING_SAFETY_PLAN.md §2.1. \
             If this increase is genuinely justified, regenerate the baseline with \
             `UPDATE_RATCHET=1 cargo test --test ratchet -- --nocapture` and justify it \
             explicitly in your PR description.{}",
            increases.len(),
            increases.join("\n"),
            decrease_note
        );
    }

    if !decreases.is_empty() {
        panic!(
            "Ratchet: {} raw axis-token count decrease(s) vs. baseline \
             (tests/rust/ratchet/baseline.txt) — nice work!\n{}\n\n\
             This is an improvement, but it must be locked in: regenerate the baseline with \
             `UPDATE_RATCHET=1 cargo test --test ratchet -- --nocapture` and include the diff \
             in your PR so the count can't silently creep back up.",
            decreases.len(),
            decreases.join("\n")
        );
    }
}
