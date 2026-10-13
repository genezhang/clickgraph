//! P-4b S1 ratchet (`docs/design/WITH_EXPORT_CONTRACT.md` §3): every reference
//! to a CTE's columns must name a column that CTE actually exports.
//!
//! Scans the corpus goldens of both dialects under `golden/corpus/` (the bound
//! plan's SQL, which `corpus_sweep` keeps equal to its output; P-4c S14c0).
//! For each binding `<cte> AS <alias>` of any CTE (a WITH's, an OPTIONAL
//! MATCH's, a subquery's, a walk's, a union's) in one SQL scope (a CTE body,
//! or the outer query outside every CTE body), each `<alias>.<col>` reference
//! in that same scope must be one of the columns that CTE's SELECT exports. A
//! violation is SQL that ClickHouse rejects with Code 47: almost always an
//! identity / join-key column that was guessed from a naming convention
//! instead of looked up.
//!
//! This is a ratchet over a text scan, not a SQL parser. The known violations
//! are allowlisted with their issue. A NEW violation fails the test (route the
//! fix through the export contract, not a new convention); a FIXED one also fails,
//! asking for its allowlist entry to be removed so the progress is locked in.

use regex::Regex;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

// The scan's patterns, compiled once.
static DEF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:WITH RECURSIVE|WITH|,)\s*(\w+)\s+AS\s*\(").unwrap());
static QUOTED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"(?i)\bAS\s+"([^"]+)""#).unwrap());
static BARE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bAS\s+([A-Za-z_]\w*)\b").unwrap());
static PLAIN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)^\s*SELECT\s+([\w\s,]+?)\s+FROM\b").unwrap());
static SELECT_STAR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"SELECT\s+(?:DISTINCT\s+)?\*").unwrap());
static BINDING: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b([\w.]+)\s+AS\s+(\w+)\b").unwrap());
static REFERENCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?:^|[^"\w.])(\w+)\.(?:"(\w+)"|(\w+))"#).unwrap());

/// `(golden path relative to golden/, violation, tracking issue)`.
/// The legacy pipeline's guessed columns (#1189, #933) went with the corpus
/// sweep's switch to the bound plan's SQL (P-4c S14c0), whose CTE columns
/// are each read through what the CTE exports.
const KNOWN_VIOLATIONS: &[(&str, &str, &str)] = &[];

/// Index just past the `)` closing the `(` that ends right before `start`,
/// skipping single-quoted string literals (backslash escapes, as rendered).
fn find_close(s: &[u8], start: usize) -> usize {
    let mut depth = 1usize;
    let mut i = start;
    while i < s.len() && depth > 0 {
        match s[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < s.len() && s[i] != b'\'' {
                    if s[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    i
}

/// Top-level CTE bodies by name, plus the outer query text (everything outside
/// every CTE body).
fn split_ctes(sql: &str) -> (HashMap<String, String>, String) {
    let bytes = sql.as_bytes();
    let mut bodies = HashMap::new();
    let mut spans: Vec<(usize, usize)> = Vec::new();
    for caps in DEF.captures_iter(sql) {
        let whole = caps.get(0).unwrap();
        if spans
            .iter()
            .any(|&(a, b)| a <= whole.start() && whole.start() < b)
        {
            continue;
        }
        let end = find_close(bytes, whole.end());
        bodies.insert(
            caps[1].to_string(),
            sql[whole.end()..end.saturating_sub(1).max(whole.end())].to_string(),
        );
        spans.push((whole.end(), end));
    }
    spans.sort();
    let mut outer = String::new();
    let mut last = 0;
    for (a, b) in spans {
        outer.push_str(&sql[last..a]);
        last = b;
    }
    outer.push_str(&sql[last..]);
    (bodies, outer)
}

/// Column names a CTE body exports (`AS "x"`, bare `AS x`, either case,
/// and a plain `SELECT a, b FROM` list). Bare aliases also catch table
/// aliases — over-permissive, which can only hide a violation, never invent
/// one.
fn cte_columns(body: &str) -> BTreeSet<String> {
    let mut cols: BTreeSet<String> = QUOTED
        .captures_iter(body)
        .chain(BARE.captures_iter(body))
        .map(|c| c[1].to_string())
        .collect();
    if let Some(c) = PLAIN.captures(body) {
        cols.extend(c[1].split(',').map(|x| x.trim().to_string()));
    }
    cols
}

/// Violations in one rendered SQL statement, as `"<alias>.<col> not in <cte>"`.
fn violations_in_sql(sql: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let (bodies, outer) = split_ctes(sql);
    let columns: HashMap<&str, BTreeSet<String>> = bodies
        .iter()
        .filter(|(_, body)| !SELECT_STAR.is_match(body.split("FROM").next().unwrap_or("")))
        .map(|(name, body)| (name.as_str(), cte_columns(body)))
        .filter(|(_, cols)| !cols.is_empty())
        .collect();
    // Each scope's bindings `<relation> AS <alias>` and references
    // `<alias>.<col>` / `<alias>."<col>"` (not inside a quoted `"a.b"`
    // label), each read once.
    for scope in bodies.values().chain(std::iter::once(&outer)) {
        let mut bound: HashMap<&str, BTreeSet<&str>> = HashMap::new();
        for caps in BINDING.captures_iter(scope) {
            let (relation, alias) = (caps.get(1).unwrap().as_str(), caps.get(2).unwrap().as_str());
            bound.entry(alias).or_default().insert(relation);
        }
        for r in REFERENCE.captures_iter(scope) {
            let alias = r.get(1).unwrap().as_str();
            // Bound to one CTE only (an alias bound to another relation in
            // the same scope too is ambiguous: skipped).
            let Some(sources) = bound.get(alias) else {
                continue;
            };
            let [cte] = sources.iter().copied().collect::<Vec<_>>()[..] else {
                continue;
            };
            let Some(cols) = columns.get(cte) else {
                continue;
            };
            let col = r.get(2).or_else(|| r.get(3)).unwrap().as_str();
            if !cols.contains(col) {
                out.insert(format!("{alias}.{col} not in {cte}"));
            }
        }
    }
    out
}

fn collect_goldens(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {dir:?}: {e}")) {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_goldens(&path, out);
        } else if path.to_string_lossy().ends_with(".sql") {
            out.push(path);
        }
    }
}

#[test]
fn with_cte_column_refs_ratchet() {
    let root = PathBuf::from(format!(
        "{}/tests/rust/integration/golden",
        env!("CARGO_MANIFEST_DIR")
    ));
    let mut files = Vec::new();
    collect_goldens(&root.join("corpus"), &mut files);
    assert!(
        files.len() > 2000,
        "expected the corpus goldens of both dialects under {root:?}, found {}",
        files.len()
    );

    let mut found: BTreeSet<(String, String)> = BTreeSet::new();
    for path in &files {
        // Spark quotes with backticks.
        let sql = std::fs::read_to_string(path).unwrap().replace('`', "\"");
        let rel = path
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        for v in violations_in_sql(&sql) {
            found.insert((rel.clone(), v));
        }
    }
    let known: BTreeSet<(String, String)> = KNOWN_VIOLATIONS
        .iter()
        .map(|(p, v, _)| (p.to_string(), v.to_string()))
        .collect();

    let new: Vec<_> = found.difference(&known).collect();
    let fixed: Vec<_> = known.difference(&found).collect();
    assert!(
        new.is_empty(),
        "NEW reference(s) to a column the WITH CTE does not export (ClickHouse \
         Code 47). Resolve the carried variable's column through the WITH export \
         contract (docs/design/WITH_EXPORT_CONTRACT.md), not a naming convention:\n{}",
        new.iter()
            .map(|(p, v)| format!("  {p}: {v}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert!(
        fixed.is_empty(),
        "Known violation(s) no longer present: progress! Remove them from \
         KNOWN_VIOLATIONS so the fix is locked in:\n{}",
        fixed
            .iter()
            .map(|(p, v)| format!("  {p}: {v}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn checker_flags_an_unexported_column_and_accepts_exported_ones() {
    let sql = r#"WITH with_c_cte_0 AS (SELECT
      t1.Dest AS "p1_c_code"
FROM flights AS t1
)
SELECT
      c.p1_c_code AS "c.code",
      t2.Dest AS "a.code"
FROM flights AS t2
INNER JOIN with_c_cte_0 AS c ON t2.Origin = c.code"#;
    assert_eq!(
        violations_in_sql(sql).into_iter().collect::<Vec<_>>(),
        vec!["c.code not in with_c_cte_0".to_string()]
    );
    let fixed = sql.replace("= c.code", "= c.p1_c_code");
    assert!(violations_in_sql(&fixed).is_empty());
}

#[test]
fn checker_scopes_a_binding_to_its_own_scope() {
    // Inside the CTE body `c` is the base table; only the outer scope binds the CTE.
    let sql = r#"WITH with_c_cte_0 AS (SELECT
      c.user_id AS "p1_c_user_id"
FROM users AS c
)
SELECT c.p1_c_user_id AS "c.user_id"
FROM with_c_cte_0 AS c"#;
    assert!(violations_in_sql(sql).is_empty());
}
