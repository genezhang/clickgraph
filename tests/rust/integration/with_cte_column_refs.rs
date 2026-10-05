//! P-4b S1 ratchet (`docs/design/WITH_EXPORT_CONTRACT.md` §3): every reference
//! to a WITH CTE's columns must name a column that CTE actually exports.
//!
//! Scans the ClickHouse goldens under `golden/` (which `corpus_sweep` and
//! `sql_golden_tests` keep equal to the renderer's current output). For each
//! binding `with_<..> AS <alias>` in one SQL scope (a CTE body, or the outer
//! query outside every CTE body), each `<alias>.<col>` reference in that same
//! scope must be one of the columns that CTE's SELECT exports. A violation is SQL
//! that ClickHouse rejects with Code 47: almost always an identity / join-key
//! column that was guessed from a naming convention instead of looked up.
//!
//! This is a ratchet over a text scan, not a SQL parser. The known violations
//! are allowlisted with their issue. A NEW violation fails the test (route the
//! fix through the export contract, not a new convention); a FIXED one also fails,
//! asking for its allowlist entry to be removed so the progress is locked in.

use regex::Regex;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

/// `(golden path relative to golden/, violation, tracking issue)`.
const KNOWN_VIOLATIONS: &[(&str, &str, &str)] = &[
    (
        "corpus/denormalized_flights/test_1182__outside_carried_node_mid_chain_not_repaired_den.clickhouse.sql",
        "c.p1_c_start_id not in with_c_cte_0",
        "#1189",
    ),
    (
        "corpus/denormalized_flights/test_1182__outside_closed_on_carried_node_not_repaired_den.clickhouse.sql",
        "c.p1_c_end_id not in with_c_cte_1",
        "#1189",
    ),
    (
        "corpus/denormalized_flights/test_1188__carry_both_then_vlp_den.clickhouse.sql",
        "c.code not in with_c_cte_0",
        "P-4b (denorm id spelled as the bare Cypher property)",
    ),
    (
        "corpus/denormalized_flights/test_1189__two_carried_path_between_them_not_repaired_den.clickhouse.sql",
        "c_z.p1_c_end_id not in with_c_z_cte_0",
        "#1189",
    ),
    (
        // The start-endpoint tie (P-4b), spelled with the same #1189 guess as the end tie.
        "corpus/denormalized_flights/test_1189__two_carried_path_between_them_not_repaired_den.clickhouse.sql",
        "c_z.p1_z_start_id not in with_c_z_cte_0",
        "#1189",
    ),
    (
        "corpus/standard/test_636_shared_anchor_comma_interleaved_stays_loud.clickhouse.sql",
        "p.post_id not in with_p_cte_0",
        "#933",
    ),
];

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
    let def = Regex::new(r"(?:WITH RECURSIVE|WITH|,)\s*(\w+)\s+AS\s*\(").unwrap();
    let bytes = sql.as_bytes();
    let mut bodies = HashMap::new();
    let mut spans: Vec<(usize, usize)> = Vec::new();
    for caps in def.captures_iter(sql) {
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

/// Column names a CTE body exports (`AS "x"` and bare `AS x` aliases). Bare
/// aliases also catch table aliases — over-permissive, which can only hide a
/// violation, never invent one.
fn cte_columns(body: &str) -> BTreeSet<String> {
    let quoted = Regex::new(r#"\bAS\s+"([^"]+)""#).unwrap();
    let bare = Regex::new(r"\bAS\s+([A-Za-z_]\w*)\s*(?:,|\n|$)").unwrap();
    quoted
        .captures_iter(body)
        .chain(bare.captures_iter(body))
        .map(|c| c[1].to_string())
        .collect()
}

/// Violations in one rendered SQL statement, as `"<alias>.<col> not in <cte>"`.
fn violations_in_sql(sql: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    if !sql.contains("with_") {
        return out;
    }
    let (bodies, outer) = split_ctes(sql);
    let select_star = Regex::new(r"SELECT\s+(?:DISTINCT\s+)?\*").unwrap();
    let columns: HashMap<&str, BTreeSet<String>> = bodies
        .iter()
        .filter(|(name, _)| name.starts_with("with_"))
        .filter(|(_, body)| !select_star.is_match(body.split("FROM").next().unwrap_or("")))
        .map(|(name, body)| (name.as_str(), cte_columns(body)))
        .filter(|(_, cols)| !cols.is_empty())
        .collect();
    let binding = Regex::new(r"\b(with_\w+)\s+AS\s+(\w+)\b").unwrap();
    for scope in bodies.values().chain(std::iter::once(&outer)) {
        for caps in binding.captures_iter(scope) {
            let (cte, alias) = (&caps[1], &caps[2]);
            let Some(cols) = columns.get(cte) else {
                continue;
            };
            let alias_q = regex::escape(alias);
            // The alias rebound to a base table in the same scope: ambiguous, skip.
            let rebind = Regex::new(&format!(r"\b([\w.]+)\s+AS\s+{alias_q}\b")).unwrap();
            if rebind
                .captures_iter(scope)
                .any(|c| !c[1].starts_with("with_"))
            {
                continue;
            }
            // `<alias>.<col>` / `<alias>."<col>"`, not inside a quoted `"a.b"` label.
            let reference =
                Regex::new(&format!(r#"(?:^|[^"\w.]){alias_q}\.(?:"(\w+)"|(\w+))"#)).unwrap();
            for r in reference.captures_iter(scope) {
                let col = r.get(1).or_else(|| r.get(2)).unwrap().as_str();
                if !cols.contains(col) {
                    out.insert(format!("{alias}.{col} not in {cte}"));
                }
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
        } else if path.to_string_lossy().ends_with(".clickhouse.sql") {
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
    collect_goldens(&root, &mut files);
    assert!(
        files.len() > 1000,
        "expected the ClickHouse corpus goldens under {root:?}, found {}",
        files.len()
    );

    let mut found: BTreeSet<(String, String)> = BTreeSet::new();
    for path in &files {
        let sql = std::fs::read_to_string(path).unwrap();
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
