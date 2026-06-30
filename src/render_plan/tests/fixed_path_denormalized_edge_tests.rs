//! Regression tests for the fixed-path renderer over denormalized/coupled edges.
//!
//! A single-edge-type path query (`MATCH p=()-[:T]->() RETURN p`) routes through the
//! "fixed_path" renderer, which emits `tuple('fixed_path', t1, t2, t3)` plus the
//! expanded node/edge property projections.
//!
//! Two historical bugs are guarded here:
//!
//! 1. **Over-quoting of dotted physical columns.** A physical column containing a dot
//!    (e.g. Zeek's `id.orig_h`) was rendered in the SELECT projection as
//!    `t1."""id.orig_h"""` — the strategy pre-quoted the name and the renderer quoted
//!    it again. ClickHouse then fails with `Code 47: Identifier ... cannot be
//!    resolved`. The column must be quoted exactly once (`t1."id.orig_h"`).
//!
//! 2. **Unbound relationship alias (`t3`).** When the edge is denormalized INTO one of
//!    its endpoint tables (the edge row shares that endpoint's physical row), it has no
//!    separate scan in FROM. Its columns were nonetheless emitted as `t3.<col>` while
//!    only `t1`/`t2` were bound, leaving `t3` dangling. The edge columns must resolve
//!    against the bound coupled-endpoint alias instead.

use crate::clickhouse_query_generator::cypher_to_sql;
use crate::graph_catalog::config::GraphSchemaConfig;
use crate::server::query_context::{set_current_schema, with_query_context, QueryContext};
use std::sync::Arc;

/// Translate Cypher → SQL through the same task-local-context path the `cg` tool and
/// embedded API use. Denormalized-alias registration (needed to bind the coupled edge
/// to its endpoint row) lives in the task-local context, so the simpler render-only
/// harness does not exercise the fixed-path property expansion.
fn translate(schema_yaml: &str, cypher: &str) -> String {
    let schema = Arc::new(
        GraphSchemaConfig::from_yaml_str(schema_yaml)
            .expect("parse schema yaml")
            .to_graph_schema()
            .expect("build graph schema"),
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));
            cypher_to_sql(cypher, &schema, 100).expect("translate cypher")
        })
        .await
    })
}

/// Fully-denormalized single-edge schema (both endpoints AND the edge live in the
/// same `dns_log` row), with a dotted physical column on the start endpoint.
const ZEEK_YAML: &str = r#"
name: zeek_logs_min
graph_schema:
  nodes:
    - label: IP
      database: zeek
      table: dns_log
      node_id: "id.orig_h"
      property_mappings: { ip: "id.orig_h" }
    - label: Domain
      database: zeek
      table: dns_log
      node_id: query
      property_mappings: { name: query }
  edges:
    - type: DNS_REQUESTED
      database: zeek
      table: dns_log
      from_id: "id.orig_h"
      to_id: query
      from_node: IP
      to_node: Domain
      property_mappings: { uid: uid, qtype: qtype_name, rcode: rcode_name, timestamp: ts, answers: answers }
"#;

/// Mixed-coupled schema: User and Post each have their OWN table, and the AUTHORED
/// edge is denormalized INTO the posts table (coupled to the Post endpoint only).
const SOCIAL_YAML: &str = r#"
name: social_mixed_coupled
graph_schema:
  nodes:
    - label: User
      database: test_integration
      table: users_test
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
    - label: Post
      database: test_integration
      table: posts_test
      node_id: post_id
      property_mappings:
        post_id: post_id
        content: post_content
        created_at: post_date
  edges:
    - type: AUTHORED
      database: test_integration
      table: posts_test
      from_id: author_id
      to_id: post_id
      from_node: User
      to_node: Post
      is_denormalized: true
      property_mappings:
        created_at: post_date
"#;

/// Parse the `(start, end, rel)` SQL aliases out of the
/// `tuple('fixed_path', 't1', 't2', 't3')` path marker. Alias *numbering* is a
/// process-global counter, so tests must not hard-code `t1/t2/t3` — they read the
/// actual aliases here instead.
fn fixed_path_aliases(sql: &str) -> (String, String, String) {
    let start = sql
        .find("tuple('fixed_path'")
        .expect("fixed_path tuple present");
    let close = sql[start..].find(')').expect("tuple close paren") + start;
    let args: Vec<String> = sql[start..close]
        .split('\'')
        .filter(|s| s.len() > 1 && s.starts_with('t') && s[1..].chars().all(|c| c.is_ascii_digit()))
        .map(|s| s.to_string())
        .collect();
    assert_eq!(args.len(), 3, "expected 3 path aliases in {sql}");
    (args[0].clone(), args[1].clone(), args[2].clone())
}

/// Return the SQL expression projected under column-alias label `"<label>"`.
fn expr_for_label(sql: &str, label: &str) -> Option<String> {
    let needle = format!("AS \"{label}\"");
    let pos = sql.find(&needle)?;
    let before = sql[..pos].trim_end();
    let cut = before.rfind([',', '\n']).map(|i| i + 1).unwrap_or(0);
    Some(before[cut..].trim().to_string())
}

/// Assert every table-qualified expression (`t<N>.col`, in expression position —
/// i.e. NOT inside a `"t<N>.prop"` column-alias label) references an alias that is
/// actually bound by a `<table> AS t<N>` in FROM/JOIN. This catches the dangling
/// `t3.<col>` (edge alias not in FROM) regression without hard-coding alias numbers.
fn assert_no_dangling_qualifiers(sql: &str) {
    let bound: std::collections::HashSet<String> = sql
        .split("AS ")
        .skip(1)
        .filter_map(|seg| {
            let tok: String = seg
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            // FROM/JOIN aliases are bare (`t3`); column aliases are quoted ("...") → empty tok.
            (!tok.is_empty()).then_some(tok)
        })
        .collect();

    let bytes = sql.as_bytes();
    for (dot, _) in sql.match_indices('.') {
        let mut start = dot;
        while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
            start -= 1;
        }
        let ident = &sql[start..dot];
        let preceded_by_quote = start > 0 && bytes[start - 1] == b'"';
        let is_table_alias = ident.len() >= 2
            && ident.starts_with('t')
            && ident[1..].chars().all(|c| c.is_ascii_digit());
        if !preceded_by_quote && is_table_alias {
            assert!(
                bound.contains(ident),
                "qualifier `{ident}.` references an unbound alias; SQL:\n{sql}"
            );
        }
    }
}

#[test]
fn fixed_path_dotted_column_quoted_exactly_once() {
    let sql = translate(
        ZEEK_YAML,
        "MATCH p=()-[:DNS_REQUESTED]->() RETURN p LIMIT 25",
    );

    // Confirm we are exercising the fixed_path renderer.
    assert!(
        sql.contains("tuple('fixed_path'"),
        "expected fixed_path renderer; SQL:\n{sql}"
    );

    // The dotted physical column must be quoted exactly once. The double-application
    // bug produced `"""id.orig_h"""`; assert that pathological form is absent.
    assert!(
        sql.contains("\"id.orig_h\""),
        "dotted column must be quoted once; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("\"\"\"id.orig_h\"\"\""),
        "dotted column must NOT be triple-quoted (double-application of quoting); SQL:\n{sql}"
    );
    assert!(
        !sql.contains("\"\"id.orig_h\"\""),
        "dotted column must NOT be double-quoted twice; SQL:\n{sql}"
    );
}

#[test]
fn fixed_path_coupled_edge_binds_to_endpoint_not_unbound_t3() {
    let sql = translate(
        ZEEK_YAML,
        "MATCH p=()-[:DNS_REQUESTED]->() RETURN p LIMIT 25",
    );

    // The fully-denormalized edge has no separate scan: its columns must resolve to a
    // bound endpoint alias, never a dangling `rel.<col>` whose alias is not in FROM.
    assert_no_dangling_qualifiers(&sql);

    // The edge property `uid` must still be projected (bound to the coupled endpoint),
    // preserving the `<rel>.uid` column-alias label for path assembly.
    let (_start, end, rel) = fixed_path_aliases(&sql);
    let uid_expr =
        expr_for_label(&sql, &format!("{rel}.uid")).expect("edge property uid must be projected");
    assert_eq!(
        uid_expr,
        format!("{end}.uid"),
        "edge uid must bind to the coupled endpoint row ({end}); SQL:\n{sql}"
    );
}

#[test]
fn fixed_path_mixed_coupled_edge_binds_to_post_endpoint() {
    let sql = translate(SOCIAL_YAML, "MATCH p=()-[:AUTHORED]->() RETURN p LIMIT 25");

    assert!(
        sql.contains("tuple('fixed_path'"),
        "expected fixed_path renderer; SQL:\n{sql}"
    );

    // AUTHORED is denormalized into posts_test (the Post endpoint). The edge property
    // `created_at` (→ post_date) must bind to the Post endpoint row, not a dangling rel
    // alias. User (own table) is joined normally; Post is the coupled anchor.
    assert_no_dangling_qualifiers(&sql);

    let (_start, end, rel) = fixed_path_aliases(&sql);
    let created_expr = expr_for_label(&sql, &format!("{rel}.created_at"))
        .expect("edge created_at must be projected");
    assert_eq!(
        created_expr,
        format!("{end}.post_date"),
        "coupled edge created_at must resolve to the Post endpoint's post_date ({end}); SQL:\n{sql}"
    );

    assert!(
        sql.contains(&format!("posts_test AS {end}")),
        "Post endpoint table must be the bound anchor ({end}); SQL:\n{sql}"
    );
}
