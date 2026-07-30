//! Common utilities for ClickHouse query generation

// ⚠️ NOTE: Literal rendering code appears in multiple places due to different Literal types:
//
// - crate::query_planner::logical_expr::Literal (used in to_sql.rs)
// - crate::render_plan::render_expr::Literal (used in to_sql_query.rs)
//
// These are structurally similar but different types, making consolidation complex.
// The rendering logic is duplicated across:
// 1. to_sql.rs (lines ~65-70): Handles LogicalExpr::Literal
// 2. to_sql_query.rs (lines ~1620-1630): Handles RenderExpr::Literal
//
// Future Improvement: Create a unified Literal trait that both types implement,
// enabling a single render_literal() function in this module.

/// Quote an identifier (column name, table name) if it contains special
/// characters, using the **active dialect's** identifier delimiter.
///
/// An identifier needs quoting when it contains any of:
/// - Dots (.)
/// - Spaces
/// - Hyphens (-)
/// - Parentheses
///
/// When quoting is needed the delimiter is dialect-specific — ClickHouse
/// double-quotes (`"id.orig_h"`), Spark/Databricks backticks
/// (`` `id.orig_h` ``) — routed through [`FunctionMapper::quote_alias`], which
/// also escapes any embedded delimiter. This keeps a CTE-build-stage reference
/// (Path C, `cte_extraction.rs`) byte-identical with the final-SELECT renderer
/// (Path A, `to_sql_query.rs`), which qualifies special-char columns via the
/// same dialect helper. Previously this hard-coded backticks for both dialects,
/// which was valid CH but drifted from Path A's `"…"` — a query could emit both
/// `` t2.`id.orig_h` `` (CTE body) and `t1."id.orig_h"` (SELECT). Plain
/// identifiers (no special char) are returned bare, unchanged.
///
/// # Examples
/// ```
/// use clickgraph::clickhouse_query_generator::quote_identifier;
/// // Default dialect (outside a query context) is ClickHouse → double-quotes.
/// assert_eq!(quote_identifier("user_id"), "user_id");
/// assert_eq!(quote_identifier("id.orig_h"), "\"id.orig_h\"");
/// assert_eq!(quote_identifier("user-name"), "\"user-name\"");
/// ```
pub fn quote_identifier(name: &str) -> String {
    if name.contains('.')
        || name.contains(' ')
        || name.contains('-')
        || name.contains('(')
        || name.contains(')')
    {
        crate::sql_generator::function_mapper::current_function_mapper().quote_alias(name)
    } else {
        name.to_string()
    }
}

/// Format a qualified column reference: table_alias.column_name
///
/// Quotes the column name (via [`quote_identifier`]) with the active dialect's
/// delimiter when it contains special characters.
///
/// # Examples
/// ```
/// use clickgraph::clickhouse_query_generator::qualified_column;
/// // Default dialect (outside a query context) is ClickHouse → double-quotes.
/// assert_eq!(qualified_column("t1", "user_id"), "t1.user_id");
/// assert_eq!(qualified_column("t1", "id.orig_h"), "t1.\"id.orig_h\"");
/// ```
pub fn qualified_column(table_alias: &str, column_name: &str) -> String {
    format!("{}.{}", table_alias, quote_identifier(column_name))
}

/// Emit a substring-containment predicate for Cypher `haystack CONTAINS needle`,
/// dialect-aware.
///
/// ClickHouse `position(haystack, needle)` and Spark/Databricks
/// `position(substr, str)` take their two arguments in OPPOSITE order, so the
/// operands are swapped when rendering for Databricks. Both return a 1-based
/// index (0 = not found), so the `> 0` test is identical.
pub fn contains_predicate(haystack: &str, needle: &str) -> String {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        // Spark: position(substr, str) — substring first.
        SqlDialect::Databricks => format!("(position({}, {}) > 0)", needle, haystack),
        // ClickHouse: position(haystack, needle) — haystack first.
        _ => format!("(position({}, {}) > 0)", haystack, needle),
    }
}

/// Emit a prefix-match predicate for Cypher `haystack STARTS WITH needle`,
/// dialect-aware.
///
/// ClickHouse spells it `startsWith(haystack, needle)` (camelCase);
/// Spark/Databricks has the builtin `startswith(str, prefix)` (lowercase, same
/// argument order). Pure name-case swap — both return a boolean. Emitted from
/// every `StartsWith` render site so the two dialects stay in sync.
pub fn starts_with_predicate(haystack: &str, needle: &str) -> String {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        SqlDialect::Databricks => format!("startswith({}, {})", haystack, needle),
        _ => format!("startsWith({}, {})", haystack, needle),
    }
}

/// Emit a suffix-match predicate for Cypher `haystack ENDS WITH needle`,
/// dialect-aware.
///
/// ClickHouse spells it `endsWith(haystack, needle)` (camelCase);
/// Spark/Databricks has the builtin `endswith(str, suffix)` (lowercase, same
/// argument order). Pure name-case swap — both return a boolean. Emitted from
/// every `EndsWith` render site so the two dialects stay in sync.
pub fn ends_with_predicate(haystack: &str, needle: &str) -> String {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        SqlDialect::Databricks => format!("endswith({}, {})", haystack, needle),
        _ => format!("endsWith({}, {})", haystack, needle),
    }
}

/// Render a Cypher `=~` regex match (`RegexMatch` operator) for the active dialect.
///
/// ClickHouse spells it `match(haystack, pattern)`; Spark/Databricks has no
/// `match` function and uses `rlike(str, regexp)` (both return a boolean).
/// Emitted from every `RegexMatch` render site so the two dialects stay in sync.
pub fn regex_match_predicate(haystack: &str, pattern: &str) -> String {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        SqlDialect::Databricks => format!("rlike({}, {})", haystack, pattern),
        _ => format!("match({}, {})", haystack, pattern),
    }
}

/// Render a Cypher `reduce(acc = init, x IN list | expr)` fold for the active
/// dialect, from its already-rendered component strings.
///
/// ClickHouse has `arrayFold((x, acc) -> expr, list, init)`; Spark/Databricks
/// has no `arrayFold` and uses `aggregate(list, init, (acc, x) -> expr)` (same
/// semantics, different arg order + spelling). Emitted from every `ReduceExpr`
/// render site so the two dialects stay in sync.
pub fn reduce_fold_sql(
    variable: &str,
    accumulator: &str,
    expr_sql: &str,
    list_sql: &str,
    init_sql: &str,
) -> String {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        SqlDialect::Databricks => format!(
            "aggregate({}, {}, ({}, {}) -> {})",
            list_sql, init_sql, accumulator, variable, expr_sql
        ),
        _ => format!(
            "arrayFold({}, {} -> {}, {}, {})",
            variable, accumulator, expr_sql, list_sql, init_sql
        ),
    }
}

/// Render a Cypher *simple* CASE (`CASE expr WHEN v1 THEN r1 ... ELSE d END`)
/// for the active dialect, from its already-rendered component strings.
///
/// ClickHouse spells the simple form as a function,
/// `caseWithExpression(expr, v1, r1, v2, r2, ..., default)`; Spark/Databricks
/// has no such function and uses the standard `CASE expr WHEN v THEN r ... ELSE
/// d END` syntax (which ClickHouse also accepts, but the function form is what
/// this pipeline has historically emitted for CH, so it is preserved
/// byte-identically here). Emitted from the `RenderExpr::Case` render site so
/// the two dialects stay in sync.
///
/// `case_expr` is the rendered subject expression, `when_then` the rendered
/// `(value, result)` branch pairs, and `default` the rendered ELSE result
/// (callers pass the fallback — `NULL` or an empty array — when the Cypher CASE
/// omits ELSE).
pub fn simple_case_sql(case_expr: &str, when_then: &[(String, String)], default: &str) -> String {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        SqlDialect::Databricks => {
            let mut sql = format!("CASE {}", case_expr);
            for (value, result) in when_then {
                sql.push_str(&format!(" WHEN {} THEN {}", value, result));
            }
            sql.push_str(&format!(" ELSE {} END", default));
            sql
        }
        // ClickHouse: caseWithExpression(expr, v1, r1, ..., default).
        _ => {
            let mut args = vec![case_expr.to_string()];
            for (value, result) in when_then {
                args.push(value.clone());
                args.push(result.clone());
            }
            args.push(default.to_string());
            format!("caseWithExpression({})", args.join(", "))
        }
    }
}

/// Resolve a SQL function name to its active-dialect spelling via the function
/// registry, falling back to `name` unchanged when it has no registry entry.
///
/// Lets the duplicate generic `ScalarFnCall` renderers (in `render_plan`) map
/// dialect-divergent names (e.g. `tuple` -> Spark `struct`) without each one
/// re-implementing the lookup. The canonical `RenderExpr::to_sql` arm already
/// routes through the registry directly; this is the shared shim for the others.
pub fn dialect_function_name(name: &str) -> String {
    use super::function_registry::get_function_mapping;
    match get_function_mapping(&name.to_lowercase()) {
        // Only remap pure name-swaps. A registry entry with an `arg_transform`
        // (e.g. `left`/`right` -> `substring(s, 1, n)`) also rewrites its
        // arguments; this shim renders args itself and cannot apply that
        // transform, so name-mapping such an entry would emit a wrong call.
        // Leave those raw — they are handled correctly by the canonical
        // `RenderExpr::to_sql` / `translate_scalar_function` paths.
        Some(mapping) if mapping.arg_transform.is_none() => mapping
            .name_for(crate::server::query_context::get_current_dialect())
            .to_string(),
        _ => name.to_string(),
    }
}

/// Intercept the openCypher percentile aggregates and render them through
/// `FunctionMapper::percentile_aggregate`, honoring the percentile argument
/// (#639). Returns `Some(sql)` for `percentileCont`/`percentileDisc` called
/// with exactly `(expr, percentile)`; `None` for any other function name or
/// arity, so the caller falls through to its normal (loud) handling — we never
/// emit a percentile call with a dropped or mis-placed argument.
///
/// `args_sql` are the already-rendered argument fragments in Cypher order:
/// `args_sql[0]` = value expression, `args_sql[1]` = percentile.
pub fn try_render_percentile(fn_name: &str, args_sql: &[String]) -> Option<String> {
    let continuous = match fn_name.to_lowercase().as_str() {
        "percentilecont" => true,
        "percentiledisc" => false,
        _ => return None,
    };
    // openCypher percentiles are strictly binary. A wrong arity is a genuine
    // error — fall through so the raw call surfaces a loud database error
    // rather than silently guessing.
    if args_sql.len() != 2 {
        return None;
    }
    let mapper = crate::sql_generator::function_mapper::current_function_mapper();
    Some(mapper.percentile_aggregate(&args_sql[0], &args_sql[1], continuous))
}

#[cfg(test)]
mod dialect_function_name_tests {
    use super::dialect_function_name;
    use crate::server::query_context::{with_query_context, QueryContext};
    use crate::sql_generator::SqlDialect;

    #[tokio::test]
    async fn maps_pure_name_swaps_only() {
        // tuple is a pure name-swap -> mapped per dialect.
        assert_eq!(dialect_function_name("tuple"), "tuple"); // default = ClickHouse
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let dbx_tuple = with_query_context(ctx, async { dialect_function_name("tuple") }).await;
        assert_eq!(dbx_tuple, "struct");

        // left/right have an arg_transform (-> substring with rewritten args), so
        // this shim must NOT name-map them — it would emit substring() without the
        // transform. They stay raw on both dialects.
        assert_eq!(dialect_function_name("left"), "left");
        assert_eq!(dialect_function_name("right"), "right");
        let ctx2 = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let dbx_left = with_query_context(ctx2, async { dialect_function_name("left") }).await;
        assert_eq!(dbx_left, "left");

        // Unknown functions fall through unchanged.
        assert_eq!(dialect_function_name("some_native_fn"), "some_native_fn");
    }
}

#[cfg(test)]
mod try_render_percentile_tests {
    use super::try_render_percentile;
    use crate::server::query_context::{with_query_context, QueryContext};
    use crate::sql_generator::SqlDialect;

    #[test]
    fn renders_parametric_quantile_on_clickhouse_default() {
        // Default (no scope) = ClickHouse: parametric quantile forms (#639).
        assert_eq!(
            try_render_percentile("percentilecont", &["t.x".into(), "0.9".into()]),
            Some("quantileExactInclusive(0.9)(t.x)".into())
        );
        assert_eq!(
            try_render_percentile("percentiledisc", &["t.x".into(), "0.9".into()]),
            Some(
                "arrayElementOrNull(arraySort(groupArray(t.x)), greatest(1, toUInt32(ceil(0.9 * count(t.x)))))"
                    .into()
            )
        );
    }

    #[tokio::test]
    async fn renders_spark_forms_under_databricks() {
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let sql = with_query_context(ctx, async {
            try_render_percentile("percentilecont", &["t.x".into(), "0.9".into()])
        })
        .await;
        assert_eq!(sql, Some("percentile(t.x, 0.9)".into()));
    }

    #[test]
    fn returns_none_for_non_percentile_or_wrong_arity() {
        // Non-percentile name → None (caller handles it normally).
        assert_eq!(try_render_percentile("avg", &["t.x".into()]), None);
        // Wrong arity → None: never emit a percentile with a dropped/guessed
        // arg — the caller falls through to a loud error (#639).
        assert_eq!(
            try_render_percentile("percentilecont", &["t.x".into()]),
            None
        );
        assert_eq!(
            try_render_percentile(
                "percentiledisc",
                &["t.x".into(), "0.9".into(), "extra".into()]
            ),
            None
        );
    }
}

#[cfg(test)]
mod simple_case_sql_tests {
    use super::simple_case_sql;
    use crate::server::query_context::{with_query_context, QueryContext};
    use crate::sql_generator::SqlDialect;

    fn branches() -> Vec<(String, String)> {
        vec![
            ("'Alice'".into(), "'Admin'".into()),
            ("'Bob'".into(), "'User'".into()),
        ]
    }

    #[test]
    fn clickhouse_default_uses_case_with_expression() {
        // Default (no scope) = ClickHouse: the historical function form,
        // byte-identical to what the pipeline emitted before this helper.
        assert_eq!(
            simple_case_sql("n.name", &branches(), "'Guest'"),
            "caseWithExpression(n.name, 'Alice', 'Admin', 'Bob', 'User', 'Guest')"
        );
    }

    #[tokio::test]
    async fn databricks_uses_standard_case_syntax() {
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let sql = with_query_context(ctx, async {
            simple_case_sql("n.name", &branches(), "'Guest'")
        })
        .await;
        // Spark has no caseWithExpression; standard CASE-expr syntax instead.
        assert_eq!(
            sql,
            "CASE n.name WHEN 'Alice' THEN 'Admin' WHEN 'Bob' THEN 'User' ELSE 'Guest' END"
        );
    }
}

#[cfg(test)]
mod string_predicate_tests {
    use super::{ends_with_predicate, starts_with_predicate};
    use crate::server::query_context::{with_query_context, QueryContext};
    use crate::sql_generator::SqlDialect;

    #[test]
    fn clickhouse_default_uses_camelcase_functions() {
        // Default (no scope) = ClickHouse: the historical camelCase spelling,
        // byte-identical to what the pipeline emitted before these helpers.
        assert_eq!(
            starts_with_predicate("n.name", "'Al'"),
            "startsWith(n.name, 'Al')"
        );
        assert_eq!(
            ends_with_predicate("n.name", "'ce'"),
            "endsWith(n.name, 'ce')"
        );
    }

    #[tokio::test]
    async fn databricks_uses_lowercase_builtins() {
        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let (starts, ends) = with_query_context(ctx, async {
            (
                starts_with_predicate("n.name", "'Al'"),
                ends_with_predicate("n.name", "'ce'"),
            )
        })
        .await;
        // Spark builtins are lowercase, same arg order.
        assert_eq!(starts, "startswith(n.name, 'Al')");
        assert_eq!(ends, "endswith(n.name, 'ce')");
    }
}
