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

/// Quote a ClickHouse identifier (column name, table name) if it contains special characters.
///
/// ClickHouse requires backtick quoting for identifiers that contain:
/// - Dots (.)
/// - Spaces
/// - Hyphens (-)
/// - Other special characters
///
/// # Examples
/// ```
/// use clickgraph::clickhouse_query_generator::quote_identifier;
/// assert_eq!(quote_identifier("user_id"), "user_id");
/// assert_eq!(quote_identifier("id.orig_h"), "`id.orig_h`");
/// assert_eq!(quote_identifier("user-name"), "`user-name`");
/// ```
pub fn quote_identifier(name: &str) -> String {
    if name.contains('.')
        || name.contains(' ')
        || name.contains('-')
        || name.contains('(')
        || name.contains(')')
    {
        format!("`{}`", name)
    } else {
        name.to_string()
    }
}

/// Format a qualified column reference: table_alias.column_name
///
/// This function properly quotes the column name if it contains special characters.
///
/// # Examples
/// ```
/// use clickgraph::clickhouse_query_generator::qualified_column;
/// assert_eq!(qualified_column("t1", "user_id"), "t1.user_id");
/// assert_eq!(qualified_column("t1", "id.orig_h"), "t1.`id.orig_h`");
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
            Some("quantileExact(0.9)(t.x)".into())
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
