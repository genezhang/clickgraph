//! Dynamic PREWHERE pushdown for ClickHouse (people-perf, opt-in).
//!
//! When `CLICKGRAPH_PREWHERE=1`, eligible single-table filter conjuncts move
//! from `WHERE` to `PREWHERE`: ClickHouse reads only the filter columns first,
//! drops non-matching rows, then reads the remaining columns. The win scales
//! with table width and size, so wide 100M+ row graphs gain most.
//!
//! Conservative v1 rules (see SPEC-people-perf.md):
//! - ClickHouse dialect only, via the [`SqlDialect::supports_prewhere`]
//!   capability (axis-dispatch: the renderer asks the dialect).
//! - Bound-plan path only (already-resolved `LogicalPlan::Empty` table refs);
//!   the legacy `ViewScan` pipeline is being deleted.
//! - The table must come from the catalog as MergeTree-family, with no FINAL
//!   (resolved flag or engine-implied): PREWHERE filters rows before
//!   merge-time deduplication, so FINAL/dedup engines would return stale
//!   versions. Unknown engines and non-MergeTree engines stay on WHERE.
//! - Only top-level AND conjuncts shaped as plain-column `=`, `<>`, `<`,
//!   `<=`, `>`, `>=` against a literal/parameter, `IN` (literal/parameter
//!   list), `IS [NOT] NULL`. Everything else stays in `WHERE`.
//! - Default OFF with byte-identical output.

use crate::graph_catalog::engine_detection::TableEngine;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::{
    render_expr::{Operator, PropertyAccess, RenderExpr},
    ArrayJoinItem, FilterItems, FromTableItem, JoinItems,
};

/// Env toggle. Accepts `1`/`true` (case-insensitive). Default OFF.
pub(crate) fn prewhere_enabled() -> bool {
    match std::env::var("CLICKGRAPH_PREWHERE") {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true"
        }
        Err(_) => false,
    }
}

/// Flatten a top-level AND tree into borrowed conjuncts (single pass-through
/// for non-AND expressions).
pub(crate) fn split_conjuncts(expr: &RenderExpr) -> Vec<&RenderExpr> {
    let mut out = Vec::new();
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        if let RenderExpr::OperatorApplicationExp(op) = e {
            if op.operator == Operator::And {
                stack.extend(op.operands.iter());
                continue;
            }
        }
        out.push(e);
    }
    out.reverse();
    out
}

/// The SQL alias predicates must use for this plan to qualify: exactly one
/// FROM ref stamped `LogicalPlan::Empty` (bound pipeline's resolved table
/// ref) with a plain `db.table` name (no parameter list), no joins, no ARRAY
/// JOINs. CTE refs (no dot), parameterized views/table functions (`(`),
/// legacy `ViewScan` sources, and missing aliases never qualify.
/// Returns `None` (feature does not apply) otherwise.
pub(crate) fn single_table_alias(
    from: &FromTableItem,
    joins: &JoinItems,
    array_join: &ArrayJoinItem,
) -> Option<String> {
    if !joins.0.is_empty() || !array_join.0.is_empty() {
        return None;
    }
    let from_ref = from.0.as_ref()?;
    // Bound-plan path only: the legacy ViewScan pipeline is being deleted.
    if !matches!(&*from_ref.source, LogicalPlan::Empty) {
        return None;
    }
    if from_ref.name.contains('(') || !from_ref.name.contains('.') {
        return None;
    }
    from_ref.alias.clone()
}

/// Catalog decision for one table: `use_final` is the resolved FINAL flag
/// (explicit override wins). Engines requiring FINAL for correctness, unknown
/// engines, and non-MergeTree engines stay on WHERE — filtering rows before
/// merge-time deduplication can return stale versions, and other engines
/// reject or mishandle PREWHERE.
pub(crate) fn engine_allows_prewhere(use_final: bool, engine: Option<&TableEngine>) -> bool {
    if use_final {
        return false;
    }
    match engine {
        Some(e) if e.requires_final_for_correctness() => false,
        Some(TableEngine::Other(_)) => false,
        Some(_) => true,
        None => false,
    }
}

/// Look up `(resolved use_final, engine)` for a `db.table` name in the
/// catalog (nodes then edges). Returns `None` when the table is unknown —
/// callers deny.
fn lookup_table(table_name: &str, schema: &GraphSchema) -> Option<(bool, Option<TableEngine>)> {
    for ns in schema.all_node_schemas().values() {
        if format!("{}.{}", ns.database, ns.table_name) == table_name {
            return Some((ns.should_use_final(), ns.engine.clone()));
        }
    }
    for rs in schema.get_relationships_schemas().values() {
        if format!("{}.{}", rs.database, rs.table_name) == table_name {
            return Some((rs.should_use_final(), rs.engine.clone()));
        }
    }
    None
}

/// Whether the FROM table of this plan may move filters to PREWHERE:
/// single bound-plan table + catalog allows it.
fn table_allows_prewhere(from: &FromTableItem, schema: &GraphSchema) -> bool {
    let from_ref = match from.0.as_ref() {
        Some(r) => r,
        None => return false,
    };
    if from_ref.use_final {
        return false;
    }
    match lookup_table(&from_ref.name, schema) {
        Some((use_final, engine)) => engine_allows_prewhere(use_final, engine.as_ref()),
        None => false,
    }
}

/// A plain `alias.column` reference to the single FROM table.
fn plain_from_column(expr: &RenderExpr, from_alias: &str) -> bool {
    matches!(
        expr,
        RenderExpr::PropertyAccessExp(PropertyAccess { table_alias, .. })
        if table_alias.0 == from_alias
    )
}

/// Literal or query parameter — safe to compare in PREWHERE.
fn scalar_const(expr: &RenderExpr) -> bool {
    matches!(expr, RenderExpr::Literal(_) | RenderExpr::Parameter(_))
}

/// Whether one flattened conjunct may move to PREWHERE. `from_alias` is the
/// single FROM alias from [`single_table_alias`].
pub(crate) fn conjunct_eligible(conjunct: &RenderExpr, from_alias: &str) -> bool {
    let RenderExpr::OperatorApplicationExp(op) = conjunct else {
        return false;
    };
    match op.operator {
        Operator::Equal
        | Operator::NotEqual
        | Operator::LessThan
        | Operator::LessThanEqual
        | Operator::GreaterThan
        | Operator::GreaterThanEqual => {
            if op.operands.len() != 2 {
                return false;
            }
            let (lhs, rhs) = (&op.operands[0], &op.operands[1]);
            (plain_from_column(lhs, from_alias) && scalar_const(rhs))
                || (scalar_const(lhs) && plain_from_column(rhs, from_alias))
        }
        Operator::In => {
            if op.operands.len() != 2 {
                return false;
            }
            let (lhs, rhs) = (&op.operands[0], &op.operands[1]);
            if !plain_from_column(lhs, from_alias) {
                return false;
            }
            match rhs {
                RenderExpr::List(items) => items.iter().all(scalar_const),
                _ => false,
            }
        }
        Operator::IsNull | Operator::IsNotNull => {
            op.operands.len() == 1 && plain_from_column(&op.operands[0], from_alias)
        }
        _ => false,
    }
}

/// Split a filter into `(prewhere_sql, where_sql)` rendered fragments.
/// Conjuncts render through the same `to_sql()` as the WHERE path, so a
/// moved conjunct is textually identical to its unmoved form.
/// Returns `None` when disabled or when no conjunct is eligible, so callers
/// fall back to the existing `WHERE` rendering byte-identically.
pub(crate) fn split_filters(
    filter: &RenderExpr,
    from_alias: Option<&str>,
    has_joins: bool,
    has_array_join: bool,
    enabled: bool,
) -> Option<(String, String)> {
    if !enabled || has_joins || has_array_join {
        return None;
    }
    let from_alias = from_alias?;
    let mut prewhere = Vec::new();
    let mut where_rest = Vec::new();
    for conjunct in split_conjuncts(filter) {
        if conjunct_eligible(conjunct, from_alias) {
            prewhere.push(conjunct.to_sql());
        } else {
            where_rest.push(conjunct.to_sql());
        }
    }
    if prewhere.is_empty() {
        return None;
    }
    Some((prewhere.join(" AND "), where_rest.join(" AND ")))
}

/// Plan-level entry point used by the final-SELECT emitters. Returns `None`
/// when the feature is off, the dialect has no PREWHERE, or the plan/table
/// does not qualify.
pub(crate) fn filters_with_prewhere(
    from: &FromTableItem,
    joins: &JoinItems,
    array_join: &ArrayJoinItem,
    filters: &FilterItems,
    enabled: bool,
) -> Option<(String, String)> {
    use crate::server::query_context::get_current_dialect;
    if !enabled {
        return None;
    }
    // Axis-dispatch: the renderer asks the dialect (Databricks: none).
    if !get_current_dialect().supports_prewhere() {
        return None;
    }
    let alias = single_table_alias(from, joins, array_join)?;
    let schema = crate::server::query_context::get_current_schema()?;
    if !table_allows_prewhere(from, &schema) {
        return None;
    }
    let filter = filters.0.as_ref()?;
    split_filters(
        filter,
        Some(&alias),
        !joins.0.is_empty(),
        !array_join.0.is_empty(),
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::render_plan::render_expr::{Literal, OperatorApplication, ScalarFnCall, TableAlias};

    fn col(alias: &str, column: &str) -> RenderExpr {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(column.to_string()),
        })
    }

    fn lit(v: i64) -> RenderExpr {
        RenderExpr::Literal(Literal::Integer(v))
    }

    fn binop(op: Operator, lhs: RenderExpr, rhs: RenderExpr) -> RenderExpr {
        RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: op,
            operands: vec![lhs, rhs],
        })
    }

    fn and(a: RenderExpr, b: RenderExpr) -> RenderExpr {
        binop(Operator::And, a, b)
    }

    #[test]
    fn prewhere_enabled_defaults_off() {
        std::env::remove_var("CLICKGRAPH_PREWHERE");
        assert!(!prewhere_enabled());
    }

    #[test]
    fn split_conjuncts_flattens_and() {
        let e = and(
            binop(Operator::Equal, col("u", "a"), lit(1)),
            and(
                binop(Operator::Equal, col("u", "b"), lit(2)),
                binop(Operator::Equal, col("u", "c"), lit(3)),
            ),
        );
        assert_eq!(split_conjuncts(&e).len(), 3);
    }

    #[test]
    fn split_conjuncts_single_passthrough() {
        let e = binop(Operator::Equal, col("u", "a"), lit(1));
        assert_eq!(split_conjuncts(&e).len(), 1);
    }

    #[test]
    fn eligible_plain_equality() {
        let e = binop(Operator::Equal, col("u", "age"), lit(30));
        assert!(conjunct_eligible(&e, "u"));
    }

    #[test]
    fn eligible_const_on_left() {
        let e = binop(Operator::GreaterThan, lit(20), col("u", "age"));
        assert!(conjunct_eligible(&e, "u"));
    }

    #[test]
    fn eligible_in_list() {
        let e = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::In,
            operands: vec![col("u", "country"), RenderExpr::List(vec![lit(1), lit(2)])],
        });
        assert!(conjunct_eligible(&e, "u"));
    }

    #[test]
    fn eligible_is_null() {
        let e = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNull,
            operands: vec![col("u", "email")],
        });
        assert!(conjunct_eligible(&e, "u"));
    }

    #[test]
    fn reject_wrong_alias() {
        let e = binop(Operator::Equal, col("v", "age"), lit(30));
        assert!(!conjunct_eligible(&e, "u"));
    }

    #[test]
    fn reject_or_nested() {
        let e = binop(
            Operator::Or,
            binop(Operator::Equal, col("u", "a"), lit(1)),
            binop(Operator::Equal, col("u", "b"), lit(2)),
        );
        assert!(!conjunct_eligible(&e, "u"));
    }

    #[test]
    fn reject_function_wrapped_column() {
        let e = binop(
            Operator::Equal,
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "toString".to_string(),
                args: vec![col("u", "age")],
            }),
            lit(30),
        );
        assert!(!conjunct_eligible(&e, "u"));
    }

    #[test]
    fn reject_raw_and_subquery_shapes() {
        assert!(!conjunct_eligible(
            &RenderExpr::Raw("u.age = 30".to_string()),
            "u"
        ));
        let nested = and(
            binop(Operator::Equal, col("u", "a"), lit(1)),
            binop(Operator::Equal, col("u", "b"), lit(2)),
        );
        assert!(!conjunct_eligible(&nested, "u"));
    }

    #[test]
    fn split_filters_moves_eligible_only() {
        let filter = and(
            binop(Operator::Equal, col("u", "country"), lit(7)),
            binop(
                Operator::Equal,
                RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "toString".to_string(),
                    args: vec![col("u", "age")],
                }),
                lit(30),
            ),
        );
        let (pre, whr) =
            split_filters(&filter, Some("u"), false, false, true).expect("one eligible conjunct");
        assert!(pre.contains("country"), "prewhere holds it: {pre}");
        assert!(whr.contains("toString"), "rest stays: {whr}");
    }

    #[test]
    fn split_filters_off_is_none() {
        let filter = binop(Operator::Equal, col("u", "country"), lit(7));
        assert!(split_filters(&filter, Some("u"), false, false, false).is_none());
    }

    #[test]
    fn split_filters_no_alias_is_none() {
        let filter = binop(Operator::Equal, col("u", "country"), lit(7));
        assert!(split_filters(&filter, None, false, false, true).is_none());
    }

    #[test]
    fn split_filters_joins_is_none() {
        let filter = binop(Operator::Equal, col("u", "country"), lit(7));
        assert!(split_filters(&filter, Some("u"), true, false, true).is_none());
    }

    #[test]
    fn split_filters_nothing_eligible_is_none() {
        let filter = binop(
            Operator::Or,
            binop(Operator::Equal, col("u", "a"), lit(1)),
            binop(Operator::Equal, col("u", "b"), lit(2)),
        );
        assert!(split_filters(&filter, Some("u"), false, false, true).is_none());
    }

    #[test]
    fn moved_conjunct_renders_identically() {
        // Review item 4: a moved conjunct must be textually identical to its
        // unmoved form (same renderer, no rewriting in between).
        let conjunct = binop(Operator::Equal, col("u", "country"), lit(7));
        let filter = and(
            conjunct.clone(),
            binop(Operator::GreaterThan, col("u", "age"), lit(30)),
        );
        let (pre, whr) =
            split_filters(&filter, Some("u"), false, false, true).expect("both eligible");
        assert_eq!(pre, "u.country = 7 AND u.age > 30");
        assert_eq!(whr, "");
        // Each piece equals its standalone rendering.
        assert_eq!(conjunct.to_sql(), "u.country = 7");
    }

    fn table_from(name: &str, alias: Option<&str>) -> FromTableItem {
        FromTableItem(Some(crate::render_plan::ViewTableRef {
            source: std::sync::Arc::new(LogicalPlan::Empty),
            name: name.to_string(),
            alias: alias.map(str::to_string),
            use_final: false,
        }))
    }

    fn no_joins() -> (JoinItems, ArrayJoinItem) {
        (JoinItems(vec![]), ArrayJoinItem(vec![]))
    }

    #[test]
    fn bound_empty_source_dotted_table_accepted() {
        let (joins, array) = no_joins();
        assert_eq!(
            single_table_alias(&table_from("e2e.wide_users", Some("v0")), &joins, &array),
            Some("v0".to_string())
        );
    }

    #[test]
    fn cte_name_rejected() {
        let (joins, array) = no_joins();
        assert_eq!(
            single_table_alias(&table_from("with_x_cte_3", Some("v0")), &joins, &array),
            None
        );
    }

    #[test]
    fn parameterized_view_rejected() {
        let (joins, array) = no_joins();
        assert_eq!(
            single_table_alias(
                &table_from("db.view(tenant_id = 'acme')", Some("v0")),
                &joins,
                &array
            ),
            None
        );
    }

    #[test]
    fn missing_alias_rejected() {
        let (joins, array) = no_joins();
        assert_eq!(
            single_table_alias(&table_from("e2e.wide_users", None), &joins, &array),
            None
        );
    }

    #[test]
    fn engine_decision_matrix() {
        use TableEngine::*;
        // FINAL-resolved refs never move (pre-merge filtering hazard).
        assert!(!engine_allows_prewhere(true, Some(&MergeTree)));
        // Unknown engine: catalog decides, and it has no data.
        assert!(!engine_allows_prewhere(false, None));
        // Non-MergeTree engines reject or mishandle PREWHERE.
        assert!(!engine_allows_prewhere(
            false,
            Some(&Other("Distributed".to_string()))
        ));
        assert!(!engine_allows_prewhere(
            false,
            Some(&Other("Buffer".to_string()))
        ));
        // Dedup/collapsing engines need merge-time resolution.
        assert!(!engine_allows_prewhere(
            false,
            Some(&ReplacingMergeTree {
                version_column: None
            })
        ));
        assert!(!engine_allows_prewhere(
            false,
            Some(&CollapsingMergeTree {
                sign_column: "s".to_string()
            })
        ));
        // Plain MergeTree (and siblings without merge hazards) move.
        assert!(engine_allows_prewhere(false, Some(&MergeTree)));
        assert!(engine_allows_prewhere(false, Some(&AggregatingMergeTree)));
        assert!(engine_allows_prewhere(
            false,
            Some(&SummingMergeTree {
                sum_columns: vec![]
            })
        ));
    }

    #[test]
    fn dialect_capability() {
        use crate::sql_generator::SqlDialect;
        assert!(SqlDialect::ClickHouse.supports_prewhere());
        assert!(!SqlDialect::Databricks.supports_prewhere());
    }
}
