//! Pure utility functions for plan building
//!
//! This module contains utility functions that have no dependencies on LogicalPlan
//! or complex state. These are safe to extract early in the refactoring process.
//!
//! Functions in this module should be:
//! - Pure (no side effects)
//! - Independent of LogicalPlan structure
//! - Reusable across different builder modules
//!
//! # File Size — Phase 2 dedup/split in progress (see docs/design/REFACTORING_SAFETY_PLAN.md)
//!
//! The January 29, 2026 investigation recorded here previously claimed "0 truly dead
//! functions" and recommended against splitting this file. Both claims are now known
//! to be stale: Phase 1 (2026-07-12) and Phase 2 slices 1-3b (2026-07-13) deleted
//! ~35 confirmed-dead functions from this file, verified by zero-callers grep across
//! the whole workspace (a module-level `#![allow(dead_code)]` used to mask all of this
//! from the compiler; it was removed once the sweep found zero remaining warnings —
//! any NEW dead code introduced here from now on will show up as a normal compiler
//! warning). The file has also grown substantially since the January investigation,
//! not shrunk. Treat any dead/live function-count claim as needing fresh verification,
//! not this comment.
//!
//! Phase 2 of the ongoing refactor plan is actively deduping and will eventually split
//! this file along the seams identified there (vlp_rewrite, cte_rewrite,
//! clause_extractors, plan_predicates, with_to_cte, pattern_comprehension_sql).
//!

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::GraphSchema;
use crate::query_planner::join_context::{
    VlpPosition, VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN,
};
use crate::query_planner::logical_expr::{Direction, LogicalExpr};
use crate::query_planner::logical_plan::{GraphRel, LogicalPlan};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::render_plan::plan_builder::RenderPlanBuilder;
use crate::sql_generator::function_mapper::current_function_mapper;
use std::collections::{HashMap, HashSet};
// Only the `#[cfg(test)]` modules below use this via `use super::*` — every
// live (non-test) call site now has its own local `use std::sync::Arc;`
// (see e.g. `update_graph_joins_cte_refs`, `replace_with_clause_with_cte_reference_v2`).
#[cfg(test)]
use std::sync::Arc;

use crate::render_plan::cte_extraction::{
    extract_relationship_columns, rel_type_to_table_name, table_to_id_column, RelationshipColumns,
};
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::render_expr::{
    AggregateFnCall, Column, ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess,
    RenderCase, RenderExpr, ScalarFnCall, TableAlias,
};
use crate::render_plan::JoinType;
use crate::render_plan::SelectItem;
use crate::render_plan::{Cte, CteContent, Join, RenderPlan, SelectItems};
// P2.6 slice 4: after `build_chained_with_match_cte_plan` moved to `with_to_cte`,
// these render-plan container types + `ViewTableRef` are used only by this file's
// `#[cfg(test)]` module (the `build_cte_column_map` fixtures); gate them so the
// non-test lib build stays warning-free.
#[cfg(test)]
use crate::render_plan::ViewTableRef;
#[cfg(test)]
use crate::render_plan::{
    ArrayJoinItem, CteItems, FilterItems, FromTableItem, GroupByExpressions, JoinItems, LimitItem,
    OrderByItems, SkipItem, UnionItems,
};
use crate::utils::cte_column_naming::{cte_column_name, parse_cte_column};
use crate::utils::cte_naming::is_generated_cte_name;

// P2.10 import hygiene: this file reaches a handful of `plan_builder_helpers` /
// `alias_utils` helpers by bare name — previously via two `*` globs. Named
// imports (the exact set the compiler requires) so shadowing becomes visible;
// other helpers are reached by explicit `super::…::` paths.
use super::plan_builder_helpers::{
    denorm_scan_cte_anchor_id_property, denorm_scan_cte_anchor_properties,
    extract_parameterized_table_ref, extract_table_name, get_graph_rel_from_plan,
};
use super::utils::alias_utils::find_label_for_alias;
// `get_anchor_alias_from_plan` / `strip_database_prefix` are used only by the
// `#[cfg(test)]` module below — gate them so the non-test lib build stays clean.
#[cfg(test)]
use super::utils::alias_utils::{get_anchor_alias_from_plan, strip_database_prefix};

type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

// P2.1 (REFACTORING_SAFETY_PLAN.md §5.1): the VLP expression-rewriting group
// was moved verbatim to `render_plan::vlp_rewrite`. Re-exported here during the
// transition so the (narrow) existing callers on this path keep resolving.
pub(crate) use super::vlp_rewrite::{
    extract_vlp_alias_mappings, rewrite_render_expr_for_vlp_with_endpoint_info,
    rewrite_render_expr_for_vlp_with_from_alias, rewrite_vlp_aggregate_aliases,
};

// P2.3 (REFACTORING_SAFETY_PLAN.md §5.1): the clause-extractor group moved to
// `clause_extractors.rs`; re-exported here so existing
// `super::plan_builder_utils::extract_*` call sites resolve during the
// transition.
pub(crate) use super::clause_extractors::{
    extract_having, extract_limit, extract_order_by, extract_skip, extract_sorted_properties,
};

// P2.4 (REFACTORING_SAFETY_PLAN.md §5.1): the WITH-detection predicate group
// moved to `plan_predicates.rs`; re-exported here so existing
// `super::plan_builder_utils::*` call sites resolve during the transition.
// (`has_with_clause_in_tree` has no production caller here — only the test
// module below uses it, importing it directly — so it is not re-exported.)
pub(crate) use super::plan_predicates::has_with_clause_in_graph_rel;
// `plan_contains_with_clause`'s only remaining caller in this file after P2.6
// slice 4 (build_chained moved out) is the `#[cfg(test)]` characterization module
// below; gate the re-export so the non-test lib build doesn't flag it unused.
#[cfg(test)]
pub(crate) use super::plan_predicates::plan_contains_with_clause;

// P2.5 (REFACTORING_SAFETY_PLAN.md §5.1): CTE-rewrite functions moved to
// `cte_rewrite.rs`; re-exported here so remaining in-module / external call sites
// resolve during the transition. Sub-slice A: the expression-rewriting cluster
// (`join_builder`'s external call + the two internal `_join` sites). The
// join-condition / remap group that P2.6 slice 4 moved out with
// `build_chained_with_match_cte_plan` (`collect_with_cte_table_aliases`,
// `remap_cte_names_in_render_plan`, `rewrite_join_conditions_for_cte_aliases`,
// `strip_table_alias_from_resolved`) is now imported directly by `with_to_cte`,
// so it is no longer re-exported here.
pub(crate) use super::cte_rewrite::{
    rewrite_operator_application_for_cte, rewrite_operator_application_for_cte_join,
};

// P2.5 sub-slice D (REFACTORING_SAFETY_PLAN.md §5.1): the LogicalPlan-level
// CTE-reference rewriters moved to `cte_graph_joins_rewrite.rs`. Its sole former
// in-module caller here was `build_chained_with_match_cte_plan`
// (`update_graph_joins_cte_refs`, 2 sites), which P2.6 slice 4 moved to
// `with_to_cte` (now importing it directly) — so the re-export is dropped.

/// Build property mapping from select items for CTE column resolution.
/// Maps (alias, property) -> column_name for property access resolution.
///
/// This function handles three patterns:
/// 1. "alias.property" (dotted, used in VLP CTEs)
/// 2. "p{N}_alias_property" (new unambiguous CTE format)
/// 3. "alias_property" (legacy underscore, fallback for backward compat)
/// 4. No separator - aggregate column like "friends" from collect()
pub fn build_property_mapping_from_columns(
    select_items: &[SelectItem],
) -> HashMap<(String, String), String> {
    use crate::render_plan::render_expr::RenderExpr;
    let mut property_mapping = HashMap::new();

    for item in select_items {
        if let Some(col_alias) = &item.col_alias {
            let col_name = &col_alias.0;

            // Pattern 1: "alias.property" (dotted, used in VLP CTEs)
            if let Some(dot_pos) = col_name.find('.') {
                let alias = col_name[..dot_pos].to_string();
                let property = col_name[dot_pos + 1..].to_string();
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping: ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );

                // ALSO add mapping from ClickHouse column name (from expression) to CTE column
                if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                    if let PropertyValue::Column(ref expr_col) = pa.column {
                        if expr_col != &property {
                            property_mapping
                                .insert((alias.clone(), expr_col.clone()), col_name.clone());
                            log::debug!(
                                "  Property mapping (clickhouse): ({}, {}) → {}",
                                alias,
                                expr_col,
                                col_name
                            );
                        }
                    }
                }
            }
            // Pattern 2: "p{N}_alias_property" (new unambiguous CTE format)
            else if let Some((alias, property)) = parse_cte_column(col_name) {
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping (p{{N}}): ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );

                // ALSO add mapping from ClickHouse column name (from expression) to CTE column
                if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                    if let PropertyValue::Column(ref expr_col) = pa.column {
                        if expr_col != &property {
                            property_mapping
                                .insert((alias.clone(), expr_col.clone()), col_name.clone());
                            log::debug!(
                                "  Property mapping (clickhouse): ({}, {}) → {}",
                                alias,
                                expr_col,
                                col_name
                            );
                        }
                    }
                }
            }
            // Pattern 3: "alias_property" (legacy underscore fallback)
            else if let Some(underscore_pos) = col_name.find('_') {
                let alias = col_name[..underscore_pos].to_string();
                let property = col_name[underscore_pos + 1..].to_string();
                property_mapping.insert((alias.clone(), property.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping (legacy underscore): ({}, {}) → {}",
                    alias,
                    property,
                    col_name
                );

                // ALSO add mapping from ClickHouse column name (from expression) to CTE column
                if let RenderExpr::PropertyAccessExp(ref pa) = item.expression {
                    if let PropertyValue::Column(ref expr_col) = pa.column {
                        if expr_col != &property {
                            property_mapping
                                .insert((alias.clone(), expr_col.clone()), col_name.clone());
                            log::debug!(
                                "  Property mapping (clickhouse): ({}, {}) → {}",
                                alias,
                                expr_col,
                                col_name
                            );
                        }
                    }
                }
            }
            // Pattern 4: No separator - aggregate column like "friends" from collect()
            // Store with empty alias so ARRAY JOIN can find it: ("", column_name) → column_name
            else {
                property_mapping.insert(("".to_string(), col_name.clone()), col_name.clone());
                log::debug!(
                    "  Property mapping (aggregate): (\"\", {}) → {}",
                    col_name,
                    col_name
                );
            }
        }
    }

    log::info!(
        "Built property mapping with {} entries",
        property_mapping.len()
    );
    property_mapping
}

pub fn extract_correlation_predicates(
    plan: &LogicalPlan,
) -> Vec<crate::query_planner::logical_expr::LogicalExpr> {
    let mut predicates = vec![];

    match plan {
        LogicalPlan::GraphJoins(gj) => {
            log::debug!("🔍 extract_correlation_predicates: Found GraphJoins with {} correlation predicates",
                       gj.correlation_predicates.len());
            predicates.extend(gj.correlation_predicates.clone());
            predicates.extend(extract_correlation_predicates(&gj.input));
        }
        LogicalPlan::GraphRel(gr) => {
            predicates.extend(extract_correlation_predicates(&gr.left));
            predicates.extend(extract_correlation_predicates(&gr.center));
            predicates.extend(extract_correlation_predicates(&gr.right));
        }
        LogicalPlan::GraphNode(gn) => {
            predicates.extend(extract_correlation_predicates(&gn.input));
        }
        LogicalPlan::WithClause(wc) => {
            predicates.extend(extract_correlation_predicates(&wc.input));
        }
        LogicalPlan::CartesianProduct(cp) => {
            // CRITICAL: Extract join_condition from CartesianProduct - this is where
            // cross-table WITH correlation predicates (e.g., a.user_id = c.user_id) are stored!
            if let Some(ref join_cond) = cp.join_condition {
                log::debug!(
            "🔍 extract_correlation_predicates: Found CartesianProduct.join_condition: {:?}",
                    join_cond
                );
                predicates.push(join_cond.clone());
            }
            predicates.extend(extract_correlation_predicates(&cp.left));
            predicates.extend(extract_correlation_predicates(&cp.right));
        }
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                predicates.extend(extract_correlation_predicates(input));
            }
        }
        // CRITICAL: Handle wrapper types that may contain CartesianProduct
        LogicalPlan::Projection(proj) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through Projection");
            predicates.extend(extract_correlation_predicates(&proj.input));
        }
        LogicalPlan::Limit(lim) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through Limit");
            predicates.extend(extract_correlation_predicates(&lim.input));
        }
        LogicalPlan::OrderBy(ob) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through OrderBy");
            predicates.extend(extract_correlation_predicates(&ob.input));
        }
        LogicalPlan::Filter(f) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through Filter");
            predicates.extend(extract_correlation_predicates(&f.input));
        }
        LogicalPlan::GroupBy(gb) => {
            log::debug!("🔍 extract_correlation_predicates: Recursing through GroupBy");
            predicates.extend(extract_correlation_predicates(&gb.input));
        }
        _ => {
            log::debug!("🔍 extract_correlation_predicates: Unhandled plan type, not recursing");
        }
    }

    log::info!(
        "🔍 extract_correlation_predicates: Returning {} predicates total",
        predicates.len()
    );
    predicates
}

/// Rewrite an OperatorApplication for CTE JOIN conditions.
/// Find the ID column name in a WITH CTE for a given alias
/// Looks for columns like `{alias}_user_id`, `{alias}_id`, etc.
pub(crate) fn find_id_column_in_cte(
    cte_name: &str,
    cte_alias: &str,
    ctes: &super::CteItems,
) -> String {
    for cte in &ctes.0 {
        if cte.cte_name == cte_name {
            // Look for ID columns in the CTE's column metadata
            for col in &cte.columns {
                let col_name = &col.cte_column_name;
                if col_name.as_str() == format!("{}_user_id", cte_alias)
                    || col_name.as_str() == format!("{}_id", cte_alias)
                {
                    return col_name.clone();
                }
            }
            // Fallback: check for any column ending in "_id" or "_user_id"
            for col in &cte.columns {
                let col_name = &col.cte_column_name;
                if col_name.starts_with(&format!("{}_", cte_alias))
                    && (col_name.ends_with("_id") || col_name.ends_with("_user_id"))
                {
                    return col_name.clone();
                }
            }
            // If CTE is structured, look at SELECT items
            if let super::CteContent::Structured(plan) = &cte.content {
                for item in &plan.select.items {
                    if let Some(alias) = &item.col_alias {
                        let alias_str = &alias.0;
                        if alias_str == &format!("{}_user_id", cte_alias)
                            || alias_str == &format!("{}_id", cte_alias)
                        {
                            return alias_str.clone();
                        }
                    }
                }
            }
        }
    }
    // Ultimate fallback
    format!("{}_user_id", cte_alias)
}

/// Extract a JOIN condition from a filter expression.
/// Looks for equality patterns between CTE aliases and other tables.
#[allow(clippy::only_used_in_recursion)] // Some context parameters are intentionally threaded through recursive descent (notably `cte_schemas`).
pub(crate) fn extract_cte_join_condition_from_filter(
    filter_expr: &RenderExpr,
    cte_alias: &str,
    cte_aliases: &[String],
    cte_references: &HashMap<String, String>,
    cte_schemas: &crate::render_plan::CteSchemas,
) -> Option<OperatorApplication> {
    match filter_expr {
        RenderExpr::OperatorApplicationExp(op_app) => {
            match op_app.operator {
                Operator::Equal if op_app.operands.len() == 2 => {
                    let left = &op_app.operands[0];
                    let right = &op_app.operands[1];

                    // Check if one side references a CTE alias
                    let left_is_cte = if let RenderExpr::PropertyAccessExp(pa) = left {
                        cte_aliases.iter().any(|a| &pa.table_alias.0 == a)
                    } else {
                        false
                    };

                    let right_is_cte = if let RenderExpr::PropertyAccessExp(pa) = right {
                        cte_aliases.iter().any(|a| &pa.table_alias.0 == a)
                    } else {
                        false
                    };

                    // If one side is CTE and other is not, this is a join condition
                    if (left_is_cte && !right_is_cte) || (!left_is_cte && right_is_cte) {
                        return Some(rewrite_operator_application_for_cte_join(
                            op_app,
                            cte_alias,
                            cte_references,
                        ));
                    }
                    None
                }
                Operator::And => {
                    // Try both operands
                    for operand in &op_app.operands {
                        if let Some(cond) = extract_cte_join_condition_from_filter(
                            operand,
                            cte_alias,
                            cte_aliases,
                            cte_references,
                            cte_schemas,
                        ) {
                            return Some(cond);
                        }
                    }
                    None
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Try to extract a CTE join condition from an equality comparison.
/// Returns: Some((cte_name, cte_column, main_table_alias, main_column)) if found
/// Returns the alias name if found, None otherwise.
/// Collect aliases from a single RenderExpr into a HashSet.
/// Recursively traverses PropertyAccessExp, OperatorApplicationExp, and ScalarFnCall expressions
/// to collect all table aliases referenced in the expression.
pub fn collect_aliases_from_single_render_expr(
    expr: &crate::render_plan::render_expr::RenderExpr,
    aliases: &mut std::collections::HashSet<String>,
) {
    match expr {
        crate::render_plan::render_expr::RenderExpr::PropertyAccessExp(prop) => {
            aliases.insert(prop.table_alias.0.clone());
        }
        crate::render_plan::render_expr::RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_aliases_from_single_render_expr(operand, aliases);
            }
        }
        crate::render_plan::render_expr::RenderExpr::ScalarFnCall(fn_call) => {
            for arg in &fn_call.args {
                collect_aliases_from_single_render_expr(arg, aliases);
            }
        }
        _ => {}
    }
}

// `items_after_test_module` is allowed here: this 17K-line module's
// test block sits in the middle and many helpers follow. Reordering
// would shuffle thousands of lines for no behavioural gain.
#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;
    // P2.4: `has_with_clause_in_tree` moved to `plan_predicates` and is not
    // re-exported (no production caller); the characterization tests below use
    // it directly.
    use super::super::plan_predicates::has_with_clause_in_tree;

    #[test]
    fn test_placeholder_functions() {
        // Basic tests to ensure module compiles
        assert_eq!(strip_database_prefix("test"), "test");
        assert_eq!(strip_database_prefix("db.table"), "table");

        // Note: has_multi_type_vlp requires a schema, tested elsewhere
        assert_eq!(
            get_anchor_alias_from_plan(&Arc::new(LogicalPlan::Empty)),
            None
        );
    }

    /// Minimal empty schema — `should_use_join_expansion` (which
    /// `has_multi_type_vlp` delegates to for the `GraphRel` case) never reads
    /// `schema`, so an empty one is sufficient for these traversal tests.
    fn empty_schema_for_vlp_test() -> crate::graph_catalog::graph_schema::GraphSchema {
        use crate::graph_catalog::config::GraphSchemaConfig;
        let yaml = r#"
name: empty_vlp_test_schema
graph_schema:
  nodes:
    - label: Dummy
      database: db_vlp_test
      table: dummy
      node_id: id
      property_mappings: {}
  edges: []
"#;
        GraphSchemaConfig::from_yaml_str(yaml)
            .expect("parse empty test schema")
            .to_graph_schema()
            .expect("build empty test GraphSchema")
    }

    /// A multi-type VLP GraphRel: variable-length with more than one
    /// relationship type in `labels`, which `should_use_join_expansion`
    /// ("Case 2: Multiple relationship types ALWAYS require UNION ALL CTE")
    /// treats as requiring JOIN expansion. `left`/`right` are left as `Empty`
    /// — deliberately avoiding a `GraphNode` literal here so this fixture
    /// doesn't add a new occurrence of a schema-pattern-axis token tracked by
    /// `cargo test --test ratchet` in this file.
    fn multi_type_vlp_graph_rel() -> LogicalPlan {
        use crate::query_planner::logical_expr::Direction;
        use crate::query_planner::logical_plan::{GraphRel, VariableLengthSpec};
        LogicalPlan::GraphRel(GraphRel {
            left: Arc::new(LogicalPlan::Empty),
            center: Arc::new(LogicalPlan::Empty),
            right: Arc::new(LogicalPlan::Empty),
            alias: "r".to_string(),
            direction: Direction::Outgoing,
            left_connection: "a".to_string(),
            right_connection: "b".to_string(),
            is_rel_anchor: false,
            variable_length: Some(VariableLengthSpec::default()),
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec!["KNOWS".to_string(), "FOLLOWS".to_string()]),
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
            match_clause_index: 0, // #586 (synthetic/test)
            optional_anchor_where: None,
        })
    }

    /// Sanity check: `has_multi_type_vlp` detects a multi-type VLP `GraphRel`
    /// directly at the plan root.
    #[test]
    fn has_multi_type_vlp_detects_direct_graph_rel() {
        let schema = empty_schema_for_vlp_test();
        assert!(has_multi_type_vlp(&multi_type_vlp_graph_rel(), &schema));
    }

    /// Regression (Phase 1 Slice 2, gap-fix #4): before migrating to the
    /// exhaustive `LogicalPlan::children()` traversal, `has_multi_type_vlp`'s
    /// catch-all (`_ => false`) silently skipped `Union` (and `GraphNode`,
    /// `WithClause`, `Cte`, `CartesianProduct`, `Unwind`) subtrees, so a
    /// multi-type VLP nested under a `Union` branch was invisible to this
    /// check. Verify the fixed version finds it.
    #[test]
    fn has_multi_type_vlp_detects_graph_rel_nested_under_union() {
        use crate::query_planner::logical_plan::{Union, UnionType};

        let schema = empty_schema_for_vlp_test();
        let union_plan = LogicalPlan::Union(Union {
            inputs: vec![
                Arc::new(LogicalPlan::Empty),
                Arc::new(multi_type_vlp_graph_rel()),
            ],
            union_type: UnionType::All,
            is_cypher_union: false,
        });
        assert!(
            has_multi_type_vlp(&union_plan, &schema),
            "has_multi_type_vlp must see a multi-type VLP nested inside a Union branch"
        );
    }

    /// `emit_array_count_call` outside a task-local scope defaults to
    /// ClickHouse — matching the pre-Phase-1.1 hardcoded behavior.
    #[test]
    fn emit_array_count_call_defaults_to_clickhouse() {
        let sql = emit_array_count_call("x", "x IN (SELECT id FROM t)", "vp.path_nodes");
        assert_eq!(
            sql,
            "arrayCount(x -> x IN (SELECT id FROM t), vp.path_nodes)"
        );
    }

    /// `quote_qualified_col` keeps CH double-quotes (byte-identical to the old
    /// hardcoded form) and flips to Spark backticks under Databricks — Spark
    /// parses `"col"` as a string literal, so the delimiter must change.
    #[tokio::test]
    async fn quote_qualified_col_is_dialect_aware() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        // Default (no task-local scope) → ClickHouse, matching historical output.
        assert_eq!(quote_qualified_col("alias", "posts"), "alias.\"posts\"");

        let ch = with_query_context(
            QueryContext {
                dialect: SqlDialect::ClickHouse,
                ..QueryContext::default()
            },
            async { quote_qualified_col("alias", "posts") },
        )
        .await;
        assert_eq!(ch, "alias.\"posts\"");

        let dbx = with_query_context(
            QueryContext {
                dialect: SqlDialect::Databricks,
                ..QueryContext::default()
            },
            async { quote_qualified_col("alias", "posts") },
        )
        .await;
        assert_eq!(dbx, "alias.`posts`");
    }

    /// Databricks: a hoisted WHERE predicate on a `count(x)` becomes
    /// `count(CASE WHEN cond THEN x END)` — Spark's `count_if` takes only a
    /// predicate (1 arg), so the CH 2-arg `countIf(x, cond)` shape would error
    /// with `WRONG_NUM_ARGS`. `count` ignores NULLs, matching `countIf`.
    #[tokio::test]
    async fn rewrite_count_to_conditional_databricks_wraps_in_case() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let agg = with_query_context(ctx, async {
            let mut agg = AggregateFnCall {
                name: "count".to_string(),
                args: vec![RenderExpr::Literal(Literal::Integer(1))],
            };
            rewrite_count_to_conditional(&mut agg, RenderExpr::Literal(Literal::Boolean(true)));
            agg
        })
        .await;

        assert_eq!(agg.name, "count", "name must stay `count` for Spark");
        assert_eq!(agg.args.len(), 1, "Spark count takes exactly one arg");
        match &agg.args[0] {
            RenderExpr::Case(c) => {
                assert!(c.expr.is_none(), "searched CASE (no scrutinee)");
                assert_eq!(c.when_then.len(), 1);
                assert_eq!(
                    c.when_then[0].0,
                    RenderExpr::Literal(Literal::Boolean(true))
                );
                assert_eq!(c.when_then[0].1, RenderExpr::Literal(Literal::Integer(1)));
                assert!(c.else_expr.is_none(), "no ELSE → NULL, dropped by count");
            }
            other => panic!("expected CASE-wrapped count arg, got {other:?}"),
        }
    }

    /// ClickHouse keeps the native 2-arg `-If` combinator `countIf(x, cond)`.
    #[tokio::test]
    async fn rewrite_count_to_conditional_clickhouse_uses_countif() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let ctx = QueryContext {
            dialect: SqlDialect::ClickHouse,
            ..QueryContext::default()
        };
        let agg = with_query_context(ctx, async {
            let mut agg = AggregateFnCall {
                name: "count".to_string(),
                args: vec![RenderExpr::Literal(Literal::Integer(1))],
            };
            rewrite_count_to_conditional(&mut agg, RenderExpr::Literal(Literal::Boolean(true)));
            agg
        })
        .await;

        assert_eq!(agg.name, "countIf");
        assert_eq!(
            agg.args,
            vec![
                RenderExpr::Literal(Literal::Integer(1)),
                RenderExpr::Literal(Literal::Boolean(true)),
            ]
        );
    }

    /// When the task-local dialect is Databricks, the helper builds the
    /// `size(filter(arr, x -> pred))` structural rewrite — Spark has no
    /// `arrayCount` equivalent. Note `filter` reverses arg order (arr,
    /// predicate) vs CH's (predicate, arr).
    #[tokio::test]
    async fn emit_array_count_call_databricks_uses_size_filter() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let sql = with_query_context(ctx, async {
            emit_array_count_call("x", "x IN (SELECT id FROM t)", "vp.path_nodes")
        })
        .await;
        assert_eq!(
            sql,
            "size(filter(vp.path_nodes, x -> x IN (SELECT id FROM t)))"
        );
    }

    /// `emit_array_count_in_subquery` defaults to ClickHouse's native
    /// `arrayCount(x -> <lhs> IN (subq), arr)` outside a task-local scope.
    #[test]
    fn emit_array_count_in_subquery_defaults_to_clickhouse() {
        let sql = emit_array_count_in_subquery("x", "SELECT id FROM t", "vp.path_nodes");
        assert_eq!(
            sql,
            "arrayCount(x -> x IN (SELECT id FROM t), vp.path_nodes)"
        );
    }

    /// Databricks/Spark forbids subqueries inside HOF lambdas, so the IN-subquery
    /// array count explodes the array in a scalar subquery and tests membership in
    /// a plain WHERE — duplicate-preserving, matching `arrayCount` semantics.
    #[tokio::test]
    async fn emit_array_count_in_subquery_databricks_uses_explode_scalar() {
        use crate::server::query_context::{with_query_context, QueryContext};
        use crate::sql_generator::SqlDialect;

        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        // Simple non-correlated membership (LDBC Q10 shape).
        let sql = with_query_context(ctx.clone(), async {
            emit_array_count_in_subquery("x", "SELECT id FROM t", "p.posts")
        })
        .await;
        assert_eq!(
            sql,
            "(SELECT count(*) FROM (SELECT explode(p.posts) AS x) WHERE x IN (SELECT id FROM t))"
        );

        // Correlated tuple membership (the tuple-fallback path).
        let tuple = with_query_context(ctx, async {
            emit_array_count_in_subquery("(x, friend.pid)", "SELECT id, gid FROM t", "p.posts")
        })
        .await;
        assert_eq!(
            tuple,
            "(SELECT count(*) FROM (SELECT explode(p.posts) AS x) WHERE (x, friend.pid) IN (SELECT id, gid FROM t))"
        );
    }

    /// Regression test: build_cte_column_map must use real column names from expressions,
    /// not CTE alias names like p1_a_user_id. When the FROM is a base table (e.g., social.users),
    /// correlated subqueries must reference `a.user_id`, not `a.p1_a_user_id`.
    /// See: click-to-expand regression (Code 47 ClickHouse error).
    #[test]
    fn test_build_cte_column_map_uses_expression_column_not_alias() {
        use crate::graph_catalog::expression_parser::PropertyValue;
        use crate::render_plan::render_expr::{ColumnAlias, PropertyAccess, TableAlias};

        // Build a minimal RenderPlan with a base table FROM and PropertyAccessExp SELECT items
        let render_plan = RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![
                    // `a.user_id AS p1_a_user_id` — the expression has the real column name
                    SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("a".to_string()),
                            column: PropertyValue::Column("user_id".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("p1_a_user_id".to_string())),
                    },
                    // `a.full_name AS p1_a_name` — property mapping: Cypher "name" → DB "full_name"
                    SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("a".to_string()),
                            column: PropertyValue::Column("full_name".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("p1_a_name".to_string())),
                    },
                ],
                distinct: false,
            },
            from: FromTableItem(Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name: "social.users".to_string(),
                alias: Some("a".to_string()),
                use_final: false,
            })),
            joins: JoinItems(vec![]),
            array_join: ArrayJoinItem(vec![]),
            filters: FilterItems(None),
            group_by: GroupByExpressions(vec![]),
            having_clause: None,
            order_by: OrderByItems(vec![]),
            skip: SkipItem(None),
            limit: LimitItem(None),
            union: UnionItems(None),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        };

        let map = build_cte_column_map(&render_plan, "with_some_cte");

        // Must use real column "user_id", NOT the alias "p1_a_user_id"
        assert_eq!(
            map.get(&("a".to_string(), "user_id".to_string())),
            Some(&"a.user_id".to_string()),
            "Correlated subquery should reference real column a.user_id, not a.p1_a_user_id"
        );

        // Property-mapped column: Cypher "name" → real DB column "full_name"
        assert_eq!(
            map.get(&("a".to_string(), "name".to_string())),
            Some(&"a.full_name".to_string()),
            "Correlated subquery should reference real column a.full_name, not a.p1_a_name"
        );
    }

    /// Regression test: build_cte_column_map should fall back to CTE alias name
    /// when expression is not a PropertyAccessExp (e.g., aggregate or subquery).
    #[test]
    fn test_build_cte_column_map_fallback_for_non_property_expr() {
        use crate::render_plan::render_expr::ColumnAlias;

        let render_plan = RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![
                    // Non-PropertyAccessExp: e.g., COUNT(*) AS p1_a_count
                    SelectItem {
                        expression: RenderExpr::Literal(
                            crate::render_plan::render_expr::Literal::Integer(1),
                        ),
                        col_alias: Some(ColumnAlias("p1_a_count".to_string())),
                    },
                ],
                distinct: false,
            },
            from: FromTableItem(Some(ViewTableRef {
                source: Arc::new(LogicalPlan::Empty),
                name: "with_some_cte".to_string(),
                alias: Some("cte_alias".to_string()),
                use_final: false,
            })),
            joins: JoinItems(vec![]),
            array_join: ArrayJoinItem(vec![]),
            filters: FilterItems(None),
            group_by: GroupByExpressions(vec![]),
            having_clause: None,
            order_by: OrderByItems(vec![]),
            skip: SkipItem(None),
            limit: LimitItem(None),
            union: UnionItems(None),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        };

        let map = build_cte_column_map(&render_plan, "with_some_cte");

        // For non-PropertyAccess expressions, falls back to the CTE column alias name
        assert_eq!(
            map.get(&("a".to_string(), "count".to_string())),
            Some(&"cte_alias.p1_a_count".to_string()),
            "Non-PropertyAccess expressions should fall back to CTE alias column name"
        );
    }

    /// Build a minimal `GraphRel` with `left_connection`/`right_connection`
    /// set to `left`/`right` and relationship type `rel_type` — the shape
    /// `find_denorm_connection_node_label` traverses to. Children are all
    /// `LogicalPlan::Empty`; only the connection metadata matters for this
    /// function.
    fn minimal_connection_rel(left: &str, right: &str, rel_type: &str) -> LogicalPlan {
        LogicalPlan::GraphRel(GraphRel {
            left: Arc::new(LogicalPlan::Empty),
            center: Arc::new(LogicalPlan::Empty),
            right: Arc::new(LogicalPlan::Empty),
            alias: "r".to_string(),
            direction: Direction::Outgoing,
            left_connection: left.to_string(),
            right_connection: right.to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec![rel_type.to_string()]),
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
            match_clause_index: 0, // #586 (synthetic/test)
            optional_anchor_where: None,
        })
    }

    /// #562 (hardening, non-blocking finding from #551's adversarial
    /// review): `find_denorm_connection_node_label` must NOT silently pick
    /// the FIRST candidate label when a relationship TYPE is registered
    /// against multiple node-label pairs with DIFFERING id-property shapes
    /// — blindly trusting the first would resolve the WRONG id property
    /// name whenever the "losing" pair's node is the one actually being
    /// grouped. No real fixture in this repo reaches this today (the
    /// analyzer's multi-type VLP routing, #538, already intercepts genuine
    /// ambiguity upstream of this function — see #551's PR discussion), so
    /// this test constructs the ambiguous schema directly and calls the
    /// function without going through the full analyzer pipeline.
    #[test]
    fn find_denorm_connection_node_label_refuses_ambiguous_multi_registration_562() {
        use crate::graph_catalog::config::GraphSchemaConfig;

        let yaml = r#"
name: ambiguous_multi_reg_562
graph_schema:
  nodes:
    - label: WidgetA
      database: db_ambiguous
      table: widget_a
      node_id: widget_a_id
      property_mappings: {}
    - label: WidgetB
      database: db_ambiguous
      table: widget_b
      node_id: widget_b_id
      property_mappings: {}
  edges:
    - type: AMBIGUOUS_LINK
      database: db_ambiguous
      table: link_a
      from_id: from_id
      to_id: to_id
      from_node: WidgetA
      to_node: WidgetA
    - type: AMBIGUOUS_LINK
      database: db_ambiguous
      table: link_b
      from_id: from_id
      to_id: to_id
      from_node: WidgetB
      to_node: WidgetB
"#;
        let schema = GraphSchemaConfig::from_yaml_str(yaml)
            .expect("parse synthetic #562 schema")
            .to_graph_schema()
            .expect("build synthetic #562 GraphSchema");

        // `b` is the right_connection of an AMBIGUOUS_LINK GraphRel — the
        // unlabeled denorm-chain-node shape `find_denorm_connection_node_label`
        // exists to resolve. `AMBIGUOUS_LINK` resolves to TWO candidate
        // to_node labels (WidgetA: `widget_a_id`, WidgetB: `widget_b_id`) —
        // differing id-property names, so the function must refuse to guess.
        let plan = minimal_connection_rel("a", "b", "AMBIGUOUS_LINK");
        assert_eq!(
            find_denorm_connection_node_label(&plan, "b", &schema),
            None,
            "ambiguous multi-registration with differing id shapes must not silently resolve a label"
        );
    }

    /// Regression guard for #562: a relationship type registered against
    /// TWO node-label pairs that happen to share the SAME id-property shape
    /// is NOT ambiguous in the way #562 guards against — the function must
    /// still resolve a label normally (not over-trigger the new guard).
    #[test]
    fn find_denorm_connection_node_label_resolves_multi_registration_with_same_id_shape_562() {
        use crate::graph_catalog::config::GraphSchemaConfig;

        let yaml = r#"
name: same_shape_multi_reg_562
graph_schema:
  nodes:
    - label: WidgetA
      database: db_ambiguous
      table: widget_a
      node_id: widget_id
      property_mappings: {}
    - label: WidgetC
      database: db_ambiguous
      table: widget_c
      node_id: widget_id
      property_mappings: {}
  edges:
    - type: SAME_SHAPE_LINK
      database: db_ambiguous
      table: link_a
      from_id: from_id
      to_id: to_id
      from_node: WidgetA
      to_node: WidgetA
    - type: SAME_SHAPE_LINK
      database: db_ambiguous
      table: link_c
      from_id: from_id
      to_id: to_id
      from_node: WidgetC
      to_node: WidgetC
"#;
        let schema = GraphSchemaConfig::from_yaml_str(yaml)
            .expect("parse synthetic #562 schema")
            .to_graph_schema()
            .expect("build synthetic #562 GraphSchema");

        let plan = minimal_connection_rel("a", "b", "SAME_SHAPE_LINK");
        let resolved = find_denorm_connection_node_label(&plan, "b", &schema);
        assert!(
            resolved == Some("WidgetA".to_string()) || resolved == Some("WidgetC".to_string()),
            "same id-property shape across registrations must still resolve a label (got {:?})",
            resolved
        );
    }

    // ========================================================================
    // Characterization: the five WITH-traversal functions (P1.2 / §4.2)
    //
    // These lock the CURRENT answers of the WITH-detection/collection walkers
    // over a synthetic-plan matrix — WITH under each structural position and
    // under write variants — BEFORE any behavior is unified. Post-3a3af0bf,
    // has_with_clause_in_tree / plan_contains_with_clause / needs_processing /
    // find_all_with_clauses_grouped already route through the exhaustive
    // children() API; these tests document that they now AGREE (the historical
    // plan_contains_with_clause GraphRel.center / Cte / ViewScan drift is
    // closed) and additionally reach write-variant inputs. If a later unify
    // step changes any of these answers, that is a behavior change and must be
    // reviewed as such (§10), not silently absorbed.
    //
    // `needs_processing` and `replace_with_clause_with_cte_reference_v2` are
    // nested/transform functions not directly callable here; their traversal
    // agreement is exercised end-to-end by the golden + corpus sweeps.
    // ========================================================================
    mod with_traversal_characterization {
        use super::*;
        use crate::query_planner::logical_expr::{Direction, Literal, LogicalExpr};
        use crate::query_planner::logical_plan::{
            Cte, Filter, GraphNode, GraphRel, ProjectionItem, Unwind, WithClause,
        };
        use std::sync::Arc;

        fn leaf() -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::Empty)
        }

        /// A WithClause exporting alias `a` (key becomes "a"), wrapping `input`.
        fn with_a(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            let item = ProjectionItem {
                expression: LogicalExpr::Column(crate::query_planner::logical_expr::Column(
                    "a".to_string(),
                )),
                col_alias: None,
            };
            Arc::new(LogicalPlan::WithClause(
                WithClause::new(input, vec![item]).expect("WITH a is valid"),
            ))
        }

        fn node(alias: &str, input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::GraphNode(GraphNode {
                input,
                alias: alias.to_string(),
                label: None,
                is_denormalized: false,
                projected_columns: None,
                node_types: None,
            }))
        }

        /// GraphRel with the given left/center/right children.
        fn graph_rel(
            left: Arc<LogicalPlan>,
            center: Arc<LogicalPlan>,
            right: Arc<LogicalPlan>,
        ) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::GraphRel(GraphRel {
                left,
                center,
                right,
                alias: "r".to_string(),
                direction: Direction::Outgoing,
                left_connection: "a".to_string(),
                right_connection: "b".to_string(),
                is_rel_anchor: false,
                variable_length: None,
                shortest_path_mode: None,
                path_variable: None,
                where_predicate: None,
                labels: None,
                is_optional: None,
                anchor_connection: None,
                cte_references: std::collections::HashMap::new(),
                pattern_combinations: None,
                was_undirected: None,
                match_clause_index: 0,
                optional_anchor_where: None,
            }))
        }

        fn cte(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::Cte(Cte {
                input,
                name: "c1".to_string(),
            }))
        }

        fn view_scan_with_input(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            let mut vs = crate::query_planner::logical_plan::ViewScan::new(
                "t".to_string(),
                None,
                std::collections::HashMap::new(),
                "id".to_string(),
                vec!["id".to_string()],
                vec![],
            );
            vs.input = Some(input);
            Arc::new(LogicalPlan::ViewScan(Arc::new(vs)))
        }

        fn unwind(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::Unwind(Unwind {
                input,
                expression: LogicalExpr::Column(crate::query_planner::logical_expr::Column(
                    "xs".to_string(),
                )),
                alias: "x".to_string(),
                label: None,
                tuple_properties: None,
            }))
        }

        fn cartesian(left: Arc<LogicalPlan>, right: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::CartesianProduct(
                crate::query_planner::logical_plan::CartesianProduct {
                    left,
                    right,
                    is_optional: false,
                    join_condition: None,
                },
            ))
        }

        fn union(inputs: Vec<Arc<LogicalPlan>>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::Union(
                crate::query_planner::logical_plan::Union {
                    inputs,
                    union_type: crate::query_planner::logical_plan::UnionType::All,
                    is_cypher_union: false,
                },
            ))
        }

        fn filter(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::Filter(Filter {
                input,
                predicate: LogicalExpr::Literal(Literal::Boolean(true)),
            }))
        }

        fn create(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::Create(
                crate::query_planner::logical_plan::Create {
                    input,
                    patterns: vec![],
                },
            ))
        }

        fn set_props(input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
            Arc::new(LogicalPlan::SetProperties(
                crate::query_planner::logical_plan::SetProperties {
                    input,
                    items: vec![],
                },
            ))
        }

        /// Assert the two existence predicates AGREE and both return `expected`.
        fn assert_existence(plan: &LogicalPlan, expected: bool, case: &str) {
            assert_eq!(
                has_with_clause_in_tree(plan),
                expected,
                "has_with_clause_in_tree wrong for: {case}"
            );
            assert_eq!(
                plan_contains_with_clause(plan),
                expected,
                "plan_contains_with_clause wrong for: {case}"
            );
        }

        // ---- WITH directly under each structural position -----------------

        #[test]
        fn with_under_graph_rel_left_is_found() {
            let plan = graph_rel(with_a(node("a", leaf())), leaf(), node("b", leaf()));
            assert_existence(&plan, true, "GraphRel.left");
            assert!(find_all_with_clauses_grouped(&plan).contains_key("a"));
        }

        #[test]
        fn with_under_graph_rel_right_is_found() {
            let plan = graph_rel(node("b", leaf()), leaf(), with_a(node("a", leaf())));
            assert_existence(&plan, true, "GraphRel.right");
            assert!(find_all_with_clauses_grouped(&plan).contains_key("a"));
        }

        /// GraphRel.center: the historically-divergent position. Both existence
        /// predicates now find it (children()-backed). `find_all_with_clauses_
        /// grouped` also collects it (its `other =>` arm recurses via
        /// for_each_child, which includes center).
        #[test]
        fn with_under_graph_rel_center_is_found_by_existence() {
            let plan = graph_rel(
                node("b", leaf()),
                with_a(node("a", leaf())),
                node("c", leaf()),
            );
            assert_existence(&plan, true, "GraphRel.center");
            assert!(
                find_all_with_clauses_grouped(&plan).contains_key("a"),
                "center WITH collected by find_all_with_clauses_grouped"
            );
        }

        #[test]
        fn with_under_cte_input_is_found() {
            let plan = cte(with_a(node("a", leaf())));
            assert_existence(&plan, true, "Cte.input");
            assert!(find_all_with_clauses_grouped(&plan).contains_key("a"));
        }

        #[test]
        fn with_under_view_scan_input_is_found() {
            let plan = view_scan_with_input(with_a(node("a", leaf())));
            assert_existence(&plan, true, "ViewScan.input");
            assert!(find_all_with_clauses_grouped(&plan).contains_key("a"));
        }

        #[test]
        fn with_under_unwind_is_found() {
            let plan = unwind(with_a(node("a", leaf())));
            assert_existence(&plan, true, "Unwind.input");
            assert!(find_all_with_clauses_grouped(&plan).contains_key("a"));
        }

        #[test]
        fn with_under_cartesian_product_is_found() {
            let plan = cartesian(with_a(node("a", leaf())), node("b", leaf()));
            assert_existence(&plan, true, "CartesianProduct.left");
            assert!(find_all_with_clauses_grouped(&plan).contains_key("a"));
        }

        #[test]
        fn with_under_union_arm_is_found() {
            let plan = union(vec![with_a(node("a", leaf())), node("b", leaf())]);
            assert_existence(&plan, true, "Union arm");
            // NB find_all_with_clauses_grouped has bespoke Union dedup logic;
            // characterize only the existence predicates + that it doesn't panic.
            let _ = find_all_with_clauses_grouped(&plan);
        }

        // ---- WITH under WRITE variants (the latent §6 gap the migration
        //      closes for the existence predicates) --------------------------

        /// CURRENT behavior: a WITH inside a Create.input IS visible to both
        /// existence predicates (they route through children(), whose Create
        /// arm returns [&c.input]). This documents the closed gap.
        #[test]
        fn with_under_create_input_is_found_by_existence() {
            let plan = create(with_a(node("a", leaf())));
            assert_existence(&plan, true, "Create.input");
        }

        #[test]
        fn with_under_set_properties_input_is_found_by_existence() {
            let plan = set_props(with_a(node("a", leaf())));
            assert_existence(&plan, true, "SetProperties.input");
        }

        // ---- Nested combinations ------------------------------------------

        #[test]
        fn nested_with_under_cte_under_graph_rel_center() {
            // GraphRel{center: Cte(Filter(WITH a))}
            let inner = cte(filter(with_a(node("a", leaf()))));
            let plan = graph_rel(node("b", leaf()), inner, node("c", leaf()));
            assert_existence(&plan, true, "GraphRel.center -> Cte -> Filter -> WITH");
        }

        #[test]
        fn no_with_anywhere_is_false() {
            let plan = graph_rel(node("a", leaf()), leaf(), node("b", leaf()));
            assert_existence(&plan, false, "no WITH present");
            assert!(find_all_with_clauses_grouped(&plan).is_empty());
        }

        /// The two existence predicates must AGREE on every matrix position —
        /// this is the core §6 invariant the migration makes structural. A
        /// single loop over all positions catches any future re-divergence.
        #[test]
        fn existence_predicates_agree_across_matrix() {
            let positions: Vec<(&str, Arc<LogicalPlan>)> = vec![
                ("graph_rel.left", graph_rel(with_a(leaf()), leaf(), leaf())),
                (
                    "graph_rel.center",
                    graph_rel(leaf(), with_a(leaf()), leaf()),
                ),
                ("graph_rel.right", graph_rel(leaf(), leaf(), with_a(leaf()))),
                ("cte.input", cte(with_a(leaf()))),
                ("view_scan.input", view_scan_with_input(with_a(leaf()))),
                ("unwind.input", unwind(with_a(leaf()))),
                ("cartesian.left", cartesian(with_a(leaf()), leaf())),
                ("cartesian.right", cartesian(leaf(), with_a(leaf()))),
                ("union.arm", union(vec![leaf(), with_a(leaf())])),
                ("filter.input", filter(with_a(leaf()))),
                ("create.input", create(with_a(leaf()))),
                ("set.input", set_props(with_a(leaf()))),
                ("no_with", graph_rel(leaf(), leaf(), leaf())),
            ];
            for (name, plan) in &positions {
                assert_eq!(
                    has_with_clause_in_tree(plan),
                    plan_contains_with_clause(plan),
                    "existence predicates disagree at position: {name}"
                );
            }
        }

        // ---- D5: the twin UNWIND collectors and their barrier difference ---

        /// Both collectors gather UNWIND aliases on the projection spine.
        #[test]
        fn unwind_collectors_agree_without_a_barrier() {
            // Filter(Unwind x (Unwind y (Empty)))
            let plan = filter(unwind(Arc::new(LogicalPlan::Unwind(
                crate::query_planner::logical_plan::Unwind {
                    input: leaf(),
                    expression: LogicalExpr::Column(crate::query_planner::logical_expr::Column(
                        "ys".to_string(),
                    )),
                    alias: "y".to_string(),
                    label: None,
                    tuple_properties: None,
                },
            ))));

            let mut set = std::collections::HashSet::new();
            collect_unwind_aliases(&plan, &mut set);
            let mut vec = Vec::new();
            find_unwind_aliases(&plan, &mut vec);

            assert!(set.contains("x") && set.contains("y"));
            assert!(vec.contains(&"x".to_string()) && vec.contains(&"y".to_string()));
        }

        /// The ONE deliberate difference: `collect_unwind_aliases` STOPS at a
        /// WithClause barrier (an UNWIND below is a prior segment's, now a CTE
        /// column); `find_unwind_aliases` CROSSES it (ID-column detection).
        /// This is the `stop_at_with` / `cross_with_barrier` distinction the D5
        /// merge preserves.
        #[test]
        fn unwind_collectors_differ_across_with_barrier() {
            // Unwind x ( WITH a ( Unwind pre ( Empty ) ) )
            let below_barrier = Arc::new(LogicalPlan::Unwind(
                crate::query_planner::logical_plan::Unwind {
                    input: leaf(),
                    expression: LogicalExpr::Column(crate::query_planner::logical_expr::Column(
                        "pres".to_string(),
                    )),
                    alias: "pre".to_string(),
                    label: None,
                    tuple_properties: None,
                },
            ));
            let plan = unwind(with_a(below_barrier));

            let mut set = std::collections::HashSet::new();
            collect_unwind_aliases(&plan, &mut set);
            // Stops at WITH: sees the top `x`, NOT the `pre` below the barrier.
            assert!(set.contains("x"), "top-of-spine UNWIND collected");
            assert!(
                !set.contains("pre"),
                "collect_unwind_aliases must STOP at the WithClause barrier"
            );

            let mut vec = Vec::new();
            find_unwind_aliases(&plan, &mut vec);
            // Crosses WITH: sees both.
            assert!(vec.contains(&"x".to_string()));
            assert!(
                vec.contains(&"pre".to_string()),
                "find_unwind_aliases must CROSS the WithClause barrier"
            );
        }
    }
}
// ============================================================================
// CTE Expression Rewriting Functions
// ============================================================================

/// Rewrite a `count(x)` aggregate so a WHERE predicate that was hoisted into
/// the aggregate becomes a conditional count — dialect-aware.
///
/// ClickHouse expresses this as the 2-arg `-If` combinator `countIf(x, cond)`:
/// count rows where `cond` holds (and `x` is non-null). Spark's `count_if`
/// takes only a predicate (1 arg), so the same 2-arg shape errors with
/// `WRONG_NUM_ARGS`; emulate it there as `count(CASE WHEN cond THEN x END)` —
/// `count` ignores NULLs, so rows failing `cond` drop out exactly like `countIf`
/// does. Mirrors the `min_if` mapper's CASE rewrite. Caller guarantees
/// `agg.args` is non-empty (the count target is `args[0]`).
pub(crate) fn rewrite_count_to_conditional(agg: &mut AggregateFnCall, cond: RenderExpr) {
    use crate::sql_generator::SqlDialect;
    match crate::server::query_context::get_current_dialect() {
        SqlDialect::Databricks => {
            let target = agg.args[0].clone();
            agg.args[0] = RenderExpr::Case(RenderCase {
                expr: None,
                when_then: vec![(cond, target)],
                else_expr: None,
            });
            // name stays "count"
        }
        _ => {
            agg.name = current_function_mapper().count_if().to_string();
            agg.args.push(cond);
        }
    }
}

/// Build a dialect-quoted qualified column reference `alias.<quoted col>`.
///
/// Bare (non-`p{N}`) CTE variables are referenced as `alias."col"` on
/// ClickHouse and `` alias.`col` `` on Spark/Databricks — Spark parses `"col"`
/// as a string literal, so the identifier delimiter must flip. Routes through
/// `FunctionMapper::quote_alias` (CH → `"x"`, Spark → `` `x` ``), keeping CH
/// output byte-identical (the helper escapes embedded quotes too) while fixing
/// the Databricks syntax error. Used by the alias-mapping builders for
/// variables that lack the `p{N}_{alias}_{prop}` column shape.
pub(crate) fn quote_qualified_col(alias: &str, col: &str) -> String {
    format!("{}.{}", alias, current_function_mapper().quote_alias(col))
}

/// Rewrite all occurrences of `old_alias` → `new_alias` in PropertyAccessExp table_alias
/// across SELECT, JOINs, WHERE, ORDER BY, and UNION branches.
/// Used when preserving the original node alias (e.g., "a") instead of
/// the combined CTE alias (e.g., "a_allNeighboursCount").
pub(crate) fn rewrite_table_alias_in_render_plan(
    plan: &mut crate::render_plan::RenderPlan,
    old_alias: &str,
    new_alias: &str,
) {
    use crate::render_plan::render_expr::RenderExpr;

    fn rewrite_expr(expr: RenderExpr, old: &str, new: &str) -> RenderExpr {
        use super::render_expr::{map_render_expr, RenderRewrite};
        // Exhaustive combinator: rename a table alias on each property access,
        // recurse structurally into every value-wrapper. The former hand-rolled
        // walk handled PropertyAccess/Operator/ScalarFn/Aggregate/Case/List and
        // fell through `other => other` for ArraySubscript/ArraySlicing/
        // ReduceExpr/MapLiteral, silently leaving a stale alias inside those
        // wrappers. Latent (no corpus query reached it; byte-identical on
        // migration). Subqueries are still NOT descended (map_render_expr's
        // policy) — matching the old code, which cloned them too.
        map_render_expr(&expr, &mut |node| {
            if let RenderExpr::PropertyAccessExp(pa) = node {
                if pa.table_alias.0 == old {
                    let mut pa = pa.clone();
                    pa.table_alias.0 = new.to_string();
                    return RenderRewrite::Replace(RenderExpr::PropertyAccessExp(pa));
                }
                return RenderRewrite::Replace(node.clone());
            }
            RenderRewrite::Recurse
        })
    }

    // SELECT items
    for item in &mut plan.select.items {
        item.expression = rewrite_expr(item.expression.clone(), old_alias, new_alias);
    }

    // JOIN conditions
    for join in &mut plan.joins.0 {
        // Rewrite table_alias in JOIN itself
        if join.table_alias == old_alias {
            join.table_alias = new_alias.to_string();
        }
        for op in &mut join.joining_on {
            if let RenderExpr::OperatorApplicationExp(new_op) = rewrite_expr(
                RenderExpr::OperatorApplicationExp(op.clone()),
                old_alias,
                new_alias,
            ) {
                *op = new_op;
            }
        }
    }

    // WHERE
    if let Some(filter) = &plan.filters.0 {
        plan.filters.0 = Some(rewrite_expr(filter.clone(), old_alias, new_alias));
    }

    // ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = rewrite_expr(item.expression.clone(), old_alias, new_alias);
    }

    // GROUP BY
    plan.group_by.0 = plan
        .group_by
        .0
        .iter()
        .map(|e| rewrite_expr(e.clone(), old_alias, new_alias))
        .collect();

    // HAVING
    if let Some(having) = &plan.having_clause {
        plan.having_clause = Some(rewrite_expr(having.clone(), old_alias, new_alias));
    }

    // UNION branches
    if let Some(ref mut union) = plan.union.0 {
        for branch in &mut union.input {
            rewrite_table_alias_in_render_plan(branch, old_alias, new_alias);
        }
    }
}

// ============================================================================
// VLP and Scope Analysis Functions
// ============================================================================

/// Check if the plan contains a multi-type VLP pattern
/// Returns true if there's a variable-length path with multiple relationship types
///
/// Currently exercised only by this module's own unit tests (no production caller) —
/// scoped `allow(dead_code)` instead of the module-level one removed in Phase 2, since
/// `#[cfg(test)]` callers aren't visible to a non-test build of the lib target.
#[allow(dead_code)]
pub fn has_multi_type_vlp(
    plan: &crate::query_planner::logical_plan::LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> bool {
    use crate::query_planner::logical_plan::LogicalPlan;

    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Check if it's a VLP pattern
            if graph_rel.variable_length.is_some() {
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                // Use the same logic as CTE extraction
                crate::render_plan::cte_extraction::should_use_join_expansion_public(
                    graph_rel, &rel_types, schema,
                )
            } else {
                false
            }
        }
        // Everything else — recurse into every direct child via the exhaustive
        // `LogicalPlan::children` API (covers GraphNode, Union, WithClause, Cte,
        // CartesianProduct, Unwind, etc. that the previous hand-rolled catch-all
        // silently skipped).
        _ => plan
            .children()
            .iter()
            .any(|c| has_multi_type_vlp(c, schema)),
    }
}

// ============================================================================
// Utility Functions - CTE Management
// ============================================================================

/// Hoist nested CTEs from a RenderPlan to a parent CTE list.
///
/// This is used to flatten CTE hierarchies.
pub fn hoist_nested_ctes(from: &mut RenderPlan, to: &mut Vec<Cte>) {
    let nested_ctes = std::mem::take(&mut from.ctes.0);
    if !nested_ctes.is_empty() {
        log::info!(
            "🔧 hoist_nested_ctes: Hoisting {} nested CTEs",
            nested_ctes.len()
        );
        to.extend(nested_ctes);
    }
}

pub(crate) fn generate_swapped_joins_for_optional_match(
    graph_rel: &GraphRel,
) -> RenderPlanBuilderResult<Vec<Join>> {
    let mut joins = Vec::new();

    // Extract table names and columns with parameterized view syntax if applicable
    // CRITICAL FIX: Use extract_parameterized_table_ref for ViewScan to handle parameterized views
    let start_table = extract_parameterized_table_ref(&graph_rel.left)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("left node".to_string()))?;
    let _end_table = extract_parameterized_table_ref(&graph_rel.right)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("right node".to_string()))?;

    // For ID column lookup, we need the plain table name (without parameterized syntax)
    let start_table_plain = extract_table_name(&graph_rel.left)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("left node".to_string()))?;
    let end_table_plain = extract_table_name(&graph_rel.right)
        .ok_or_else(|| RenderBuildError::MissingTableInfo("right node".to_string()))?;

    let start_id_col = table_to_id_column(&start_table_plain);
    let end_id_col = table_to_id_column(&end_table_plain);

    // Get relationship table with parameterized view syntax if applicable
    // If center is wrapped in a CTE (for alternate relationships), use the CTE name
    // Otherwise, derive from labels or extract from plan with parameterized view support
    let rel_table = if matches!(&*graph_rel.center, LogicalPlan::Cte(_)) {
        // CTEs don't have parameterized views
        extract_table_name(&graph_rel.center).unwrap_or_else(|| graph_rel.alias.clone())
    } else if let Some(labels) = &graph_rel.labels {
        if !labels.is_empty() {
            // Labels-based lookup doesn't support parameterized views
            rel_type_to_table_name(&labels[0])
        } else {
            // Use parameterized table ref for ViewScan
            extract_parameterized_table_ref(&graph_rel.center)
                .unwrap_or_else(|| graph_rel.alias.clone())
        }
    } else {
        // Use parameterized table ref for ViewScan
        extract_parameterized_table_ref(&graph_rel.center)
            .unwrap_or_else(|| graph_rel.alias.clone())
    };

    // Get relationship columns
    let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(RelationshipColumns {
        from_id: Identifier::Single("from_node_id".to_string()),
        to_id: Identifier::Single("to_node_id".to_string()),
    });

    // For OPTIONAL MATCH with swapped anchor:
    // - anchor is right_connection (post)
    // - new node is left_connection (liker)
    // - For outgoing direction (liker)-[:LIKES]->(post):
    //   - rel.to_id connects to anchor (post)
    //   - rel.from_id connects to new node (liker)

    // Determine join column based on direction
    let (rel_col_to_anchor, rel_col_to_new) = match graph_rel.direction {
        Direction::Incoming => {
            // (liker)<-[:LIKES]-(post) means rel points from post to liker
            // rel.from_id = anchor (post), rel.to_id = new (liker)
            (rel_cols.from_id.to_string(), rel_cols.to_id.to_string())
        }
        _ => {
            // Direction::Outgoing or Direction::Either
            // (liker)-[:LIKES]->(post) means rel points from liker to post
            // rel.to_id = anchor (post), rel.from_id = new (liker)
            (rel_cols.to_id.to_string(), rel_cols.from_id.to_string())
        }
    };

    crate::debug_print!("  Generating swapped joins:");
    crate::debug_print!(
        "    rel.{} = {}.{} (anchor)",
        rel_col_to_anchor,
        graph_rel.right_connection,
        end_id_col
    );
    crate::debug_print!(
        "    {}.{} = rel.{} (new node)",
        graph_rel.left_connection,
        start_id_col,
        rel_col_to_new
    );

    // JOIN 1: Relationship table connecting to anchor (right_connection)
    let rel_join_condition = OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.alias.clone()),
                column: PropertyValue::Column(rel_col_to_anchor.clone()),
            }),
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.right_connection.clone()),
                column: PropertyValue::Column(end_id_col.clone()),
            }),
        ],
    };

    joins.push(Join {
        table_name: rel_table,
        table_alias: graph_rel.alias.clone(),
        joining_on: vec![rel_join_condition],
        join_type: JoinType::Left,
        pre_filter: None,
        from_id_column: Some(rel_col_to_anchor.clone()),
        to_id_column: Some(rel_col_to_new.clone()),
        graph_rel: None,
        is_cartesian: false,
    });

    // JOIN 2: New node (left_connection) connecting to relationship
    let new_node_join_condition = OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.left_connection.clone()),
                column: PropertyValue::Column(start_id_col),
            }),
            RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(graph_rel.alias.clone()),
                column: PropertyValue::Column(rel_col_to_new.clone()),
            }),
        ],
    };

    joins.push(Join {
        table_name: start_table,
        table_alias: graph_rel.left_connection.clone(),
        joining_on: vec![new_node_join_condition],
        join_type: JoinType::Left,
        pre_filter: None,
        from_id_column: None,
        to_id_column: None,
        graph_rel: None,
        is_cartesian: false,
    });

    Ok(joins)
}

pub(crate) fn rewrite_vlp_union_branch_aliases(
    plan: &mut RenderPlan,
    source_plan: &LogicalPlan,
    schema: &GraphSchema,
) -> RenderPlanBuilderResult<()> {
    log::debug!("TRACING: rewrite_vlp_union_branch_aliases called");
    log::info!(
        "🔍 VLP Union Branch: Checking for VLP CTEs... (found {} CTEs total)",
        plan.ctes.0.len()
    );

    // Check if this plan has VLP CTEs
    let vlp_mappings = extract_vlp_alias_mappings(&plan.ctes);

    if vlp_mappings.is_empty() {
        log::debug!("🔍 VLP Union Branch: No VLP mappings found, skipping rewrite");
        return Ok(()); // No VLP CTEs, nothing to rewrite
    }

    log::info!(
        "🔄 VLP Union Branch: Found {} VLP CTE(s), checking if rewrite is needed",
        vlp_mappings.len()
    );

    // 🔧 FIX: For OPTIONAL MATCH + VLP, FROM uses anchor table (not VLP CTE)
    // In this case, anchor node properties should NOT be rewritten
    // Detection: FROM uses regular table when VLP CTEs are present
    let is_optional_vlp = if let Some(from_ref) = &plan.from.0 {
        !from_ref.name.starts_with("vlp_") && !vlp_mappings.is_empty()
    } else {
        false
    };

    if is_optional_vlp {
        log::debug!("OPTIONAL VLP detected: FROM uses anchor table '{}', skipping VLP property rewriting for anchor nodes", 
            plan.from.0.as_ref().map(|f| f.name.as_str()).unwrap_or("unknown"));
        // Continue with rewriting, but skip start aliases in cte_column_mapping
    }

    // Extract VLP column metadata for property name resolution
    // This maps (cypher_alias, property_name) → cte_column_name
    // E.g., (a, email_address) → start_email (or start_email_address depending on CTE)
    let _cte_column_mapping: HashMap<(String, String), String> = HashMap::new();
    log::debug!("🔧 VLP: Total CTEs in plan: {}", plan.ctes.0.len());

    // ✨ NEW APPROACH: Build metadata-based lookup mapping
    // Maps: (cypher_alias, db_column) → (cte_column_name, vlp_position)
    // This is lookup-based, NOT heuristic-based. No splitting needed!
    let mut cte_column_mapping: HashMap<
        (String, String),
        (String, crate::render_plan::cte_manager::VlpColumnPosition),
    > = HashMap::new();

    for (idx, cte) in plan.ctes.0.iter().enumerate() {
        log::debug!(
            "🔧 VLP: CTE[{}]: name={}, columns={}, vlp_cypher_start={:?}, vlp_cypher_end={:?}",
            idx,
            cte.cte_name,
            cte.columns.len(),
            cte.vlp_cypher_start_alias,
            cte.vlp_cypher_end_alias
        );
        if cte.cte_name.starts_with("vlp_") {
            log::debug!(
                "🔧 VLP: Processing VLP CTE '{}' with {} columns",
                cte.cte_name,
                cte.columns.len()
            );
            for (col_idx, col_meta) in cte.columns.iter().enumerate() {
                if let Some(position) = col_meta.vlp_position {
                    log::debug!(
                        "🔧   Column[{}]: cte={}, alias={}, cypher_prop={}, db_col={}, pos={:?}",
                        col_idx,
                        col_meta.cte_column_name,
                        col_meta.cypher_alias,
                        col_meta.cypher_property,
                        col_meta.db_column,
                        position
                    );
                    // Build lookup key: (cypher_alias, db_column_name)
                    // This is what we'll match against when rewriting PropertyAccessExp
                    let key = (col_meta.cypher_alias.clone(), col_meta.db_column.clone());
                    let value = (col_meta.cte_column_name.clone(), position);
                    cte_column_mapping.insert(key, value);
                    log::info!(
                        "✅ VLP Lookup Entry: ({}, {}) → ({}, {:?})",
                        col_meta.cypher_alias,
                        col_meta.db_column,
                        col_meta.cte_column_name,
                        position
                    );
                }
            }
        }
    }

    log::debug!(
        "🔧 VLP: Built {} column lookup entries from CTE metadata (NO splitting!)",
        cte_column_mapping.len()
    );

    // ✨ ARCHITECTURAL FIX: Filter mappings based on whether endpoint JOINs exist
    //
    // For NORMAL VLP:
    //   - VLP CTEs contain: start_id, end_id, hop_count, path tracking, edge properties
    //   - VLP CTEs do NOT contain node properties!
    //   - Node properties fetched by JOINing to source tables
    //   - Therefore: Exclude endpoint aliases from rewriting (a → vlp1)
    //
    // For DENORMALIZED VLP:
    //   - VLP CTEs contain: Everything above PLUS node properties (from edge table)
    //   - No separate node tables exist - no JOINs added
    //   - Therefore: INCLUDE endpoint aliases for rewriting (a → vlp1)
    //
    // Detection Strategy: Check if endpoint JOINs exist in plan.joins
    //   - If endpoint JOINs exist → Normal VLP → Exclude endpoint aliases
    //   - If endpoint JOINs missing → Denormalized VLP → Include endpoint aliases
    let mut vlp_endpoint_aliases: HashSet<String> = HashSet::new();
    let mut endpoint_has_joins: HashMap<String, bool> = HashMap::new();

    // 🔧 FIX: Detect aliases that are covered by WITH CTEs
    // If FROM references a `with_*_cte_*` CTE, the corresponding alias should NOT be rewritten
    // because the WITH CTE already provides the aliased columns, not the raw VLP CTE
    let mut aliases_covered_by_with_cte: HashSet<String> = HashSet::new();

    // Check if FROM references a WITH CTE
    if let Some(from_ref) = &plan.from.0 {
        if is_generated_cte_name(&from_ref.name) {
            // Extract the alias from the FROM clause or the CTE name
            // CTE names are like "with_u2_cte_1" - extract "u2"
            if let Some(alias) = &from_ref.alias {
                aliases_covered_by_with_cte.insert(alias.clone());
                log::info!(
                    "🔧 VLP: FROM uses WITH CTE '{}' with alias '{}' - excluding from rewrite",
                    from_ref.name,
                    alias
                );
            } else if let Some(captured) = from_ref
                .name
                .strip_prefix("with_")
                .and_then(|s| s.split("_cte_").next())
            {
                aliases_covered_by_with_cte.insert(captured.to_string());
                log::info!(
                    "🔧 VLP: FROM uses WITH CTE '{}' covering alias '{}' - excluding from rewrite",
                    from_ref.name,
                    captured
                );
            }
        }
    }

    for cte in &plan.ctes.0 {
        if let (Some(start), Some(end)) = (&cte.vlp_cypher_start_alias, &cte.vlp_cypher_end_alias) {
            vlp_endpoint_aliases.insert(start.clone());
            vlp_endpoint_aliases.insert(end.clone());

            // Check if these endpoint aliases have corresponding JOINs
            let start_has_join = plan.joins.0.iter().any(|j| j.table_alias == *start);
            let end_has_join = plan.joins.0.iter().any(|j| j.table_alias == *end);

            endpoint_has_joins.insert(start.clone(), start_has_join);
            endpoint_has_joins.insert(end.clone(), end_has_join);

            log::info!(
                "🔍 VLP: Endpoint aliases: '{}' (has_join={}), '{}' (has_join={})",
                start,
                start_has_join,
                end,
                end_has_join
            );
        }
    }

    // #647: For an OPTIONAL VLP the FROM clause binds the ANCHOR node directly
    // (its properties come from the base table, NOT the VLP CTE), so the anchor
    // alias must be EXCLUDED from endpoint rewriting.
    //
    // Normal anchor-at-START layout: the anchor is the CTE's START alias, so the
    // original rule excludes every CTE `vlp_cypher_start_alias`. Kept verbatim so
    // all pre-existing shapes stay BYTE-IDENTICAL (including chained forward VLPs).
    //
    // END-anchored OPTIONAL VLP (`(a)<-[*]-(b)` / pre-bound `(a)-[*]->(b)`, #647):
    // the orientation is mirrored — the FROM binds the CTE END alias (the anchor),
    // and the CTE START alias is now the FAR (counted) endpoint that MUST be
    // rewritten to `vt0.start_*`. So for the end-anchored CTE: exclude its END
    // alias (the FROM anchor) and DO NOT exclude its START alias. Detected by the
    // FROM alias matching a CTE's END alias with a distinct START alias (excludes
    // closed VLP start==end, which keeps the historical start-exclusion).
    let end_anchored_from_alias: Option<String> = if is_optional_vlp {
        let from_alias = plan
            .from
            .0
            .as_ref()
            .and_then(|from_ref| from_ref.alias.clone());
        from_alias.filter(|fa| {
            plan.ctes.0.iter().any(|cte| {
                cte.vlp_cypher_end_alias.as_deref() == Some(fa.as_str())
                    && cte.vlp_cypher_start_alias.as_deref() != Some(fa.as_str())
            })
        })
    } else {
        None
    };

    let filtered_mappings: HashMap<String, String> = vlp_mappings
        .clone()
        .into_iter()
        .filter(|(cypher_alias, _vlp_alias)| {
            // 🔧 FIX: For OPTIONAL VLP, exclude the anchor alias from rewriting —
            // it refers to the anchor table in FROM, not the VLP CTE. Anchor is
            // the CTE START alias in the normal layout (original rule); for an
            // end-anchored VLP (#647) it is instead the CTE END alias, and that
            // CTE's START alias (the far endpoint) MUST stay included.
            if is_optional_vlp {
                for cte in &plan.ctes.0 {
                    if let Some(start_alias) = &cte.vlp_cypher_start_alias {
                        // #647: for the end-anchored CTE, the START alias is the
                        // far endpoint — keep it in the rewrite (→ `vt0.start_*`).
                        let this_cte_is_end_anchored = end_anchored_from_alias.as_deref()
                            == cte.vlp_cypher_end_alias.as_deref();
                        if !this_cte_is_end_anchored && cypher_alias == start_alias {
                            log::debug!(
                                "🔧 OPTIONAL VLP: Excluding start alias '{}' from rewrite (anchor table in FROM)",
                                cypher_alias
                            );
                            return false;
                        }
                    }
                }
            }
            if let Some(anchor_alias) = &end_anchored_from_alias {
                if cypher_alias == anchor_alias {
                    log::debug!(
                        "🔧 OPTIONAL VLP (#647 end-anchored): Excluding FROM-bound END anchor alias '{}' from rewrite",
                        cypher_alias
                    );
                    return false;
                }
            }

            // 🔧 FIX: Exclude aliases covered by WITH CTEs
            // These aliases reference the WITH CTE columns, not the raw VLP CTE
            if aliases_covered_by_with_cte.contains(cypher_alias) {
                log::debug!(
                    "🔧 VLP: Excluding alias '{}' from rewrite (covered by WITH CTE)",
                    cypher_alias
                );
                return false;
            }

            let is_endpoint = vlp_endpoint_aliases.contains(cypher_alias);
            if is_endpoint {
                // ✅ FIX: ALWAYS include endpoints for rewriting!
                log::debug!(
                    "✅ VLP: INCLUDING endpoint alias '{}' in rewrite (for correct column mapping)",
                    cypher_alias
                );
                return true;
            }
            true // Keep non-endpoint mappings (e.g., path variable)
        })
        .collect();

    // 🔧 ENHANCEMENT: Build a reverse mapping to infer start/end from CTE structure
    // CTE names are formatted as "vlp_{start}_{end}", so we can infer which endpoint is which
    // Example: cte_name = "vlp_u_f" means start="u", end="f"
    let mut endpoint_position: HashMap<String, &str> = HashMap::new();

    for cte in &plan.ctes.0 {
        if let (Some(start), Some(end)) = (
            cte.vlp_cypher_start_alias.as_ref(),
            cte.vlp_cypher_end_alias.as_ref(),
        ) {
            endpoint_position.insert(start.clone(), "start");
            endpoint_position.insert(end.clone(), "end");

            log::debug!(
                "🔄 VLP: Endpoint position mapping: '{}' = start, '{}' = end (from CTE {})",
                start,
                end,
                cte.cte_name
            );
        }
    }

    // #659: the LOGICAL node-id property name for each VLP endpoint alias.
    // `count(b)` normalizes to `count(b.<logical_id>)` (e.g. `b.code` on a denorm
    // Airport whose node_id is `code`), un-translated to its physical column. That
    // reference MISSES `cte_column_mapping` (keyed on db_column, e.g. `Dest`) and
    // the rewriter's prefix-fallback would build a non-existent `end_code`. Supply
    // the logical id name so the fallback can instead resolve it to the CTE's
    // canonical `start_id`/`end_id`. Composite ids are left out (stay loud, #605).
    let mut id_property_by_alias: HashMap<String, String> = HashMap::new();
    for alias in endpoint_position.keys() {
        if let Some(label) = super::cte_extraction::get_node_label_for_alias(alias, source_plan)
            .or_else(|| find_denorm_connection_node_label(source_plan, alias, schema))
        {
            if let Some(node_schema) = schema.node_schema_opt(&label) {
                if !node_schema.node_id.is_composite() {
                    id_property_by_alias
                        .insert(alias.clone(), node_schema.node_id.column().to_string());
                }
            }
        }
    }

    if filtered_mappings.is_empty() {
        log::debug!("🔍 VLP Union Branch: All mappings filtered out - nothing to rewrite");
        return Ok(());
    }

    log::info!(
        "🔄 VLP Union Branch: Applying {} filtered mapping(s) (excluded {} endpoint aliases)",
        filtered_mappings.len(),
        vlp_endpoint_aliases.len()
    );

    // Log what mappings we're applying
    for (from, to) in &filtered_mappings {
        log::debug!("   Mapping: {} → {}", from, to);
    }

    // 🔍 DEBUG: Log CTE column mapping entries
    log::debug!(
        "🔍 DEBUG: CTE column mapping has {} entries:",
        cte_column_mapping.len()
    );
    for ((alias, db_col), (cte_col, pos)) in &cte_column_mapping {
        log::debug!("   ({}, {}) → ({}, {:?})", alias, db_col, cte_col, pos);
    }

    // The VLP CTE's table alias to use when rewriting endpoint references in
    // SELECT / WHERE / GROUP BY / ORDER BY. For a REQUIRED VLP the outer FROM IS
    // the VLP CTE (`FROM vlp_a_b AS t`), so it's the FROM alias. For an OPTIONAL
    // VLP the FROM is the anchor table and the VLP CTE is LEFT JOINed
    // (`LEFT JOIN vlp_a_b AS vt0`), so it's the JOIN alias. Computed ONCE here so
    // every clause agrees — previously WHERE and GROUP BY hardcoded `"t"`, which
    // dangled for OPTIONAL VLP where the join alias is `vt0` (#630; SELECT/ORDER
    // BY already used this value).
    let vlp_join_count = plan
        .joins
        .0
        .iter()
        .filter(|j| j.table_name.starts_with("vlp_"))
        .count();
    let vlp_from_alias = if is_optional_vlp {
        plan.joins
            .0
            .iter()
            .find(|j| j.table_name.starts_with("vlp_"))
            .map(|j| j.table_alias.clone())
            .unwrap_or_else(|| "t".to_string())
    } else {
        plan.from
            .0
            .as_ref()
            .and_then(|from_ref| from_ref.alias.as_ref())
            .cloned()
            .unwrap_or_else(|| "t".to_string())
    };

    // The alias to use for the WHERE / GROUP BY endpoint rewrite. Use the
    // resolved `vlp_from_alias` ONLY when there is a single VLP join (or the
    // required-VLP FROM case) so the endpoint reference is UNAMBIGUOUS. With TWO
    // OR MORE VLP joins (`OPTIONAL MATCH (a)-[*]->(b) OPTIONAL MATCH (b)-[*]->(c)`)
    // `vlp_from_alias` is just the FIRST vlp join — using it for GROUP BY on the
    // SECOND endpoint would silently group by the WRONG VLP (a separate,
    // deeper endpoint-resolution defect, tracked as #643). Keep the historical
    // `"t"` there so that shape continues to FAIL LOUD (Code 47) rather than
    // return a silently-wrong aggregate (#630 must not trade loud for silent).
    let group_where_alias: &str = if is_optional_vlp && vlp_join_count > 1 {
        "t"
    } else {
        &vlp_from_alias
    };

    // 🔧 CRITICAL: Check if this is a multi-type VLP (from CTE name)
    // Multi-type VLP CTEs use Cypher aliases directly in SELECT (e.g., x.end_type)
    // and properties are extracted via JSON_VALUE() - no table alias rewriting needed
    let is_multi_type_vlp = plan
        .ctes
        .0
        .iter()
        .any(|cte| cte.cte_name.starts_with("vlp_multi_type_"));

    if is_multi_type_vlp {
        log::info!(
            "🎯 VLP: Multi-type VLP detected - FROM uses Cypher alias, no rewriting needed!"
        );
        // With the correct FROM (vlp_multi_type_u_x AS x), everything works naturally:
        //   - x.end_type → CTE column (direct access)
        //   - x.name → property (SQL generator extracts from end_properties JSON)
        // No table alias rewriting needed - the FROM clause is already correct!
    } else {
        // `vlp_from_alias` (computed above, before the multi-type check) is the
        // VLP CTE alias to rewrite endpoint references to — the FROM alias for a
        // REQUIRED VLP, the JOIN alias for an OPTIONAL one.
        log::debug!(
            "🔧 VLP: VLP table alias to use: '{}' (is_optional_vlp={})",
            vlp_from_alias,
            is_optional_vlp
        );

        // Rewrite SELECT items using filtered VLP mappings (for non-multi-type VLP)
        log::debug!("🔍 VLP: Rewriting {} SELECT items", plan.select.items.len());
        for (idx, select_item) in plan.select.items.iter_mut().enumerate() {
            log::debug!("   SELECT[{}]: {:?}", idx, select_item.expression);
            rewrite_render_expr_for_vlp_with_endpoint_info(
                &mut select_item.expression,
                &filtered_mappings,
                &vlp_from_alias,
                &endpoint_position,
                &cte_column_mapping,
                &id_property_by_alias,
            );
        }

        // #608: ORDER BY on a VLP endpoint property must get the SAME
        // endpoint-column rewrite the SELECT items just got — `b.name` →
        // `<vlp_join_alias>.end_name` (the CTE projects the endpoint's property
        // as a prefixed `end_<prop>` / `start_<prop>` column). Without this
        // ORDER BY reached `rewrite_vlp_aggregate_aliases`, which only swaps the
        // table alias (`b` → `vt0`) and leaves the column as `name`, emitting
        // `vt0.name` — a column the CTE does not expose (Code 47 at execution).
        //
        // Scope to OPTIONAL VLP only: for a REQUIRED VLP the outer query's FROM
        // IS the VLP CTE and its ORDER BY is already rewritten to `t.end_name`
        // downstream by `rewrite_vlp_select_aliases` (to_sql_query.rs). Running
        // this rewrite there too double-processes it and diverges per SQL
        // dialect (one dialect regressed to a backtick-quoted `b.name`). OPTIONAL
        // VLP is exactly the shape that pass returns early on (FROM is the anchor
        // table, not the CTE), so it never rewrote ORDER BY — the #608 gap.
        // Reuse the same mappings/alias/position the SELECT loop used (here
        // `vlp_from_alias` is the VLP CTE's JOIN alias, e.g. `vt0`). The anchor
        // property (`a.name`) is excluded by the OPTIONAL-VLP start-alias
        // filter, so ORDER BY on the anchor is unaffected.
        if is_optional_vlp {
            log::debug!("🔍 VLP: Rewriting {} ORDER BY items", plan.order_by.0.len());
            for (idx, order_item) in plan.order_by.0.iter_mut().enumerate() {
                log::debug!("   ORDER BY[{}]: {:?}", idx, order_item.expression);
                rewrite_render_expr_for_vlp_with_endpoint_info(
                    &mut order_item.expression,
                    &filtered_mappings,
                    &vlp_from_alias,
                    &endpoint_position,
                    &cte_column_mapping,
                    &id_property_by_alias,
                );
            }
        }
    }

    // CRITICAL: Also rewrite WHERE clause expressions
    // The WHERE clause may contain filters on Cypher aliases (e.g., friend.firstName = 'Wei')
    // that need to be rewritten to use VLP table aliases (e.g., end_node.firstName = 'Wei')
    if let Some(where_expr) = &mut plan.filters.0 {
        log::debug!("🔄 VLP Union Branch: Rewriting WHERE clause");
        rewrite_render_expr_for_vlp_with_endpoint_info(
            where_expr,
            &filtered_mappings,
            group_where_alias,
            &endpoint_position,
            &cte_column_mapping,
            &id_property_by_alias,
        );
    }

    // 🔧 FIX #5: Also rewrite GROUP BY expressions
    // The GROUP BY clause may contain Cypher aliases (e.g., f.DestCityName)
    // that need to be rewritten to use VLP table aliases (e.g., vlp4.DestCityName)
    log::info!(
        "🔍 VLP: Rewriting {} GROUP BY expressions",
        plan.group_by.0.len()
    );
    for (idx, group_expr) in plan.group_by.0.iter_mut().enumerate() {
        log::debug!("   GROUP BY[{}]: {:?}", idx, group_expr);
        rewrite_render_expr_for_vlp_with_endpoint_info(
            group_expr,
            &filtered_mappings,
            group_where_alias,
            &endpoint_position,
            &cte_column_mapping,
            &id_property_by_alias,
        );
    }

    // 🔧 FIX #6: Also rewrite CTE bodies - BUT ONLY FOR PATH FUNCTION REWRITES (t → vlp1)
    // DO NOT rewrite endpoint aliases (u1 → start_node) in WITH CTEs!
    //
    // WITH CTEs have their own JOINs like: JOIN users AS u1 ON vlp1.start_id = u1.user_id
    // So their SELECT items should use u1/u2 (from JOINs), NOT start_node/end_node (VLP internal)
    //
    // We ONLY need to rewrite the generic "t" alias that comes from path functions
    // like length(path) → t.hop_count, which should become vlp1.hop_count
    log::info!(
        "🔍 VLP: Rewriting {} CTE bodies (PATH FUNCTIONS ONLY)",
        plan.ctes.0.len()
    );

    // Create a mapping that ONLY includes the "t" → vlp alias mapping
    // Exclude endpoint aliases (u1, u2, etc.) for CTE body rewriting
    //
    // Rationale:
    // - Normal VLP: CTE has JOINs (JOIN users AS u1), SELECT should use u1.name ✅
    // - Denormalized VLP: CTE has NO JOINs, properties from VLP CTE columns (vlp1_Origin)
    //   BUT the column names in CTE are already prefixed (u1_name), so we don't rewrite table aliases
    let path_function_mappings: HashMap<String, String> = filtered_mappings
        .iter()
        .filter(|(from_alias, _to_alias)| {
            // Only keep VLP_CTE_FROM_ALIAS mapping (for path functions like length(path))
            // Exclude endpoint node aliases
            *from_alias == VLP_CTE_FROM_ALIAS
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    log::info!(
        "🔍 VLP: Path function mappings for CTE rewrite: {:?}",
        path_function_mappings
    );

    if !path_function_mappings.is_empty() {
        for (idx, cte) in plan.ctes.0.iter_mut().enumerate() {
            // Skip VLP CTEs themselves - only rewrite CTEs that reference VLP results
            if cte.cte_name.starts_with("vlp_cte") || cte.cte_name.starts_with("chained_path_") {
                log::debug!("   CTE[{}]: Skipping VLP CTE '{}'", idx, cte.cte_name);
                continue;
            }

            log::info!(
                "   CTE[{}]: Rewriting path functions in CTE body '{}'",
                idx,
                cte.cte_name
            );

            // CTEs have a content field that can be Structured(RenderPlan) or RawSql(String)
            // We only need to rewrite Structured CTEs
            if let CteContent::Structured(ref mut cte_plan) = cte.content {
                // Rewrite SELECT items in the CTE (only t → vlp alias)
                log::info!(
                    "      CTE: Rewriting {} SELECT items (path functions only)",
                    cte_plan.select.items.len()
                );
                for (item_idx, select_item) in cte_plan.select.items.iter_mut().enumerate() {
                    log::info!(
                        "         SELECT[{}]: {:?}",
                        item_idx,
                        select_item.expression
                    );
                    rewrite_render_expr_for_vlp_with_from_alias(
                        &mut select_item.expression,
                        &path_function_mappings,
                        "t",
                    );
                }

                // Rewrite WHERE clause if present
                if let Some(ref mut where_expr) = cte_plan.filters.0 {
                    log::debug!("      CTE: Rewriting WHERE clause (path functions only)");
                    rewrite_render_expr_for_vlp_with_from_alias(
                        where_expr,
                        &path_function_mappings,
                        "t",
                    );
                }

                // Rewrite GROUP BY if present
                log::info!(
                    "      CTE: Rewriting {} GROUP BY expressions (path functions only)",
                    cte_plan.group_by.0.len()
                );
                for (group_idx, group_expr) in cte_plan.group_by.0.iter_mut().enumerate() {
                    log::debug!("         GROUP BY[{}]: {:?}", group_idx, group_expr);
                    rewrite_render_expr_for_vlp_with_from_alias(
                        group_expr,
                        &path_function_mappings,
                        "t",
                    );
                }
            }
        }
    } else {
        log::debug!("🔍 VLP: No path function mappings - skipping CTE body rewrite");
    }

    Ok(())
}

/// Detect if an alias is a VLP (Variable-Length Path) endpoint by examining the plan structure.
///
/// This is a fallback method when plan_ctx is not available during rendering.
/// It traverses the plan to find GraphRel nodes with variable_length and determines
/// if the alias is a start or end endpoint.
///
/// Returns Some(VlpEndpointInfo) if the alias is a VLP endpoint, None otherwise.
use crate::query_planner::join_context::VlpEndpointInfo;

fn detect_vlp_endpoint_from_plan(plan: &LogicalPlan, alias: &str) -> Option<VlpEndpointInfo> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this is a variable-length pattern (not fixed-length like *1, *2)
            if let Some(spec) = &rel.variable_length {
                // Fixed-length patterns (*1, *2, *3) don't use CTE column naming
                let is_fixed_length = spec.exact_hop_count().is_some();
                if is_fixed_length {
                    // Continue searching in child nodes
                    if let Some(info) = detect_vlp_endpoint_from_plan(&rel.left, alias) {
                        return Some(info);
                    }
                    if let Some(info) = detect_vlp_endpoint_from_plan(&rel.right, alias) {
                        return Some(info);
                    }
                    return None;
                }

                // Check if alias matches left_connection (start endpoint)
                if alias == rel.left_connection {
                    log::debug!(
                        "🔍 detect_vlp_endpoint_from_plan: '{}' is VLP START endpoint (rel='{}')",
                        alias,
                        rel.alias
                    );
                    return Some(VlpEndpointInfo {
                        position: VlpPosition::Start,
                        other_endpoint_alias: rel.right_connection.clone(),
                        rel_alias: rel.alias.clone(),
                        vlp_alias: crate::query_planner::join_context::VLP_CTE_FROM_ALIAS
                            .to_string(),
                    });
                }

                // Check if alias matches right_connection (end endpoint)
                if alias == rel.right_connection {
                    log::debug!(
                        "🔍 detect_vlp_endpoint_from_plan: '{}' is VLP END endpoint (rel='{}')",
                        alias,
                        rel.alias
                    );
                    return Some(VlpEndpointInfo {
                        position: VlpPosition::End,
                        other_endpoint_alias: rel.left_connection.clone(),
                        rel_alias: rel.alias.clone(),
                        vlp_alias: crate::query_planner::join_context::VLP_CTE_FROM_ALIAS
                            .to_string(),
                    });
                }
            }

            // Not a VLP endpoint in this GraphRel, search children
            if let Some(info) = detect_vlp_endpoint_from_plan(&rel.left, alias) {
                return Some(info);
            }
            if let Some(info) = detect_vlp_endpoint_from_plan(&rel.right, alias) {
                return Some(info);
            }
            None
        }
        LogicalPlan::GraphNode(_) => None,
        LogicalPlan::ViewScan(_) => None,
        LogicalPlan::Projection(proj) => detect_vlp_endpoint_from_plan(&proj.input, alias),
        LogicalPlan::Filter(filter) => detect_vlp_endpoint_from_plan(&filter.input, alias),
        LogicalPlan::Limit(limit) => detect_vlp_endpoint_from_plan(&limit.input, alias),
        LogicalPlan::GraphJoins(gj) => detect_vlp_endpoint_from_plan(&gj.input, alias),
        // UNWIND wraps the MATCH in a WITH segment (`MATCH ...VLP... UNWIND ... WITH b,n`),
        // so recurse into its input to find the VLP GraphRel below — otherwise a VLP
        // endpoint like `b` is missed here, falls through to STEP 3, and is rendered
        // against a non-existent base alias instead of the VLP CTE's end_* columns. (#410)
        LogicalPlan::Unwind(u) => detect_vlp_endpoint_from_plan(&u.input, alias),
        LogicalPlan::WithClause(wc) => detect_vlp_endpoint_from_plan(&wc.input, alias),
        LogicalPlan::OrderBy(ob) => detect_vlp_endpoint_from_plan(&ob.input, alias),
        LogicalPlan::Skip(skip) => detect_vlp_endpoint_from_plan(&skip.input, alias),
        LogicalPlan::CartesianProduct(cp) => {
            if let Some(info) = detect_vlp_endpoint_from_plan(&cp.left, alias) {
                return Some(info);
            }
            detect_vlp_endpoint_from_plan(&cp.right, alias)
        }
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                if let Some(info) = detect_vlp_endpoint_from_plan(input, alias) {
                    return Some(info);
                }
            }
            None
        }
        _ => None,
    }
}

/// Find the Cypher property name used as the node ID for a given alias.
/// For denormalized nodes, the ViewScan.id_column is the ClickHouse column (e.g., origin_code),
/// but CTE columns use Cypher property names (e.g., code). This function reverse-looks up
/// the Cypher property from from_node_properties.
fn find_cypher_id_property_for_alias(plan: &LogicalPlan, alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            // Reverse lookups iterate a HashMap; sort by cypher property name so
            // that when several properties map to the id column the returned one
            // is deterministic across processes (#480 class).
            let sorted_id_match = |props: &std::collections::HashMap<
                String,
                crate::graph_catalog::expression_parser::PropertyValue,
            >,
                                   id_column: &str|
             -> Option<String> {
                let mut entries: Vec<_> = props.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                entries
                    .into_iter()
                    .find(|(_, ch_col)| ch_col.raw() == id_column)
                    .map(|(cypher_prop, _)| cypher_prop.clone())
            };
            if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                if let Some(ref from_props) = scan.from_node_properties {
                    if let Some(found) = sorted_id_match(from_props, &scan.id_column) {
                        return Some(found);
                    }
                }
            } else if let LogicalPlan::Union(union_plan) = node.input.as_ref() {
                // For denormalized nodes with UNION input, check first branch
                if let Some(first) = union_plan.inputs.first() {
                    if let LogicalPlan::ViewScan(scan) = first.as_ref() {
                        if let Some(ref from_props) = scan.from_node_properties {
                            if let Some(found) = sorted_id_match(from_props, &scan.id_column) {
                                return Some(found);
                            }
                        }
                    }
                }
            }
            None
        }
        LogicalPlan::GraphRel(rel) => find_cypher_id_property_for_alias(&rel.left, alias)
            .or_else(|| find_cypher_id_property_for_alias(&rel.right, alias)),
        LogicalPlan::Projection(p) => find_cypher_id_property_for_alias(&p.input, alias),
        LogicalPlan::Filter(f) => find_cypher_id_property_for_alias(&f.input, alias),
        LogicalPlan::GroupBy(g) => find_cypher_id_property_for_alias(&g.input, alias),
        LogicalPlan::GraphJoins(j) => find_cypher_id_property_for_alias(&j.input, alias),
        LogicalPlan::OrderBy(o) => find_cypher_id_property_for_alias(&o.input, alias),
        LogicalPlan::Skip(s) => find_cypher_id_property_for_alias(&s.input, alias),
        LogicalPlan::Limit(l) => find_cypher_id_property_for_alias(&l.input, alias),
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .find_map(|i| find_cypher_id_property_for_alias(i, alias)),
        LogicalPlan::CartesianProduct(cp) => find_cypher_id_property_for_alias(&cp.left, alias)
            .or_else(|| find_cypher_id_property_for_alias(&cp.right, alias)),
        LogicalPlan::WithClause(wc) => find_cypher_id_property_for_alias(&wc.input, alias),
        _ => None,
    }
}

/// Compute the CTE ID column name for an alias using the deterministic formula.
/// This should be called AFTER expand_table_alias_to_select_items generates the columns.
///
/// CTE columns use Cypher property names (e.g., a_code), not ClickHouse column names
/// (e.g., a_origin_code). For denormalized nodes where these differ, we use the
/// Cypher property name from the schema's from_node_properties mapping.
///
/// This is the single source of truth for alias→ID column mapping.
pub(crate) fn compute_cte_id_column_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    // For denormalized nodes, use the Cypher property name (e.g., "code")
    // rather than the ClickHouse column (e.g., "origin_code")
    if let Some(cypher_id) = find_cypher_id_property_for_alias(plan, alias) {
        log::info!(
            "📊 compute_cte_id: alias '{}' → Cypher ID property '{}' (denormalized)",
            alias,
            cypher_id
        );
        return Some(cte_column_name(alias, &cypher_id));
    }

    // Fallback: use find_id_column_for_alias (returns ClickHouse column name)
    if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
        Some(cte_column_name(alias, &id_col))
    } else {
        None
    }
}

pub(crate) fn expand_table_alias_to_select_items(
    alias: &str,
    plan: &LogicalPlan,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references: &HashMap<String, String>,
    has_aggregation: bool,
    plan_ctx: Option<&PlanCtx>,
    _vlp_cte_metadata: Option<
        &HashMap<String, (String, Vec<crate::render_plan::CteColumnMetadata>)>,
    >,
) -> Vec<SelectItem> {
    log::info!(
        "🔍 expand_table_alias_to_select_items: Expanding alias '{}', cte_references={:?}",
        alias,
        cte_references
    );

    // STEP 1: Check if analyzer resolved this alias to a CTE
    if let Some(cte_name) = cte_references.get(alias) {
        log::info!(
            "✅ expand_table_alias_to_select_items: Found CTE ref '{}' -> '{}'",
            alias,
            cte_name
        );
        log::info!(
            "🔍 expand_table_alias_to_select_items: Available CTE schemas: {:?}",
            cte_schemas.keys().collect::<Vec<_>>()
        );

        // STEP 2: Get columns from that CTE with this alias prefix
        if let Some(meta) = cte_schemas.get(cte_name) {
            log::info!(
                "✅ expand_table_alias_to_select_items: Found CTE schema '{}' with {} items",
                cte_name,
                meta.select_items.len()
            );
            // Calculate the CTE alias used in FROM clause
            // Special case: __union_vlp is a pseudo-CTE representing UNION results
            // The actual subquery alias is __union
            let cte_alias = if cte_name == "__union_vlp" {
                "__union".to_string()
            } else {
                // Normal CTE: extract FROM alias (e.g., "with_a_b_cte_1" -> "a_b")
                extract_from_alias_from_cte_name(cte_name).to_string()
            };

            let is_union_reference = cte_name == "__union_vlp";

            let alias_prefix_underscore = format!("{}_", alias);
            let alias_prefix_dot = format!("{}.", alias);
            log::debug!(
                "expand_table_alias_to_select_items: CTE '{}' has {} items",
                cte_name,
                meta.select_items.len()
            );
            let filtered_items: Vec<SelectItem> = meta.select_items.iter()
                .filter(|item| {
                    if let Some(col_alias) = &item.col_alias {
                        // Match columns that belong to this alias:
                        // 1. Parse p{N}_ format (e.g., "p6_person_id" for alias "person")
                        // 2. Start with alias_ (legacy format, e.g., "friend_firstName")
                        // 3. Start with alias. (e.g., "friend.birthday" from UNION subqueries)
                        // 4. Exactly match the alias (e.g., "cnt" for alias "cnt")
                        let matches_parsed = parse_cte_column(&col_alias.0)
                            .map(|(parsed_alias, _)| parsed_alias == alias)
                            .unwrap_or(false);
                        let matches_underscore = col_alias.0.starts_with(&alias_prefix_underscore);
                        let matches_dot = col_alias.0.starts_with(&alias_prefix_dot);
                        let matches_exact = col_alias.0 == alias;
                        matches_parsed || matches_underscore || matches_dot || matches_exact
                    } else {
                        false
                    }
                })
                .map(|item| {
                    // CRITICAL: Rewrite to use CTE's column names and table alias
                    // The CTE has columns like "a_city", "a_name" (from col_alias)
                    // We need to reference them as: a_b.a_city, a_b.a_name
                    // NOT the original DB columns like: a_b.city, a_b.full_name
                    //
                    // ALSO: For UNION subquery columns with dots (e.g., "friend.birthday"),
                    // we reference them as quoted identifiers and output underscore aliases
                    let (mut rewritten_expr, output_alias) = if let Some(ref cte_col_alias) = item.col_alias {
                        // Check if column name has dots (from UNION subquery)
                        let col_name = &cte_col_alias.0;
                        if col_name.contains('.') {
                            // Column with dot notation (e.g., "friend.birthday")
                            // Handling depends on whether we're referencing a UNION or a CTE:
                            // - UNION: columns aliased as "friend.birthday" (quoted) → reference as __union."friend.birthday"
                            // - CTE: columns aliased as friend_birthday (underscore) → reference as cte_alias.friend_birthday
                            let normalized_alias = col_name.replace('.', "_");

                            if is_union_reference {
                                // UNION reference: use quoted dotted column name
                                (
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(cte_alias.to_string()),
                                        column: PropertyValue::Column(col_name.clone()),
                                    }),
                                    Some(ColumnAlias(normalized_alias)),
                                )
                            } else {
                                // CTE reference: use normalized underscore column name
                                (
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(cte_alias.to_string()),
                                        column: PropertyValue::Column(normalized_alias.clone()),
                                    }),
                                    Some(ColumnAlias(normalized_alias)),
                                )
                            }
                        } else {
                            // Normal underscore column: use as-is
                            (
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_alias.to_string()),
                                    column: PropertyValue::Column(col_name.clone()),
                                }),
                                item.col_alias.clone(),
                            )
                        }
                    } else {
                        // Fallback: use original expression (shouldn't happen for CTE columns)
                        (item.expression.clone(), item.col_alias.clone())
                    };

                    // 🔧 FIX: Wrap with any() aggregation if needed
                    // When has_aggregation=true, non-ID columns must be wrapped with any()
                    // to be valid in SELECT with GROUP BY
                    if has_aggregation {
                        // Check if this column is an ID column
                        // ID columns end with "_id" or ".id" (e.g., "friend_id", "friend.id")
                        let is_id_column = if let Some(ref alias_obj) = output_alias {
                            let alias_str = &alias_obj.0;
                            alias_str.ends_with("_id") || alias_str.ends_with(".id")
                        } else {
                            false
                        };

                        if !is_id_column {
                            // Wrap non-ID column with anyLast() aggregation
                            // Note: Use anyLast() not any() to avoid conflict with list predicate any() function
                            rewritten_expr = RenderExpr::AggregateFnCall(AggregateFnCall {
                                name: "anyLast".to_string(),
                                args: vec![rewritten_expr],
                            });
                            log::debug!("🔧 expand_table_alias_to_select_items: Wrapped column '{:?}' with anyLast() for aggregation", output_alias);
                        }
                    }

                    SelectItem {
                        expression: rewritten_expr,
                        col_alias: output_alias,
                    }
                })
                .collect();

            if !filtered_items.is_empty() {
                log::info!(
                    "🔧 expand_table_alias_to_select_items: Found alias '{}' in CTE '{}' ({} columns), using CTE alias '{}'",
                    alias, cte_name, filtered_items.len(), cte_alias
                );
                return filtered_items;
            } else if cte_name.starts_with("vlp_") {
                // VLP CTE columns use start_*/end_* naming, not alias_* prefix.
                // Determine VLP position from metadata, then generate all properties
                // from base schema with the correct VLP column prefix.
                let mut vlp_position_prefix: Option<(String, String)> = None; // (prefix, from_alias)
                if let Some(vlp_metadata) = _vlp_cte_metadata {
                    if let Some((vlp_from_alias, col_metadata)) = vlp_metadata.get(cte_name) {
                        // Find the VLP position for this alias from any matching column
                        for col_meta in col_metadata {
                            if col_meta.cypher_alias == alias {
                                if let Some(pos) = &col_meta.vlp_position {
                                    let prefix = match pos {
                                        super::cte_manager::VlpColumnPosition::Start => "start_",
                                        super::cte_manager::VlpColumnPosition::End => "end_",
                                    };
                                    vlp_position_prefix =
                                        Some((prefix.to_string(), vlp_from_alias.clone()));
                                    break;
                                }
                            }
                        }
                    }
                }
                if let Some((prefix, from_alias)) = vlp_position_prefix {
                    // Get property list from base schema, filtered to only those
                    // actually propagated in the VLP CTE (start_*/end_* columns).
                    // Use property requirements to determine what's needed.
                    if let Ok((properties, _)) = plan.get_properties_with_table_alias(alias) {
                        if !properties.is_empty() {
                            // Use property requirements to filter if available
                            let required_props: Option<&std::collections::HashSet<String>> =
                                plan_ctx
                                    .and_then(|ctx| ctx.get_property_requirements())
                                    .and_then(|reqs| reqs.get_requirements(alias));

                            let vlp_items: Vec<SelectItem> = properties
                                .iter()
                                .filter(|(cypher_prop, _db_col)| {
                                    // Always include 'id' property; if requirements exist,
                                    // only include required ones; otherwise include all
                                    *cypher_prop == "id"
                                        || required_props
                                            .is_none_or(|r| r.contains(cypher_prop.as_str()))
                                })
                                .map(|(cypher_prop, db_col)| {
                                    // VLP CTE column: prefix + db_column (e.g., start_content)
                                    let vlp_col = format!("{}{}", prefix, db_col);
                                    let expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(from_alias.clone()),
                                        column: PropertyValue::Column(vlp_col),
                                    });
                                    let col_alias_name =
                                        crate::utils::cte_column_naming::cte_column_name(
                                            alias,
                                            cypher_prop,
                                        );
                                    SelectItem {
                                        expression: expr,
                                        col_alias: Some(ColumnAlias(col_alias_name)),
                                    }
                                })
                                .collect();
                            if !vlp_items.is_empty() {
                                log::info!(
                                    "🔧 expand_table_alias_to_select_items: VLP CTE '{}' → {} columns for alias '{}' (prefix={})",
                                    cte_name, vlp_items.len(), alias, prefix
                                );
                                return vlp_items;
                            }
                        }
                    }
                }
                log::warn!(
                    "⚠️ expand_table_alias_to_select_items: VLP CTE '{}' — could not determine position for alias '{}', falling through",
                    cte_name, alias
                );
                // Continue to fallback as recovery attempt
            } else {
                // CTE exists but no columns matched the alias prefix
                // This is an INTERNAL ERROR - analyzer said this alias is from this CTE,
                // but the CTE doesn't have the expected columns!
                log::error!(
                    "❌ INTERNAL ERROR: CTE '{}' found but no columns match prefix '{}_'! Analyzer/render mismatch!",
                    cte_name, alias
                );
                log::error!(
                    "❌ CTE '{}' has {} total columns: {:?}",
                    cte_name,
                    meta.select_items.len(),
                    meta.select_items
                        .iter()
                        .filter_map(|item| item.col_alias.as_ref().map(|a| &a.0))
                        .collect::<Vec<_>>()
                );
                // Continue to fallback as recovery attempt
            }
        } else {
            // CTE not in schemas - could be legitimate if schemas not yet built for this level
            log::warn!("⚠️ expand_table_alias_to_select_items: CTE '{}' not found in cte_schemas (may not be built yet)", cte_name);
        }
    }

    // STEP 2.5: Check if this alias is a VLP endpoint (needs CTE column naming)
    // VLP endpoints like u2 in (u1)-[*1..2]->(u2) need to use columns like t.end_city
    // instead of u2.city from the base table
    //
    // CRITICAL: Only use VLP info if the CURRENT plan tree actually contains a VLP pattern
    // for this alias. PlanCtx registers VLP endpoints globally, but when building a WITH CTE
    // body, the VLP may be in a LATER scope (after the WITH). Using VLP columns from a later
    // scope contaminates the current CTE with wrong column names.
    let vlp_info_from_plan = detect_vlp_endpoint_from_plan(plan, alias);
    let vlp_info_from_ctx = if vlp_info_from_plan.is_some() {
        // Plan tree confirms VLP — prefer ctx info (more detailed) if available
        plan_ctx.and_then(|ctx| ctx.get_vlp_endpoint(alias))
    } else {
        None
    };

    if let Some(vlp_info) = vlp_info_from_ctx.or(vlp_info_from_plan.as_ref()) {
        log::info!(
            "✅ expand_table_alias_to_select_items: Alias '{}' is VLP endpoint (position={:?}), generating VLP columns",
            alias, vlp_info.position
        );

        // Get properties from the base table to know what columns to generate
        if let Ok((properties, _)) = plan.get_properties_with_table_alias(alias) {
            if !properties.is_empty() {
                // Determine the column prefix based on VLP position
                let col_prefix = match vlp_info.position {
                    VlpPosition::Start => "start",
                    VlpPosition::End => "end",
                };

                // Get property requirements for pruning optimization
                let property_requirements =
                    plan_ctx.and_then(|ctx| ctx.get_property_requirements());

                // Generate SELECT items with VLP column naming
                // VLP CTE columns are named: start_id, end_id, start_city, end_city, etc.
                let mut items = Vec::new();

                // First, add ID column
                // For VLP endpoints, find_id_column_for_alias returns "start_id" or "end_id" directly
                // (these are the VLP CTE column names, not raw DB column names)
                // So we should NOT prefix them again - use them directly
                if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
                    // 🔧 FIX: Don't double-prefix VLP ID columns.
                    // The VLP recursive CTE ALWAYS names the identity column `start_id`/`end_id`
                    // (generic `_id` suffix), regardless of the schema's node_id property name —
                    // see `add_property_selections` in cte_manager which skips `prop.alias == "id"`
                    // and emits the id explicitly as `start_id`/`end_id`. So a renamed node_id
                    // (e.g. `user_id`) must still reference `end_id`, NOT `end_user_id` (issue #411).
                    let vlp_col_name = if id_col.starts_with(col_prefix) {
                        id_col.clone()
                    } else {
                        format!("{}_id", col_prefix)
                    };
                    // 🔧 CRITICAL FIX (Jan 23, 2026): Don't use explicit table alias for VLP columns during WITH clause expansion
                    // During WITH clause rendering, the FROM alias isn't final yet, so we generate columns without
                    // a table qualifier. The SQL generator will add the correct alias when rendering FROM clauses.
                    items.push(SelectItem {
                        expression: RenderExpr::Column(Column(PropertyValue::Column(
                            vlp_col_name.clone(),
                        ))),
                        col_alias: Some(ColumnAlias(cte_column_name(alias, &id_col))),
                    });
                }

                // Add property columns (e.g., end_city AS u2_city)
                for (prop_name, _) in &properties {
                    // Skip ID column (already added above)
                    if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
                        if prop_name == &id_col {
                            continue;
                        }
                    }

                    // Check property requirements for pruning
                    if let Some(reqs) = property_requirements {
                        // If not wildcard and has specific requirements, check if property is needed
                        if !reqs.requires_all(alias) {
                            if let Some(props_needed) = reqs.get_requirements(alias) {
                                if !props_needed.contains(prop_name) {
                                    continue;
                                }
                            }
                        }
                    }

                    // VLP CTE columns are named: end_city, end_name, etc.
                    let vlp_col_name = format!("{}_{}", col_prefix, prop_name);
                    // 🔧 CRITICAL FIX (Jan 23, 2026): Use bare Column expression instead of PropertyAccessExp with table alias
                    // This allows the column to be resolved from context (the FROM clause) rather than requiring a specific alias
                    let mut expr =
                        RenderExpr::Column(Column(PropertyValue::Column(vlp_col_name.clone())));

                    // Wrap with anyLast() if aggregation is needed
                    if has_aggregation {
                        expr = RenderExpr::AggregateFnCall(AggregateFnCall {
                            name: "anyLast".to_string(),
                            args: vec![expr],
                        });
                    }

                    items.push(SelectItem {
                        expression: expr,
                        col_alias: Some(ColumnAlias(cte_column_name(alias, prop_name))),
                    });
                }

                log::info!(
                    "🔧 expand_table_alias_to_select_items: Generated {} VLP columns for alias '{}' (prefix='{}', using bare Column expressions)",
                    items.len(), alias, col_prefix
                );

                return items;
            }
        }
    }

    // STEP 2-AND-A-HALF (#510): when `alias` is the anchor of an OPTIONAL
    // denorm CTE + LEFT JOIN pattern (`__denorm_scan_{alias}`), its
    // properties come from that CTE — under their OWN Cypher property
    // names, never a raw db column or the LEFT-JOINed edge alias. STEP 3's
    // `get_properties_with_table_alias` resolves alias binding STRUCTURALLY
    // against the pre-render LogicalPlan tree, where this node is still
    // nested inside the OPTIONAL edge's GraphRel — correct for the ordinary
    // embedded-denorm case (#493/#475's target), but for this pattern it
    // returns the EDGE alias, which is NULL-extended on an OPTIONAL-miss
    // row. Checked first so it preempts STEP 3 for this pattern only.
    if let Some(exposed_props) = denorm_scan_cte_anchor_properties(plan, alias) {
        let items: Vec<SelectItem> = exposed_props
            .into_iter()
            .map(|(cypher_name, cte_col)| SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(alias.to_string()),
                    column: PropertyValue::Column(cte_col),
                }),
                col_alias: Some(ColumnAlias(cte_column_name(alias, &cypher_name))),
            })
            .collect();
        if !items.is_empty() {
            log::info!(
                "🔧 expand_table_alias_to_select_items: Using denorm-scan CTE properties for anchor alias '{}' (#510)",
                alias
            );
            return items;
        }
    }

    // STEP 3: Not a CTE reference - it's a fresh variable from current MATCH
    match plan.get_properties_with_table_alias(alias) {
        Ok((properties, actual_table_alias)) => {
            log::debug!("🔍🔍 expand_table_alias_to_select_items: alias='{}', got {} properties, actual_table_alias={:?}",
                       alias, properties.len(), actual_table_alias);

            if !properties.is_empty() {
                // Get ID column for aggregation handling
                let id_col = plan
                    .find_id_column_for_alias(alias)
                    .unwrap_or_else(|_| "id".to_string());

                // Get property requirements for pruning optimization (Dec 2025)
                let property_requirements =
                    plan_ctx.and_then(|ctx| ctx.get_property_requirements());

                // 🔧 FIX: For VLP queries with JOINs, use the Cypher alias (e.g., "u1") not VLP internal alias (e.g., "start_node")
                // When WITH clause has "WITH u1, u2", we want to SELECT from the JOIN aliases u1/u2,
                // not from the VLP CTE internal aliases start_node/end_node (which don't exist in FROM clause)
                //
                // IMPORTANT: actual_table_alias is None for VLP endpoints because they come from ViewScan
                // which doesn't track the internal VLP aliases. So we use the Cypher alias (u1/u2) instead.
                let table_alias_to_use = if let Some(ref table_alias) = actual_table_alias {
                    if table_alias == "start_node" || table_alias == "end_node" {
                        // VLP internal alias detected (shouldn't happen, but handle it)
                        log::debug!("🔧 expand_table_alias_to_select_items: VLP internal alias '{}' detected, using Cypher alias '{}' instead", table_alias, alias);
                        Some(alias.to_string())
                    } else {
                        log::info!(
                            "🔧 expand_table_alias_to_select_items: Using actual_table_alias '{}'",
                            table_alias
                        );
                        actual_table_alias.clone()
                    }
                } else {
                    // No table alias from plan - use the Cypher alias
                    // This is the common case for VLP endpoints where ViewScan returns None
                    log::debug!("🔧 expand_table_alias_to_select_items: No actual_table_alias, using Cypher alias '{}'", alias);
                    Some(alias.to_string())
                };

                // Use unified expansion helper with aggregation support (Dec 2025)
                use crate::render_plan::property_expansion::{
                    expand_alias_to_select_items_unified, PropertyAliasFormat,
                };
                let items = expand_alias_to_select_items_unified(
                    alias,
                    properties,
                    &id_col,
                    table_alias_to_use.clone(),
                    has_aggregation, // Enables anyLast() wrapping for non-ID columns
                    PropertyAliasFormat::Underscore,
                    property_requirements, // Enable property pruning if requirements available
                );

                log::info!(
                    "🔧 expand_table_alias_to_select_items: Found alias '{}' in base tables ({} properties), using table alias '{}'",
                    alias, items.len(), table_alias_to_use.as_deref().unwrap_or(alias)
                );

                return items;
            }
        }
        Err(e) => {
            log::debug!(
                "🔧 expand_table_alias_to_select_items: Error querying plan for alias '{}': {:?}",
                alias,
                e
            );
        }
    }

    // STEP 4: Fallback - use PlanCtx + schema to get properties
    // This handles cases where the alias exists in JOINs but its GraphNode isn't in the plan tree,
    // e.g., shared variables in comma-separated MATCH patterns:
    //   MATCH (a:Person)-[:IS_LOCATED_IN]->(:City), (a)-[:KNOWS]-(b:Person)
    //   WITH a, b  -- 'a' is in JOINs but its GraphNode has Empty input
    if let Some(ctx) = plan_ctx {
        if let Ok(table_ctx) = ctx.get_table_ctx(alias) {
            if let Some(label) = table_ctx.get_label_opt() {
                if let Ok(node_schema) = ctx.schema().node_schema(&label) {
                    let properties = extract_sorted_properties(&node_schema.property_mappings);
                    if !properties.is_empty() {
                        let id_col = node_schema
                            .node_id
                            .id
                            .columns()
                            .first()
                            .unwrap_or(&"id")
                            .to_string();
                        let property_requirements = ctx.get_property_requirements();

                        use crate::render_plan::property_expansion::{
                            expand_alias_to_select_items_unified, PropertyAliasFormat,
                        };
                        let items = expand_alias_to_select_items_unified(
                            alias,
                            properties,
                            &id_col,
                            Some(alias.to_string()),
                            has_aggregation,
                            PropertyAliasFormat::Underscore,
                            property_requirements,
                        );

                        log::info!(
                            "🔧 expand_table_alias_to_select_items: Found alias '{}' via schema fallback (label='{}', {} properties)",
                            alias, label, items.len()
                        );

                        return items;
                    }
                }
            }
        }
    }

    log::debug!(
        "🔧 expand_table_alias_to_select_items: Alias '{}' not found (not in CTE refs, not in base tables, not in schema)",
        alias
    );
    Vec::new()
}

/// #551: resolve the node LABEL for `alias` when it is a `GraphRel`
/// `left_connection`/`right_connection` with NO backing `GraphNode` anywhere
/// in the plan — the shape for a fully denormalized "virtual" node sitting
/// between two edges in an UNLABELED chain (e.g. `b` in
/// `(a)-[:T]->(b)-[:T]->(c)` with no explicit `:Label` on `b`: type
/// inference resolves its properties straight off the edge `ViewScan`'s
/// role-specific property maps, without ever synthesizing a `GraphNode` for
/// it — see `properties_builder.rs`'s `GraphRel` arm). `cte_extraction::
/// get_node_label_for_alias` only matches actual `GraphNode`s and returns
/// `None` for this shape.
///
/// Resolves via the relationship's OWN schema definition instead: finds the
/// `GraphRel` whose connection is `alias`, reads the relationship TYPE off
/// `GraphRel.labels`, and looks up that type's `from_node`/`to_node` label in
/// the schema catalog. `left_connection` is always the FROM side and
/// `right_connection` always the TO side —
/// `get_properties_with_table_alias` (`properties_builder.rs`) already
/// relies on the identical invariant for this exact shape (BidirectionalUnion
/// normalizes Incoming edges to preserve it).
pub(super) fn find_denorm_connection_node_label(
    plan: &LogicalPlan,
    alias: &str,
    schema: &GraphSchema,
) -> Option<String> {
    fn find_rel<'a>(plan: &'a LogicalPlan, alias: &str) -> Option<&'a GraphRel> {
        match plan {
            LogicalPlan::GraphRel(rel) => {
                if rel.left_connection == alias || rel.right_connection == alias {
                    return Some(rel);
                }
                find_rel(&rel.left, alias)
                    .or_else(|| find_rel(&rel.center, alias))
                    .or_else(|| find_rel(&rel.right, alias))
            }
            LogicalPlan::GraphNode(n) => find_rel(&n.input, alias),
            LogicalPlan::Filter(f) => find_rel(&f.input, alias),
            LogicalPlan::Projection(p) => find_rel(&p.input, alias),
            LogicalPlan::GraphJoins(g) => find_rel(&g.input, alias),
            LogicalPlan::OrderBy(o) => find_rel(&o.input, alias),
            LogicalPlan::Skip(s) => find_rel(&s.input, alias),
            LogicalPlan::Limit(l) => find_rel(&l.input, alias),
            LogicalPlan::GroupBy(gb) => find_rel(&gb.input, alias),
            LogicalPlan::Cte(c) => find_rel(&c.input, alias),
            LogicalPlan::Union(u) => u.inputs.iter().find_map(|i| find_rel(i, alias)),
            _ => None,
        }
    }

    let rel = find_rel(plan, alias)?;
    let rel_type_label = rel.labels.as_ref()?.first()?;
    // Labels may be a plain type ("CARRIER_FLIGHT") or a composite
    // "TYPE::FromNode::ToNode" form (see `get_relationship_type_for_alias`'s
    // doc comment) — only the leading TYPE component is needed here.
    let rel_type = rel_type_label.split("::").next().unwrap_or(rel_type_label);
    let is_left = rel.left_connection == alias;

    let candidate_labels: Vec<&String> = schema
        .get_all_rel_schemas_for_type(rel_type)
        .into_iter()
        .map(|rs| if is_left { &rs.from_node } else { &rs.to_node })
        .filter(|label| schema.node_schema_opt(label).is_some())
        .collect();

    // #562 (hardening, non-blocking finding from #551's adversarial review):
    // `get_all_rel_schemas_for_type` can legitimately return MULTIPLE
    // registrations of the same relationship TYPE against different
    // node-label pairs (e.g. one edge type registered for both
    // Message->Person and Post->Person, with differing id-property names
    // per pair). Blindly taking the FIRST candidate (the pre-#562 behavior)
    // would silently trust whichever registration happened to be inserted
    // first — sound only by coincidence. Compare the candidates' identity
    // shape (`node_id.columns()`, composite-safe — never calls the
    // single-column-only `.column()`, which panics on a composite id) and
    // refuse to guess if they disagree: return `None`, which makes every
    // caller fall through to its OWN pre-existing fallback unchanged (the
    // same "fail closed, not silently wrong" precedent as #544's multi-VLP
    // rejection). Single-registration and identical-shape multi-registration
    // types are completely unaffected — this only changes behavior for
    // genuinely ambiguous schemas, which no fixture in this repo currently
    // has (the analyzer's multi-type VLP routing, #538, already intercepts
    // real ambiguity upstream of this function today).
    let mut id_shapes: Vec<Vec<&str>> = candidate_labels
        .iter()
        .filter_map(|label| schema.node_schema_opt(label))
        .map(|ns| ns.node_id.columns())
        .collect();
    id_shapes.dedup();
    if id_shapes.len() > 1 {
        log::warn!(
            "⚠️ find_denorm_connection_node_label: relationship type '{}' resolves to {} candidate node label(s) with DIFFERING id-property shapes {:?} for alias '{}' — refusing to silently pick one (#562)",
            rel_type,
            candidate_labels.len(),
            id_shapes,
            alias
        );
        return None;
    }

    candidate_labels.first().map(|s| (*s).clone())
}

/// #551/#560/#561: resolve a single (non-composite) id property for `alias`
/// to its physical column and actual table alias, for a denormalized chain
/// node whose `GraphNode` either doesn't exist at all (fully unlabeled
/// connection-only node — see `find_denorm_connection_node_label`'s doc
/// comment) or exists but has no directly-attached `ViewScan`
/// `find_id_column_for_alias` can read (labeled, but denormalized: the
/// physical scan lives on the edge, not the node).
///
/// This is the single-id counterpart of `group_by_builder::
/// composite_id_group_by_columns`, and — per the same sharing principle —
/// is the ONE place this identity-resolution fallback lives. It is called
/// from both the WITH→CTE render path (`expand_table_alias_to_group_by_id_only`,
/// below — #551's original fix site) and the non-WITH implicit GROUP BY path
/// (`group_by_builder::handle_table_alias_group_by` /
/// `handle_wildcard_group_by`, #561's fix site), instead of each carrying its
/// own copy.
///
/// Resolves the label via the same two-step lookup #551 established — the
/// recursive real-`GraphNode` lookup (`cte_extraction::
/// get_node_label_for_alias`), falling back to `find_denorm_connection_node_label`
/// for the fully-unlabeled shape — then maps the label's `node_id` PROPERTY
/// name to its physical column via `get_properties_with_table_alias`.
/// Returns `None` (leaving the caller's own pre-existing fallback behavior
/// unchanged) for composite ids (handled separately by
/// `composite_id_group_by_columns`), VLP/CTE-backed aliases, or when any
/// resolution step fails.
pub(super) fn resolve_single_id_denorm_column(
    plan: &LogicalPlan,
    alias: &str,
    schema: &GraphSchema,
) -> Option<(String, String)> {
    let label = super::cte_extraction::get_node_label_for_alias(alias, plan)
        .or_else(|| find_denorm_connection_node_label(plan, alias, schema))?;
    let node_schema = schema.node_schema_opt(&label)?;
    if node_schema.node_id.is_composite() {
        return None;
    }
    let id_prop = node_schema.node_id.column();
    let (properties, actual_table_alias) = plan.get_properties_with_table_alias(alias).ok()?;
    let (_, col_name) = properties.iter().find(|(name, _)| name == id_prop)?;
    let table_alias_to_use = actual_table_alias.unwrap_or_else(|| alias.to_string());
    Some((col_name.clone(), table_alias_to_use))
}

/// #550/#560/#561: resolve composite-id GROUP BY columns for `alias` to
/// their PHYSICAL columns — mapping through `get_properties_with_table_alias`
/// for a denormalized composite-id node, where the raw `node_id.columns()`
/// property names (e.g. `code`, `state`) are NOT themselves physical column
/// names (e.g. `origin_code`, `origin_state`) — plus the actual table alias
/// to qualify them with (e.g. the edge alias, not the Cypher node alias).
///
/// Originally #550's fix, inline in `expand_table_alias_to_group_by_id_only`
/// (the WITH→CTE path) only. While fixing #561 (the non-WITH sibling of
/// #551/#560), the non-WITH path's `handle_table_alias_group_by`/
/// `handle_wildcard_group_by` (`group_by_builder.rs`) were found to call
/// `composite_id_group_by_columns` directly and push the RAW property names
/// straight into `GROUP BY` with no mapping step at all — a real,
/// independently-reachable bug for a LABELED denormalized composite-id node
/// grouped via its whole-node alias with no WITH clause (`RETURN b,
/// count(*)` over `AirportComposite`): pre-fix this rendered `GROUP BY
/// t0.code, t0.state`, a hard ClickHouse `UNKNOWN_IDENTIFIER` (neither
/// `code` nor `state` is a real column — `origin_code`/`origin_state` are).
/// Extracted here so BOTH paths share the identical mapping logic.
///
/// Returns `None` when `alias` is not a composite-id node (delegates the gate
/// to `composite_id_group_by_columns`). Property-mapping resolution failure
/// is non-fatal: falls back to the raw `node_id.columns()` names and `alias`
/// itself unchanged — reproducing #550's original, pre-denormalization-aware
/// behavior for a STANDARD composite-id node, where the raw property names
/// already ARE the physical column names.
pub(super) fn resolve_composite_id_group_by_columns(
    plan: &LogicalPlan,
    alias: &str,
) -> Option<(Vec<String>, String)> {
    let id_columns = super::group_by_builder::composite_id_group_by_columns(plan, alias)?;
    let (resolved_columns, group_alias) = match plan.get_properties_with_table_alias(alias) {
        Ok((props, actual_table_alias)) if !props.is_empty() => {
            let prop_map: HashMap<&str, &str> = props
                .iter()
                .map(|(name, col)| (name.as_str(), col.as_str()))
                .collect();
            (
                id_columns
                    .iter()
                    .map(|c| {
                        prop_map
                            .get(c.as_str())
                            .map_or(c.clone(), |m| m.to_string())
                    })
                    .collect(),
                actual_table_alias.unwrap_or_else(|| alias.to_string()),
            )
        }
        _ => (id_columns.clone(), alias.to_string()),
    };
    Some((resolved_columns, group_alias))
}

pub(crate) fn expand_table_alias_to_group_by_id_only(
    alias: &str,
    plan: &LogicalPlan,
    schema: &GraphSchema,
    cte_schemas: &crate::render_plan::CteSchemas,
    cte_references: &HashMap<String, String>,
    // Optional VLP CTE metadata for deterministic lookups (Phase 3 CTE integration)
    vlp_cte_metadata: Option<&HashMap<String, (String, Vec<super::CteColumnMetadata>)>>,
) -> Vec<RenderExpr> {
    log::info!(
        "🔧 expand_table_alias_to_group_by_id_only: Looking for ID column for alias '{}'",
        alias
    );

    // ZEROTH: Check if this is a VLP endpoint alias in a GraphRel with variable_length
    // For VLP queries, the FROM clause uses "vlp_xxx AS t", so we need to use "t" as the table alias
    // not the original Cypher alias (e.g., "u2")
    if let Some(graph_rel) = get_graph_rel_from_plan(plan) {
        if graph_rel.variable_length.is_some() {
            let is_start = alias == graph_rel.left_connection;
            let is_end = alias == graph_rel.right_connection;

            if is_start || is_end {
                // PHASE 3: Use VLP CTE metadata for deterministic lookup if available
                // Otherwise fall back to semantic defaults (start_id/end_id)
                if let Some(vlp_metadata) = vlp_cte_metadata {
                    // Find the VLP CTE for this pattern
                    let vlp_cte_prefix = format!(
                        "vlp_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );
                    for (cte_name, (from_alias, columns)) in vlp_metadata {
                        if cte_name.starts_with(&vlp_cte_prefix) || cte_name.starts_with("vlp_") {
                            // Look up the ID column for this alias from the metadata
                            if let Some(col_meta) = columns
                                .iter()
                                .find(|c| c.cypher_alias == alias && c.is_id_column)
                            {
                                log::info!(
                                    "🔧 expand_table_alias_to_group_by_id_only: Using VLP CTE metadata: '{}.{}' for alias '{}'",
                                    from_alias, col_meta.cte_column_name, alias
                                );
                                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(from_alias.clone()),
                                    column: PropertyValue::Column(col_meta.cte_column_name.clone()),
                                })];
                            }
                        }
                    }
                }

                // ⚠️ FALLBACK: CTE metadata lookup FAILED - using constants from join_context.rs
                // This indicates a gap in CTE metadata propagation. The deterministic path
                // via CteColumnMetadata should have found this alias.
                let vlp_alias = VLP_CTE_FROM_ALIAS;
                let id_column = if is_start {
                    VLP_START_ID_COLUMN
                } else {
                    VLP_END_ID_COLUMN
                };

                log::debug!(
                    "⚠️ expand_table_alias_to_group_by_id_only: METADATA MISSING for VLP endpoint '{}'. \
                    Falling back to conventions: '{}.{}'. \
                    This should not happen - investigate why CteColumnMetadata lookup failed. \
                    Graph pattern: ({})--[{:?}]-->({})",
                    alias, vlp_alias, id_column,
                    graph_rel.left_connection, graph_rel.labels, graph_rel.right_connection
                );
                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(vlp_alias.to_string()),
                    column: PropertyValue::Column(id_column.to_string()),
                })];
            }
        }
    }

    // FIRST: Check if this alias comes from a CTE (e.g., VLP CTE or UNION pseudo-CTE)
    if let Some(cte_name) = cte_references.get(alias) {
        log::info!(
            "🔧 expand_table_alias_to_group_by_id_only: Alias '{}' is from CTE '{}'",
            alias,
            cte_name
        );
        if let Some(meta) = cte_schemas.get(cte_name) {
            if let Some(id_col) = meta.alias_to_id.get(alias) {
                // Special case: __union_vlp is a pseudo-CTE representing UNION results
                // For UNION subqueries, GROUP BY needs to reference: __union."friend.id"
                // (table alias is __union, column name is "alias.id" with dots)
                if cte_name == "__union_vlp" {
                    // UNION subquery: use __union as table alias and "alias.id" as column
                    let dot_column_name = format!("{}.{}", alias, id_col);
                    log::debug!("🔧 expand_table_alias_to_group_by_id_only: UNION pattern - using __union.\"{}\"", dot_column_name);
                    return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("__union".to_string()),
                        column: PropertyValue::Column(dot_column_name),
                    })];
                }

                // Normal CTE: use FROM alias and id column
                let from_alias = extract_from_alias_from_cte_name(cte_name);
                log::debug!("🔧 expand_table_alias_to_group_by_id_only: Using ID column '{}' from CTE schema for alias '{}', FROM alias '{}'", id_col, alias, from_alias);
                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(from_alias.to_string()),
                    column: PropertyValue::Column(id_col.clone()),
                })];
            } else if meta.column_names.contains(&alias.to_string()) {
                // Fallback: CTE has a direct column matching alias (e.g. UNWIND scalar)
                let from_alias = extract_from_alias_from_cte_name(cte_name);
                log::info!("🔧 expand_table_alias_to_group_by_id_only: Using bare column '{}' from CTE '{}' for alias '{}', FROM alias '{}'", alias, cte_name, alias, from_alias);
                return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(from_alias.to_string()),
                    column: PropertyValue::Column(alias.to_string()),
                })];
            } else {
                log::warn!("⚠️ expand_table_alias_to_group_by_id_only: CTE '{}' does not have ID mapping for alias '{}'", cte_name, alias);
            }
        } else {
            log::debug!(
                "⚠️ expand_table_alias_to_group_by_id_only: CTE '{}' not found in schemas",
                cte_name
            );
        }
    }

    // SECOND-AND-A-HALF (#510): when `alias` is the anchor of an OPTIONAL
    // denorm CTE + LEFT JOIN pattern (`__denorm_scan_{alias}`,
    // #502/#505/#506/#507's shared machinery), the alias resolves through
    // that CTE's Cypher-property-named columns — NEVER the raw physical
    // `ViewScan.id_column` `find_id_column_for_alias` (THIRD, below) would
    // otherwise return. That CTE isn't registered in the general
    // `cte_schemas`/`cte_references` maps the FIRST check above reads, so
    // without this check the raw db column (e.g. `"id.orig_h"`) leaks into
    // GROUP BY against a CTE that only exposes `"ip"` — invalid SQL. Checked
    // before THIRD specifically to preempt it for this pattern; returns
    // `None` (falls through unaffected) for every other alias shape.
    if let Some(id_prop) = denorm_scan_cte_anchor_id_property(plan, alias) {
        log::debug!(
            "🔧 expand_table_alias_to_group_by_id_only: Using denorm-scan CTE id property '{}' for alias '{}' (#510)",
            id_prop, alias
        );
        return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(id_prop),
        })];
    }

    // SECOND (composite guard, issue #457): a composite-id node must contribute
    // EVERY identity column as a GROUP BY key. `find_id_column_for_alias` below
    // returns only the FIRST id column and would short-circuit before the
    // composite-aware label fallback further down, silently merging distinct
    // nodes that share the first component. Same expansion as the sites in
    // group_by_builder.rs (see the §1.4 triplication note on
    // `composite_id_group_by_columns`). #550's physical-column mapping (the
    // schema's `node_id.columns()` are CYPHER property names, not necessarily
    // physical columns for a denormalized composite-id node) now lives in the
    // shared `resolve_composite_id_group_by_columns` helper — see its doc
    // comment — so this path and the non-WITH `group_by_builder.rs` paths
    // cannot drift.
    if let Some((resolved_columns, group_alias)) =
        resolve_composite_id_group_by_columns(plan, alias)
    {
        log::debug!(
            "🔧 expand_table_alias_to_group_by_id_only: Using {} composite ID columns {:?} for alias '{}'",
            resolved_columns.len(),
            resolved_columns,
            alias
        );
        let mut result = Vec::new();
        super::group_by_builder::push_composite_id_group_by(
            &mut result,
            &group_alias,
            &resolved_columns,
        );
        return result;
    }

    // THIRD: Use find_id_column_for_alias which traverses the plan to find ViewScan.id_column
    // This is more reliable than find_label_for_alias because it directly gets the ID from the schema
    if let Ok(id_col) = plan.find_id_column_for_alias(alias) {
        log::debug!("🔧 expand_table_alias_to_group_by_id_only: Using ID column '{}' from ViewScan for alias '{}'", id_col, alias);
        return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(id_col),
        })];
    }

    // Fallback 1: Try to find label and look up in schema
    if let Some(label) = find_label_for_alias(plan, alias) {
        log::info!(
            "🔧 expand_table_alias_to_group_by_id_only: Found label '{}' for alias '{}'",
            label,
            alias
        );
        if let Some(node_schema) = schema.node_schema_opt(&label) {
            // Unified: columns() works for both single and composite IDs
            let cols = node_schema.node_id.columns();
            log::info!("🔧 expand_table_alias_to_group_by_id_only: Using node_id columns {:?} for alias '{}'",
                cols, alias);
            return cols
                .iter()
                .map(|col| {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(alias.to_string()),
                        column: PropertyValue::Column(col.to_string()),
                    })
                })
                .collect();
        } else {
            log::debug!(
                "⚠️ expand_table_alias_to_group_by_id_only: Label '{}' not found in schema",
                label
            );
        }
    } else {
        log::debug!(
            "⚠️ expand_table_alias_to_group_by_id_only: Could not find label for alias '{}'",
            alias
        );
    }

    // #551: before falling back to "first property" (Fallback 2 below — which
    // silently groups by whatever property happens to sort first in
    // `get_properties_with_table_alias`'s map, NOT necessarily the node's
    // identity), try to resolve the alias's REAL identity property through the
    // schema catalog first. This is the single-id counterpart to #550's
    // composite-id fix above (SECOND): a single-id DENORMALIZED node reached
    // deep inside a GraphRel chain (e.g. `b` in `(a)-[]->(b)-[]->(c)`, which
    // never gets its own ViewScan/GraphNode reachable by THIRD's
    // `find_id_column_for_alias`, and is never a DIRECT child of
    // Filter/Cte/Projection so Fallback 1's non-recursive `find_label_for_alias`
    // also misses it) falls through both THIRD and Fallback 1 untouched and
    // lands here with no identity resolved.
    //
    // First try the same RECURSIVE label lookup the composite guard already
    // relies on (`cte_extraction::get_node_label_for_alias`, which — unlike
    // `find_label_for_alias` — walks into `GraphRel.left/center/right`); this
    // covers a LABELED denorm chain node (`(b:AirportWithCarrier)`), which
    // gets a real `GraphNode` even though it has no ViewScan of its own.
    //
    // For an UNLABELED denorm chain node (`(b)` with no `:Label` — the
    // shape in #551's own repro), type inference resolves `b`'s properties
    // straight off the edge ViewScan's role-specific property maps (see
    // `properties_builder.rs`) WITHOUT ever synthesizing a `GraphNode` for
    // it, so `get_node_label_for_alias` misses it too.
    // `find_denorm_connection_node_label` below covers that shape:
    // it finds the `GraphRel` whose connection is `alias` and resolves the
    // node's label from the relationship's OWN schema definition
    // (`from_node`/`to_node`) instead of from a `GraphNode` that doesn't
    // exist.
    //
    // Either way, once a label is found — for a non-composite id only;
    // composite is already fully handled above — resolve its single id
    // PROPERTY name to the role-specific physical column and actual table
    // alias via `get_properties_with_table_alias` (same call Fallback 2
    // makes; the composite fix above uses it identically). If label or
    // property resolution fails at any point, fall through unchanged to
    // Fallback 2's pre-#551 behavior.
    //
    // #561: this label-then-property resolution is now the SHARED
    // `resolve_single_id_denorm_column` helper (see its doc comment) rather
    // than an inline copy — the non-WITH implicit GROUP BY path
    // (`group_by_builder::handle_table_alias_group_by`/
    // `handle_wildcard_group_by`) needs the exact same fallback for its own
    // "whole node `b` in GROUP BY, no WITH" shape, and duplicating it a
    // second time was the explicit thing to avoid (#551 review).
    if let Some((col_name, table_alias_to_use)) =
        resolve_single_id_denorm_column(plan, alias, schema)
    {
        log::debug!(
            "🔧 expand_table_alias_to_group_by_id_only: Resolved single id property -> physical column '{}' on alias '{}' (#551/#561)",
            col_name, table_alias_to_use
        );
        return vec![RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table_alias_to_use),
            column: PropertyValue::Column(col_name),
        })];
    }

    // Fallback 2: try to get properties and use first one (usually the ID)
    log::debug!(
        "⚠️ expand_table_alias_to_group_by_id_only: Using fallback for alias '{}'",
        alias
    );
    match plan.get_properties_with_table_alias(alias) {
        Ok((properties, actual_table_alias)) => {
            if !properties.is_empty() {
                let table_alias_to_use = actual_table_alias.unwrap_or_else(|| alias.to_string());
                // Just use the first property (typically the ID)
                let (_, col_name) = &properties[0];
                log::warn!("⚠️ expand_table_alias_to_group_by_id_only: Fallback using first property '{}' for alias '{}'", col_name, alias);
                vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(table_alias_to_use),
                    column: PropertyValue::Column(col_name.clone()),
                })]
            } else {
                Vec::new()
            }
        }
        Err(_) => {
            // Final fallback: assume this is a scalar alias from WITH clause
            // For scalars, use the alias as a column reference
            log::warn!("⚠️ expand_table_alias_to_group_by_id_only: Final fallback - treating '{}' as scalar column", alias);
            vec![RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(alias.to_string()),
                column: PropertyValue::Column(alias.to_string()),
            })]
        }
    }
}

/// Collect all "live" table aliases from the plan tree — aliases that appear in
/// GraphNode or GraphRel nodes that are NOT inside a ViewScan/CTE reference.
/// These are the aliases that actually need physical table joins.
pub(crate) fn collect_live_table_aliases(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    use std::collections::HashSet;
    fn collect(plan: &LogicalPlan, aliases: &mut HashSet<String>) {
        match plan {
            LogicalPlan::GraphNode(n) => {
                aliases.insert(n.alias.clone());
                collect(&n.input, aliases);
            }
            LogicalPlan::GraphRel(r) => {
                if !r.alias.is_empty() {
                    aliases.insert(r.alias.clone());
                }
                collect(&r.left, aliases);
                collect(&r.center, aliases);
                collect(&r.right, aliases);
            }
            LogicalPlan::GraphJoins(gj) => collect(&gj.input, aliases),
            LogicalPlan::Projection(p) => collect(&p.input, aliases),
            LogicalPlan::Filter(f) => collect(&f.input, aliases),
            LogicalPlan::OrderBy(o) => collect(&o.input, aliases),
            LogicalPlan::Limit(l) => collect(&l.input, aliases),
            LogicalPlan::GroupBy(g) => collect(&g.input, aliases),
            LogicalPlan::Skip(s) => collect(&s.input, aliases),
            LogicalPlan::Unwind(u) => collect(&u.input, aliases),
            LogicalPlan::CartesianProduct(cp) => {
                collect(&cp.left, aliases);
                collect(&cp.right, aliases);
            }
            LogicalPlan::WithClause(wc) => collect(&wc.input, aliases),
            LogicalPlan::Union(u) => {
                for input in &u.inputs {
                    collect(input, aliases);
                }
            }
            // ViewScan = CTE reference, NOT a physical table — don't collect
            LogicalPlan::ViewScan(_) => {}
            _ => {}
        }
    }
    let mut aliases = HashSet::new();
    collect(plan, &mut aliases);
    aliases
}

/// #596: Collect the node/relationship aliases bound in the current subplan's
/// EXISTS-correlation scope — like [`collect_live_table_aliases`] but treating a
/// `Union` as a HARD SCOPE BOUNDARY (does NOT descend into its arms).
///
/// UNION arms are independent sibling subplans that each render under their own
/// `CteScopeGenerationGuard` and merge THEIR OWN aliases as they render; pulling
/// every arm's aliases up to the union level would make one arm's fresh inner
/// EXISTS variable look like an outer anchor in a sibling arm that reuses the
/// same name (the #596 cross-arm leak — Code 47). `CartesianProduct` sides, by
/// contrast, form ONE combined scope (`MATCH (a), (b)`), so we still descend
/// into both. Used only to populate `exists_outer_aliases` at render entry.
pub(crate) fn collect_exists_scope_aliases(
    plan: &LogicalPlan,
) -> std::collections::HashSet<String> {
    use std::collections::HashSet;
    fn collect(plan: &LogicalPlan, aliases: &mut HashSet<String>) {
        match plan {
            LogicalPlan::GraphNode(n) => {
                aliases.insert(n.alias.clone());
                collect(&n.input, aliases);
            }
            LogicalPlan::GraphRel(r) => {
                if !r.alias.is_empty() {
                    aliases.insert(r.alias.clone());
                }
                collect(&r.left, aliases);
                collect(&r.center, aliases);
                collect(&r.right, aliases);
            }
            LogicalPlan::GraphJoins(gj) => collect(&gj.input, aliases),
            LogicalPlan::Projection(p) => collect(&p.input, aliases),
            LogicalPlan::Filter(f) => collect(&f.input, aliases),
            LogicalPlan::OrderBy(o) => collect(&o.input, aliases),
            LogicalPlan::Limit(l) => collect(&l.input, aliases),
            LogicalPlan::GroupBy(g) => collect(&g.input, aliases),
            LogicalPlan::Skip(s) => collect(&s.input, aliases),
            LogicalPlan::Unwind(u) => collect(&u.input, aliases),
            LogicalPlan::CartesianProduct(cp) => {
                // Same combined scope — both sides are in scope together.
                collect(&cp.left, aliases);
                collect(&cp.right, aliases);
            }
            LogicalPlan::WithClause(wc) => collect(&wc.input, aliases),
            // Union = independent sibling subplans; each arm merges its own
            // aliases under its own generation guard. Do NOT descend (#596).
            LogicalPlan::Union(_) => {}
            LogicalPlan::ViewScan(_) => {}
            _ => {}
        }
    }
    let mut aliases = HashSet::new();
    collect(plan, &mut aliases);
    aliases
}

/// Remove pre-computed joins from GraphJoins that are stale after WITH→CTE replacement.
/// A join is stale if:
/// 1. Its table_alias is a CTE-scoped variable
/// 2. Its join conditions reference a CTE-scoped alias
/// 3. Its join conditions reference an alias not in the "live" set
///    (i.e., the alias no longer exists as a physical node in the plan tree)
pub(crate) fn clear_stale_joins_for_cte_aliases(
    plan: &LogicalPlan,
    cte_aliases: &std::collections::HashSet<&str>,
) -> LogicalPlan {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    // Collect all live table aliases from the plan tree
    let live_aliases = collect_live_table_aliases(plan);

    fn clear_recursive(
        plan: &LogicalPlan,
        cte_aliases: &std::collections::HashSet<&str>,
        live_aliases: &std::collections::HashSet<String>,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::GraphJoins(gj) => {
                let new_input = clear_recursive(&gj.input, cte_aliases, live_aliases);

                let cleaned_joins: Vec<Join> = gj
                    .joins
                    .iter()
                    .filter(|j| {
                        // Check if this join's alias is CTE-scoped
                        let alias_is_stale = cte_aliases.contains(j.table_alias.as_str());
                        // Check if any join condition references a CTE-scoped alias
                        let condition_refs_cte = j.joining_on.iter().any(|op| {
                            op.operands.iter().any(|operand| {
                                if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) = operand {
                                    cte_aliases.contains(pa.table_alias.0.as_str())
                                } else {
                                    false
                                }
                            })
                        });
                        // Check if any join condition references an alias no longer in the plan tree
                        let condition_refs_dead = j.joining_on.iter().any(|op| {
                            op.operands.iter().any(|operand| {
                                if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(pa) = operand {
                                    let alias = &pa.table_alias.0;
                                    // Skip VLP/CTE aliases (they're valid render-time
                                    // references not present in the logical plan tree)
                                    !crate::query_planner::join_context::is_vlp_or_cte_alias(alias)
                                        && !live_aliases.contains(alias.as_str())
                                } else {
                                    false
                                }
                            })
                        });
                        if alias_is_stale || condition_refs_cte || condition_refs_dead {
                            log::debug!(
                                "🔧 clear_stale_joins: Removing stale join for '{}' (alias_stale={}, cond_refs_cte={}, cond_refs_dead={})",
                                j.table_alias, alias_is_stale, condition_refs_cte, condition_refs_dead
                            );
                            false
                        } else {
                            true
                        }
                    })
                    .cloned()
                    .collect();

                LogicalPlan::GraphJoins(GraphJoins {
                    input: Arc::new(new_input),
                    joins: cleaned_joins,
                    optional_aliases: gj.optional_aliases.clone(),
                    anchor_table: gj.anchor_table.clone(),
                    cte_references: gj.cte_references.clone(),
                    correlation_predicates: gj.correlation_predicates.clone(),
                })
            }
            LogicalPlan::Projection(p) => LogicalPlan::Projection(Projection {
                input: Arc::new(clear_recursive(&p.input, cte_aliases, live_aliases)),
                items: p.items.clone(),
                distinct: p.distinct,
                pattern_comprehensions: p.pattern_comprehensions.clone(),
            }),
            LogicalPlan::Filter(f) => LogicalPlan::Filter(Filter {
                input: Arc::new(clear_recursive(&f.input, cte_aliases, live_aliases)),
                predicate: f.predicate.clone(),
            }),
            LogicalPlan::OrderBy(o) => LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(clear_recursive(&o.input, cte_aliases, live_aliases)),
                items: o.items.clone(),
            }),
            LogicalPlan::Limit(l) => LogicalPlan::Limit(Limit {
                input: Arc::new(clear_recursive(&l.input, cte_aliases, live_aliases)),
                count: l.count,
            }),
            LogicalPlan::GroupBy(g) => LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(clear_recursive(&g.input, cte_aliases, live_aliases)),
                expressions: g.expressions.clone(),
                having_clause: g.having_clause.clone(),
                is_materialization_boundary: g.is_materialization_boundary,
                exposed_alias: g.exposed_alias.clone(),
            }),
            LogicalPlan::Skip(s) => LogicalPlan::Skip(Skip {
                input: Arc::new(clear_recursive(&s.input, cte_aliases, live_aliases)),
                count: s.count,
            }),
            LogicalPlan::Unwind(u) => LogicalPlan::Unwind(Unwind {
                input: Arc::new(clear_recursive(&u.input, cte_aliases, live_aliases)),
                expression: u.expression.clone(),
                alias: u.alias.clone(),
                label: u.label.clone(),
                tuple_properties: u.tuple_properties.clone(),
            }),
            // Leaf/other nodes: no joins to clear
            other => other.clone(),
        }
    }

    clear_recursive(plan, cte_aliases, &live_aliases)
}

/// Extract FROM alias from CTE name by stripping "with_" prefix and "_cte[_<digits>]" suffix
///
/// Examples:
/// - "with_a_follows_cte" → "a_follows"
/// - "with_a_follows_cte_1" → "a_follows"
/// - "with_a_follows_cte_999" → "a_follows"
/// - "a_follows" → "a_follows" (no prefix/suffix to strip)
pub(crate) fn extract_from_alias_from_cte_name(cte_name: &str) -> &str {
    // Strip optional "with_" prefix
    let base = cte_name.strip_prefix("with_").unwrap_or(cte_name);

    // Handle unnumbered suffix "_cte"
    if let Some(stripped) = base.strip_suffix("_cte") {
        return stripped;
    }

    // Handle numbered suffixes like "_cte_1", "_cte_2", ..., "_cte_<digits>"
    if let Some(pos) = base.rfind("_cte_") {
        let suffix = &base[pos + "_cte_".len()..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            return &base[..pos];
        }
    }

    base
}

/// Run the two outer-plan resolution passes (`rewrite_bare_variables_in_plan`
/// then `fix_orphan_table_aliases`) on `plan` using a scope filtered to only
/// the CTEs `plan`'s own FROM/JOINs reference (#593).
///
/// Used to process each arm of a Cypher UNION independently: the shared
/// `final_scope` carries every arm's WITH-CTE variables, so filtering to this
/// arm's own referenced CTEs prevents a WITH arm's CTE from leaking into a
/// sibling arm that merely reuses the same Cypher alias name.
pub(crate) fn apply_outer_scope_passes(
    plan: &mut super::RenderPlan,
    full_scope: &super::variable_scope::VariableScope,
) {
    let mut referenced: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(f) = &plan.from.0 {
        referenced.insert(f.name.clone());
    }
    for j in &plan.joins.0 {
        referenced.insert(j.table_name.clone());
    }
    let scoped = full_scope.scoped_to_referenced_ctes(&referenced);
    super::variable_scope::rewrite_bare_variables_in_plan(plan, &scoped);
    super::variable_scope::fix_orphan_table_aliases(plan, &scoped);
}

/// Detect `head(collect({key1: val1, key2: val2, ...})) AS alias` patterns where
/// any MapLiteral value is a bare node reference (TableAlias). ClickHouse map() requires
/// homogeneous value types, but nodes have no single SQL value. Flatten each map entry
/// into separate CTE columns:
///   - Node values → one column per schema property: `arrayElement(groupArray(prop), 1) AS alias_key_prop`
///   - Scalar values → single column: `arrayElement(groupArray(expr), 1) AS alias_key`
///
/// Returns (flattened_items, compound_key_mappings) where compound_key_mappings contains
/// entries like ("msg.id", "latestLike_msg_id") for downstream property_mapping injection.
/// (flattened_items, compound_key_mappings) — compound_key_mappings entries like
/// ("msg.id", "latestLike_msg_id") feed downstream property_mapping injection.
type FlattenedMapLiteralResult = (Vec<SelectItem>, Vec<(String, String)>);

pub(crate) fn try_flatten_head_collect_map_literal(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    col_alias: Option<&str>,
    plan: &LogicalPlan,
    plan_ctx: Option<&PlanCtx>,
    scope: Option<&super::variable_scope::VariableScope>,
) -> Option<FlattenedMapLiteralResult> {
    use crate::query_planner::logical_expr::LogicalExpr;

    let alias = col_alias?;

    log::info!(
        "🔧 try_flatten_head_collect_map_literal: checking alias='{}', expr type={:?}",
        alias,
        std::mem::discriminant(expr)
    );

    // Match: ScalarFnCall("head", [AggregateFnCall("collect", [MapLiteral(entries)])])
    let entries = match expr {
        LogicalExpr::ScalarFnCall(sf)
            if sf.name.eq_ignore_ascii_case("head") && sf.args.len() == 1 =>
        {
            match &sf.args[0] {
                LogicalExpr::AggregateFnCall(agg)
                    if agg.name.eq_ignore_ascii_case("collect") && agg.args.len() == 1 =>
                {
                    match &agg.args[0] {
                        LogicalExpr::MapLiteral(entries) => entries,
                        _ => return None,
                    }
                }
                _ => return None,
            }
        }
        _ => return None,
    };

    // Check if any value is a node reference (TableAlias with multiple properties).
    // Bare variables like "likeTime" are also TableAlias but are scalars — empty/no property mapping.
    // Try multiple detection methods: plan_ctx labels, scope CTE variables, plan tree, schema.
    let is_node_alias = |alias_name: &str| -> bool {
        // Method 1: Check plan_ctx for node labels
        if let Some(ctx) = plan_ctx {
            if let Ok(tc) = ctx.get_table_ctx(alias_name) {
                if tc.get_label_opt().is_some() {
                    return true;
                }
            }
        }
        // Method 2: Check scope CTE variables for multi-property mapping
        if let Some(s) = scope {
            if let Some(cte_info) = s.cte_variables().get(alias_name) {
                log::info!(
                    "🔧 is_node_alias('{}') Method 2: property_mapping.len()={}, labels={:?}",
                    alias_name,
                    cte_info.property_mapping.len(),
                    cte_info.labels
                );
                // Node aliases have multiple properties (id, name, etc.)
                // Scalar aliases have 0 or 1 properties
                if cte_info.property_mapping.len() > 1 {
                    return true;
                }
                // Also check if there are labels
                if !cte_info.labels.is_empty() {
                    return true;
                }
            }
        }
        // Method 3: Check plan tree for GraphNode with this alias
        if let Some(label) =
            crate::render_plan::cte_extraction::get_node_label_for_alias(alias_name, plan)
        {
            if !label.is_empty() {
                return true;
            }
        }
        false
    };

    let has_node_value = entries.iter().any(|(_, v)| {
        if let LogicalExpr::TableAlias(ta) = v {
            is_node_alias(&ta.0)
        } else {
            false
        }
    });
    if !has_node_value {
        return None; // All scalar — keep using map() (preserves bi-14)
    }

    log::info!(
        "🔧 try_flatten_head_collect_map_literal: Flattening {} entries for alias '{}'",
        entries.len(),
        alias
    );

    let schema = crate::server::query_context::get_current_schema();
    let schema_ref = schema.as_deref();

    let mut flattened_items: Vec<SelectItem> = Vec::new();
    // Collect compound key mappings: ("map_key.property", "alias_mapkey_property")
    // These are stored at generation time to avoid ambiguous reverse-engineering from column names.
    let mut compound_keys: Vec<(String, String)> = Vec::new();

    for (key, value) in entries {
        // Determine if this value is a node alias by trying to get its properties.
        // If we find >1 property, it's a node; otherwise treat as scalar.
        let node_properties: Option<Vec<(String, String)>> = if let LogicalExpr::TableAlias(ta) =
            value
        {
            // Try scope CTE variables first (after WITH barriers)
            let from_scope = scope.and_then(|s| {
                s.cte_variables().get(&ta.0).and_then(|cte_info| {
                    if cte_info.property_mapping.len() > 1 {
                        // Sorted: this list drives generated SELECT-item order and
                        // `property_mapping` is a HashMap whose iteration order is
                        // per-process random (#480 class).
                        let mut props: Vec<(String, String)> = cte_info
                            .property_mapping
                            .keys()
                            .map(|prop| (prop.clone(), prop.clone()))
                            .collect();
                        props.sort_by(|a, b| a.0.cmp(&b.0));
                        Some(props)
                    } else {
                        None
                    }
                })
            });
            if from_scope.is_some() {
                from_scope
            } else {
                // Try schema lookup via plan_ctx
                let label = plan_ctx
                    .and_then(|ctx| {
                        ctx.get_table_ctx(&ta.0)
                            .ok()
                            .and_then(|tc| tc.get_label_opt())
                    })
                    .or_else(|| {
                        crate::render_plan::cte_extraction::get_node_label_for_alias(&ta.0, plan)
                    });
                label
                    .and_then(|l| schema_ref.map(|s| s.get_node_properties(&[l])))
                    .filter(|props| props.len() > 1)
            }
        } else {
            None
        };

        let is_node = node_properties.is_some();

        if is_node {
            let node_alias = match value {
                LogicalExpr::TableAlias(ta) => ta,
                _ => unreachable!(),
            };

            let properties = node_properties.unwrap();

            for (cypher_prop, db_col) in &properties {
                let mut prop_access = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(node_alias.0.clone()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                        db_col.clone(),
                    ),
                });
                // Apply scope rewriting for CTE-backed references
                if let Some(s) = scope {
                    prop_access = super::variable_scope::rewrite_render_expr(&prop_access, s);
                }
                let mapper = current_function_mapper();
                let group_array = RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: mapper.collect_list().to_string(),
                    args: vec![prop_access],
                });
                let head_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: mapper.array_element().to_string(),
                    args: vec![group_array, RenderExpr::Literal(Literal::Integer(1))],
                });

                let col_name = format!("{}_{}", key, cypher_prop);
                let full_alias = format!("{}_{}", alias, col_name);
                // Record compound key: "msg.id" → "latestLike_msg_id"
                let compound_key = format!("{}.{}", key, cypher_prop);
                compound_keys.push((compound_key, full_alias.clone()));
                flattened_items.push(SelectItem {
                    expression: head_expr,
                    col_alias: Some(ColumnAlias(full_alias)),
                });
            }
        } else {
            // Scalar value — convert to RenderExpr and wrap in arrayElement(groupArray(...), 1)
            let render_value: Option<RenderExpr> = value.clone().try_into().ok();
            if let Some(mut val_expr) = render_value {
                // Rewrite CTE-backed references
                if let Some(s) = scope {
                    val_expr = super::variable_scope::rewrite_render_expr(&val_expr, s);
                }
                let mapper = current_function_mapper();
                let group_array = RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: mapper.collect_list().to_string(),
                    args: vec![val_expr],
                });
                let head_expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: mapper.array_element().to_string(),
                    args: vec![group_array, RenderExpr::Literal(Literal::Integer(1))],
                });

                let full_alias = format!("{}_{}", alias, key);
                flattened_items.push(SelectItem {
                    expression: head_expr,
                    col_alias: Some(ColumnAlias(full_alias)),
                });
            }
        }
    }

    if flattened_items.is_empty() {
        return None;
    }

    Some((flattened_items, compound_keys))
}

/// Find the first `GraphRel` reachable through the common single-branch wrappers
/// (`GraphJoins`/`Projection`/`Filter`/…). Used by the #460/#462 post-WITH
/// OPTIONAL restructure to recover the optional pattern's WHERE predicate and
/// relationship alias, otherwise dropped for the reversed-anchor shape.
pub(crate) fn find_graphrel(
    plan: &LogicalPlan,
) -> Option<&crate::query_planner::logical_plan::GraphRel> {
    match plan {
        LogicalPlan::GraphRel(gr) => Some(gr),
        LogicalPlan::GraphJoins(gj) => find_graphrel(&gj.input),
        LogicalPlan::Projection(proj) => find_graphrel(&proj.input),
        LogicalPlan::Filter(f) => find_graphrel(&f.input),
        LogicalPlan::GroupBy(g) => find_graphrel(&g.input),
        LogicalPlan::OrderBy(o) => find_graphrel(&o.input),
        LogicalPlan::Limit(l) => find_graphrel(&l.input),
        LogicalPlan::Skip(s) => find_graphrel(&s.input),
        LogicalPlan::GraphNode(gn) => find_graphrel(&gn.input),
        LogicalPlan::Unwind(u) => find_graphrel(&u.input),
        _ => None,
    }
}

/// Find the `where_predicate` of the first `GraphRel` reachable through the
/// common single-branch wrappers. Used by the #460 post-WITH OPTIONAL
/// restructure to recover the optional pattern's WHERE predicate.
pub(crate) fn find_graphrel_where_predicate(plan: &LogicalPlan) -> Option<&Option<LogicalExpr>> {
    find_graphrel(plan).map(|gr| &gr.where_predicate)
}

/// Flatten a (possibly nested) `AND` chain of a rendered predicate into its
/// individual conjuncts. Non-`AND` expressions (including `OR`) return as a
/// single element — an unsplittable OR must stay whole. Used by the #462 GAP 1
/// move of cross-alias WHERE conjuncts into a LEFT JOIN ON condition.
pub(crate) fn split_render_and_conjuncts(expr: RenderExpr) -> Vec<RenderExpr> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::And => op
            .operands
            .into_iter()
            .flat_map(split_render_and_conjuncts)
            .collect(),
        other => vec![other],
    }
}

pub(crate) fn show_plan_structure(plan: &LogicalPlan, indent: usize) {
    let prefix = "  ".repeat(indent);
    match plan {
        LogicalPlan::WithClause(wc) => {
            let key = if !wc.exported_aliases.is_empty() {
                let mut aliases = wc.exported_aliases.clone();
                aliases.sort();
                aliases.join("_")
            } else {
                "?".to_string()
            };
            log::debug!("{}WITH[{}]", prefix, key,);
            show_plan_structure(&wc.input, indent + 1);
        }
        LogicalPlan::Projection(proj) => {
            log::debug!("{}Proj({})", prefix, proj.items.len());
            show_plan_structure(&proj.input, indent + 1);
        }
        LogicalPlan::GraphJoins(gj) => {
            log::debug!("{}GJoins({})", prefix, gj.joins.len());
            show_plan_structure(&gj.input, indent + 1);
        }
        LogicalPlan::Filter(f) => {
            log::debug!("{}Filter", prefix);
            show_plan_structure(&f.input, indent + 1);
        }
        LogicalPlan::Limit(l) => {
            log::debug!("{}Limit({})", prefix, l.count);
            show_plan_structure(&l.input, indent + 1);
        }
        LogicalPlan::ViewScan(vs) => {
            log::debug!("{}VS('{}')", prefix, vs.source_table);
        }
        LogicalPlan::GraphNode(gn) => {
            log::debug!("{}GN('{}')", prefix, gn.alias);
        }
        LogicalPlan::Union(u) => {
            log::debug!("{}Union({}br)", prefix, u.inputs.len());
            for (i, input) in u.inputs.iter().enumerate() {
                log::debug!("{}  br{}:", prefix, i);
                show_plan_structure(input, indent + 2);
            }
        }
        LogicalPlan::GraphRel(gr) => {
            log::debug!(
                "{}GR({}->{}, {:?})",
                prefix,
                gr.left_connection,
                gr.right_connection,
                gr.labels.as_ref().map(|l| l.join(",")).unwrap_or_default()
            );
            show_plan_structure(&gr.left, indent + 1);
            show_plan_structure(&gr.right, indent + 1);
        }
        LogicalPlan::CartesianProduct(cp) => {
            log::debug!("{}CP", prefix);
            show_plan_structure(&cp.left, indent + 1);
            show_plan_structure(&cp.right, indent + 1);
        }
        LogicalPlan::GroupBy(_) => {
            log::debug!("{}GroupBy", prefix);
        }
        LogicalPlan::OrderBy(ob) => {
            log::debug!("{}OrderBy", prefix);
            show_plan_structure(&ob.input, indent + 1);
        }
        LogicalPlan::Skip(s) => {
            log::debug!("{}Skip({})", prefix, s.count);
            show_plan_structure(&s.input, indent + 1);
        }
        LogicalPlan::Unwind(u) => {
            log::debug!("{}Unwind('{}')", prefix, u.alias);
            show_plan_structure(&u.input, indent + 1);
        }
        other => {
            log::debug!("{}{:?}", prefix, std::mem::discriminant(other));
        }
    }
}

// Count plan tree depth to diagnose excessive iterations.
// Deep nesting can come from any combination of plan nodes (Projection, Filter, WITH, etc.)
pub(crate) fn count_plan_depth(plan: &LogicalPlan) -> usize {
    count_plan_depth_impl(plan, 0)
}

fn count_plan_depth_impl(plan: &LogicalPlan, current: usize) -> usize {
    if current > crate::render_plan::MAX_TRAVERSAL_DEPTH {
        return current;
    }
    match plan {
        LogicalPlan::WithClause(wc) => count_plan_depth_impl(&wc.input, current + 1),
        LogicalPlan::Projection(p) => count_plan_depth_impl(&p.input, current + 1),
        LogicalPlan::Filter(f) => count_plan_depth_impl(&f.input, current + 1),
        LogicalPlan::GroupBy(gb) => count_plan_depth_impl(&gb.input, current + 1),
        LogicalPlan::OrderBy(ob) => count_plan_depth_impl(&ob.input, current + 1),
        LogicalPlan::Limit(lim) => count_plan_depth_impl(&lim.input, current + 1),
        LogicalPlan::Skip(skip) => count_plan_depth_impl(&skip.input, current + 1),
        LogicalPlan::GraphJoins(gj) => count_plan_depth_impl(&gj.input, current + 1),
        LogicalPlan::Unwind(u) => count_plan_depth_impl(&u.input, current + 1),
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .map(|i| count_plan_depth_impl(i, current + 1))
            .max()
            .unwrap_or(current + 1),
        _ => current + 1, // Leaf nodes
    }
}

pub(crate) fn show_with_structure(plan: &LogicalPlan, indent: usize) {
    let prefix = "  ".repeat(indent);
    match plan {
        LogicalPlan::WithClause(wc) => {
            let key = if !wc.exported_aliases.is_empty() {
                let mut aliases = wc.exported_aliases.clone();
                aliases.sort();
                aliases.join("_")
            } else {
                "with_var".to_string()
            };
            log::debug!(
                "{}WithClause(key='{}', cte_refs={:?})",
                prefix,
                key,
                wc.cte_references
            );
            show_with_structure(&wc.input, indent + 1);
        }
        LogicalPlan::Limit(lim) => {
            log::debug!("{}Limit({})", prefix, lim.count);
            show_with_structure(&lim.input, indent + 1);
        }
        LogicalPlan::GraphJoins(gj) => {
            log::debug!("{}GraphJoins({} joins)", prefix, gj.joins.len());
            show_with_structure(&gj.input, indent + 1);
        }
        LogicalPlan::Projection(proj) => {
            log::debug!("{}Projection({} items)", prefix, proj.items.len());
            show_with_structure(&proj.input, indent + 1);
        }
        LogicalPlan::GraphNode(gn) => {
            log::debug!("{}GraphNode(alias='{}')", prefix, gn.alias);
        }
        LogicalPlan::ViewScan(vs) => {
            log::debug!("{}ViewScan(table='{}')", prefix, vs.source_table);
        }
        LogicalPlan::Union(u) => {
            log::debug!("{}Union({} branches)", prefix, u.inputs.len());
            for (i, input) in u.inputs.iter().enumerate() {
                log::debug!("{}  Branch {}:", prefix, i);
                show_with_structure(input, indent + 2);
            }
        }
        LogicalPlan::GraphRel(gr) => {
            log::debug!(
                "{}GraphRel(l='{}', r='{}', dir={:?})",
                prefix,
                gr.left_connection,
                gr.right_connection,
                gr.labels.as_ref().map(|l| l.join(",")).unwrap_or_default()
            );
            log::debug!("{}  left:", prefix);
            show_with_structure(&gr.left, indent + 2);
            log::debug!("{}  right:", prefix);
            show_with_structure(&gr.right, indent + 2);
        }
        LogicalPlan::Filter(f) => {
            log::debug!("{}Filter", prefix);
            show_with_structure(&f.input, indent + 1);
        }
        LogicalPlan::CartesianProduct(cp) => {
            log::debug!("{}CartesianProduct", prefix);
            log::debug!("{}  left:", prefix);
            show_with_structure(&cp.left, indent + 2);
            log::debug!("{}  right:", prefix);
            show_with_structure(&cp.right, indent + 2);
        }
        LogicalPlan::GroupBy(gb) => {
            log::debug!("{}GroupBy", prefix);
            show_with_structure(&gb.input, indent + 1);
        }
        LogicalPlan::OrderBy(ob) => {
            log::debug!("{}OrderBy", prefix);
            show_with_structure(&ob.input, indent + 1);
        }
        LogicalPlan::Skip(s) => {
            log::debug!("{}Skip({})", prefix, s.count);
            show_with_structure(&s.input, indent + 1);
        }
        LogicalPlan::Unwind(u) => {
            log::debug!("{}Unwind(alias='{}')", prefix, u.alias);
            show_with_structure(&u.input, indent + 1);
        }
        other => {
            log::debug!("{}Other({:?})", prefix, std::mem::discriminant(other));
        }
    }
}

pub(crate) fn collect_analyzer_cte_names(
    plan: &LogicalPlan,
    names: &mut std::collections::HashSet<String>,
) {
    match plan {
        LogicalPlan::WithClause(wc) => {
            for cte_name in wc.cte_references.values() {
                names.insert(cte_name.clone());
            }
            collect_analyzer_cte_names(&wc.input, names);
        }
        LogicalPlan::Projection(proj) => collect_analyzer_cte_names(&proj.input, names),
        LogicalPlan::Filter(f) => collect_analyzer_cte_names(&f.input, names),
        LogicalPlan::GroupBy(gb) => collect_analyzer_cte_names(&gb.input, names),
        LogicalPlan::OrderBy(ob) => collect_analyzer_cte_names(&ob.input, names),
        LogicalPlan::Limit(lim) => collect_analyzer_cte_names(&lim.input, names),
        LogicalPlan::Skip(skip) => collect_analyzer_cte_names(&skip.input, names),
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                collect_analyzer_cte_names(input, names);
            }
        }
        _ => {}
    }
}

/// Check if a plan is a CTE reference (ViewScan or GraphNode wrapping ViewScan with table starting with "with_")
pub(crate) fn is_cte_reference(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(vs) if vs.source_table.starts_with("with_") => {
            Some(vs.source_table.clone())
        }
        LogicalPlan::GraphNode(gn) => {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                if vs.source_table.starts_with("with_") {
                    return Some(vs.source_table.clone());
                }
            }
            None
        }
        _ => None,
    }
}

/// `from_alias` is the alias the enclosing CTE's FROM binds (when known). It
/// gates the #584 coupled-rel-var remap: the rewrite `r.<col>` → node alias `o`
/// is only valid when the FROM actually binds `o`. When the FROM binds the rel
/// var itself (e.g. `WITH count(r)` / `WITH r.order_id` where only `r` is
/// carried past the barrier), `r.<col>` is already a valid reference to the
/// coupled row and must be left untouched — remapping it to `o` would unbind it.
pub(crate) fn resolve_denormalized_property_in_expr_impl(
    expr: &mut RenderExpr,
    plan: &LogicalPlan,
    from_alias: Option<&str>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // #584: an FK-edge / coupled relationship variable shares one physical
            // row with its node (edge table == node table). Under a WITH aggregate
            // the CTE's FROM binds only the NODE alias (`o`) and the rel var `r`
            // is never emitted as a separate JOIN alias, so `count(r)` →
            // `count(r.<edge_id>)` dangles (Code 47). Resolve `r` to the coupled
            // endpoint alias whose physical table IS the edge table
            // (`coupled_edge_render_alias`). Returns None for a traditional
            // separate edge table (standard schema binds `r` as its own JOIN
            // alias), so this is FK-edge/coupled-scoped. Runs before the
            // denormalized-NODE property resolution below, which only handles node
            // aliases (`get_properties_with_table_alias` returns None for a rel
            // var), leaving `r` untouched today.
            //
            // Gate: only remap when the coupled NODE alias is what the CTE's FROM
            // binds. When the FROM binds the rel var itself (only `r` carried past
            // the WITH barrier), `r.<col>` is already valid and remapping to `o`
            // would unbind it (regressing `WITH count(r)` / `WITH r.order_id`).
            if let Some(gr) = plan.find_graph_rel_by_rel_alias(&prop.table_alias.0) {
                if let Some(node_alias) = LogicalPlan::coupled_edge_render_alias_for_aggregate(
                    gr,
                    &gr.left_connection,
                    &gr.right_connection,
                    &prop.table_alias.0,
                ) {
                    if from_alias == Some(node_alias.as_str()) {
                        log::info!(
                            "🔧 #584: FK-edge coupled rel var '{}' → node alias '{}' (denorm resolve)",
                            prop.table_alias.0,
                            node_alias
                        );
                        prop.table_alias = super::render_expr::TableAlias(node_alias);
                        return;
                    }
                }
            }
            if let Ok((properties, Some(edge_alias))) =
                plan.get_properties_with_table_alias(&prop.table_alias.0)
            {
                {
                    // This is a denormalized node — resolve both alias and property.
                    // The properties list is (cypher_name, db_column) pairs
                    // from from_node_properties or to_node_properties,
                    // correctly distinguishing Origin* vs Dest* columns.
                    let current_col = prop.column.raw().to_string();

                    // Match by Cypher property name first (before schema rewriting),
                    // then by DB column name (after schema rewriting).
                    // This handles both pre- and post-rewritten expressions.
                    let mapped_column = properties
                                                .iter()
                                                .find(|(prop_name, _)| *prop_name == current_col)
                                                .map(|(_, col)| col.clone())
                                                .or_else(|| {
                                                    // The column may have been rewritten by schema mapping
                                                    // to a DB column (e.g., city → OriginCityName).
                                                    // Check if current_col matches any DB column in our
                                                    // properties list (correct side).
                                                    if properties.iter().any(|(_, col)| *col == current_col) {
                                                        Some(current_col.clone())
                                                    } else {
                                                        // Schema mapped to wrong side's column (e.g., b.city
                                                        // became b.OriginCityName but should be DestCityName).
                                                        // Reverse-lookup: find the Cypher property that maps
                                                        // to current_col using from/to_properties on the
                                                        // node schema, then map through our properties list.
                                                        // Scoped to the alias's node label to avoid false matches.
                                                        use crate::query_planner::logical_expr::expression_rewriter::find_label_for_alias_in_plan;
                                                        use crate::server::query_context::get_current_schema_with_fallback;
                                                        let node_label = find_label_for_alias_in_plan(plan, &prop.table_alias.0);
                                                        if let (Some(label), Some(schema)) = (node_label, get_current_schema_with_fallback()) {
                                                            if let Some(node_schema) = schema.all_node_schemas().get(&label) {
                                                                // Check from_properties (sorted: HashMap iteration
                                                                // order is per-process random — #480 class)
                                                                if let Some(from_props) = &node_schema.from_properties {
                                                                    let mut from_props: Vec<_> = from_props.iter().collect();
                                                                    from_props.sort_by(|a, b| a.0.cmp(b.0));
                                                                    for (cypher_name, db_col) in from_props {
                                                                        if *db_col == current_col {
                                                                            if let Some((_, correct_col)) = properties
                                                                                .iter()
                                                                                .find(|(pn, _)| pn == cypher_name)
                                                                            {
                                                                                log::info!(
                                                                                    "🔧 Denormalized cross-side fix: '{}.{}' (from '{}') → '{}.{}'",
                                                                                    prop.table_alias.0, current_col,
                                                                                    cypher_name, edge_alias, correct_col
                                                                                );
                                                                                return Some(correct_col.clone());
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                                // Check to_properties (sorted — see above)
                                                                if let Some(to_props) = &node_schema.to_properties {
                                                                    let mut to_props: Vec<_> = to_props.iter().collect();
                                                                    to_props.sort_by(|a, b| a.0.cmp(b.0));
                                                                    for (cypher_name, db_col) in to_props {
                                                                        if *db_col == current_col {
                                                                            if let Some((_, correct_col)) = properties
                                                                                .iter()
                                                                                .find(|(pn, _)| pn == cypher_name)
                                                                            {
                                                                                log::info!(
                                                                                    "🔧 Denormalized cross-side fix: '{}.{}' (from '{}') → '{}.{}'",
                                                                                    prop.table_alias.0, current_col,
                                                                                    cypher_name, edge_alias, correct_col
                                                                                );
                                                                                return Some(correct_col.clone());
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        None
                                                    }
                                                });

                    if let Some(actual_column) = mapped_column {
                        if edge_alias != prop.table_alias.0 || actual_column != current_col {
                            log::info!(
                                "🔧 Denormalized property resolve in WITH: '{}.{}' → '{}.{}'",
                                prop.table_alias.0,
                                current_col,
                                edge_alias,
                                actual_column
                            );
                            prop.table_alias =
                                crate::render_plan::render_expr::TableAlias(edge_alias);
                            prop.column =
                                crate::graph_catalog::expression_parser::PropertyValue::Column(
                                    actual_column,
                                );
                        }
                    } else if edge_alias != prop.table_alias.0 {
                        // Property not in any mapping but alias needs rewriting
                        log::info!(
                            "🔧 Denormalized alias rewrite in WITH: '{}.{}' → '{}.{}'",
                            prop.table_alias.0,
                            current_col,
                            edge_alias,
                            current_col
                        );
                        prop.table_alias = crate::render_plan::render_expr::TableAlias(edge_alias);
                    }
                }
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                resolve_denormalized_property_in_expr_impl(arg, plan, from_alias);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &mut f.args {
                resolve_denormalized_property_in_expr_impl(arg, plan, from_alias);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                resolve_denormalized_property_in_expr_impl(operand, plan, from_alias);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(expr) = &mut case.expr {
                resolve_denormalized_property_in_expr_impl(expr, plan, from_alias);
            }
            for (cond, then_expr) in &mut case.when_then {
                resolve_denormalized_property_in_expr_impl(cond, plan, from_alias);
                resolve_denormalized_property_in_expr_impl(then_expr, plan, from_alias);
            }
            if let Some(else_expr) = &mut case.else_expr {
                resolve_denormalized_property_in_expr_impl(else_expr, plan, from_alias);
            }
        }
        RenderExpr::List(items) => {
            for item in items {
                resolve_denormalized_property_in_expr_impl(item, plan, from_alias);
            }
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, value) in entries {
                resolve_denormalized_property_in_expr_impl(value, plan, from_alias);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            resolve_denormalized_property_in_expr_impl(array, plan, from_alias);
            resolve_denormalized_property_in_expr_impl(index, plan, from_alias);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            resolve_denormalized_property_in_expr_impl(array, plan, from_alias);
            if let Some(f) = from {
                resolve_denormalized_property_in_expr_impl(f, plan, from_alias);
            }
            if let Some(t) = to {
                resolve_denormalized_property_in_expr_impl(t, plan, from_alias);
            }
        }
        RenderExpr::InSubquery(insub) => {
            resolve_denormalized_property_in_expr_impl(&mut insub.expr, plan, from_alias);
        }
        RenderExpr::ReduceExpr(reduce) => {
            resolve_denormalized_property_in_expr_impl(&mut reduce.initial_value, plan, from_alias);
            resolve_denormalized_property_in_expr_impl(&mut reduce.list, plan, from_alias);
            resolve_denormalized_property_in_expr_impl(&mut reduce.expression, plan, from_alias);
        }
        _ => {}
    }
}

/// Shared core for the two UNWIND-alias collectors (§4.2 D5 merge).
///
/// Walks the *linear modifier spine* above an UNWIND — `Unwind` (collect its
/// alias, then continue) and the single-input wrappers `Filter` / `Projection`
/// / `OrderBy` / `Limit` / `Skip` / `GroupBy` — pushing each UNWIND alias into
/// `sink`. It deliberately does NOT branch into `GraphRel` / `Union` /
/// `CartesianProduct`: UNWIND aliases live on the projection spine, not inside
/// pattern subtrees.
///
/// The ONE behavioral difference between the two historical copies is the
/// `WithClause` barrier, captured by `cross_with_barrier`:
/// - `false` — stop at a `WithClause` (its UNWIND vars are a prior segment,
///   now CTE columns; re-emitting them as bare columns is wrong). This is
///   [`collect_unwind_aliases`].
/// - `true` — descend through a `WithClause` too (for ID-column detection,
///   where an earlier UNWIND scalar still needs to be recognized). This is
///   [`find_unwind_aliases`].
fn collect_unwind_aliases_core(
    plan: &LogicalPlan,
    cross_with_barrier: bool,
    sink: &mut impl FnMut(&str),
) {
    match plan {
        LogicalPlan::Unwind(u) => {
            sink(&u.alias);
            collect_unwind_aliases_core(&u.input, cross_with_barrier, sink);
        }
        LogicalPlan::Filter(f) => collect_unwind_aliases_core(&f.input, cross_with_barrier, sink),
        LogicalPlan::Projection(p) => {
            collect_unwind_aliases_core(&p.input, cross_with_barrier, sink)
        }
        LogicalPlan::OrderBy(ob) => {
            collect_unwind_aliases_core(&ob.input, cross_with_barrier, sink)
        }
        LogicalPlan::Limit(lim) => {
            collect_unwind_aliases_core(&lim.input, cross_with_barrier, sink)
        }
        LogicalPlan::Skip(s) => collect_unwind_aliases_core(&s.input, cross_with_barrier, sink),
        LogicalPlan::GroupBy(gb) => {
            collect_unwind_aliases_core(&gb.input, cross_with_barrier, sink)
        }
        // The barrier: only crossed for the `find_unwind_aliases` flavor.
        LogicalPlan::WithClause(wc) if cross_with_barrier => {
            collect_unwind_aliases_core(&wc.input, cross_with_barrier, sink)
        }
        _ => {}
    }
}

/// Collect UNWIND aliases on the projection spine, STOPPING at a `WithClause`
/// barrier. See [`collect_unwind_aliases_core`].
pub(crate) fn collect_unwind_aliases(
    plan: &LogicalPlan,
    out: &mut std::collections::HashSet<String>,
) {
    collect_unwind_aliases_core(plan, false, &mut |alias| {
        out.insert(alias.to_string());
    });
}

pub(crate) fn plan_has_denormalized_union(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Union(u) => u.inputs.iter().any(|input| {
            fn has_denorm_vs(p: &LogicalPlan) -> bool {
                match p {
                    LogicalPlan::ViewScan(vs) => {
                        crate::graph_catalog::pattern_schema::scan_denormalized_flag(vs)
                    }
                    LogicalPlan::GraphNode(gn) => has_denorm_vs(gn.input.as_ref()),
                    LogicalPlan::Filter(f) => has_denorm_vs(f.input.as_ref()),
                    LogicalPlan::Projection(p) => has_denorm_vs(p.input.as_ref()),
                    _ => false,
                }
            }
            has_denorm_vs(input.as_ref())
        }),
        LogicalPlan::Filter(f) => plan_has_denormalized_union(f.input.as_ref()),
        LogicalPlan::GraphNode(gn) => plan_has_denormalized_union(gn.input.as_ref()),
        LogicalPlan::Projection(p) => plan_has_denormalized_union(p.input.as_ref()),
        _ => false,
    }
}

pub(crate) fn rename_branch_aliases(select: &mut SelectItems, alias: &str) {
    use crate::utils::cte_column_naming::cte_column_name;
    for item in &mut select.items {
        if let Some(ref mut col_alias) = item.col_alias {
            if col_alias.0 == "__label__" {
                continue;
            }
            let new_name = cte_column_name(alias, &col_alias.0);
            col_alias.0 = new_name;
        }
    }
    select.distinct = true;
    // Sort by alias to ensure consistent column order across
    // UNION branches (SQL UNION maps by position, not name)
    select.items.sort_by(|a, b| {
        let a_alias = a.col_alias.as_ref().map(|c| c.0.as_str()).unwrap_or("");
        let b_alias = b.col_alias.as_ref().map(|c| c.0.as_str()).unwrap_or("");
        a_alias.cmp(b_alias)
    });
}

/// Check if a LogicalExpr is a constant literal (no need to GROUP BY)
pub(crate) fn is_literal_expr(expr: &crate::query_planner::logical_expr::LogicalExpr) -> bool {
    matches!(
        expr,
        crate::query_planner::logical_expr::LogicalExpr::Literal(_)
    )
}

/// Recursively check if an expression contains an aggregate function.
/// Comprehensive variant used to decide whether a WITH projection aggregates.
pub(crate) fn expr_contains_aggregate(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
) -> bool {
    use crate::query_planner::logical_expr::LogicalExpr;
    match expr {
        LogicalExpr::AggregateFnCall(_) => true,
        LogicalExpr::ScalarFnCall(f) => f.args.iter().any(expr_contains_aggregate),
        LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
            op.operands.iter().any(expr_contains_aggregate)
        }
        LogicalExpr::Case(c) => {
            // #576: a simple CASE can carry an aggregate in its scrutinee too.
            c.expr.as_ref().is_some_and(|e| expr_contains_aggregate(e))
                || c.when_then.iter().any(|(cond, val)| {
                    expr_contains_aggregate(cond) || expr_contains_aggregate(val)
                })
                || c.else_expr
                    .as_ref()
                    .is_some_and(|e| expr_contains_aggregate(e))
        }
        LogicalExpr::List(items) => items.iter().any(expr_contains_aggregate),
        LogicalExpr::ArraySubscript { array, index } => {
            expr_contains_aggregate(array) || expr_contains_aggregate(index)
        }
        _ => false,
    }
}

pub(crate) fn rewrite_person_to_fk(
    expr: &mut RenderExpr,
    person_alias: &str,
    rel_alias: &str,
    fk_col: &str,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) if pa.table_alias.0 == person_alias => {
            pa.table_alias = TableAlias(rel_alias.to_string());
            pa.column = PropertyValue::Column(fk_col.to_string());
        }
        RenderExpr::TableAlias(ta) if ta.0 == person_alias => {
            *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(fk_col.to_string()),
            });
        }
        RenderExpr::ColumnAlias(ca) if ca.0 == person_alias => {
            *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(fk_col.to_string()),
            });
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in agg.args.iter_mut() {
                rewrite_person_to_fk(arg, person_alias, rel_alias, fk_col);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in f.args.iter_mut() {
                rewrite_person_to_fk(arg, person_alias, rel_alias, fk_col);
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in op.operands.iter_mut() {
                rewrite_person_to_fk(operand, person_alias, rel_alias, fk_col);
            }
        }
        RenderExpr::List(items) => {
            for item in items.iter_mut() {
                rewrite_person_to_fk(item, person_alias, rel_alias, fk_col);
            }
        }
        // The Databricks count→conditional rewrite wraps the
        // moved predicate in `count(CASE WHEN cond THEN x END)`;
        // descend so the person ref inside `cond` is rewritten to
        // the FK column (CH keeps it in countIf args, walked above).
        RenderExpr::Case(case) => {
            if let Some(e) = case.expr.as_mut() {
                rewrite_person_to_fk(e, person_alias, rel_alias, fk_col);
            }
            for (when, then) in case.when_then.iter_mut() {
                rewrite_person_to_fk(when, person_alias, rel_alias, fk_col);
                rewrite_person_to_fk(then, person_alias, rel_alias, fk_col);
            }
            if let Some(e) = case.else_expr.as_mut() {
                rewrite_person_to_fk(e, person_alias, rel_alias, fk_col);
            }
        }
        _ => {}
    }
}

pub(crate) fn plan_has_shortest_path(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(gr) => {
            gr.shortest_path_mode.is_some()
                || plan_has_shortest_path(gr.left.as_ref())
                || plan_has_shortest_path(gr.right.as_ref())
        }
        LogicalPlan::WithClause(wc) => plan_has_shortest_path(wc.input.as_ref()),
        LogicalPlan::Filter(f) => plan_has_shortest_path(f.input.as_ref()),
        LogicalPlan::Projection(p) => plan_has_shortest_path(p.input.as_ref()),
        LogicalPlan::GraphJoins(gj) => plan_has_shortest_path(gj.input.as_ref()),
        LogicalPlan::GraphNode(gn) => plan_has_shortest_path(gn.input.as_ref()),
        LogicalPlan::GroupBy(gb) => plan_has_shortest_path(gb.input.as_ref()),
        LogicalPlan::OrderBy(ob) => plan_has_shortest_path(ob.input.as_ref()),
        LogicalPlan::Skip(s) => plan_has_shortest_path(s.input.as_ref()),
        LogicalPlan::Limit(l) => plan_has_shortest_path(l.input.as_ref()),
        LogicalPlan::CartesianProduct(cp) => {
            plan_has_shortest_path(cp.left.as_ref()) || plan_has_shortest_path(cp.right.as_ref())
        }
        LogicalPlan::Union(u) => u.inputs.iter().any(|i| plan_has_shortest_path(i.as_ref())),
        _ => false,
    }
}

/// Collect UNWIND aliases on the projection spine, CROSSING `WithClause`
/// barriers (for ID-column detection). See [`collect_unwind_aliases_core`].
///
/// NB: pushes duplicates if the same alias appears twice — callers only ever
/// use the result via `.contains(...)`, so this preserves the historical
/// `Vec`-with-possible-duplicates behavior exactly.
pub(crate) fn find_unwind_aliases(plan: &LogicalPlan, out: &mut Vec<String>) {
    collect_unwind_aliases_core(plan, true, &mut |alias| {
        out.push(alias.to_string());
    });
}

// P2.6 slice 4 (REFACTORING_SAFETY_PLAN.md §5.1): the giant WITH→CTE builder
// `build_chained_with_match_cte_plan` (~5,478 lines) + its two orbit structs
// (`WithBarrierScope`, `CteNameAllocator`, used only by it) moved verbatim to
// the `render_plan::with_to_cte` module — the last function of the "entangled
// core". Re-exported here `pub(crate)` during the transition so `plan_builder.rs`'s
// four call sites keep resolving via `use super::plan_builder_utils::{…}`. The 24
// private head utilities it calls back into (still in this file) were widened
// `fn` → `pub(crate) fn` at their definitions above so the moved function can
// back-import them; bodies are byte-identical.
pub(crate) use super::with_to_cte::build_chained_with_match_cte_plan;

// P2.6 (slices 1–3): the #529 property helpers, the WITH-discovery / join-pruning
// cluster, and `replace_with_clause_with_cte_reference_v2` also moved to
// `render_plan::with_to_cte`. After slice 4 moved `build_chained_with_match_cte_plan`
// out (their sole production caller here), the only remaining callers in THIS file
// are the `#[cfg(test)]` characterization tests — which reference
// `find_all_with_clauses_grouped` (9 sites). So its re-export is gated `#[cfg(test)]`;
// the other moved names have no caller left here and are called directly from
// `with_to_cte` (their re-exports were dropped). `collect_property_accesses` grep
// hits elsewhere are the unrelated `Self::collect_property_accesses` analyzer method.
#[cfg(test)]
pub(crate) use super::with_to_cte::find_all_with_clauses_grouped;

// P2.2 (REFACTORING_SAFETY_PLAN.md §5.1): the pattern-comprehension SQL
// string-emitting group (build SQL *text* for `size([(a)-[:R]->(b) | ...])` /
// pattern-count comprehensions) was moved verbatim to
// `render_plan::pattern_comprehension_sql`. Re-exported here during the
// transition so the existing callers on this path keep resolving. The seven
// names whose only caller here was `build_chained_with_match_cte_plan`
// (`add_join_to_plan_or_union_branches`, `find_node_id_column_from_schema`,
// `find_pc_cte_join_column`, `generate_and_replace_arraycount_pc_subqueries`,
// `generate_pattern_comprehension_cte`,
// `replace_count_star_placeholders_in_select_or_union`, `rewrite_logical_expr_aliases`)
// are imported directly by `with_to_cte` after P2.6 slice 4, so they are no
// longer re-exported here.
pub(crate) use super::pattern_comprehension_sql::{
    build_node_id_expr_for_join, build_pattern_comprehension_sql,
};
// These three are referenced only from this file's `#[cfg(test)]` module now
// (their production callers moved with the group); gate the re-export so a
// non-test lib build doesn't flag them as unused.
#[cfg(test)]
pub(crate) use super::pattern_comprehension_sql::{
    build_cte_column_map, emit_array_count_call, emit_array_count_in_subquery,
};
