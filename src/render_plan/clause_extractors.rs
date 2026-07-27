//! Clause extractors — pure walkers that pull a single SQL clause out of a
//! `LogicalPlan` tail (HAVING / ORDER BY / LIMIT / SKIP) plus the
//! property-sorting helper they share with the whole-node property builders.
//!
//! Extracted verbatim from `plan_builder_utils.rs` in P2.3
//! (`REFACTORING_SAFETY_PLAN.md` §5.1). No logic edits — the functions are
//! re-exported `pub(crate)` from `plan_builder_utils` during the transition so
//! existing `super::plan_builder_utils::extract_*` call sites keep resolving.
//!
//! Note: `extract_filters` / `extract_from` / `extract_group_by` /
//! `extract_distinct` from the original §5.1 group already migrated to
//! `filter_builder.rs` / `group_by_builder.rs` via incremental work, so the
//! genuinely-pure remainder that lands here is these five.

use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::plan_builder_helpers::apply_property_mapping_to_expr;
use crate::render_plan::render_expr::RenderExpr;
use crate::render_plan::OrderByItem;

type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

pub fn extract_having(plan: &LogicalPlan) -> RenderPlanBuilderResult<Option<RenderExpr>> {
    let having_clause = match plan {
        LogicalPlan::Limit(limit) => extract_having(&limit.input)?,
        LogicalPlan::Skip(skip) => extract_having(&skip.input)?,
        LogicalPlan::OrderBy(order_by) => extract_having(&order_by.input)?,
        LogicalPlan::Projection(projection) => extract_having(&projection.input)?,
        LogicalPlan::GroupBy(group_by) => {
            if let Some(having) = &group_by.having_clause {
                let mut render_expr: RenderExpr = having.clone().try_into()?;
                // Apply property mapping to the HAVING expression
                apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
                Some(render_expr)
            } else {
                None
            }
        }
        _ => None,
    };
    Ok(having_clause)
}

pub fn extract_order_by(plan: &LogicalPlan) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
    let order_by =
        match plan {
            LogicalPlan::Limit(limit) => extract_order_by(&limit.input)?,
            LogicalPlan::Skip(skip) => extract_order_by(&skip.input)?,
            LogicalPlan::OrderBy(order_by) => order_by
                .items
                .iter()
                .cloned()
                .map(|item| {
                    // #484: id()/elementId() in ORDER BY position must route through
                    // the same pattern_union-aware / schema-driven ID resolution as
                    // GROUP BY and SELECT (see `resolve_id_function_for_group_order`
                    // doc comment in group_by_builder.rs) — otherwise it falls
                    // through to the generic function-registry `toInt64(0)`
                    // placeholder, making `ORDER BY id(o)` a silent no-op.
                    if let Some(resolved) =
                        super::group_by_builder::resolve_id_function_for_group_order(
                            &order_by.input,
                            &item.expression,
                        )
                    {
                        return Ok(OrderByItem {
                            expression: resolved,
                            order: match item.order {
                                crate::query_planner::logical_plan::OrderByOrder::Asc => {
                                    crate::render_plan::OrderByOrder::Asc
                                }
                                crate::query_planner::logical_plan::OrderByOrder::Desc => {
                                    crate::render_plan::OrderByOrder::Desc
                                }
                            },
                        });
                    }
                    let mut order_item: OrderByItem = item.try_into()?;
                    // Apply property mapping to the order by expression
                    apply_property_mapping_to_expr(&mut order_item.expression, &order_by.input);
                    Ok(order_item)
                })
                .collect::<Result<Vec<OrderByItem>, RenderBuildError>>()?,
            _ => vec![],
        };
    Ok(order_by)
}

pub fn extract_limit(plan: &LogicalPlan) -> Option<i64> {
    match plan {
        LogicalPlan::Limit(limit) => Some(limit.count),
        _ => None,
    }
}

pub fn extract_skip(plan: &LogicalPlan) -> Option<i64> {
    match plan {
        LogicalPlan::Limit(limit) => extract_skip(&limit.input),
        LogicalPlan::Skip(skip) => Some(skip.count),
        _ => None,
    }
}

pub fn extract_sorted_properties(
    property_map: &std::collections::HashMap<
        String,
        crate::graph_catalog::expression_parser::PropertyValue,
    >,
) -> Vec<(String, String)> {
    let mut properties: Vec<(String, String)> = property_map
        .iter()
        .map(|(prop_name, prop_value)| (prop_name.clone(), prop_value.raw().to_string()))
        .collect();
    properties.sort_by(|a, b| a.0.cmp(&b.0));
    properties
}
