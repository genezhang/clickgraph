use std::sync::Arc;

use crate::query_planner::{
    analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
    logical_expr::visitors::{self, HasAggregateCheck},
    logical_expr::LogicalExpr,
    logical_plan::{GroupBy, LogicalPlan, Projection},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct GroupByBuilding;

impl GroupByBuilding {
    /// Check if an expression references a projection alias (from WITH clause).
    /// Returns true if the expression contains a TableAlias that is registered as a projection alias.
    fn references_projection_alias(expr: &LogicalExpr, plan_ctx: &PlanCtx) -> bool {
        match expr {
            LogicalExpr::TableAlias(alias) => {
                // Check if this alias is a projection alias (not a table alias)
                plan_ctx.is_projection_alias(&alias.0)
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                // Check if any operand references a projection alias
                op.operands
                    .iter()
                    .any(|operand| Self::references_projection_alias(operand, plan_ctx))
            }
            LogicalExpr::ScalarFnCall(func) => {
                // Check if any argument references a projection alias
                func.args
                    .iter()
                    .any(|arg| Self::references_projection_alias(arg, plan_ctx))
            }
            LogicalExpr::AggregateFnCall(func) => {
                // Check if any argument references a projection alias
                func.args
                    .iter()
                    .any(|arg| Self::references_projection_alias(arg, plan_ctx))
            }
            LogicalExpr::List(list) => {
                // Check if any list element references a projection alias
                list.iter()
                    .any(|item| Self::references_projection_alias(item, plan_ctx))
            }
            // Other expression types don't contain aliases
            _ => false,
        }
    }
}

// In the final projections, if there is an aggregate fn then add other projections in group by clause
// build group by plan after projection tagging.
//
// P1.3 migration note: this pass runs on `LogicalPlan::transform_up`, which
// recurses exhaustively (children before parents) via `map_children_arc` — so
// the rewrite hook below sees every node with its children already rewritten,
// exactly like the old hand-rolled walker's `self.analyze(child)?`-then-match
// arms, but without per-variant recursion arms that could silently skip new
// variants. Only the three arms with real logic remain: Projection (build
// GroupBy from aggregations / pass-through elimination) and Filter (fold a
// projection-alias predicate into HAVING).
impl AnalyzerPass for GroupByBuilding {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        LogicalPlan::transform_up(&logical_plan, &mut |node| {
            Ok(self.rewrite_node(node, _plan_ctx))
        })
    }
}

impl GroupByBuilding {
    /// Per-node rewrite applied bottom-up by `transform_up`. `node`'s children
    /// have already been rewritten when this runs, so inspecting `.input` here
    /// sees the transformed child — same as the old walker's analyze-then-check.
    ///
    /// Returning `Transformed::No(<different plan>)` reproduces the old
    /// walker's pass-through contract exactly: the replacement is kept if an
    /// ancestor (or the root caller's `get_plan()`) picks it up, and discarded
    /// if an untransformed parent clones itself — byte-for-byte the behavior
    /// of the old `return Ok(child_tf)` pass-through arm.
    fn rewrite_node(
        &self,
        node: &Arc<LogicalPlan>,
        plan_ctx: &PlanCtx,
    ) -> Transformed<Arc<LogicalPlan>> {
        match node.as_ref() {
            LogicalPlan::Projection(projection) => {
                // Split items into aggregate-containing and aggregate-free, and
                // build the implicit GROUP BY keys.
                //
                // #637: a grouping key can be BURIED inside an item that also
                // contains an aggregate (e.g. `a.city` in
                // `a.city + toString(count(b))`). The old code keyed GROUP BY on
                // whole aggregate-free items only, so such buried keys were
                // dropped and no GROUP BY was emitted → ClickHouse Code 215.
                //
                // Behavior-preserving split:
                //  - aggregate-FREE item  → push the WHOLE item as a key,
                //    byte-identically to the old path (incl. constant keys like
                //    `RETURN 'all_users', count(n)` → `GROUP BY 'all_users'`).
                //  - aggregate-CONTAINING item → extract its buried
                //    non-aggregate keys via the shared `collect_grouping_keys`
                //    (the #637 fix; empty for an aggregates-only item).
                let has_aggregate = projection
                    .items
                    .iter()
                    .any(|item| HasAggregateCheck::check(&item.expression));

                let mut grouping_keys: Vec<LogicalExpr> = Vec::new();
                for item in &projection.items {
                    if HasAggregateCheck::check(&item.expression) {
                        for key in visitors::collect_grouping_keys(&item.expression) {
                            if !grouping_keys.contains(&key) {
                                grouping_keys.push(key);
                            }
                        }
                    } else if !grouping_keys.contains(&item.expression) {
                        grouping_keys.push(item.expression.clone());
                    }
                }

                log::trace!(
                    "GroupByBuilding: has_aggregate={}, {} grouping keys",
                    has_aggregate,
                    grouping_keys.len()
                );

                if has_aggregate && !grouping_keys.is_empty() {
                    // Projection mixes aggregates and plain expressions — wrap
                    // it in a GroupBy keyed on the non-aggregate items. (The
                    // old walker had separate "two-level" and "single-level"
                    // branches here, but both built the identical structure —
                    // they differed only in a trace line.)
                    //
                    // Old-walker visibility quirk, preserved (same as the
                    // Filter arm below): this arm unwrapped its child's
                    // `Transformed` unconditionally, so a pass-through-
                    // eliminated child Projection WAS spliced in here even
                    // though every other parent discarded it. Recompute it
                    // for the direct child.
                    let effective_input: &Arc<LogicalPlan> = match projection.input.as_ref() {
                        LogicalPlan::Projection(p) => {
                            Self::passthrough_eliminated(p).unwrap_or(&projection.input)
                        }
                        _ => &projection.input,
                    };
                    if matches!(effective_input.as_ref(), LogicalPlan::GroupBy(_)) {
                        log::trace!(
                            "GroupByBuilding: Two-level aggregation detected - RETURN aggregates over WITH GroupBy"
                        );
                    } else {
                        log::trace!(
                            "GroupByBuilding: Creating GroupBy node with {} grouping expressions",
                            grouping_keys.len()
                        );
                    }
                    let wrapped: Arc<LogicalPlan> =
                        if Arc::ptr_eq(effective_input, &projection.input) {
                            Arc::clone(node)
                        } else {
                            Arc::new(LogicalPlan::Projection(Projection {
                                input: Arc::clone(effective_input),
                                items: projection.items.clone(),
                                distinct: projection.distinct,
                                pattern_comprehensions: projection.pattern_comprehensions.clone(),
                            }))
                        };
                    Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy {
                        input: wrapped,
                        expressions: grouping_keys,
                        having_clause: None,
                        is_materialization_boundary: false,
                        exposed_alias: None,
                    })))
                } else {
                    // No mixed aggregation. OPTIMIZATION: if the (already
                    // rewritten) child is a GroupBy over a WITH Projection and
                    // this Projection just passes the WITH aliases through
                    // unchanged, eliminate it.
                    if let Some(group_by_child) = Self::passthrough_eliminated(projection) {
                        log::trace!(
                            "GroupByBuilding: OPTIMIZATION - RETURN passes through all WITH aliases unchanged, eliminating outer Projection"
                        );
                        return Transformed::No(Arc::clone(group_by_child));
                    }
                    Transformed::No(Arc::clone(node))
                }
            }
            LogicalPlan::Filter(filter) => {
                // If the (already rewritten) child is a GroupBy and the filter
                // predicate references a projection alias, fold it in as HAVING.
                //
                // Old-walker visibility quirk, preserved: the hand-rolled
                // version inspected the plan inside the child's `Transformed`
                // regardless of Yes/No, so a pass-through-eliminated
                // Projection child (reported as `Transformed::No(<GroupBy>)`
                // and therefore discarded by every OTHER parent's
                // rebuild_or_clone) was still visible HERE as a GroupBy. Since
                // this driver discards No-replacements at unchanged parents,
                // recompute the elimination for the Filter's direct child.
                let effective_input: &Arc<LogicalPlan> = match filter.input.as_ref() {
                    LogicalPlan::Projection(p) => {
                        Self::passthrough_eliminated(p).unwrap_or(&filter.input)
                    }
                    _ => &filter.input,
                };
                if let LogicalPlan::GroupBy(group_by) = effective_input.as_ref() {
                    let refs_proj_alias =
                        Self::references_projection_alias(&filter.predicate, plan_ctx);
                    log::trace!(
                        "GroupByBuilding: Filter over GroupBy, references projection alias: {}",
                        refs_proj_alias
                    );
                    if refs_proj_alias {
                        log::trace!("GroupByBuilding: Converting Filter to HAVING clause");
                        return Transformed::Yes(Arc::new(LogicalPlan::GroupBy(GroupBy {
                            input: group_by.input.clone(),
                            expressions: group_by.expressions.clone(),
                            having_clause: Some(filter.predicate.clone()),
                            is_materialization_boundary: group_by.is_materialization_boundary,
                            exposed_alias: group_by.exposed_alias.clone(),
                        })));
                    }
                }
                Transformed::No(Arc::clone(node))
            }
            _ => Transformed::No(Arc::clone(node)),
        }
    }

    /// The pass-through-elimination check: if `projection` is a pure alias
    /// pass-through of the WITH Projection beneath its GroupBy child, return
    /// that GroupBy child (the plan the Projection collapses to).
    ///
    /// Known one-level limit (review-verified unreachable): for STACKED
    /// pass-through Projections `P2(P1(GroupBy(Projection)))` where P1's
    /// elimination fired as a pure `Transformed::No`, the old walker would
    /// also eliminate P2; the recompute sites here only look one child deep.
    /// That shape requires a `GroupBy(Projection)` to exist BEFORE this pass
    /// runs, and no analyzer stage constructs one before GroupByBuilding —
    /// the elimination can only fire against a GroupBy freshly built in this
    /// pass (a `Transformed::Yes` child), where the driver splices it in and
    /// the ancestor arms see it directly. If a future pass starts emitting
    /// pre-built `GroupBy(Projection)` shapes, revisit this.
    fn passthrough_eliminated(projection: &Projection) -> Option<&Arc<LogicalPlan>> {
        let LogicalPlan::GroupBy(group_by) = projection.input.as_ref() else {
            return None;
        };
        let LogicalPlan::Projection(inner_proj) = group_by.input.as_ref() else {
            return None;
        };
        let with_aliases: std::collections::HashSet<String> = inner_proj
            .items
            .iter()
            .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
            .collect();

        let all_pass_through = projection.items.iter().all(|item| {
            if let LogicalExpr::TableAlias(alias) = &item.expression {
                with_aliases.contains(&alias.0)
                    && item.col_alias.as_ref().is_some_and(|col| col.0 == alias.0)
            } else {
                false
            }
        });

        if all_pass_through && projection.items.len() == with_aliases.len() {
            Some(&projection.input)
        } else {
            None
        }
    }
}

impl GroupByBuilding {
    pub fn new() -> Self {
        GroupByBuilding
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::query_planner::logical_expr::{AggregateFnCall, Column, PropertyAccess, TableAlias};
    use crate::query_planner::logical_plan::{LogicalPlan, Projection, ProjectionItem};

    fn create_property_access(table: &str, column: &str) -> LogicalExpr {
        LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(table.to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                column.to_string(),
            ),
        })
    }

    fn create_aggregate_function(name: &str, arg_table: &str, arg_column: &str) -> LogicalExpr {
        LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: name.to_string(),
            args: vec![create_property_access(arg_table, arg_column)],
        })
    }

    fn create_scan(_alias: Option<String>, _table_name: Option<String>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Empty)
    }

    #[test]
    fn test_projection_with_mixed_aggregate_and_non_aggregate() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // Test projection: SELECT user.name, COUNT(order.id) FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
            ],
            distinct: false,
            pattern_comprehensions: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should create GroupBy plan wrapping the projection
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GroupBy(group_by) => {
                        // GroupBy should wrap the original projection
                        assert_eq!(group_by.input, projection);

                        // Group expressions should contain only non-aggregate expressions
                        assert_eq!(group_by.expressions.len(), 1);
                        match &group_by.expressions[0] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.table_alias.0, "user");
                                assert_eq!(prop_acc.column.raw(), "name");
                            }
                            _ => panic!("Expected PropertyAccess in group expressions"),
                        }
                    }
                    _ => panic!("Expected GroupBy plan"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_projection_with_only_aggregates_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // Test projection: SELECT COUNT(order.id), SUM(order.amount) FROM ...
        let scan = create_scan(Some("order".to_string()), Some("orders".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("sum", "order", "amount"),
                    col_alias: None,
                },
            ],
            distinct: false,
            pattern_comprehensions: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (only aggregates, no grouping needed)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection); // Should return original plan unchanged
            }
            _ => panic!("Expected no transformation for aggregates-only projection"),
        }
    }

    #[test]
    fn test_projection_with_only_non_aggregates_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // Test projection: SELECT user.name, user.email FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_property_access("user", "email"),
                    col_alias: None,
                },
            ],
            distinct: false,
            pattern_comprehensions: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (no aggregates present)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection); // Should return original plan unchanged
            }
            _ => panic!("Expected no transformation for non-aggregates-only projection"),
        }
    }

    #[test]
    fn test_projection_with_multiple_non_aggregates_and_aggregate() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // Test projection: SELECT user.name, user.city, COUNT(order.id) FROM ...
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_property_access("user", "city"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: create_aggregate_function("count", "order", "id"),
                    col_alias: None,
                },
            ],
            distinct: false,
            pattern_comprehensions: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should create GroupBy with both non-aggregate expressions
        match result {
            Transformed::Yes(new_plan) => {
                match new_plan.as_ref() {
                    LogicalPlan::GroupBy(group_by) => {
                        assert_eq!(group_by.expressions.len(), 2);

                        // First group expression: user.name
                        match &group_by.expressions[0] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.column.raw(), "name");
                            }
                            _ => panic!("Expected PropertyAccess"),
                        }

                        // Second group expression: user.city
                        match &group_by.expressions[1] {
                            LogicalExpr::PropertyAccessExp(prop_acc) => {
                                assert_eq!(prop_acc.column.raw(), "city");
                            }
                            _ => panic!("Expected PropertyAccess"),
                        }
                    }
                    _ => panic!("Expected GroupBy plan"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_empty_projection_no_groupby() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // Test empty projection
        let scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: scan,
            items: vec![],
            distinct: false,
            pattern_comprehensions: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        // Should NOT create GroupBy (empty projection)
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, projection);
            }
            _ => panic!("Expected no transformation for empty projection"),
        }
    }

    fn operator(
        op: crate::query_planner::logical_expr::Operator,
        operands: Vec<LogicalExpr>,
    ) -> LogicalExpr {
        LogicalExpr::OperatorApplicationExp(
            crate::query_planner::logical_expr::OperatorApplication {
                operator: op,
                operands,
            },
        )
    }

    /// #637: a SINGLE projection item that BURIES a grouping key inside an
    /// aggregate-containing expression (`user.name + count(order.id)`) must
    /// still build a GroupBy keyed on the buried `user.name` — the old split
    /// produced no GroupBy at all (ClickHouse Code 215).
    #[test]
    fn test_projection_buried_key_in_aggregate_item() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // RETURN user.name + count(order.id) AS mixed
        let item = operator(
            crate::query_planner::logical_expr::Operator::Addition,
            vec![
                create_property_access("user", "name"),
                create_aggregate_function("count", "order", "id"),
            ],
        );
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: create_scan(Some("user".to_string()), Some("users".to_string())),
            items: vec![ProjectionItem {
                expression: item,
                col_alias: None,
            }],
            distinct: false,
            pattern_comprehensions: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        match result {
            Transformed::Yes(new_plan) => match new_plan.as_ref() {
                LogicalPlan::GroupBy(group_by) => {
                    assert_eq!(
                        group_by.expressions.len(),
                        1,
                        "buried user.name must be the sole key"
                    );
                    match &group_by.expressions[0] {
                        LogicalExpr::PropertyAccessExp(prop_acc) => {
                            assert_eq!(prop_acc.table_alias.0, "user");
                            assert_eq!(prop_acc.column.raw(), "name");
                        }
                        other => panic!("Expected user.name grouping key, got {other:?}"),
                    }
                }
                other => panic!("Expected GroupBy plan, got {other:?}"),
            },
            _ => panic!("Expected transformation (GroupBy) for buried-key item"),
        }
    }

    /// #637: a buried key alongside a SEPARATE bare key must not duplicate the
    /// bare key, and both must appear.
    #[test]
    fn test_projection_buried_key_dedups_with_bare_key() {
        let analyzer = GroupByBuilding::new();
        let mut plan_ctx = PlanCtx::new_empty();

        // RETURN user.name, user.name + count(order.id) AS mixed
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: create_scan(Some("user".to_string()), Some("users".to_string())),
            items: vec![
                ProjectionItem {
                    expression: create_property_access("user", "name"),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: operator(
                        crate::query_planner::logical_expr::Operator::Addition,
                        vec![
                            create_property_access("user", "name"),
                            create_aggregate_function("count", "order", "id"),
                        ],
                    ),
                    col_alias: None,
                },
            ],
            distinct: false,
            pattern_comprehensions: vec![],
        }));

        let result = analyzer.analyze(projection.clone(), &mut plan_ctx).unwrap();

        match result {
            Transformed::Yes(new_plan) => match new_plan.as_ref() {
                LogicalPlan::GroupBy(group_by) => {
                    // user.name appears once (deduped), not twice.
                    assert_eq!(group_by.expressions.len(), 1);
                }
                other => panic!("Expected GroupBy plan, got {other:?}"),
            },
            _ => panic!("Expected transformation"),
        }
    }
}
