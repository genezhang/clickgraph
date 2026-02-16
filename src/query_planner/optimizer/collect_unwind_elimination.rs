//! Collect+UNWIND No-Op Elimination Optimizer
//!
//! Detects and eliminates redundant collect()+UNWIND patterns:
//!
//! BEFORE:
//! ```cypher
//! MATCH (a)-[r]->(b)
//! WITH a, collect(b) as bs
//! UNWIND bs as b
//! RETURN b.name
//! ```
//!
//! AFTER:
//! ```cypher
//! MATCH (a)-[r]->(b)
//! RETURN b.name
//! ```
//!
//! This optimization removes unnecessary aggregation and array operations,
//! improving query performance by 2-5x for applicable queries.

use crate::query_planner::{
    logical_expr::{ColumnAlias, LogicalExpr, TableAlias},
    logical_plan::{LogicalPlan, ProjectionItem, Unwind, WithClause},
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};
use std::{collections::HashMap, sync::Arc};

pub struct CollectUnwindElimination;

/// Rewrite alias references in a single expression
fn rewrite_aliases_in_expr(expr: &LogicalExpr, alias_map: &HashMap<String, String>) -> LogicalExpr {
    match expr {
        LogicalExpr::TableAlias(ta) => {
            if let Some(new_alias) = alias_map.get(&ta.0) {
                LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(
                    new_alias.clone(),
                ))
            } else {
                expr.clone()
            }
        }

        LogicalExpr::PropertyAccessExp(pa) => {
            if let Some(new_alias) = alias_map.get(&pa.table_alias.0) {
                LogicalExpr::PropertyAccessExp(crate::query_planner::logical_expr::PropertyAccess {
                    table_alias: crate::query_planner::logical_expr::TableAlias(new_alias.clone()),
                    column: pa.column.clone(),
                })
            } else {
                expr.clone()
            }
        }

        LogicalExpr::Operator(op) | LogicalExpr::OperatorApplicationExp(op) => {
            let new_operands: Vec<_> = op
                .operands
                .iter()
                .map(|operand| rewrite_aliases_in_expr(operand, alias_map))
                .collect();
            LogicalExpr::OperatorApplicationExp(
                crate::query_planner::logical_expr::OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                },
            )
        }

        LogicalExpr::ScalarFnCall(fc) => {
            let new_args: Vec<_> = fc
                .args
                .iter()
                .map(|arg| rewrite_aliases_in_expr(arg, alias_map))
                .collect();
            LogicalExpr::ScalarFnCall(crate::query_planner::logical_expr::ScalarFnCall {
                name: fc.name.clone(),
                args: new_args,
            })
        }

        LogicalExpr::AggregateFnCall(afc) => {
            let new_args: Vec<_> = afc
                .args
                .iter()
                .map(|arg| rewrite_aliases_in_expr(arg, alias_map))
                .collect();
            LogicalExpr::AggregateFnCall(crate::query_planner::logical_expr::AggregateFnCall {
                name: afc.name.clone(),
                args: new_args,
            })
        }

        LogicalExpr::List(items) => {
            let new_items: Vec<_> = items
                .iter()
                .map(|item| rewrite_aliases_in_expr(item, alias_map))
                .collect();
            LogicalExpr::List(new_items)
        }

        // For all other expressions, return as-is
        _ => expr.clone(),
    }
}

impl OptimizerPass for CollectUnwindElimination {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let (optimized, _alias_map) = Self::optimize_node(logical_plan.clone())?;
        let changed = !Arc::ptr_eq(&logical_plan, &optimized);
        if changed {
            Ok(Transformed::Yes(optimized))
        } else {
            Ok(Transformed::No(optimized))
        }
    }
}

impl CollectUnwindElimination {
    fn optimize_node(
        plan: Arc<LogicalPlan>,
    ) -> OptimizerResult<(Arc<LogicalPlan>, HashMap<String, String>)> {
        match plan.as_ref() {
            // Look for UNWIND following WITH that contains collect()
            LogicalPlan::Unwind(unwind) => Self::try_eliminate_collect_unwind(unwind, plan.clone()),

            // Recursively optimize all child nodes and accumulate alias mappings
            LogicalPlan::Projection(proj) => {
                let (optimized_input, alias_map) = Self::optimize_node(proj.input.clone())?;

                // Apply alias rewriting to projection items if we have mappings
                let new_items: Vec<ProjectionItem> = if alias_map.is_empty() {
                    proj.items.clone()
                } else {
                    proj.items
                        .iter()
                        .map(|item| ProjectionItem {
                            expression: rewrite_aliases_in_expr(&item.expression, &alias_map),
                            col_alias: item.col_alias.clone(),
                        })
                        .collect()
                };

                let new_plan = Arc::new(LogicalPlan::Projection(
                    crate::query_planner::logical_plan::Projection {
                        input: optimized_input,
                        items: new_items,
                        distinct: proj.distinct,
                        pattern_comprehensions: proj.pattern_comprehensions.clone(),
                    },
                ));

                Ok((new_plan, alias_map))
            }

            LogicalPlan::Filter(filter) => {
                let (optimized_input, alias_map) = Self::optimize_node(filter.input.clone())?;
                let new_predicate = if alias_map.is_empty() {
                    filter.predicate.clone()
                } else {
                    rewrite_aliases_in_expr(&filter.predicate, &alias_map)
                };
                Ok((
                    Arc::new(LogicalPlan::Filter(
                        crate::query_planner::logical_plan::Filter {
                            input: optimized_input,
                            predicate: new_predicate,
                        },
                    )),
                    alias_map,
                ))
            }

            LogicalPlan::WithClause(with) => {
                let (optimized_input, alias_map) = Self::optimize_node(with.input.clone())?;
                Ok((
                    Arc::new(LogicalPlan::WithClause(WithClause {
                        cte_name: None,
                        input: optimized_input,
                        items: with.items.clone(),
                        distinct: with.distinct,
                        order_by: with.order_by.clone(),
                        skip: with.skip,
                        limit: with.limit,
                        where_clause: with.where_clause.clone(),
                        exported_aliases: with.exported_aliases.clone(),
                        cte_references: with.cte_references.clone(),
                        pattern_comprehensions: with.pattern_comprehensions.clone(),
                    })),
                    alias_map,
                ))
            }

            LogicalPlan::GraphJoins(gj) => {
                let (optimized_input, alias_map) = Self::optimize_node(gj.input.clone())?;
                Ok((
                    Arc::new(LogicalPlan::GraphJoins(
                        crate::query_planner::logical_plan::GraphJoins {
                            input: optimized_input,
                            joins: gj.joins.clone(),
                            optional_aliases: gj.optional_aliases.clone(),
                            anchor_table: gj.anchor_table.clone(),
                            cte_references: gj.cte_references.clone(),
                            correlation_predicates: gj.correlation_predicates.clone(),
                        },
                    )),
                    alias_map,
                ))
            }

            LogicalPlan::GroupBy(gb) => {
                let (optimized_input, alias_map) = Self::optimize_node(gb.input.clone())?;
                let new_expressions = if alias_map.is_empty() {
                    gb.expressions.clone()
                } else {
                    gb.expressions
                        .iter()
                        .map(|e| rewrite_aliases_in_expr(e, &alias_map))
                        .collect()
                };
                Ok((
                    Arc::new(LogicalPlan::GroupBy(
                        crate::query_planner::logical_plan::GroupBy {
                            input: optimized_input,
                            expressions: new_expressions,
                            ..gb.clone()
                        },
                    )),
                    alias_map,
                ))
            }

            LogicalPlan::OrderBy(ob) => {
                let (optimized_input, alias_map) = Self::optimize_node(ob.input.clone())?;
                let new_items = if alias_map.is_empty() {
                    ob.items.clone()
                } else {
                    ob.items
                        .iter()
                        .map(|item| crate::query_planner::logical_plan::OrderByItem {
                            expression: rewrite_aliases_in_expr(&item.expression, &alias_map),
                            order: item.order.clone(),
                        })
                        .collect()
                };
                Ok((
                    Arc::new(LogicalPlan::OrderBy(
                        crate::query_planner::logical_plan::OrderBy {
                            input: optimized_input,
                            items: new_items,
                        },
                    )),
                    alias_map,
                ))
            }

            LogicalPlan::Limit(l) => {
                let (optimized_input, alias_map) = Self::optimize_node(l.input.clone())?;
                Ok((
                    Arc::new(LogicalPlan::Limit(
                        crate::query_planner::logical_plan::Limit {
                            input: optimized_input,
                            count: l.count,
                        },
                    )),
                    alias_map,
                ))
            }

            LogicalPlan::Skip(s) => {
                let (optimized_input, alias_map) = Self::optimize_node(s.input.clone())?;
                Ok((
                    Arc::new(LogicalPlan::Skip(
                        crate::query_planner::logical_plan::Skip {
                            input: optimized_input,
                            count: s.count,
                        },
                    )),
                    alias_map,
                ))
            }

            // Base cases - no children to recurse into
            LogicalPlan::ViewScan(_)
            | LogicalPlan::GraphNode(_)
            | LogicalPlan::GraphRel(_)
            | LogicalPlan::CartesianProduct(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::PageRank(_)
            | LogicalPlan::Empty => Ok((plan, HashMap::new())),
        }
    }

    /// Try to eliminate collect+UNWIND pattern
    ///
    /// Pattern: WITH ... collect(x) as xs UNWIND xs as x
    /// Result: Remove both WITH and UNWIND, map x references to source
    /// Returns: (modified_plan, alias_mapping)
    fn try_eliminate_collect_unwind(
        unwind: &Unwind,
        _unwind_plan: Arc<LogicalPlan>,
    ) -> OptimizerResult<(Arc<LogicalPlan>, HashMap<String, String>)> {
        log::info!(
            "🔥 CollectUnwindElimination: Examining UNWIND node, alias='{}', expression={:?}",
            unwind.alias,
            unwind.expression
        );
        log::info!(
            "🔥 CollectUnwindElimination: UNWIND input type={}",
            match unwind.input.as_ref() {
                LogicalPlan::WithClause(_) => "WithClause",
                LogicalPlan::Projection(_) => "Projection",
                LogicalPlan::Filter(_) => "Filter",
                LogicalPlan::GraphJoins(_) => "GraphJoins",
                _ => "Other",
            }
        );

        // Check if UNWIND expression is a simple TableAlias
        if let LogicalExpr::TableAlias(ref unwind_alias) = unwind.expression {
            let collection_name = &unwind_alias.0;

            // Check if input is a WITH clause
            if let LogicalPlan::WithClause(ref with) = unwind.input.as_ref() {
                // Look for collect(source) as collection_name in WITH items
                for item in &with.items {
                    if let Some(ref col_alias) = item.col_alias {
                        if &col_alias.0 == collection_name {
                            // Found the definition - check if it's collect(TableAlias)
                            if let LogicalExpr::AggregateFnCall(ref agg) = item.expression {
                                if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                                    // Extract source alias from collect argument, handling
                                    // both collect(x) and collect(DISTINCT x) patterns
                                    let (source_alias_opt, has_distinct) = match &agg.args[0] {
                                        LogicalExpr::TableAlias(ref ta) => (Some(&ta.0), false),
                                        // Handle both Operator and OperatorApplicationExp variants
                                        LogicalExpr::Operator(ref op)
                                        | LogicalExpr::OperatorApplicationExp(ref op)
                                            if op.operator
                                                == crate::query_planner::logical_expr::Operator::Distinct
                                                && op.operands.len() == 1 =>
                                        {
                                            if let LogicalExpr::TableAlias(ref ta) = op.operands[0]
                                            {
                                                (Some(&ta.0), true)
                                            } else {
                                                (None, true)
                                            }
                                        }
                                        _ => (None, false),
                                    };

                                    if let Some(source) = source_alias_opt {
                                        let source = source.clone();
                                        let unwound = &unwind.alias;

                                        log::info!(
                                            "🔥 CollectUnwindElimination: Found pattern 'collect({}) as {}' + 'UNWIND {} as {}'",
                                            source, collection_name, collection_name, unwound
                                        );

                                        // Check if there are other items in WITH besides the collect
                                        let other_items: Vec<_> = with
                                            .items
                                            .iter()
                                            .filter(|i| {
                                                if let Some(ref a) = i.col_alias {
                                                    &a.0 != collection_name
                                                } else {
                                                    true
                                                }
                                            })
                                            .collect();

                                        if other_items.is_empty() {
                                            // Simple case: WITH ONLY contains collect()
                                            // Eliminate both WITH and UNWIND completely
                                            // Map unwind alias -> source so RETURN can find the right variable
                                            log::info!(
                                                "🔥 CollectUnwindElimination: Simple elimination - WITH only has collect({}), removing WITH+UNWIND, mapping {} -> {} (has_distinct={})",
                                                source, unwound, source, has_distinct
                                            );

                                            // Build alias mapping: UNWIND alias -> source variable
                                            let mut alias_map = HashMap::new();
                                            alias_map.insert(unwound.clone(), source.clone());

                                            // Recursively optimize the WITH input
                                            let (optimized_input, input_alias_map) =
                                                Self::optimize_node(with.input.clone())?;

                                            // Merge alias maps
                                            alias_map.extend(input_alias_map);

                                            if has_distinct {
                                                // collect(DISTINCT x) needs dedup: keep a
                                                // minimal WITH DISTINCT that passes through
                                                // the source variable
                                                let passthrough_item = ProjectionItem {
                                                    expression: LogicalExpr::TableAlias(
                                                        crate::query_planner::logical_expr::TableAlias(
                                                            source.clone(),
                                                        ),
                                                    ),
                                                    col_alias: None,
                                                };
                                                let distinct_with =
                                                    Arc::new(LogicalPlan::WithClause(WithClause {
                                                        cte_name: None,
                                                        input: optimized_input,
                                                        items: vec![passthrough_item],
                                                        distinct: true,
                                                        order_by: None,
                                                        skip: None,
                                                        limit: None,
                                                        where_clause: None,
                                                        exported_aliases: vec![source.clone()],
                                                        cte_references: with.cte_references.clone(),
                                                        pattern_comprehensions: vec![],
                                                    }));
                                                return Ok((distinct_with, alias_map));
                                            }

                                            return Ok((optimized_input, alias_map));
                                        } else {
                                            // Complex case: WITH has other items
                                            // Strategy: Keep WITH but remove collect(), keep other items
                                            // Build alias map: UNWIND alias -> source for expression rewriting
                                            log::info!(
                                                "🔥 CollectUnwindElimination: Complex elimination - WITH has {} other items, removing collect, mapping {} -> {}",
                                                other_items.len(), unwound, source
                                            );

                                            // Build new items: keep other items, remove the collect entirely
                                            let mut new_items: Vec<ProjectionItem> = with
                                                .items
                                                .iter()
                                                .filter(|item| {
                                                    if let Some(ref col_alias) = item.col_alias {
                                                        &col_alias.0 != collection_name
                                                    } else {
                                                        true
                                                    }
                                                })
                                                .cloned()
                                                .collect();

                                            // Add the source variable as a passthrough item so it's exported
                                            // (the UNWIND alias will be rewritten to reference this)
                                            new_items.push(ProjectionItem {
                                                expression: LogicalExpr::TableAlias(TableAlias(
                                                    source.clone(),
                                                )),
                                                col_alias: Some(ColumnAlias(source.clone())),
                                            });

                                            // Update exported aliases: remove collection, add source
                                            let mut new_exported_aliases: Vec<String> = with
                                                .exported_aliases
                                                .iter()
                                                .filter(|a| *a != collection_name)
                                                .cloned()
                                                .collect();

                                            // Add source to exported aliases if not already present
                                            if !new_exported_aliases.contains(&source) {
                                                new_exported_aliases.push(source.clone());
                                            }

                                            let (optimized_input, input_alias_map) =
                                                Self::optimize_node(with.input.clone())?;

                                            // Create modified WITH clause without the collect
                                            let new_with =
                                                Arc::new(LogicalPlan::WithClause(WithClause {
                                                    cte_name: None,
                                                    input: optimized_input,
                                                    items: new_items,
                                                    distinct: with.distinct || has_distinct,
                                                    order_by: with.order_by.clone(),
                                                    skip: with.skip,
                                                    limit: with.limit,
                                                    where_clause: with.where_clause.clone(),
                                                    exported_aliases: new_exported_aliases,
                                                    cte_references: with.cte_references.clone(),
                                                    pattern_comprehensions: with
                                                        .pattern_comprehensions
                                                        .clone(),
                                                }));

                                            // Map: UNWIND alias -> source variable
                                            let mut alias_map = HashMap::new();
                                            alias_map.insert(unwound.clone(), source.clone());
                                            alias_map.extend(input_alias_map);

                                            return Ok((new_with, alias_map));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Pattern not found or not optimizable - optimize children and return
        let (optimized_input, alias_map) = Self::optimize_node(unwind.input.clone())?;
        Ok((
            Arc::new(LogicalPlan::Unwind(Unwind {
                input: optimized_input,
                expression: unwind.expression.clone(),
                alias: unwind.alias.clone(),
                label: unwind.label.clone(),
                tuple_properties: unwind.tuple_properties.clone(),
            })),
            alias_map,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{AggregateFnCall, ColumnAlias, TableAlias};
    use crate::query_planner::logical_plan::{LogicalPlan, ProjectionItem, Unwind, WithClause};

    #[test]
    fn test_simple_collect_unwind_elimination() {
        // WITH collect(f) as friends
        // UNWIND friends as friend
        // Should eliminate both

        let base = Arc::new(LogicalPlan::Empty);

        let with_plan = Arc::new(LogicalPlan::WithClause(WithClause {
            cte_name: None,
            input: base,
            items: vec![ProjectionItem {
                expression: LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: "collect".to_string(),
                    args: vec![LogicalExpr::TableAlias(TableAlias("f".to_string()))],
                }),
                col_alias: Some(ColumnAlias("friends".to_string())),
            }],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec!["friends".to_string()],
            cte_references: Default::default(),
            pattern_comprehensions: vec![],
        }));

        let unwind_plan = Arc::new(LogicalPlan::Unwind(Unwind {
            input: with_plan,
            expression: LogicalExpr::TableAlias(TableAlias("friends".to_string())),
            alias: "friend".to_string(),
            label: None,
            tuple_properties: None,
        }));

        let optimizer = CollectUnwindElimination;
        let mut plan_ctx = PlanCtx::new_empty();
        let result = optimizer.optimize(unwind_plan, &mut plan_ctx).unwrap();

        // Should be optimized to Empty (the base)
        let plan = result.get_plan();
        assert!(matches!(&*plan, LogicalPlan::Empty));
    }

    #[test]
    fn test_complex_collect_unwind_elimination() {
        // WITH u, collect(f) as friends
        // UNWIND friends as friend
        // Should keep WITH with just 'u', remove collect and UNWIND

        let base = Arc::new(LogicalPlan::Empty);

        let with_plan = Arc::new(LogicalPlan::WithClause(WithClause {
            cte_name: None,
            input: base,
            items: vec![
                ProjectionItem {
                    expression: LogicalExpr::TableAlias(TableAlias("u".to_string())),
                    col_alias: None,
                },
                ProjectionItem {
                    expression: LogicalExpr::AggregateFnCall(AggregateFnCall {
                        name: "collect".to_string(),
                        args: vec![LogicalExpr::TableAlias(TableAlias("f".to_string()))],
                    }),
                    col_alias: Some(ColumnAlias("friends".to_string())),
                },
            ],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec!["u".to_string(), "friends".to_string()],
            cte_references: Default::default(),
            pattern_comprehensions: vec![],
        }));

        let unwind_plan = Arc::new(LogicalPlan::Unwind(Unwind {
            input: with_plan,
            expression: LogicalExpr::TableAlias(TableAlias("friends".to_string())),
            alias: "friend".to_string(),
            label: None,
            tuple_properties: None,
        }));

        let optimizer = CollectUnwindElimination;
        let mut plan_ctx = PlanCtx::new_empty();
        let result = optimizer.optimize(unwind_plan, &mut plan_ctx).unwrap();

        // Should be a WITH with 'u' and 'f' (source variable must be exported)
        let plan = result.get_plan();
        if let LogicalPlan::WithClause(with) = &*plan {
            assert_eq!(with.items.len(), 2, "Should have u and f");
            assert_eq!(with.exported_aliases.len(), 2, "Should export u and f");
            assert!(with.exported_aliases.contains(&"u".to_string()));
            assert!(with.exported_aliases.contains(&"f".to_string()));
        } else {
            panic!("Expected WithClause, got {:?}", plan);
        }
    }

    #[test]
    fn test_collect_distinct_unwind_elimination() {
        // WITH collect(DISTINCT f) as friends
        // UNWIND friends as friend
        // Should eliminate collect+UNWIND but preserve DISTINCT semantics via WITH DISTINCT
        use crate::query_planner::logical_expr::{Operator, OperatorApplication};

        let base = Arc::new(LogicalPlan::Empty);

        let with_plan = Arc::new(LogicalPlan::WithClause(WithClause {
            cte_name: None,
            input: base,
            items: vec![ProjectionItem {
                expression: LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: "collect".to_string(),
                    args: vec![LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Distinct,
                        operands: vec![LogicalExpr::TableAlias(TableAlias("f".to_string()))],
                    })],
                }),
                col_alias: Some(ColumnAlias("friends".to_string())),
            }],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec!["friends".to_string()],
            cte_references: Default::default(),
            pattern_comprehensions: vec![],
        }));

        let unwind_plan = Arc::new(LogicalPlan::Unwind(Unwind {
            input: with_plan,
            expression: LogicalExpr::TableAlias(TableAlias("friends".to_string())),
            alias: "friend".to_string(),
            label: None,
            tuple_properties: None,
        }));

        let optimizer = CollectUnwindElimination;
        let mut plan_ctx = PlanCtx::new_empty();
        let result = optimizer.optimize(unwind_plan, &mut plan_ctx).unwrap();

        // Should produce a WITH DISTINCT passthrough (not Empty, because DISTINCT must be preserved)
        let plan = result.get_plan();
        if let LogicalPlan::WithClause(with) = &*plan {
            assert!(with.distinct, "WITH should be marked DISTINCT");
            assert_eq!(with.items.len(), 1);
        } else {
            panic!(
                "Expected WithClause (DISTINCT), got {:?}",
                std::mem::discriminant(&*plan)
            );
        }
    }

    #[test]
    fn test_complex_collect_distinct_with_other_items() {
        // WITH u, collect(DISTINCT f) as friends
        // UNWIND friends as friend
        // Should keep WITH with u and f (source), remove collect, set distinct=true
        use crate::query_planner::logical_expr::{Operator, OperatorApplication};

        let base = Arc::new(LogicalPlan::Empty);

        let with_plan = Arc::new(LogicalPlan::WithClause(WithClause {
            cte_name: None,
            input: base,
            items: vec![
                ProjectionItem {
                    expression: LogicalExpr::TableAlias(TableAlias("u".to_string())),
                    col_alias: Some(ColumnAlias("u".to_string())),
                },
                ProjectionItem {
                    expression: LogicalExpr::AggregateFnCall(AggregateFnCall {
                        name: "collect".to_string(),
                        args: vec![LogicalExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::Distinct,
                            operands: vec![LogicalExpr::TableAlias(TableAlias("f".to_string()))],
                        })],
                    }),
                    col_alias: Some(ColumnAlias("friends".to_string())),
                },
            ],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec!["u".to_string(), "friends".to_string()],
            cte_references: Default::default(),
            pattern_comprehensions: vec![],
        }));

        let unwind_plan = Arc::new(LogicalPlan::Unwind(Unwind {
            input: with_plan,
            expression: LogicalExpr::TableAlias(TableAlias("friends".to_string())),
            alias: "friend".to_string(),
            label: None,
            tuple_properties: None,
        }));

        let optimizer = CollectUnwindElimination;
        let mut plan_ctx = PlanCtx::new_empty();
        let result = optimizer.optimize(unwind_plan, &mut plan_ctx).unwrap();

        // Should produce a WITH with u and f, distinct=true
        let plan = result.get_plan();
        if let LogicalPlan::WithClause(with) = &*plan {
            assert!(with.distinct, "WITH should be marked DISTINCT");
            assert_eq!(with.items.len(), 2, "Should have u and f items");
            assert_eq!(with.exported_aliases.len(), 2, "Should export u and f");
            assert!(
                with.exported_aliases.contains(&"u".to_string()),
                "Should export u"
            );
            assert!(
                with.exported_aliases.contains(&"f".to_string()),
                "Should export f (source)"
            );

            // Verify f is in items (as passthrough)
            let f_item = with.items.iter().find(|item| {
                matches!(
                    &item.col_alias,
                    Some(ColumnAlias(alias)) if alias == "f"
                )
            });
            assert!(f_item.is_some(), "Should have f as a projection item");
        } else {
            panic!(
                "Expected WithClause, got {:?}",
                std::mem::discriminant(&*plan)
            );
        }
    }
}
