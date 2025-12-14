use std::{collections::HashSet, sync::Arc};

use crate::query_planner::{
    analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
    logical_plan::{LogicalPlan, Unwind},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct DuplicateScansRemoving;

impl AnalyzerPass for DuplicateScansRemoving {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let mut traversed: HashSet<String> = HashSet::new();
        Self::remove_duplicate_scans(logical_plan, &mut traversed, plan_ctx)
    }
}

impl DuplicateScansRemoving {
    pub fn new() -> Self {
        DuplicateScansRemoving
    }

    fn remove_duplicate_scans(
        logical_plan: Arc<LogicalPlan>,
        traversed: &mut HashSet<String>,
        plan_ctx: &PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::ViewScan(_scan) => {
                // ViewScans are leaf nodes, no transformation needed
                Transformed::No(logical_plan.clone())
            }
            LogicalPlan::Projection(projection) => {
                let child_tf =
                    Self::remove_duplicate_scans(projection.input.clone(), traversed, plan_ctx)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphNode(graph_node) => {
                traversed.insert(graph_node.alias.clone());

                let child_tf =
                    Self::remove_duplicate_scans(graph_node.input.clone(), traversed, plan_ctx)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let right_tf =
                    Self::remove_duplicate_scans(graph_rel.right.clone(), traversed, plan_ctx)?;
                let center_tf =
                    Self::remove_duplicate_scans(graph_rel.center.clone(), traversed, plan_ctx)?;

                let left_alias = &graph_rel.left_connection;

                log::debug!(
                    "DuplicateScansRemoving: Processing GraphRel, left_alias='{}', traversed contains it: {}",
                    left_alias,
                    traversed.contains(left_alias)
                );

                let left_tf = if traversed.contains(left_alias) {
                    let is_optional = plan_ctx.is_optional(left_alias);
                    log::debug!(
                        "DuplicateScansRemoving: left_alias='{}' is in traversed set, is_optional={}",
                        left_alias,
                        is_optional
                    );

                    // NEW: Check if this alias is optional before removing
                    if is_optional {
                        log::debug!(
                            "DuplicateScansRemoving: Keeping left node for OPTIONAL MATCH (alias='{}')",
                            left_alias
                        );
                        // Keep the node for OPTIONAL MATCH JOIN generation
                        Self::remove_duplicate_scans(graph_rel.left.clone(), traversed, plan_ctx)?
                    } else {
                        log::debug!(
                            "DuplicateScansRemoving: Removing duplicate left node (alias='{}')",
                            left_alias
                        );
                        // Remove duplicate for regular MATCH
                        Transformed::Yes(Arc::new(LogicalPlan::Empty))
                    }
                } else {
                    Self::remove_duplicate_scans(graph_rel.left.clone(), traversed, plan_ctx)?
                };

                // let left_tf = Self::remove_duplicate_scans(graph_rel.left.clone(), traversed);

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf =
                    Self::remove_duplicate_scans(cte.input.clone(), traversed, plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf =
                    Self::remove_duplicate_scans(graph_joins.input.clone(), traversed, plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    Self::remove_duplicate_scans(filter.input.clone(), traversed, plan_ctx)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf =
                    Self::remove_duplicate_scans(group_by.input.clone(), traversed, plan_ctx)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf =
                    Self::remove_duplicate_scans(order_by.input.clone(), traversed, plan_ctx)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    Self::remove_duplicate_scans(skip.input.clone(), traversed, plan_ctx)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    Self::remove_duplicate_scans(limit.input.clone(), traversed, plan_ctx)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf =
                        Self::remove_duplicate_scans(input_plan.clone(), traversed, plan_ctx)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => {
                // PageRank is a leaf node, no transformation needed
                Transformed::No(logical_plan.clone())
            }
            LogicalPlan::Unwind(u) => {
                let child_tf = Self::remove_duplicate_scans(u.input.clone(), traversed, plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        Transformed::Yes(Arc::new(LogicalPlan::Unwind(Unwind {
                            input: new_input,
                            expression: u.expression.clone(),
                            alias: u.alias.clone(),
                        })))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                let transformed_left =
                    Self::remove_duplicate_scans(cp.left.clone(), traversed, plan_ctx)?;
                let transformed_right =
                    Self::remove_duplicate_scans(cp.right.clone(), traversed, plan_ctx)?;

                if matches!(
                    (&transformed_left, &transformed_right),
                    (Transformed::No(_), Transformed::No(_))
                ) {
                    Transformed::No(logical_plan.clone())
                } else {
                    let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                        left: match transformed_left {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        right: match transformed_right {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    };
                    Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(new_cp)))
                }
            }
            LogicalPlan::WithClause(with_clause) => {
                // WithClause is a boundary - transform its input independently
                let child_tf =
                    Self::remove_duplicate_scans(with_clause.input.clone(), traversed, plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        let new_with = crate::query_planner::logical_plan::WithClause {
                            input: new_input,
                            items: with_clause.items.clone(),
                            distinct: with_clause.distinct,
                            order_by: with_clause.order_by.clone(),
                            skip: with_clause.skip,
                            limit: with_clause.limit,
                            where_clause: with_clause.where_clause.clone(),
                            exported_aliases: with_clause.exported_aliases.clone(),
                            cte_references: with_clause.cte_references.clone(),
                        };
                        Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_with)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
        };
        Ok(transformed_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{
        Direction, Literal, LogicalExpr, Operator, OperatorApplication,
    };
    use crate::query_planner::logical_plan::{
        Filter, GraphNode, GraphRel, LogicalPlan, Projection, ProjectionItem, Scan,
    };

    // helper functions
    fn create_scan(alias: Option<String>, table_name: Option<String>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Scan(Scan {
            table_alias: alias.map(|s| s.to_string()),
            table_name: table_name.map(|s| s.to_string()),
        }))
    }

    fn create_graph_node(alias: &str, input: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input,
            alias: alias.to_string(),
            label: None,
            is_denormalized: false,
            projected_columns: None,
        }))
    }

    fn create_graph_rel(
        left: Arc<LogicalPlan>,
        center: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        alias: &str,
        left_connection: &str,
        right_connection: &str,
        direction: Direction,
    ) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphRel(GraphRel {
            left,
            center,
            right,
            alias: alias.to_string(),
            direction,
            left_connection: left_connection.to_string(),
            right_connection: right_connection.to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None, // Will be populated by filter pushdown
            labels: None,
            is_optional: None,
            anchor_connection: None,
        }))
    }

    #[test]
    fn test_complex_nested_plan_with_duplicates() {
        let analyzer = DuplicateScansRemoving::new();
        let mut plan_ctx = PlanCtx::default();

        // Create a complex plan: Projection -> Filter -> GraphRel with duplicate detection

        let left_user_scan = create_scan(Some("user".to_string()), Some("users".to_string()));
        let left_user_node = create_graph_node("user", left_user_scan);

        let center_scan = create_scan(
            Some("follows".to_string()),
            Some("follows_table".to_string()),
        );

        let right_user_scan = create_scan(Some("user".to_string()), Some("users".to_string())); // Duplicate of left
        let right_user_node = create_graph_node("user", right_user_scan);

        let graph_rel = create_graph_rel(
            right_user_node, // This should be replaced with Empty
            center_scan,
            left_user_node, // This traverses "user" first
            "follows",
            "user", // left_connection matches traversed alias
            "user", // right_connection
            Direction::Either,
        );

        // Wrap in Filter
        let filter = Arc::new(LogicalPlan::Filter(Filter {
            input: graph_rel,
            predicate: LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![
                    LogicalExpr::Literal(Literal::Integer(1)),
                    LogicalExpr::Literal(Literal::Integer(0)),
                ],
            }),
        }));

        // Wrap in Projection
        let projection = Arc::new(LogicalPlan::Projection(Projection {
            input: filter,
            items: vec![ProjectionItem {
                expression: LogicalExpr::Literal(Literal::String("test".to_string())),
                col_alias: None,
            }],
            distinct: false,
        }));

        let result = analyzer.analyze(projection, &mut plan_ctx).unwrap();

        match result {
            Transformed::Yes(transformed_plan) => {
                match transformed_plan.as_ref() {
                    LogicalPlan::Projection(proj) => {
                        match proj.input.as_ref() {
                            LogicalPlan::Filter(filter) => {
                                match filter.input.as_ref() {
                                    LogicalPlan::GraphRel(rel) => {
                                        // Left side should be Empty due to duplicate
                                        match rel.left.as_ref() {
                                            LogicalPlan::Empty => (), // Expected
                                            _ => panic!(
                                                "Expected left side to be Empty due to duplicate"
                                            ),
                                        }
                                    }
                                    _ => panic!("Expected GraphRel in filter"),
                                }
                            }
                            _ => panic!("Expected Filter in projection"),
                        }
                    }
                    _ => panic!("Expected Projection at top"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_traversed_set_tracking() {
        let analyzer = DuplicateScansRemoving::new();
        let mut plan_ctx = PlanCtx::default();

        // Create a plan that will exercise the traversed set logic
        // Structure: (userA)-[rel1]->(userB)-[rel2]->(userA)
        // The second userA should be detected as duplicate

        let user_a_scan = create_scan(Some("userA".to_string()), Some("users".to_string()));
        let user_a_node = create_graph_node("userA", user_a_scan);

        let user_b_scan = create_scan(Some("userB".to_string()), Some("users".to_string()));
        let user_b_node = create_graph_node("userB", user_b_scan);

        let rel1_scan = create_scan(Some("follows".to_string()), Some("follows".to_string()));

        // First relationship: (userA)-[follows]->(userB)
        let first_rel = create_graph_rel(
            user_a_node,
            rel1_scan,
            user_b_node,
            "follows",
            "userA",
            "userB",
            Direction::Outgoing,
        );

        // Second relationship involving userA again (should detect duplicate)
        let duplicate_user_a_scan =
            create_scan(Some("userA".to_string()), Some("users".to_string()));
        let duplicate_user_a_node = create_graph_node("userA", duplicate_user_a_scan);

        let rel2_scan = create_scan(Some("likes".to_string()), Some("likes".to_string()));

        let second_rel = create_graph_rel(
            duplicate_user_a_node, // This should become Empty
            rel2_scan,
            first_rel, // This registers userA and userB
            "likes",
            "userA", // Duplicate connection
            "userB",
            Direction::Either,
        );

        let result = analyzer.analyze(second_rel, &mut plan_ctx).unwrap();

        match result {
            Transformed::Yes(transformed_plan) => {
                match transformed_plan.as_ref() {
                    LogicalPlan::GraphRel(rel) => {
                        // Left side should be Empty due to userA duplicate
                        assert!(matches!(rel.left.as_ref(), LogicalPlan::Empty));
                        assert_eq!(rel.left_connection, "userA");
                    }
                    _ => panic!("Expected GraphRel"),
                }
            }
            _ => panic!("Expected transformation due to duplicate userA"),
        }
    }
}
