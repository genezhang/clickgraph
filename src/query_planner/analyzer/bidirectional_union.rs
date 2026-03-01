//! Bidirectional Pattern to UNION ALL Transformation
//!
//! This optimizer pass transforms undirected relationship patterns `(a)-[r]-(b)`
//! from a single GraphRel with Direction::Either into a Union of two directed patterns:
//! - One for outgoing: (a)-[r]->(b)
//! - One for incoming: (a)<-[r]-(b)
//!
//! For multi-hop patterns like `(a)-[r1]-(b)-[r2]-(c)`, we generate the full cartesian
//! product of directions (2^n combinations for n undirected edges):
//! - (a)-[r1]->(b)-[r2]->(c)
//! - (a)-[r1]->(b)<-[r2]-(c)
//! - (a)<-[r1]-(b)-[r2]->(c)
//! - (a)<-[r1]-(b)<-[r2]-(c)
//!
//! This is necessary because ClickHouse doesn't handle OR conditions in JOINs correctly,
//! leading to missing rows. UNION ALL ensures all direction combinations are properly matched.

use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::GraphSchema;
use crate::query_planner::analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult};
use crate::query_planner::logical_expr::{
    Direction, LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias,
};
use crate::query_planner::logical_plan::{
    Filter, GraphNode, GraphRel, GroupBy, LogicalPlan, Projection, ProjectionItem, Union, UnionType,
};
use crate::query_planner::plan_ctx::PlanCtx;
use crate::query_planner::transformed::Transformed;

pub struct BidirectionalUnion;

impl AnalyzerPass for BidirectionalUnion {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        transform_bidirectional(&logical_plan, plan_ctx, graph_schema)
    }
}

fn transform_bidirectional(
    plan: &Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
    graph_schema: &GraphSchema,
) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
    match plan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            // Collect all undirected edges in this path to handle multi-hop patterns correctly
            let undirected_count = count_undirected_edges(plan);

            if undirected_count > 0 {
                // Skip Union split for VLP multi-type patterns — the VLP CTE already
                // generates UNION ALL branches for both directions internally.
                // Splitting here creates duplicate results and a second CTE with wrong columns.
                let is_vlp_multi_type = graph_rel.variable_length.is_some()
                    && graph_rel.labels.as_ref().is_some_and(|l| l.len() > 1);
                if is_vlp_multi_type {
                    crate::debug_print!(
                        "🔄 BidirectionalUnion: VLP multi-type pattern detected ({} labels), skipping Union split — CTE handles both directions",
                        graph_rel.labels.as_ref().unwrap().len()
                    );
                    // Mark as undirected but keep Outgoing direction — VLP CTE handles both
                    let new_graph_rel = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: graph_rel.left.clone(),
                        center: graph_rel.center.clone(),
                        right: graph_rel.right.clone(),
                        alias: graph_rel.alias.clone(),
                        direction: Direction::Outgoing,
                        left_connection: graph_rel.left_connection.clone(),
                        right_connection: graph_rel.right_connection.clone(),
                        is_rel_anchor: graph_rel.is_rel_anchor,
                        variable_length: graph_rel.variable_length.clone(),
                        shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                        path_variable: graph_rel.path_variable.clone(),
                        where_predicate: graph_rel.where_predicate.clone(),
                        labels: graph_rel.labels.clone(),
                        is_optional: graph_rel.is_optional,
                        anchor_connection: graph_rel.anchor_connection.clone(),
                        cte_references: graph_rel.cte_references.clone(),
                        pattern_combinations: graph_rel.pattern_combinations.clone(),
                        was_undirected: Some(true),
                    }));
                    return Ok(Transformed::Yes(new_graph_rel));
                }

                // NOTE: Previously skipped Union split for nested undirected edges,
                // but this left Direction::Either unhandled (OR fallback was never
                // implemented in GraphJoinInference path). The Incoming branch swap
                // correctly restructures the chain — left/right subtrees swap
                // and connections swap, maintaining valid FROM/JOIN ordering.

                crate::debug_print!(
                    "🔄 BidirectionalUnion: Found {} undirected edge(s) in path, generating {} UNION branches",
                    undirected_count,
                    1 << undirected_count  // 2^n
                );

                // Generate all 2^n direction combinations
                let branches =
                    generate_direction_combinations(plan, undirected_count, graph_schema);

                if branches.len() == 1 {
                    // Only one branch (shouldn't happen if undirected_count > 0, but handle it)
                    return Ok(Transformed::Yes(
                        branches
                            .into_iter()
                            .next()
                            .expect("Vector with len==1 must have element"),
                    ));
                }

                // Create Union of all branches
                let union = Union {
                    inputs: branches,
                    union_type: UnionType::All,
                };

                crate::debug_print!(
                    "🔄 BidirectionalUnion: Created UNION ALL with {} branches for multi-hop pattern",
                    union.inputs.len()
                );

                Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(union))))
            } else {
                // No undirected edges, just recurse into children (they might have undirected patterns)
                let transformed_left =
                    transform_bidirectional(&graph_rel.left, plan_ctx, graph_schema)?;
                let transformed_center =
                    transform_bidirectional(&graph_rel.center, plan_ctx, graph_schema)?;
                let transformed_right =
                    transform_bidirectional(&graph_rel.right, plan_ctx, graph_schema)?;

                if matches!(
                    (&transformed_left, &transformed_center, &transformed_right),
                    (Transformed::No(_), Transformed::No(_), Transformed::No(_))
                ) {
                    Ok(Transformed::No(plan.clone()))
                } else {
                    let new_rel = GraphRel {
                        left: match transformed_left {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        center: match transformed_center {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        right: match transformed_right {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        },
                        ..graph_rel.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                }
            }
        }

        LogicalPlan::Projection(proj) => {
            // Check if there are undirected edges in the input
            // If so, we need to handle the Projection AND the GraphRel together
            // so that column swaps are properly applied to the projection items
            let undirected_count = count_undirected_edges(&proj.input);

            if undirected_count > 0 {
                // Skip Union split for VLP multi-type patterns — the VLP CTE already
                // handles both directions internally
                if is_vlp_multi_type_subtree(&proj.input) {
                    crate::debug_print!(
                        "🔄 BidirectionalUnion: VLP multi-type in Projection subtree, skipping Union split"
                    );
                    let transformed = transform_bidirectional(&proj.input, plan_ctx, graph_schema)?;
                    let new_input = match transformed {
                        Transformed::Yes(p) => p,
                        Transformed::No(p) => p,
                    };
                    let new_proj = Projection {
                        input: new_input,
                        items: proj.items.clone(),
                        distinct: proj.distinct,
                        pattern_comprehensions: proj.pattern_comprehensions.clone(),
                    };
                    return Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                        new_proj,
                    ))));
                }

                // Skip Union split for nested undirected edges (same as GraphRel case)
                if has_nested_undirected_edge(&proj.input) {
                    crate::debug_print!(
                        "🔄 BidirectionalUnion: Nested undirected edge in Projection subtree, skipping Union split"
                    );
                    return Ok(Transformed::No(plan.clone()));
                }

                crate::debug_print!(
                    "🔄 BidirectionalUnion: Found {} undirected edge(s) in Projection input, generating {} UNION branches",
                    undirected_count,
                    1 << undirected_count
                );

                // Generate direction combinations for the ENTIRE Projection(GraphRel) tree
                // This ensures column swaps are applied to the projection items
                let branches =
                    generate_direction_combinations(plan, undirected_count, graph_schema);

                if branches.len() == 1 {
                    return Ok(Transformed::Yes(
                        branches
                            .into_iter()
                            .next()
                            .expect("Vector with len==1 must have element"),
                    ));
                }

                // 🔧 FIX: Use UNION DISTINCT when Projection has DISTINCT
                // This ensures deduplication across bidirectional branches
                let union_type = if proj.distinct {
                    crate::debug_print!(
                        "🔄 BidirectionalUnion: Using UNION DISTINCT (Projection has DISTINCT)"
                    );
                    UnionType::Distinct
                } else {
                    UnionType::All
                };

                let union = Union {
                    inputs: branches,
                    union_type,
                };

                crate::debug_print!(
                    "🔄 BidirectionalUnion: Created UNION {:?} with {} branches (with column swaps)",
                    union.union_type,
                    union.inputs.len()
                );

                Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(union))))
            } else {
                // No undirected edges, just recurse normally
                let transformed = transform_bidirectional(&proj.input, plan_ctx, graph_schema)?;
                match transformed {
                    Transformed::Yes(new_input) => {
                        let new_proj = Projection {
                            input: new_input,
                            items: proj.items.clone(),
                            distinct: proj.distinct,
                            pattern_comprehensions: proj.pattern_comprehensions.clone(),
                        };
                        Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                            new_proj,
                        ))))
                    }
                    Transformed::No(_) => Ok(Transformed::No(plan.clone())),
                }
            }
        }

        LogicalPlan::Filter(filter) => {
            let transformed = transform_bidirectional(&filter.input, plan_ctx, graph_schema)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_filter = Filter {
                        input: new_input,
                        predicate: filter.predicate.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::Union(union) => {
            let mut any_transformed = false;
            let new_inputs: Vec<Arc<LogicalPlan>> = union
                .inputs
                .iter()
                .map(|input| {
                    let result = transform_bidirectional(input, plan_ctx, graph_schema);
                    match result {
                        Ok(Transformed::Yes(new_plan)) => {
                            any_transformed = true;
                            new_plan
                        }
                        Ok(Transformed::No(plan)) => plan,
                        Err(_) => input.clone(), // On error, keep original
                    }
                })
                .collect();

            if any_transformed {
                Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(Union {
                    inputs: new_inputs,
                    union_type: union.union_type.clone(),
                }))))
            } else {
                Ok(Transformed::No(plan.clone()))
            }
        }

        LogicalPlan::Limit(limit) => {
            let transformed = transform_bidirectional(&limit.input, plan_ctx, graph_schema)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_limit = crate::query_planner::logical_plan::Limit {
                        input: new_input,
                        count: limit.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::OrderBy(order_by) => {
            let transformed = transform_bidirectional(&order_by.input, plan_ctx, graph_schema)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_order_by = crate::query_planner::logical_plan::OrderBy {
                        input: new_input,
                        items: order_by.items.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::OrderBy(
                        new_order_by,
                    ))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::Skip(skip) => {
            let transformed = transform_bidirectional(&skip.input, plan_ctx, graph_schema)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_skip = crate::query_planner::logical_plan::Skip {
                        input: new_input,
                        count: skip.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Skip(new_skip))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::GraphNode(graph_node) => {
            let transformed = transform_bidirectional(&graph_node.input, plan_ctx, graph_schema)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_node = GraphNode {
                        input: new_input,
                        alias: graph_node.alias.clone(),
                        label: graph_node.label.clone(),
                        is_denormalized: graph_node.is_denormalized,
                        projected_columns: None,
                        node_types: graph_node.node_types.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(new_node))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::GroupBy(group_by) => {
            // Transform bidirectional patterns inside GroupBy
            let transformed = transform_bidirectional(&group_by.input, plan_ctx, graph_schema)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_group_by = GroupBy {
                        input: new_input,
                        expressions: group_by.expressions.clone(),
                        having_clause: group_by.having_clause.clone(),
                        is_materialization_boundary: group_by.is_materialization_boundary,
                        exposed_alias: group_by.exposed_alias.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(
                        new_group_by,
                    ))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::Unwind(unwind) => {
            let transformed = transform_bidirectional(&unwind.input, plan_ctx, graph_schema)?;
            match transformed {
                Transformed::Yes(new_input) => {
                    let new_unwind = crate::query_planner::logical_plan::Unwind {
                        input: new_input,
                        expression: unwind.expression.clone(),
                        alias: unwind.alias.clone(),
                        label: unwind.label.clone(),
                        tuple_properties: unwind.tuple_properties.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Unwind(new_unwind))))
                }
                Transformed::No(_) => Ok(Transformed::No(plan.clone())),
            }
        }

        LogicalPlan::CartesianProduct(cp) => {
            let transformed_left = transform_bidirectional(&cp.left, plan_ctx, graph_schema)?;
            let transformed_right = transform_bidirectional(&cp.right, plan_ctx, graph_schema)?;

            if matches!(
                (&transformed_left, &transformed_right),
                (Transformed::No(_), Transformed::No(_))
            ) {
                Ok(Transformed::No(plan.clone()))
            } else {
                let new_left = match transformed_left {
                    Transformed::Yes(p) => p,
                    Transformed::No(p) => p,
                };
                let new_right = match transformed_right {
                    Transformed::Yes(p) => p,
                    Transformed::No(p) => p,
                };

                // Collapse leaf undirected Unions inside CartesianProduct.
                // When both endpoints of an undirected edge are bound by other patterns
                // in the CP, the edge is a filter (existence check), not a traversal.
                // Both UNION branches produce identical results — one direction suffices.
                let collapsed_left = collapse_leaf_unions_in_cp(new_left);
                let collapsed_right = collapse_leaf_unions_in_cp(new_right);

                let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                    left: collapsed_left,
                    right: collapsed_right,
                    is_optional: cp.is_optional,
                    join_condition: cp.join_condition.clone(),
                };
                Ok(Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(
                    new_cp,
                ))))
            }
        }

        // Leaf nodes - no transformation needed
        LogicalPlan::ViewScan(_)
        | LogicalPlan::Empty
        | LogicalPlan::PageRank(_)
        | LogicalPlan::GraphJoins(_)
        | LogicalPlan::Cte(_) => Ok(Transformed::No(plan.clone())),

        // WithClause is a BOUNDARY - transform its input independently, don't propagate Union beyond
        LogicalPlan::WithClause(with_clause) => {
            crate::debug_print!(
                "🔄 BidirectionalUnion: Processing WithClause boundary (exports: {:?})",
                with_clause.exported_aliases
            );

            // Transform only the input (the query segment BEFORE this WITH)
            // Any bidirectional patterns in the input will be expanded to Union WITHIN this scope
            let transformed_input =
                transform_bidirectional(&with_clause.input, plan_ctx, graph_schema)?;

            match transformed_input {
                Transformed::Yes(new_input) => {
                    // The input was transformed (may now be a Union)
                    // Collapse leaf undirected Unions where both endpoints are bound
                    // by other patterns (e.g., multi-pattern MATCH with undirected KNOWS)
                    let new_input = collapse_leaf_unions_in_cp(new_input);

                    // Wrap it in WithClause - the Union stays INSIDE the WITH boundary
                    let new_with = crate::query_planner::logical_plan::WithClause {
                        cte_name: with_clause.cte_name.clone(), // PRESERVE from CteSchemaResolver
                        input: new_input,
                        items: with_clause.items.clone(),
                        distinct: with_clause.distinct,
                        order_by: with_clause.order_by.clone(),
                        skip: with_clause.skip,
                        limit: with_clause.limit,
                        where_clause: with_clause.where_clause.clone(),
                        exported_aliases: with_clause.exported_aliases.clone(),
                        cte_references: with_clause.cte_references.clone(),
                        pattern_comprehensions: with_clause.pattern_comprehensions.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(
                        new_with,
                    ))))
                }
                Transformed::No(_) => {
                    // No transformation needed
                    Ok(Transformed::No(plan.clone()))
                }
            }
        }
    }
}

/// Check if a plan is a Union from an undirected edge where both endpoints
/// are already bound by patterns in the left subtree. When both endpoints
/// are bound, the edge is a filter (existence check) — both Union branches
/// produce identical results, so one direction suffices.
///
/// This handles multi-pattern MATCH like:
///   MATCH (p1chain), (p2chain), (person1)-[:KNOWS]-(person2)
/// where person1 and person2 are already bound by p1chain and p2chain.
fn is_redundant_undirected_union(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Union(u) = plan {
        if u.inputs.len() == 2 {
            // Unwrap Filter from first branch (relationship uniqueness filter)
            let first_inner = match u.inputs[0].as_ref() {
                LogicalPlan::Filter(f) => f.input.as_ref(),
                other => other,
            };
            // Check if first branch is a GraphRel where both endpoints exist in left subtree
            if let LogicalPlan::GraphRel(gr) = first_inner {
                let left_conn = &gr.left_connection;
                let right_conn = &gr.right_connection;
                let left_has_both = has_alias_in_plan(&gr.left, left_conn)
                    && has_alias_in_plan(&gr.left, right_conn);
                if left_has_both {
                    crate::debug_print!(
                        "🔄 BidirectionalUnion: Redundant Union detected — both endpoints '{}' and '{}' bound in left subtree",
                        left_conn, right_conn
                    );
                    return true;
                }
            }
        }
    }
    false
}

/// Check if a plan tree contains an alias as a node or connection.
fn has_alias_in_plan(plan: &LogicalPlan, alias: &str) -> bool {
    match plan {
        LogicalPlan::GraphNode(gn) => gn.alias == alias || has_alias_in_plan(&gn.input, alias),
        LogicalPlan::GraphRel(gr) => {
            gr.left_connection == alias
                || gr.right_connection == alias
                || has_alias_in_plan(&gr.left, alias)
                || has_alias_in_plan(&gr.right, alias)
        }
        LogicalPlan::CartesianProduct(cp) => {
            has_alias_in_plan(&cp.left, alias) || has_alias_in_plan(&cp.right, alias)
        }
        LogicalPlan::Filter(f) => has_alias_in_plan(&f.input, alias),
        LogicalPlan::Projection(p) => has_alias_in_plan(&p.input, alias),
        _ => false,
    }
}

/// Recursively collapse leaf undirected Unions.
/// When both endpoints of an undirected edge are bound by other patterns,
/// the UNION is redundant — take just the Outgoing (first) branch.
/// Recurses through CartesianProduct and Filter wrappers.
fn collapse_leaf_unions_in_cp(plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    match plan.as_ref() {
        LogicalPlan::Union(u) if is_redundant_undirected_union(&plan) => {
            crate::debug_print!(
                "🔄 BidirectionalUnion: Collapsing redundant undirected Union to single branch"
            );
            u.inputs[0].clone() // Take Outgoing branch
        }
        LogicalPlan::CartesianProduct(cp) => {
            let new_left = collapse_leaf_unions_in_cp(cp.left.clone());
            let new_right = collapse_leaf_unions_in_cp(cp.right.clone());
            if Arc::ptr_eq(&new_left, &cp.left) && Arc::ptr_eq(&new_right, &cp.right) {
                plan
            } else {
                Arc::new(LogicalPlan::CartesianProduct(
                    crate::query_planner::logical_plan::CartesianProduct {
                        left: new_left,
                        right: new_right,
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    },
                ))
            }
        }
        LogicalPlan::Filter(f) => {
            let new_input = collapse_leaf_unions_in_cp(f.input.clone());
            if Arc::ptr_eq(&new_input, &f.input) {
                plan
            } else {
                Arc::new(LogicalPlan::Filter(Filter {
                    input: new_input,
                    predicate: f.predicate.clone(),
                }))
            }
        }
        _ => plan,
    }
}

/// Count the number of undirected (Direction::Either) edges in a GraphRel path.
/// IMPORTANT: Stops at WITH clause boundaries because WITH creates a scope boundary -
/// undirected edges before a WITH have already been resolved into their Union branches
/// within the WITH's scope.
fn count_undirected_edges(plan: &Arc<LogicalPlan>) -> usize {
    match plan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            let self_count = if graph_rel.direction == Direction::Either {
                1
            } else {
                0
            };
            let left_count = count_undirected_edges(&graph_rel.left);
            // CRITICAL FIX: Also recurse into right branch to find nested undirected edges
            // Without this, patterns like (a)-[:R1]-(b)<-[:R2]-(c) miss the R1 edge
            let right_count = count_undirected_edges(&graph_rel.right);
            self_count + left_count + right_count
        }
        LogicalPlan::Projection(proj) => {
            // Note: Projection(kind: With) no longer exists - WITH uses WithClause instead.
            // Regular projections don't create scope boundaries - recurse into input.
            count_undirected_edges(&proj.input)
        }
        // WithClause is an explicit boundary - do NOT count edges beyond it
        LogicalPlan::WithClause(_) => {
            crate::debug_print!(
                "🔄 BidirectionalUnion: Stopping undirected edge count at WithClause boundary"
            );
            0
        }
        LogicalPlan::Filter(filter) => count_undirected_edges(&filter.input),
        _ => 0,
    }
}

/// Check if any undirected edge in the plan has a nested GraphRel as its left subtree.
/// When this is the case, the UNION branch swap for the Incoming direction would
/// destructively restructure the FROM/JOIN chain, causing broken SQL (wrong FROM table,
/// duplicate aliases, joins referencing tables before declaration).
/// Instead, these patterns should keep Direction::Either and let the join builder
/// handle both directions with an OR condition.
fn has_nested_undirected_edge(plan: &Arc<LogicalPlan>) -> bool {
    match plan.as_ref() {
        LogicalPlan::GraphRel(gr) => {
            if gr.direction == Direction::Either
                && matches!(gr.left.as_ref(), LogicalPlan::GraphRel(_))
            {
                return true;
            }
            has_nested_undirected_edge(&gr.left) || has_nested_undirected_edge(&gr.right)
        }
        LogicalPlan::Projection(p) => has_nested_undirected_edge(&p.input),
        LogicalPlan::Filter(f) => has_nested_undirected_edge(&f.input),
        _ => false,
    }
}

/// Mapping of (edge_alias, column_name) -> swapped_column_name for column swapping
/// Key: (edge_alias like "r1", original_column like "Origin")
/// Value: swapped_column like "Dest"
type ColumnSwapMap = std::collections::HashMap<(String, String), String>;

/// Information about a relationship for uniqueness filtering
#[derive(Debug, Clone)]
struct RelInfo {
    alias: String,
    /// Edge identity columns for uniqueness filtering.
    /// If edge_id defined in schema, use those columns.
    /// Otherwise, default to [from_id, to_id].
    edge_id_cols: Vec<String>,
}

/// Collect all relationship info from a plan for uniqueness filtering
fn collect_relationship_info(plan: &Arc<LogicalPlan>, graph_schema: &GraphSchema) -> Vec<RelInfo> {
    let mut rels = Vec::new();
    collect_relationship_info_inner(plan, graph_schema, &mut rels);
    rels
}

fn collect_relationship_info_inner(
    plan: &Arc<LogicalPlan>,
    graph_schema: &GraphSchema,
    rels: &mut Vec<RelInfo>,
) {
    match plan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            // Get from_id/to_id from the center ViewScan
            if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                let from_id = scan
                    .from_id
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "from_id".to_string());
                let to_id = scan
                    .to_id
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "to_id".to_string());

                // Look up edge_id from schema using relationship labels
                let edge_id_cols = graph_rel
                    .labels
                    .as_ref()
                    .and_then(|labels| {
                        labels.iter().find_map(|label| {
                            graph_schema
                                .get_relationships_schema_opt(label.as_str())
                                .and_then(|rel_schema| rel_schema.edge_id.as_ref())
                                .map(|id| {
                                    id.columns()
                                        .iter()
                                        .map(|s| s.to_string())
                                        .collect::<Vec<_>>()
                                })
                        })
                    })
                    // Default to [from_id, to_id] if no edge_id defined
                    .unwrap_or_else(|| vec![from_id, to_id]);

                rels.push(RelInfo {
                    alias: graph_rel.alias.clone(),
                    edge_id_cols,
                });
            }
            // Recurse into left (inner relationships in chain)
            collect_relationship_info_inner(&graph_rel.left, graph_schema, rels);
        }
        LogicalPlan::Projection(proj) => {
            collect_relationship_info_inner(&proj.input, graph_schema, rels);
        }
        LogicalPlan::Filter(filter) => {
            collect_relationship_info_inner(&filter.input, graph_schema, rels);
        }
        _ => {}
    }
}

/// Generate relationship uniqueness filter for multiple relationships.
/// Prevents the same physical edge from being used as both r1 and r2.
/// Uses edge_id columns from schema (or defaults to [from_id, to_id]).
fn generate_relationship_uniqueness_filter(rels: &[RelInfo]) -> Option<LogicalExpr> {
    if rels.len() < 2 {
        return None; // Need at least 2 relationships
    }

    let mut filters = Vec::new();

    // Generate pairwise filters: NOT (r1.col1 = r2.col1 AND r1.col2 = r2.col2 AND ...)
    for i in 0..rels.len() {
        for j in (i + 1)..rels.len() {
            let r1 = &rels[i];
            let r2 = &rels[j];

            // Build equality comparisons for all edge_id columns
            // If edge_id columns differ between relationships, we can't compare them
            // (this happens when comparing different relationship types)
            if r1.edge_id_cols != r2.edge_id_cols {
                // Different edge types with different edge_id - they can't be the same edge
                continue;
            }

            // Build: NOT (r1.col1 = r2.col1 AND r1.col2 = r2.col2 AND ...)
            let mut col_equalities = Vec::new();
            for col in &r1.edge_id_cols {
                let r1_col = LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(r1.alias.clone()),
                    column: PropertyValue::Column(col.clone()),
                });
                let r2_col = LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(r2.alias.clone()),
                    column: PropertyValue::Column(col.clone()),
                });

                let col_eq = LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![r1_col, r2_col],
                });
                col_equalities.push(col_eq);
            }

            if col_equalities.is_empty() {
                continue;
            }

            // AND all column equalities together
            let all_equal = col_equalities
                .into_iter()
                .reduce(|acc, eq| {
                    LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![acc, eq],
                    })
                })
                .unwrap();

            // NOT the result
            let not_equal = LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![all_equal],
            });

            filters.push(not_equal);
        }
    }

    if filters.is_empty() {
        return None;
    }

    // Combine all filters with AND
    Some(
        filters
            .into_iter()
            .reduce(|acc, filter| {
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: vec![acc, filter],
                })
            })
            .unwrap(),
    )
}

/// Wrap a plan with a relationship uniqueness filter if needed
fn wrap_with_uniqueness_filter(
    plan: Arc<LogicalPlan>,
    graph_schema: &GraphSchema,
) -> Arc<LogicalPlan> {
    let rels = collect_relationship_info(&plan, graph_schema);

    if let Some(filter_expr) = generate_relationship_uniqueness_filter(&rels) {
        crate::debug_print!(
            "🔒 BidirectionalUnion: Adding relationship uniqueness filter for {} relationships (edge_id_cols: {:?})",
            rels.len(),
            rels.iter().map(|r| &r.edge_id_cols).collect::<Vec<_>>()
        );
        Arc::new(LogicalPlan::Filter(Filter {
            input: plan,
            predicate: filter_expr,
        }))
    } else {
        plan
    }
}

/// Generate all 2^n direction combinations for a path with n undirected edges.
/// Each combination produces a fully-directed plan structure with correctly swapped columns.
fn generate_direction_combinations(
    plan: &Arc<LogicalPlan>,
    undirected_count: usize,
    graph_schema: &GraphSchema,
) -> Vec<Arc<LogicalPlan>> {
    let total_combinations = 1 << undirected_count; // 2^n
    let mut branches = Vec::with_capacity(total_combinations);

    for combination in 0..total_combinations {
        // Each bit in `combination` represents the direction of an undirected edge:
        // 0 = Outgoing, 1 = Incoming
        let mut column_swaps: ColumnSwapMap = std::collections::HashMap::new();
        let branch = apply_direction_combination(plan, combination, &mut column_swaps);

        // Skip branches with invalid relationship directions (e.g., FK-edge only
        // defines one direction, so the reverse branch is invalid)
        if !is_valid_direction_branch(&branch, graph_schema) {
            log::debug!(
                "🔄 BidirectionalUnion: Skipping invalid direction combination {:b}/{}",
                combination,
                total_combinations - 1
            );
            continue;
        }

        // Apply relationship uniqueness filter to prevent same edge from being used twice.
        // Uses edge_id columns from schema (or defaults to [from_id, to_id]).
        // This ensures Neo4j-compatible behavior where relationship instances are unique in paths.
        let filtered_branch = wrap_with_uniqueness_filter(branch, graph_schema);

        branches.push(filtered_branch);
    }

    branches
}

/// Check if all relationship patterns in a branch are valid against the schema.
/// Returns false only if the schema explicitly defines the relationship type but with
/// different from/to node labels (i.e., the reverse direction is invalid).
/// If the relationship type is not in the schema at all, returns true (validation pass
/// will catch it later with a proper error).
fn is_valid_direction_branch(plan: &Arc<LogicalPlan>, graph_schema: &GraphSchema) -> bool {
    match plan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            // Only validate edges that were swapped by bidirectional union.
            // Directed edges (Outgoing/Incoming without was_undirected) are valid as-is.
            if graph_rel.was_undirected != Some(true) {
                // Not a swapped undirected edge — skip validation, just recurse
                return is_valid_direction_branch(&graph_rel.left, graph_schema)
                    && is_valid_direction_branch(&graph_rel.right, graph_schema);
            }

            if let Some(labels) = &graph_rel.labels {
                // For multi-type patterns ([:TYPE1|TYPE2]), a branch is valid if ANY
                // label is valid for this direction (each type generates its own UNION branch).
                // Only reject if ALL labels are invalid.
                let from_label = extract_node_label_from_plan(&graph_rel.left);
                let to_label = extract_node_label_from_plan(&graph_rel.right);
                let left_table = extract_source_table_from_plan(&graph_rel.left);
                let right_table = extract_source_table_from_plan(&graph_rel.right);

                let mut all_invalid = true;
                for rel_type in labels {
                    if let Some(rel_schema) = graph_schema.get_relationships_schema_opt(rel_type) {
                        let schema_from = &rel_schema.from_node;
                        let schema_to = &rel_schema.to_node;
                        // Wildcard schemas are always valid
                        if schema_from == "$any" || schema_to == "$any" {
                            all_invalid = false;
                            break;
                        }
                        // Self-referencing relationships are always valid in both directions
                        if schema_from == schema_to {
                            all_invalid = false;
                            break;
                        }
                        // Check explicit labels
                        if let (Some(from), Some(to)) = (&from_label, &to_label) {
                            if from == schema_from && to == schema_to {
                                all_invalid = false;
                                break;
                            }
                        } else {
                            // Unlabeled nodes: check ViewScan source tables against schema
                            let from_node_table = graph_schema
                                .node_schema(schema_from)
                                .ok()
                                .map(|n| n.full_table_name());
                            let to_node_table = graph_schema
                                .node_schema(schema_to)
                                .ok()
                                .map(|n| n.full_table_name());
                            if let (Some(lt), Some(rt), Some(ft), Some(tt)) =
                                (&left_table, &right_table, &from_node_table, &to_node_table)
                            {
                                if ft == tt || (lt == ft && rt == tt) {
                                    all_invalid = false;
                                    break;
                                }
                            } else {
                                // Can't determine tables — assume valid
                                all_invalid = false;
                                break;
                            }
                        }
                    } else {
                        // Unknown rel type — assume valid
                        all_invalid = false;
                        break;
                    }
                }
                if all_invalid && !labels.is_empty() {
                    log::debug!(
                        "🔄 is_valid_direction_branch: REJECTING branch, no valid types among {:?}",
                        labels
                    );
                    return false;
                }
            }
            // Recurse into nested patterns
            is_valid_direction_branch(&graph_rel.left, graph_schema)
                && is_valid_direction_branch(&graph_rel.right, graph_schema)
        }
        LogicalPlan::GraphNode(gn) => is_valid_direction_branch(&gn.input, graph_schema),
        LogicalPlan::Projection(p) => is_valid_direction_branch(&p.input, graph_schema),
        LogicalPlan::Filter(f) => is_valid_direction_branch(&f.input, graph_schema),
        _ => true,
    }
}

/// Extract node label from a plan node (GraphNode → label)
fn extract_node_label_from_plan(plan: &Arc<LogicalPlan>) -> Option<String> {
    match plan.as_ref() {
        LogicalPlan::GraphNode(gn) => gn.label.clone(),
        LogicalPlan::Projection(p) => extract_node_label_from_plan(&p.input),
        LogicalPlan::Filter(f) => extract_node_label_from_plan(&f.input),
        _ => None,
    }
}

/// Extract source table name from a plan node (ViewScan → source_table)
fn extract_source_table_from_plan(plan: &Arc<LogicalPlan>) -> Option<String> {
    match plan.as_ref() {
        LogicalPlan::ViewScan(vs) => Some(vs.source_table.clone()),
        LogicalPlan::GraphNode(gn) => extract_source_table_from_plan(&gn.input),
        LogicalPlan::Projection(p) => extract_source_table_from_plan(&p.input),
        LogicalPlan::Filter(f) => extract_source_table_from_plan(&f.input),
        _ => None,
    }
}

/// Apply a specific direction combination to a plan.
/// `combination` is a bitmask where each bit represents the direction of an undirected edge.
/// `column_swaps` collects the column swap information for updating projections.
fn apply_direction_combination(
    plan: &Arc<LogicalPlan>,
    combination: usize,
    column_swaps: &mut ColumnSwapMap,
) -> Arc<LogicalPlan> {
    apply_direction_combination_inner(plan, combination, &mut 0, column_swaps)
}

/// Inner recursive function that tracks which bit position we're at
fn apply_direction_combination_inner(
    plan: &Arc<LogicalPlan>,
    combination: usize,
    bit_position: &mut usize,
    column_swaps: &mut ColumnSwapMap,
) -> Arc<LogicalPlan> {
    match plan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            // Recurse into BOTH left and right subtrees to handle nested undirected edges
            // This is critical for patterns like (a)-[:R1]-(b)<-[:R2]-(c) where the
            // undirected R1 edge is in the right branch of the outer R2 GraphRel.
            let new_left = apply_direction_combination_inner(
                &graph_rel.left,
                combination,
                bit_position,
                column_swaps,
            );
            let new_right = apply_direction_combination_inner(
                &graph_rel.right,
                combination,
                bit_position,
                column_swaps,
            );

            // Determine this edge's direction
            let new_direction = if graph_rel.direction == Direction::Either {
                let dir = if (combination >> *bit_position) & 1 == 0 {
                    Direction::Outgoing
                } else {
                    Direction::Incoming
                };
                *bit_position += 1;

                // For Incoming direction with denormalized nodes, record column swap info
                //
                // In the plan, `a.code` is resolved to `a.Origin` (using node alias + column from from_props).
                // For Incoming direction, we need to swap:
                // - left_connection (e.g., "a") properties: from columns → to columns
                // - right_connection (e.g., "b") properties: to columns → from columns
                //
                // So swap map should be keyed by node alias, not edge alias.
                if dir == Direction::Incoming {
                    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                        if let (Some(from_props), Some(to_props)) =
                            (&scan.from_node_properties, &scan.to_node_properties)
                        {
                            let left_node = &graph_rel.left_connection;
                            let right_node = &graph_rel.right_connection;

                            // For each property, find the corresponding from and to columns
                            for (prop_name, from_col) in from_props {
                                if let Some(to_col) = to_props.get(prop_name) {
                                    let from_col_name = from_col.raw().to_string();
                                    let to_col_name = to_col.raw().to_string();

                                    if from_col_name != to_col_name {
                                        // For left_connection node: from → to (e.g., a.Origin → a.Dest)
                                        column_swaps.insert(
                                            (left_node.clone(), from_col_name.clone()),
                                            to_col_name.clone(),
                                        );

                                        // For right_connection node: to → from (e.g., b.Dest → b.Origin)
                                        column_swaps.insert(
                                            (right_node.clone(), to_col_name.clone()),
                                            from_col_name.clone(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                dir
            } else {
                graph_rel.direction.clone()
            };

            // For Incoming direction (from bidirectional transformation), swap left/right both:
            // 1. The plan structures (left ↔ right) - so FROM/TO tables are swapped
            // 2. The connection strings (left_connection ↔ right_connection)
            //
            // This maintains the invariant that left is FROM and right is TO in the generated SQL.
            // The parser already does this swap for explicitly-written incoming patterns like (a)<-[r]-(b),
            // so we need to do the same when we create an Incoming branch from an Either pattern.
            let is_incoming_swap =
                new_direction == Direction::Incoming && graph_rel.direction == Direction::Either;
            let (final_left, final_right, new_left_connection, new_right_connection) =
                if is_incoming_swap {
                    // Swap both plan structures and connections for the Incoming branch
                    // new_left/new_right were from recursively processing graph_rel.left/right
                    (
                        new_right,                          // Right becomes left (FROM table)
                        new_left,                           // Left becomes right (TO table)
                        graph_rel.right_connection.clone(), // Swap connections too
                        graph_rel.left_connection.clone(),
                    )
                } else {
                    (
                        new_left,
                        new_right, // Use recursively processed right
                        graph_rel.left_connection.clone(),
                        graph_rel.right_connection.clone(),
                    )
                };

            // Create new GraphRel with the determined direction
            Arc::new(LogicalPlan::GraphRel(GraphRel {
                left: final_left,
                center: graph_rel.center.clone(),
                right: final_right,
                alias: graph_rel.alias.clone(),
                direction: new_direction,
                left_connection: new_left_connection,
                right_connection: new_right_connection,
                is_rel_anchor: graph_rel.is_rel_anchor,
                variable_length: graph_rel.variable_length.clone(),
                shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                path_variable: graph_rel.path_variable.clone(),
                where_predicate: graph_rel.where_predicate.clone(),
                labels: graph_rel.labels.clone(),
                is_optional: graph_rel.is_optional,
                anchor_connection: if is_incoming_swap {
                    graph_rel.anchor_connection.as_ref().map(|ac| {
                        if ac == &graph_rel.left_connection {
                            graph_rel.right_connection.clone()
                        } else if ac == &graph_rel.right_connection {
                            graph_rel.left_connection.clone()
                        } else {
                            ac.clone()
                        }
                    })
                } else {
                    graph_rel.anchor_connection.clone()
                },
                cte_references: graph_rel.cte_references.clone(),
                pattern_combinations: graph_rel.pattern_combinations.clone(),
                was_undirected: if graph_rel.direction == Direction::Either {
                    Some(true)
                } else {
                    graph_rel.was_undirected
                },
            }))
        }
        LogicalPlan::Projection(proj) => {
            // Recurse into input first to build column_swaps
            let new_input = apply_direction_combination_inner(
                &proj.input,
                combination,
                bit_position,
                column_swaps,
            );

            // Now apply column swaps to projection items
            let new_items = if !column_swaps.is_empty() {
                proj.items
                    .iter()
                    .map(|item| swap_projection_item_columns(item, column_swaps))
                    .collect()
            } else {
                proj.items.clone()
            };

            Arc::new(LogicalPlan::Projection(Projection {
                input: new_input,
                items: new_items,
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }
        LogicalPlan::Filter(filter) => {
            let new_input = apply_direction_combination_inner(
                &filter.input,
                combination,
                bit_position,
                column_swaps,
            );
            Arc::new(LogicalPlan::Filter(Filter {
                input: new_input,
                predicate: filter.predicate.clone(),
            }))
        }
        // For other node types, just return as-is
        _ => plan.clone(),
    }
}

/// Swap column references in a ProjectionItem based on direction changes
/// For incoming direction, columns need to be swapped (from ↔ to)
fn swap_projection_item_columns(
    item: &ProjectionItem,
    column_swaps: &ColumnSwapMap,
) -> ProjectionItem {
    ProjectionItem {
        expression: swap_expr_columns(&item.expression, column_swaps),
        col_alias: item.col_alias.clone(),
    }
}

/// Recursively swap column references in a LogicalExpr
fn swap_expr_columns(expr: &LogicalExpr, column_swaps: &ColumnSwapMap) -> LogicalExpr {
    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            // Check if this property access needs column swapping
            // The table_alias is the node alias (e.g., "a", "b", "c")
            // column_swaps maps (node_alias, column_name) -> swapped_column_name

            let current_col = pa.column.raw();
            let node_alias = &pa.table_alias.0;

            // Look up if this (node, column) needs swapping
            let key = (node_alias.clone(), current_col.to_string());
            if let Some(swapped_col) = column_swaps.get(&key) {
                return LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: pa.table_alias.clone(),
                    column: PropertyValue::Column(swapped_col.clone()),
                });
            }

            // No swap needed, return as-is
            expr.clone()
        }
        LogicalExpr::OperatorApplicationExp(op) => LogicalExpr::OperatorApplicationExp(
            crate::query_planner::logical_expr::OperatorApplication {
                operator: op.operator,
                operands: op
                    .operands
                    .iter()
                    .map(|o| swap_expr_columns(o, column_swaps))
                    .collect(),
            },
        ),
        LogicalExpr::ScalarFnCall(call) => {
            LogicalExpr::ScalarFnCall(crate::query_planner::logical_expr::ScalarFnCall {
                name: call.name.clone(),
                args: call
                    .args
                    .iter()
                    .map(|a| swap_expr_columns(a, column_swaps))
                    .collect(),
            })
        }
        LogicalExpr::Case(case) => {
            LogicalExpr::Case(crate::query_planner::logical_expr::LogicalCase {
                expr: case
                    .expr
                    .as_ref()
                    .map(|e| Box::new(swap_expr_columns(e, column_swaps))),
                when_then: case
                    .when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            swap_expr_columns(w, column_swaps),
                            swap_expr_columns(t, column_swaps),
                        )
                    })
                    .collect(),
                else_expr: case
                    .else_expr
                    .as_ref()
                    .map(|e| Box::new(swap_expr_columns(e, column_swaps))),
            })
        }
        // For other expression types, return as-is
        _ => expr.clone(),
    }
}

/// Check if a plan subtree contains a VLP multi-type GraphRel that handles
/// both directions internally (labels.len() > 1 + variable_length).
/// Walks through Filter, Projection, and other wrapper nodes to find GraphRel.
fn is_vlp_multi_type_subtree(plan: &Arc<LogicalPlan>) -> bool {
    match plan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            graph_rel.variable_length.is_some()
                && graph_rel.labels.as_ref().is_some_and(|l| l.len() > 1)
        }
        LogicalPlan::Filter(f) => is_vlp_multi_type_subtree(&f.input),
        LogicalPlan::Projection(p) => is_vlp_multi_type_subtree(&p.input),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::GraphSchema;
    use crate::query_planner::logical_plan::ViewScan;
    use std::collections::HashMap;

    fn make_test_node(table: &str, alias: &str, label: &str) -> Arc<LogicalPlan> {
        let scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            table.to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: scan,
            alias: alias.to_string(),
            label: Some(label.to_string()),
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        }))
    }

    fn make_test_graph_rel(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        rel_alias: &str,
        rel_table: &str,
        left_conn: &str,
        right_conn: &str,
        direction: Direction,
        label: &str,
    ) -> GraphRel {
        let center = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            rel_table.to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));
        GraphRel {
            left,
            center,
            right,
            alias: rel_alias.to_string(),
            direction,
            left_connection: left_conn.to_string(),
            right_connection: right_conn.to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec![label.to_string()]),
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
        }
    }

    #[test]
    fn test_bidirectional_detection() {
        let left_node = make_test_node("users", "a", "User");
        let right_node = make_test_node("users", "b", "User");

        let graph_rel = make_test_graph_rel(
            left_node,
            right_node,
            "r",
            "follows",
            "a",
            "b",
            Direction::Either,
            "FOLLOWS",
        );

        let plan = Arc::new(LogicalPlan::GraphRel(graph_rel));
        let mut plan_ctx = PlanCtx::new_empty();
        let graph_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

        let result = transform_bidirectional(&plan, &mut plan_ctx, &graph_schema);
        assert!(result.is_ok());

        match result.unwrap() {
            Transformed::Yes(new_plan) => {
                // Should be a Union now
                match new_plan.as_ref() {
                    LogicalPlan::Union(union) => {
                        assert_eq!(union.inputs.len(), 2);
                        assert!(matches!(union.union_type, UnionType::All));

                        // Check first branch is Outgoing with original connections
                        if let LogicalPlan::GraphRel(rel) = union.inputs[0].as_ref() {
                            assert_eq!(rel.direction, Direction::Outgoing);
                            // Outgoing branch: connections stay as original (a->b)
                            assert_eq!(
                                rel.left_connection, "a",
                                "Outgoing branch should have left_connection='a'"
                            );
                            assert_eq!(
                                rel.right_connection, "b",
                                "Outgoing branch should have right_connection='b'"
                            );
                        } else {
                            panic!("Expected GraphRel in first union branch");
                        }

                        // Check second branch is Incoming with SWAPPED connections
                        // This is critical for correct JOIN generation!
                        if let LogicalPlan::GraphRel(rel) = union.inputs[1].as_ref() {
                            assert_eq!(rel.direction, Direction::Incoming);
                            // Incoming branch: connections should be swapped (b->a becomes a<-b)
                            // The parser normalizes so left=FROM, right=TO
                            // For incoming, we swap so JOIN conditions generate correctly
                            assert_eq!(
                                rel.left_connection, "b",
                                "Incoming branch should have left_connection='b' (swapped)"
                            );
                            assert_eq!(
                                rel.right_connection, "a",
                                "Incoming branch should have right_connection='a' (swapped)"
                            );
                        } else {
                            panic!("Expected GraphRel in second union branch");
                        }
                    }
                    _ => panic!("Expected Union plan"),
                }
            }
            Transformed::No(_) => panic!("Expected transformation to occur"),
        }
    }

    #[test]
    fn test_nested_undirected_edge_detected() {
        // Build nested (a)-[r1]-(b)-[r2]-(c): inner GraphRel as left of outer
        let node_a = make_test_node("users", "a", "User");
        let node_b = make_test_node("users", "b", "User");
        let node_c = make_test_node("users", "c", "User");

        let inner_rel = make_test_graph_rel(
            node_a,
            node_b,
            "r1",
            "follows",
            "a",
            "b",
            Direction::Either,
            "FOLLOWS",
        );
        let inner_plan = Arc::new(LogicalPlan::GraphRel(inner_rel));

        let outer_rel = make_test_graph_rel(
            inner_plan,
            node_c,
            "r2",
            "follows",
            "b",
            "c",
            Direction::Either,
            "FOLLOWS",
        );
        let plan = Arc::new(LogicalPlan::GraphRel(outer_rel));

        // Nested undirected edges should still be detected by the helper
        assert!(has_nested_undirected_edge(&plan));

        // transform_bidirectional should NOW transform nested undirected edges
        // into UNION branches (previously skipped, but the Incoming branch swap
        // correctly restructures the chain)
        let mut plan_ctx = PlanCtx::new_empty();
        let graph_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let result = transform_bidirectional(&plan, &mut plan_ctx, &graph_schema).unwrap();
        assert!(
            matches!(result, Transformed::Yes(_)),
            "Nested undirected edges should now be transformed into UNION branches"
        );
    }

    #[test]
    fn test_simple_undirected_edge_not_nested() {
        // Build single (a)-[r]-(b): left is GraphNode, not GraphRel
        let node_a = make_test_node("users", "a", "User");
        let node_b = make_test_node("users", "b", "User");

        let rel = make_test_graph_rel(
            node_a,
            node_b,
            "r",
            "follows",
            "a",
            "b",
            Direction::Either,
            "FOLLOWS",
        );
        let plan = Arc::new(LogicalPlan::GraphRel(rel));

        assert!(
            !has_nested_undirected_edge(&plan),
            "Single undirected edge should not be detected as nested"
        );
    }

    /// Helper to build a VLP multi-type GraphRel (variable_length + multiple labels).
    fn make_vlp_multi_type_rel(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        rel_alias: &str,
        left_conn: &str,
        right_conn: &str,
        direction: Direction,
        labels: Vec<&str>,
    ) -> GraphRel {
        let center = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "rels".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));
        GraphRel {
            left,
            center,
            right,
            alias: rel_alias.to_string(),
            direction,
            left_connection: left_conn.to_string(),
            right_connection: right_conn.to_string(),
            is_rel_anchor: false,
            variable_length: Some(crate::query_planner::logical_plan::VariableLengthSpec {
                min_hops: Some(1),
                max_hops: Some(1),
            }),
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(labels.into_iter().map(|s| s.to_string()).collect()),
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
        }
    }

    /// Regression: VLP multi-type undirected pattern should set was_undirected=true
    /// and direction=Outgoing, rather than creating a UNION split.
    /// This is critical for to-side-only nodes like Post that have no outgoing edges —
    /// the VLP CTE handles both directions internally.
    #[test]
    fn test_vlp_multi_type_undirected_sets_was_undirected() {
        let node_a = make_test_node("posts", "a", "Post");
        let node_o = make_test_node("users", "o", "User");

        let graph_rel = make_vlp_multi_type_rel(
            node_a,
            node_o,
            "r",
            "a",
            "o",
            Direction::Either,
            vec!["AUTHORED", "LIKED"],
        );
        let plan = Arc::new(LogicalPlan::GraphRel(graph_rel));

        let mut plan_ctx = PlanCtx::new_empty();
        let graph_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let result = transform_bidirectional(&plan, &mut plan_ctx, &graph_schema).unwrap();

        match result {
            Transformed::Yes(new_plan) => {
                if let LogicalPlan::GraphRel(rel) = new_plan.as_ref() {
                    assert_eq!(
                        rel.direction,
                        Direction::Outgoing,
                        "VLP multi-type should be converted to Outgoing"
                    );
                    assert_eq!(
                        rel.was_undirected,
                        Some(true),
                        "VLP multi-type should mark was_undirected=true"
                    );
                    assert_eq!(
                        rel.labels.as_ref().unwrap().len(),
                        2,
                        "Labels should be preserved"
                    );
                    assert!(
                        rel.variable_length.is_some(),
                        "Variable length should be preserved"
                    );
                } else {
                    panic!("Expected GraphRel, got Union — VLP multi-type should NOT be split into Union");
                }
            }
            Transformed::No(_) => panic!("Expected transformation for undirected pattern"),
        }
    }

    /// VLP single-type undirected should still create a UNION split (not skip).
    #[test]
    fn test_vlp_single_type_undirected_creates_union() {
        let node_a = make_test_node("users", "a", "User");
        let node_b = make_test_node("users", "b", "User");

        // Single label + variable_length = NOT multi-type, should get Union split
        let center = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "follows".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));
        let graph_rel = GraphRel {
            left: node_a,
            center,
            right: node_b,
            alias: "r".to_string(),
            direction: Direction::Either,
            left_connection: "a".to_string(),
            right_connection: "b".to_string(),
            is_rel_anchor: false,
            variable_length: Some(crate::query_planner::logical_plan::VariableLengthSpec {
                min_hops: Some(1),
                max_hops: Some(1),
            }),
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec!["FOLLOWS".to_string()]),
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
        };
        let plan = Arc::new(LogicalPlan::GraphRel(graph_rel));

        let mut plan_ctx = PlanCtx::new_empty();
        let graph_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let result = transform_bidirectional(&plan, &mut plan_ctx, &graph_schema).unwrap();

        match result {
            Transformed::Yes(new_plan) => {
                assert!(
                    matches!(new_plan.as_ref(), LogicalPlan::Union(_)),
                    "Single-type VLP undirected should create Union, not skip"
                );
            }
            Transformed::No(_) => panic!("Expected transformation for undirected pattern"),
        }
    }

    /// Directed VLP multi-type should not be transformed at all.
    #[test]
    fn test_vlp_multi_type_directed_no_transform() {
        let node_a = make_test_node("users", "a", "User");
        let node_o = make_test_node("posts", "o", "Post");

        let graph_rel = make_vlp_multi_type_rel(
            node_a,
            node_o,
            "r",
            "a",
            "o",
            Direction::Outgoing, // Already directed
            vec!["AUTHORED", "LIKED"],
        );
        let plan = Arc::new(LogicalPlan::GraphRel(graph_rel));

        let mut plan_ctx = PlanCtx::new_empty();
        let graph_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        let result = transform_bidirectional(&plan, &mut plan_ctx, &graph_schema).unwrap();

        assert!(
            matches!(result, Transformed::No(_)),
            "Directed VLP multi-type should not be transformed"
        );
    }
}
