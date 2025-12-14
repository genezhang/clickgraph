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
    Filter, GraphNode, GraphRel, GroupBy, LogicalPlan, Projection, ProjectionItem,
    Union, UnionType,
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
                    return Ok(Transformed::Yes(branches.into_iter().next().unwrap()));
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
                    return Ok(Transformed::Yes(branches.into_iter().next().unwrap()));
                }

                let union = Union {
                    inputs: branches,
                    union_type: UnionType::All,
                };

                crate::debug_print!(
                    "🔄 BidirectionalUnion: Created UNION ALL with {} branches (with column swaps)",
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
        | LogicalPlan::Scan(_)
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
                    // Wrap it in WithClause - the Union stays INSIDE the WITH boundary
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
                    .clone()
                    .unwrap_or_else(|| "from_id".to_string());
                let to_id = scan.to_id.clone().unwrap_or_else(|| "to_id".to_string());

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

        // Apply relationship uniqueness filter to prevent same edge from being used twice.
        // Uses edge_id columns from schema (or defaults to [from_id, to_id]).
        // This ensures Neo4j-compatible behavior where relationship instances are unique in paths.
        let filtered_branch = wrap_with_uniqueness_filter(branch, graph_schema);

        branches.push(filtered_branch);
    }

    branches
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
            let (final_left, final_right, new_left_connection, new_right_connection) =
                if new_direction == Direction::Incoming && graph_rel.direction == Direction::Either
                {
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
                anchor_connection: graph_rel.anchor_connection.clone(),
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
                operator: op.operator.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::GraphSchema;
    use crate::query_planner::logical_plan::ViewScan;
    use std::collections::HashMap;

    #[test]
    fn test_bidirectional_detection() {
        // Create a simple bidirectional GraphRel
        let left_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "users".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));

        let center_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "follows".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));

        let right_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan::new(
            "users".to_string(),
            None,
            HashMap::new(),
            "id".to_string(),
            vec![],
            vec![],
        ))));

        let left_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: left_scan,
            alias: "a".to_string(),
            label: Some("User".to_string()),
            is_denormalized: false,
            projected_columns: None,
        }));

        let right_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: right_scan,
            alias: "b".to_string(),
            label: Some("User".to_string()),
            is_denormalized: false,
            projected_columns: None,
        }));

        let graph_rel = GraphRel {
            left: left_node,
            center: center_scan,
            right: right_node,
            alias: "r".to_string(),
            direction: Direction::Either,
            left_connection: "a".to_string(),
            right_connection: "b".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec!["FOLLOWS".to_string()]),
            is_optional: None,
            anchor_connection: None,
        };

        let plan = Arc::new(LogicalPlan::GraphRel(graph_rel));
        let mut plan_ctx = PlanCtx::default();
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
}
