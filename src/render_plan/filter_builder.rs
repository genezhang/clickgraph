//! Filter Builder Module
//!
//! This module handles extraction of filter expressions from logical plans.
//! It processes WHERE clauses, HAVING clauses, and other filter conditions
//! that need to be applied to the generated SQL queries.

use crate::query_planner::logical_expr::expression_rewriter::{
    rewrite_expression_with_property_mapping, ExpressionRewriteContext,
};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::cte_extraction::{
    extract_relationship_columns, table_to_id_column, RelationshipColumns,
};
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::plan_builder_helpers::*;
use crate::render_plan::render_expr::{Operator, OperatorApplication, RenderExpr};

/// Result type for filter builder operations
pub type FilterBuilderResult<T> = Result<T, RenderBuildError>;

/// Trait for extracting filter expressions from logical plans
pub trait FilterBuilder {
    /// Extract filters from WHERE clauses and other filter conditions
    fn extract_filters(&self) -> FilterBuilderResult<Option<RenderExpr>>;

    /// Extract DISTINCT flag from projection nodes
    fn extract_distinct(&self) -> bool;
}

impl FilterBuilder for LogicalPlan {
    fn extract_filters(&self) -> FilterBuilderResult<Option<RenderExpr>> {
        let filters = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::ViewScan(_) => None,
            LogicalPlan::GraphNode(graph_node) => {
                // For node-only queries, extract both view_filter and schema_filter from the input ViewScan
                if let LogicalPlan::ViewScan(scan) = graph_node.input.as_ref() {
                    log::info!(
                        "🔍 GraphNode '{}' extract_filters: ViewScan table={}",
                        graph_node.alias,
                        scan.source_table
                    );

                    let mut filters = Vec::new();

                    // Extract view_filter (user's WHERE clause, injected by optimizer)
                    if let Some(ref view_filter) = scan.view_filter {
                        log::debug!(
                            "extract_filters: view_filter BEFORE rewrite: {:?}",
                            view_filter
                        );

                        // 🔧 FIX: Rewrite property names to DB column names BEFORE converting to RenderExpr
                        // This uses the same function as WITH clause processing for consistency
                        let rewrite_ctx = ExpressionRewriteContext::new(&graph_node.input);
                        let rewritten_filter =
                            rewrite_expression_with_property_mapping(view_filter, &rewrite_ctx);

                        log::debug!(
                            "extract_filters: view_filter AFTER rewrite: {:?}",
                            rewritten_filter
                        );

                        let expr: RenderExpr = rewritten_filter.try_into()?;
                        log::debug!("extract_filters: view_filter AFTER conversion: {:?}", expr);
                        log::info!(
                            "GraphNode '{}': Adding view_filter: {:?}",
                            graph_node.alias,
                            expr
                        );
                        filters.push(expr);
                    }

                    // Extract schema_filter (from YAML schema)
                    // Wrap in parentheses to ensure correct operator precedence when combined with user filters
                    if let Some(ref schema_filter) = scan.schema_filter {
                        if let Ok(sql) = schema_filter.to_sql(&graph_node.alias) {
                            log::info!(
                                "GraphNode '{}': Adding schema filter: {}",
                                graph_node.alias,
                                sql
                            );
                            // Always wrap schema filter in parentheses for safe combination
                            filters.push(RenderExpr::Raw(format!("({})", sql)));
                        }
                    }

                    // Combine filters with AND if multiple
                    // Use explicit AND combination - each operand will be wrapped appropriately
                    if filters.is_empty() {
                        return Ok(None);
                    } else if filters.len() == 1 {
                        // Safety: len() == 1 guarantees next() returns Some
                        return Ok(Some(
                            filters
                                .into_iter()
                                .next()
                                .expect("filters has exactly one element"),
                        ));
                    } else {
                        // When combining filters, wrap non-Raw expressions in parentheses
                        // to handle AND/OR precedence correctly
                        let combined = filters
                            .into_iter()
                            .reduce(|acc, pred| {
                                // The OperatorApplicationExp will render as "(left) AND (right)"
                                // due to the render_expr_to_sql_string logic
                                RenderExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![acc, pred],
                                })
                            })
                            .expect("filters is non-empty, reduce succeeds");
                        return Ok(Some(combined));
                    }
                }
                None
            }
            LogicalPlan::GraphRel(graph_rel) => {
                log::trace!(
                    "GraphRel node detected, collecting filters from ALL nested where_predicates"
                );

                // 🔧 BUG #10 FIX: For VLP/shortest path queries, filters from where_predicate
                // are already pushed into the CTE during extraction. Don't duplicate them
                // in the outer SELECT WHERE clause.
                //
                // ⚠️ CRITICAL FIX (Jan 23, 2026): Don't skip ALL filters!
                // Filters that reference nodes OUTSIDE the VLP pattern should still be applied
                // in the final query. Only skip filters that are entirely on VLP nodes.
                //
                // Example: MATCH (a:User)-[*]->(b:User) WHERE a.name = 'Alice' AND c.status = 'active'
                //   - "a.name = 'Alice'" is inside VLP → stays in CTE
                //   - "c.status = 'active'" is outside VLP → should be in outer SELECT
                //
                // 🔧 FIX (Jan 31, 2026): For OPTIONAL VLP, start node filters are removed from CTE
                // and should be applied to the outer query WHERE clause.
                if graph_rel.variable_length.is_some() || graph_rel.shortest_path_mode.is_some() {
                    // Check if this is OPTIONAL VLP - start filters need to be in outer query
                    if graph_rel.is_optional.unwrap_or(false) {
                        log::info!(
                            "🔧 OPTIONAL VLP: Extracting start node filters for outer WHERE clause"
                        );
                        // For OPTIONAL VLP, extract the where_predicate (start node filter)
                        // The CTE extraction intentionally removes these from the CTE
                        if let Some(ref predicate) = graph_rel.where_predicate {
                            if let Ok(expr) = RenderExpr::try_from(predicate.clone()) {
                                log::info!("🔧 OPTIONAL VLP: Found start filter: {:?}", expr);
                                return Ok(Some(expr));
                            }
                        }
                        return Ok(None);
                    }

                    log::info!(
                        "🔧 BUG #10: Skipping GraphRel filter extraction for VLP/shortest path - already in CTE"
                    );
                    log::warn!(
                        "⚠️ NOTE: Filters on nodes OUTSIDE VLP pattern are also skipped (limitation)"
                    );
                    // Don't extract filters - they're already in the CTE
                    // TODO: Implement proper filter splitting to handle external filters
                    return Ok(None);
                }

                // Collect all where_predicates from this GraphRel and nested GraphRel nodes
                // Using helper functions from plan_builder_helpers module
                let all_predicates =
                    collect_graphrel_predicates(&LogicalPlan::GraphRel(graph_rel.clone()));

                let mut all_predicates = all_predicates;

                // 🔒 Add schema-level filters from ViewScans
                let schema_filters =
                    collect_schema_filters(&LogicalPlan::GraphRel(graph_rel.clone()), None);
                if !schema_filters.is_empty() {
                    log::info!(
                        "Adding {} schema filter(s) to WHERE clause",
                        schema_filters.len()
                    );
                    all_predicates.extend(schema_filters);
                }

                // TODO: Add relationship uniqueness filters for undirected multi-hop patterns
                // This requires fixing Issue #1 (Undirected Multi-Hop Patterns Generate Broken SQL) first.
                // See KNOWN_ISSUES.md for details.
                // Currently, undirected multi-hop patterns generate broken SQL with wrong aliases,
                // so adding uniqueness filters here would not work correctly.

                // 🚀 ADD CYCLE PREVENTION for fixed-length paths (only for 2+ hops)
                // Single hop (*1) can't have cycles - no need for cycle prevention
                if let Some(spec) = &graph_rel.variable_length {
                    if let Some(exact_hops) = spec.exact_hop_count() {
                        // Skip cycle prevention for *1 - single hop can't cycle
                        if exact_hops >= 2 && graph_rel.shortest_path_mode.is_none() {
                            crate::debug_println!("DEBUG: extract_filters - Adding cycle prevention for fixed-length *{}", exact_hops);

                            // Check if this is a denormalized pattern
                            let is_denormalized = is_node_denormalized(&graph_rel.left)
                                && is_node_denormalized(&graph_rel.right);

                            // Extract table/column info for cycle prevention
                            // Use extract_table_name directly to avoid wrong fallbacks
                            let start_table =
                                extract_table_name(&graph_rel.left).ok_or_else(|| {
                                    RenderBuildError::MissingTableInfo(
                                        "start node in cycle prevention".to_string(),
                                    )
                                })?;
                            let end_table =
                                extract_table_name(&graph_rel.right).ok_or_else(|| {
                                    RenderBuildError::MissingTableInfo(
                                        "end node in cycle prevention".to_string(),
                                    )
                                })?;

                            let rel_cols = extract_relationship_columns(&graph_rel.center)
                                .unwrap_or(RelationshipColumns {
                                    from_id: "from_node_id".to_string(),
                                    to_id: "to_node_id".to_string(),
                                });

                            // For denormalized, use relationship columns directly
                            // For normal, use node ID columns
                            let (start_id_col, end_id_col) = if is_denormalized {
                                (rel_cols.from_id.clone(), rel_cols.to_id.clone())
                            } else {
                                let start = extract_id_column(&graph_rel.left)
                                    .unwrap_or_else(|| table_to_id_column(&start_table));
                                let end = extract_id_column(&graph_rel.right)
                                    .unwrap_or_else(|| table_to_id_column(&end_table));
                                (start, end)
                            };

                            // Generate cycle prevention filters
                            if let Some(cycle_filter) = crate::render_plan::cte_extraction::generate_cycle_prevention_filters(
                                exact_hops,
                                &start_id_col,
                                &rel_cols.to_id,
                                &rel_cols.from_id,
                                &end_id_col,
                                &graph_rel.left_connection,
                                &graph_rel.right_connection,
                            ) {
                                crate::debug_println!("DEBUG: extract_filters - Generated cycle prevention filter");
                                all_predicates.push(cycle_filter);
                            }
                        }
                    }
                }

                if all_predicates.is_empty() {
                    None
                } else if all_predicates.len() == 1 {
                    log::trace!("Found 1 GraphRel predicate");
                    // Safety: len() == 1 guarantees next() returns Some
                    let mut single_pred = all_predicates
                        .into_iter()
                        .next()
                        .expect("all_predicates has exactly one element");
                    // Apply property mapping for denormalized nodes
                    apply_property_mapping_to_expr(
                        &mut single_pred,
                        &LogicalPlan::GraphRel(graph_rel.clone()),
                    );
                    Some(single_pred)
                } else {
                    // Combine with AND
                    log::trace!(
                        "Found {} GraphRel predicates, combining with AND",
                        all_predicates.len()
                    );
                    let combined = all_predicates
                        .into_iter()
                        .map(|mut pred| {
                            // Apply property mapping for denormalized nodes
                            apply_property_mapping_to_expr(
                                &mut pred,
                                &LogicalPlan::GraphRel(graph_rel.clone()),
                            );
                            pred
                        })
                        .reduce(|acc, pred| {
                            RenderExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![acc, pred],
                            })
                        })
                        .expect("all_predicates is non-empty, reduce succeeds");
                    Some(combined)
                }
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                // For GraphJoins, extract filters from the input
                graph_joins.input.extract_filters()?
            }
            LogicalPlan::Filter(filter) => {
                log::warn!("🔍 extract_filters: Found Filter node");
                log::warn!("🔍 extract_filters: Filter predicate: {:?}", filter.predicate);
                log::warn!("🔍 extract_filters: Filter input type: {:?}", std::mem::discriminant(&*filter.input));

                // 🔧 BUG #10 FIX: For VLP/shortest path queries, filters on start/end nodes
                // are already pushed into the CTE during extraction. Don't duplicate them
                // in the outer SELECT WHERE clause.
                let has_vlp_or_shortest_path = has_variable_length_or_shortest_path(&filter.input);
                
                log::warn!("🔍 extract_filters: has_vlp_or_shortest_path = {}", has_vlp_or_shortest_path);

                println!(
                    "DEBUG: has_vlp_or_shortest_path = {}",
                    has_vlp_or_shortest_path
                );

                if has_vlp_or_shortest_path {
                    log::info!(
                        "🔧 BUG #10: Skipping Filter extraction for VLP/shortest path - already in CTE"
                    );
                    println!("DEBUG: 🔧 BUG #10: Skipping Filter extraction for VLP/shortest path - already in CTE");
                    // Don't extract this filter - it's already in the CTE
                    // Just extract filters from the input (schema filters, etc.)
                    filter.input.extract_filters()?
                } else {
                    println!("DEBUG: Normal filter extraction");
                    // Normal filter extraction
                    let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                    // Apply property mapping to the filter expression
                    apply_property_mapping_to_expr(&mut expr, &filter.input);

                    // Also check for schema filters from the input (e.g., GraphNode → ViewScan)
                    if let Some(input_filter) = filter.input.extract_filters()? {
                        crate::debug_println!("DEBUG: extract_filters - Combining Filter predicate with input schema filter");
                        // Combine the Filter predicate with input's schema filter using AND
                        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![input_filter, expr],
                        }))
                    } else {
                        crate::debug_println!("DEBUG: extract_filters - Returning Filter predicate only (no input filter)");
                        Some(expr)
                    }
                }
            }
            LogicalPlan::Projection(projection) => {
                crate::debug_println!(
                    "DEBUG: extract_filters - Projection, recursing to input type: {:?}",
                    std::mem::discriminant(&*projection.input)
                );
                projection.input.extract_filters()?
            }
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_filters()?,
            LogicalPlan::Limit(limit) => limit.input.extract_filters()?,
            LogicalPlan::Cte(cte) => cte.input.extract_filters()?,
            LogicalPlan::Union(_) => None,
            LogicalPlan::PageRank(_) => None,
            LogicalPlan::Unwind(u) => u.input.extract_filters()?,
            LogicalPlan::CartesianProduct(cp) => {
                // Combine filters from both sides with AND
                let left_filters = cp.left.extract_filters()?;
                let right_filters = cp.right.extract_filters()?;

                // DEBUG: Log what we're extracting
                log::info!("🔍 CartesianProduct extract_filters:");
                log::info!("  Left filters: {:?}", left_filters);
                log::info!("  Right filters: {:?}", right_filters);

                match (left_filters, right_filters) {
                    (None, None) => None,
                    (Some(l), None) => {
                        log::info!("  ✅ Returning left filters only");
                        Some(l)
                    }
                    (None, Some(r)) => {
                        log::info!("  ✅ Returning right filters only");
                        Some(r)
                    }
                    (Some(l), Some(r)) => {
                        log::warn!("  ⚠️ BOTH sides have filters - combining with AND!");
                        log::warn!("  ⚠️ This may cause duplicates if filters are the same!");
                        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![l, r],
                        }))
                    }
                }
            }
            LogicalPlan::WithClause(wc) => wc.input.extract_filters()?,
        };
        Ok(filters)
    }

    fn extract_distinct(&self) -> bool {
        // Extract distinct flag from Projection nodes
        let result = match &self {
            LogicalPlan::Projection(projection) => {
                crate::debug_println!(
                    "DEBUG extract_distinct: Found Projection, distinct={}",
                    projection.distinct
                );
                projection.distinct
            }
            LogicalPlan::OrderBy(order_by) => {
                crate::debug_println!("DEBUG extract_distinct: OrderBy, recursing");
                order_by.input.extract_distinct()
            }
            LogicalPlan::Skip(skip) => {
                crate::debug_println!("DEBUG extract_distinct: Skip, recursing");
                skip.input.extract_distinct()
            }
            LogicalPlan::Limit(limit) => {
                crate::debug_println!("DEBUG extract_distinct: Limit, recursing");
                limit.input.extract_distinct()
            }
            LogicalPlan::GroupBy(group_by) => {
                crate::debug_println!("DEBUG extract_distinct: GroupBy, recursing");
                group_by.input.extract_distinct()
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                crate::debug_println!("DEBUG extract_distinct: GraphJoins, recursing");
                graph_joins.input.extract_distinct()
            }
            LogicalPlan::Filter(filter) => {
                crate::debug_println!("DEBUG extract_distinct: Filter, recursing");
                filter.input.extract_distinct()
            }
            _ => {
                crate::debug_println!("DEBUG extract_distinct: Other variant, returning false");
                false
            }
        };
        crate::debug_println!("DEBUG extract_distinct: Returning {}", result);
        result
    }
}
