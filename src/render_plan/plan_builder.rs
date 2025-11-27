use crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_expr::Direction;
use crate::query_planner::logical_plan::{GraphRel, LogicalPlan, ProjectionItem};
use crate::query_planner::plan_ctx::PlanCtx;
use std::sync::Arc;

use super::cte_generation::{
    analyze_property_requirements, extract_var_len_properties, map_property_to_column_with_schema,
};
use super::errors::RenderBuildError;
use super::filter_pipeline::{
    CategorizedFilters, categorize_filters, clean_last_node_filters, extract_start_end_filters,
    filter_expr_to_sql, render_end_filter_to_column_alias,
    rewrite_end_filters_for_variable_length_cte, rewrite_expr_for_outer_query,
    rewrite_expr_for_var_len_cte,
};
use super::render_expr::{
    AggregateFnCall, Column, ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess,
    RenderExpr, ScalarFnCall, TableAlias,
};
use super::{
    Cte, CteItems, FilterItems, FromTable, FromTableItem, GroupByExpressions, Join, JoinItems,
    JoinType, LimitItem, OrderByItem, OrderByItems, OrderByOrder, RenderPlan, SelectItem, SelectItems, SkipItem,
    Union, UnionItems, ViewTableRef,
    view_table_ref::{from_table_to_view_ref, view_ref_to_from_table},
};
use crate::render_plan::cte_extraction::extract_ctes_with_context;
use crate::render_plan::cte_extraction::{
    RelationshipColumns, extract_node_label_from_viewscan, extract_relationship_columns,
    get_path_variable, get_shortest_path_mode, get_variable_length_spec, has_variable_length_rel,
    label_to_table_name, rel_type_to_table_name, rel_types_to_table_names, table_to_id_column,
};

// Import ALL helper functions from the dedicated helpers module using glob import
// This allows existing code to call helpers without changes (e.g., extract_table_name())
// The compiler will use the module functions when available
use super::CteGenerationContext;
#[allow(unused_imports)]
use super::plan_builder_helpers::*;

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

pub(crate) trait RenderPlanBuilder {
    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>>;

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>>;

    fn extract_ctes_with_context(
        &self,
        last_node_alias: &str,
        context: &mut CteGenerationContext,
    ) -> RenderPlanBuilderResult<Vec<Cte>>;

    /// Find the ID column for a given table alias by traversing the logical plan
    fn find_id_column_for_alias(&self, alias: &str) -> RenderPlanBuilderResult<String>;

    /// Get all properties for a table alias by traversing the logical plan
    /// Returns a vector of (property_name, column_name) tuples
    fn get_all_properties_for_alias(
        &self,
        alias: &str,
    ) -> RenderPlanBuilderResult<Vec<(String, String)>>;

    /// Find denormalized properties for a given alias
    /// Returns a HashMap of logical property name -> physical column name
    fn find_denormalized_properties(
        &self,
        alias: &str,
    ) -> Option<std::collections::HashMap<String, String>>;

    /// Normalize aggregate function arguments: convert TableAlias(a) to PropertyAccess(a.id_column)
    /// This is needed for queries like COUNT(b) where b is a node alias
    fn normalize_aggregate_args(&self, expr: RenderExpr) -> RenderPlanBuilderResult<RenderExpr>;

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>>;

    fn extract_distinct(&self) -> bool;

    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>>;

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_joins(&self) -> RenderPlanBuilderResult<Vec<Join>>;

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>>;

    fn extract_having(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>>;

    fn extract_limit(&self) -> Option<i64>;

    fn extract_skip(&self) -> Option<i64>;

    fn extract_union(&self) -> RenderPlanBuilderResult<Option<Union>>;

    fn try_build_join_based_plan(&self) -> RenderPlanBuilderResult<RenderPlan>;

    fn build_simple_relationship_render_plan(&self, distinct_override: Option<bool>) -> RenderPlanBuilderResult<RenderPlan>;

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan>;
}

impl RenderPlanBuilder for LogicalPlan {
    fn find_id_column_for_alias(&self, alias: &str) -> RenderPlanBuilderResult<String> {
        // Traverse the plan tree to find a GraphNode or ViewScan with matching alias
        match self {
            LogicalPlan::GraphNode(node) if node.alias == alias => {
                // Found the matching node - extract ID column from its ViewScan
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    return Ok(scan.id_column.clone());
                }
            }
            LogicalPlan::GraphRel(rel) => {
                // Check both left and right branches
                if let Ok(id) = rel.left.find_id_column_for_alias(alias) {
                    return Ok(id);
                }
                if let Ok(id) = rel.right.find_id_column_for_alias(alias) {
                    return Ok(id);
                }
            }
            LogicalPlan::Projection(proj) => {
                return proj.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Filter(filter) => {
                return filter.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::GroupBy(gb) => {
                return gb.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::GraphJoins(joins) => {
                return joins.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::OrderBy(order) => {
                return order.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Skip(skip) => {
                return skip.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Limit(limit) => {
                return limit.input.find_id_column_for_alias(alias);
            }
            _ => {}
        }
        Err(RenderBuildError::InvalidRenderPlan(format!(
            "Cannot find ID column for alias '{}'",
            alias
        )))
    }

    /// Get all properties for a table alias by traversing the logical plan
    /// Returns a vector of (property_name, column_name) tuples
    fn get_all_properties_for_alias(
        &self,
        alias: &str,
    ) -> RenderPlanBuilderResult<Vec<(String, String)>> {
        // Traverse the plan tree to find a GraphNode or ViewScan with matching alias
        match self {
            LogicalPlan::GraphNode(node) if node.alias == alias => {
                // Found the matching node - extract all properties from its ViewScan
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    // Convert property_mapping HashMap to Vec of tuples
                    let properties: Vec<(String, String)> = scan
                        .property_mapping
                        .iter()
                        .map(|(prop_name, prop_value)| (prop_name.clone(), prop_value.raw().to_string()))
                        .collect();
                    return Ok(properties);
                }
            }
            LogicalPlan::GraphRel(rel) => {
                // Check both left and right branches
                if let Ok(props) = rel.left.get_all_properties_for_alias(alias) {
                    return Ok(props);
                }
                if let Ok(props) = rel.right.get_all_properties_for_alias(alias) {
                    return Ok(props);
                }
            }
            LogicalPlan::Projection(proj) => {
                return proj.input.get_all_properties_for_alias(alias);
            }
            LogicalPlan::Filter(filter) => {
                return filter.input.get_all_properties_for_alias(alias);
            }
            LogicalPlan::GroupBy(gb) => {
                return gb.input.get_all_properties_for_alias(alias);
            }
            LogicalPlan::GraphJoins(joins) => {
                return joins.input.get_all_properties_for_alias(alias);
            }
            LogicalPlan::OrderBy(order) => {
                return order.input.get_all_properties_for_alias(alias);
            }
            LogicalPlan::Skip(skip) => {
                return skip.input.get_all_properties_for_alias(alias);
            }
            LogicalPlan::Limit(limit) => {
                return limit.input.get_all_properties_for_alias(alias);
            }
            _ => {}
        }
        Err(RenderBuildError::InvalidRenderPlan(format!(
            "Cannot find properties for alias '{}'",
            alias
        )))
    }

    /// Find denormalized properties for a given alias
    /// Returns a HashMap of logical property name -> physical column name
    /// Only returns Some if the alias refers to a denormalized node
    fn find_denormalized_properties(
        &self,
        alias: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        match self {
            LogicalPlan::GraphNode(node) if node.alias == alias => {
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    if scan.is_denormalized {
                        // Prefer from_node_properties, fall back to to_node_properties
                        // For UNION ALL case, this will be handled separately
                        let props = scan.from_node_properties.as_ref()
                            .or(scan.to_node_properties.as_ref());
                        
                        if let Some(prop_map) = props {
                            return Some(
                                prop_map.iter()
                                    .map(|(k, v)| (k.clone(), v.raw().to_string()))
                                    .collect()
                            );
                        }
                    }
                }
                None
            }
            LogicalPlan::GraphRel(rel) => {
                if let Some(props) = rel.left.find_denormalized_properties(alias) {
                    return Some(props);
                }
                rel.right.find_denormalized_properties(alias)
            }
            LogicalPlan::Projection(proj) => proj.input.find_denormalized_properties(alias),
            LogicalPlan::Filter(filter) => filter.input.find_denormalized_properties(alias),
            LogicalPlan::GroupBy(gb) => gb.input.find_denormalized_properties(alias),
            LogicalPlan::GraphJoins(joins) => joins.input.find_denormalized_properties(alias),
            LogicalPlan::OrderBy(order) => order.input.find_denormalized_properties(alias),
            LogicalPlan::Skip(skip) => skip.input.find_denormalized_properties(alias),
            LogicalPlan::Limit(limit) => limit.input.find_denormalized_properties(alias),
            _ => None,
        }
    }

    fn normalize_aggregate_args(&self, expr: RenderExpr) -> RenderPlanBuilderResult<RenderExpr> {
        match expr {
            RenderExpr::AggregateFnCall(mut agg) => {
                // Recursively normalize all arguments
                agg.args = agg
                    .args
                    .into_iter()
                    .map(|arg| self.normalize_aggregate_args(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::AggregateFnCall(agg))
            }
            RenderExpr::TableAlias(alias) => {
                // Convert COUNT(b) to COUNT(b.user_id)
                let id_col = self.find_id_column_for_alias(&alias.0)?;
                Ok(RenderExpr::PropertyAccessExp(
                    super::render_expr::PropertyAccess {
                        table_alias: alias,
                        column: super::render_expr::Column(PropertyValue::Column(id_col)),
                    },
                ))
            }
            RenderExpr::OperatorApplicationExp(mut op) => {
                // Recursively normalize operands
                op.operands = op
                    .operands
                    .into_iter()
                    .map(|operand| self.normalize_aggregate_args(operand))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::OperatorApplicationExp(op))
            }
            RenderExpr::ScalarFnCall(mut func) => {
                // Recursively normalize function arguments
                func.args = func
                    .args
                    .into_iter()
                    .map(|arg| self.normalize_aggregate_args(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::ScalarFnCall(func))
            }
            // Other expressions pass through unchanged
            _ => Ok(expr),
        }
    }

    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>> {
        let last_node_cte = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::ViewScan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_last_node_cte()?,
            LogicalPlan::GraphRel(graph_rel) => {
                // Last node is at the top of the tree.
                // process left node first.
                let left_node_cte_opt = graph_rel.left.extract_last_node_cte()?;

                // If last node is still not found then check at the right tree
                if left_node_cte_opt.is_none() {
                    graph_rel.right.extract_last_node_cte()?
                } else {
                    left_node_cte_opt
                }
            }
            LogicalPlan::Filter(filter) => filter.input.extract_last_node_cte()?,
            LogicalPlan::Projection(projection) => projection.input.extract_last_node_cte()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_last_node_cte()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_last_node_cte()?,
            LogicalPlan::Skip(skip) => skip.input.extract_last_node_cte()?,
            LogicalPlan::Limit(limit) => limit.input.extract_last_node_cte()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_last_node_cte()?,
            LogicalPlan::Cte(logical_cte) => {
                // let filters = logical_cte.input.extract_filters()?;
                // let select_items = logical_cte.input.extract_select_items()?;
                // let from_table = logical_cte.input.extract_from()?;
                use crate::graph_catalog::graph_schema::GraphSchema;
                use std::collections::HashMap;
                let empty_schema =
                    GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());
                let render_cte = Cte {
                    cte_name: logical_cte.name.clone(),
                    content: super::CteContent::Structured(
                        logical_cte.input.to_render_plan(&empty_schema)?,
                    ),
                    is_recursive: false,
                    // select: SelectItems(select_items),
                    // from: from_table,
                    // filters: FilterItems(filters)
                };
                Some(render_cte)
            }
            LogicalPlan::Union(union) => {
                for input_plan in union.inputs.iter() {
                    if let Some(cte) = input_plan.extract_last_node_cte()? {
                        return Ok(Some(cte));
                    }
                }
                None
            }
            LogicalPlan::PageRank(_) => None,
        };
        Ok(last_node_cte)
    }

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>> {
        match &self {
            LogicalPlan::Empty => Ok(vec![]),
            LogicalPlan::Scan(_) => Ok(vec![]),
            LogicalPlan::ViewScan(_) => Ok(vec![]),
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_ctes(last_node_alias),
            LogicalPlan::GraphRel(graph_rel) => {
                // Extract table names and column information - SAME LOGIC FOR BOTH PATHS
                // Get node labels first, then convert to table names
                let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                    .unwrap_or_else(|| "User".to_string()); // Fallback to User if not found
                let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                    .unwrap_or_else(|| "User".to_string()); // Fallback to User if not found
                let start_table = label_to_table_name(&start_label);
                let end_table = label_to_table_name(&end_label);

                // Handle multiple relationship types
                let rel_tables = if let Some(labels) = &graph_rel.labels {
                    if labels.len() > 1 {
                        // Multiple relationship types: get all table names
                        rel_types_to_table_names(labels)
                    } else if labels.len() == 1 {
                        // Single relationship type
                        vec![rel_type_to_table_name(&labels[0])]
                    } else {
                        // Fallback to old logic
                        vec![rel_type_to_table_name(
                            &extract_table_name(&graph_rel.center)
                                .unwrap_or_else(|| graph_rel.alias.clone()),
                        )]
                    }
                } else {
                    // Fallback to old logic
                    vec![rel_type_to_table_name(
                        &extract_table_name(&graph_rel.center)
                            .unwrap_or_else(|| graph_rel.alias.clone()),
                    )]
                };

                // For now, use the first table for single-table logic
                // TODO: Implement UNION logic for multiple tables
                let rel_table = rel_tables
                    .first()
                    .ok_or(RenderBuildError::NoRelationshipTablesFound)?
                    .clone(); // Extract ID columns
                let start_id_col = extract_id_column(&graph_rel.left)
                    .unwrap_or_else(|| table_to_id_column(&start_table));
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));

                // Extract relationship columns from ViewScan (will use schema-specific names if available)
                let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                    RelationshipColumns {
                        from_id: "from_node_id".to_string(), // Generic fallback
                        to_id: "to_node_id".to_string(),     // Generic fallback
                    },
                );
                let from_col = rel_cols.from_id;
                let to_col = rel_cols.to_id;

                // Handle variable-length paths differently
                if let Some(spec) = &graph_rel.variable_length {
                    // Define aliases that will be used throughout
                    let start_alias = graph_rel.left_connection.clone();
                    let end_alias = graph_rel.right_connection.clone();

                    // Extract node labels for property mapping
                    let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                        .unwrap_or_else(|| "User".to_string()); // fallback
                    let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                        .unwrap_or_else(|| "User".to_string()); // fallback

                    // Extract and categorize filters for variable-length paths from GraphRel.where_predicate
                    let (start_filters_sql, end_filters_sql) =
                        if let Some(where_predicate) = &graph_rel.where_predicate {
                            // Convert LogicalExpr to RenderExpr
                            let mut render_expr = RenderExpr::try_from(where_predicate.clone())
                                .map_err(|e| {
                                    RenderBuildError::UnsupportedFeature(format!(
                                        "Failed to convert LogicalExpr to RenderExpr: {}",
                                        e
                                    ))
                                })?;

                            // Apply property mapping to the filter expression before categorization
                            apply_property_mapping_to_expr(
                                &mut render_expr,
                                &LogicalPlan::GraphRel(graph_rel.clone()),
                            );

                            // Categorize filters
                            let categorized = categorize_filters(
                                Some(&render_expr),
                                &start_alias,
                                &end_alias,
                                "", // rel_alias not used yet
                            );

                            // Create alias mapping
                            let alias_mapping = [
                                (start_alias.clone(), "start_node".to_string()),
                                (end_alias.clone(), "end_node".to_string()),
                            ];

                            let start_sql = categorized
                                .start_node_filters
                                .map(|expr| render_expr_to_sql_string(&expr, &alias_mapping));
                            let end_sql = categorized
                                .end_node_filters
                                .map(|expr| render_expr_to_sql_string(&expr, &alias_mapping));

                            (start_sql, end_sql)
                        } else {
                            (None, None)
                        };

                    // Extract properties from the projection for variable-length paths
                    let properties = extract_var_len_properties(
                        self,
                        &start_alias,
                        &end_alias,
                        &start_label,
                        &end_label,
                        graph_rel.labels.as_ref().and_then(|labels| labels.first().map(|s| s.as_str())),
                    );

                    // Choose between inline JOINs (for exact hop counts) or recursive CTE (for ranges)
                    // For shortest path queries, always use recursive CTE (even for exact hops)
                    // because we need proper filtering and shortest path selection logic
                    let use_inline_joins =
                        spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();

                    if use_inline_joins {
                        // Fixed-length patterns (*2, *3, etc) - NO CTE needed!
                        // extract_joins() will handle inline JOIN generation
                        println!("DEBUG extract_ctes: Fixed-length pattern - skipping CTE, will use inline JOINs");
                        
                        // Continue extracting CTEs from child nodes
                        let mut child_ctes = graph_rel.left.extract_ctes(last_node_alias)?;
                        child_ctes.extend(graph_rel.right.extract_ctes(last_node_alias)?);
                        return Ok(child_ctes);
                    }
                    
                    // Variable-length or shortest path - generate recursive CTE
                    let var_len_cte = {
                        // Range, unbounded, or shortest path: use recursive CTE
                        let generator = VariableLengthCteGenerator::new(
                            spec.clone(),
                            &start_table,                // actual start table name
                            &start_id_col,               // start node ID column
                            &rel_table,                  // actual relationship table name
                            &from_col,                   // relationship from column
                            &to_col,                     // relationship to column
                            &end_table,                  // actual end table name
                            &end_id_col,                 // end node ID column
                            &graph_rel.left_connection,  // start node alias (for output)
                            &graph_rel.right_connection, // end node alias (for output)
                            properties,                  // properties to include in CTE
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()), // convert logical plan mode to SQL mode
                            start_filters_sql, // start node filters for CTE
                            end_filters_sql,   // end node filters for CTE
                            graph_rel.path_variable.clone(), // path variable name
                            graph_rel.labels.clone(), // relationship type labels
                            None, // edge_id - no edge ID tracking for now
                        );
                        generator.generate_cte()
                    }; // Close the var_len_cte block

                    // Also extract CTEs from child plans
                    let mut child_ctes = graph_rel.right.extract_ctes(last_node_alias)?;
                    child_ctes.push(var_len_cte);

                    return Ok(child_ctes);
                }

                // Regular single-hop relationship: use JOIN logic instead of CTEs
                // For simple relationships (single type, no variable-length), don't create CTEs
                // Let the normal plan building logic handle JOINs
                if rel_tables.len() == 1 && graph_rel.variable_length.is_none() {
                    // Simple relationship: no CTEs needed, use JOINs
                    return Ok(vec![]);
                }

                // Handle multiple relationship types or complex cases with UNION/CTEs
                let mut relationship_ctes = vec![];

                if rel_tables.len() > 1 {
                    // Multiple relationship types: create a UNION CTE
                    let union_queries: Vec<String> = rel_tables
                        .iter()
                        .map(|table| {
                            // Get the correct column names for this table
                            let (from_col, to_col) = get_relationship_columns_by_table(table)
                                .unwrap_or(("from_node_id".to_string(), "to_node_id".to_string())); // fallback
                            format!(
                                "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                from_col, to_col, table
                            )
                        })
                        .collect();

                    let union_sql = union_queries.join(" UNION ALL ");
                    let cte_name = format!(
                        "rel_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );

                    // Format as proper CTE: cte_name AS (union_sql)
                    let formatted_union_sql = format!("{} AS (\n{}\n)", cte_name, union_sql);

                    relationship_ctes.push(Cte {
                        cte_name: cte_name.clone(),
                        content: super::CteContent::RawSql(formatted_union_sql),
                        is_recursive: false,
                    });

                    // PATCH: Ensure join uses the union CTE name
                    // Instead of context, propagate rel_table for join construction
                    // We'll use rel_table (CTE name) directly in join construction below
                }

                // TODO: Apply the resolved table/column names to the child CTEs
                // For now, fall back to the old path which doesn't resolve properly
                // first extract the bottom one
                let mut right_cte = graph_rel.right.extract_ctes(last_node_alias)?;
                // then process the center
                let mut center_cte = graph_rel.center.extract_ctes(last_node_alias)?;
                right_cte.append(&mut center_cte);
                // then left
                let left_alias = &graph_rel.left_connection;
                if left_alias != last_node_alias {
                    let mut left_cte = graph_rel.left.extract_ctes(last_node_alias)?;
                    right_cte.append(&mut left_cte);
                }

                // Add relationship CTEs to the result
                relationship_ctes.append(&mut right_cte);

                Ok(relationship_ctes)
            }
            LogicalPlan::Filter(filter) => filter.input.extract_ctes(last_node_alias),
            LogicalPlan::Projection(projection) => projection.input.extract_ctes(last_node_alias),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_ctes(last_node_alias),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_ctes(last_node_alias),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_ctes(last_node_alias),
            LogicalPlan::Skip(skip) => skip.input.extract_ctes(last_node_alias),
            LogicalPlan::Limit(limit) => limit.input.extract_ctes(last_node_alias),
            LogicalPlan::Cte(logical_cte) => {
                // let mut select_items = logical_cte.input.extract_select_items()?;

                // for select_item in select_items.iter_mut() {
                //     if let RenderExpr::PropertyAccessExp(pro_acc) = &select_item.expression {
                //         *select_item = SelectItem {
                //             expression: RenderExpr::Column(pro_acc.column.clone()),
                //             col_alias: None,
                //         };
                //     }
                // }

                // let mut from_table = logical_cte.input.extract_from()?;
                // from_table.table_alias = None;
                // let filters = logical_cte.input.extract_filters()?;
                use crate::graph_catalog::graph_schema::GraphSchema;
                use std::collections::HashMap;
                let empty_schema =
                    GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());
                Ok(vec![Cte {
                    cte_name: logical_cte.name.clone(),
                    content: super::CteContent::Structured(
                        logical_cte.input.to_render_plan(&empty_schema)?,
                    ),
                    is_recursive: false,
                    // select: SelectItems(select_items),
                    // from: from_table,
                    // filters: FilterItems(filters)
                }])
            }
            LogicalPlan::Union(union) => {
                let mut ctes = vec![];
                for input_plan in union.inputs.iter() {
                    ctes.append(&mut input_plan.extract_ctes(last_node_alias)?);
                }
                Ok(ctes)
            }
            LogicalPlan::PageRank(_) => Ok(vec![]),
        }
    }

    fn extract_ctes_with_context(
        &self,
        last_node_alias: &str,
        context: &mut CteGenerationContext,
    ) -> RenderPlanBuilderResult<Vec<Cte>> {
        extract_ctes_with_context(self, last_node_alias, context)
    }

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>> {
        println!("DEBUG: extract_select_items called on: {:?}", self);
        let select_items = match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::ViewScan(view_scan) => {
                // Build select items from ViewScan's property mappings and projections
                // This is needed for multiple relationship types where ViewScan nodes are created
                // for start/end nodes but don't have explicit projections

                if !view_scan.projections.is_empty() {
                    // Use explicit projections if available
                    view_scan
                        .projections
                        .iter()
                        .map(|proj| {
                            let expr: RenderExpr = proj.clone().try_into()?;
                            Ok(SelectItem {
                                expression: expr,
                                col_alias: None,
                            })
                        })
                        .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else if !view_scan.property_mapping.is_empty() {
                    // Fall back to property mappings - build select items for each property
                    view_scan
                        .property_mapping
                        .iter()
                        .map(|(prop_name, col_name)| {
                            Ok(SelectItem {
                                expression: RenderExpr::Column(Column(col_name.clone())),
                                col_alias: Some(ColumnAlias(prop_name.clone())),
                            })
                        })
                        .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else {
                    // No projections or property mappings - this might be a relationship scan
                    // Return empty for now (relationship CTEs are handled differently)
                    vec![]
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                // FIX: GraphNode must generate PropertyAccessExp with its own alias,
                // not delegate to ViewScan which doesn't know the alias.
                // This fixes the bug where "a.name" becomes "u.name" in OPTIONAL MATCH queries.

                match graph_node.input.as_ref() {
                    LogicalPlan::ViewScan(view_scan) => {
                        if !view_scan.projections.is_empty() {
                            // Use explicit projections if available
                            view_scan
                                .projections
                                .iter()
                                .map(|proj| {
                                    let expr: RenderExpr = proj.clone().try_into()?;
                                    Ok(SelectItem {
                                        expression: expr,
                                        col_alias: None,
                                    })
                                })
                                .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                        } else if !view_scan.property_mapping.is_empty() {
                            // Build PropertyAccessExp using GraphNode's alias (e.g., "a")
                            // instead of bare Column which defaults to heuristic "u"
                            view_scan
                                .property_mapping
                                .iter()
                                .map(|(prop_name, col_name)| {
                                    Ok(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(graph_node.alias.clone()),
                                            column: Column(col_name.clone()),
                                        }),
                                        col_alias: Some(ColumnAlias(prop_name.clone())),
                                    })
                                })
                                .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                        } else if view_scan.is_denormalized && (view_scan.from_node_properties.is_some() || view_scan.to_node_properties.is_some()) {
                            // DENORMALIZED NODE-ONLY QUERY
                            // For denormalized nodes, we need to translate logical property names
                            // to actual column names from the edge table.
                            //
                            // For BOTH positions (from + to), we'll generate UNION ALL later.
                            // For now, use from_node_properties if available, else to_node_properties.
                            
                            let props_to_use = view_scan.from_node_properties.as_ref()
                                .or(view_scan.to_node_properties.as_ref());
                            
                            if let Some(props) = props_to_use {
                                props
                                    .iter()
                                    .map(|(prop_name, prop_value)| {
                                        // Extract the actual column name from PropertyValue
                                        let actual_column = match prop_value {
                                            PropertyValue::Column(col) => col.clone(),
                                            PropertyValue::Expression(expr) => expr.clone(),
                                        };
                                        
                                        Ok(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(graph_node.alias.clone()),
                                                column: Column(PropertyValue::Column(actual_column)),
                                            }),
                                            col_alias: Some(ColumnAlias(format!("{}.{}", graph_node.alias, prop_name))),
                                        })
                                    })
                                    .collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                            } else {
                                vec![]
                            }
                        } else {
                            vec![]
                        }
                    }
                    _ => graph_node.input.extract_select_items()?,
                }
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate SELECT items for both left and right nodes
                // This fixes OPTIONAL MATCH queries where the right node (b) was being ignored
                let mut items = vec![];

                // Get SELECT items from left node
                items.extend(graph_rel.left.extract_select_items()?);

                // Get SELECT items from right node (for OPTIONAL MATCH, this is the optional part)
                items.extend(graph_rel.right.extract_select_items()?);

                items
            }
            LogicalPlan::Filter(filter) => filter.input.extract_select_items()?,
            LogicalPlan::Projection(projection) => {
                // Check if input is a Projection(With) - if so, collect its aliases for resolution
                // The kind might have been changed from With to Return by analyzer passes, so also check
                // if the inner Projection has aliases (which indicate it was originally a WITH clause)
                let with_aliases: std::collections::HashMap<
                    String,
                    crate::query_planner::logical_expr::LogicalExpr,
                > = match projection.input.as_ref() {
                    LogicalPlan::Projection(inner_proj) => {
                        // Check if this was a WITH projection (either still marked as With, or has aliases)
                        let has_aliases =
                            inner_proj.items.iter().any(|item| item.col_alias.is_some());
                        if matches!(
                            inner_proj.kind,
                            crate::query_planner::logical_plan::ProjectionKind::With
                        ) || has_aliases
                        {
                            println!(
                                "DEBUG: Found projection with aliases (possibly WITH) with {} items",
                                inner_proj.items.len()
                            );
                            // Collect aliases from projection
                            let aliases: std::collections::HashMap<_, _> = inner_proj
                                .items
                                .iter()
                                .filter_map(|item| {
                                    item.col_alias.as_ref().map(|alias| {
                                        println!(
                                            "DEBUG: Registering alias: {} -> {:?}",
                                            alias.0, item.expression
                                        );
                                        (alias.0.clone(), item.expression.clone())
                                    })
                                })
                                .collect();
                            println!("DEBUG: Collected {} aliases", aliases.len());
                            aliases
                        } else {
                            std::collections::HashMap::new()
                        }
                    }
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Look through GraphJoins to find the inner Projection(With)
                        if let LogicalPlan::Projection(inner_proj) = graph_joins.input.as_ref() {
                            if let LogicalPlan::Projection(with_proj) = inner_proj.input.as_ref() {
                                let has_aliases =
                                    with_proj.items.iter().any(|item| item.col_alias.is_some());
                                if matches!(
                                    with_proj.kind,
                                    crate::query_planner::logical_plan::ProjectionKind::With
                                ) || has_aliases
                                {
                                    println!(
                                        "DEBUG: Found projection with aliases (through GraphJoins) with {} items",
                                        with_proj.items.len()
                                    );
                                    let aliases: std::collections::HashMap<_, _> = with_proj
                                        .items
                                        .iter()
                                        .filter_map(|item| {
                                            item.col_alias.as_ref().map(|alias| {
                                                println!(
                                                    "DEBUG: Registering alias: {} -> {:?}",
                                                    alias.0, item.expression
                                                );
                                                (alias.0.clone(), item.expression.clone())
                                            })
                                        })
                                        .collect();
                                    println!(
                                        "DEBUG: Collected {} aliases through GraphJoins",
                                        aliases.len()
                                    );
                                    aliases
                                } else {
                                    std::collections::HashMap::new()
                                }
                            } else {
                                std::collections::HashMap::new()
                            }
                        } else {
                            std::collections::HashMap::new()
                        }
                    }
                    _ => std::collections::HashMap::new(),
                };

                let path_var = get_path_variable(&projection.input);

                // EXPANDED NODE FIX: Check if we need to expand node variables to all properties
                // This happens when users write `RETURN u` (returning whole node)
                // The ProjectionTagging analyzer may convert this to `u.*`, OR it may leave it as TableAlias
                let mut expanded_items = Vec::new();
                for item in &projection.items {
                    // Check for TableAlias (u) - expand to all properties
                    if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) =
                        &item.expression
                    {
                        println!(
                            "DEBUG: Found TableAlias {} - checking if should expand to properties",
                            alias.0
                        );

                        // Get all properties for this table alias from the schema
                        if let Ok(properties) = self.get_all_properties_for_alias(&alias.0) {
                            if !properties.is_empty() {
                                println!(
                                    "DEBUG: Expanding TableAlias {} to {} properties",
                                    alias.0,
                                    properties.len()
                                );

                                // Create a separate ProjectionItem for each property
                                for (prop_name, col_name) in properties {
                                    expanded_items.push(ProjectionItem {
                                        expression: crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: alias.clone(),
                                                column: PropertyValue::Column(col_name),
                                            }
                                        ),
                                        col_alias: Some(crate::query_planner::logical_expr::ColumnAlias(prop_name)),
                                    });
                                }
                                continue; // Skip adding the TableAlias item itself
                            }
                        }
                    }

                    // Check for PropertyAccessExp with wildcard (u.*) - expand to all properties
                    if let crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                        prop,
                    ) = &item.expression
                    {
                        if prop.column.raw() == "*" {
                            // This is u.* - need to expand to all properties from schema
                            println!(
                                "DEBUG: Found wildcard property access {}.* - expanding to all properties",
                                prop.table_alias.0
                            );

                            // Get all properties for this table alias from the schema
                            if let Ok(properties) =
                                self.get_all_properties_for_alias(&prop.table_alias.0)
                            {
                                println!(
                                    "DEBUG: Expanding {}.* to {} properties",
                                    prop.table_alias.0,
                                    properties.len()
                                );

                                // Create a separate ProjectionItem for each property
                                for (prop_name, col_name) in properties {
                                    expanded_items.push(ProjectionItem {
                                        expression: crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: prop.table_alias.clone(),
                                                column: PropertyValue::Column(col_name),
                                            }
                                        ),
                                        col_alias: Some(crate::query_planner::logical_expr::ColumnAlias(prop_name)),
                                    });
                                }
                                continue; // Skip adding the wildcard item itself
                            } else {
                                println!(
                                    "DEBUG: Could not expand {}.* - falling back to wildcard",
                                    prop.table_alias.0
                                );
                            }
                        }
                    }

                    // Not a node variable or wildcard expansion failed - keep the item as-is
                    expanded_items.push(item.clone());
                }

                let items = expanded_items.iter().map(|item| {
                    // Resolve TableAlias references to WITH aliases BEFORE conversion
                    let resolved_expr =
                        if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(
                            ref table_alias,
                        ) = item.expression
                        {
                            println!("DEBUG: Checking TableAlias: {}", table_alias.0);
                            if let Some(with_expr) = with_aliases.get(&table_alias.0) {
                                // Replace with the actual expression from WITH
                                println!("DEBUG: Resolved {} to {:?}", table_alias.0, with_expr);
                                with_expr.clone()
                            } else {
                                println!("DEBUG: No WITH alias found for {}", table_alias.0);
                                item.expression.clone()
                            }
                        } else {
                            item.expression.clone()
                        };

                    // Apply denormalized property mapping for denormalized nodes
                    let mut expr: RenderExpr = resolved_expr.try_into()?;
                    
                    // DENORMALIZED PROPERTY TRANSLATION:
                    // For denormalized nodes, translate logical property names to physical columns
                    // e.g., a.code -> a.origin_code (using from_node_properties)
                    let translated_expr = if let RenderExpr::PropertyAccessExp(ref prop_access) = expr {
                        // Try to find a denormalized ViewScan for this alias
                        if let Some(denorm_props) = self.find_denormalized_properties(&prop_access.table_alias.0) {
                            let prop_name = prop_access.column.0.raw();
                            if let Some(physical_col) = denorm_props.get(prop_name) {
                                // Replace with physical column
                                println!(
                                    "DEBUG: Translated denormalized property {}.{} -> {}.{}",
                                    prop_access.table_alias.0, prop_name,
                                    prop_access.table_alias.0, physical_col
                                );
                                Some(RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: prop_access.table_alias.clone(),
                                    column: Column(PropertyValue::Column(physical_col.clone())),
                                }))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    
                    let mut expr = translated_expr.unwrap_or(expr);
                    
                    // Check if this is a path variable that needs to be converted to tuple construction
                    if let (Some(path_var_name), RenderExpr::TableAlias(TableAlias(alias))) =
                        (&path_var, &expr)
                    {
                        if alias == path_var_name {
                            // Convert path variable to named tuple construction
                            // Use tuple(nodes, length, relationships) instead of map() to avoid type conflicts
                            expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "tuple".to_string(),
                                args: vec![
                                    RenderExpr::Column(Column(PropertyValue::Column("path_nodes".to_string()))),
                                    RenderExpr::Column(Column(PropertyValue::Column("hop_count".to_string()))),
                                    RenderExpr::Column(Column(PropertyValue::Column("path_relationships".to_string()))),
                                ],
                            });
                        }
                    }

                    // Rewrite path function calls: length(p), nodes(p), relationships(p)
                    // Use table alias "t" to reference CTE columns
                    if let Some(path_var_name) = &path_var {
                        expr = rewrite_path_functions_with_table(&expr, path_var_name, "t");
                    }

                    // IMPORTANT: Property mapping is already done in the analyzer phase by FilterTagging.apply_property_mapping
                    // for schema-based queries (which use ViewScan). Re-mapping here causes errors because the analyzer
                    // has already converted Cypher property names (e.g., "name") to database column names (e.g., "full_name").
                    // Trying to map "full_name" again fails because it's not in the property_mappings.
                    //
                    // DO NOT apply property mapping here for Projection nodes - it's already been done correctly.

                    let alias = item
                        .col_alias
                        .clone()
                        .map(ColumnAlias::try_from)
                        .transpose()?;
                    Ok(SelectItem {
                        expression: expr,
                        col_alias: alias,
                    })
                });

                items.collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
            }
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_select_items()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_select_items()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items()?,
            LogicalPlan::Skip(skip) => skip.input.extract_select_items()?,
            LogicalPlan::Limit(limit) => limit.input.extract_select_items()?,
            LogicalPlan::Cte(cte) => cte.input.extract_select_items()?,
            LogicalPlan::Union(_) => vec![],
            LogicalPlan::PageRank(_) => vec![],
        };

        Ok(select_items)
    }

    fn extract_distinct(&self) -> bool {
        // Extract distinct flag from Projection nodes
        let result = match &self {
            LogicalPlan::Projection(projection) => {
                println!("DEBUG extract_distinct: Found Projection, distinct={}", projection.distinct);
                projection.distinct
            }
            LogicalPlan::OrderBy(order_by) => {
                println!("DEBUG extract_distinct: OrderBy, recursing");
                order_by.input.extract_distinct()
            }
            LogicalPlan::Skip(skip) => {
                println!("DEBUG extract_distinct: Skip, recursing");
                skip.input.extract_distinct()
            }
            LogicalPlan::Limit(limit) => {
                println!("DEBUG extract_distinct: Limit, recursing");
                limit.input.extract_distinct()
            }
            LogicalPlan::GroupBy(group_by) => {
                println!("DEBUG extract_distinct: GroupBy, recursing");
                group_by.input.extract_distinct()
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                println!("DEBUG extract_distinct: GraphJoins, recursing");
                graph_joins.input.extract_distinct()
            }
            LogicalPlan::Filter(filter) => {
                println!("DEBUG extract_distinct: Filter, recursing");
                filter.input.extract_distinct()
            }
            _ => {
                println!("DEBUG extract_distinct: Other variant, returning false");
                false
            }
        };
        println!("DEBUG extract_distinct: Returning {}", result);
        result
    }

    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>> {
        let from_ref = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(scan) => {
                let table_name_raw = scan
                    .table_name
                    .clone()
                    .ok_or(RenderBuildError::MissingFromTable)?;

                // Check if this is a CTE placeholder for multiple relationships
                // CTE names start with "rel_" and should not be included in FROM clause
                if table_name_raw.starts_with("rel_") {
                    log::info!(
                        "✓ Skipping CTE placeholder '{}' in FROM clause - will be referenced in JOINs",
                        table_name_raw
                    );
                    return Ok(None);
                }

                // Apply relationship type mapping if this might be a relationship scan
                // (Node scans should be ViewScan after our fix, so remaining Scans are likely relationships)
                let table_name = rel_type_to_table_name(&table_name_raw);

                // Get the alias - use Scan's table_alias if available
                let alias = if let Some(ref scan_alias) = scan.table_alias {
                    log::info!(
                        "✓ Scan has table_alias='{}' for table '{}'",
                        scan_alias,
                        table_name
                    );
                    scan_alias.clone()
                } else {
                    // No alias in Scan - this shouldn't happen for relationship scans!
                    // Generate a warning and use a default
                    let default_alias = "t".to_string();
                    log::error!(
                        "❌ BUG: Scan for table '{}' has NO table_alias! Using fallback '{}'",
                        table_name,
                        default_alias
                    );
                    log::error!(
                        "   This indicates the Scan was created without preserving the Cypher variable name!"
                    );
                    default_alias
                };

                log::info!(
                    "✓ Creating ViewTableRef: table='{}', alias='{}'",
                    table_name,
                    alias
                );
                Some(ViewTableRef::new_view_with_alias(
                    Arc::new(LogicalPlan::Scan(scan.clone())),
                    table_name,
                    alias,
                ))
            }
            LogicalPlan::ViewScan(scan) => {
                // Check if this is a relationship ViewScan (has from_id/to_id)
                if scan.from_id.is_some() && scan.to_id.is_some() {
                    // For relationship ViewScans, use the CTE name instead of table name
                    let cte_name =
                        format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""));
                    Some(ViewTableRef::new_table(scan.as_ref().clone(), cte_name))
                } else {
                    // For node ViewScans, use the table name
                    Some(ViewTableRef::new_table(
                        scan.as_ref().clone(),
                        scan.source_table.clone(),
                    ))
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                // For GraphNode, extract FROM from the input but use this GraphNode's alias
                // CROSS JOINs for multiple standalone nodes are handled in extract_joins
                println!(
                    "DEBUG: GraphNode.extract_from() - alias: {}, input: {:?}",
                    graph_node.alias, graph_node.input
                );
                match &*graph_node.input {
                    LogicalPlan::ViewScan(scan) => {
                        println!(
                            "DEBUG: GraphNode.extract_from() - matched ViewScan, table: {}",
                            scan.source_table
                        );
                        // Check if this is a relationship ViewScan (has from_id/to_id)
                        let table_or_cte_name = if scan.from_id.is_some() && scan.to_id.is_some() {
                            // For relationship ViewScans, use the CTE name instead of table name
                            format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""))
                        } else {
                            // For node ViewScans, use the table name
                            scan.source_table.clone()
                        };
                        // ViewScan already returns ViewTableRef, just update the alias
                        let mut view_ref =
                            ViewTableRef::new_table(scan.as_ref().clone(), table_or_cte_name);
                        view_ref.alias = Some(graph_node.alias.clone());
                        println!(
                            "DEBUG: GraphNode.extract_from() - created ViewTableRef: {:?}",
                            view_ref
                        );
                        Some(view_ref)
                    }
                    _ => {
                        println!(
                            "DEBUG: GraphNode.extract_from() - not a ViewScan, input type: {:?}",
                            graph_node.input
                        );
                        // For other input types, extract FROM and convert
                        let mut from_ref = from_table_to_view_ref(graph_node.input.extract_from()?);
                        // Use this GraphNode's alias
                        if let Some(ref mut view_ref) = from_ref {
                            view_ref.alias = Some(graph_node.alias.clone());
                        }
                        from_ref
                    }
                }
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // DENORMALIZED EDGE TABLE CHECK
                // For denormalized patterns, both nodes are virtual - use relationship table as FROM
                let left_is_denormalized = is_node_denormalized(&graph_rel.left);
                let right_is_denormalized = is_node_denormalized(&graph_rel.right);
                
                if left_is_denormalized && right_is_denormalized {
                    println!("DEBUG: extract_from - DENORMALIZED pattern, using relationship table as FROM");
                    
                    // For multi-hop denormalized, find the first (leftmost) relationship
                    // We need to traverse recursively to find the leftmost GraphRel
                    fn find_first_graph_rel(graph_rel: &crate::query_planner::logical_plan::GraphRel) 
                        -> &crate::query_planner::logical_plan::GraphRel 
                    {
                        match graph_rel.left.as_ref() {
                            LogicalPlan::GraphRel(left_rel) => find_first_graph_rel(left_rel),
                            _ => graph_rel,
                        }
                    }
                    
                    let first_graph_rel = find_first_graph_rel(graph_rel);
                    
                    if let LogicalPlan::ViewScan(scan) = first_graph_rel.center.as_ref() {
                        println!(
                            "DEBUG: Using relationship table '{}' as FROM with alias '{}'",
                            scan.source_table, first_graph_rel.alias
                        );
                        return Ok(Some(FromTable::new(Some(ViewTableRef {
                            source: first_graph_rel.center.clone(),
                            name: scan.source_table.clone(),
                            alias: Some(first_graph_rel.alias.clone()),
                            use_final: scan.use_final,
                        }))));
                    }
                }
                
                // Check if both nodes are anonymous (edge-driven query)
                let left_table_name = extract_table_name(&graph_rel.left);
                let right_table_name = extract_table_name(&graph_rel.right);

                // If both nodes are anonymous, use the relationship table as FROM
                if left_table_name.is_none() && right_table_name.is_none() {
                    // Edge-driven query: use relationship table directly (not as CTE)
                    // Extract table name from the relationship ViewScan
                    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                        // Use actual table name, not CTE name
                        return Ok(Some(FromTable::new(Some(ViewTableRef::new_table(
                            scan.as_ref().clone(),
                            scan.source_table.clone(),
                        )))));
                    }
                    // Fallback to normal extraction if not a ViewScan
                    return Ok(None);
                }

                // For GraphRel with labeled nodes, we need to include the start node in the FROM clause
                // This handles simple relationship queries where the start node should be FROM

                // For OPTIONAL MATCH, prefer the required (non-optional) node as FROM
                // Check if this is an optional relationship (left node optional, right node required)
                let prefer_right_as_from = graph_rel.is_optional == Some(true);

                println!(
                    "DEBUG: graph_rel.is_optional = {:?}, prefer_right_as_from = {}",
                    graph_rel.is_optional, prefer_right_as_from
                );

                let (primary_from, fallback_from) = if prefer_right_as_from {
                    // For optional relationships, use right (required) node as FROM
                    (
                        graph_rel.right.extract_from(),
                        graph_rel.left.extract_from(),
                    )
                } else {
                    // For required relationships, use left (start) node as FROM
                    (
                        graph_rel.left.extract_from(),
                        graph_rel.right.extract_from(),
                    )
                };

                println!("DEBUG: primary_from = {:?}", primary_from);
                println!("DEBUG: fallback_from = {:?}", fallback_from);

                if let Ok(Some(from_table)) = primary_from {
                    from_table_to_view_ref(Some(from_table))
                } else {
                    // If primary node doesn't have FROM, try fallback
                    let right_from = fallback_from;
                    println!("DEBUG: Using fallback FROM");
                    println!("DEBUG: right_from = {:?}", right_from);

                    if let Ok(Some(from_table)) = right_from {
                        from_table_to_view_ref(Some(from_table))
                    } else {
                        // If right also doesn't have FROM, check if right contains a nested GraphRel
                        if let LogicalPlan::GraphRel(nested_graph_rel) = graph_rel.right.as_ref() {
                            // Extract FROM from the nested GraphRel's left node
                            let nested_left_from = nested_graph_rel.left.extract_from();
                            println!("DEBUG: nested_graph_rel.left = {:?}", nested_graph_rel.left);
                            println!("DEBUG: nested_left_from = {:?}", nested_left_from);

                            if let Ok(Some(nested_from_table)) = nested_left_from {
                                from_table_to_view_ref(Some(nested_from_table))
                            } else {
                                // If nested left also doesn't have FROM, create one from the nested left_connection alias
                                let table_name = extract_table_name(&nested_graph_rel.left)
                                    .ok_or_else(|| super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for alias '{}', plan: {:?}",
                                        nested_graph_rel.left_connection, nested_graph_rel.left
                                    )))?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(nested_graph_rel.left_connection.clone()),
                                    use_final: false,
                                })
                            }
                        } else {
                            // If right doesn't have FROM, we need to determine which node should be the anchor
                            // Use find_anchor_node logic to choose the correct anchor
                            let all_connections = get_all_relationship_connections(&self);
                            let optional_aliases = std::collections::HashSet::new();
                            let denormalized_aliases = std::collections::HashSet::new();

                            if let Some(anchor_alias) =
                                find_anchor_node(&all_connections, &optional_aliases, &denormalized_aliases)
                            {
                                // Determine which node (left or right) the anchor corresponds to
                                let (table_plan, connection_alias) =
                                    if anchor_alias == graph_rel.left_connection {
                                        (&graph_rel.left, &graph_rel.left_connection)
                                    } else {
                                        (&graph_rel.right, &graph_rel.right_connection)
                                    };

                                let table_name = extract_table_name(table_plan)
                                    .ok_or_else(|| super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for anchor alias '{}', plan: {:?}",
                                        connection_alias, table_plan
                                    )))?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(connection_alias.clone()),
                                    use_final: false,
                                })
                            } else {
                                // Fallback: use left_connection as anchor (traditional behavior)
                                let table_name = extract_table_name(&graph_rel.left)
                                    .ok_or_else(|| super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for alias '{}', plan: {:?}",
                                        graph_rel.left_connection, graph_rel.left
                                    )))?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(graph_rel.left_connection.clone()),
                                    use_final: false,
                                })
                            }
                        }
                    }
                }
            }
            LogicalPlan::Filter(filter) => from_table_to_view_ref(filter.input.extract_from()?),
            LogicalPlan::Projection(projection) => {
                from_table_to_view_ref(projection.input.extract_from()?)
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                // SIMPLE RULE: No joins = use relationship table, otherwise use first join
                if graph_joins.joins.is_empty() {
                    // DENORMALIZED: No physical node tables, only the relationship table exists
                    fn find_graph_rel(plan: &LogicalPlan) -> Option<&GraphRel> {
                        match plan {
                            LogicalPlan::GraphRel(gr) => Some(gr),
                            LogicalPlan::Projection(proj) => find_graph_rel(&proj.input),
                            LogicalPlan::Filter(filter) => find_graph_rel(&filter.input),
                            _ => None,
                        }
                    }
                    
                    // Helper to find GraphNode for node-only queries
                    fn find_graph_node(plan: &LogicalPlan) -> Option<&crate::query_planner::logical_plan::GraphNode> {
                        match plan {
                            LogicalPlan::GraphNode(gn) => Some(gn),
                            LogicalPlan::Projection(proj) => find_graph_node(&proj.input),
                            LogicalPlan::Filter(filter) => find_graph_node(&filter.input),
                            _ => None,
                        }
                    }
                    
                    if let Some(graph_rel) = find_graph_rel(&graph_joins.input) {
                        if let Some(rel_table) = extract_table_name(&graph_rel.center) {
                            log::info!(
                                "🎯 DENORMALIZED: No JOINs, using relationship table '{}' as '{}'",
                                rel_table, graph_rel.alias
                            );
                            // CRITICAL FIX: Pass the actual graph_rel as source so extract_filters() can find view_filter
                            let view_ref = super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::GraphRel(graph_rel.clone())),
                                name: rel_table,
                                alias: Some(graph_rel.alias.clone()),
                                use_final: false,
                            };
                            return Ok(from_table_to_view_ref(Some(FromTable::new(Some(view_ref)))).map(|vr| FromTable::new(Some(vr))));
                        }
                    }
                    
                    // NODE-ONLY QUERY: No GraphRel, look for GraphNode
                    if let Some(graph_node) = find_graph_node(&graph_joins.input) {
                        log::info!(
                            "🎯 NODE-ONLY: No JOINs, no GraphRel, using GraphNode alias '{}' for FROM",
                            graph_node.alias
                        );
                        // Get table from GraphNode's ViewScan
                        if let LogicalPlan::ViewScan(scan) = graph_node.input.as_ref() {
                            let view_ref = super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::GraphNode(graph_node.clone())),
                                name: scan.source_table.clone(),
                                alias: Some(graph_node.alias.clone()),
                                use_final: scan.use_final,
                            };
                            log::info!(
                                "🎯 NODE-ONLY: Created ViewTableRef for table '{}' as '{}'",
                                scan.source_table, graph_node.alias
                            );
                            return Ok(from_table_to_view_ref(Some(FromTable::new(Some(view_ref)))).map(|vr| FromTable::new(Some(vr))));
                        }
                    }
                    
                    return Ok(from_table_to_view_ref(None).map(|vr| FromTable::new(Some(vr))));
                }

                // NORMAL PATH: JOINs exist, use existing anchor logic
                // Helper function to unwrap Projection/Filter layers to find GraphRel
                fn find_graph_rel(plan: &LogicalPlan) -> Option<&GraphRel> {
                    match plan {
                        LogicalPlan::GraphRel(gr) => Some(gr),
                        LogicalPlan::Projection(proj) => find_graph_rel(&proj.input),
                        LogicalPlan::Filter(filter) => find_graph_rel(&filter.input),
                        _ => None,
                    }
                }

                // DENORMALIZED: If no joins, just use the relationship table
                if graph_joins.joins.is_empty() {
                    if let Some(graph_rel) = find_graph_rel(&graph_joins.input) {
                        if let Some(rel_table) = extract_table_name(&graph_rel.center) {
                            log::info!("🎯 DENORMALIZED: No JOINs, using relationship table '{}' as '{}'", rel_table, graph_rel.alias);
                            // CRITICAL FIX: Pass the actual graph_rel as source so extract_filters() can find view_filter
                            Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::GraphRel(graph_rel.clone())),
                                name: rel_table,
                                alias: Some(graph_rel.alias.clone()),
                                use_final: false,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                // NORMAL PATH with JOINs: Try to find GraphRel through any Projection/Filter wrappers
                if let Some(graph_rel) = find_graph_rel(&graph_joins.input) {
                    if let Some(labels) = &graph_rel.labels {
                        if labels.len() > 1 {
                            // Multiple relationship types: need both start and end nodes in FROM
                            // Get end node from GraphRel
                            let end_from = graph_rel.right.extract_from()?;

                            // Return the end node - start node will be added as CROSS JOIN
                            from_table_to_view_ref(end_from)
                        } else {
                            // Single relationship type: Use anchor table from GraphJoins
                            // The anchor was already computed during join reordering
                            let anchor_alias = &graph_joins.anchor_table;

                            if let Some(anchor_alias) = anchor_alias {
                                log::info!(
                                    "Using anchor table from GraphJoins: {}",
                                    anchor_alias
                                );
                                // Get the table name for the anchor node by recursively finding the GraphNode with matching alias
                                if let Some(table_name) =
                                    find_table_name_for_alias(&graph_joins.input, anchor_alias)
                                {
                                    Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(LogicalPlan::Empty),
                                        name: table_name,
                                        alias: Some(anchor_alias.clone()),
                                        use_final: false,
                                    })
                                } else {
                                    // Fallback to first join
                                    if let Some(first_join) = graph_joins.joins.first() {
                                        Some(super::ViewTableRef {
                                            source: std::sync::Arc::new(LogicalPlan::Empty),
                                            name: first_join.table_name.clone(),
                                            alias: Some(first_join.table_alias.clone()),
                                            use_final: false,
                                        })
                                    } else {
                                        None
                                    }
                                }
                            } else {
                                // No anchor found, use first join
                                if let Some(first_join) = graph_joins.joins.first() {
                                    Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(LogicalPlan::Empty),
                                        name: first_join.table_name.clone(),
                                        alias: Some(first_join.table_alias.clone()),
                                        use_final: false,
                                    })
                                } else {
                                    None
                                }
                            }
                        }
                    } else {
                        // No labels: Use anchor table from GraphJoins
                        let anchor_alias = &graph_joins.anchor_table;

                        if let Some(anchor_alias) = anchor_alias {
                            // Get the table name for the anchor node
                            if let Some(table_name) =
                                find_table_name_for_alias(&graph_joins.input, anchor_alias)
                            {
                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(anchor_alias.clone()),
                                    use_final: false,
                                })
                            } else {
                                if let Some(first_join) = graph_joins.joins.first() {
                                    Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(LogicalPlan::Empty),
                                        name: first_join.table_name.clone(),
                                        alias: Some(first_join.table_alias.clone()),
                                        use_final: false,
                                    })
                                } else {
                                    None
                                }
                            }
                        } else {
                            // Not a GraphRel input: fallback to first join
                            if let Some(first_join) = graph_joins.joins.first() {
                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: first_join.table_name.clone(),
                                    alias: Some(first_join.table_alias.clone()),
                                    use_final: false,
                                })
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    // Not a GraphRel input: normal processing
                    // First try to extract FROM from the input
                    let input_from = graph_joins.input.extract_from()?;
                    if input_from.is_some() {
                        from_table_to_view_ref(input_from)
                    } else {
                        // If input has no FROM clause but we have joins, use the first join as FROM
                        // This handles the case of simple relationships where GraphRel returns None
                        if let Some(first_join) = graph_joins.joins.first() {
                            Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: first_join.table_name.clone(),
                                alias: Some(first_join.table_alias.clone()),
                                use_final: false,
                            })
                        } else {
                            None
                        }
                    }
                }
                } // close the else block for joins.is_empty() check
            }
            LogicalPlan::GroupBy(group_by) => {
                from_table_to_view_ref(group_by.input.extract_from()?)
            }
            LogicalPlan::OrderBy(order_by) => {
                from_table_to_view_ref(order_by.input.extract_from()?)
            }
            LogicalPlan::Skip(skip) => from_table_to_view_ref(skip.input.extract_from()?),
            LogicalPlan::Limit(limit) => from_table_to_view_ref(limit.input.extract_from()?),
            LogicalPlan::Cte(cte) => from_table_to_view_ref(cte.input.extract_from()?),
            LogicalPlan::Union(_) => None,
            LogicalPlan::PageRank(_) => None,
        };
        Ok(view_ref_to_from_table(from_ref))
    }

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let filters = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::ViewScan(scan) => {
                // ViewScan.view_filter should be None after CleanupViewScanFilters optimizer.
                // All filters are consolidated in GraphRel.where_predicate.
                // This case handles standalone ViewScans outside of GraphRel contexts.
                if let Some(ref filter) = scan.view_filter {
                    let mut expr: RenderExpr = filter.clone().try_into()?;
                    // Apply property mapping to the filter expression
                    apply_property_mapping_to_expr(&mut expr, &LogicalPlan::ViewScan(scan.clone()));
                    Some(expr)
                } else {
                    None
                }
            }
            LogicalPlan::GraphNode(graph_node) => {
                // GraphNode's filters are handled by the parent GraphRel.
                // Don't extract from the input ViewScan to avoid duplicates.
                None
            }
            LogicalPlan::GraphRel(graph_rel) => {
                log::trace!("GraphRel node detected, collecting filters from ALL nested where_predicates");

                // Collect all where_predicates from this GraphRel and nested GraphRel nodes
                // This fixes the bug where only ONE GraphRel's predicate was being used
                fn collect_graphrel_predicates(plan: &LogicalPlan) -> Vec<RenderExpr> {
                    let mut predicates = Vec::new();
                    match plan {
                        LogicalPlan::GraphRel(gr) => {
                            // Add this GraphRel's predicate
                            if let Some(ref pred) = gr.where_predicate {
                                if let Ok(render_expr) = RenderExpr::try_from(pred.clone()) {
                                    predicates.push(render_expr);
                                }
                            }
                            // Recursively collect from children (to get nested GraphRel predicates)
                            predicates.extend(collect_graphrel_predicates(&gr.left));
                            predicates.extend(collect_graphrel_predicates(&gr.center));
                            predicates.extend(collect_graphrel_predicates(&gr.right));
                        }
                        LogicalPlan::GraphNode(gn) => {
                            predicates.extend(collect_graphrel_predicates(&gn.input));
                        }
                        LogicalPlan::ViewScan(_scan) => {
                            // ViewScan.view_filter should be empty after CleanupViewScanFilters optimizer
                            // All filters come from GraphRel.where_predicate
                        }
                        // Don't recurse into other node types - only GraphRel/GraphNode/ViewScan
                        _ => {}
                    }
                    predicates
                }

                let all_predicates = collect_graphrel_predicates(&LogicalPlan::GraphRel(graph_rel.clone()));
                
                let mut all_predicates = all_predicates;
                
                // 🚀 ADD CYCLE PREVENTION for fixed-length paths
                if let Some(spec) = &graph_rel.variable_length {
                    if let Some(exact_hops) = spec.exact_hop_count() {
                        if graph_rel.shortest_path_mode.is_none() {
                            println!("DEBUG: extract_filters - Adding cycle prevention for fixed-length *{}", exact_hops);
                            
                            // Extract table/column info for cycle prevention
                            let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                                .unwrap_or_else(|| "User".to_string());
                            let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                                .unwrap_or_else(|| "User".to_string());
                            let start_table = label_to_table_name(&start_label);
                            let end_table = label_to_table_name(&end_label);
                            
                            let start_id_col = extract_id_column(&graph_rel.left)
                                .unwrap_or_else(|| table_to_id_column(&start_table));
                            let end_id_col = extract_id_column(&graph_rel.right)
                                .unwrap_or_else(|| table_to_id_column(&end_table));
                            
                            let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                                RelationshipColumns {
                                    from_id: "from_node_id".to_string(),
                                    to_id: "to_node_id".to_string(),
                                },
                            );
                            
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
                                println!("DEBUG: extract_filters - Generated cycle prevention filter");
                                all_predicates.push(cycle_filter);
                            }
                        }
                    }
                }
                
                if all_predicates.is_empty() {
                    None
                } else if all_predicates.len() == 1 {
                    log::trace!("Found 1 GraphRel predicate");
                    Some(all_predicates.into_iter().next().unwrap())
                } else {
                    // Combine with AND
                    log::trace!("Found {} GraphRel predicates, combining with AND", all_predicates.len());
                    let combined = all_predicates.into_iter().reduce(|acc, pred| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![acc, pred],
                        })
                    }).unwrap();
                    Some(combined)
                }
            }
            LogicalPlan::Filter(filter) => {
                println!(
                    "DEBUG: extract_filters - Found Filter node with predicate: {:?}",
                    filter.predicate
                );
                println!(
                    "DEBUG: extract_filters - Filter input type: {:?}",
                    std::mem::discriminant(&*filter.input)
                );
                let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut expr, &filter.input);
                println!("DEBUG: extract_filters - Returning Some(expr) from Filter");
                Some(expr)
            }
            LogicalPlan::Projection(projection) => {
                println!("DEBUG: extract_filters - Projection, recursing to input type: {:?}", std::mem::discriminant(&*projection.input));
                projection.input.extract_filters()?
            }
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_filters()?,
            LogicalPlan::Limit(limit) => limit.input.extract_filters()?,
            LogicalPlan::Cte(cte) => cte.input.extract_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_filters()?,
            LogicalPlan::Union(_) => None,
            LogicalPlan::PageRank(_) => None,
        };
        Ok(filters)
    }

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let final_filters = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_final_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_final_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_final_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_final_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_final_filters()?,
            LogicalPlan::Projection(projection) => projection.input.extract_final_filters()?,
            LogicalPlan::Filter(filter) => {
                let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut expr, &filter.input);
                Some(expr)
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // For GraphRel, extract path function filters that should be applied to the final query
                if let Some(logical_expr) = &graph_rel.where_predicate {
                    let mut filter_expr: RenderExpr = logical_expr.clone().try_into()?;
                    // Apply property mapping to the where predicate
                    apply_property_mapping_to_expr(
                        &mut filter_expr,
                        &LogicalPlan::GraphRel(graph_rel.clone()),
                    );
                    let start_alias = graph_rel.left_connection.clone();
                    let end_alias = graph_rel.right_connection.clone();

                    let categorized = categorize_filters(
                        Some(&filter_expr),
                        &start_alias,
                        &end_alias,
                        &graph_rel.alias,
                    );

                    categorized.path_function_filters
                } else {
                    None
                }
            }
            _ => None,
        };
        Ok(final_filters)
    }

    fn extract_joins(&self) -> RenderPlanBuilderResult<Vec<Join>> {
        let joins = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_joins()?,
            LogicalPlan::Skip(skip) => skip.input.extract_joins()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_joins()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_joins()?,
            LogicalPlan::Filter(filter) => filter.input.extract_joins()?,
            LogicalPlan::Projection(projection) => projection.input.extract_joins()?,
            LogicalPlan::GraphNode(graph_node) => {
                // For nested GraphNodes (multiple standalone nodes), create CROSS JOINs
                let mut joins = vec![];

                // If this GraphNode has another GraphNode as input, create a CROSS JOIN for the inner node
                if let LogicalPlan::GraphNode(inner_node) = graph_node.input.as_ref() {
                    if let Some(table_name) = extract_table_name(&graph_node.input) {
                        joins.push(Join {
                            table_name,
                            table_alias: inner_node.alias.clone(), // Use the inner GraphNode's alias
                            joining_on: vec![],                    // Empty for CROSS JOIN
                            join_type: JoinType::Join,             // CROSS JOIN
                        });
                    }
                }

                // Recursively get joins from the input
                let mut inner_joins = graph_node.input.extract_joins()?;
                joins.append(&mut inner_joins);

                joins
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                // Use the pre-computed joins from GraphJoinInference analyzer
                // These were carefully constructed to handle OPTIONAL MATCH, multi-hop, etc.
                println!(
                    "DEBUG: GraphJoins extract_joins - using pre-computed joins from analyzer"
                );
                println!(
                    "DEBUG: graph_joins.joins.len() = {}",
                    graph_joins.joins.len()
                );

                // Convert from logical_plan::Join to render_plan::Join
                graph_joins
                    .joins
                    .iter()
                    .map(|j| j.clone().try_into())
                    .collect::<Result<Vec<Join>, RenderBuildError>>()?
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate JOINs for the relationship traversal
                // This fixes OPTIONAL MATCH queries by creating proper JOIN clauses

                // 🚀 FIXED-LENGTH OPTIMIZATION: Check if this is a fixed-length pattern
                if let Some(spec) = &graph_rel.variable_length {
                    if let Some(exact_hops) = spec.exact_hop_count() {
                        if graph_rel.shortest_path_mode.is_none() {
                            println!(
                                "DEBUG: extract_joins - Fixed-length pattern (*{}) - generating inline JOINs",
                                exact_hops
                            );
                            
                            // Extract table information
                            let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                                .unwrap_or_else(|| "User".to_string());
                            let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                                .unwrap_or_else(|| "User".to_string());
                            let start_table = label_to_table_name(&start_label);
                            let end_table = label_to_table_name(&end_label);
                            
                            let rel_table = if let Some(labels) = &graph_rel.labels {
                                if !labels.is_empty() {
                                    rel_type_to_table_name(&labels[0])
                                } else {
                                    extract_table_name(&graph_rel.center)
                                        .unwrap_or_else(|| graph_rel.alias.clone())
                                }
                            } else {
                                extract_table_name(&graph_rel.center)
                                    .unwrap_or_else(|| graph_rel.alias.clone())
                            };
                            
                            // Extract ID columns
                            let start_id_col = extract_id_column(&graph_rel.left)
                                .unwrap_or_else(|| table_to_id_column(&start_table));
                            let end_id_col = extract_id_column(&graph_rel.right)
                                .unwrap_or_else(|| table_to_id_column(&end_table));
                            
                            // Get relationship columns
                            let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                                RelationshipColumns {
                                    from_id: "from_node_id".to_string(),
                                    to_id: "to_node_id".to_string(),
                                },
                            );
                            
                            // Generate inline JOINs using the new function
                            let fixed_length_joins = crate::render_plan::cte_extraction::expand_fixed_length_joins(
                                exact_hops,
                                &start_table,
                                &start_id_col,
                                &rel_table,
                                &rel_cols.from_id,
                                &rel_cols.to_id,
                                &end_table,
                                &end_id_col,
                                &graph_rel.left_connection,
                                &graph_rel.right_connection,
                            );
                            
                            return Ok(fixed_length_joins);
                        }
                    }
                }

                // MULTI-HOP FIX: If left side is another GraphRel, recursively extract its joins first
                // This handles patterns like (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)
                let mut joins = vec![];
                
                // DENORMALIZED EDGE TABLE CHECK
                // For denormalized patterns, nodes are virtual (stored on edge table)
                // We need to JOIN edge tables directly, not node tables
                let left_is_denormalized = is_node_denormalized(&graph_rel.left);
                let right_is_denormalized = is_node_denormalized(&graph_rel.right);
                
                println!(
                    "DEBUG: extract_joins - left_is_denormalized={}, right_is_denormalized={}",
                    left_is_denormalized, right_is_denormalized
                );
                
                // For denormalized patterns, handle specially
                if left_is_denormalized && right_is_denormalized {
                    println!("DEBUG: DENORMALIZED multi-hop pattern detected");
                    
                    // Get the relationship table 
                    let rel_table = extract_table_name(&graph_rel.center)
                        .unwrap_or_else(|| graph_rel.alias.clone());
                    
                    // Get relationship columns (from_id and to_id)
                    let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                        RelationshipColumns {
                            from_id: "from_node_id".to_string(),
                            to_id: "to_node_id".to_string(),
                        },
                    );
                    
                    // Check if this is a chained hop (left side is another GraphRel)
                    if let LogicalPlan::GraphRel(left_rel) = graph_rel.left.as_ref() {
                        println!(
                            "DEBUG: DENORMALIZED multi-hop - chaining {} -> {}",
                            left_rel.alias, graph_rel.alias
                        );
                        
                        // First, recursively get joins from the left GraphRel
                        let mut left_joins = graph_rel.left.extract_joins()?;
                        joins.append(&mut left_joins);
                        
                        // Get the left relationship's to_id column for joining
                        let left_rel_cols = extract_relationship_columns(&left_rel.center).unwrap_or(
                            RelationshipColumns {
                                from_id: "from_node_id".to_string(),
                                to_id: "to_node_id".to_string(),
                            },
                        );
                        
                        // JOIN this relationship table to the previous one
                        // e.g., INNER JOIN flights AS f2 ON f2.Origin = f1.Dest
                        joins.push(Join {
                            table_name: rel_table.clone(),
                            table_alias: graph_rel.alias.clone(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: Column(PropertyValue::Column(rel_cols.from_id.clone())),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_rel.alias.clone()),
                                        column: Column(PropertyValue::Column(left_rel_cols.to_id.clone())),
                                    }),
                                ],
                            }],
                            join_type: JoinType::Inner,
                        });
                    }
                    // For single-hop denormalized, no JOINs needed - relationship table IS the data
                    // Just return empty joins, the FROM clause will use the relationship table
                    
                    return Ok(joins);
                }
                
                // STANDARD (non-denormalized) multi-hop handling
                if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    println!(
                        "DEBUG: Multi-hop pattern detected - recursively extracting left GraphRel joins"
                    );
                    let mut left_joins = graph_rel.left.extract_joins()?;
                    joins.append(&mut left_joins);
                }

                // First, check if the plan_ctx marks this relationship as optional
                // This is set by OPTIONAL MATCH clause processing
                let is_optional = graph_rel.is_optional.unwrap_or(false);
                let join_type = if is_optional {
                    JoinType::Left
                } else {
                    JoinType::Inner
                };

                // Extract table names and columns
                let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                    .unwrap_or_else(|| "User".to_string());
                let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                    .unwrap_or_else(|| "User".to_string());
                let start_table = label_to_table_name(&start_label);
                let end_table = label_to_table_name(&end_label);

                // Get relationship table
                let rel_table = if let Some(labels) = &graph_rel.labels {
                    if !labels.is_empty() {
                        rel_type_to_table_name(&labels[0])
                    } else {
                        extract_table_name(&graph_rel.center)
                            .unwrap_or_else(|| graph_rel.alias.clone())
                    }
                } else {
                    extract_table_name(&graph_rel.center).unwrap_or_else(|| graph_rel.alias.clone())
                };

                // MULTI-HOP FIX: For ID columns, use table lookup based on connection aliases
                // instead of extract_id_column which fails for nested GraphRel
                // The left_connection tells us which node alias we're connecting from
                let start_id_col = if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    // Multi-hop: left side is another GraphRel, so left_connection points to intermediate node
                    // Look up the node's table and get its ID column
                    println!(
                        "DEBUG: Multi-hop - left_connection={}, using table lookup for ID column",
                        graph_rel.left_connection
                    );
                    table_to_id_column(&start_table)
                } else {
                    // Single hop: extract ID column from the node ViewScan
                    extract_id_column(&graph_rel.left)
                        .unwrap_or_else(|| table_to_id_column(&start_table))
                };
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));

                // Get relationship columns
                let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
                    RelationshipColumns {
                        from_id: "from_node_id".to_string(),
                        to_id: "to_node_id".to_string(),
                    },
                );

                // OPTIONAL MATCH FIX: For incoming optional relationships like (b:User)-[:FOLLOWS]->(a)
                // where 'a' is required and 'b' is optional, we need to reverse the JOIN order:
                // 1. JOIN b first (optional node)
                // 2. Then JOIN relationship (can reference both a and b)
                //
                // Detect this case: is_optional=true AND FROM clause is right node
                // (The FROM clause selection logic prefers right node when is_optional=true)
                let reverse_join_order = is_optional;

                if reverse_join_order {
                    println!(
                        "DEBUG: Reversing JOIN order for optional relationship (b)-[:FOLLOWS]->(a) where a is FROM"
                    );

                    // JOIN 1: End node (optional left node 'b')
                    //   e.g., LEFT JOIN users AS b ON b.user_id = r.to_node_id
                    joins.push(Join {
                        table_name: start_table.clone(),
                        table_alias: graph_rel.left_connection.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.left_connection.clone()),
                                    column: Column(PropertyValue::Column(start_id_col.clone())),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: Column(PropertyValue::Column(rel_cols.from_id.clone())),
                                }),
                            ],
                        }],
                        join_type: join_type.clone(),
                    });

                    // JOIN 2: Relationship table (can now reference both nodes)
                    //   e.g., LEFT JOIN follows AS r ON r.follower_id = b.user_id AND r.followed_id = a.user_id
                    joins.push(Join {
                        table_name: rel_table.clone(),
                        table_alias: graph_rel.alias.clone(),
                        joining_on: vec![
                            OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: Column(PropertyValue::Column(rel_cols.from_id.clone())),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.left_connection.clone()),
                                        column: Column(PropertyValue::Column(start_id_col.clone())),
                                    }),
                                ],
                            },
                            OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: Column(PropertyValue::Column(rel_cols.to_id.clone())),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.right_connection.clone()),
                                        column: Column(PropertyValue::Column(end_id_col.clone())),
                                    }),
                                ],
                            },
                        ],
                        join_type: join_type.clone(),
                    });
                } else {
                    // Normal order: relationship first, then end node
                    // JOIN 1: Start node -> Relationship table
                    //   e.g., INNER JOIN follows AS r ON r.from_node_id = a.user_id
                    joins.push(Join {
                        table_name: rel_table.clone(),
                        table_alias: graph_rel.alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: Column(PropertyValue::Column(rel_cols.from_id.clone())),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.left_connection.clone()),
                                    column: Column(PropertyValue::Column(start_id_col.clone())),
                                }),
                            ],
                        }],
                        join_type: join_type.clone(),
                    });

                    // JOIN 2: Relationship table -> End node
                    //   e.g., LEFT JOIN users AS b ON b.user_id = r.to_node_id
                    joins.push(Join {
                        table_name: end_table,
                        table_alias: graph_rel.right_connection.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.right_connection.clone()),
                                    column: Column(PropertyValue::Column(end_id_col.clone())),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: Column(PropertyValue::Column(rel_cols.to_id.clone())),
                                }),
                            ],
                        }],
                        join_type,
                    });
                }

                joins
            }
            _ => vec![],
        };
        Ok(joins)
    }

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>> {
        let group_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_group_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_group_by()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_group_by()?,
            LogicalPlan::GroupBy(group_by) => group_by
                .expressions
                .iter()
                .cloned()
                .map(|expr| {
                    let mut render_expr: RenderExpr = expr.try_into()?;
                    // Apply property mapping to the group by expression
                    apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
                    Ok(render_expr)
                })
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?, //.collect::<Vec<RenderExpr>>(),
            _ => vec![],
        };
        Ok(group_by)
    }

    fn extract_having(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let having_clause = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_having()?,
            LogicalPlan::Skip(skip) => skip.input.extract_having()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_having()?,
            LogicalPlan::Projection(projection) => projection.input.extract_having()?,
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

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
        let order_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_order_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_order_by()?,
            LogicalPlan::OrderBy(order_by) => order_by
                .items
                .iter()
                .cloned()
                .map(|item| {
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

    fn extract_skip(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_skip(),
            LogicalPlan::Skip(skip) => Some(skip.count),
            _ => None,
        }
    }

    fn extract_limit(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => Some(limit.count),
            _ => None,
        }
    }

    fn extract_union(&self) -> RenderPlanBuilderResult<Option<Union>> {
        use crate::graph_catalog::graph_schema::GraphSchema;
        use std::collections::HashMap;
        let empty_schema =
            GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());

        let union_opt = match &self {
            LogicalPlan::Union(union) => Some(Union {
                input: union
                    .inputs
                    .iter()
                    .map(|input| input.to_render_plan(&empty_schema))
                    .collect::<Result<Vec<RenderPlan>, RenderBuildError>>()?,
                union_type: union.union_type.clone().try_into()?,
            }),
            _ => None,
        };
        Ok(union_opt)
    }

    /// Try to build a JOIN-based render plan for simple queries
    /// Returns Ok(plan) if successful, Err(_) if this query needs CTE-based processing
    fn try_build_join_based_plan(&self) -> RenderPlanBuilderResult<RenderPlan> {
        println!("DEBUG: try_build_join_based_plan called");
        println!("DEBUG: self plan type = {:?}", std::mem::discriminant(self));

        // Extract DISTINCT flag BEFORE unwrapping OrderBy/Limit/Skip
        let distinct = self.extract_distinct();
        println!("DEBUG: try_build_join_based_plan - extracted distinct: {}", distinct);

        // First, extract ORDER BY/LIMIT/SKIP if present
        let (core_plan, order_by_items, limit_val, skip_val) = match self {
            LogicalPlan::Limit(limit_node) => {
                println!("DEBUG: Found Limit node, checking input...");
                match limit_node.input.as_ref() {
                    LogicalPlan::OrderBy(order_node) => {
                        println!("DEBUG: Limit input is OrderBy with {} items", order_node.items.len());
                        (
                            order_node.input.as_ref(),
                            Some(&order_node.items),
                            Some(limit_node.count),
                            None,
                        )
                    }
                    other => {
                        println!("DEBUG: Limit input is NOT OrderBy: {:?}", std::mem::discriminant(other));
                        (other, None, Some(limit_node.count), None)
                    }
                }
            }
            LogicalPlan::OrderBy(order_node) => (
                order_node.input.as_ref(),
                Some(&order_node.items),
                None,
                None,
            ),
            LogicalPlan::Skip(skip_node) => {
                (skip_node.input.as_ref(), None, None, Some(skip_node.count))
            }
            other => {
                println!("DEBUG: self is NOT Limit/OrderBy/Skip: {:?}", std::mem::discriminant(other));
                (other, None, None, None)
            }
        };
        
        println!("DEBUG: order_by_items present = {}", order_by_items.is_some());

        // Check if the core plan contains a Union (denormalized node-only queries)
        // For Union, we need to build each branch separately and combine them
        // If branches have aggregation, we'll handle it specially (subquery + outer GROUP BY)
        if let Some(union) = find_nested_union(core_plan) {
            println!("DEBUG: Found nested Union with {} inputs, building UNION ALL plan", union.inputs.len());
            
            use crate::graph_catalog::graph_schema::GraphSchema;
            use std::collections::HashMap;
            let empty_schema = GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());
            
            // Build render plan for each Union branch
            // NOTE: Don't add LIMIT to branches - LIMIT applies to the combined UNION result
            let union_plans: Result<Vec<RenderPlan>, RenderBuildError> = union.inputs.iter().map(|branch| {
                branch.to_render_plan(&empty_schema)
            }).collect();
            
            let union_plans = union_plans?;
            
            // Check if the OUTER plan has GROUP BY or aggregation
            // This happens when return_clause.rs keeps aggregation at the outer level
            // We need to extract this info from core_plan (which wraps the Union)
            let outer_aggregation_info = extract_outer_aggregation_info(core_plan);
            
            println!("DEBUG: outer_aggregation_info = {:?}", outer_aggregation_info.is_some());
            
            if let Some((outer_select, outer_group_by)) = outer_aggregation_info {
                println!("DEBUG: Creating aggregation-aware UNION plan with {} outer SELECT items, {} GROUP BY", 
                    outer_select.len(), outer_group_by.len());
                
                // The union branches already have the correct base columns (no aggregation)
                // We just need to apply outer SELECT and GROUP BY on top
                
                // Convert ORDER BY for outer query
                let order_by_items_converted: Vec<OrderByItem> = if let Some(items) = order_by_items {
                    items.iter().filter_map(|item| {
                        use crate::query_planner::logical_expr::LogicalExpr;
                        match &item.expression {
                            LogicalExpr::PropertyAccessExp(prop) => {
                                Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!("\"{}.{}\"", prop.table_alias.0, prop.column.raw())),
                                    order: item.order.clone().try_into().unwrap_or(OrderByOrder::Asc),
                                })
                            }
                            LogicalExpr::ColumnAlias(alias) => {
                                Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!("\"{}\"", alias.0)),
                                    order: item.order.clone().try_into().unwrap_or(OrderByOrder::Asc),
                                })
                            }
                            _ => None,
                        }
                    }).collect()
                } else {
                    vec![]
                };
                
                return Ok(RenderPlan {
                    ctes: CteItems(vec![]),
                    select: SelectItems {
                        items: outer_select,
                        distinct: distinct,
                    },
                    from: FromTableItem(None),
                    joins: JoinItems(vec![]),
                    filters: FilterItems(None),
                    group_by: GroupByExpressions(outer_group_by),
                    having_clause: None,
                    order_by: OrderByItems(order_by_items_converted),
                    skip: SkipItem(skip_val),
                    limit: LimitItem(limit_val),
                    union: UnionItems(Some(Union {
                        input: union_plans,
                        union_type: union.union_type.clone().try_into()?,
                    })),
                });
            }
            
            // Also check if branches have GROUP BY with aggregation (legacy case where analyzers pushed it down)
            let branches_have_aggregation = union_plans.iter().any(|plan| {
                !plan.group_by.0.is_empty() || 
                plan.select.items.iter().any(|item| matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
            });
            
            println!("DEBUG: branches_have_aggregation = {}", branches_have_aggregation);
            
            if branches_have_aggregation {
                // Extract GROUP BY and aggregation from first branch (all branches should be similar)
                let first_plan = union_plans.first().ok_or_else(|| {
                    RenderBuildError::InvalidRenderPlan("Union has no inputs".to_string())
                })?;
                
                // Collect non-aggregate SELECT items (these become GROUP BY columns)
                let base_select_items: Vec<SelectItem> = first_plan.select.items.iter()
                    .filter(|item| !matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
                    .cloned()
                    .collect();
                
                // If there are no base columns but there are aggregates, use constant 1
                let branch_select = if base_select_items.is_empty() {
                    SelectItems {
                        items: vec![SelectItem {
                            expression: RenderExpr::Literal(Literal::Integer(1)),
                            col_alias: Some(ColumnAlias("__dummy".to_string())),
                        }],
                        distinct: false,
                    }
                } else {
                    SelectItems {
                        items: base_select_items.clone(),
                        distinct: first_plan.select.distinct,
                    }
                };
                
                // Create stripped branch plans (no GROUP BY, no aggregation)
                let stripped_union_plans: Vec<RenderPlan> = union_plans.iter().map(|plan| {
                    // Extract only the non-aggregate SELECT items from this branch
                    let branch_items: Vec<SelectItem> = if base_select_items.is_empty() {
                        vec![SelectItem {
                            expression: RenderExpr::Literal(Literal::Integer(1)),
                            col_alias: Some(ColumnAlias("__dummy".to_string())),
                        }]
                    } else {
                        plan.select.items.iter()
                            .filter(|item| !matches!(&item.expression, RenderExpr::AggregateFnCall(_)))
                            .cloned()
                            .collect()
                    };
                    
                    RenderPlan {
                        ctes: CteItems(vec![]),
                        select: SelectItems {
                            items: branch_items,
                            distinct: plan.select.distinct,
                        },
                        from: plan.from.clone(),
                        joins: plan.joins.clone(),
                        filters: plan.filters.clone(),
                        group_by: GroupByExpressions(vec![]), // No GROUP BY in branches
                        having_clause: None,
                        order_by: OrderByItems(vec![]),
                        skip: SkipItem(None),
                        limit: LimitItem(None),
                        union: UnionItems(None),
                    }
                }).collect();
                
                // Build outer GROUP BY expressions (use column aliases from SELECT)
                let outer_group_by: Vec<RenderExpr> = base_select_items.iter()
                    .filter_map(|item| {
                        item.col_alias.as_ref().map(|alias| {
                            RenderExpr::Raw(format!("\"{}\"", alias.0))
                        })
                    })
                    .collect();
                
                // Build outer SELECT with aggregations referencing column aliases
                let outer_select_items: Vec<SelectItem> = first_plan.select.items.iter()
                    .map(|item| {
                        // For non-aggregates, reference the column alias
                        // For aggregates, keep as-is (they'll reference subquery columns)
                        if matches!(&item.expression, RenderExpr::AggregateFnCall(_)) {
                            item.clone()
                        } else {
                            // Use the column alias as the expression
                            if let Some(alias) = &item.col_alias {
                                SelectItem {
                                    expression: RenderExpr::Raw(format!("\"{}\"", alias.0)),
                                    col_alias: item.col_alias.clone(),
                                }
                            } else {
                                item.clone()
                            }
                        }
                    })
                    .collect();
                
                // Convert ORDER BY for outer query
                let order_by_items_converted: Vec<OrderByItem> = if let Some(items) = order_by_items {
                    items.iter().filter_map(|item| {
                        use crate::query_planner::logical_expr::LogicalExpr;
                        match &item.expression {
                            LogicalExpr::PropertyAccessExp(prop) => {
                                Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!("\"{}.{}\"", prop.table_alias.0, prop.column.raw())),
                                    order: item.order.clone().try_into().unwrap_or(OrderByOrder::Asc),
                                })
                            }
                            LogicalExpr::ColumnAlias(alias) => {
                                Some(OrderByItem {
                                    expression: RenderExpr::Raw(format!("\"{}\"", alias.0)),
                                    order: item.order.clone().try_into().unwrap_or(OrderByOrder::Asc),
                                })
                            }
                            _ => None,
                        }
                    }).collect()
                } else {
                    vec![]
                };
                
                println!("DEBUG: Creating aggregation-aware UNION plan with {} outer SELECT items, {} GROUP BY", 
                    outer_select_items.len(), outer_group_by.len());
                
                return Ok(RenderPlan {
                    ctes: CteItems(vec![]),
                    select: SelectItems {
                        items: outer_select_items,
                        distinct: first_plan.select.distinct,
                    },
                    from: FromTableItem(None),
                    joins: JoinItems(vec![]),
                    filters: FilterItems(None),
                    group_by: GroupByExpressions(outer_group_by),
                    having_clause: first_plan.having_clause.clone(),
                    order_by: OrderByItems(order_by_items_converted),
                    skip: SkipItem(skip_val),
                    limit: LimitItem(limit_val),
                    union: UnionItems(Some(Union {
                        input: stripped_union_plans,
                        union_type: union.union_type.clone().try_into()?,
                    })),
                });
            }
            
            // Non-aggregation case: use original logic
            // Create a render plan with the union field populated
            // The first branch provides the SELECT structure
            let first_plan = union_plans.first().ok_or_else(|| {
                RenderBuildError::InvalidRenderPlan("Union has no inputs".to_string())
            })?;
            
            // Convert ORDER BY items for UNION - use quoted alias names when possible
            // For UNION, ORDER BY must reference result column aliases.
            // If ORDER BY column matches a SELECT alias, use "alias"
            // If not, apply property mapping (for columns not in SELECT list)
            let order_by_items_converted: Vec<OrderByItem> = if let Some(items) = order_by_items {
                items.iter().filter_map(|item| {
                    use crate::query_planner::logical_expr::LogicalExpr;
                    
                    let expr = match &item.expression {
                        LogicalExpr::PropertyAccessExp(prop) => {
                            // Try to find matching SELECT item by table alias
                            let matching_select = first_plan.select.items.iter()
                                .find(|s| matches!(&s.expression, RenderExpr::PropertyAccessExp(p) if p.table_alias.0 == prop.table_alias.0));
                            
                            if let Some(select_item) = matching_select {
                                // Found matching SELECT item - use its alias
                                select_item.col_alias.as_ref()
                                    .map(|a| RenderExpr::Raw(format!("\"{}\"", a.0)))
                            } else {
                                // Not in SELECT - apply property mapping
                                let mut order_item: OrderByItem = item.clone().try_into().ok()?;
                                apply_property_mapping_to_expr(&mut order_item.expression, core_plan);
                                Some(order_item.expression)
                            }
                        }
                        LogicalExpr::ColumnAlias(alias) => Some(RenderExpr::Raw(format!("\"{}\"", alias.0))),
                        _ => None,
                    };
                    
                    expr.map(|e| OrderByItem {
                        expression: e,
                        order: item.order.clone().try_into().unwrap_or(OrderByOrder::Asc),
                    })
                }).collect()
            } else {
                vec![]
            };
            
            return Ok(RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems { items: vec![], distinct: false }, // Empty - let to_sql use SELECT *
                from: FromTableItem(None), // Union doesn't need FROM at top level
                joins: JoinItems(vec![]),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(order_by_items_converted),
                skip: SkipItem(skip_val),
                limit: LimitItem(limit_val), // LIMIT applies to entire UNION result
                union: UnionItems(Some(Union {
                    input: union_plans,
                    union_type: union.union_type.clone().try_into()?,
                })),
            });
        }

        // Check for GraphJoins wrapping Projection(Return) -> GroupBy pattern
        if let LogicalPlan::GraphJoins(graph_joins) = core_plan {
            // Check if there's a variable-length or shortest path pattern in the tree
            // These require recursive CTEs and cannot use inline JOINs
            if has_variable_length_or_shortest_path(&graph_joins.input) {
                println!(
                    "DEBUG: Variable-length or shortest path detected in GraphJoins tree, returning Err to use CTE path"
                );
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Variable-length or shortest path patterns require CTE-based processing"
                        .to_string(),
                ));
            }

            // Check if there's a multiple-relationship GraphRel anywhere in the tree
            if has_multiple_relationship_types(&graph_joins.input) {
                println!(
                    "DEBUG: Multiple relationship types detected in GraphJoins tree, returning Err to use CTE path"
                );
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Multiple relationship types require CTE-based processing with UNION"
                        .to_string(),
                ));
            }

            if let LogicalPlan::Projection(proj) = graph_joins.input.as_ref() {
                if matches!(
                    proj.kind,
                    crate::query_planner::logical_plan::ProjectionKind::Return
                ) {
                    if let LogicalPlan::GroupBy(group_by) = proj.input.as_ref() {
                        if group_by.having_clause.is_some() || !group_by.expressions.is_empty() {
                            println!(
                                "DEBUG: GraphJoins wrapping Projection(Return)->GroupBy detected, delegating to child"
                            );
                            // Delegate to the inner Projection -> GroupBy for CTE-based processing
                            let mut plan = graph_joins.input.try_build_join_based_plan()?;

                            // Add ORDER BY/LIMIT/SKIP if they were present in the original query
                            if let Some(items) = order_by_items {
                                // Rewrite ORDER BY expressions for CTE context
                                let mut order_by_items_vec = vec![];
                                for item in items {
                                    let rewritten_expr = match &item.expression {
                                        crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(col_alias) => {
                                            // ORDER BY column_alias -> ORDER BY grouped_data.column_alias
                                            RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: TableAlias("grouped_data".to_string()),
                                                    column: Column(PropertyValue::Column(col_alias.0.clone())),
                                                }
                                            )
                                        }
                                        other_expr => {
                                            // Try to convert the expression
                                            other_expr.clone().try_into()?
                                        }
                                    };
                                    order_by_items_vec.push(OrderByItem {
                                        expression: rewritten_expr,
                                        order: item.order.clone().try_into()?,
                                    });
                                }
                                plan.order_by = OrderByItems(order_by_items_vec);
                            }

                            if let Some(limit) = limit_val {
                                plan.limit = LimitItem(Some(limit));
                            }

                            if let Some(skip) = skip_val {
                                plan.skip = SkipItem(Some(skip));
                            }

                            return Ok(plan);
                        }
                    }
                }
            }
        }

        // Check if this query needs CTE-based processing
        if let LogicalPlan::Projection(proj) = self {
            if let LogicalPlan::GraphRel(graph_rel) = proj.input.as_ref() {
                // Variable-length paths: check if truly variable or just fixed-length
                if let Some(spec) = &graph_rel.variable_length {
                    let is_fixed_length = spec.exact_hop_count().is_some() 
                        && graph_rel.shortest_path_mode.is_none();
                    
                    if is_fixed_length {
                        // 🚀 Fixed-length pattern (*2, *3) - can use inline JOINs!
                        println!(
                            "DEBUG: Fixed-length pattern (*{}) detected - will use inline JOINs",
                            spec.exact_hop_count().unwrap()
                        );
                        // Continue to extract_joins() path
                    } else {
                        // Truly variable-length (*1.., *0..5) or shortest path - needs CTE
                        println!("DEBUG: Variable-length pattern detected, returning Err to use CTE path");
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "Variable-length paths require CTE-based processing".to_string(),
                        ));
                    }
                }

                // Multiple relationship types need UNION CTEs
                if let Some(labels) = &graph_rel.labels {
                    if labels.len() > 1 {
                        println!(
                            "DEBUG: Multiple relationship types detected ({}), returning Err to use CTE path",
                            labels.len()
                        );
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "Multiple relationship types require CTE-based processing with UNION"
                                .to_string(),
                        ));
                    }
                }
            }
        }

        // Try to build with JOINs - this will work for:
        // - Simple MATCH queries with relationships
        // - OPTIONAL MATCH queries (via GraphRel.extract_joins)
        // - Multiple MATCH clauses (via GraphRel.extract_joins)
        // It will fail (return Err) for:
        // - Variable-length paths (need recursive CTEs)
        // - Multiple relationship types (need UNION CTEs)
        // - Complex nested queries
        // - Queries that don't have extractable JOINs

        println!("DEBUG: Calling build_simple_relationship_render_plan with distinct: {}", distinct);
        self.build_simple_relationship_render_plan(Some(distinct))
    }

    /// Build render plan for simple relationship queries using direct JOINs
    fn build_simple_relationship_render_plan(&self, distinct_override: Option<bool>) -> RenderPlanBuilderResult<RenderPlan> {
        println!(
            "DEBUG: build_simple_relationship_render_plan START - plan type: {:?}",
            std::mem::discriminant(self)
        );

        // Extract distinct flag from the outermost Projection BEFORE unwrapping
        // This must be done first because unwrapping will replace self with core_plan
        // However, if distinct_override is provided, use that instead
        let distinct = distinct_override.unwrap_or_else(|| self.extract_distinct());
        println!(
            "DEBUG: build_simple_relationship_render_plan - extracted distinct (early): {}",
            distinct
        );

        // Special case: Detect Projection(kind=Return) over GroupBy
        // This can be wrapped in OrderBy/Limit/Skip nodes
        // CTE is needed when RETURN items require data not available from WITH output

        // Unwrap OrderBy, Limit, Skip to find the core Projection
        let (core_plan, order_by, limit_val, skip_val) = match self {
            LogicalPlan::Limit(limit_node) => {
                println!("DEBUG: Unwrapping Limit node, count={}", limit_node.count);
                let limit_val = limit_node.count;
                match limit_node.input.as_ref() {
                    LogicalPlan::OrderBy(order_node) => {
                        println!("DEBUG: Found OrderBy inside Limit");
                        (
                            order_node.input.as_ref(),
                            Some(&order_node.items),
                            Some(limit_val),
                            None,
                        )
                    }
                    LogicalPlan::Skip(skip_node) => {
                        println!("DEBUG: Found Skip inside Limit");
                        (
                            skip_node.input.as_ref(),
                            None,
                            Some(limit_val),
                            Some(skip_node.count),
                        )
                    }
                    other => {
                        println!(
                            "DEBUG: Limit contains other type: {:?}",
                            std::mem::discriminant(other)
                        );
                        (other, None, Some(limit_val), None)
                    }
                }
            }
            LogicalPlan::OrderBy(order_node) => {
                println!("DEBUG: Unwrapping OrderBy node");
                (
                    order_node.input.as_ref(),
                    Some(&order_node.items),
                    None,
                    None,
                )
            }
            LogicalPlan::Skip(skip_node) => {
                println!("DEBUG: Unwrapping Skip node");
                (skip_node.input.as_ref(), None, None, Some(skip_node.count))
            }
            other => {
                println!(
                    "DEBUG: No unwrapping needed, plan type: {:?}",
                    std::mem::discriminant(other)
                );
                (other, None, None, None)
            }
        };

        println!(
            "DEBUG: After unwrapping - core_plan type: {:?}, has_order_by: {}, has_limit: {}, has_skip: {}",
            std::mem::discriminant(core_plan),
            order_by.is_some(),
            limit_val.is_some(),
            skip_val.is_some()
        );

        // Now check if core_plan is Projection(Return) over GroupBy
        if let LogicalPlan::Projection(outer_proj) = core_plan {
            println!(
                "DEBUG: core_plan is Projection, kind: {:?}",
                outer_proj.kind
            );
            if matches!(
                outer_proj.kind,
                crate::query_planner::logical_plan::ProjectionKind::Return
            ) {
                println!("DEBUG: Projection is Return type");
                if let LogicalPlan::GroupBy(group_by) = outer_proj.input.as_ref() {
                    println!("DEBUG: Found GroupBy under Projection(Return)!");
                    // Check if RETURN items need data beyond what WITH provides
                    // CTE is needed if RETURN contains:
                    // 1. Node references (TableAlias that refers to a node, not a WITH alias)
                    // 2. Wildcards (like `a.*`)
                    // 3. References to WITH projection aliases that aren't in the inner projection

                    // Collect all WITH projection aliases from the inner Projection
                    let with_aliases: std::collections::HashSet<String> =
                        if let LogicalPlan::Projection(inner_proj) = group_by.input.as_ref() {
                            inner_proj
                                .items
                                .iter()
                                .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                                .collect()
                        } else {
                            std::collections::HashSet::new()
                        };

                    // CTE is always needed when there are WITH aliases (aggregates)
                    // because the outer query needs to reference them from the CTE
                    let needs_cte = !with_aliases.is_empty()
                        || outer_proj.items.iter().any(|item| match &item.expression {
                            crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                prop,
                            ) if prop.column.raw() == "*" => true,
                            _ => false,
                        });

                    if needs_cte {
                        println!(
                            "DEBUG: Detected Projection(Return) over GroupBy where RETURN needs data beyond WITH output - using CTE pattern"
                        );

                        // Build the GROUP BY subquery as a CTE
                        // Step 1: Build inner query (GROUP BY + HAVING) as a RenderPlan
                        use crate::graph_catalog::graph_schema::GraphSchema;
                        use std::collections::HashMap;
                        let empty_schema = GraphSchema::build(
                            1,
                            "default".to_string(),
                            HashMap::new(),
                            HashMap::new(),
                        );
                        let inner_render_plan = group_by.input.to_render_plan(&empty_schema)?;

                        // Step 2: Extract GROUP BY expressions and HAVING clause
                        // Fix wildcard grouping: a.* -> a.user_id (use ID column from schema)
                        let group_by_exprs: Vec<RenderExpr> = group_by.expressions
                            .iter()
                            .cloned()
                            .map(|expr| {
                                // Check if this is a wildcard (PropertyAccess with column="*" or TableAlias)
                                let fixed_expr = match &expr {
                                    crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(prop) if prop.column.raw() == "*" => {
                                        // Replace a.* with a.{id_column}
                                        // Extract ID column from the schema
                                        let id_column = self.find_id_column_for_alias(&prop.table_alias.0)?;
                                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: prop.table_alias.clone(),
                                                column: PropertyValue::Column(id_column),
                                            }
                                        )
                                    }
                                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                                        // Replace table alias with table_alias.id_column
                                        let id_column = self.find_id_column_for_alias(&alias.0)?;
                                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: alias.clone(),
                                                column: PropertyValue::Column(id_column),
                                            }
                                        )
                                    }
                                    _ => expr.clone()
                                };
                                fixed_expr.try_into()
                            })
                            .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?;

                        let having_expr: Option<RenderExpr> =
                            if let Some(having) = &group_by.having_clause {
                                Some(having.clone().try_into()?)
                            } else {
                                None
                            };

                        // Step 2.5: Build SELECT list for CTE (only grouping keys + aggregates, not wildcards)
                        // Extract from the inner Projection (child of GroupBy)
                        let cte_select_items = if let LogicalPlan::Projection(inner_proj) =
                            group_by.input.as_ref()
                        {
                            inner_proj
                                .items
                                .iter()
                                .map(|item| {
                                    // For each projection item, check if it's an aggregate or grouping key
                                    let render_expr: RenderExpr =
                                        item.expression.clone().try_into()?;

                                    // Normalize aggregate arguments: COUNT(b) -> COUNT(b.user_id)
                                    let normalized_expr =
                                        self.normalize_aggregate_args(render_expr)?;

                                    // Replace wildcard expressions with the specific ID column
                                    let (fixed_expr, auto_alias) = match &normalized_expr {
                                        RenderExpr::PropertyAccessExp(prop)
                                            if prop.column.0.raw() == "*" =>
                                        {
                                            // Find the ID column for this alias
                                            let id_col =
                                                self.find_id_column_for_alias(&prop.table_alias.0)?;
                                            let expr = RenderExpr::PropertyAccessExp(
                                                super::render_expr::PropertyAccess {
                                                    table_alias: prop.table_alias.clone(),
                                                    column: super::render_expr::Column(
                                                        PropertyValue::Column(id_col.clone()),
                                                    ),
                                                },
                                            );
                                            // Add alias so it can be referenced as grouped_data.user_id
                                            (expr, Some(super::render_expr::ColumnAlias(id_col)))
                                        }
                                        _ => (normalized_expr, None),
                                    };

                                    // Use existing alias if present, otherwise use auto-generated alias for grouping keys
                                    let col_alias = item
                                        .col_alias
                                        .as_ref()
                                        .map(|a| super::render_expr::ColumnAlias(a.0.clone()))
                                        .or(auto_alias);

                                    Ok(super::SelectItem {
                                        expression: fixed_expr,
                                        col_alias,
                                    })
                                })
                                .collect::<Result<Vec<super::SelectItem>, RenderBuildError>>()?
                        } else {
                            // Fallback to original select items
                            inner_render_plan.select.items.clone()
                        };

                        // Step 3: Create CTE with GROUP BY + HAVING
                        let cte_name = "grouped_data".to_string();
                        let cte = Cte {
                            cte_name: cte_name.clone(),
                            content: super::CteContent::Structured(RenderPlan {
                                ctes: CteItems(vec![]),
                                select: SelectItems {
                                    items: cte_select_items,
                                    distinct: false,
                                },
                                from: inner_render_plan.from.clone(),
                                joins: inner_render_plan.joins.clone(),
                                filters: inner_render_plan.filters.clone(),
                                group_by: GroupByExpressions(group_by_exprs.clone()), // Clone to preserve for later use
                                having_clause: having_expr,
                                order_by: OrderByItems(vec![]),
                                skip: SkipItem(None),
                                limit: LimitItem(None),
                                union: UnionItems(None),
                            }),
                            is_recursive: false,
                        };

                        // Step 4: Build outer query that joins to CTE
                        // Extract the grouping key to use for join (use the FIXED expression with ID column)
                        let grouping_key_render = if let Some(first_expr) = group_by_exprs.first() {
                            first_expr.clone()
                        } else {
                            return Err(RenderBuildError::InvalidRenderPlan(
                                "GroupBy has no grouping expressions after fixing wildcards"
                                    .to_string(),
                            ));
                        };

                        // Extract table alias and column name from the fixed grouping key
                        let (table_alias, key_column) = match &grouping_key_render {
                            RenderExpr::PropertyAccessExp(prop_access) => (
                                prop_access.table_alias.0.clone(),
                                prop_access.column.0.clone(),
                            ),
                            _ => {
                                return Err(RenderBuildError::InvalidRenderPlan(
                                    "Grouping expression is not a property access after fixing"
                                        .to_string(),
                                ));
                            }
                        };

                        // Build outer SELECT items from outer_proj
                        // Need to rewrite references to WITH aliases to pull from the CTE
                        let outer_select_items = outer_proj
                            .items
                            .iter()
                            .map(|item| {
                                let expr: RenderExpr = item.expression.clone().try_into()?;

                                // Rewrite TableAlias references that are WITH aliases to reference the CTE
                                let rewritten_expr = match &expr {
                                    RenderExpr::TableAlias(alias) => {
                                        // Check if this is a WITH alias (from the CTE)
                                        if with_aliases.contains(&alias.0) {
                                            // Reference it from the CTE: grouped_data.follows
                                            RenderExpr::PropertyAccessExp(
                                                super::render_expr::PropertyAccess {
                                                    table_alias: super::render_expr::TableAlias(
                                                        cte_name.clone(),
                                                    ),
                                                    column: super::render_expr::Column(
                                                        PropertyValue::Column(alias.0.clone()),
                                                    ),
                                                },
                                            )
                                        } else {
                                            expr
                                        }
                                    }
                                    _ => expr,
                                };

                                Ok(super::SelectItem {
                                    expression: rewritten_expr,
                                    col_alias: item.col_alias.as_ref().map(|alias| {
                                        super::render_expr::ColumnAlias(alias.0.clone())
                                    }),
                                })
                            })
                            .collect::<Result<Vec<super::SelectItem>, RenderBuildError>>()?;

                        // Extract FROM table for the outer query (from the original table)
                        // NOTE: ClickHouse CTE scoping - we need to be careful about table references
                        let outer_from = inner_render_plan.from.clone();

                        // Create JOIN condition: a.user_id = grouped_data.user_id
                        let cte_key_expr =
                            RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                                table_alias: super::render_expr::TableAlias(cte_name.clone()),
                                column: super::render_expr::Column(key_column.clone()),
                            });

                        let join_condition = super::render_expr::OperatorApplication {
                            operator: super::render_expr::Operator::Equal,
                            operands: vec![grouping_key_render, cte_key_expr],
                        };

                        // Create a join to the CTE
                        let cte_join = super::Join {
                            table_name: cte_name.clone(),
                            table_alias: cte_name.clone(),
                            joining_on: vec![join_condition],
                            join_type: super::JoinType::Inner,
                        };

                        println!(
                            "DEBUG: Created GroupBy CTE pattern with table_alias={}, key_column={}",
                            table_alias, key_column.raw().clone()
                        );

                        // Build ORDER BY items, rewriting WITH alias references to CTE references
                        let order_by_items = if let Some(order_items) = order_by {
                            order_items.iter()
                                .map(|item| {
                                    let expr: RenderExpr = item.expression.clone().try_into()?;

                                    // Rewrite TableAlias references to WITH aliases
                                    let rewritten_expr = match &expr {
                                        RenderExpr::TableAlias(alias) => {
                                            if with_aliases.contains(&alias.0) {
                                                RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                                                    table_alias: super::render_expr::TableAlias(cte_name.clone()),
                                                    column: super::render_expr::Column(PropertyValue::Column(alias.0.clone())),
                                                })
                                            } else {
                                                expr
                                            }
                                        }
                                        _ => expr
                                    };

                                    Ok(super::OrderByItem {
                                        expression: rewritten_expr,
                                        order: match item.order {
                                            crate::query_planner::logical_plan::OrderByOrder::Asc => super::OrderByOrder::Asc,
                                            crate::query_planner::logical_plan::OrderByOrder::Desc => super::OrderByOrder::Desc,
                                        },
                                    })
                                })
                                .collect::<Result<Vec<_>, RenderBuildError>>()?
                        } else {
                            vec![]
                        };

                        // Return the CTE-based plan with proper JOIN, ORDER BY, and LIMIT
                        return Ok(RenderPlan {
                            ctes: CteItems(vec![cte]),
                            select: SelectItems {
                                items: outer_select_items,
                                distinct: false,
                            },
                            from: outer_from,
                            joins: JoinItems(vec![cte_join]),
                            filters: FilterItems(None),
                            group_by: GroupByExpressions(vec![]),
                            having_clause: None,
                            order_by: OrderByItems(order_by_items),
                            skip: SkipItem(skip_val),
                            limit: LimitItem(limit_val),
                            union: UnionItems(None),
                        });
                    }
                } else {
                    println!(
                        "DEBUG: Projection(Return) input is NOT GroupBy, discriminant: {:?}",
                        std::mem::discriminant(outer_proj.input.as_ref())
                    );
                }
            } else {
                println!("DEBUG: Projection is not Return type");
            }
        } else {
            println!(
                "DEBUG: core_plan is NOT Projection, discriminant: {:?}",
                std::mem::discriminant(core_plan)
            );
        }

        let final_select_items = self.extract_select_items()?;
        println!(
            "DEBUG: build_simple_relationship_render_plan - final_select_items: {:?}",
            final_select_items
        );

        // Validate that we have proper select items
        if final_select_items.is_empty() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No select items found for relationship query. This usually indicates missing schema information or incomplete query planning.".to_string()
            ));
        }

        // Validate that select items are not just literals (which would indicate failed expression conversion)
        for item in &final_select_items {
            if let RenderExpr::Literal(_) = &item.expression {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Select item is a literal value, indicating failed expression conversion. Check schema mappings and query structure.".to_string()
                ));
            }
        }

        let final_from = self.extract_from()?;
        println!(
            "DEBUG: build_simple_relationship_render_plan - final_from: {:?}",
            final_from
        );

        // Validate that we have a FROM clause
        if final_from.is_none() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No FROM table found for relationship query. Schema inference may have failed."
                    .to_string(),
            ));
        }

        let final_filters = self.extract_filters()?;
        println!(
            "DEBUG: build_simple_relationship_render_plan - final_filters: {:?}",
            final_filters
        );

        // Validate that filters don't contain obviously invalid expressions
        if let Some(ref filter_expr) = final_filters {
            if is_invalid_filter_expression(filter_expr) {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Filter expression appears invalid (e.g., '1 = 0'). This usually indicates schema mapping issues.".to_string()
                ));
            }
        }

        let extracted_joins = self.extract_joins()?;
        println!(
            "DEBUG: build_simple_relationship_render_plan - extracted_joins: {:?}",
            extracted_joins
        );

        // Filter out JOINs that duplicate the FROM table
        // If we're starting FROM node 'a', we shouldn't also have it in the JOINs list
        let from_alias = final_from
            .as_ref()
            .and_then(|ft| ft.table.as_ref())
            .and_then(|vt| vt.alias.clone());
        let filtered_joins: Vec<Join> = if let Some(ref anchor_alias) = from_alias {
            extracted_joins.into_iter()
                .filter(|join| {
                    if &join.table_alias == anchor_alias {
                        println!("DEBUG: Filtering out JOIN for '{}' because it's already in FROM clause", anchor_alias);
                        false
                    } else {
                        true
                    }
                })
                .collect()
        } else {
            extracted_joins
        };
        println!(
            "DEBUG: build_simple_relationship_render_plan - filtered_joins: {:?}",
            filtered_joins
        );

        // distinct was already extracted at the beginning of this function
        println!(
            "DEBUG: build_simple_relationship_render_plan - using pre-extracted distinct: {}",
            distinct
        );

        Ok(RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: final_select_items,
                distinct,
            },
            from: FromTableItem(from_table_to_view_ref(final_from)),
            joins: JoinItems(filtered_joins), // GraphJoinInference already ordered these correctly
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(self.extract_group_by()?),
            having_clause: self.extract_having()?,
            order_by: OrderByItems(self.extract_order_by()?),
            skip: SkipItem(self.extract_skip()),
            limit: LimitItem(self.extract_limit()),
            union: UnionItems(None),
        })
    }

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan> {
        println!(
            "DEBUG: to_render_plan called for plan type: {:?}",
            std::mem::discriminant(self)
        );

        // CRITICAL: Apply alias transformation BEFORE rendering
        // This rewrites denormalized node aliases to use relationship table aliases
        let transformed_plan = {
            use crate::render_plan::alias_resolver::AliasResolverContext;
            let alias_context = AliasResolverContext::from_logical_plan(self);
            alias_context.transform_plan(self.clone())
        };

        // Special case for PageRank - it generates complete SQL directly
        if let LogicalPlan::PageRank(_pagerank) = &transformed_plan {
            // For PageRank, we create a minimal RenderPlan that will be handled specially
            // The actual SQL generation happens in the server handler
            return Ok(RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems {
                    items: vec![],
                    distinct: false,
                },
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
            });
        }

        // NEW ARCHITECTURE: Prioritize JOINs over CTEs
        // Only use CTEs for variable-length paths and complex cases
        // Try to build a simple JOIN-based plan first
        println!("DEBUG: Trying try_build_join_based_plan");
        match transformed_plan.try_build_join_based_plan() {
            Ok(plan) => {
                println!("DEBUG: try_build_join_based_plan succeeded");
                return Ok(plan);
            }
            Err(_) => {
                println!("DEBUG: try_build_join_based_plan failed, falling back to CTE logic");
            }
        }

        // Variable-length paths are now supported via recursive CTE generation
        // Two-pass architecture:
        // 1. Analyze property requirements across the entire plan
        // 2. Generate CTEs with full context including required properties

        log::trace!(
            "Starting render plan generation for plan type: {}",
            match &transformed_plan {
                LogicalPlan::Empty => "Empty",
                LogicalPlan::Scan(_) => "Scan",
                LogicalPlan::ViewScan(_) => "ViewScan",
                LogicalPlan::GraphNode(_) => "GraphNode",
                LogicalPlan::GraphRel(_) => "GraphRel",
                LogicalPlan::Filter(_) => "Filter",
                LogicalPlan::Projection(_) => "Projection",
                LogicalPlan::GraphJoins(_) => "GraphJoins",
                LogicalPlan::GroupBy(_) => "GroupBy",
                LogicalPlan::OrderBy(_) => "OrderBy",
                LogicalPlan::Skip(_) => "Skip",
                LogicalPlan::Limit(_) => "Limit",
                LogicalPlan::Cte(_) => "Cte",
                LogicalPlan::Union(_) => "Union",
                LogicalPlan::PageRank(_) => "PageRank",
            }
        );

        // First pass: analyze what properties are needed
        let mut context = analyze_property_requirements(&transformed_plan, schema);

        let extracted_ctes: Vec<Cte>;
        let mut final_from: Option<FromTable>;
        let final_filters: Option<RenderExpr>;

        let last_node_cte_opt = transformed_plan.extract_last_node_cte()?;

        if let Some(last_node_cte) = last_node_cte_opt {
            // Extract the last part after splitting by '_'
            // This handles both "prefix_alias" and "rel_left_right" formats
            let parts: Vec<&str> = last_node_cte.cte_name.split('_').collect();
            let last_node_alias = parts.last().ok_or(RenderBuildError::MalformedCTEName)?;

            // Second pass: generate CTEs with full context
            extracted_ctes = transformed_plan.extract_ctes_with_context(last_node_alias, &mut context)?;

            // Check if we have a variable-length CTE (it will be a recursive RawSql CTE)
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| {
                let is_recursive = cte.is_recursive;
                let is_raw_sql = matches!(&cte.content, super::CteContent::RawSql(_));
                is_recursive && is_raw_sql
            });

            if has_variable_length_cte {
                // For variable-length paths, use the CTE itself as the FROM clause
                let var_len_cte = extracted_ctes
                    .iter()
                    .find(|cte| cte.is_recursive)
                    .expect("Variable-length CTE should exist");

                // Create a ViewTableRef that references the CTE by name
                // We'll use an empty LogicalPlan as the source since the CTE is already defined
                final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                    source: std::sync::Arc::new(
                        crate::query_planner::logical_plan::LogicalPlan::Empty,
                    ),
                    name: var_len_cte.cte_name.clone(),
                    alias: Some("t".to_string()), // CTE uses 't' as alias
                    use_final: false,             // CTEs don't use FINAL
                })));

                // Check if there are end filters stored in the context that need to be applied to the outer query
                final_filters = context.get_end_filters_for_outer_query().cloned();
            } else {
                // Extract from the CTE content (normal path)
                let (cte_from, cte_filters) = match &last_node_cte.content {
                    super::CteContent::Structured(plan) => {
                        (plan.from.0.clone(), plan.filters.0.clone())
                    }
                    super::CteContent::RawSql(_) => (None, None), // Raw SQL CTEs don't have structured access
                };

                final_from = view_ref_to_from_table(cte_from);

                let last_node_filters_opt = clean_last_node_filters(cte_filters);

                let final_filters_opt = transformed_plan.extract_final_filters()?;

                let final_combined_filters = if let (Some(final_filters), Some(last_node_filters)) =
                    (&final_filters_opt, &last_node_filters_opt)
                {
                    Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![final_filters.clone(), last_node_filters.clone()],
                    }))
                } else if final_filters_opt.is_some() {
                    final_filters_opt
                } else if last_node_filters_opt.is_some() {
                    last_node_filters_opt
                } else {
                    None
                };

                final_filters = final_combined_filters;
            }
        } else {
            // No CTE wrapper, but check for variable-length paths which generate CTEs directly
            // Extract CTEs with a dummy alias and context (variable-length doesn't use the alias)
            extracted_ctes = transformed_plan.extract_ctes_with_context("_", &mut context)?;

            // Check if we have a variable-length CTE (recursive or chained join)
            // Both types use RawSql content and need special FROM clause handling
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| {
                matches!(&cte.content, super::CteContent::RawSql(_))
                    && (cte.cte_name.starts_with("variable_path_")
                        || cte.cte_name.starts_with("chained_path_"))
            });

            if has_variable_length_cte {
                // For variable-length paths, use the CTE itself as the FROM clause
                let var_len_cte = extracted_ctes
                    .iter()
                    .find(|cte| {
                        cte.cte_name.starts_with("variable_path_")
                            || cte.cte_name.starts_with("chained_path_")
                    })
                    .expect("Variable-length CTE should exist");

                // Create a ViewTableRef that references the CTE by name
                final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                    source: std::sync::Arc::new(
                        crate::query_planner::logical_plan::LogicalPlan::Empty,
                    ),
                    name: var_len_cte.cte_name.clone(),
                    alias: Some("t".to_string()), // CTE uses 't' as alias
                    use_final: false,             // CTEs don't use FINAL
                })));
                // For variable-length paths, apply end filters in the outer query
                if let Some((_start_alias, _end_alias)) = has_variable_length_rel(self) {
                    final_filters = context.get_end_filters_for_outer_query().cloned();
                } else {
                    final_filters = None;
                }
            } else {
                // Normal case: no CTEs, extract FROM, joins, and filters normally
                final_from = transformed_plan.extract_from()?;
                final_filters = transformed_plan.extract_filters()?;
            }
        }

        let final_select_items = transformed_plan.extract_select_items()?;

        // NOTE: Removed rewrite for select_items in variable-length paths to keep a.*, b.*

        let mut extracted_joins = transformed_plan.extract_joins()?;

        // For variable-length paths, add joins to get full user data
        if let Some((start_alias, end_alias)) = has_variable_length_rel(&transformed_plan) {
            // IMPORTANT: Remove any joins that were extracted from the GraphRel itself
            // The CTE already handles the path traversal, so we only want to join the
            // endpoint nodes to the CTE result. Keeping the GraphRel joins causes
            // "Multiple table expressions with same alias" errors.
            extracted_joins.clear();

            // Get the actual table names and ID columns from the schema
            let start_table = get_node_table_for_alias(&start_alias);
            let end_table = get_node_table_for_alias(&end_alias);
            let start_id_col = get_node_id_column_for_alias(&start_alias);
            let end_id_col = get_node_id_column_for_alias(&end_alias);

            // Check for self-loop: start and end are the same node (e.g., (a)-[*0..]->(a))
            if start_alias == end_alias {
                // Self-loop: Only add ONE JOIN with compound ON condition
                // JOIN users AS a ON t.start_id = a.user_id AND t.end_id = a.user_id
                extracted_joins.push(Join {
                    table_name: start_table,
                    table_alias: start_alias.clone(),
                    joining_on: vec![
                        OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias("t".to_string()),
                                    column: Column(PropertyValue::Column("start_id".to_string())),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(start_alias.clone()),
                                    column: Column(PropertyValue::Column(start_id_col.clone())),
                                }),
                            ],
                        },
                        OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias("t".to_string()),
                                    column: Column(PropertyValue::Column("end_id".to_string())),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(start_alias.clone()),
                                    column: Column(PropertyValue::Column(start_id_col.clone())),
                                }),
                            ],
                        },
                    ],
                    join_type: JoinType::Join,
                });
            } else {
                // Different start and end nodes: Add two separate JOINs
                extracted_joins.push(Join {
                    table_name: start_table,
                    table_alias: start_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias("t".to_string()),
                                column: Column(PropertyValue::Column("start_id".to_string())),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(start_alias.clone()),
                                column: Column(PropertyValue::Column(start_id_col.clone())),
                            }),
                        ],
                    }],
                    join_type: JoinType::Join,
                });
                extracted_joins.push(Join {
                    table_name: end_table,
                    table_alias: end_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias("t".to_string()),
                                column: Column(PropertyValue::Column("end_id".to_string())),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(end_alias.clone()),
                                column: Column(PropertyValue::Column(end_id_col.clone())),
                            }),
                        ],
                    }],
                    join_type: JoinType::Join,
                });
            }
        }

        // For multiple relationship types (UNION CTE), add joins to connect nodes
        // Similar to variable-length paths, we need to clear and rebuild joins
        if let Some(union_cte) = extracted_ctes
            .iter()
            .find(|cte| cte.cte_name.starts_with("rel_") && !cte.is_recursive)
        {
            // Check if this is actually a multi-relationship query (has UNION in plan)
            if has_multiple_relationship_types(&transformed_plan) {
                eprintln!(
                    "DEBUG: Multi-relationship query detected! Clearing extracted joins and rebuilding..."
                );
                eprintln!("DEBUG: Before clear: {} joins", extracted_joins.len());

                // Clear extracted joins like we do for variable-length paths
                // The GraphRel joins include duplicate source node joins which cause
                // "Multiple table expressions with same alias" errors
                extracted_joins.clear();

                // Extract the node aliases from the CTE name (e.g., "rel_u_target" → "u", "target")
                let cte_name = union_cte.cte_name.clone();
                let parts: Vec<&str> = cte_name
                    .strip_prefix("rel_")
                    .unwrap_or(&cte_name)
                    .split('_')
                    .collect();

                if parts.len() >= 2 {
                    let source_alias = parts[0].to_string();
                    let target_alias = parts[parts.len() - 1].to_string();

                    // Get table names and ID columns from schema
                    let source_table = get_node_table_for_alias(&source_alias);
                    let target_table = get_node_table_for_alias(&target_alias);
                    let source_id_col = get_node_id_column_for_alias(&source_alias);
                    let target_id_col = get_node_id_column_for_alias(&target_alias);

                    // Generate a random alias for the CTE JOIN
                    let cte_alias = crate::query_planner::logical_plan::generate_id();

                    // Add JOIN from CTE to source node (using CTE's from_node_id)
                    extracted_joins.push(Join {
                        table_name: cte_name.clone(),
                        table_alias: cte_alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_alias.clone()),
                                    column: Column(PropertyValue::Column("from_node_id".to_string())),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(source_alias.clone()),
                                    column: Column(PropertyValue::Column(source_id_col.clone())),
                                }),
                            ],
                        }],
                        join_type: JoinType::Join,
                    });

                    // Add JOIN from CTE to target node (using CTE's to_node_id)
                    extracted_joins.push(Join {
                        table_name: target_table,
                        table_alias: target_alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_alias.clone()),
                                    column: Column(PropertyValue::Column("to_node_id".to_string())),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(target_alias.clone()),
                                    column: Column(PropertyValue::Column(target_id_col.clone())),
                                }),
                            ],
                        }],
                        join_type: JoinType::Join,
                    });
                }
            } else {
                // Old PATCH code for non-UNION multi-rel (keep for backward compat)
                let cte_name = union_cte.cte_name.clone();
                eprintln!("DEBUG: Found union CTE '{}', updating JOINs", cte_name);
                for join in extracted_joins.iter_mut() {
                    eprintln!(
                        "DEBUG: Checking JOIN table_name='{}' alias='{}'",
                        join.table_name, join.table_alias
                    );
                    // Update joins that are relationship tables
                    // Check both with and without schema prefix (e.g., "follows" or "test_integration.follows")
                    let table_lower = join.table_name.to_lowercase();
                    if table_lower.contains("follow")
                        || table_lower.contains("friend")
                        || table_lower.contains("like")
                        || table_lower.contains("purchase")
                        || join.table_name.starts_with("rel_")
                    {
                        eprintln!(
                            "DEBUG: Updating JOIN to use CTE '{}' (was '{}')",
                            cte_name, join.table_name
                        );
                        join.table_name = cte_name.clone();
                        // Also update joining_on expressions to use standardized column names
                        for op_app in join.joining_on.iter_mut() {
                            update_join_expression_for_union_cte(op_app, &join.table_alias);
                        }
                    }
                    // Also update any join that references the union CTE in its expressions
                    else if references_union_cte_in_join(&join.joining_on, &cte_name) {
                        for op_app in join.joining_on.iter_mut() {
                            update_join_expression_for_union_cte(op_app, &cte_name);
                        }
                    }
                }
            }
        }
        // For variable-length (recursive) CTEs, keep previous logic
        if let Some(last_node_cte) = transformed_plan.extract_last_node_cte().ok().flatten() {
            if let super::CteContent::RawSql(_) = &last_node_cte.content {
                let cte_name = last_node_cte.cte_name.clone();
                if cte_name.starts_with("rel_") {
                    for join in extracted_joins.iter_mut() {
                        join.table_name = cte_name.clone();
                    }
                }
            }
        }
        extracted_joins.sort_by_key(|join| join.joining_on.len());

        let mut extracted_group_by_exprs = transformed_plan.extract_group_by()?;

        // Rewrite GROUP BY expressions for variable-length paths
        if let Some((left_alias, right_alias)) = has_variable_length_rel(&transformed_plan) {
            let path_var = get_path_variable(&transformed_plan);
            extracted_group_by_exprs = extracted_group_by_exprs
                .into_iter()
                .map(|expr| {
                    rewrite_expr_for_var_len_cte(
                        &expr,
                        &left_alias,
                        &right_alias,
                        path_var.as_deref(),
                    )
                })
                .collect();
        }

        let mut extracted_order_by = transformed_plan.extract_order_by()?;

        // Rewrite ORDER BY expressions for variable-length paths that use recursive CTEs
        // Fixed-length patterns use inline JOINs, so rewriting is not needed (a.name, c.name work fine)
        // Variable-length patterns use recursive CTEs (t.start_id, t.end_id), so rewrite to t.start_name
        if let Some((left_alias, right_alias)) = has_variable_length_rel(&transformed_plan) {
            // Check if this is truly variable-length (needs recursive CTE)
            // Fixed-length (*2, *3) use inline JOINs and don't need rewriting
            let needs_cte = if let Some(spec) = get_variable_length_spec(&transformed_plan) {
                spec.exact_hop_count().is_none() || get_shortest_path_mode(self).is_some()
            } else {
                false
            };

            // Only rewrite ORDER BY for patterns that use recursive CTEs
            if needs_cte {
                let path_var = get_path_variable(&transformed_plan);
                extracted_order_by = extracted_order_by
                    .into_iter()
                    .map(|item| OrderByItem {
                        expression: rewrite_expr_for_var_len_cte(
                            &item.expression,
                            &left_alias,
                            &right_alias,
                            path_var.as_deref(),
                        ),
                        order: item.order,
                    })
                    .collect();
            }
        }

        let extracted_limit_item = transformed_plan.extract_limit();

        let extracted_skip_item = transformed_plan.extract_skip();

        let extracted_union = transformed_plan.extract_union()?;

        // Validate render plan before construction (for CTE path)
        if final_select_items.is_empty() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No select items found. This usually indicates missing schema information or incomplete query planning.".to_string()
            ));
        }

        // Check if this is a standalone RETURN query (no MATCH, only literals/parameters/functions)
        let is_standalone_return = final_from.is_none()
            && final_select_items
                .iter()
                .all(|item| is_standalone_expression(&item.expression));

        if is_standalone_return {
            // For standalone RETURN queries (e.g., "RETURN 1 + 1", "RETURN toUpper($name)"),
            // use ClickHouse's system.one table as a dummy FROM clause
            log::debug!("Detected standalone RETURN query, using system.one as FROM clause");

            // Create a ViewTableRef that references system.one
            // Use an Empty LogicalPlan since we don't need actual view resolution for system tables
            final_from = Some(FromTable::new(Some(ViewTableRef {
                source: std::sync::Arc::new(crate::query_planner::logical_plan::LogicalPlan::Empty),
                name: "system.one".to_string(),
                alias: None,
                use_final: false,
            })));
        }

        // Validate FROM clause exists (after potentially adding system.one for standalone queries)
        if final_from.is_none() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No FROM clause found. This usually indicates missing table information or incomplete query planning.".to_string()
            ));
        }

        // Validate filters don't contain invalid expressions like "1 = 0"
        if let Some(filter_expr) = &final_filters {
            if is_invalid_filter_expression(filter_expr) {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Filter contains invalid expression (e.g., '1 = 0'). This indicates failed schema mapping or expression conversion.".to_string()
                ));
            }
        }

        Ok(RenderPlan {
            ctes: CteItems(extracted_ctes),
            select: SelectItems {
                items: final_select_items,
                distinct: self.extract_distinct(),
            },
            from: FromTableItem(from_table_to_view_ref(final_from)),
            joins: JoinItems(extracted_joins),
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(extracted_group_by_exprs),
            having_clause: self.extract_having()?,
            order_by: OrderByItems(extracted_order_by),
            skip: SkipItem(extracted_skip_item),
            limit: LimitItem(extracted_limit_item),
            union: UnionItems(extracted_union),
        })
    }
}

/// Post-process a RenderExpr to apply property mapping based on node labels
/// This function recursively walks the expression tree and maps property names to column names
fn plan_to_string(plan: &LogicalPlan, depth: usize) -> String {
    let indent = "  ".repeat(depth);
    match plan {
        LogicalPlan::GraphNode(node) => format!(
            "{}GraphNode(alias='{}', input={})",
            indent,
            node.alias,
            plan_to_string(&node.input, depth + 1)
        ),
        LogicalPlan::GraphRel(rel) => format!(
            "{}GraphRel(alias='{}', left={}, center={}, right={})",
            indent,
            rel.alias,
            plan_to_string(&rel.left, depth + 1),
            plan_to_string(&rel.center, depth + 1),
            plan_to_string(&rel.right, depth + 1)
        ),
        LogicalPlan::Filter(filter) => format!(
            "{}Filter(input={})",
            indent,
            plan_to_string(&filter.input, depth + 1)
        ),
        LogicalPlan::Projection(proj) => format!(
            "{}Projection(input={})",
            indent,
            plan_to_string(&proj.input, depth + 1)
        ),
        LogicalPlan::ViewScan(scan) => format!("{}ViewScan(table='{}')", indent, scan.source_table),
        LogicalPlan::Scan(scan) => format!("{}Scan(table={:?})", indent, scan.table_name),
        _ => format!("{}Other({})", indent, plan_type_name(plan)),
    }
}

/// Extract outer aggregation info from a plan that wraps a Union
/// Returns (select_items, group_by_exprs) if the plan has Projection → GroupBy → Union pattern
fn extract_outer_aggregation_info(plan: &LogicalPlan) -> Option<(Vec<SelectItem>, Vec<RenderExpr>)> {
    // Look for patterns like:
    // - GraphJoins → Projection(Return) → GroupBy → Union
    // - Projection(Return) → GroupBy → Union
    
    println!("DEBUG: extract_outer_aggregation_info called with plan type: {:?}", plan_type_name(plan));
    
    let (projection, group_by) = match plan {
        LogicalPlan::GraphJoins(graph_joins) => {
            println!("DEBUG: GraphJoins input type: {:?}", plan_type_name(&graph_joins.input));
            if let LogicalPlan::Projection(proj) = graph_joins.input.as_ref() {
                println!("DEBUG: Projection input type: {:?}", plan_type_name(&proj.input));
                if let LogicalPlan::GroupBy(gb) = proj.input.as_ref() {
                    println!("DEBUG: GroupBy input type: {:?}, has union: {}", 
                        plan_type_name(&gb.input), find_nested_union(&gb.input).is_some());
                    if find_nested_union(&gb.input).is_some() {
                        (Some(proj), Some(gb))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        }
        LogicalPlan::Projection(proj) => {
            if let LogicalPlan::GroupBy(gb) = proj.input.as_ref() {
                if find_nested_union(&gb.input).is_some() {
                    (Some(proj), Some(gb))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        }
        _ => (None, None),
    };
    
    let (projection, group_by) = match (projection, group_by) {
        (Some(p), Some(g)) => (p, g),
        _ => return None,
    };
    
    // Check if projection has aggregation
    let has_aggregation = projection.items.iter().any(|item| {
        matches!(&item.expression, crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_))
    });
    
    if !has_aggregation {
        return None;
    }
    
    println!("DEBUG: extract_outer_aggregation_info found aggregation pattern");
    
    // Convert outer SELECT items
    let outer_select: Vec<SelectItem> = projection.items.iter().map(|item| {
        let expr: RenderExpr = match &item.expression {
            crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(alias) => {
                // Reference the column alias from the subquery
                RenderExpr::Raw(format!("\"{}\"", alias.0))
            }
            crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(agg) => {
                // Keep aggregation function, but rewrite args to reference aliases
                let args: Vec<RenderExpr> = agg.args.iter().map(|arg| {
                    match arg {
                        crate::query_planner::logical_expr::LogicalExpr::Star => RenderExpr::Raw("*".to_string()),
                        crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(alias) => {
                            RenderExpr::Raw(format!("\"{}\"", alias.0))
                        }
                        other => other.clone().try_into().unwrap_or(RenderExpr::Raw("?".to_string()))
                    }
                }).collect();
                
                RenderExpr::AggregateFnCall(AggregateFnCall {
                    name: agg.name.clone(),
                    args,
                })
            }
            other => other.clone().try_into().unwrap_or(RenderExpr::Raw("?".to_string()))
        };
        
        SelectItem {
            expression: expr,
            col_alias: item.col_alias.as_ref().map(|a| ColumnAlias(a.0.clone())),
        }
    }).collect();
    
    // Convert GROUP BY expressions
    let outer_group_by: Vec<RenderExpr> = group_by.expressions.iter().map(|expr| {
        match expr {
            crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(alias) => {
                RenderExpr::Raw(format!("\"{}\"", alias.0))
            }
            other => other.clone().try_into().unwrap_or(RenderExpr::Raw("?".to_string()))
        }
    }).collect();
    
    Some((outer_select, outer_group_by))
}

/// Find a Union node nested inside a plan (within Projection, GraphJoins, GroupBy, etc.)
fn find_nested_union(plan: &LogicalPlan) -> Option<&crate::query_planner::logical_plan::Union> {
    match plan {
        LogicalPlan::Union(union) => Some(union),
        LogicalPlan::GraphJoins(graph_joins) => find_nested_union(&graph_joins.input),
        LogicalPlan::Projection(projection) => find_nested_union(&projection.input),
        LogicalPlan::Filter(filter) => find_nested_union(&filter.input),
        LogicalPlan::GroupBy(group_by) => find_nested_union(&group_by.input),
        _ => None,
    }
}

/// Find the 1-based position of a SELECT item that matches an ORDER BY expression.
/// For denormalized UNION queries, ORDER BY a.code should match SELECT ... AS "a.code".
/// We match by comparing the property access pattern.
fn find_select_position_for_order_by(
    order_expr: &crate::query_planner::logical_expr::LogicalExpr,
    select_items: &[SelectItem],
) -> Option<usize> {
    use crate::query_planner::logical_expr::LogicalExpr;
    
    // Extract table alias and property from ORDER BY
    let (order_table, order_prop) = match order_expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            (prop.table_alias.0.as_str(), prop.column.raw())
        }
        _ => return None,
    };
    
    // Find matching SELECT item - the one whose expression has same table alias
    // and whose col_alias matches the pattern "table.property"
    for (i, item) in select_items.iter().enumerate() {
        // Check if this SELECT item's expression has the same table alias
        if let RenderExpr::PropertyAccessExp(ref prop_access) = item.expression {
            if prop_access.table_alias.0 == order_table {
                // This SELECT item is for the same table - use its position
                // The ORDER BY property should map to this SELECT item
                return Some(i + 1); // 1-based position
            }
        }
    }
    
    // Fallback: find by col_alias table prefix
    for (i, item) in select_items.iter().enumerate() {
        if let Some(ref alias) = item.col_alias {
            if alias.0.starts_with(&format!("{}.", order_table)) {
                return Some(i + 1);
            }
        }
    }
    
    None
}

/// Convert an ORDER BY expression for use in a UNION query.
/// For UNION, ORDER BY must reference the SELECT alias (e.g., "a.code"), 
/// not the original table column (e.g., a.origin_code).
fn convert_order_by_expr_for_union(expr: &crate::query_planner::logical_expr::LogicalExpr) -> RenderExpr {
    use crate::query_planner::logical_expr::LogicalExpr;
    
    match expr {
        LogicalExpr::PropertyAccessExp(prop_access) => {
            // Convert property access to quoted alias: a.code -> "a.code"
            // Use raw() to get the property name from PropertyValue
            let alias = format!("\"{}.{}\"", prop_access.table_alias.0, prop_access.column.raw());
            RenderExpr::Raw(alias)
        }
        LogicalExpr::ColumnAlias(col_alias) => {
            // Already an alias, just quote it
            RenderExpr::Raw(format!("\"{}\"", col_alias.0))
        }
        // For other expressions (literals, function calls, etc.), convert normally
        _ => expr.clone().try_into().unwrap_or_else(|_| RenderExpr::Raw("1".to_string()))
    }
}

fn plan_type_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Empty => "Empty",
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::ViewScan(_) => "ViewScan",
        LogicalPlan::GraphNode(_) => "GraphNode",
        LogicalPlan::GraphRel(_) => "GraphRel",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::GraphJoins(_) => "GraphJoins",
        LogicalPlan::GroupBy(_) => "GroupBy",
        LogicalPlan::OrderBy(_) => "OrderBy",
        LogicalPlan::Skip(_) => "Skip",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Cte(_) => "Cte",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::PageRank(_) => "PageRank",
    }
}

fn plan_contains_view_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ViewScan(_) => true,
        LogicalPlan::GraphNode(node) => plan_contains_view_scan(&node.input),
        LogicalPlan::GraphRel(rel) => {
            plan_contains_view_scan(&rel.left)
                || plan_contains_view_scan(&rel.right)
                || plan_contains_view_scan(&rel.center)
        }
        LogicalPlan::Filter(filter) => plan_contains_view_scan(&filter.input),
        LogicalPlan::Projection(proj) => plan_contains_view_scan(&proj.input),
        LogicalPlan::GraphJoins(joins) => plan_contains_view_scan(&joins.input),
        LogicalPlan::GroupBy(group_by) => plan_contains_view_scan(&group_by.input),
        LogicalPlan::OrderBy(order_by) => plan_contains_view_scan(&order_by.input),
        LogicalPlan::Skip(skip) => plan_contains_view_scan(&skip.input),
        LogicalPlan::Limit(limit) => plan_contains_view_scan(&limit.input),
        LogicalPlan::Cte(cte) => plan_contains_view_scan(&cte.input),
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|i| plan_contains_view_scan(i.as_ref())),
        _ => false,
    }
}

fn apply_property_mapping_to_expr(_expr: &mut RenderExpr, _plan: &LogicalPlan) {
    // DISABLED: Property mapping is now handled in the FilterTagging analyzer pass
    // The analyzer phase maps Cypher properties → database columns, so we should not
    // attempt to re-map them here in the render phase.
    // Re-mapping causes failures because database column names don't exist in property_mappings.

    // The LogicalExpr PropertyAccessExp nodes already have the correct database column names
    // when they arrive here from the analyzer, so we just pass them through unchanged.
}

/// Check if a filter expression appears to be invalid (e.g., "1 = 0")
fn is_invalid_filter_expression(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::OperatorApplicationExp(op) => {
            // Check for "1 = 0" pattern
            if matches!(op.operator, Operator::Equal) && op.operands.len() == 2 {
                matches!(
                    (&op.operands[0], &op.operands[1]),
                    (
                        RenderExpr::Literal(Literal::Integer(1)),
                        RenderExpr::Literal(Literal::Integer(0))
                    ) | (
                        RenderExpr::Literal(Literal::Integer(0)),
                        RenderExpr::Literal(Literal::Integer(1))
                    )
                )
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Get the node label for a given Cypher alias by searching the plan
fn get_node_label_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    log::debug!(
        "get_node_label_for_alias: Searching for alias '{}' in plan type {:?}",
        alias,
        std::mem::discriminant(plan)
    );

    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            log::debug!(
                "get_node_label_for_alias: Found GraphNode with matching alias '{}'",
                alias
            );
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!(
                "get_node_label_for_alias: GraphNode alias '{}' doesn't match '{}', recursing",
                node.alias,
                alias
            );
            get_node_label_for_alias(alias, &node.input)
        }
        LogicalPlan::GraphRel(rel) => {
            log::debug!(
                "get_node_label_for_alias: Searching GraphRel for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &rel.left)
                .or_else(|| {
                    log::debug!(
                        "get_node_label_for_alias: Alias '{}' not in left, trying center",
                        alias
                    );
                    get_node_label_for_alias(alias, &rel.center)
                })
                .or_else(|| {
                    log::debug!(
                        "get_node_label_for_alias: Alias '{}' not in center, trying right",
                        alias
                    );
                    get_node_label_for_alias(alias, &rel.right)
                })
        }
        LogicalPlan::Filter(filter) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through Filter for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &filter.input)
        }
        LogicalPlan::Projection(proj) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through Projection for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &proj.input)
        }
        LogicalPlan::GraphJoins(joins) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through GraphJoins for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &joins.input)
        }
        LogicalPlan::OrderBy(order_by) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through OrderBy for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &order_by.input)
        }
        LogicalPlan::Skip(skip) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through Skip for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &skip.input)
        }
        LogicalPlan::Limit(limit) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through Limit for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &limit.input)
        }
        LogicalPlan::GroupBy(group_by) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through GroupBy for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &group_by.input)
        }
        LogicalPlan::Cte(cte) => {
            log::debug!(
                "get_node_label_for_alias: Recursing through Cte for alias '{}'",
                alias
            );
            get_node_label_for_alias(alias, &cte.input)
        }
        LogicalPlan::Union(union) => {
            log::debug!(
                "get_node_label_for_alias: Searching Union for alias '{}'",
                alias
            );
            for input in &union.inputs {
                if let Some(label) = get_node_label_for_alias(alias, input) {
                    return Some(label);
                }
            }
            None
        }
        _ => {
            log::debug!(
                "get_node_label_for_alias: No match for alias '{}' in plan type {:?}",
                alias,
                std::mem::discriminant(plan)
            );
            None
        }
    }
}

fn references_union_cte_in_join(joining_on: &[OperatorApplication], cte_name: &str) -> bool {
    for op_app in joining_on {
        if references_union_cte_in_operand(&op_app.operands[0], cte_name)
            || references_union_cte_in_operand(&op_app.operands[1], cte_name)
        {
            return true;
        }
    }
    false
}

fn references_union_cte_in_operand(operand: &RenderExpr, cte_name: &str) -> bool {
    match operand {
        RenderExpr::PropertyAccessExp(prop_access) => {
            // Check if this property access references the union CTE
            // We can't easily check table alias here, but we can check if it references the CTE name
            // For now, just check if it's a property access that might need updating
            prop_access.column.0.raw() == "from_id" || prop_access.column.0.raw() == "to_id"
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            references_union_cte_in_join(&[op_app.clone()], cte_name)
        }
        _ => false,
    }
}

fn update_join_expression_for_union_cte(op_app: &mut OperatorApplication, table_alias: &str) {
    // Recursively update expressions to use standardized column names for union CTEs
    for operand in op_app.operands.iter_mut() {
        update_operand_for_union_cte(operand, table_alias);
    }
}

fn update_operand_for_union_cte(operand: &mut RenderExpr, table_alias: &str) {
    match operand {
        RenderExpr::Column(col) => {
            // Update column references to use standardized names
            if col.0.raw() == "from_id" {
                *operand = RenderExpr::Column(Column(PropertyValue::Column("from_node_id".to_string())));
            } else if col.0.raw() == "to_id" {
                *operand = RenderExpr::Column(Column(PropertyValue::Column("to_node_id".to_string())));
            }
        }
        RenderExpr::PropertyAccessExp(prop_access) => {
            // Update property access column references
            if prop_access.column.0.raw() == "from_id" {
                prop_access.column = Column(PropertyValue::Column("from_node_id".to_string()));
            } else if prop_access.column.0.raw() == "to_id" {
                prop_access.column = Column(PropertyValue::Column("to_node_id".to_string()));
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op_app) => {
            update_join_expression_for_union_cte(inner_op_app, table_alias);
        }
        _ => {} // Other expression types don't need updating
    }
}
