//! Select Builder Module
//!
//! This module handles the extraction and processing of SELECT items from logical plans.
//! It manages property expansion, aggregation handling, wildcard expansion, and
//! denormalized node processing for RETURN clauses.
//!
//! Key responsibilities:
//! - Convert LogicalExpr items to SelectItem structures
//! - Handle property expansion for table aliases (u.name, u.email, etc.)
//! - Process wildcard expansion (u.* → explicit property list)
//! - Apply aggregation wrapping (anyLast() for non-ID columns in GROUP BY)
//! - Handle denormalized node properties from edge tables
//! - Support path variable extraction (nodes(p), relationships(p))
//! - Manage collect() function expansion

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};
use crate::query_planner::logical_plan::{LogicalPlan, ProjectionItem};
use crate::query_planner::typed_variable::{TypedVariable, VariableSource};
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::properties_builder::PropertiesBuilder;
use crate::render_plan::render_expr::{
    Column, ColumnAlias, PropertyAccess, RenderExpr, ScalarFnCall, TableAlias as RenderTableAlias,
};
use crate::render_plan::SelectItem;

/// SelectBuilder trait for extracting SELECT items from logical plans
pub trait SelectBuilder {
    /// Extract SELECT items from the logical plan
    fn extract_select_items(
        &self,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) -> Result<Vec<SelectItem>, RenderBuildError>;
}

/// Implementation of SelectBuilder for LogicalPlan
impl SelectBuilder for LogicalPlan {
    fn extract_select_items(
        &self,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) -> Result<Vec<SelectItem>, RenderBuildError> {
        log::warn!("🔍🔍🔍 extract_select_items CALLED on plan type");
        crate::debug_println!("DEBUG: extract_select_items called on: {:?}", self);
        let select_items = match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::ViewScan(view_scan) => {
                // Build select items from ViewScan's property mappings and projections
                // This is needed for multiple relationship types where ViewScan nodes are created
                // for start/end nodes but don't have explicit projections

                if !view_scan.projections.is_empty() {
                    // Use explicit projections if available
                    view_scan
                        .projections
                        .iter()
                        .map(|proj: &LogicalExpr| {
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
                        .map(|(prop_name, prop_value): (&String, &PropertyValue)| {
                            Ok(SelectItem {
                                expression: RenderExpr::Column(Column(prop_value.clone())),
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
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate SELECT items for both left and right nodes
                // This fixes OPTIONAL MATCH queries where the right node (b) was being ignored
                let mut items = vec![];

                // Get SELECT items from left node
                items.extend(graph_rel.left.extract_select_items(plan_ctx)?);

                // Get SELECT items from right node (for OPTIONAL MATCH, this is the optional part)
                items.extend(graph_rel.right.extract_select_items(plan_ctx)?);

                items
            }
            LogicalPlan::Filter(filter) => filter.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Projection(projection) => {
                // Convert ProjectionItem expressions to SelectItems
                // CRITICAL: Expand table aliases (RETURN n → all properties)
                let mut select_items = vec![];

                for item in &projection.items {
                    log::debug!("🔍 TRACING: Processing SELECT item: {:?}", item.expression);
                    match &item.expression {
                        // Case 0: ColumnAlias (regular column reference)
                        LogicalExpr::ColumnAlias(col_alias) => {
                            log::info!(
                                "🔍 ColumnAlias('{}') - treating as regular column",
                                col_alias.0
                            );

                            // Regular column alias - pass through as-is
                            select_items.push(SelectItem {
                                expression: RenderExpr::ColumnAlias(ColumnAlias(
                                    col_alias.0.clone(),
                                )),
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ColumnAlias(ca.0.clone())),
                            });
                        }

                        // Case 1: TableAlias (e.g., RETURN n)
                        LogicalExpr::TableAlias(table_alias) => {
                            log::warn!(
                                "🔍 Processing TableAlias('{}'), has_plan_ctx={}",
                                table_alias.0, plan_ctx.is_some()
                            );

                            // NEW APPROACH: Use TypedVariable for type/source checking
                            if let Some(plan_ctx) = plan_ctx {
                                log::warn!("  🔍 Looking up '{}' in plan_ctx...", table_alias.0);
                                match plan_ctx.lookup_variable(&table_alias.0) {
                                    Some(typed_var) if typed_var.is_entity() => {
                                        // Entity (Node or Relationship) - expand properties
                                        match &typed_var.source() {
                                            VariableSource::Match => {
                                                // Base table: use schema + logical plan table alias
                                                self.expand_base_table_entity(
                                                    &table_alias.0,
                                                    typed_var,
                                                    &mut select_items,
                                                    Some(plan_ctx),
                                                );
                                            }
                                            VariableSource::Cte { cte_name } => {
                                                // CTE: parse CTE name, compute FROM alias, expand
                                                self.expand_cte_entity(
                                                    &table_alias.0,
                                                    typed_var,
                                                    cte_name,
                                                    Some(plan_ctx),
                                                    &mut select_items,
                                                );
                                            }
                                            _ => {
                                                log::warn!("⚠️ Entity variable '{}' has unexpected source, treating as scalar", table_alias.0);
                                                select_items.push(SelectItem {
                                                    expression: RenderExpr::ColumnAlias(
                                                        ColumnAlias(table_alias.0.clone()),
                                                    ),
                                                    col_alias: item
                                                        .col_alias
                                                        .as_ref()
                                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                                });
                                            }
                                        }
                                    }
                                    Some(typed_var) if typed_var.is_scalar() => {
                                        // Scalar - single item, no expansion
                                        match &typed_var.source() {
                                            VariableSource::Cte { cte_name } => {
                                                self.expand_cte_scalar(
                                                    &table_alias.0,
                                                    cte_name,
                                                    &mut select_items,
                                                );
                                            }
                                            _ => {
                                                // Base table scalar or other
                                                select_items.push(SelectItem {
                                                    expression: RenderExpr::ColumnAlias(
                                                        ColumnAlias(table_alias.0.clone()),
                                                    ),
                                                    col_alias: item
                                                        .col_alias
                                                        .as_ref()
                                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                                });
                                            }
                                        }
                                    }
                                    Some(typed_var) if typed_var.is_path() => {
                                        // Path variable - expand to tuple of path components
                                        // Handles both VLP (variable-length) and fixed single-hop paths
                                        log::warn!(
                                            "🔍 Found PATH variable '{}', calling expand_path_variable",
                                            table_alias.0
                                        );
                                        self.expand_path_variable(
                                            &table_alias.0,
                                            typed_var,
                                            &mut select_items,
                                            Some(plan_ctx),
                                        );
                                    }
                                    _ => {
                                        log::warn!("  ✗ Variable '{}' NOT FOUND or not a recognized type in plan_ctx", table_alias.0);
                                        // Unknown variable - check if it's a path by looking for GraphRel
                                        if let Some(graph_rel) = self.find_graph_rel_for_path(&table_alias.0) {
                                            log::info!(
                                                "🔍 Found unregistered path variable '{}' in GraphRel, expanding with actual aliases",
                                                table_alias.0
                                            );
                                            // Create a minimal TypedVariable for path expansion
                                            // The expand_path_variable will use find_graph_rel_for_path again to get aliases
                                            use crate::query_planner::typed_variable::{TypedVariable, PathVariable, VariableSource};
                                            let path_var = TypedVariable::Path(
                                                PathVariable {
                                                    source: VariableSource::Match,
                                                    start_node: Some(graph_rel.left_connection.clone()),
                                                    end_node: Some(graph_rel.right_connection.clone()),
                                                    relationship: Some(graph_rel.alias.clone()),
                                                    length_bounds: graph_rel.variable_length.as_ref().map(|v| (v.min_hops, v.max_hops)),
                                                    is_shortest_path: graph_rel.shortest_path_mode.is_some(),
                                                }
                                            );
                                            self.expand_path_variable(
                                                &table_alias.0,
                                                &path_var,
                                                &mut select_items,
                                                Some(plan_ctx),
                                            );
                                        } else {
                                            // Really unknown - fallback to old logic
                                            log::warn!("⚠️ Variable '{}' not found in TypedVariable registry or GraphRel, using fallback logic", table_alias.0);
                                            self.fallback_table_alias_expansion(
                                                table_alias,
                                                item,
                                                &mut select_items,
                                            );
                                        }
                                    }
                                }
                            } else {
                                // No PlanCtx available - check if it's a path by looking for GraphRel
                                if let Some(graph_rel) = self.find_graph_rel_for_path(&table_alias.0) {
                                    log::info!(
                                        "🔍 Found unregistered path variable '{}' in GraphRel (no plan_ctx), expanding with actual aliases",
                                        table_alias.0
                                    );
                                    // Create a minimal TypedVariable for path expansion
                                    use crate::query_planner::typed_variable::{TypedVariable, PathVariable, VariableSource};
                                    let path_var = TypedVariable::Path(
                                        PathVariable {
                                            source: VariableSource::Match,
                                            start_node: Some(graph_rel.left_connection.clone()),
                                            end_node: Some(graph_rel.right_connection.clone()),
                                            relationship: Some(graph_rel.alias.clone()),
                                            length_bounds: graph_rel.variable_length.as_ref().map(|v| (v.min_hops, v.max_hops)),
                                            is_shortest_path: graph_rel.shortest_path_mode.is_some(),
                                        }
                                    );
                                    self.expand_path_variable(
                                        &table_alias.0,
                                        &path_var,
                                        &mut select_items,
                                        None, // No plan_ctx available
                                    );
                                } else {
                                    log::warn!(
                                        "⚠️ No PlanCtx available for '{}' and no GraphRel found, using fallback logic",
                                        table_alias.0
                                    );
                                    self.fallback_table_alias_expansion(
                                        table_alias,
                                        item,
                                        &mut select_items,
                                    );
                                }
                            }
                        }

                        // Case 2: PropertyAccessExp with wildcard (e.g., RETURN n.*)
                        LogicalExpr::PropertyAccessExp(prop) if prop.column.raw() == "*" => {
                            log::info!(
                                "🔍 Expanding PropertyAccessExp('{}.*') to properties",
                                prop.table_alias.0
                            );

                            // Check if this is a denormalized edge alias mapping
                            let mapped_alias = crate::render_plan::get_denormalized_alias_mapping(
                                &prop.table_alias.0,
                            )
                            .unwrap_or_else(|| prop.table_alias.0.clone());

                            if mapped_alias != prop.table_alias.0 {
                                log::info!(
                                    "🔍 Denormalized alias mapping found for wildcard: '{}' → '{}'",
                                    prop.table_alias.0,
                                    mapped_alias
                                );
                            }

                            let (properties, table_alias_for_render) =
                                match self.get_properties_with_table_alias(&mapped_alias) {
                                    Ok((props, _)) => {
                                        let props: Vec<(String, String)> = props;
                                        if props.is_empty() {
                                            (None, prop.table_alias.0.clone())
                                        } else {
                                            (Some(props), mapped_alias)
                                        }
                                    }
                                    Err(_) => (None, prop.table_alias.0.clone()),
                                };

                            if let Some(properties) = properties {
                                // Expand to multiple SelectItems, one per property
                                for (prop_name, col_name) in properties {
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(
                                                table_alias_for_render.clone(),
                                            ),
                                            column: PropertyValue::Column(col_name),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.{}",
                                            prop.table_alias.0, prop_name
                                        ))),
                                    });
                                }

                                log::info!(
                                    "✅ Expanded '{}.*' to {} properties",
                                    prop.table_alias.0,
                                    select_items.len()
                                );
                            } else {
                                log::warn!(
                                    "⚠️ No properties found for alias '{}'",
                                    prop.table_alias.0
                                );
                            }
                        }

                        // Case 3: CteEntityRef (e.g., RETURN u when u comes from WITH)
                        // CteEntityRef contains the CTE name and the prefixed columns
                        LogicalExpr::CteEntityRef(cte_ref) => {
                            log::info!(
                                "🔍 Expanding CteEntityRef('{}') from CTE '{}' with {} columns",
                                cte_ref.alias,
                                cte_ref.cte_name,
                                cte_ref.columns.len()
                            );

                            if cte_ref.columns.is_empty() {
                                log::warn!("⚠️ CteEntityRef '{}' has no columns - falling back to TableAlias", cte_ref.alias);
                                select_items.push(SelectItem {
                                    expression: RenderExpr::TableAlias(RenderTableAlias(
                                        cte_ref.alias.clone(),
                                    )),
                                    col_alias: item
                                        .col_alias
                                        .as_ref()
                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                });
                                continue;
                            }

                            // The CTE was aliased as the original variable name (e.g., FROM cte AS u)
                            // So we use the alias as the table reference
                            let table_alias_to_use = cte_ref.alias.clone();

                            // Expand to multiple SelectItems, one per CTE column
                            // CTE columns are already prefixed (u_name, u_email, etc.)
                            for col_name in &cte_ref.columns {
                                // Extract property name from prefixed column (e.g., "u_name" -> "name")
                                let prop_name = col_name
                                    .strip_prefix(&format!("{}_", cte_ref.alias))
                                    .unwrap_or(col_name);

                                select_items.push(SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: RenderTableAlias(table_alias_to_use.clone()),
                                        column: PropertyValue::Column(col_name.clone()),
                                    }),
                                    col_alias: Some(ColumnAlias(format!(
                                        "{}.{}",
                                        cte_ref.alias, prop_name
                                    ))),
                                });
                            }

                            log::info!(
                                "✅ Expanded CteEntityRef '{}' to {} columns",
                                cte_ref.alias,
                                cte_ref.columns.len()
                            );
                        }

                        // Case 4: PropertyAccessExp - special handling for denormalized nodes
                        LogicalExpr::PropertyAccessExp(prop_access) => {
                            let cypher_alias = &prop_access.table_alias.0;
                            let col_name = prop_access.column.raw(); // This is the resolved column name (e.g., "OriginCityName")

                            log::warn!(
                                "🔍🔍🔍 Case 4 PropertyAccessExp: cypher_alias='{}', col_name='{}'",
                                cypher_alias,
                                col_name
                            );

                            // ✅ DETERMINISTIC LOGIC: Check if this variable comes from a CTE
                            // VLP endpoint nodes and WITH clause variables are CTE-sourced
                            // For CTE variables, properties should reference the CTE alias directly,
                            // NOT use denormalized property table resolution
                            log::error!(
                                "🔍🔍🔍 TRACING: Checking TypedVariable for alias '{}'",
                                cypher_alias
                            );
                            if let Some(ctx) = plan_ctx {
                                if let Some(typed_var) = ctx.lookup_variable(cypher_alias) {
                                    log::error!(
                                        "🔍🔍🔍 TRACING: Found typed_var for '{}', source={:?}",
                                        cypher_alias,
                                        typed_var.source()
                                    );
                                    if matches!(
                                        typed_var.source(),
                                        crate::query_planner::typed_variable::VariableSource::Cte { .. }
                                    ) {
                                        log::error!(
                                            "🔍🔍🔍 TRACING: Variable '{}' is CTE-sourced - skipping get_properties_with_table_alias",
                                            cypher_alias
                                        );
                                        // Pass through as-is - will use CTE alias from PropertyAccessExp
                                        select_items.push(SelectItem {
                                            expression: item.expression.clone().try_into()?,
                                            col_alias: item
                                                .col_alias
                                                .as_ref()
                                                .map(|ca| ca.clone().try_into())
                                                .transpose()?,
                                        });
                                        continue;
                                    } else {
                                        log::error!(
                                            "🔍🔍🔍 TRACING: Variable '{}' is NOT CTE-sourced (source={:?}) - will call get_properties_with_table_alias",
                                            cypher_alias, typed_var.source()
                                        );
                                    }
                                } else {
                                    log::error!("🔍🔍🔍 TRACING: Variable '{}' NOT found in TypedVariable registry", cypher_alias);
                                }
                            } else {
                                log::error!("🔍🔍🔍 TRACING: No plan_ctx available for TypedVariable lookup");
                            }

                            log::warn!("   → trying get_properties_with_table_alias...");

                            // For denormalized nodes in edges, we need to get the actual table alias
                            // Try to get properties with actual table alias
                            if let Ok((_properties, Some(actual_table_alias))) =
                                self.get_properties_with_table_alias(cypher_alias)
                            {
                                log::warn!(
                                    "🔍 Using actual table alias '{}' for {}.{}",
                                    actual_table_alias,
                                    cypher_alias,
                                    col_name
                                );
                                select_items.push(SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: RenderTableAlias(actual_table_alias),
                                        column: PropertyValue::Column(col_name.to_string()),
                                    }),
                                    col_alias: item
                                        .col_alias
                                        .as_ref()
                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                });
                                continue;
                            }

                            // Default handling: pass through the PropertyAccessExp as-is
                            select_items.push(SelectItem {
                                expression: item.expression.clone().try_into()?,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ca.clone().try_into())
                                    .transpose()?,
                            });
                        }

                        // Case 5: Other regular expressions (function call, literals, etc.)
                        _ => {
                            log::warn!(
                                "🔍 SelectBuilder Case 5 (Other): Expression type = {:?}",
                                item.expression
                            );
                            select_items.push(SelectItem {
                                expression: item.expression.clone().try_into()?,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ca.clone().try_into())
                                    .transpose()?,
                            });
                        }
                    }
                }

                select_items
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                graph_joins.input.extract_select_items(plan_ctx)?
            }
            LogicalPlan::GroupBy(group_by) => {
                // GroupBy doesn't define select items, extract from input
                group_by.input.extract_select_items(plan_ctx)?
            }
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Skip(skip) => skip.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Limit(limit) => limit.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Cte(cte) => cte.input.extract_select_items(plan_ctx)?,
            LogicalPlan::Union(_) => vec![],
            LogicalPlan::PageRank(_) => vec![],
            LogicalPlan::Unwind(u) => u.input.extract_select_items(plan_ctx)?,
            LogicalPlan::CartesianProduct(cp) => {
                // Combine select items from both sides
                log::warn!("🔍 CartesianProduct.extract_select_items START");
                let left_items = cp.left.extract_select_items(plan_ctx)?;
                log::warn!(
                    "🔍 CartesianProduct.extract_select_items: left side returned {} items",
                    left_items.len()
                );
                let right_items = cp.right.extract_select_items(plan_ctx)?;
                log::warn!(
                    "🔍 CartesianProduct.extract_select_items: right side returned {} items, combining...",
                    right_items.len()
                );
                let mut items = left_items;
                items.extend(right_items);
                log::warn!(
                    "🔍 CartesianProduct.extract_select_items DONE: total {} items",
                    items.len()
                );
                items
            }
            LogicalPlan::GraphNode(graph_node) => {
                graph_node.input.extract_select_items(plan_ctx)?
            }
            LogicalPlan::WithClause(wc) => {
                log::warn!("🔍 WithClause.extract_select_items: calling extract on input");
                let items = wc.input.extract_select_items(plan_ctx)?;
                log::warn!(
                    "🔍 WithClause.extract_select_items DONE: extracted {} items from input plan",
                    items.len()
                );
                for (idx, item) in items.iter().enumerate() {
                    log::warn!(
                        "🔍   Item[{}]: alias={:?}",
                        idx,
                        item.col_alias.as_ref().map(|a| a.0.clone())
                    );
                }
                items
            }
        };

        Ok(select_items)
    }
}

// ============================================================================
// Helper Methods for TypedVariable-Based Resolution
// ============================================================================

impl LogicalPlan {
    /// Expand a base table entity (Node/Relationship from MATCH)
    fn expand_base_table_entity(
        &self,
        alias: &str,
        typed_var: &TypedVariable,
        select_items: &mut Vec<SelectItem>,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) {
        log::info!("✅ Expanding base table entity '{}' to properties", alias);

        // Get labels from TypedVariable
        let _labels = match typed_var {
            TypedVariable::Node(node) => &node.labels,
            TypedVariable::Relationship(rel) => &rel.rel_types,
            _ => return, // Should not happen
        };

        // CRITICAL: Check if this alias is a FK-edge (denormalized on another table)
        // For FK-edge patterns like (u)-[r:AUTHORED]->(po), relationship r is stored ON po table
        // We need to select columns from po table but alias them as r.*
        let (actual_table_alias, is_fk_edge) = if let Some(ctx) = plan_ctx {
            if let Some((edge_alias, _is_from, _label, _type)) = ctx.get_denormalized_alias_info(alias) {
                log::info!(
                    "🔑 FK-edge detected: '{}' is denormalized on '{}'",
                    alias, edge_alias
                );
                (edge_alias.clone(), true)
            } else {
                // Try global denormalized alias mapping (for SingleTableScan)
                let mapped = crate::render_plan::get_denormalized_alias_mapping(alias)
                    .unwrap_or_else(|| alias.to_string());
                (mapped, false)
            }
        } else {
            // No plan_ctx, use global mapping
            let mapped = crate::render_plan::get_denormalized_alias_mapping(alias)
                .unwrap_or_else(|| alias.to_string());
            (mapped, false)
        };

        if actual_table_alias != alias {
            log::info!(
                "🔍 {} alias mapping: '{}' → '{}'",
                if is_fk_edge { "FK-edge" } else { "Denormalized" },
                alias,
                actual_table_alias
            );
        }

        match self.get_properties_with_table_alias(&actual_table_alias) {
            Ok((properties, _)) if !properties.is_empty() => {
                let prop_count = properties.len();
                for (prop_name, col_name) in properties {
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(actual_table_alias.clone()),
                            column: PropertyValue::Column(col_name),
                        }),
                        col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
                    });
                }
                log::info!(
                    "✅ Expanded base table '{}' (actual: '{}') to {} properties",
                    alias,
                    actual_table_alias,
                    prop_count
                );
            }
            _ => {
                log::warn!("⚠️ No properties found for base table entity '{}'", alias);
            }
        }
    }

    /// Expand a CTE-sourced entity (Node/Relationship from WITH)
    fn expand_cte_entity(
        &self,
        alias: &str,
        typed_var: &TypedVariable,
        cte_name: &str,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
        select_items: &mut Vec<SelectItem>,
    ) {
        log::info!(
            "✅ Expanding CTE entity '{}' from CTE '{}' to properties",
            alias,
            cte_name
        );

        // Parse CTE name to get aliases and compute FROM alias
        let from_alias = self.compute_from_alias_from_cte_name(cte_name);
        log::info!("🔍 CTE '{}' → FROM alias '{}'", cte_name, from_alias);

        // Get labels from TypedVariable
        let labels = match typed_var {
            TypedVariable::Node(node) => &node.labels,
            TypedVariable::Relationship(rel) => &rel.rel_types,
            _ => return, // Should not happen
        };

        // Get properties from schema
        let plan_ctx = plan_ctx.unwrap(); // Should always be Some for CTE expansion
        let schema = plan_ctx.schema();
        let properties = if let TypedVariable::Node(_) = typed_var {
            schema.get_node_properties(labels)
        } else {
            schema.get_relationship_properties(labels)
        };

        if properties.is_empty() {
            log::warn!(
                "⚠️ No properties found in schema for CTE entity '{}'",
                alias
            );
            return;
        }

        // Generate CTE column names and SelectItems
        let prop_count = properties.len();
        for (prop_name, db_column) in properties {
            let cte_column = format!("{}_{}", alias, db_column);
            select_items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(from_alias.clone()),
                    column: PropertyValue::Column(cte_column),
                }),
                col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
            });
        }
        log::info!(
            "✅ Expanded CTE entity '{}' to {} properties",
            alias,
            prop_count
        );
    }

    /// Handle a CTE-sourced scalar (from WITH)
    fn expand_cte_scalar(&self, alias: &str, cte_name: &str, select_items: &mut Vec<SelectItem>) {
        log::info!("✅ Handling CTE scalar '{}' from CTE '{}'", alias, cte_name);

        // Compute FROM alias
        let from_alias = self.compute_from_alias_from_cte_name(cte_name);

        // For scalars, use the alias as column name (assumes CTE generates alias column)
        select_items.push(SelectItem {
            expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                table_alias: RenderTableAlias(from_alias),
                column: PropertyValue::Column(alias.to_string()),
            }),
            col_alias: Some(ColumnAlias(alias.to_string())),
        });
    }

    /// Fallback logic for when TypedVariable is not available
    fn fallback_table_alias_expansion(
        &self,
        table_alias: &TableAlias,
        item: &ProjectionItem,
        select_items: &mut Vec<SelectItem>,
    ) {
        // Base table logic
        let mapped_alias = crate::render_plan::get_denormalized_alias_mapping(&table_alias.0)
            .unwrap_or_else(|| table_alias.0.clone());

        let (properties, table_alias_for_render) =
            match self.get_properties_with_table_alias(&mapped_alias) {
                Ok((props, _)) if !props.is_empty() => (Some(props), mapped_alias),
                _ => (None, table_alias.0.clone()),
            };

        if let Some(properties) = properties {
            for (prop_name, col_name) in properties {
                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(table_alias_for_render.clone()),
                        column: PropertyValue::Column(col_name),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.{}", table_alias.0, prop_name))),
                });
            }
        } else {
            // Scalar fallback
            select_items.push(SelectItem {
                expression: RenderExpr::ColumnAlias(ColumnAlias(table_alias.0.clone())),
                col_alias: item.col_alias.as_ref().map(|ca| ColumnAlias(ca.0.clone())),
            });
        }
    }

    /// Compute FROM alias from CTE name
    fn compute_from_alias_from_cte_name(&self, cte_name: &str) -> String {
        // The FROM alias is simply the CTE name itself
        cte_name.to_string()
    }

    /// Find GraphRel with matching path_variable in the plan tree.
    /// This is used to get the actual connection aliases used in UNION branches.
    fn find_graph_rel_for_path(&self, path_name: &str) -> Option<&crate::query_planner::logical_plan::GraphRel> {
        use crate::query_planner::logical_plan::LogicalPlan;
        match self {
            LogicalPlan::GraphRel(gr) if gr.path_variable.as_deref() == Some(path_name) => Some(gr),
            LogicalPlan::GraphRel(gr) => {
                // Check children
                gr.left.find_graph_rel_for_path(path_name)
                    .or_else(|| gr.right.find_graph_rel_for_path(path_name))
            }
            LogicalPlan::GraphJoins(gj) => gj.input.find_graph_rel_for_path(path_name),
            LogicalPlan::GraphNode(gn) => gn.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Projection(p) => p.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Filter(f) => f.input.find_graph_rel_for_path(path_name),
            LogicalPlan::GroupBy(gb) => gb.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Limit(l) => l.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Skip(s) => s.input.find_graph_rel_for_path(path_name),
            LogicalPlan::OrderBy(o) => o.input.find_graph_rel_for_path(path_name),
            LogicalPlan::Union(u) => {
                // Check first branch - all branches should have same path structure
                u.inputs.first().and_then(|branch| branch.find_graph_rel_for_path(path_name))
            }
            _ => None,
        }
    }

    /// Expand a path variable to its constituent components
    ///
    /// For VLP (variable-length paths) queries:
    ///   - Uses VLP CTE columns: path_nodes, path_edges, path_relationships, hop_count
    ///   - tuple(t.path_nodes, t.path_edges, t.path_relationships, t.hop_count) AS "p"
    ///
    /// For fixed single-hop paths:
    ///   - Constructs path from actual node/relationship aliases
    ///   - Adds component property columns based on schema mappings
    fn expand_path_variable(
        &self,
        path_alias: &str,
        typed_var: &TypedVariable,
        select_items: &mut Vec<SelectItem>,
        plan_ctx: Option<&crate::query_planner::plan_ctx::PlanCtx>,
    ) {
        log::warn!("🔍 expand_path_variable ENTRY: path='{}', has_plan_ctx={}", path_alias, plan_ctx.is_some());
        
        // Check if this is a VLP (variable-length path) or fixed-hop path
        let path_var = match typed_var.as_path() {
            Some(pv) => pv,
            None => {
                log::warn!("expand_path_variable called with non-path variable");
                return;
            }
        };
        
        // VLP paths have length_bounds set (e.g., *1..3, *, *2..)
        // Fixed single-hop paths have length_bounds = None
        let is_vlp = path_var.length_bounds.is_some() || path_var.is_shortest_path;
        
        if is_vlp {
            // VLP path - use VLP CTE columns
            use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
            let cte_alias = VLP_CTE_FROM_ALIAS;
            
            log::info!(
                "🔍 Expanding VLP path variable '{}' using CTE columns from '{}'",
                path_alias, cte_alias
            );

            // Create a tuple expression wrapping all path components
            // tuple(t.path_nodes, t.path_edges, t.path_relationships, t.hop_count)
            select_items.push(SelectItem {
                expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("path_nodes".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("path_edges".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("path_relationships".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.to_string()),
                            column: PropertyValue::Column("hop_count".to_string()),
                        }),
                    ],
                }),
                col_alias: Some(ColumnAlias(path_alias.to_string())),
            });
        } else {
            // Fixed single-hop path - expand component properties
            // All node tables are now in FROM clause (after FK-edge duplicate fix),
            // so we can expand properties for start node, end node, and relationship.
            
            // Try to find the actual GraphRel in the plan tree to get real aliases
            // This is critical for UNION branches which use branch-specific aliases (t1_0, t2_0)
            // instead of the original aliases (a, b) registered in plan_ctx
            let (start_alias, end_alias, rel_alias) = if let Some(graph_rel) = self.find_graph_rel_for_path(path_alias) {
                log::info!(
                    "🔍 Found GraphRel for path '{}' with actual aliases: left={}, right={}, rel={}",
                    path_alias, graph_rel.left_connection, graph_rel.right_connection, graph_rel.alias
                );
                (
                    graph_rel.left_connection.clone(),
                    graph_rel.right_connection.clone(),
                    graph_rel.alias.clone(),
                )
            } else {
                // Fallback to registered aliases from plan_ctx (for non-UNION patterns)
                let start = path_var.start_node.as_deref().unwrap_or("_start").to_string();
                let end = path_var.end_node.as_deref().unwrap_or("_end").to_string();
                let rel = path_var.relationship.as_deref().unwrap_or("_rel").to_string();
                log::info!(
                    "🔍 Using registered aliases for path '{}': start={}, end={}, rel={}",
                    path_alias, start, end, rel
                );
                (start, end, rel)
            };
            
            log::info!(
                "🔍 Expanding fixed-hop path variable '{}': start={}, end={}, rel={}",
                path_alias, start_alias, end_alias, rel_alias
            );

            // Expand properties for each component if we have plan_ctx
            if let Some(ctx) = plan_ctx {
                log::warn!("  🔍 Have plan_ctx, looking up path components: start={}, end={}, rel={}", start_alias, end_alias, rel_alias);
                
                // Expand start node properties
                if let Some(typed_var) = ctx.lookup_variable(&start_alias) {
                    let variant_name = if typed_var.is_node() { "Node" } else if typed_var.is_relationship() { "Relationship" } else if typed_var.is_scalar() { "Scalar" } else if typed_var.as_path().is_some() { "Path" } else { "Unknown" };
                    log::debug!("  ✓ Found start node '{}' in plan_ctx, variant={}, is_entity={}", start_alias, variant_name, typed_var.is_entity());
                    if typed_var.is_entity() {
                        log::info!("  📦 Expanding start node '{}' properties", start_alias);
                        match typed_var.source() {
                            VariableSource::Match => {
                                self.expand_base_table_entity(&start_alias, typed_var, select_items, Some(ctx));
                            }
                            VariableSource::Cte { cte_name } => {
                                self.expand_cte_entity(&start_alias, typed_var, cte_name, Some(ctx), select_items);
                            }
                            _ => {}
                        }
                    }
                } else {
                    log::warn!("  ✗ Start node '{}' not found in plan_ctx", start_alias);
                }

                // Expand end node properties
                if let Some(typed_var) = ctx.lookup_variable(&end_alias) {
                    log::debug!("  ✓ Found end node '{}' in plan_ctx", end_alias);
                    if typed_var.is_entity() {
                        log::info!("  📦 Expanding end node '{}' properties", end_alias);
                        match typed_var.source() {
                            VariableSource::Match => {
                                self.expand_base_table_entity(&end_alias, typed_var, select_items, Some(ctx));
                            }
                            VariableSource::Cte { cte_name } => {
                                self.expand_cte_entity(&end_alias, typed_var, cte_name, Some(ctx), select_items);
                            }
                            _ => {}
                        }
                    }
                } else {
                    log::warn!("  ✗ End node '{}' not found in plan_ctx", end_alias);
                }

                // Expand relationship properties
                if let Some(typed_var) = ctx.lookup_variable(&rel_alias) {
                    log::debug!("  ✓ Found relationship '{}' in plan_ctx, is_entity={}, source={:?}", 
                        rel_alias, typed_var.is_entity(), typed_var.source());
                    if typed_var.is_entity() {
                        log::info!("  📦 Expanding relationship '{}' properties", rel_alias);
                        match typed_var.source() {
                            VariableSource::Match => {
                                self.expand_base_table_entity(&rel_alias, typed_var, select_items, Some(ctx));
                            }
                            VariableSource::Cte { cte_name } => {
                                self.expand_cte_entity(&rel_alias, typed_var, cte_name, Some(ctx), select_items);
                            }
                            _ => {}
                        }
                    }
                } else {
                    log::warn!("  ✗ Relationship '{}' not found in plan_ctx", rel_alias);
                }
            } else {
                log::warn!("  ✗ NO plan_ctx available for path variable '{}' property expansion!", path_alias);
            }

            // Add the path metadata column with component aliases
            // Format: tuple('fixed_path', start_alias, end_alias, rel_alias)
            select_items.push(SelectItem {
                expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                    name: "tuple".to_string(),
                    args: vec![
                        // Path type marker
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            "fixed_path".to_string(),
                        )),
                        // Start node alias
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            start_alias.to_string(),
                        )),
                        // End node alias
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            end_alias.to_string(),
                        )),
                        // Relationship alias
                        RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                            rel_alias.to_string(),
                        )),
                    ],
                }),
                col_alias: Some(ColumnAlias(path_alias.to_string())),
            });
        }
    }

    /// Expand path variable directly from GraphRel metadata (for UNION branches without plan_ctx)
    /// Creates the fixed-path tuple: tuple('fixed_path', start_alias, end_alias, rel_alias)
    fn expand_path_variable_from_graph_rel(
        path_alias: &str,
        start_alias: &str,
        end_alias: &str,
        rel_alias: &str,
    ) -> SelectItem {
        log::info!(
            "🔍 Expanding fixed-hop path variable '{}' from GraphRel: start={}, end={}, rel={}",
            path_alias, start_alias, end_alias, rel_alias
        );

        // Add the path metadata column with component aliases
        // Format: tuple('fixed_path', start_alias, end_alias, rel_alias)
        SelectItem {
            expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                name: "tuple".to_string(),
                args: vec![
                    // Path type marker
                    RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                        "fixed_path".to_string(),
                    )),
                    // Start node alias
                    RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                        start_alias.to_string(),
                    )),
                    // End node alias
                    RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                        end_alias.to_string(),
                    )),
                    // Relationship alias
                    RenderExpr::Literal(crate::render_plan::render_expr::Literal::String(
                        rel_alias.to_string(),
                    )),
                ],
            }),
            col_alias: Some(ColumnAlias(path_alias.to_string())),
        }
    }
}
