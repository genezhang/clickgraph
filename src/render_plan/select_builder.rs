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
                    log::error!(
                        "🔍🔍🔍 TRACING: Processing SELECT item: {:?}",
                        item.expression
                    );
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
                            log::info!(
                                "🔍 Expanding TableAlias('{}') to properties",
                                table_alias.0
                            );

                            // NEW APPROACH: Use TypedVariable for type/source checking
                            if let Some(plan_ctx) = plan_ctx {
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
                                        // Path variables come from VLP CTEs with columns: path_nodes, path_edges, path_relationships, hop_count
                                        log::info!(
                                            "🔍 Expanding path variable '{}' to path components",
                                            table_alias.0
                                        );
                                        self.expand_path_variable(
                                            &table_alias.0,
                                            &mut select_items,
                                        );
                                    }
                                    _ => {
                                        // Unknown variable or path/collection - fallback to old logic
                                        log::warn!("⚠️ Variable '{}' not found in TypedVariable registry, using fallback logic", table_alias.0);
                                        self.fallback_table_alias_expansion(
                                            table_alias,
                                            item,
                                            &mut select_items,
                                        );
                                    }
                                }
                            } else {
                                // No PlanCtx available - use fallback logic
                                log::warn!(
                                    "⚠️ No PlanCtx available for '{}', using fallback logic",
                                    table_alias.0
                                );
                                self.fallback_table_alias_expansion(
                                    table_alias,
                                    item,
                                    &mut select_items,
                                );
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
    ) {
        log::info!("✅ Expanding base table entity '{}' to properties", alias);

        // Get labels from TypedVariable
        let _labels = match typed_var {
            TypedVariable::Node(node) => &node.labels,
            TypedVariable::Relationship(rel) => &rel.rel_types,
            _ => return, // Should not happen
        };

        // Use existing logic to get properties and table alias
        let mapped_alias = crate::render_plan::get_denormalized_alias_mapping(alias)
            .unwrap_or_else(|| alias.to_string());

        if mapped_alias != alias {
            log::info!(
                "🔍 Denormalized alias mapping: '{}' → '{}'",
                alias,
                mapped_alias
            );
        }

        match self.get_properties_with_table_alias(&mapped_alias) {
            Ok((properties, _)) if !properties.is_empty() => {
                let prop_count = properties.len();
                for (prop_name, col_name) in properties {
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(mapped_alias.clone()),
                            column: PropertyValue::Column(col_name),
                        }),
                        col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
                    });
                }
                log::info!(
                    "✅ Expanded base table '{}' to {} properties",
                    alias,
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

    /// Expand a path variable to its constituent components
    ///
    /// Path variables from VLP queries contain: path_nodes, path_edges, path_relationships, hop_count
    /// We wrap these in a tuple() for clean output: tuple(path_nodes, path_edges, path_relationships, hop_count) AS "p"
    fn expand_path_variable(&self, path_alias: &str, select_items: &mut Vec<SelectItem>) {
        use crate::query_planner::join_context::VLP_CTE_FROM_ALIAS;
        // The VLP CTE uses alias defined in join_context for the final SELECT from the CTE
        // Path components are columns in the CTE result
        let cte_alias = VLP_CTE_FROM_ALIAS;

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
    }
}
