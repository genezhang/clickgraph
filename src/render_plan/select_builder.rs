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
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::join_context::{JoinContext, VLP_CTE_FROM_ALIAS};
use crate::query_planner::logical_expr::{
    CteEntityRef as LogicalCteEntityRef, LogicalExpr, PropertyAccess as LogicalPropertyAccess,
    TableAlias,
};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::cte_extraction::get_path_variable;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::properties_builder::PropertiesBuilder;
use crate::render_plan::render_expr::{
    AggregateFnCall, Column, ColumnAlias, PropertyAccess, RenderExpr, ScalarFnCall,
    TableAlias as RenderTableAlias,
};
use crate::render_plan::SelectItem;

/// Try to resolve properties for an alias from the CTE column registry
/// This is used for WITH-exported variables like 'person' in 'WITH u AS person'
fn try_get_cte_properties(alias: &str) -> Option<Vec<(String, String)>> {
    use crate::render_plan::get_cte_column_registry;
    
    let registry = get_cte_column_registry()?;
    
    log::debug!("🔍 try_get_cte_properties({}) - checking registry", alias);
    log::debug!("  Registry aliases: {:?}", registry.alias_to_cte_name.keys().collect::<Vec<_>>());
    log::debug!("  Registry mappings: {} entries", registry.alias_property_to_column.len());
    
    // Check if this alias is registered as a CTE alias
    if !registry.is_cte_alias(alias) {
        log::debug!("  ❌ '{}' is NOT a CTE alias", alias);
        return None;
    }
    
    log::debug!("  ✅ '{}' IS a CTE alias", alias);
    
    // Collect all properties for this CTE alias from the registry
    let mut properties = Vec::new();
    for ((cte_alias, prop_name), col_name) in &registry.alias_property_to_column {
        if cte_alias == alias {
            properties.push((prop_name.clone(), col_name.clone()));
            log::debug!("    Found property: {} -> {}", prop_name, col_name);
        }
    }
    
    // Sort for consistent output
    properties.sort_by(|a, b| a.0.cmp(&b.0));
    
    log::debug!("  Collected {} properties", properties.len());
    
    if properties.is_empty() {
        None
    } else {
        Some(properties)
    }
}

/// SelectBuilder trait for extracting SELECT items from logical plans
pub trait SelectBuilder {
    /// Extract SELECT items from the logical plan
    fn extract_select_items(&self) -> Result<Vec<SelectItem>, RenderBuildError>;
}

/// Implementation of SelectBuilder for LogicalPlan
impl SelectBuilder for LogicalPlan {
    fn extract_select_items(&self) -> Result<Vec<SelectItem>, RenderBuildError> {
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
                // Convert ProjectionItem expressions to SelectItems
                // CRITICAL: Expand table aliases (RETURN n → all properties)
                let mut select_items = vec![];

                for item in &projection.items {
                    match &item.expression {
                        // Case 1: TableAlias (e.g., RETURN n)
                        LogicalExpr::TableAlias(table_alias) => {
                            log::info!(
                                "🔍 Expanding TableAlias('{}') to properties",
                                table_alias.0
                            );

                            // CRITICAL FIX: Check if this is a WITH-exported CTE variable first
                            // For 'WITH u AS person', person is a CTE alias with pre-determined columns
                            // These should NOT be looked up from the schema, but from the CTE registry
                            let properties_opt = try_get_cte_properties(&table_alias.0);
                            
                            let properties = if let Some(cte_props) = properties_opt {
                                log::info!("✅ Using CTE properties for CTE alias '{}' (found {} properties)", table_alias.0, cte_props.len());
                                Some(cte_props)
                            } else {
                                // Not a CTE alias, try to get from the logical plan
                                match self.get_properties_with_table_alias(&table_alias.0) {
                                    Ok((props, _)) => {
                                        if props.is_empty() {
                                            None
                                        } else {
                                            Some(props)
                                        }
                                    }
                                    Err(_) => None,
                                }
                            };
                            
                            if let Some(properties) = properties {
                                // Expand to multiple SelectItems, one per property
                                for (prop_name, col_name) in properties {
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(
                                            PropertyAccess {
                                                table_alias: RenderTableAlias(
                                                    table_alias.0.clone(),
                                                ),
                                                column: PropertyValue::Column(col_name),
                                            },
                                        ),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.{}",
                                            table_alias.0, prop_name
                                        ))),
                                    });
                                }
                                log::info!(
                                    "✅ Expanded '{}' to {} properties",
                                    table_alias.0,
                                    select_items.len()
                                );
                            } else if let Some(path_var_name) = get_path_variable(self) {
                                // Check if this is a path variable
                                if path_var_name == table_alias.0 {
                                    log::info!(
                                        "✅ Path variable '{}' detected, expanding to VLP CTE path columns",
                                        table_alias.0
                                    );

                                    // Use the VLP CTE default alias for path columns
                                    let cte_alias = VLP_CTE_FROM_ALIAS;

                                    // Expand to the standard VLP path columns
                                    // These columns are generated by the VLP CTE builder
                                    let path_columns = vec![
                                        ("nodes", "path_nodes"),
                                        ("relationships", "path_relationships"),
                                        ("edges", "path_edges"),
                                        ("length", "hop_count"),
                                    ];

                                    for (prop_name, col_name) in path_columns {
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias(
                                                        cte_alias.to_string(),
                                                    ),
                                                    column: PropertyValue::Column(
                                                        col_name.to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.{}",
                                                table_alias.0, prop_name
                                            ))),
                                        });
                                    }
                                } else {
                                    // Not a path variable and no properties - treat as scalar
                                    log::warn!(
                                        "⚠️ No properties found for alias '{}', treating as scalar",
                                        table_alias.0
                                    );
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::ColumnAlias(ColumnAlias(
                                            table_alias.0.clone(),
                                        )),
                                        col_alias: item
                                            .col_alias
                                            .as_ref()
                                            .map(|ca| ColumnAlias(ca.0.clone())),
                                    });
                                }
                            } else {
                                // No properties and not a path variable - treat as scalar
                                log::warn!(
                                    "⚠️ No properties found for alias '{}', treating as scalar",
                                    table_alias.0
                                );
                                select_items.push(SelectItem {
                                    expression: RenderExpr::ColumnAlias(ColumnAlias(
                                        table_alias.0.clone(),
                                    )),
                                    col_alias: item
                                        .col_alias
                                        .as_ref()
                                        .map(|ca| ColumnAlias(ca.0.clone())),
                                });
                            }
                        }

                        // Case 2: PropertyAccessExp with wildcard (e.g., RETURN n.*)
                        LogicalExpr::PropertyAccessExp(prop) if prop.column.raw() == "*" => {
                            log::info!(
                                "🔍 Expanding PropertyAccessExp('{}.*') to properties",
                                prop.table_alias.0
                            );

                            // CRITICAL FIX: Check if this is a CTE-sourced variable first
                            let properties_opt = try_get_cte_properties(&prop.table_alias.0);
                            let properties = if let Some(cte_props) = properties_opt {
                                log::info!("✅ Using CTE properties for wildcard expansion on CTE alias '{}' (found {} properties)", prop.table_alias.0, cte_props.len());
                                Some(cte_props)
                            } else {
                                // Not a CTE alias, get from logical plan
                                match self.get_properties_with_table_alias(&prop.table_alias.0) {
                                    Ok((props, _)) => {
                                        if props.is_empty() {
                                            None
                                        } else {
                                            Some(props)
                                        }
                                    }
                                    Err(_) => None,
                                }
                            };
                            
                            if let Some(properties) = properties {
                                // Expand to multiple SelectItems, one per property
                                for (prop_name, col_name) in properties {
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(
                                            PropertyAccess {
                                                table_alias: RenderTableAlias(
                                                    prop.table_alias.0.clone(),
                                                ),
                                                column: PropertyValue::Column(col_name),
                                            },
                                        ),
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

                            // For denormalized nodes in edges, we need to get the actual table alias
                            // Try to get properties with actual table alias
                            if let Ok((_properties, actual_table_alias_opt)) =
                                self.get_properties_with_table_alias(cypher_alias)
                            {
                                if let Some(actual_table_alias) = actual_table_alias_opt {
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
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_select_items()?,
            LogicalPlan::GroupBy(group_by) => {
                // GroupBy doesn't define select items, extract from input
                group_by.input.extract_select_items()?
            }
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items()?,
            LogicalPlan::Skip(skip) => skip.input.extract_select_items()?,
            LogicalPlan::Limit(limit) => limit.input.extract_select_items()?,
            LogicalPlan::Cte(cte) => cte.input.extract_select_items()?,
            LogicalPlan::Union(_) => vec![],
            LogicalPlan::PageRank(_) => vec![],
            LogicalPlan::Unwind(u) => u.input.extract_select_items()?,
            LogicalPlan::CartesianProduct(cp) => {
                // Combine select items from both sides
                let mut items = cp.left.extract_select_items()?;
                items.extend(cp.right.extract_select_items()?);
                items
            }
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_select_items()?,
            LogicalPlan::WithClause(wc) => wc.input.extract_select_items()?,
        };

        Ok(select_items)
    }
}
