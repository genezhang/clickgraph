//! Properties Builder Module
//!
//! This module handles extraction of property information from logical plans.
//! It resolves property mappings for nodes and relationships, handling
//! denormalized schemas and table alias resolution.

use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::plan_builder_helpers::*;
use crate::render_plan::plan_builder_utils::extract_sorted_properties;

/// Result type for properties builder operations
pub type PropertiesBuilderResult<T> = Result<T, RenderBuildError>;

/// Trait for extracting property information from logical plans
pub trait PropertiesBuilder {
    /// Get all properties for an alias, returning both properties and the actual table alias to use.
    /// For denormalized nodes, the table alias is the relationship alias (not the node alias).
    /// Returns: (properties, actual_table_alias) where actual_table_alias is None to use the original alias
    fn get_properties_with_table_alias(
        &self,
        alias: &str,
    ) -> PropertiesBuilderResult<(Vec<(String, String)>, Option<String>)>;
}

impl PropertiesBuilder for LogicalPlan {
    fn get_properties_with_table_alias(
        &self,
        alias: &str,
    ) -> PropertiesBuilderResult<(Vec<(String, String)>, Option<String>)> {
        crate::debug_println!(
            "DEBUG get_properties_with_table_alias: alias='{}', plan type={:?}",
            alias,
            std::mem::discriminant(self)
        );
        match self {
            LogicalPlan::GraphNode(node) if node.alias == alias => {
                // FAST PATH: Use pre-computed projected_columns if available
                // (populated by ProjectedColumnsResolver analyzer pass)
                if let Some(projected_cols) = &node.projected_columns {
                    // projected_columns format: Vec<(property_name, qualified_column)>
                    // e.g., [("firstName", "p.first_name"), ("age", "p.age")]
                    // We need to return unqualified column names: ("firstName", "first_name")
                    let properties: Vec<(String, String)> = projected_cols
                        .iter()
                        .map(|(prop_name, qualified_col)| {
                            // Extract unqualified column: "p.first_name" -> "first_name"
                            // 🔧 FIX: Handle column names with multiple dots like "n.id.orig_h" -> "id.orig_h"
                            // Use splitn(2) to split only on the FIRST dot, keeping the rest intact
                            let unqualified = qualified_col
                                .splitn(2, '.')
                                .nth(1)
                                .unwrap_or(qualified_col)
                                .to_string();
                            (prop_name.clone(), unqualified)
                        })
                        .collect();
                    return Ok((properties, None));
                }

                // FALLBACK: Compute from ViewScan (for nodes without projected_columns)
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    log::debug!("get_properties_with_table_alias: GraphNode '{}' has ViewScan, is_denormalized={}, from_node_properties={:?}, to_node_properties={:?}",
                        alias, scan.is_denormalized,
                        scan.from_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()),
                        scan.to_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()));
                    // For denormalized nodes with properties on the ViewScan (from standalone node query)
                    if scan.is_denormalized {
                        if let Some(from_props) = &scan.from_node_properties {
                            let properties = extract_sorted_properties(from_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} from_node_properties for '{}'", properties.len(), alias);
                                return Ok((properties, None)); // Use original alias
                            }
                        }
                        if let Some(to_props) = &scan.to_node_properties {
                            let properties = extract_sorted_properties(to_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} to_node_properties for '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    } else {
                        // For non-denormalized nodes, properties come from the node table itself
                        // This happens when we have a standalone node query like MATCH (n) RETURN n
                        if let Some(from_props) = &scan.from_node_properties {
                            let properties = extract_sorted_properties(from_props);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} from_node_properties for non-denormalized '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    }
                    // Standard nodes - try property_mapping first
                    let mut properties = extract_sorted_properties(&scan.property_mapping);

                    // ZEEK FIX: If property_mapping is empty, try from_node_properties (for coupled edge schemas)
                    if properties.is_empty() {
                        if let Some(from_props) = &scan.from_node_properties {
                            properties = extract_sorted_properties(from_props);
                        }
                        if properties.is_empty() {
                            if let Some(to_props) = &scan.to_node_properties {
                                properties = extract_sorted_properties(to_props);
                            }
                        }
                    }
                    return Ok((properties, None));
                } else if let LogicalPlan::Union(union_plan) = node.input.as_ref() {
                    // For denormalized polymorphic nodes, the input is a UNION of ViewScans
                    // Each ViewScan has either from_node_properties or to_node_properties
                    // Use the first available ViewScan to get the property list
                    log::debug!(
                        "get_properties_with_table_alias: GraphNode '{}' has Union with {} inputs",
                        alias,
                        union_plan.inputs.len()
                    );
                    if let Some(first_input) = union_plan.inputs.first() {
                        if let LogicalPlan::ViewScan(scan) = first_input.as_ref() {
                            log::debug!("get_properties_with_table_alias: First UNION input is ViewScan, is_denormalized={}, from_node_properties={:?}, to_node_properties={:?}",
                                scan.is_denormalized,
                                scan.from_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()),
                                scan.to_node_properties.as_ref().map(|p| p.keys().collect::<Vec<_>>()));

                            // Try from_node_properties first
                            if let Some(from_props) = &scan.from_node_properties {
                                let properties = extract_sorted_properties(from_props);
                                if !properties.is_empty() {
                                    log::debug!("get_properties_with_table_alias: Returning {} from_node_properties from UNION for '{}'", properties.len(), alias);
                                    return Ok((properties, None));
                                }
                            }
                            // Then try to_node_properties
                            if let Some(to_props) = &scan.to_node_properties {
                                let properties = extract_sorted_properties(to_props);
                                if !properties.is_empty() {
                                    log::debug!("get_properties_with_table_alias: Returning {} to_node_properties from UNION for '{}'", properties.len(), alias);
                                    return Ok((properties, None));
                                } else {
                                    // continue to next case
                                }
                            }
                            // Fallback to property_mapping
                            let properties = extract_sorted_properties(&scan.property_mapping);
                            if !properties.is_empty() {
                                log::debug!("get_properties_with_table_alias: Returning {} property_mapping from UNION for '{}'", properties.len(), alias);
                                return Ok((properties, None));
                            }
                        }
                    }
                }
                // If we reach here, no properties found for this GraphNode
                Ok((vec![], None))
            }
            LogicalPlan::GraphRel(rel) => {
                // Check if this relationship's alias matches
                if rel.alias == alias {
                    if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                        let mut properties = extract_sorted_properties(&scan.property_mapping);

                        // Add from_id and to_id columns for relationships
                        // These are required for RETURN r to expand correctly
                        if let Some(ref from_id) = scan.from_id {
                            properties.insert(0, ("from_id".to_string(), from_id.clone()));
                        }
                        if let Some(ref to_id) = scan.to_id {
                            properties.insert(1, ("to_id".to_string(), to_id.clone()));
                        }

                        return Ok((properties, None));
                    }
                }

                // For denormalized nodes, properties are in the relationship center's ViewScan
                // IMPORTANT: Direction affects which properties to use!
                // - Outgoing: left_connection → from_node_properties, right_connection → to_node_properties
                // - Incoming: left_connection → to_node_properties, right_connection → from_node_properties
                if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    let is_incoming =
                        rel.direction == crate::query_planner::logical_expr::Direction::Incoming;

                    crate::debug_println!("DEBUG GraphRel: alias='{}' checking left='{}', right='{}', rel_alias='{}', direction={:?}",
                        alias, rel.left_connection, rel.right_connection, rel.alias, rel.direction);
                    crate::debug_println!(
                        "DEBUG GraphRel: from_node_properties={:?}, to_node_properties={:?}",
                        scan.from_node_properties
                            .as_ref()
                            .map(|p| p.keys().collect::<Vec<_>>()),
                        scan.to_node_properties
                            .as_ref()
                            .map(|p| p.keys().collect::<Vec<_>>())
                    );

                    // Check if BOTH nodes are denormalized on this edge
                    // If so, right_connection should use left_connection's alias (the FROM table)
                    // because the edge is fully denormalized - no separate JOIN for the edge
                    let left_props_exist = if is_incoming {
                        scan.to_node_properties.is_some()
                    } else {
                        scan.from_node_properties.is_some()
                    };
                    let right_props_exist = if is_incoming {
                        scan.from_node_properties.is_some()
                    } else {
                        scan.to_node_properties.is_some()
                    };
                    let both_nodes_denormalized = left_props_exist && right_props_exist;

                    // Check if alias matches left_connection
                    if alias == rel.left_connection {
                        // For Incoming direction, left node is on the TO side of the edge
                        let props = if is_incoming {
                            &scan.to_node_properties
                        } else {
                            &scan.from_node_properties
                        };
                        if let Some(node_props) = props {
                            let properties = extract_sorted_properties(node_props);
                            if !properties.is_empty() {
                                // Left connection uses its own alias as the FROM table
                                // Return None to use the original alias (which IS the FROM)
                                return Ok((properties, None));
                            }
                        }
                    }
                    // Check if alias matches right_connection
                    if alias == rel.right_connection {
                        // For Incoming direction, right node is on the FROM side of the edge
                        let props = if is_incoming {
                            &scan.from_node_properties
                        } else {
                            &scan.to_node_properties
                        };
                        if let Some(node_props) = props {
                            let properties = extract_sorted_properties(node_props);
                            if !properties.is_empty() {
                                // For fully denormalized edges (both nodes on edge), use left_connection
                                // alias because it's the FROM table and right node shares the same row
                                // For partially denormalized, use relationship alias as before
                                if both_nodes_denormalized {
                                    // Use left_connection alias (the FROM table)
                                    return Ok((properties, Some(rel.left_connection.clone())));
                                } else {
                                    // Use relationship alias for denormalized nodes
                                    return Ok((properties, Some(rel.alias.clone())));
                                }
                            }
                        }
                    }
                }

                // Check left and right branches
                if let Ok(result) = rel.left.get_properties_with_table_alias(alias) {
                    return Ok(result);
                }
                if let Ok(result) = rel.right.get_properties_with_table_alias(alias) {
                    return Ok(result);
                }
                if let Ok(result) = rel.center.get_properties_with_table_alias(alias) {
                    return Ok(result);
                } else {
                    // continue to next case
                }
                // If we reach here, no properties found in this GraphRel
                Ok((vec![], None))
            }
            LogicalPlan::Projection(proj) => {
                return proj.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Filter(filter) => {
                return filter.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::GroupBy(gb) => {
                return gb.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::GraphJoins(joins) => {
                return joins.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::OrderBy(order) => {
                return order.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Skip(skip) => {
                return skip.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Limit(limit) => {
                return limit.input.get_properties_with_table_alias(alias);
            }
            LogicalPlan::Union(union) => {
                // For UNION, check all branches and return the first match
                // All branches should have the same schema, so any match is valid
                for input in &union.inputs {
                    if let Ok(result) = input.get_properties_with_table_alias(alias) {
                        if !result.0.is_empty() {
                            return Ok(result);
                        }
                    }
                }
                Ok((vec![], None)) // No properties found
            }
            _ => Ok((vec![], None)), // No properties found
        }
    }
}
