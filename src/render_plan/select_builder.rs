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
use crate::query_planner::join_context::{VLP_END_ID_COLUMN, VLP_START_ID_COLUMN};
use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};
use crate::query_planner::logical_plan::{LogicalPlan, ProjectionItem};
use crate::query_planner::typed_variable::{TypedVariable, VariableSource};
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::properties_builder::PropertiesBuilder;
use crate::render_plan::render_expr::{
    Column, ColumnAlias, Literal, PropertyAccess, RenderExpr, ScalarFnCall,
    TableAlias as RenderTableAlias,
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
        log::trace!("🔍🔍🔍 extract_select_items CALLED on plan type");
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
                log::trace!(
                    "🔍 GraphRel.extract_select_items: alias={}, path_variable={:?}",
                    graph_rel.alias,
                    graph_rel.path_variable
                );
                // FIX: GraphRel must generate SELECT items for both left and right nodes
                // This fixes OPTIONAL MATCH queries where the right node (b) was being ignored
                let mut items = vec![];

                // Get SELECT items from left node
                items.extend(graph_rel.left.extract_select_items(plan_ctx)?);

                // Get SELECT items from right node (for OPTIONAL MATCH, this is the optional part)
                items.extend(graph_rel.right.extract_select_items(plan_ctx)?);

                // SIMPLE FIX: If GraphRel has path_variable, add the path tuple directly
                // This handles UNION branches without needing plan_ctx or Projection wrapping
                if let Some(ref path_var) = graph_rel.path_variable {
                    log::warn!(
                        "🔍 GraphRel has path_variable '{}', adding path tuple to SELECT",
                        path_var
                    );
                    items.push(SelectItem {
                        expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                            name: "tuple".to_string(),
                            args: vec![
                                RenderExpr::Literal(Literal::String("fixed_path".to_string())),
                                RenderExpr::Literal(Literal::String(
                                    graph_rel.left_connection.clone(),
                                )),
                                RenderExpr::Literal(Literal::String(
                                    graph_rel.right_connection.clone(),
                                )),
                                RenderExpr::Literal(Literal::String(graph_rel.alias.clone())),
                            ],
                        }),
                        col_alias: Some(ColumnAlias(path_var.clone())),
                    });

                    // CRITICAL FIX: For path queries, also include node and relationship properties
                    // Neo4j Browser (and Bolt protocol) expects full properties in path objects
                    // This enables convert_path_branches_to_json() to build _start_properties, _end_properties
                    log::warn!(
                        "🔍 Path query: expanding properties for left='{}', right='{}', rel='{}'",
                        graph_rel.left_connection,
                        graph_rel.right_connection,
                        graph_rel.alias
                    );

                    // Add left node properties with prefixed aliases (e.g., "a_0.user_id")
                    self.add_node_properties_for_path(
                        &graph_rel.left,
                        &graph_rel.left_connection,
                        &mut items,
                    )?;

                    // Add right node properties with prefixed aliases (e.g., "o_0.post_id")
                    self.add_node_properties_for_path(
                        &graph_rel.right,
                        &graph_rel.right_connection,
                        &mut items,
                    )?;

                    // Add relationship properties with prefixed aliases (e.g., "t20.follow_date")
                    self.add_relationship_properties_for_path(
                        &graph_rel.center,
                        &graph_rel.alias,
                        &mut items,
                    )?;
                }

                log::warn!(
                    "🔍 GraphRel.extract_select_items: returning {} items",
                    items.len()
                );

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
                                table_alias.0,
                                plan_ctx.is_some()
                            );

                            // NEW APPROACH: Use TypedVariable for type/source checking
                            if let Some(plan_ctx) = plan_ctx {
                                log::trace!("  🔍 Looking up '{}' in plan_ctx...", table_alias.0);
                                match plan_ctx.lookup_variable(&table_alias.0) {
                                    Some(typed_var) if typed_var.is_entity() => {
                                        log::trace!(
                                            "  ✓ Found ENTITY variable '{}'",
                                            table_alias.0
                                        );
                                        // Entity (Node or Relationship) - expand properties
                                        match &typed_var.source() {
                                            VariableSource::Match => {
                                                // Check if this node is in a multi-type VLP context.
                                                // If so, use JSON columns from the CTE instead of
                                                // expanding individual properties (which differ per type).
                                                if let TypedVariable::Node(_) = typed_var {
                                                    if let Some(gr) = self
                                                        .find_graph_rel_for_alias(&table_alias.0)
                                                    {
                                                        if let Some(ref labels) = gr.labels {
                                                            if labels.len() > 1 {
                                                                log::info!(
                                                                    "🎯 Multi-type VLP node '{}' detected ({} rel types), using JSON columns",
                                                                    table_alias.0, labels.len()
                                                                );
                                                                let position = if gr.left_connection
                                                                    == table_alias.0
                                                                {
                                                                    "start"
                                                                } else {
                                                                    "end"
                                                                };
                                                                // Emit JSON properties column
                                                                select_items.push(SelectItem {
                                                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                                        table_alias: RenderTableAlias("t".to_string()),
                                                                        column: PropertyValue::Column(format!("{}_properties", position)),
                                                                    }),
                                                                    col_alias: Some(ColumnAlias(format!("{}.properties", table_alias.0))),
                                                                });
                                                                // Emit ID column
                                                                select_items.push(SelectItem {
                                                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                                        table_alias: RenderTableAlias("t".to_string()),
                                                                        column: PropertyValue::Column(format!("{}_id", position)),
                                                                    }),
                                                                    col_alias: Some(ColumnAlias(format!("{}.id", table_alias.0))),
                                                                });
                                                                // Emit type column
                                                                select_items.push(SelectItem {
                                                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                                        table_alias: RenderTableAlias("t".to_string()),
                                                                        column: PropertyValue::Column(format!("{}_type", position)),
                                                                    }),
                                                                    col_alias: Some(ColumnAlias(format!("{}.__label__", table_alias.0))),
                                                                });
                                                                continue; // skip expand_base_table_entity
                                                            }
                                                        }
                                                    }
                                                }
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
                                                log::debug!("⚠️ Entity variable '{}' has unexpected source, treating as scalar", table_alias.0);
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
                                        log::trace!(
                                            "  ✓ Found SCALAR variable '{}'",
                                            table_alias.0
                                        );
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
                                        log::trace!("  ✗ Variable '{}' NOT FOUND or not a recognized type in plan_ctx", table_alias.0);
                                        // Unknown variable - check if it's a path by looking for GraphRel
                                        if let Some(graph_rel) =
                                            self.find_graph_rel_for_path(&table_alias.0)
                                        {
                                            log::info!(
                                                "🔍 Found unregistered path variable '{}' in GraphRel, expanding with actual aliases",
                                                table_alias.0
                                            );
                                            // Create a minimal TypedVariable for path expansion
                                            // The expand_path_variable will use find_graph_rel_for_path again to get aliases
                                            use crate::query_planner::typed_variable::{
                                                PathVariable, TypedVariable, VariableSource,
                                            };
                                            let path_var = TypedVariable::Path(PathVariable {
                                                source: VariableSource::Match,
                                                start_node: Some(graph_rel.left_connection.clone()),
                                                end_node: Some(graph_rel.right_connection.clone()),
                                                relationship: Some(graph_rel.alias.clone()),
                                                length_bounds: graph_rel
                                                    .variable_length
                                                    .as_ref()
                                                    .map(|v| (v.min_hops, v.max_hops)),
                                                is_shortest_path: graph_rel
                                                    .shortest_path_mode
                                                    .is_some(),
                                            });
                                            self.expand_path_variable(
                                                &table_alias.0,
                                                &path_var,
                                                &mut select_items,
                                                Some(plan_ctx),
                                            );
                                        } else {
                                            // Really unknown - fallback to old logic
                                            log::debug!("⚠️ Variable '{}' not found in TypedVariable registry or GraphRel, using fallback logic", table_alias.0);
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
                                if let Some(graph_rel) =
                                    self.find_graph_rel_for_path(&table_alias.0)
                                {
                                    log::info!(
                                        "🔍 Found unregistered path variable '{}' in GraphRel (no plan_ctx), expanding with actual aliases",
                                        table_alias.0
                                    );
                                    // Create a minimal TypedVariable for path expansion
                                    use crate::query_planner::typed_variable::{
                                        PathVariable, TypedVariable, VariableSource,
                                    };
                                    let path_var = TypedVariable::Path(PathVariable {
                                        source: VariableSource::Match,
                                        start_node: Some(graph_rel.left_connection.clone()),
                                        end_node: Some(graph_rel.right_connection.clone()),
                                        relationship: Some(graph_rel.alias.clone()),
                                        length_bounds: graph_rel
                                            .variable_length
                                            .as_ref()
                                            .map(|v| (v.min_hops, v.max_hops)),
                                        is_shortest_path: graph_rel.shortest_path_mode.is_some(),
                                    });
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

                            // Multi-type VLP nodes: use JSON columns instead of individual properties
                            if let Some(gr) = self.find_graph_rel_for_alias(&prop.table_alias.0) {
                                if let Some(ref labels) = gr.labels {
                                    if labels.len() > 1 {
                                        log::info!(
                                            "🎯 Multi-type VLP node '{}' detected ({} rel types), using JSON columns for wildcard",
                                            prop.table_alias.0, labels.len()
                                        );
                                        let position = if gr.left_connection == prop.table_alias.0 {
                                            "start"
                                        } else {
                                            "end"
                                        };
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(format!(
                                                        "{}_properties",
                                                        position
                                                    )),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.properties",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(format!(
                                                        "{}_id",
                                                        position
                                                    )),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.id",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(format!(
                                                        "{}_type",
                                                        position
                                                    )),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.__label__",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        continue;
                                    }
                                }
                            }

                            // Multi-type VLP relationship: use CTE relationship columns
                            // Only applies to VLP multi-type path (labels > 1 + variable_length)
                            // NOT to pattern_combinations path which uses regular table JOINs
                            if let Some(gr) = self.find_graph_rel_by_rel_alias(&prop.table_alias.0)
                            {
                                if let Some(ref labels) = gr.labels {
                                    if labels.len() > 1
                                        && gr.variable_length.is_some()
                                        && gr.pattern_combinations.is_none()
                                    {
                                        log::info!(
                                            "🎯 Multi-type VLP relationship '{}' detected ({} types), using CTE columns",
                                            prop.table_alias.0, labels.len()
                                        );
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "path_relationships".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.type",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "rel_properties".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.properties",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "start_id".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.start_id",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: RenderTableAlias("t".to_string()),
                                                    column: PropertyValue::Column(
                                                        "end_id".to_string(),
                                                    ),
                                                },
                                            ),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.end_id",
                                                prop.table_alias.0
                                            ))),
                                        });
                                        continue;
                                    }
                                }
                            }

                            // CTE reference wildcard: alias was renamed to CTE name
                            // (e.g., "a" → "with_a_cte_0" by rewrite_logical_expr_cte_refs)
                            // Reverse-map to original alias, then expand using CTE columns.
                            if let Some(original_alias) = self.find_cte_original_alias(&prop.table_alias.0) {
                                let cte_props = crate::server::query_context::get_all_cte_properties(&original_alias);
                                if !cte_props.is_empty() {
                                    log::info!(
                                        "🔍 CTE wildcard expansion: '{}' → original alias '{}' with {} properties",
                                        prop.table_alias.0, original_alias, cte_props.len()
                                    );
                                    for (prop_name, cte_column) in &cte_props {
                                        select_items.push(SelectItem {
                                            expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: RenderTableAlias(original_alias.clone()),
                                                column: PropertyValue::Column(cte_column.clone()),
                                            }),
                                            col_alias: Some(ColumnAlias(format!(
                                                "{}.{}",
                                                original_alias, prop_name
                                            ))),
                                        });
                                    }
                                    continue;
                                }
                            }

                            // Check if this is a denormalized edge alias mapping
                            // First try plan_ctx (populated during planning), then fall back to
                            // logical plan inspection, then QUERY_CONTEXT
                            let mapped_alias = if let Some(ctx) = plan_ctx {
                                if let Some((edge_alias, _is_from, _label, _type)) =
                                    ctx.get_denormalized_alias_info(&prop.table_alias.0)
                                {
                                    edge_alias.clone()
                                } else {
                                    // plan_ctx doesn't have it (e.g., Union branches use cloned ctx)
                                    // Fall back to inspecting the logical plan's GraphRel
                                    self.find_denormalized_edge_alias(&prop.table_alias.0)
                                        .or_else(|| {
                                            crate::render_plan::get_denormalized_alias_mapping(
                                                &prop.table_alias.0,
                                            )
                                        })
                                        .unwrap_or_else(|| prop.table_alias.0.clone())
                                }
                            } else {
                                self.find_denormalized_edge_alias(&prop.table_alias.0)
                                    .or_else(|| {
                                        crate::render_plan::get_denormalized_alias_mapping(
                                            &prop.table_alias.0,
                                        )
                                    })
                                    .unwrap_or_else(|| prop.table_alias.0.clone())
                            };

                            if mapped_alias != prop.table_alias.0 {
                                log::info!(
                                    "🔍 Denormalized alias mapping found for wildcard: '{}' → '{}'",
                                    prop.table_alias.0,
                                    mapped_alias
                                );
                            }

                            // For denormalized nodes, look up properties using original node alias
                            // but render with the edge table alias
                            let property_lookup_alias = if mapped_alias != prop.table_alias.0 {
                                &prop.table_alias.0
                            } else {
                                &mapped_alias
                            };

                            let (properties, table_alias_for_render) =
                                match self.get_properties_with_table_alias(property_lookup_alias) {
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
                                log::debug!("⚠️ CteEntityRef '{}' has no columns - falling back to TableAlias", cte_ref.alias);
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
                            // AND map the property name to the actual column name
                            if let Ok((properties, table_alias_override)) =
                                self.get_properties_with_table_alias(cypher_alias)
                            {
                                // Look up the column name for this property
                                let mapped_column = properties
                                    .iter()
                                    .find(|(prop_name, _)| prop_name == col_name)
                                    .map(|(_, col)| col.clone());

                                if let Some(actual_column) = mapped_column {
                                    let table_alias_to_use = table_alias_override
                                        .unwrap_or_else(|| cypher_alias.to_string());
                                    log::warn!(
                                        "🔍 Mapped property '{}.{}' to column '{}.{}'",
                                        cypher_alias,
                                        col_name,
                                        table_alias_to_use,
                                        actual_column
                                    );
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(table_alias_to_use),
                                            column: PropertyValue::Column(actual_column),
                                        }),
                                        col_alias: item
                                            .col_alias
                                            .as_ref()
                                            .map(|ca| ColumnAlias(ca.0.clone())),
                                    });
                                    continue;
                                } else if table_alias_override.is_some() {
                                    // Has actual_table_alias but property not found in mapping
                                    // Use original column name with the overridden alias
                                    let actual_table_alias = table_alias_override.unwrap();
                                    log::warn!(
                                        "🔍 Using actual table alias '{}' for {}.{} (property not in mapping)",
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

                        // Case 5: id() function - transform to ID column access
                        // The id() function needs special handling because:
                        // 1. We preserve ScalarFnCall("id") in LogicalPlan for metadata extraction
                        // 2. But for SQL we need the actual ID column value to compute encoded ID
                        LogicalExpr::ScalarFnCall(fn_call)
                            if fn_call.name.eq_ignore_ascii_case("id")
                                && fn_call.args.len() == 1 =>
                        {
                            if let LogicalExpr::TableAlias(ref alias) = fn_call.args[0] {
                                log::info!(
                                    "🔍 SelectBuilder: id({}) - transforming to ID column access",
                                    alias.0
                                );

                                // Get schema from plan_ctx to find the ID column
                                if let Some(ctx) = plan_ctx {
                                    if let Some(typed_var) = ctx.lookup_variable(&alias.0) {
                                        let id_column = match typed_var {
                                            TypedVariable::Node(node_var) => {
                                                if let Some(label) = node_var.labels.first() {
                                                    ctx.schema().node_schema(label).ok().and_then(
                                                        |ns| {
                                                            ns.node_id
                                                                .columns()
                                                                .first()
                                                                .map(|s| s.to_string())
                                                        },
                                                    )
                                                } else {
                                                    None
                                                }
                                            }
                                            TypedVariable::Relationship(rel_var) => {
                                                if let Some(rel_type) = rel_var.rel_types.first() {
                                                    ctx.schema()
                                                        .get_rel_schema(rel_type)
                                                        .ok()
                                                        .and_then(|rs| {
                                                            if let Some(ref edge_id) = rs.edge_id {
                                                                edge_id
                                                                    .columns()
                                                                    .first()
                                                                    .map(|s| s.to_string())
                                                            } else {
                                                                Some(rs.from_id.clone())
                                                            }
                                                        })
                                                } else {
                                                    None
                                                }
                                            }
                                            _ => None,
                                        };

                                        if let Some(id_col) = id_column {
                                            log::info!(
                                                "🔍 SelectBuilder: id({}) -> {}.{}",
                                                alias.0,
                                                alias.0,
                                                id_col
                                            );
                                            select_items.push(SelectItem {
                                                expression: RenderExpr::PropertyAccessExp(
                                                    PropertyAccess {
                                                        table_alias: RenderTableAlias(
                                                            alias.0.clone(),
                                                        ),
                                                        column: PropertyValue::Column(id_col),
                                                    },
                                                ),
                                                col_alias: item
                                                    .col_alias
                                                    .as_ref()
                                                    .map(|ca| ColumnAlias(ca.0.clone())),
                                            });
                                            continue;
                                        }
                                    }
                                }

                                // Fallback: couldn't resolve ID column, pass through as-is
                                log::warn!("🔍 SelectBuilder: id({}) - couldn't resolve ID column, passing through", alias.0);
                            }

                            // Fallback for non-alias argument or failed resolution
                            select_items.push(SelectItem {
                                expression: item.expression.clone().try_into()?,
                                col_alias: item
                                    .col_alias
                                    .as_ref()
                                    .map(|ca| ca.clone().try_into())
                                    .transpose()?,
                            });
                        }

                        // Case 6: Other regular expressions (function call, literals, etc.)
                        _ => {
                            log::warn!(
                                "🔍 SelectBuilder Case 6 (Other): Expression type = {:?}",
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
                log::warn!(
                    "🔍 GraphJoins.extract_select_items: input type={:?}",
                    std::mem::discriminant(graph_joins.input.as_ref())
                );
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
                log::trace!("🔍 CartesianProduct.extract_select_items START");
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
                log::trace!("🔍 WithClause.extract_select_items: calling extract on input");
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
    /// Check if this plan contains a GraphRel with pattern_combinations for the given alias
    fn has_pattern_combinations_for_alias(&self, alias: &str) -> bool {
        match self {
            LogicalPlan::GraphRel(gr) => {
                if gr.alias == alias && gr.pattern_combinations.is_some() {
                    return true;
                }
                // Check recursively
                gr.left.has_pattern_combinations_for_alias(alias)
                    || gr.center.has_pattern_combinations_for_alias(alias)
                    || gr.right.has_pattern_combinations_for_alias(alias)
            }
            LogicalPlan::GraphNode(gn) => gn.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Projection(p) => p.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Filter(f) => f.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::GroupBy(g) => g.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::OrderBy(o) => o.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Limit(l) => l.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::Skip(s) => s.input.has_pattern_combinations_for_alias(alias),
            LogicalPlan::GraphJoins(gj) => gj.input.has_pattern_combinations_for_alias(alias),
            _ => false,
        }
    }

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
        let labels = match typed_var {
            TypedVariable::Node(node) => &node.labels,
            TypedVariable::Relationship(rel) => &rel.rel_types,
            _ => return, // Should not happen
        };

        // 🔧 FIX: Check if this is a multi-type relationship (multiple labels) that uses a CTE
        // Multi-type relationships generate a vlp_multi_type_{start}_{end} CTE with columns:
        // - path_relationships: array of relationship types
        // - rel_properties: array of relationship property JSON objects
        // - end_type, end_id, start_id, end_properties, hop_count
        //
        // IMPORTANT: Only use CTE columns when the current plan branch actually uses a CTE.
        // PlanCtx may list multiple rel_types for 'r' globally, but pattern_combinations
        // splits the query into single-type UNION branches that use regular table JOINs.
        // We verify by checking the GraphRel on the plan: it must have variable_length
        // (VLP CTE) or pattern_combinations (pattern_union CTE).
        if let TypedVariable::Relationship(_) = typed_var {
            let graph_rel = self.find_graph_rel_by_rel_alias(alias);
            let uses_cte = graph_rel.is_some_and(|gr| {
                gr.variable_length.is_some() || gr.pattern_combinations.is_some()
            });
            if labels.len() > 1 && uses_cte {
                // This is a multi-type relationship - properties come from CTE, not base table
                log::info!(
                    "🎯 Multi-type relationship '{}' detected ({} types), using CTE columns",
                    alias,
                    labels.len()
                );

                // === PATTERNRESOLVER 2.0: Determine CTE alias ===
                // For pattern_combinations, CTE alias is the relationship alias (e.g., "r")
                // For regular multi-type ([:TYPE1|TYPE2]), CTE alias is "t" (VLP_CTE_FROM_ALIAS)
                let has_pattern_combinations = self.has_pattern_combinations_for_alias(alias);
                let cte_alias = if has_pattern_combinations {
                    log::info!(
                        "🔀 PatternResolver 2.0: Using relationship alias '{}' for CTE reference",
                        alias
                    );
                    alias // Pattern combinations use relationship alias
                } else {
                    "t" // Regular multi-type uses VLP_CTE_FROM_ALIAS
                };

                // Add all CTE columns needed to reconstruct the relationship
                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(cte_alias.to_string()),
                        column: PropertyValue::Column("path_relationships".to_string()),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.type", alias))),
                });

                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(cte_alias.to_string()),
                        column: PropertyValue::Column("rel_properties".to_string()),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.properties", alias))),
                });

                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(cte_alias.to_string()),
                        column: PropertyValue::Column(VLP_START_ID_COLUMN.to_string()),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.start_id", alias))),
                });

                select_items.push(SelectItem {
                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: RenderTableAlias(cte_alias.to_string()),
                        column: PropertyValue::Column(VLP_END_ID_COLUMN.to_string()),
                    }),
                    col_alias: Some(ColumnAlias(format!("{}.end_id", alias))),
                });

                log::info!(
                    "✅ Expanded multi-type relationship '{}' to {} CTE columns",
                    alias,
                    select_items.len()
                );
                return;
            }
        }

        // CRITICAL: Check if this alias is a FK-edge (denormalized on another table)
        // For FK-edge patterns like (u)-[r:AUTHORED]->(po), relationship r is stored ON po table
        // We need to select columns from po table but alias them as r.*
        let (actual_table_alias, is_fk_edge) = if let Some(ctx) = plan_ctx {
            if let Some((edge_alias, _is_from, _label, _type)) =
                ctx.get_denormalized_alias_info(alias)
            {
                log::info!(
                    "🔑 FK-edge detected: '{}' is denormalized on '{}'",
                    alias,
                    edge_alias
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
                if is_fk_edge {
                    "FK-edge"
                } else {
                    "Denormalized"
                },
                alias,
                actual_table_alias
            );
        }

        // For FK-edge (denormalized nodes), first try the original alias to get projected_columns
        // Then fall back to actual_table_alias for relationship tables
        let lookup_alias = if is_fk_edge {
            alias
        } else {
            &actual_table_alias
        };

        match self.get_properties_with_table_alias(lookup_alias) {
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
                    "✅ Expanded base table '{}' (lookup: '{}', table: '{}') to {} properties",
                    alias,
                    lookup_alias,
                    actual_table_alias,
                    prop_count
                );
            }
            _ => {
                log::debug!("⚠️ No properties found for base table entity '{}'", alias);
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

        // 🔧 FIX: Check if this is a multi-type CTE (uses JSON columns)
        if cte_name.starts_with("vlp_multi_type_") {
            log::info!(
                "🎯 Multi-type CTE detected: '{}', using JSON columns",
                cte_name
            );

            // Multi-type CTEs use JSON columns instead of individual property columns:
            // - start_properties (JSON) for start node
            // - end_properties (JSON) for end node
            // We need to select the appropriate JSON column

            let from_alias = self.compute_from_alias_from_cte_name(cte_name);

            // Determine which JSON column to use based on alias position in CTE name
            // CTE name format: vlp_multi_type_{start}_{end}
            let json_column = if cte_name.contains(&format!("_{}_", alias))
                || cte_name.ends_with(&format!("_{}", alias))
            {
                // This is the end node
                "end_properties"
            } else {
                // This is the start node
                "start_properties"
            };

            log::info!("🔍 Node '{}' maps to JSON column '{}'", alias, json_column);

            // Select the JSON column and also the ID column
            select_items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(from_alias.clone()),
                    column: PropertyValue::Column(json_column.to_string()),
                }),
                col_alias: Some(ColumnAlias(format!("{}.properties", alias))),
            });

            // Also select the ID
            let id_column = if json_column == "end_properties" {
                "end_id"
            } else {
                "start_id"
            };
            select_items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(from_alias),
                    column: PropertyValue::Column(id_column.to_string()),
                }),
                col_alias: Some(ColumnAlias(format!("{}.id", alias))),
            });

            log::info!("✅ Expanded multi-type node '{}' to JSON column", alias);
            return;
        }

        // Parse CTE name to get aliases and compute FROM alias
        let from_alias = self.compute_from_alias_from_cte_name(cte_name);
        log::trace!("🔍 CTE '{}' → FROM alias '{}'", cte_name, from_alias);

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
        // Use CTE property mappings from query context (populated by cte_manager)
        // to get the actual column names rather than constructing them manually.
        let prop_count = properties.len();
        for (prop_name, _db_column) in properties {
            let cte_column =
                crate::server::query_context::get_cte_property_mapping(&from_alias, &prop_name)
                    .unwrap_or_else(|| format!("{}_{}", alias, prop_name));
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
        // For WITH CTEs: "with_a_allNeighboursCount_cte_0" → "a_allNeighboursCount"
        if cte_name.starts_with("with_") {
            if let Some(base) = cte_name.strip_prefix("with_") {
                // Strip _cte_N suffix
                if let Some(idx) = base.rfind("_cte_") {
                    return base[..idx].to_string();
                }
            }
        }
        cte_name.to_string()
    }

    /// Find GraphRel with matching path_variable in the plan tree.
    /// This is used to get the actual connection aliases used in UNION branches.
    fn find_graph_rel_for_path(
        &self,
        path_name: &str,
    ) -> Option<&crate::query_planner::logical_plan::GraphRel> {
        use crate::query_planner::logical_plan::LogicalPlan;
        match self {
            LogicalPlan::GraphRel(gr) if gr.path_variable.as_deref() == Some(path_name) => Some(gr),
            LogicalPlan::GraphRel(gr) => {
                // Check children
                gr.left
                    .find_graph_rel_for_path(path_name)
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
                u.inputs
                    .first()
                    .and_then(|branch| branch.find_graph_rel_for_path(path_name))
            }
            _ => None,
        }
    }

    /// Find GraphRel where the given alias is a left or right connection.
    /// Used to detect multi-type VLP context for whole-node expansion.
    fn find_graph_rel_for_alias(
        &self,
        alias: &str,
    ) -> Option<&crate::query_planner::logical_plan::GraphRel> {
        use crate::query_planner::logical_plan::LogicalPlan;
        match self {
            LogicalPlan::GraphRel(gr)
                if gr.left_connection == alias || gr.right_connection == alias =>
            {
                Some(gr)
            }
            LogicalPlan::GraphRel(gr) => gr
                .left
                .find_graph_rel_for_alias(alias)
                .or_else(|| gr.right.find_graph_rel_for_alias(alias)),
            LogicalPlan::GraphJoins(gj) => gj.input.find_graph_rel_for_alias(alias),
            LogicalPlan::GraphNode(gn) => gn.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Projection(p) => p.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Filter(f) => f.input.find_graph_rel_for_alias(alias),
            LogicalPlan::GroupBy(gb) => gb.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Limit(l) => l.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Skip(s) => s.input.find_graph_rel_for_alias(alias),
            LogicalPlan::OrderBy(o) => o.input.find_graph_rel_for_alias(alias),
            LogicalPlan::Union(u) => u
                .inputs
                .first()
                .and_then(|branch| branch.find_graph_rel_for_alias(alias)),
            _ => None,
        }
    }

    /// Find the edge alias for a denormalized node.
    /// If the given alias is a node in a GraphRel with a denormalized center ViewScan
    /// (has from_node_properties or to_node_properties), return the relationship alias.
    fn find_denormalized_edge_alias(&self, node_alias: &str) -> Option<String> {
        let gr = self.find_graph_rel_for_alias(node_alias)?;
        if let LogicalPlan::ViewScan(scan) = gr.center.as_ref() {
            if scan.from_node_properties.is_some() || scan.to_node_properties.is_some() {
                return Some(gr.alias.clone());
            }
        }
        None
    }

    /// Reverse-map a CTE name to the original alias.
    /// Searches GraphJoins.cte_references for an entry where value == cte_name,
    /// returning the key (original alias).
    fn find_cte_original_alias(&self, cte_name: &str) -> Option<String> {
        match self {
            LogicalPlan::GraphJoins(gj) => {
                for (alias, name) in &gj.cte_references {
                    if name == cte_name {
                        return Some(alias.clone());
                    }
                }
                gj.input.find_cte_original_alias(cte_name)
            }
            LogicalPlan::Union(u) => u
                .inputs
                .iter()
                .find_map(|branch| branch.find_cte_original_alias(cte_name)),
            LogicalPlan::Projection(p) => p.input.find_cte_original_alias(cte_name),
            LogicalPlan::Filter(f) => f.input.find_cte_original_alias(cte_name),
            LogicalPlan::GraphRel(gr) => {
                for (alias, name) in &gr.cte_references {
                    if name == cte_name {
                        return Some(alias.clone());
                    }
                }
                gr.left
                    .find_cte_original_alias(cte_name)
                    .or_else(|| gr.right.find_cte_original_alias(cte_name))
            }
            _ => None,
        }
    }

    /// Find a GraphRel whose own relationship alias matches the given alias.
    /// Unlike `find_graph_rel_for_alias` which matches node connections (left/right),
    /// this matches the GraphRel's own `alias` field (the relationship variable).
    fn find_graph_rel_by_rel_alias(
        &self,
        alias: &str,
    ) -> Option<&crate::query_planner::logical_plan::GraphRel> {
        use crate::query_planner::logical_plan::LogicalPlan;
        match self {
            LogicalPlan::GraphRel(gr) if gr.alias == alias => Some(gr),
            LogicalPlan::GraphRel(gr) => gr
                .left
                .find_graph_rel_by_rel_alias(alias)
                .or_else(|| gr.right.find_graph_rel_by_rel_alias(alias)),
            LogicalPlan::GraphJoins(gj) => gj.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::GraphNode(gn) => gn.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Projection(p) => p.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Filter(f) => f.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::GroupBy(gb) => gb.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Limit(l) => l.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Skip(s) => s.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::OrderBy(o) => o.input.find_graph_rel_by_rel_alias(alias),
            LogicalPlan::Union(u) => u
                .inputs
                .first()
                .and_then(|branch| branch.find_graph_rel_by_rel_alias(alias)),
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
        log::warn!(
            "🔍 expand_path_variable ENTRY: path='{}', has_plan_ctx={}",
            path_alias,
            plan_ctx.is_some()
        );

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
                path_alias,
                cte_alias
            );

            // 🔧 FIX: Check if this is a multi-type VLP (doesn't have path_nodes/path_edges)
            // Multi-type CTEs use different columns: start_properties, end_properties, rel_properties
            // Detection: check if the GraphRel has multiple relationship types (implicit *1 multi-type)
            let is_multi_type = self
                .find_graph_rel_for_path(path_alias)
                .map(|gr| {
                    gr.labels.as_ref().is_some_and(|l| l.len() > 1)
                        || gr.pattern_combinations.is_some()
                })
                .unwrap_or(false);

            if is_multi_type {
                log::info!("🎯 Multi-type VLP path detected for '{}'", path_alias);

                // For multi-type paths, we construct the path from individual components
                // tuple(start_properties, end_properties, rel_properties, hop_count)
                select_items.push(SelectItem {
                    expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: "tuple".to_string(),
                        args: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("start_properties".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("end_properties".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("rel_properties".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("path_relationships".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("start_id".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("end_id".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("hop_count".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("start_type".to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: RenderTableAlias(cte_alias.to_string()),
                                column: PropertyValue::Column("end_type".to_string()),
                            }),
                        ],
                    }),
                    col_alias: Some(ColumnAlias(path_alias.to_string())),
                });
                return;
            }

            // Standard VLP with path_nodes and path_edges
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
            let graph_rel_ref = self.find_graph_rel_for_path(path_alias);
            log::info!(
                "🔍 Fixed-hop path '{}': graph_rel_ref found={}, has_pattern_combinations={}",
                path_alias,
                graph_rel_ref.is_some(),
                graph_rel_ref
                    .as_ref()
                    .and_then(|g| g.pattern_combinations.as_ref())
                    .is_some()
            );
            let (start_alias, end_alias, rel_alias) = if let Some(graph_rel) = &graph_rel_ref {
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
                let start = path_var
                    .start_node
                    .as_deref()
                    .unwrap_or("_start")
                    .to_string();
                let end = path_var.end_node.as_deref().unwrap_or("_end").to_string();
                let rel = path_var
                    .relationship
                    .as_deref()
                    .unwrap_or("_rel")
                    .to_string();
                log::info!(
                    "🔍 Using registered aliases for path '{}': start={}, end={}, rel={}",
                    path_alias,
                    start,
                    end,
                    rel
                );
                (start, end, rel)
            };

            // 🔥 PatternResolver 2.0: Check if using pattern_union CTE
            // PatternResolver 2.0 CTEs have JSON property columns
            // We need to expand these as individual SELECT columns for result transformer
            if let Some(graph_rel) = &graph_rel_ref {
                if graph_rel.pattern_combinations.is_some() {
                    log::info!("🔀 PatternResolver 2.0 path detected: expanding JSON columns for result transformer");

                    // Use CTE alias (pattern_union_{rel_alias})
                    let cte_alias = &rel_alias;

                    // Expand JSON property columns individually (for result transformer to parse)
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("start_properties".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("_start_properties".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("end_properties".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("_end_properties".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("rel_properties".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("_rel_properties".to_string())),
                    });

                    // Extract relationship type from array (first element)
                    select_items.push(SelectItem {
                        expression: RenderExpr::ScalarFnCall(ScalarFnCall {
                            name: "arrayElement".to_string(),
                            args: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: RenderTableAlias(cte_alias.clone()),
                                    column: PropertyValue::Column("path_relationships".to_string()),
                                }),
                                RenderExpr::Literal(Literal::Integer(1)), // ClickHouse arrays are 1-indexed
                            ],
                        }),
                        col_alias: Some(ColumnAlias("__rel_type__".to_string())),
                    });

                    // Add start_id and end_id for element_id construction
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("start_id".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("__start_id__".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: RenderTableAlias(cte_alias.clone()),
                            column: PropertyValue::Column("end_id".to_string()),
                        }),
                        col_alias: Some(ColumnAlias("__end_id__".to_string())),
                    });

                    // Add stub __start_label__ and __end_label__ (will be inferred from properties)
                    select_items.push(SelectItem {
                        expression: RenderExpr::Literal(Literal::String("Node".to_string())),
                        col_alias: Some(ColumnAlias("__start_label__".to_string())),
                    });
                    select_items.push(SelectItem {
                        expression: RenderExpr::Literal(Literal::String("Node".to_string())),
                        col_alias: Some(ColumnAlias("__end_label__".to_string())),
                    });

                    return; // Done - skip the rest of fixed-path expansion
                }
            }

            // Check if relationship is denormalized
            let is_rel_denormalized = if let Some(graph_rel) = &graph_rel_ref {
                log::info!(
                    "🔍 Checking if relationship '{}' is denormalized, center type: {:?}",
                    rel_alias,
                    std::mem::discriminant(graph_rel.center.as_ref())
                );
                if let crate::query_planner::logical_plan::LogicalPlan::ViewScan(vs) =
                    graph_rel.center.as_ref()
                {
                    log::info!(
                        "🔍 Relationship '{}' center IS ViewScan, table={}, is_denormalized={}",
                        rel_alias,
                        vs.source_table,
                        vs.is_denormalized
                    );
                    vs.is_denormalized
                } else {
                    log::trace!("🔍 Relationship '{}' center is NOT ViewScan", rel_alias);
                    false
                }
            } else {
                log::warn!(
                    "🔍 No GraphRel found for relationship '{}', assuming not denormalized",
                    rel_alias
                );
                false
            };

            log::info!(
                "🔍 Expanding fixed-hop path variable '{}': start={}, end={}, rel={}",
                path_alias,
                start_alias,
                end_alias,
                rel_alias
            );

            // Expand properties for each component if we have plan_ctx
            if let Some(ctx) = plan_ctx {
                log::warn!(
                    "  🔍 Have plan_ctx, looking up path components: start={}, end={}, rel={}",
                    start_alias,
                    end_alias,
                    rel_alias
                );

                // Expand start node properties
                if let Some(typed_var) = ctx.lookup_variable(&start_alias) {
                    let variant_name = if typed_var.is_node() {
                        "Node"
                    } else if typed_var.is_relationship() {
                        "Relationship"
                    } else if typed_var.is_scalar() {
                        "Scalar"
                    } else if typed_var.as_path().is_some() {
                        "Path"
                    } else {
                        "Unknown"
                    };
                    log::debug!(
                        "  ✓ Found start node '{}' in plan_ctx, variant={}, is_entity={}",
                        start_alias,
                        variant_name,
                        typed_var.is_entity()
                    );
                    if typed_var.is_entity() {
                        log::info!("  📦 Expanding start node '{}' properties", start_alias);
                        match typed_var.source() {
                            VariableSource::Match => {
                                self.expand_base_table_entity(
                                    &start_alias,
                                    typed_var,
                                    select_items,
                                    Some(ctx),
                                );
                            }
                            VariableSource::Cte { cte_name } => {
                                self.expand_cte_entity(
                                    &start_alias,
                                    typed_var,
                                    cte_name,
                                    Some(ctx),
                                    select_items,
                                );
                            }
                            _ => {}
                        }
                    }
                } else {
                    log::trace!("  ✗ Start node '{}' not found in plan_ctx", start_alias);
                }

                // Expand end node properties
                if let Some(typed_var) = ctx.lookup_variable(&end_alias) {
                    log::debug!("  ✓ Found end node '{}' in plan_ctx", end_alias);
                    if typed_var.is_entity() {
                        log::info!("  📦 Expanding end node '{}' properties", end_alias);
                        match typed_var.source() {
                            VariableSource::Match => {
                                self.expand_base_table_entity(
                                    &end_alias,
                                    typed_var,
                                    select_items,
                                    Some(ctx),
                                );
                            }
                            VariableSource::Cte { cte_name } => {
                                self.expand_cte_entity(
                                    &end_alias,
                                    typed_var,
                                    cte_name,
                                    Some(ctx),
                                    select_items,
                                );
                            }
                            _ => {}
                        }
                    }
                } else {
                    log::trace!("  ✗ End node '{}' not found in plan_ctx", end_alias);
                }

                // Expand relationship properties (ONLY if not denormalized)
                // Denormalized relationships (e.g., AUTHORED) don't have a separate relationship table
                if !is_rel_denormalized {
                    if let Some(typed_var) = ctx.lookup_variable(&rel_alias) {
                        log::debug!(
                            "  ✓ Found relationship '{}' in plan_ctx, is_entity={}, source={:?}",
                            rel_alias,
                            typed_var.is_entity(),
                            typed_var.source()
                        );
                        if typed_var.is_entity() {
                            log::info!("  📦 Expanding relationship '{}' properties", rel_alias);
                            match typed_var.source() {
                                VariableSource::Match => {
                                    self.expand_base_table_entity(
                                        &rel_alias,
                                        typed_var,
                                        select_items,
                                        Some(ctx),
                                    );
                                }
                                VariableSource::Cte { cte_name } => {
                                    self.expand_cte_entity(
                                        &rel_alias,
                                        typed_var,
                                        cte_name,
                                        Some(ctx),
                                        select_items,
                                    );
                                }
                                _ => {}
                            }
                        }
                    } else {
                        log::trace!("  ✗ Relationship '{}' not found in plan_ctx", rel_alias);
                    }
                } else {
                    // Denormalized relationship: properties come from end node's table
                    // Get relationship properties from schema and select using end_alias table
                    log::info!("  📦 Expanding denormalized relationship '{}' properties using end node table '{}'", rel_alias, end_alias);
                    if let Some(graph_rel) = &graph_rel_ref {
                        // Get relationship type from GraphRel labels
                        if let Some(ref labels) = graph_rel.labels {
                            if let Some(rel_type) = labels.first() {
                                // Get property mappings from schema via plan_ctx
                                let schema = ctx.schema();
                                let rel_props =
                                    schema.get_relationship_properties(&[rel_type.clone()]);
                                log::info!(
                                    "  🔍 Found {} properties for denormalized rel '{}': {:?}",
                                    rel_props.len(),
                                    rel_type,
                                    rel_props
                                );
                                for (prop_name, db_column) in rel_props {
                                    // For denormalized relationships, use the relationship alias
                                    // (which points to the actual table) not the virtual node alias
                                    select_items.push(SelectItem {
                                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: RenderTableAlias(rel_alias.clone()),
                                            column: PropertyValue::Column(db_column),
                                        }),
                                        col_alias: Some(ColumnAlias(format!(
                                            "{}.{}",
                                            rel_alias, prop_name
                                        ))),
                                    });
                                }
                            }
                        }
                    }
                }
            } else {
                log::warn!(
                    "  ✗ NO plan_ctx available for path variable '{}' property expansion!",
                    path_alias
                );
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
            path_alias,
            start_alias,
            end_alias,
            rel_alias
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

    /// Add node properties for path queries with prefixed aliases
    ///
    /// For path queries like `MATCH path = (a)-[r]->(b) RETURN path`,
    /// we need to include node properties with aliases like "a.user_id", "a.full_name"
    /// so that convert_path_branches_to_json() can build _start_properties JSON.
    fn add_node_properties_for_path(
        &self,
        node_plan: &std::sync::Arc<LogicalPlan>,
        alias: &str,
        items: &mut Vec<SelectItem>,
    ) -> Result<(), RenderBuildError> {
        // Get properties from the node plan (ViewScan or denormalized)
        let (properties, actual_table_alias) =
            PropertiesBuilder::get_properties_with_table_alias(node_plan.as_ref(), alias)?;

        log::debug!(
            "🔍 add_node_properties_for_path: node '{}' has {} properties (table: {:?})",
            alias,
            properties.len(),
            actual_table_alias
        );

        // Use actual_table_alias for denormalized properties, fallback to alias
        let table_alias_str = actual_table_alias.unwrap_or_else(|| alias.to_string());

        // Add each property as a SELECT item with prefixed alias
        for (prop_name, col_name) in properties {
            items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(table_alias_str.clone()),
                    column: PropertyValue::Column(col_name),
                }),
                col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
            });
        }

        Ok(())
    }

    /// Add relationship properties for path queries with prefixed aliases
    ///
    /// Similar to node properties, but for the relationship in the path.
    fn add_relationship_properties_for_path(
        &self,
        rel_plan: &std::sync::Arc<LogicalPlan>,
        alias: &str,
        items: &mut Vec<SelectItem>,
    ) -> Result<(), RenderBuildError> {
        // Get properties from the relationship plan (ViewScan or denormalized)
        let (properties, actual_table_alias) =
            PropertiesBuilder::get_properties_with_table_alias(rel_plan.as_ref(), alias)?;

        log::debug!(
            "🔍 add_relationship_properties_for_path: rel '{}' has {} properties (table: {:?})",
            alias,
            properties.len(),
            actual_table_alias
        );

        // Use actual_table_alias for denormalized properties, fallback to alias
        let table_alias_str = actual_table_alias.unwrap_or_else(|| alias.to_string());

        // Add each property as a SELECT item with prefixed alias
        for (prop_name, col_name) in properties {
            items.push(SelectItem {
                expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: RenderTableAlias(table_alias_str.clone()),
                    column: PropertyValue::Column(col_name),
                }),
                col_alias: Some(ColumnAlias(format!("{}.{}", alias, prop_name))),
            });
        }

        Ok(())
    }
}
