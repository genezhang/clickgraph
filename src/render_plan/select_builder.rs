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
use crate::query_planner::logical_expr::{
    LogicalExpr, PropertyAccess as LogicalPropertyAccess, TableAlias,
};
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::errors::RenderBuildError;
use crate::render_plan::render_expr::{
    AggregateFnCall, Column, ColumnAlias, PropertyAccess, RenderExpr, ScalarFnCall,
};
use crate::render_plan::{SelectItem};

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
                projection
                    .items
                    .iter()
                    .map(|item| {
                        Ok(SelectItem {
                            expression: item.expression.clone().try_into()?,
                            col_alias: item.col_alias.as_ref().map(|ca| ca.clone().try_into()).transpose()?,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
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