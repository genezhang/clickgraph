//! Graph context structures for query analysis
//!
//! Some fields are reserved for future schema-aware optimization features.
// Note: PatternContext fields like node_labels and rel_labels are kept for future optimization passes
#![allow(dead_code)]

use crate::{
    graph_catalog::graph_schema::{
        edge_has_node_properties, GraphSchema, NodeSchema, RelationshipSchema,
    },
    query_planner::{
        analyzer::{
            analyzer_pass::AnalyzerResult,
            errors::{AnalyzerError, Pass},
        },
        logical_expr::Direction,
        logical_plan::{GraphRel, LogicalPlan},
        plan_ctx::{PlanCtx, TableCtx},
    },
};

use super::view_resolver::ViewResolver;

/// Strip database prefix from table name for CTE names
/// CTEs cannot have database prefixes in ClickHouse
fn strip_database_prefix(table_name: &str) -> String {
    table_name
        .rsplit_once('.')
        .map(|(_, table)| table.to_string())
        .unwrap_or_else(|| table_name.to_string())
}

#[derive(Debug, Clone)]
pub struct GraphContext<'a> {
    pub left: GraphNodeContext<'a>,
    pub rel: GraphRelContext<'a>,
    pub right: GraphNodeContext<'a>,
    pub view_resolver: Option<ViewResolver<'a>>,
    pub schema: &'a GraphSchema,
}

impl<'a> GraphContext<'a> {
    /// Get schema for a node table
    pub fn node_schema(&self, table_name: &str) -> Option<&'a NodeSchema> {
        self.schema.node_schema(table_name).ok()
    }

    /// Get schema for a relationship table
    pub fn get_relationship_schema(&self, table_name: &str) -> Option<&'a RelationshipSchema> {
        self.schema.get_rel_schema(table_name).ok()
    }
}

#[derive(Debug, Clone)]
pub struct GraphNodeContext<'a> {
    pub alias: &'a String,
    pub table_ctx: &'a TableCtx,
    pub label: String,
    pub schema: &'a NodeSchema,
    pub id_column: String,
    pub cte_name: String,
}

#[derive(Debug, Clone)]
pub struct GraphRelContext<'a> {
    pub alias: &'a String,
    pub table_ctx: &'a TableCtx,
    pub label: String,
    pub schema: &'a RelationshipSchema,
    pub cte_name: String, // id_column: String,
}

pub fn get_graph_context<'a>(
    graph_rel: &'a GraphRel,
    plan_ctx: &'a mut PlanCtx,
    graph_schema: &'a GraphSchema,
    pass: Pass,
) -> AnalyzerResult<GraphContext<'a>> {
    // CRITICAL: Register column mappings for multi-variant CTEs BEFORE borrowing from plan_ctx
    // This must happen before get_node_table_ctx/get_rel_table_ctx which create immutable borrows
    if let Some(labels) = &graph_rel.labels {
        if labels.len() > 1 {
            // Use vlp_multi_type_ prefix for VLP multi-type relationships
            // to match CTE extraction naming in cte_extraction.rs
            let cte_name = if graph_rel.variable_length.is_some() {
                format!(
                    "vlp_multi_type_{}_{}",
                    graph_rel.left_connection, graph_rel.right_connection
                )
            } else {
                format!(
                    "rel_{}_{}",
                    graph_rel.left_connection, graph_rel.right_connection
                )
            };
            log::info!("🔍 graph_context: REL alias '{}' has {} labels - registering columns for multi-variant CTE: '{}'", 
                       graph_rel.alias, labels.len(), cte_name);

            // Collect all column names from all relationship schemas
            let mut column_mappings: Vec<(String, String)> = Vec::new();
            for label in labels.iter() {
                if let Ok(rel_schema) = graph_schema.get_rel_schema(label) {
                    column_mappings
                        .push((rel_schema.from_id.to_string(), "from_node_id".to_string()));
                    column_mappings.push((rel_schema.to_id.to_string(), "to_node_id".to_string()));
                }
            }

            // Register all mappings
            log::info!(
                "🔧 Registering {} column mappings for multi-variant CTE '{}'",
                column_mappings.len(),
                cte_name
            );
            for (schema_col, cte_col) in column_mappings {
                log::debug!("🔧 Mapping {} → {}", schema_col, cte_col);
                plan_ctx.register_cte_column(&cte_name, &schema_col, &cte_col);
            }
        }
    }

    // get required information
    let left_alias = &graph_rel.left_connection;
    let rel_alias = &graph_rel.alias;
    let right_alias = &graph_rel.right_connection;

    let left_ctx = plan_ctx
        .get_node_table_ctx(left_alias)
        .map_err(|e| AnalyzerError::PlanCtx {
            pass: pass.clone(),
            source: e,
        })?;
    let rel_ctx = plan_ctx
        .get_rel_table_ctx(rel_alias)
        .map_err(|e| AnalyzerError::PlanCtx {
            pass: pass.clone(),
            source: e,
        })?;
    let right_ctx =
        plan_ctx
            .get_node_table_ctx(right_alias)
            .map_err(|e| AnalyzerError::PlanCtx {
                pass: pass.clone(),
                source: e,
            })?;

    // FIX: For anonymous nodes, infer labels from relationship schema
    // Get relationship label first to use for inference
    let rel_label = rel_ctx
        .get_label_str()
        .map_err(|e| AnalyzerError::PlanCtx {
            pass: pass.clone(),
            source: e,
        })?;
    let original_rel_label = rel_label
        .replace(format!("_{}", Direction::Incoming).as_str(), "")
        .replace(format!("_{}", Direction::Outgoing).as_str(), "")
        .replace(format!("_{}", Direction::Either).as_str(), "");

    // Get relationship schema for label inference (if needed)
    // First, try to get node labels from context (if available)
    let left_label_hint = left_ctx.get_label_str().ok();
    let right_label_hint = right_ctx.get_label_str().ok();

    let rel_schema_for_inference = graph_schema
        .get_rel_schema_with_nodes(
            &original_rel_label,
            left_label_hint.as_deref(),
            right_label_hint.as_deref(),
        )
        .map_err(|e| AnalyzerError::GraphSchema {
            pass: pass.clone(),
            source: e,
        })?;

    // Try to get left label, or infer from relationship if anonymous
    // IMPORTANT: Must consider direction when inferring labels!
    // For (a)<-[:REL]-(b) with Incoming direction:
    //   - left (a) connects to to_id, so left label is rel.to_node
    //   - right (b) connects to from_id, so right label is rel.from_node
    // For (a)-[:REL]->(b) with Outgoing direction:
    //   - left (a) connects to from_id, so left label is rel.from_node
    //   - right (b) connects to to_id, so right label is rel.to_node
    let left_label = match left_ctx.get_label_str() {
        Ok(label) => label,
        Err(_) => {
            // Anonymous node - infer from relationship schema considering direction
            match graph_rel.direction {
                Direction::Incoming => rel_schema_for_inference.to_node.clone(),
                _ => rel_schema_for_inference.from_node.clone(),
            }
        }
    };

    // Try to get right label, or infer from relationship if anonymous
    let right_label = match right_ctx.get_label_str() {
        Ok(label) => label,
        Err(_) => {
            // Anonymous node - infer from relationship schema considering direction
            match graph_rel.direction {
                Direction::Incoming => rel_schema_for_inference.from_node.clone(),
                _ => rel_schema_for_inference.to_node.clone(),
            }
        }
    };

    // NOTE: For polymorphic $any nodes, this function should not be called.
    // The graph_traversal_planning pass should skip $any nodes and let the normal JOIN path handle them.
    // If we reach here with $any, it's a programming error - but we'll handle it gracefully.

    let left_schema =
        graph_schema
            .node_schema(&left_label)
            .map_err(|e| AnalyzerError::GraphSchema {
                pass: pass.clone(),
                source: e,
            })?;
    let rel_schema = graph_schema
        .get_rel_schema_with_nodes(&original_rel_label, Some(&left_label), Some(&right_label))
        .map_err(|e| AnalyzerError::GraphSchema {
            pass: pass.clone(),
            source: e,
        })?;

    // Handle $any (polymorphic) right node - create a placeholder schema
    // $any means the node type is determined at runtime from the edge's to_label_column
    let right_schema = if right_label == "$any" {
        // Use a placeholder schema for polymorphic nodes
        // We can use the left schema as a template (or create a minimal one)
        // The actual table/columns don't matter since we won't JOIN to this node
        left_schema
    } else {
        graph_schema
            .node_schema(&right_label)
            .map_err(|e| AnalyzerError::GraphSchema {
                pass: pass.clone(),
                source: e,
            })?
    };

    // Use SQL tuple expressions for node IDs (handles both single and composite)
    let left_node_id_sql = left_schema.node_id.sql_tuple(left_alias);
    let right_node_id_sql = right_schema.node_id.sql_tuple(right_alias);

    // Use fully qualified table names from schema for CTEs/JOINs
    // For nodes whose properties are available from the edge table (via from_node_properties/to_node_properties),
    // use the edge table instead of the node's "primary" table.
    // This handles cases where node data is denormalized onto edge tables.
    // Note: CTE names must NOT have database prefix for ClickHouse compatibility

    // CRITICAL: Check if relationship center is wrapped in LogicalCte (for alternate relationships)
    // If so, extract the CTE name instead of using schema table name
    let rel_cte_name = if let LogicalPlan::Cte(cte) = graph_rel.center.as_ref() {
        // Alternate relationship types - center wrapped in CTE by GraphTraversalPlanning
        log::info!(
            "🔍 graph_context: REL alias '{}' uses CTE: '{}' (alternate relationships)",
            rel_alias,
            cte.name
        );
        cte.name.clone()
    } else if let Some(labels) = &graph_rel.labels {
        // Check if this is a multi-variant relationship (multiple labels for same rel type)
        // If so, a UNION CTE should have been created with name: rel_{left_connection}_{right_connection}
        if labels.len() > 1 {
            let cte_name = if graph_rel.variable_length.is_some() {
                format!(
                    "vlp_multi_type_{}_{}",
                    graph_rel.left_connection, graph_rel.right_connection
                )
            } else {
                format!(
                    "rel_{}_{}",
                    graph_rel.left_connection, graph_rel.right_connection
                )
            };
            log::info!("🔍 graph_context: REL alias '{}' has {} labels - using multi-variant CTE: '{}' (mappings registered earlier)",
                       rel_alias, labels.len(), cte_name);
            cte_name
        } else {
            // Single label - use schema table name
            let rel_table_full = format!("{}.{}", rel_schema.database, rel_schema.table_name);
            let base_name = strip_database_prefix(&rel_table_full);
            log::info!(
                "🔍 graph_context: REL alias '{}' uses base table: '{}'",
                rel_alias,
                base_name
            );
            base_name
        }
    } else {
        // No labels specified - use schema table name
        let rel_table_full = format!("{}.{}", rel_schema.database, rel_schema.table_name);
        let base_name = strip_database_prefix(&rel_table_full);
        log::info!(
            "🔍 graph_context: REL alias '{}' uses base table: '{}'",
            rel_alias,
            base_name
        );
        base_name
    };

    // Left node: check if this alias references a CTE (from WITH clause export)
    let left_cte_name = if let Some(cte_name) = left_ctx.get_cte_name() {
        // This alias was exported from a WITH clause - use CTE instead of base table
        log::info!(
            "🔍 graph_context: LEFT alias '{}' has CTE reference: '{}'",
            left_alias,
            cte_name
        );
        cte_name.clone()
    } else if edge_has_node_properties(rel_schema, true) {
        // Edge has from_node_properties - node data is on edge table
        log::info!(
            "🔍 graph_context: LEFT alias '{}' uses edge properties: '{}'",
            left_alias,
            rel_cte_name
        );
        rel_cte_name.clone()
    } else {
        // Base table name - strip database prefix for CTE compatibility
        let base_table_full = format!("{}.{}", left_schema.database, left_schema.table_name);
        let base_table = strip_database_prefix(&base_table_full);
        log::info!(
            "🔍 graph_context: LEFT alias '{}' uses base table: '{}'",
            left_alias,
            base_table
        );
        base_table
    };

    // Right node: check CTE reference first, then edge properties, then base table
    let right_cte_name = if let Some(cte_name) = right_ctx.get_cte_name() {
        // This alias was exported from a WITH clause - use CTE instead of base table
        log::info!(
            "🔍 graph_context: RIGHT alias '{}' has CTE reference: '{}'",
            right_alias,
            cte_name
        );
        cte_name.clone()
    } else if right_label == "$any" {
        // Polymorphic node - doesn't matter, won't be JOINed directly
        // But strip database prefix for consistency
        let base_table_full = format!("{}.{}", right_schema.database, right_schema.table_name);
        let base_table = strip_database_prefix(&base_table_full);
        log::info!(
            "🔍 graph_context: RIGHT alias '{}' is polymorphic, using: '{}'",
            right_alias,
            base_table
        );
        base_table
    } else if edge_has_node_properties(rel_schema, false) {
        // Edge has to_node_properties - node data is on edge table
        log::info!(
            "🔍 graph_context: RIGHT alias '{}' uses edge properties: '{}'",
            right_alias,
            rel_cte_name
        );
        rel_cte_name.clone()
    } else {
        // Base table name - strip database prefix for CTE compatibility
        let base_table_full = format!("{}.{}", right_schema.database, right_schema.table_name);
        let base_table = strip_database_prefix(&base_table_full);
        log::info!(
            "🔍 graph_context: RIGHT alias '{}' uses base table: '{}'",
            right_alias,
            base_table
        );
        base_table
    };

    // Create the initial GraphContext with schema
    let mut graph_context = GraphContext {
        left: GraphNodeContext {
            alias: left_alias,
            table_ctx: left_ctx,
            label: left_label,
            schema: left_schema,
            id_column: left_node_id_sql,
            cte_name: left_cte_name,
        },
        rel: GraphRelContext {
            alias: rel_alias,
            table_ctx: rel_ctx,
            label: rel_label,
            schema: rel_schema,
            cte_name: rel_cte_name,
        },
        right: GraphNodeContext {
            alias: right_alias,
            table_ctx: right_ctx,
            label: right_label,
            schema: right_schema,
            id_column: right_node_id_sql,
            cte_name: right_cte_name,
        },
        schema: graph_schema,
        view_resolver: None,
    };

    // Initialize view resolver for schema-only operation
    let view_resolver = Some(ViewResolver::from_schema(graph_schema));

    // Set the resolver and return
    graph_context.view_resolver = view_resolver;
    Ok(graph_context)
}
