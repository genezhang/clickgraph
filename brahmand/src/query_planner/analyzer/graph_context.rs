use crate::{
    graph_catalog::{
        graph_schema::{GraphSchema, NodeSchema, RelationshipSchema},
        GraphViewDefinition,
    },
    query_planner::{
        analyzer::{
            analyzer_pass::AnalyzerResult,
            errors::{AnalyzerError, Pass},
        },
        logical_expr::Direction,
        logical_plan::GraphRel,
        plan_ctx::{PlanCtx, TableCtx},
    },
};

use super::view_resolver::ViewResolver;

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
    pub fn get_node_schema(&self, table_name: &str) -> Option<&'a NodeSchema> {
        if let Some(view_resolver) = &self.view_resolver {
            // First try to get schema from view resolver
            view_resolver.get_node_schema(table_name)
        } else {
            // Fall back to direct schema lookup
            self.schema.get_node_schema(table_name).ok()
        }
    }
    
    /// Get schema for a relationship table
    pub fn get_relationship_schema(&self, table_name: &str) -> Option<&'a RelationshipSchema> {
        if let Some(view_resolver) = &self.view_resolver {
            // First try to get schema from view resolver
            view_resolver.get_relationship_schema(table_name)
        } else {
            // Fall back to direct schema lookup
            self.schema.get_rel_schema(table_name).ok()
        }
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
    view: Option<&'a GraphViewDefinition>,
    pass: Pass,
) -> AnalyzerResult<GraphContext<'a>> {
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

    let left_label = left_ctx
        .get_label_str()
        .map_err(|e| AnalyzerError::PlanCtx {
            pass: pass.clone(),
            source: e,
        })?;
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
    let right_label = right_ctx
        .get_label_str()
        .map_err(|e| AnalyzerError::PlanCtx {
            pass: pass.clone(),
            source: e,
        })?;

    let left_schema =
        graph_schema
            .get_node_schema(&left_label)
            .map_err(|e| AnalyzerError::GraphSchema {
                pass: pass.clone(),
                source: e,
            })?;
    let rel_schema = graph_schema
        .get_rel_schema(&original_rel_label)
        .map_err(|e| AnalyzerError::GraphSchema {
            pass: pass.clone(),
            source: e,
        })?;
    let right_schema = graph_schema
        .get_node_schema(&right_label)
        .map_err(|e| AnalyzerError::GraphSchema { pass, source: e })?;

    let left_node_id_column = left_schema.node_id.column.clone();
    let right_node_id_column = right_schema.node_id.column.clone();

    let left_cte_name = format!("{}_{}", left_label, left_alias);
    let rel_cte_name = rel_schema.table_name.clone();
    let right_cte_name = format!("{}_{}", right_label, right_alias);

    // Create the initial GraphContext with schema
    let mut graph_context = GraphContext {
        left: GraphNodeContext {
            alias: left_alias,
            table_ctx: left_ctx,
            label: left_label,
            schema: left_schema,
            id_column: left_node_id_column,
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
            id_column: right_node_id_column,
            cte_name: right_cte_name,
        },
        schema: graph_schema,
        view_resolver: None,
    };

    // Initialize view resolver if we have a view definition
    let view_resolver = view.and_then(|view_def| {
        Some(ViewResolver::new(
            graph_context.schema,
            view_def
        ))
    });

    // Set the resolver and return
    graph_context.view_resolver = view_resolver;
    Ok(graph_context)
}
