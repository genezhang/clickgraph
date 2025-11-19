use crate::{
    graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema},
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
        self.schema.get_node_schema(table_name).ok()
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

    // Try to get left label, or infer from relationship if anonymous
    let left_label = match left_ctx.get_label_str() {
        Ok(label) => label,
        Err(_) => {
            // Anonymous node - infer from relationship schema
            let rel_schema = graph_schema
                .get_rel_schema(&original_rel_label)
                .map_err(|e| AnalyzerError::GraphSchema {
                    pass: pass.clone(),
                    source: e,
                })?;
            rel_schema.from_node.clone()
        }
    };

    // Try to get right label, or infer from relationship if anonymous
    let right_label = match right_ctx.get_label_str() {
        Ok(label) => label,
        Err(_) => {
            // Anonymous node - infer from relationship schema
            let rel_schema = graph_schema
                .get_rel_schema(&original_rel_label)
                .map_err(|e| AnalyzerError::GraphSchema {
                    pass: pass.clone(),
                    source: e,
                })?;
            rel_schema.to_node.clone()
        }
    };

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

    // Use fully qualified table names from schema for CTEs/JOINs
    let left_cte_name = format!("{}.{}", left_schema.database, left_schema.table_name);
    let rel_cte_name = format!("{}.{}", rel_schema.database, rel_schema.table_name);
    let right_cte_name = format!("{}.{}", right_schema.database, right_schema.table_name);

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

    // Initialize view resolver for schema-only operation
    let view_resolver = Some(ViewResolver::from_schema(graph_schema));

    // Set the resolver and return
    graph_context.view_resolver = view_resolver;
    Ok(graph_context)
}
