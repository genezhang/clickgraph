use serde::{Deserialize, Serialize};

use super::plan_builder::RenderPlanBuilder;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::render_plan::RenderPlan;

use crate::query_planner::logical_expr::LogicalExpr;

use crate::query_planner::logical_expr::{
    AggregateFnCall as LogicalAggregateFnCall, Column as LogicalColumn,
    ColumnAlias as LogicalColumnAlias, ExistsSubquery as LogicalExistsSubquery,
    InSubquery as LogicalInSubquery, Literal as LogicalLiteral,
    LogicalCase, Operator as LogicalOperator, OperatorApplication as LogicalOperatorApplication,
    PathPattern, Direction, PatternCount as LogicalPatternCount,
    PropertyAccess as LogicalPropertyAccess, ScalarFnCall as LogicalScalarFnCall,
    TableAlias as LogicalTableAlias, ReduceExpr as LogicalReduceExpr,
};
use crate::query_planner::logical_plan::LogicalPlan;

use super::errors::RenderBuildError;

/// Generate SQL for an EXISTS subquery directly from the logical plan
/// This is a simplified approach that generates basic EXISTS SQL
fn generate_exists_sql(exists: &LogicalExistsSubquery) -> Result<String, RenderBuildError> {
    use crate::server::GLOBAL_SCHEMAS;
    
    // Try to extract pattern info from the subplan
    // The subplan is typically a GraphRel representing a relationship pattern
    match exists.subplan.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            // Get the relationship type
            let rel_type = graph_rel.labels.as_ref()
                .and_then(|l| l.first())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string());
            
            // Get the start node alias (the correlated variable)
            let start_alias = &graph_rel.left_connection;
            
            // Try to get schema for relationship lookup
            // GLOBAL_SCHEMAS is OnceCell<RwLock<HashMap<String, GraphSchema>>>
            let schemas_lock = GLOBAL_SCHEMAS.get();
            let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());
            let schema = schemas_guard.as_ref()
                .and_then(|guard| guard.get("default"));
            
            // Look up the relationship table and columns using public accessors
            if let Some(schema) = schema {
                if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                    let table_name = &rel_schema.table_name;
                    let from_col = &rel_schema.from_id; // from_id is the FK column
                    
                    // Get the start node's ID column from its label
                    let start_id_col = if let LogicalPlan::GraphNode(start_node) = graph_rel.left.as_ref() {
                        if let Some(label) = &start_node.label {
                            let node_schema = schema.get_node_schema_opt(label)
                                .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                            node_schema.node_id.column().to_string()
                        } else {
                            // No label - infer from relationship schema
                            let node_type = &rel_schema.from_node;
                            let node_schema = schema.get_node_schema_opt(node_type)
                                .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(node_type.clone()))?;
                            node_schema.node_id.column().to_string()
                        }
                    } else {
                        // Not a GraphNode - error, can't infer
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "EXISTS pattern left side is not a GraphNode".to_string()
                        ));
                    };
                    
                    // Generate the EXISTS SQL
                    // EXISTS (SELECT 1 FROM edge_table WHERE edge_table.from_id = outer.node_id)
                    return Ok(format!(
                        "SELECT 1 FROM {} WHERE {}.{} = {}.{}",
                        table_name,
                        table_name, from_col,
                        start_alias, start_id_col
                    ));
                }
            }
            
            // No schema found - this is an error
            Err(RenderBuildError::InvalidRenderPlan(
                format!("Cannot generate EXISTS pattern: relationship schema '{}' not found. \
                         Please define this relationship in your YAML schema configuration.", rel_type)
            ))
        }
        _ => {
            // For other plan types, this is unsupported
            Err(RenderBuildError::UnsupportedFeature(
                "EXISTS pattern with non-GraphRel subplan".to_string()
            ))
        }
    }
}

/// Generate SQL for a pattern count (size() on patterns)
/// 
/// For `size((n)-[:REL]->())` pattern, generates:
/// ```sql
/// (SELECT COUNT(*) FROM rel_table WHERE rel_table.from_id = n.id)
/// ```
fn generate_pattern_count_sql(pattern: &PathPattern) -> Result<String, RenderBuildError> {
    use crate::server::GLOBAL_SCHEMAS;
    
    match pattern {
        PathPattern::ConnectedPattern(connected_patterns) => {
            if connected_patterns.is_empty() {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Empty connected pattern in size()".to_string()
                ));
            }
            
            // Handle single-hop pattern (most common case)
            let conn = &connected_patterns[0];
            
            // Get the start node alias (the correlated variable)
            let start_alias = conn.start_node.name.as_ref()
                .ok_or_else(|| RenderBuildError::InvalidRenderPlan(
                    "size() pattern requires named start node".to_string()
                ))?;
            
            // Get the end node alias (can be anonymous/None)
            let end_alias = conn.end_node.name.as_ref().map(|s| s.to_string());
            
            // Get relationship type
            let rel_type = conn.relationship.labels.as_ref()
                .and_then(|l| l.first())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string());
            
            // Determine direction
            let is_undirected = matches!(conn.relationship.direction, Direction::Either);
            
            // Try to get schema for relationship lookup
            let schemas_lock = GLOBAL_SCHEMAS.get();
            let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());
            let schema = schemas_guard.as_ref()
                .and_then(|guard| guard.get("default"));
            
            // Look up the relationship table and columns
            if let Some(schema) = schema {
                if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                    let table_name = &rel_schema.table_name;
                    let full_table = if !rel_schema.database.is_empty() {
                        format!("{}.{}", rel_schema.database, table_name)
                    } else {
                        table_name.clone()
                    };
                    let from_col = &rel_schema.from_id;
                    let to_col = &rel_schema.to_id;
                    
                    // Get the start node's ID column
                    // First try the explicit label from the pattern, then fall back to relationship schema
                    let start_id_col = if let Some(label) = &conn.start_node.label {
                        let node_schema = schema.get_node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        node_schema.node_id.column().to_string()
                    } else {
                        // No label in pattern - infer from relationship's from_node
                        let node_type = &rel_schema.from_node;
                        let node_schema = schema.get_node_schema_opt(node_type)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(node_type.clone()))?;
                        node_schema.node_id.column().to_string()
                    };
                    
                    // Get end node's ID column
                    let end_id_col = if let Some(label) = &conn.end_node.label {
                        let node_schema = schema.get_node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        node_schema.node_id.column().to_string()
                    } else {
                        // No label in pattern - infer from relationship's to_node
                        let node_type = &rel_schema.to_node;
                        let node_schema = schema.get_node_schema_opt(node_type)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(node_type.clone()))?;
                        node_schema.node_id.column().to_string()
                    };
                    
                    // Generate COUNT SQL based on end node and direction
                    let count_sql = match (end_alias.as_ref(), is_undirected) {
                        (None, false) => {
                            // Anonymous end node, directed: just count from_id matches
                            match conn.relationship.direction {
                                Direction::Outgoing => format!(
                                    "(SELECT COUNT(*) FROM {} WHERE {}.{} = {}.{})",
                                    full_table, table_name, from_col, start_alias, start_id_col
                                ),
                                Direction::Incoming => format!(
                                    "(SELECT COUNT(*) FROM {} WHERE {}.{} = {}.{})",
                                    full_table, table_name, to_col, start_alias, start_id_col
                                ),
                                _ => format!(
                                    "(SELECT COUNT(*) FROM {} WHERE {}.{} = {}.{} OR {}.{} = {}.{})",
                                    full_table,
                                    table_name, from_col, start_alias, start_id_col,
                                    table_name, to_col, start_alias, start_id_col
                                ),
                            }
                        },
                        (None, true) => {
                            // Anonymous end node, undirected: count both directions
                            format!(
                                "(SELECT COUNT(*) FROM {} WHERE {}.{} = {}.{} OR {}.{} = {}.{})",
                                full_table,
                                table_name, from_col, start_alias, start_id_col,
                                table_name, to_col, start_alias, start_id_col
                            )
                        },
                        (Some(end), true) => {
                            // Named end node, undirected: check both directions
                            format!(
                                "(SELECT COUNT(*) FROM {} WHERE ({}.{} = {}.{} AND {}.{} = {}.{}) OR ({}.{} = {}.{} AND {}.{} = {}.{}))",
                                full_table,
                                table_name, from_col, start_alias, start_id_col,
                                table_name, to_col, end, end_id_col,
                                table_name, from_col, end, end_id_col,
                                table_name, to_col, start_alias, start_id_col
                            )
                        },
                        (Some(end), false) => {
                            // Named end node, directed: check single direction
                            let (fk_from, fk_to, from_id, to_id) = match conn.relationship.direction {
                                Direction::Outgoing => (start_alias.as_str(), end.as_str(), &start_id_col, &end_id_col),
                                Direction::Incoming => (end.as_str(), start_alias.as_str(), &end_id_col, &start_id_col),
                                _ => (start_alias.as_str(), end.as_str(), &start_id_col, &end_id_col),
                            };
                            format!(
                                "(SELECT COUNT(*) FROM {} WHERE {}.{} = {}.{} AND {}.{} = {}.{})",
                                full_table,
                                table_name, from_col, fk_from, from_id,
                                table_name, to_col, fk_to, to_id
                            )
                        },
                    };
                    
                    return Ok(count_sql);
                }
            }
            
            // No schema found - this is an error, not a fallback scenario
            Err(RenderBuildError::InvalidRenderPlan(
                format!("Cannot generate size() pattern count: relationship schema '{}' not found. \
                         Please define this relationship in your YAML schema configuration with proper \
                         from_node, to_node, and ID column mappings.", rel_type)
            ))
        }
        PathPattern::Node(_) => {
            Err(RenderBuildError::InvalidRenderPlan(
                "size() pattern with single node is not supported".to_string()
            ))
        }
        PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
            Err(RenderBuildError::InvalidRenderPlan(
                "size() pattern with shortest path is not supported".to_string()
            ))
        }
    }
}

/// Generate NOT EXISTS SQL for a PathPattern (negative pattern matching / anti-join)
/// 
/// For `NOT (a)-[:REL]-(b)` pattern, generates:
/// ```sql
/// NOT EXISTS (
///     SELECT 1 FROM rel_table 
///     WHERE (rel_table.from_id = a.id AND rel_table.to_id = b.id)
///        OR (rel_table.from_id = b.id AND rel_table.to_id = a.id)  -- for undirected
/// )
/// ```
fn generate_not_exists_from_path_pattern(pattern: &PathPattern) -> Result<String, RenderBuildError> {
    use crate::server::GLOBAL_SCHEMAS;
    
    match pattern {
        PathPattern::ConnectedPattern(connected_patterns) => {
            if connected_patterns.is_empty() {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Empty connected pattern in NOT expression".to_string()
                ));
            }
            
            // Handle single-hop pattern (most common case for anti-join)
            let conn = &connected_patterns[0];
            
            // Get the start and end node aliases (end node can be anonymous)
            let start_alias = conn.start_node.name.as_ref()
                .ok_or_else(|| RenderBuildError::InvalidRenderPlan(
                    "NOT pattern requires named start node".to_string()
                ))?;
            // End alias is optional - if None, we only check the from_id
            let end_alias = conn.end_node.name.as_ref();
            
            // Get the relationship type
            let rel_type = conn.relationship.labels.as_ref()
                .and_then(|l| l.first())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string());
            
            // Get direction
            let is_undirected = conn.relationship.direction == Direction::Either;
            
            // Try to get schema for relationship lookup
            let schemas_lock = GLOBAL_SCHEMAS.get();
            let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());
            let schema = schemas_guard.as_ref()
                .and_then(|guard| guard.get("default"));
            
            // Look up the relationship table and columns
            if let Some(schema) = schema {
                if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                    let db_name = &rel_schema.database;
                    let table_name = &rel_schema.table_name;
                    let full_table = format!("{}.{}", db_name, table_name);
                    let from_col = &rel_schema.from_id;
                    let to_col = &rel_schema.to_id;
                    
                    // Get the node ID columns from their labels or infer from relationship schema
                    let start_id_col = if let Some(label) = &conn.start_node.label {
                        let node_schema = schema.get_node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        node_schema.node_id.column().to_string()
                    } else {
                        // Infer from relationship's from_node
                        let node_type = &rel_schema.from_node;
                        let node_schema = schema.get_node_schema_opt(node_type)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(node_type.clone()))?;
                        node_schema.node_id.column().to_string()
                    };
                    
                    let end_id_col = if let Some(label) = &conn.end_node.label {
                        let node_schema = schema.get_node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        node_schema.node_id.column().to_string()
                    } else {
                        // Infer from relationship's to_node
                        let node_type = &rel_schema.to_node;
                        let node_schema = schema.get_node_schema_opt(node_type)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(node_type.clone()))?;
                        node_schema.node_id.column().to_string()
                    };
                    
                    // Generate the NOT EXISTS SQL
                    let exists_sql = match (end_alias, is_undirected) {
                        // Anonymous end node: just check if any relationship exists from start node
                        (None, false) => {
                            // Directed with anonymous end: check FROM or TO based on direction
                            match conn.relationship.direction {
                                Direction::Outgoing => format!(
                                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {}.{})",
                                    full_table, table_name, from_col, start_alias, start_id_col
                                ),
                                Direction::Incoming => format!(
                                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {}.{})",
                                    full_table, table_name, to_col, start_alias, start_id_col
                                ),
                                _ => format!(
                                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {}.{} OR {}.{} = {}.{})",
                                    full_table, 
                                    table_name, from_col, start_alias, start_id_col,
                                    table_name, to_col, start_alias, start_id_col
                                ),
                            }
                        },
                        (None, true) => {
                            // Undirected with anonymous end: check either direction
                            format!(
                                "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {}.{} OR {}.{} = {}.{})",
                                full_table, 
                                table_name, from_col, start_alias, start_id_col,
                                table_name, to_col, start_alias, start_id_col
                            )
                        },
                        (Some(end), true) => {
                            // Named end node, undirected: check both directions
                            format!(
                                "NOT EXISTS (SELECT 1 FROM {} WHERE ({}.{} = {}.{} AND {}.{} = {}.{}) OR ({}.{} = {}.{} AND {}.{} = {}.{}))",
                                full_table,
                                // Direction 1: start -> end
                                table_name, from_col, start_alias, start_id_col,
                                table_name, to_col, end, end_id_col,
                                // Direction 2: end -> start
                                table_name, from_col, end, end_id_col,
                                table_name, to_col, start_alias, start_id_col
                            )
                        },
                        (Some(end), false) => {
                            // Named end node, directed: check single direction
                            let (fk_from, fk_to, from_id, to_id) = match conn.relationship.direction {
                                Direction::Outgoing => (start_alias.as_str(), end.as_str(), &start_id_col, &end_id_col),
                                Direction::Incoming => (end.as_str(), start_alias.as_str(), &end_id_col, &start_id_col),
                                _ => (start_alias.as_str(), end.as_str(), &start_id_col, &end_id_col),
                            };
                            format!(
                                "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {}.{} AND {}.{} = {}.{})",
                                full_table,
                                table_name, from_col, fk_from, from_id,
                                table_name, to_col, fk_to, to_id
                            )
                        },
                    };
                    
                    return Ok(exists_sql);
                }
            }
            
            // Fallback: generate a reasonable default
            let table_name = rel_type.to_lowercase();
            match (end_alias, is_undirected) {
                (None, _) => Ok(format!(
                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.from_id = {}.id OR {}.to_id = {}.id)",
                    table_name, table_name, start_alias, table_name, start_alias
                )),
                (Some(end), true) => Ok(format!(
                    "NOT EXISTS (SELECT 1 FROM {} WHERE ({}.Person1Id = {}.id AND {}.Person2Id = {}.id) OR ({}.Person1Id = {}.id AND {}.Person2Id = {}.id))",
                    table_name,
                    table_name, start_alias, table_name, end,
                    table_name, end, table_name, start_alias
                )),
                (Some(end), false) => Ok(format!(
                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.from_id = {}.id AND {}.to_id = {}.id)",
                    table_name, table_name, start_alias, table_name, end
                )),
            }
        }
        PathPattern::Node(_) => {
            Err(RenderBuildError::InvalidRenderPlan(
                "NOT pattern with single node is not supported".to_string()
            ))
        }
        PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
            Err(RenderBuildError::InvalidRenderPlan(
                "NOT pattern with shortest path is not supported".to_string()
            ))
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum RenderExpr {
    Literal(Literal),

    /// Raw SQL expression as a string
    Raw(String),

    Star,

    TableAlias(TableAlias),

    ColumnAlias(ColumnAlias),

    Column(Column),

    Parameter(String),

    List(Vec<RenderExpr>),

    AggregateFnCall(AggregateFnCall),

    ScalarFnCall(ScalarFnCall),

    PropertyAccessExp(PropertyAccess),

    OperatorApplicationExp(OperatorApplication),

    Case(RenderCase),

    InSubquery(InSubquery),

    /// EXISTS subquery expression - checks if a pattern exists
    ExistsSubquery(ExistsSubquery),

    /// Reduce expression: fold list into single value
    ReduceExpr(ReduceExpr),

    /// Map literal: {key1: value1, key2: value2}
    /// Used in duration({days: 5}), point({x: 1, y: 2}), etc.
    MapLiteral(Vec<(String, RenderExpr)>),

    /// Pattern count: pre-rendered SQL for size((n)-[:REL]->())
    PatternCount(PatternCount),
}

/// Pattern count for size() on patterns
/// Contains pre-rendered SQL for a correlated COUNT(*) subquery
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PatternCount {
    /// Pre-rendered SQL for the pattern count subquery
    pub sql: String,
}

/// Reduce expression for folding a list into a single value
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ReduceExpr {
    /// Name of the accumulator variable
    pub accumulator: String,
    /// Initial value for the accumulator
    pub initial_value: Box<RenderExpr>,
    /// Iteration variable name
    pub variable: String,
    /// List to iterate over
    pub list: Box<RenderExpr>,
    /// Expression evaluated for each element
    pub expression: Box<RenderExpr>,
}

/// EXISTS subquery for render plan
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ExistsSubquery {
    /// Pre-rendered SQL for the EXISTS subquery
    /// This is generated during conversion since EXISTS patterns
    /// don't fit the normal query structure (no select items)
    pub sql: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct InSubquery {
    pub expr: Box<RenderExpr>,
    pub subplan: Box<RenderPlan>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RenderCase {
    /// Expression for simple CASE (CASE x WHEN ...), None for searched CASE
    pub expr: Option<Box<RenderExpr>>,
    /// WHEN conditions and THEN expressions
    pub when_then: Vec<(RenderExpr, RenderExpr)>,
    /// Optional ELSE expression
    pub else_expr: Option<Box<RenderExpr>>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TableAlias(pub String);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnAlias(pub String);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Column(pub PropertyValue);

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Operator {
    Addition,
    Subtraction,
    Multiplication,
    Division,
    ModuloDivision,
    Exponentiation,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanEqual,
    GreaterThanEqual,
    RegexMatch,  // =~ (regex match)
    And,
    Or,
    In,
    NotIn,
    StartsWith,   // STARTS WITH
    EndsWith,     // ENDS WITH
    Contains,     // CONTAINS
    Not,
    Distinct,
    IsNull,
    IsNotNull,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OperatorApplication {
    pub operator: Operator,
    pub operands: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PropertyAccess {
    pub table_alias: TableAlias,
    pub column: Column,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ScalarFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AggregateFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}

impl TryFrom<LogicalExpr> for RenderExpr {
    type Error = RenderBuildError;

    fn try_from(expr: LogicalExpr) -> Result<Self, Self::Error> {
        println!(
            "DEBUG TryFrom RenderExpr: Converting LogicalExpr discriminant={:?}",
            std::mem::discriminant(&expr)
        );
        let expression = match expr {
            LogicalExpr::Literal(lit) => {
                crate::debug_println!("DEBUG TryFrom: Converting Literal variant");
                RenderExpr::Literal(lit.try_into()?)
            }
            LogicalExpr::Raw(raw) => RenderExpr::Raw(raw),
            LogicalExpr::Star => RenderExpr::Star,
            LogicalExpr::TableAlias(alias) => RenderExpr::TableAlias(alias.try_into()?),
            LogicalExpr::ColumnAlias(alias) => RenderExpr::ColumnAlias(alias.try_into()?),
            LogicalExpr::Column(col) => RenderExpr::Column(col.try_into()?),
            LogicalExpr::Parameter(s) => RenderExpr::Parameter(s),
            LogicalExpr::List(exprs) => RenderExpr::List(
                exprs
                    .into_iter()
                    .map(RenderExpr::try_from)
                    .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
            ),
            LogicalExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(agg.try_into()?),
            LogicalExpr::ScalarFnCall(fn_call) => RenderExpr::ScalarFnCall(fn_call.try_into()?),
            LogicalExpr::PropertyAccessExp(pa) => RenderExpr::PropertyAccessExp(pa.try_into()?),
            LogicalExpr::OperatorApplicationExp(op) => {
                // Special case: NOT (PathPattern) -> NOT EXISTS subquery
                if op.operator == LogicalOperator::Not && op.operands.len() == 1 {
                    if let LogicalExpr::PathPattern(ref pattern) = op.operands[0] {
                        let not_exists_sql = generate_not_exists_from_path_pattern(pattern)?;
                        return Ok(RenderExpr::Raw(not_exists_sql));
                    }
                }
                RenderExpr::OperatorApplicationExp(op.try_into()?)
            }
            LogicalExpr::InSubquery(subq) => RenderExpr::InSubquery(subq.try_into()?),
            LogicalExpr::Case(case) => RenderExpr::Case(case.try_into()?),
            LogicalExpr::ExistsSubquery(exists) => {
                // For EXISTS subqueries, generate SQL directly since they don't fit
                // the normal RenderPlan structure (no select items needed)
                let sql = generate_exists_sql(&exists)?;
                RenderExpr::ExistsSubquery(ExistsSubquery { sql })
            }
            LogicalExpr::ReduceExpr(reduce) => {
                // Convert LogicalExpr::ReduceExpr to RenderExpr::ReduceExpr
                RenderExpr::ReduceExpr(ReduceExpr {
                    accumulator: reduce.accumulator,
                    initial_value: Box::new(RenderExpr::try_from(*reduce.initial_value)?),
                    variable: reduce.variable,
                    list: Box::new(RenderExpr::try_from(*reduce.list)?),
                    expression: Box::new(RenderExpr::try_from(*reduce.expression)?),
                })
            }
            LogicalExpr::MapLiteral(entries) => {
                // Convert map literal - preserve key-value pairs
                let converted_entries: Result<Vec<(String, RenderExpr)>, RenderBuildError> = entries
                    .into_iter()
                    .map(|(k, v)| Ok((k, RenderExpr::try_from(v)?)))
                    .collect();
                RenderExpr::MapLiteral(converted_entries?)
            }
            LogicalExpr::LabelExpression { variable, label } => {
                // LabelExpression should have been resolved at analysis time
                // If it reaches here, return false (unknown label)
                log::warn!(
                    "LabelExpression {}:{} reached RenderExpr conversion - returning false",
                    variable, label
                );
                RenderExpr::Literal(Literal::Boolean(false))
            }
            LogicalExpr::PatternCount(pc) => {
                // Generate the pattern count SQL (correlated COUNT(*) subquery)
                let sql = generate_pattern_count_sql(&pc.pattern)?;
                RenderExpr::PatternCount(PatternCount { sql })
            }
            // PathPattern is not present in RenderExpr
            _ => unimplemented!("Conversion for this LogicalExpr variant is not implemented"),
        };
        println!(
            "DEBUG TryFrom RenderExpr: Successfully converted to discriminant={:?}",
            std::mem::discriminant(&expression)
        );
        Ok(expression)
    }
}

impl TryFrom<LogicalInSubquery> for InSubquery {
    type Error = RenderBuildError;

    fn try_from(value: LogicalInSubquery) -> Result<Self, Self::Error> {
        // InSubquery needs schema but TryFrom doesn't provide it
        // Use empty schema as fallback (this is rarely used feature)
        use crate::graph_catalog::graph_schema::GraphSchema;
        let empty_schema = GraphSchema::build(
            1,
            "default".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        );
        let sub_plan = value.subplan.clone().to_render_plan(&empty_schema)?;
        let in_sub_query = InSubquery {
            expr: Box::new((value.expr.as_ref().clone()).try_into()?),
            subplan: Box::new(sub_plan),
        };
        Ok(in_sub_query)
    }
}

impl TryFrom<LogicalLiteral> for Literal {
    type Error = RenderBuildError;

    fn try_from(lit: LogicalLiteral) -> Result<Self, Self::Error> {
        let literal = match lit {
            LogicalLiteral::Integer(i) => Literal::Integer(i),
            LogicalLiteral::Float(f) => Literal::Float(f),
            LogicalLiteral::Boolean(b) => Literal::Boolean(b),
            LogicalLiteral::String(s) => Literal::String(s),
            LogicalLiteral::Null => Literal::Null,
        };
        Ok(literal)
    }
}

impl TryFrom<LogicalTableAlias> for TableAlias {
    type Error = RenderBuildError;

    fn try_from(alias: LogicalTableAlias) -> Result<Self, Self::Error> {
        Ok(TableAlias(alias.0))
    }
}

impl TryFrom<LogicalColumnAlias> for ColumnAlias {
    type Error = RenderBuildError;

    fn try_from(alias: LogicalColumnAlias) -> Result<Self, Self::Error> {
        Ok(ColumnAlias(alias.0))
    }
}

impl TryFrom<LogicalColumn> for Column {
    type Error = RenderBuildError;

    fn try_from(col: LogicalColumn) -> Result<Self, Self::Error> {
        Ok(Column(PropertyValue::Column(col.0.clone())))
    }
}

impl TryFrom<LogicalPropertyAccess> for PropertyAccess {
    type Error = RenderBuildError;

    fn try_from(pa: LogicalPropertyAccess) -> Result<Self, Self::Error> {
        Ok(PropertyAccess {
            table_alias: pa.table_alias.try_into()?,
            column: Column(pa.column), // Wrap PropertyValue in Column
        })
    }
}

impl TryFrom<LogicalOperatorApplication> for OperatorApplication {
    type Error = RenderBuildError;

    fn try_from(op: LogicalOperatorApplication) -> Result<Self, Self::Error> {
        let op_app = OperatorApplication {
            operator: op.operator.try_into()?,
            operands: op
                .operands
                .into_iter()
                .map(RenderExpr::try_from)
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
        };
        Ok(op_app)
    }
}

impl TryFrom<LogicalOperator> for Operator {
    type Error = RenderBuildError;

    fn try_from(value: LogicalOperator) -> Result<Self, Self::Error> {
        let operator = match value {
            LogicalOperator::Addition => Operator::Addition,
            LogicalOperator::Subtraction => Operator::Subtraction,
            LogicalOperator::Multiplication => Operator::Multiplication,
            LogicalOperator::Division => Operator::Division,
            LogicalOperator::ModuloDivision => Operator::ModuloDivision,
            LogicalOperator::Exponentiation => Operator::Exponentiation,
            LogicalOperator::Equal => Operator::Equal,
            LogicalOperator::NotEqual => Operator::NotEqual,
            LogicalOperator::LessThan => Operator::LessThan,
            LogicalOperator::GreaterThan => Operator::GreaterThan,
            LogicalOperator::LessThanEqual => Operator::LessThanEqual,
            LogicalOperator::GreaterThanEqual => Operator::GreaterThanEqual,
            LogicalOperator::RegexMatch => Operator::RegexMatch,
            LogicalOperator::And => Operator::And,
            LogicalOperator::Or => Operator::Or,
            LogicalOperator::In => Operator::In,
            LogicalOperator::NotIn => Operator::NotIn,
            LogicalOperator::StartsWith => Operator::StartsWith,
            LogicalOperator::EndsWith => Operator::EndsWith,
            LogicalOperator::Contains => Operator::Contains,
            LogicalOperator::Not => Operator::Not,
            LogicalOperator::Distinct => Operator::Distinct,
            LogicalOperator::IsNull => Operator::IsNull,
            LogicalOperator::IsNotNull => Operator::IsNotNull,
        };
        Ok(operator)
    }
}

impl TryFrom<LogicalScalarFnCall> for ScalarFnCall {
    type Error = RenderBuildError;

    fn try_from(fc: LogicalScalarFnCall) -> Result<Self, Self::Error> {
        let scalar_fn = ScalarFnCall {
            name: fc.name,
            args: fc
                .args
                .into_iter()
                .map(RenderExpr::try_from)
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
        };
        Ok(scalar_fn)
    }
}

impl TryFrom<LogicalAggregateFnCall> for AggregateFnCall {
    type Error = RenderBuildError;

    fn try_from(agg: LogicalAggregateFnCall) -> Result<Self, Self::Error> {
        // Special case: count(node_variable) should become count(*)
        // When counting a graph node (e.g., count(friend)), the argument is a TableAlias
        // which doesn't exist as a column name inside subqueries. Convert to count(*).
        let converted_args: Vec<RenderExpr> = if agg.name.to_lowercase() == "count" && agg.args.len() == 1 {
            match &agg.args[0] {
                crate::query_planner::logical_expr::LogicalExpr::TableAlias(_) => {
                    // count(node_var) -> count(*)
                    vec![RenderExpr::Star]
                }
                _ => {
                    agg.args
                        .into_iter()
                        .map(RenderExpr::try_from)
                        .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?
                }
            }
        } else {
            agg.args
                .into_iter()
                .map(RenderExpr::try_from)
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?
        };
        
        let agg_fn = AggregateFnCall {
            name: agg.name,
            args: converted_args,
        };
        Ok(agg_fn)
    }
}

impl TryFrom<LogicalCase> for RenderCase {
    type Error = RenderBuildError;

    fn try_from(case: LogicalCase) -> Result<Self, Self::Error> {
        let expr = if let Some(e) = case.expr {
            Some(Box::new(RenderExpr::try_from(*e)?))
        } else {
            None
        };

        let when_then = case
            .when_then
            .into_iter()
            .map(|(when, then)| Ok((RenderExpr::try_from(when)?, RenderExpr::try_from(then)?)))
            .collect::<Result<Vec<(RenderExpr, RenderExpr)>, RenderBuildError>>()?;

        let else_expr = if let Some(e) = case.else_expr {
            Some(Box::new(RenderExpr::try_from(*e)?))
        } else {
            None
        };

        Ok(RenderCase {
            expr,
            when_then,
            else_expr,
        })
    }
}
