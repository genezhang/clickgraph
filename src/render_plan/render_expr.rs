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
    PropertyAccess as LogicalPropertyAccess, ScalarFnCall as LogicalScalarFnCall,
    TableAlias as LogicalTableAlias,
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
                            schema.get_node_schema_opt(label)
                                .map(|n| n.node_id.column().to_string())
                                .unwrap_or_else(|| "id".to_string())
                        } else {
                            // No label, try to get from the context
                            "user_id".to_string() // Default for User nodes
                        }
                    } else {
                        "user_id".to_string()
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
            
            // Fallback: generate a placeholder SQL if schema lookup fails
            Ok(format!("SELECT 1 FROM {} WHERE {} = {}.id", 
                rel_type.to_lowercase(), 
                "from_id",
                start_alias))
        }
        _ => {
            // For other plan types, generate a simple placeholder
            Ok("SELECT 1".to_string())
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
        let agg_fn = AggregateFnCall {
            name: agg.name,
            args: agg
                .args
                .into_iter()
                .map(RenderExpr::try_from)
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
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
