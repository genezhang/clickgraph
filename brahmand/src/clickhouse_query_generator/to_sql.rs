use std::sync::Arc;
use super::errors::ClickhouseQueryGeneratorError;
use crate::query_planner::logical_expr::{LogicalExpr, Literal};
use crate::query_planner::logical_plan::LogicalPlan;

/// Convert a plan node to SQL
pub trait ToSql {
    /// Convert this node to a SQL string
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError>;
}

impl ToSql for Arc<LogicalPlan> {
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        self.as_ref().to_sql()
    }
}

impl ToSql for LogicalExpr {
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        match self {
            LogicalExpr::Literal(lit) => match lit {
                Literal::Integer(i) => Ok(i.to_string()),
                Literal::Float(f) => Ok(f.to_string()),
                Literal::Boolean(b) => Ok(b.to_string()),
                Literal::String(s) => Ok(format!("'{}'", s)),
                Literal::Null => Ok("NULL".to_string()),
            },
            LogicalExpr::Raw(raw) => Ok(raw.clone()),
            LogicalExpr::Star => Ok("*".into()),
            LogicalExpr::TableAlias(alias) => Ok(alias.0.clone()),
            LogicalExpr::ColumnAlias(col) => Ok(col.0.clone()),
            LogicalExpr::Column(col) => Ok(col.0.clone()),
            LogicalExpr::Parameter(param) => Ok(format!("${}", param)),
            LogicalExpr::List(items) => {
                let items_sql: Result<Vec<String>, _> = items.iter().map(|e| e.to_sql()).collect();
                Ok(format!("({})", items_sql?.join(", ")))
            },
            LogicalExpr::Operator(op) => {
                // For operators with multiple operands
                let mut operands = vec![];
                for operand in op.operands.iter() {
                    operands.push(operand.to_sql()?);
                }
                Ok(format!("({} {:?} {})", 
                    operands[0], 
                    op.operator,
                    operands[1]
                ))
            },
            _ => todo!("Not yet implemented"),
        }
    }
}

impl ToSql for LogicalPlan {
    fn to_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        match self {
            LogicalPlan::ViewScan(scan) => {
                Ok(format!("SELECT * FROM {}", scan.source_table))
            },
            LogicalPlan::Projection(proj) => {
                let mut sql = String::new();
                sql.push_str("SELECT ");
                
                let mut projections = Vec::new();
                for item in &proj.items {
                    projections.push(item.expression.to_sql()?);
                }
                sql.push_str(&projections.join(", "));
                
                sql.push_str("\nFROM ");
                sql.push_str(&proj.input.to_sql()?);
                
                Ok(sql)
            },
            _ => Err(ClickhouseQueryGeneratorError::UnsupportedDDLQuery),
        }
    }
}