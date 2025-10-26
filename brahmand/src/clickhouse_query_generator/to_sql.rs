use std::sync::Arc;
use super::errors::ClickhouseQueryGeneratorError;
use crate::query_planner::logical_expr::{LogicalExpr, Literal, Operator, OperatorApplication, AggregateFnCall, ScalarFnCall, PropertyAccess, LogicalCase, InSubquery};
use crate::query_planner::logical_plan::LogicalPlan;

/// Convert a plan node to SQL
#[allow(dead_code)]
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
            LogicalExpr::AggregateFnCall(fn_call) => {
                let args_sql: Result<Vec<String>, _> = fn_call.args.iter().map(|e| e.to_sql()).collect();
                Ok(format!("{}({})", fn_call.name, args_sql?.join(", ")))
            },
            LogicalExpr::ScalarFnCall(fn_call) => {
                let args_sql: Result<Vec<String>, _> = fn_call.args.iter().map(|e| e.to_sql()).collect();
                Ok(format!("{}({})", fn_call.name, args_sql?.join(", ")))
            },
            LogicalExpr::PropertyAccessExp(prop) => {
                Ok(format!("{}.{}", prop.table_alias.0, prop.column.0))
            },
            LogicalExpr::OperatorApplicationExp(op) => {
                let operands_sql: Vec<String> = op.operands.iter().map(|e| e.to_sql()).collect::<Result<Vec<String>, _>>()?;
                match op.operator {
                    Operator::Addition => Ok(format!("({} + {})", operands_sql[0], operands_sql[1])),
                    Operator::Subtraction => Ok(format!("({} - {})", operands_sql[0], operands_sql[1])),
                    Operator::Multiplication => Ok(format!("({} * {})", operands_sql[0], operands_sql[1])),
                    Operator::Division => Ok(format!("({} / {})", operands_sql[0], operands_sql[1])),
                    Operator::ModuloDivision => Ok(format!("({} % {})", operands_sql[0], operands_sql[1])),
                    Operator::Exponentiation => Ok(format!("power({}, {})", operands_sql[0], operands_sql[1])),
                    Operator::Equal => Ok(format!("({} = {})", operands_sql[0], operands_sql[1])),
                    Operator::NotEqual => Ok(format!("({} != {})", operands_sql[0], operands_sql[1])),
                    Operator::LessThan => Ok(format!("({} < {})", operands_sql[0], operands_sql[1])),
                    Operator::GreaterThan => Ok(format!("({} > {})", operands_sql[0], operands_sql[1])),
                    Operator::LessThanEqual => Ok(format!("({} <= {})", operands_sql[0], operands_sql[1])),
                    Operator::GreaterThanEqual => Ok(format!("({} >= {})", operands_sql[0], operands_sql[1])),
                    Operator::And => Ok(format!("({} AND {})", operands_sql[0], operands_sql[1])),
                    Operator::Or => Ok(format!("({} OR {})", operands_sql[0], operands_sql[1])),
                    Operator::In => Ok(format!("({} IN {})", operands_sql[0], operands_sql[1])),
                    Operator::NotIn => Ok(format!("({} NOT IN {})", operands_sql[0], operands_sql[1])),
                    Operator::Not => Ok(format!("NOT ({})", operands_sql[0])),
                    Operator::Distinct => Ok(format!("DISTINCT {}", operands_sql[0])),
                    Operator::IsNull => Ok(format!("({} IS NULL)", operands_sql[0])),
                    Operator::IsNotNull => Ok(format!("({} IS NOT NULL)", operands_sql[0])),
                }
            },
            LogicalExpr::Case(case_expr) => {
                let mut sql = String::from("CASE ");
                
                // Simple CASE (CASE x WHEN ...)
                if let Some(expr) = &case_expr.expr {
                    sql.push_str(&format!("{} ", expr.to_sql()?));
                }
                
                // WHEN ... THEN ... clauses
                for (when_expr, then_expr) in &case_expr.when_then {
                    sql.push_str(&format!("WHEN {} THEN {} ", when_expr.to_sql()?, then_expr.to_sql()?));
                }
                
                // ELSE clause
                if let Some(else_expr) = &case_expr.else_expr {
                    sql.push_str(&format!("ELSE {} ", else_expr.to_sql()?));
                }
                
                sql.push_str("END");
                Ok(sql)
            },
            LogicalExpr::InSubquery(in_subquery) => {
                let expr_sql = in_subquery.expr.to_sql()?;
                let subquery_sql = in_subquery.subplan.to_sql()?;
                Ok(format!("{} IN ({})", expr_sql, subquery_sql))
            },
            LogicalExpr::PathPattern(_) => {
                // Path patterns are handled at the logical plan level, not expression level
                Err(ClickhouseQueryGeneratorError::UnsupportedItemInWhereClause)
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
