//! AST to LogicalExpr Conversion
//!
//! This module contains all `From` and `TryFrom` implementations for converting
//! from `open_cypher_parser::ast` types to our logical expression types.
//!
//! Separating these conversions:
//! - Keeps `mod.rs` focused on type definitions
//! - Makes the conversion logic easier to test in isolation
//! - Clarifies the boundary between parser and planner

use std::sync::Arc;

use crate::{
    clickhouse_query_generator::{
        is_ch_passthrough_aggregate, CH_AGG_PREFIX, CH_PASSTHROUGH_PREFIX,
    },
    open_cypher_parser,
    query_planner::logical_plan::{Filter, GraphNode, GraphRel, LogicalPlan},
};

use super::{
    errors, AggregateFnCall, ConnectedPattern, Direction, ExistsSubquery, LambdaExpr, Literal,
    LogicalCase, LogicalExpr, NodePattern, Operator, OperatorApplication, PathPattern,
    PatternCount, Property, PropertyAccess, PropertyKVPair, ReduceExpr, RelationshipPattern,
    ScalarFnCall, TableAlias,
};

// =============================================================================
// Literal Conversions
// =============================================================================

impl<'a> From<open_cypher_parser::ast::Literal<'a>> for Literal {
    fn from(value: open_cypher_parser::ast::Literal) -> Self {
        match value {
            open_cypher_parser::ast::Literal::Integer(val) => Literal::Integer(val),
            open_cypher_parser::ast::Literal::Float(val) => Literal::Float(val),
            open_cypher_parser::ast::Literal::Boolean(val) => Literal::Boolean(val),
            open_cypher_parser::ast::Literal::String(val) => Literal::String(val.to_string()),
            open_cypher_parser::ast::Literal::Null => Literal::Null,
        }
    }
}

// =============================================================================
// Operator Conversions
// =============================================================================

impl From<open_cypher_parser::ast::Operator> for Operator {
    fn from(value: open_cypher_parser::ast::Operator) -> Self {
        match value {
            open_cypher_parser::ast::Operator::Addition => Operator::Addition,
            open_cypher_parser::ast::Operator::Subtraction => Operator::Subtraction,
            open_cypher_parser::ast::Operator::Multiplication => Operator::Multiplication,
            open_cypher_parser::ast::Operator::Division => Operator::Division,
            open_cypher_parser::ast::Operator::ModuloDivision => Operator::ModuloDivision,
            open_cypher_parser::ast::Operator::Exponentiation => Operator::Exponentiation,
            open_cypher_parser::ast::Operator::Equal => Operator::Equal,
            open_cypher_parser::ast::Operator::NotEqual => Operator::NotEqual,
            open_cypher_parser::ast::Operator::LessThan => Operator::LessThan,
            open_cypher_parser::ast::Operator::GreaterThan => Operator::GreaterThan,
            open_cypher_parser::ast::Operator::LessThanEqual => Operator::LessThanEqual,
            open_cypher_parser::ast::Operator::GreaterThanEqual => Operator::GreaterThanEqual,
            open_cypher_parser::ast::Operator::RegexMatch => Operator::RegexMatch,
            open_cypher_parser::ast::Operator::And => Operator::And,
            open_cypher_parser::ast::Operator::Or => Operator::Or,
            open_cypher_parser::ast::Operator::In => Operator::In,
            open_cypher_parser::ast::Operator::NotIn => Operator::NotIn,
            open_cypher_parser::ast::Operator::StartsWith => Operator::StartsWith,
            open_cypher_parser::ast::Operator::EndsWith => Operator::EndsWith,
            open_cypher_parser::ast::Operator::Contains => Operator::Contains,
            open_cypher_parser::ast::Operator::Not => Operator::Not,
            open_cypher_parser::ast::Operator::Distinct => Operator::Distinct,
            open_cypher_parser::ast::Operator::IsNull => Operator::IsNull,
            open_cypher_parser::ast::Operator::IsNotNull => Operator::IsNotNull,
        }
    }
}

// =============================================================================
// Property Access Conversions
// =============================================================================

impl<'a> From<open_cypher_parser::ast::PropertyAccess<'a>> for PropertyAccess {
    fn from(value: open_cypher_parser::ast::PropertyAccess<'a>) -> Self {
        let alias = value.base.to_string();
        let column = value.key.to_string();
        log::trace!(
            "PropertyAccess::from AST: alias='{}', column='{}'",
            alias,
            column
        );
        PropertyAccess {
            table_alias: TableAlias(alias),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(column),
        }
    }
}

// =============================================================================
// Direction Conversions
// =============================================================================

impl From<open_cypher_parser::ast::Direction> for Direction {
    fn from(value: open_cypher_parser::ast::Direction) -> Self {
        match value {
            open_cypher_parser::ast::Direction::Outgoing => Direction::Outgoing,
            open_cypher_parser::ast::Direction::Incoming => Direction::Incoming,
            open_cypher_parser::ast::Direction::Either => Direction::Either,
        }
    }
}

// =============================================================================
// Operator Application Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::OperatorApplication<'a>> for OperatorApplication {
    type Error = errors::LogicalExprError;

    fn try_from(
        value: open_cypher_parser::ast::OperatorApplication<'a>,
    ) -> Result<Self, Self::Error> {
        let operands = value
            .operands
            .into_iter()
            .map(LogicalExpr::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(OperatorApplication {
            operator: Operator::from(value.operator),
            operands,
        })
    }
}

// =============================================================================
// Function Call Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::FunctionCall<'a>> for LogicalExpr {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::FunctionCall<'a>) -> Result<Self, Self::Error> {
        let name_lower = value.name.to_lowercase();

        // Special handling for size() with pattern argument
        // size((n)-[:REL]->()) should become PatternCount
        if name_lower == "size" && value.args.len() == 1 {
            if let open_cypher_parser::ast::Expression::PathPattern(ref pp) = value.args[0] {
                return Ok(LogicalExpr::PatternCount(PatternCount {
                    pattern: PathPattern::try_from(pp.clone())?,
                }));
            }
        }

        // Standard Neo4j aggregate functions
        let agg_fns = ["count", "min", "max", "avg", "sum", "collect"];

        // Check if it's a standard aggregate function
        let is_standard_agg = agg_fns.contains(&name_lower.as_str());

        // Check if it's a ch./chagg. prefixed ClickHouse aggregate function
        // chagg. prefix is ALWAYS an aggregate (explicit declaration)
        // ch. prefix checks against the aggregate registry
        let is_ch_agg = value.name.starts_with(CH_AGG_PREFIX)
            || (value.name.starts_with(CH_PASSTHROUGH_PREFIX)
                && is_ch_passthrough_aggregate(&value.name));

        let args = value
            .args
            .into_iter()
            .map(LogicalExpr::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        if is_standard_agg || is_ch_agg {
            Ok(LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: value.name,
                args,
            }))
        } else {
            Ok(LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: value.name,
                args,
            }))
        }
    }
}

// =============================================================================
// Path Pattern Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::PathPattern<'a>> for PathPattern {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::PathPattern<'a>) -> Result<Self, Self::Error> {
        match value {
            open_cypher_parser::ast::PathPattern::Node(node) => {
                Ok(PathPattern::Node(NodePattern::try_from(node)?))
            }
            open_cypher_parser::ast::PathPattern::ConnectedPattern(vec_conn) => {
                let connected_patterns = vec_conn
                    .into_iter()
                    .map(ConnectedPattern::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PathPattern::ConnectedPattern(connected_patterns))
            }
            open_cypher_parser::ast::PathPattern::ShortestPath(inner) => Ok(
                PathPattern::ShortestPath(Box::new(PathPattern::try_from(*inner)?)),
            ),
            open_cypher_parser::ast::PathPattern::AllShortestPaths(inner) => Ok(
                PathPattern::AllShortestPaths(Box::new(PathPattern::try_from(*inner)?)),
            ),
        }
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::NodePattern<'a>> for NodePattern {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::NodePattern<'a>) -> Result<Self, Self::Error> {
        let labels_vec = value.labels.map(|ls| {
            ls.into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        });
        let first_label = labels_vec.as_ref().and_then(|ls| ls.first().cloned());

        let properties = value
            .properties
            .map(|props| {
                props
                    .into_iter()
                    .map(Property::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        Ok(NodePattern {
            name: value.name.map(|s| s.to_string()),
            label: first_label,
            labels: labels_vec,
            properties,
        })
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::Property<'a>> for Property {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::Property<'a>) -> Result<Self, Self::Error> {
        match value {
            open_cypher_parser::ast::Property::PropertyKV(kv) => {
                Ok(Property::PropertyKV(PropertyKVPair::try_from(kv)?))
            }
            open_cypher_parser::ast::Property::Param(s) => Ok(Property::Param(s.to_string())),
        }
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::PropertyKVPair<'a>> for PropertyKVPair {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::PropertyKVPair<'a>) -> Result<Self, Self::Error> {
        Ok(PropertyKVPair {
            key: value.key.to_string(),
            value: LogicalExpr::try_from(value.value)?,
        })
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::ConnectedPattern<'a>> for ConnectedPattern {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::ConnectedPattern<'a>) -> Result<Self, Self::Error> {
        Ok(ConnectedPattern {
            start_node: Arc::new(NodePattern::try_from(value.start_node.borrow().clone())?),
            relationship: RelationshipPattern::try_from(value.relationship)?,
            end_node: Arc::new(NodePattern::try_from(value.end_node.borrow().clone())?),
        })
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::RelationshipPattern<'a>> for RelationshipPattern {
    type Error = errors::LogicalExprError;

    fn try_from(
        value: open_cypher_parser::ast::RelationshipPattern<'a>,
    ) -> Result<Self, Self::Error> {
        let properties = value
            .properties
            .map(|props| {
                props
                    .into_iter()
                    .map(Property::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        Ok(RelationshipPattern {
            name: value.name.map(|s| s.to_string()),
            direction: Direction::from(value.direction),
            labels: value
                .labels
                .map(|labels| labels.into_iter().map(|s| s.to_string()).collect()),
            properties,
        })
    }
}

// =============================================================================
// Case Expression Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::Case<'a>> for LogicalCase {
    type Error = errors::LogicalExprError;

    fn try_from(case: open_cypher_parser::ast::Case<'a>) -> Result<Self, Self::Error> {
        let expr = case
            .expr
            .map(|e| LogicalExpr::try_from(*e).map(Box::new))
            .transpose()?;
        let when_then = case
            .when_then
            .into_iter()
            .map(|(when, then)| {
                let w = LogicalExpr::try_from(when)?;
                let t = LogicalExpr::try_from(then)?;
                Ok((w, t))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let else_expr = case
            .else_expr
            .map(|e| LogicalExpr::try_from(*e).map(Box::new))
            .transpose()?;
        Ok(LogicalCase {
            expr,
            when_then,
            else_expr,
        })
    }
}

// =============================================================================
// EXISTS Subquery Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::ExistsSubquery<'a>> for ExistsSubquery {
    type Error = errors::LogicalExprError;

    fn try_from(exists: open_cypher_parser::ast::ExistsSubquery<'a>) -> Result<Self, Self::Error> {
        use open_cypher_parser::ast::PathPattern as AstPathPattern;

        let pattern = exists.pattern;

        let base_plan = match pattern {
            AstPathPattern::Node(node) => Arc::new(LogicalPlan::GraphNode(GraphNode {
                input: Arc::new(LogicalPlan::Empty),
                alias: node.name.unwrap_or("").to_string(),
                label: node.first_label().map(|s| s.to_string()),
                is_denormalized: false,
                projected_columns: None,
            })),
            AstPathPattern::ConnectedPattern(connected_patterns) => {
                if connected_patterns.is_empty() {
                    Arc::new(LogicalPlan::Empty)
                } else {
                    let cp = &connected_patterns[0];
                    let start = cp.start_node.borrow();
                    let end = cp.end_node.borrow();
                    let rel = &cp.relationship;

                    let start_node = LogicalPlan::GraphNode(GraphNode {
                        input: Arc::new(LogicalPlan::Empty),
                        alias: start.name.unwrap_or("").to_string(),
                        label: start.first_label().map(|s| s.to_string()),
                        is_denormalized: false,
                        projected_columns: None,
                    });

                    let rel_scan = LogicalPlan::Empty;

                    let end_node = LogicalPlan::GraphNode(GraphNode {
                        input: Arc::new(LogicalPlan::Empty),
                        alias: end.name.unwrap_or("").to_string(),
                        label: end.first_label().map(|s| s.to_string()),
                        is_denormalized: false,
                        projected_columns: None,
                    });

                    let direction = match rel.direction {
                        open_cypher_parser::ast::Direction::Outgoing => Direction::Outgoing,
                        open_cypher_parser::ast::Direction::Incoming => Direction::Incoming,
                        open_cypher_parser::ast::Direction::Either => Direction::Either,
                    };

                    Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(start_node),
                        center: Arc::new(rel_scan),
                        right: Arc::new(end_node),
                        alias: rel.name.unwrap_or("").to_string(),
                        direction,
                        left_connection: start.name.unwrap_or("").to_string(),
                        right_connection: end.name.unwrap_or("").to_string(),
                        is_rel_anchor: false,
                        variable_length: None,
                        shortest_path_mode: None,
                        path_variable: None,
                        where_predicate: None,
                        labels: rel
                            .labels
                            .as_ref()
                            .map(|l| l.iter().map(|s| s.to_string()).collect()),
                        is_optional: None,
                        anchor_connection: None,
                        cte_references: std::collections::HashMap::new(),
                    }))
                }
            }
            AstPathPattern::ShortestPath(inner) | AstPathPattern::AllShortestPaths(inner) => {
                let inner_exists = open_cypher_parser::ast::ExistsSubquery {
                    pattern: *inner,
                    where_clause: None,
                };
                return ExistsSubquery::try_from(inner_exists);
            }
        };

        let plan = if let Some(where_clause) = exists.where_clause {
            Arc::new(LogicalPlan::Filter(Filter {
                input: base_plan,
                predicate: LogicalExpr::try_from(where_clause.conditions)?,
            }))
        } else {
            base_plan
        };

        Ok(ExistsSubquery { subplan: plan })
    }
}

// =============================================================================
// Main Expression Conversion
// =============================================================================

impl<'a> std::convert::TryFrom<open_cypher_parser::ast::Expression<'a>> for LogicalExpr {
    type Error = errors::LogicalExprError;

    fn try_from(expr: open_cypher_parser::ast::Expression<'a>) -> Result<Self, Self::Error> {
        use open_cypher_parser::ast::Expression;
        match expr {
            Expression::Literal(lit) => Ok(LogicalExpr::Literal(Literal::from(lit))),
            Expression::Variable(s) => {
                if s == "*" {
                    Ok(LogicalExpr::Star)
                } else {
                    Ok(LogicalExpr::TableAlias(TableAlias(s.to_string())))
                }
            }
            Expression::Parameter(s) => Ok(LogicalExpr::Parameter(s.to_string())),
            Expression::List(exprs) => {
                let logical_exprs = exprs
                    .into_iter()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(LogicalExpr::List(logical_exprs))
            }
            Expression::FunctionCallExp(fc) => LogicalExpr::try_from(fc),
            Expression::PropertyAccessExp(pa) => {
                Ok(LogicalExpr::PropertyAccessExp(PropertyAccess::from(pa)))
            }
            Expression::OperatorApplicationExp(oa) => Ok(LogicalExpr::OperatorApplicationExp(
                OperatorApplication::try_from(oa)?,
            )),
            Expression::PathPattern(pp) => Ok(LogicalExpr::PathPattern(PathPattern::try_from(pp)?)),
            Expression::Case(case) => Ok(LogicalExpr::Case(LogicalCase::try_from(case)?)),
            Expression::ExistsExpression(exists) => Ok(LogicalExpr::ExistsSubquery(
                ExistsSubquery::try_from(*exists)?,
            )),
            Expression::ReduceExp(reduce) => Ok(LogicalExpr::ReduceExpr(ReduceExpr {
                accumulator: reduce.accumulator.to_string(),
                initial_value: Box::new(Self::try_from(*reduce.initial_value)?),
                variable: reduce.variable.to_string(),
                list: Box::new(Self::try_from(*reduce.list)?),
                expression: Box::new(Self::try_from(*reduce.expression)?),
            })),
            Expression::MapLiteral(entries) => {
                let logical_entries = entries
                    .into_iter()
                    .map(|(k, v)| Self::try_from(v).map(|lv| (k.to_string(), lv)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(LogicalExpr::MapLiteral(logical_entries))
            }
            Expression::LabelExpression { variable, label } => Ok(LogicalExpr::LabelExpression {
                variable: variable.to_string(),
                label: label.to_string(),
            }),
            Expression::Lambda(lambda) => Ok(LogicalExpr::Lambda(LambdaExpr {
                params: lambda.params.iter().map(|s| s.to_string()).collect(),
                body: Box::new(Self::try_from(*lambda.body)?),
            })),
            Expression::PatternComprehension(_) => {
                Err(errors::LogicalExprError::PatternComprehensionNotRewritten)
            }
            Expression::ArraySubscript { array, index } => Ok(LogicalExpr::ArraySubscript {
                array: Box::new(LogicalExpr::try_from(*array)?),
                index: Box::new(LogicalExpr::try_from(*index)?),
            }),
            Expression::ArraySlicing { array, from, to } => Ok(LogicalExpr::ArraySlicing {
                array: Box::new(LogicalExpr::try_from(*array)?),
                from: from
                    .map(|f| LogicalExpr::try_from(*f).map(Box::new))
                    .transpose()?,
                to: to
                    .map(|t| LogicalExpr::try_from(*t).map(Box::new))
                    .transpose()?,
            }),
        }
    }
}
