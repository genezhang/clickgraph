use crate::{
    open_cypher_parser::{self},
    query_planner::logical_plan::LogicalPlan,
};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

// Import serde_arc module for serialization
#[path = "../../utils/serde_arc.rs"]
mod serde_arc;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum LogicalExpr {
    /// A literal, such as a number, string, boolean, or null.
    Literal(Literal),

    /// Raw SQL expression as a string
    Raw(String),

    Star,

    /// Table Alias (e.g. (p)-[f:Follow]-(u), p, f and u are table alias expr).
    TableAlias(TableAlias),

    ColumnAlias(ColumnAlias),

    /// Binary operator application (e.g. a + b)
    Operator(OperatorApplication),

    /// Columns to use in projection.
    Column(Column),

    /// A parameter, such as `$param` or `$0`.
    Parameter(String),

    /// A list literal: a vector of expressions.
    List(Vec<LogicalExpr>),

    AggregateFnCall(AggregateFnCall),

    /// A function call, e.g. length(p) or nodes(p).
    ScalarFnCall(ScalarFnCall),

    /// Property access.
    PropertyAccessExp(PropertyAccess),

    /// An operator application, e.g. 1 + 2 or 3 < 4.
    OperatorApplicationExp(OperatorApplication),

    /// A path-pattern, for instance: (a)-[]->()<-[]-(b)
    PathPattern(PathPattern),

    /// A CASE expression
    Case(LogicalCase),

    InSubquery(InSubquery),

    /// EXISTS subquery expression
    /// Checks if a pattern exists in the graph
    ExistsSubquery(ExistsSubquery),
}

/// EXISTS subquery for checking pattern existence
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ExistsSubquery {
    /// The logical plan representing the EXISTS pattern
    #[serde(with = "serde_arc")]
    pub subplan: Arc<LogicalPlan>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct InSubquery {
    pub expr: Box<LogicalExpr>,
    #[serde(with = "serde_arc")]
    pub subplan: Arc<LogicalPlan>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Direction {
    Outgoing,
    Incoming,
    Either,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Incoming => f.write_str("incoming"),
            Direction::Outgoing => f.write_str("outgoing"),
            Direction::Either => f.write_str("either"),
        }
    }
}

impl Direction {
    pub fn reverse(self) -> Self {
        if self == Direction::Incoming {
            Direction::Outgoing
        } else if self == Direction::Outgoing {
            Direction::Incoming
        } else {
            self
        }
    }
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
pub struct Column(pub String);

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
    pub operands: Vec<LogicalExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PropertyAccess {
    pub table_alias: TableAlias,
    pub column: crate::graph_catalog::expression_parser::PropertyValue,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LogicalCase {
    /// Expression for simple CASE (CASE x WHEN ...), None for searched CASE
    pub expr: Option<Box<LogicalExpr>>,
    /// WHEN conditions and THEN expressions
    pub when_then: Vec<(LogicalExpr, LogicalExpr)>,
    /// Optional ELSE expression
    pub else_expr: Option<Box<LogicalExpr>>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ScalarFnCall {
    pub name: String,
    pub args: Vec<LogicalExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AggregateFnCall {
    pub name: String,
    pub args: Vec<LogicalExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum PathPattern {
    Node(NodePattern),
    ConnectedPattern(Vec<ConnectedPattern>),
    ShortestPath(Box<PathPattern>),
    AllShortestPaths(Box<PathPattern>),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct NodePattern {
    pub name: Option<String>,
    pub label: Option<String>,
    pub properties: Option<Vec<Property>>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Property {
    PropertyKV(PropertyKVPair),
    Param(String),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PropertyKVPair {
    pub key: String,
    pub value: Literal,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ConnectedPattern {
    #[serde(with = "serde_arc")]
    pub start_node: Arc<NodePattern>,
    pub relationship: RelationshipPattern,
    #[serde(with = "serde_arc")]
    pub end_node: Arc<NodePattern>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RelationshipPattern {
    pub name: Option<String>,
    pub direction: Direction,
    pub labels: Option<Vec<String>>, // Support multiple labels: [:TYPE1|TYPE2]
    pub properties: Option<Vec<Property>>,
}

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

impl<'a> From<open_cypher_parser::ast::PropertyAccess<'a>> for PropertyAccess {
    fn from(value: open_cypher_parser::ast::PropertyAccess<'a>) -> Self {
        let alias = value.base.to_string();
        let column = value.key.to_string();
        println!(
            "PropertyAccess::from AST: alias='{}', column='{}'",
            alias, column
        );
        PropertyAccess {
            table_alias: TableAlias(alias),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(column),
        }
    }
}

impl From<open_cypher_parser::ast::Direction> for Direction {
    fn from(value: open_cypher_parser::ast::Direction) -> Self {
        match value {
            open_cypher_parser::ast::Direction::Outgoing => Direction::Outgoing,
            open_cypher_parser::ast::Direction::Incoming => Direction::Incoming,
            open_cypher_parser::ast::Direction::Either => Direction::Either,
        }
    }
}

impl<'a> From<open_cypher_parser::ast::OperatorApplication<'a>> for OperatorApplication {
    fn from(value: open_cypher_parser::ast::OperatorApplication<'a>) -> Self {
        OperatorApplication {
            operator: Operator::from(value.operator),
            operands: value.operands.into_iter().map(LogicalExpr::from).collect(),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::FunctionCall<'a>> for LogicalExpr {
    fn from(value: open_cypher_parser::ast::FunctionCall<'a>) -> Self {
        let agg_fns = ["count", "min", "max", "avg", "sum", "collect"];
        let name_lower = value.name.to_lowercase();
        if agg_fns.contains(&name_lower.as_str()) {
            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: value.name,
                args: value.args.into_iter().map(LogicalExpr::from).collect(),
            })
        } else {
            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: value.name,
                args: value.args.into_iter().map(LogicalExpr::from).collect(),
            })
        }
    }
}

impl<'a> From<open_cypher_parser::ast::PathPattern<'a>> for PathPattern {
    fn from(value: open_cypher_parser::ast::PathPattern<'a>) -> Self {
        match value {
            open_cypher_parser::ast::PathPattern::Node(node) => {
                PathPattern::Node(NodePattern::from(node))
            }
            open_cypher_parser::ast::PathPattern::ConnectedPattern(vec_conn) => {
                PathPattern::ConnectedPattern(
                    vec_conn.into_iter().map(ConnectedPattern::from).collect(),
                )
            }
            open_cypher_parser::ast::PathPattern::ShortestPath(inner) => {
                // Recursively convert the inner pattern and wrap it
                PathPattern::ShortestPath(Box::new(PathPattern::from(*inner)))
            }
            open_cypher_parser::ast::PathPattern::AllShortestPaths(inner) => {
                // Recursively convert the inner pattern and wrap it
                PathPattern::AllShortestPaths(Box::new(PathPattern::from(*inner)))
            }
        }
    }
}

impl<'a> From<open_cypher_parser::ast::NodePattern<'a>> for NodePattern {
    fn from(value: open_cypher_parser::ast::NodePattern<'a>) -> Self {
        NodePattern {
            name: value.name.map(|s| s.to_string()),
            label: value.label.map(|s| s.to_string()),
            properties: value
                .properties
                .map(|props| props.into_iter().map(Property::from).collect()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::Property<'a>> for Property {
    fn from(value: open_cypher_parser::ast::Property<'a>) -> Self {
        match value {
            open_cypher_parser::ast::Property::PropertyKV(kv) => {
                Property::PropertyKV(PropertyKVPair::from(kv))
            }
            open_cypher_parser::ast::Property::Param(s) => Property::Param(s.to_string()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::PropertyKVPair<'a>> for PropertyKVPair {
    fn from(value: open_cypher_parser::ast::PropertyKVPair<'a>) -> Self {
        PropertyKVPair {
            key: value.key.to_string(),
            value: match value.value {
                open_cypher_parser::ast::Expression::Literal(lit) => Literal::from(lit),
                _ => panic!("Property value must be a literal"),
            },
        }
    }
}

impl<'a> From<open_cypher_parser::ast::ConnectedPattern<'a>> for ConnectedPattern {
    fn from(value: open_cypher_parser::ast::ConnectedPattern<'a>) -> Self {
        ConnectedPattern {
            start_node: Arc::new(NodePattern::from(value.start_node.borrow().clone())),
            relationship: RelationshipPattern::from(value.relationship),
            end_node: Arc::new(NodePattern::from(value.end_node.borrow().clone())),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::RelationshipPattern<'a>> for RelationshipPattern {
    fn from(value: open_cypher_parser::ast::RelationshipPattern<'a>) -> Self {
        RelationshipPattern {
            name: value.name.map(|s| s.to_string()),
            direction: Direction::from(value.direction),
            labels: value
                .labels
                .map(|labels| labels.into_iter().map(|s| s.to_string()).collect()),
            properties: value
                .properties
                .map(|props| props.into_iter().map(Property::from).collect()),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::Case<'a>> for LogicalCase {
    fn from(case: open_cypher_parser::ast::Case<'a>) -> Self {
        LogicalCase {
            expr: case.expr.map(|e| Box::new(LogicalExpr::from(*e))),
            when_then: case
                .when_then
                .into_iter()
                .map(|(when, then)| (LogicalExpr::from(when), LogicalExpr::from(then)))
                .collect(),
            else_expr: case.else_expr.map(|e| Box::new(LogicalExpr::from(*e))),
        }
    }
}

impl<'a> From<open_cypher_parser::ast::ExistsSubquery<'a>> for ExistsSubquery {
    fn from(exists: open_cypher_parser::ast::ExistsSubquery<'a>) -> Self {
        use open_cypher_parser::ast::PathPattern as AstPathPattern;
        use crate::query_planner::logical_plan::{LogicalPlan, Scan, GraphNode, GraphRel, Filter};
        
        // Convert the pattern to a logical plan structure
        // The EXISTS pattern gets converted to a subplan that can be checked for existence
        let pattern = exists.pattern;
        
        // Build the logical plan from the pattern based on its type
        let base_plan = match pattern {
            AstPathPattern::Node(node) => {
                // Single node pattern - create a scan
                let scan = LogicalPlan::Scan(Scan {
                    table_alias: node.name.map(|s| s.to_string()),
                    table_name: node.label.map(|s| s.to_string()),
                });
                Arc::new(LogicalPlan::GraphNode(GraphNode {
                    input: Arc::new(scan),
                    alias: node.name.unwrap_or("").to_string(),
                    label: node.label.map(|s| s.to_string()),
                    is_denormalized: false,
                }))
            }
            AstPathPattern::ConnectedPattern(connected_patterns) => {
                // Connected patterns - create a relationship traversal
                if connected_patterns.is_empty() {
                    Arc::new(LogicalPlan::Empty)
                } else {
                    // Handle the first connected pattern
                    let cp = &connected_patterns[0];
                    let start = cp.start_node.borrow();
                    let end = cp.end_node.borrow();
                    let rel = &cp.relationship;
                    
                    let start_scan = LogicalPlan::Scan(Scan {
                        table_alias: start.name.map(|s| s.to_string()),
                        table_name: start.label.map(|s| s.to_string()),
                    });
                    let start_node = LogicalPlan::GraphNode(GraphNode {
                        input: Arc::new(start_scan),
                        alias: start.name.unwrap_or("").to_string(),
                        label: start.label.map(|s| s.to_string()),
                        is_denormalized: false,
                    });
                    
                    let rel_scan = LogicalPlan::Scan(Scan {
                        table_alias: rel.name.map(|s| s.to_string()),
                        table_name: rel.labels.as_ref().and_then(|l| l.first()).map(|s| s.to_string()),
                    });
                    
                    let end_scan = LogicalPlan::Scan(Scan {
                        table_alias: end.name.map(|s| s.to_string()),
                        table_name: end.label.map(|s| s.to_string()),
                    });
                    let end_node = LogicalPlan::GraphNode(GraphNode {
                        input: Arc::new(end_scan),
                        alias: end.name.unwrap_or("").to_string(),
                        label: end.label.map(|s| s.to_string()),
                        is_denormalized: false,
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
                        labels: rel.labels.as_ref().map(|l| l.iter().map(|s| s.to_string()).collect()),
                        is_optional: None,
                        anchor_connection: None,
                    }))
                }
            }
            AstPathPattern::ShortestPath(inner) | AstPathPattern::AllShortestPaths(inner) => {
                // For shortest path patterns, recursively convert the inner pattern
                let inner_exists = open_cypher_parser::ast::ExistsSubquery {
                    pattern: *inner,
                    where_clause: None,
                };
                return ExistsSubquery::from(inner_exists);
            }
        };
        
        // If there's a WHERE clause, add a filter
        let plan = if let Some(where_clause) = exists.where_clause {
            Arc::new(LogicalPlan::Filter(Filter {
                input: base_plan,
                predicate: LogicalExpr::from(where_clause.conditions),
            }))
        } else {
            base_plan
        };
        
        ExistsSubquery { subplan: plan }
    }
}

impl<'a> From<open_cypher_parser::ast::Expression<'a>> for LogicalExpr {
    fn from(expr: open_cypher_parser::ast::Expression<'a>) -> Self {
        use open_cypher_parser::ast::Expression;
        match expr {
            Expression::Literal(lit) => LogicalExpr::Literal(Literal::from(lit)),
            Expression::Variable(s) => {
                if s == "*" {
                    LogicalExpr::Star
                } else {
                    // TODO revisit this
                    // LogicalExpr::Variable(s.to_string())
                    LogicalExpr::TableAlias(TableAlias(s.to_string()))
                }
            }
            Expression::Parameter(s) => LogicalExpr::Parameter(s.to_string()),
            Expression::List(exprs) => {
                LogicalExpr::List(exprs.into_iter().map(LogicalExpr::from).collect())
            }
            Expression::FunctionCallExp(fc) => LogicalExpr::from(fc),
            Expression::PropertyAccessExp(pa) => {
                LogicalExpr::PropertyAccessExp(PropertyAccess::from(pa))
            }
            Expression::OperatorApplicationExp(oa) => {
                LogicalExpr::OperatorApplicationExp(OperatorApplication::from(oa))
            }
            Expression::PathPattern(pp) => LogicalExpr::PathPattern(PathPattern::from(pp)),
            Expression::Case(case) => LogicalExpr::Case(LogicalCase::from(case)),
            Expression::ExistsExpression(exists) => {
                // Convert the EXISTS pattern to a logical plan
                // The pattern needs to be converted to a scan + filter structure
                LogicalExpr::ExistsSubquery(ExistsSubquery::from(*exists))
            }
        }
    }
}

impl fmt::Display for TableAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for ColumnAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::Float(fl) => write!(f, "{}", fl),
            Literal::Boolean(b) => write!(f, "{}", b),
            Literal::String(s) => write!(f, "{}", s),
            Literal::Null => write!(f, "null"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_literal_from_ast() {
        // Test integer conversion
        let ast_int = ast::Literal::Integer(42);
        let logical_int = Literal::from(ast_int);
        assert_eq!(logical_int, Literal::Integer(42));

        // Test float conversion
        let ast_float = ast::Literal::Float(3.14);
        let logical_float = Literal::from(ast_float);
        assert_eq!(logical_float, Literal::Float(3.14));

        // Test boolean conversion
        let ast_bool = ast::Literal::Boolean(true);
        let logical_bool = Literal::from(ast_bool);
        assert_eq!(logical_bool, Literal::Boolean(true));

        // Test string conversion
        let ast_string = ast::Literal::String("New York");
        let logical_string = Literal::from(ast_string);
        assert_eq!(logical_string, Literal::String("New York".to_string()));

        // Test null conversion
        let ast_null = ast::Literal::Null;
        let logical_null = Literal::from(ast_null);
        assert_eq!(logical_null, Literal::Null);
    }

    #[test]
    fn test_direction_reverse() {
        assert_eq!(Direction::Outgoing.reverse(), Direction::Incoming);
        assert_eq!(Direction::Incoming.reverse(), Direction::Outgoing);
        assert_eq!(Direction::Either.reverse(), Direction::Either);
    }

    #[test]
    fn test_operator_application_from_ast() {
        let ast_operator_app = ast::OperatorApplication {
            operator: ast::Operator::Equal,
            operands: vec![
                ast::Expression::Variable("city"),
                ast::Expression::Literal(ast::Literal::String("San Francisco")),
            ],
        };
        let logical_operator_app = OperatorApplication::from(ast_operator_app);

        assert_eq!(logical_operator_app.operator, Operator::Equal);
        assert_eq!(logical_operator_app.operands.len(), 2);

        match &logical_operator_app.operands[0] {
            LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "city"),
            _ => panic!("Expected TableAlias"),
        }

        match &logical_operator_app.operands[1] {
            LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "San Francisco"),
            _ => panic!("Expected String literal"),
        }
    }

    #[test]
    fn test_function_call_conversion_aggregate() {
        let ast_function_call = ast::FunctionCall {
            name: "count".to_string(),
            args: vec![ast::Expression::Variable("person")],
        };
        let logical_expr = LogicalExpr::from(ast_function_call);

        match logical_expr {
            LogicalExpr::AggregateFnCall(agg_fn) => {
                assert_eq!(agg_fn.name, "count");
                assert_eq!(agg_fn.args.len(), 1);
                match &agg_fn.args[0] {
                    LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "person"),
                    _ => panic!("Expected TableAlias"),
                }
            }
            _ => panic!("Expected AggregateFnCall"),
        }
    }

    #[test]
    fn test_function_call_conversion_scalar() {
        let ast_function_call = ast::FunctionCall {
            name: "length".to_string(),
            args: vec![ast::Expression::Variable("username")],
        };
        let logical_expr = LogicalExpr::from(ast_function_call);

        match logical_expr {
            LogicalExpr::ScalarFnCall(scalar_fn) => {
                assert_eq!(scalar_fn.name, "length");
                assert_eq!(scalar_fn.args.len(), 1);
                match &scalar_fn.args[0] {
                    LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "username"),
                    _ => panic!("Expected TableAlias"),
                }
            }
            _ => panic!("Expected ScalarFnCall"),
        }
    }

    #[test]
    fn test_node_pattern_from_ast() {
        let ast_node_pattern = ast::NodePattern {
            name: Some("employee"),
            label: Some("Person"),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "department",
                value: ast::Expression::Literal(ast::Literal::String("Engineering")),
            })]),
        };
        let logical_node_pattern = NodePattern::from(ast_node_pattern);

        assert_eq!(logical_node_pattern.name, Some("employee".to_string()));
        assert_eq!(logical_node_pattern.label, Some("Person".to_string()));
        assert!(logical_node_pattern.properties.is_some());

        let properties = logical_node_pattern.properties.unwrap();
        assert_eq!(properties.len(), 1);

        match &properties[0] {
            Property::PropertyKV(kv) => {
                assert_eq!(kv.key, "department");
                assert_eq!(kv.value, Literal::String("Engineering".to_string()));
            }
            _ => panic!("Expected PropertyKV"),
        }
    }

    #[test]
    fn test_relationship_pattern_from_ast() {
        let ast_relationship_pattern = ast::RelationshipPattern {
            name: Some("follows"),
            direction: ast::Direction::Outgoing,
            labels: Some(vec!["FOLLOWS"]),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "since",
                value: ast::Expression::Literal(ast::Literal::Integer(2020)),
            })]),
            variable_length: None,
        };
        let logical_relationship_pattern = RelationshipPattern::from(ast_relationship_pattern);

        assert_eq!(
            logical_relationship_pattern.name,
            Some("follows".to_string())
        );
        assert_eq!(logical_relationship_pattern.direction, Direction::Outgoing);
        assert_eq!(
            logical_relationship_pattern.labels,
            Some(vec!["FOLLOWS".to_string()])
        );
        assert!(logical_relationship_pattern.properties.is_some());

        let properties = logical_relationship_pattern.properties.unwrap();
        assert_eq!(properties.len(), 1);

        match &properties[0] {
            Property::PropertyKV(kv) => {
                assert_eq!(kv.key, "since");
                assert_eq!(kv.value, Literal::Integer(2020));
            }
            _ => panic!("Expected PropertyKV"),
        }
    }

    #[test]
    fn test_connected_pattern_from_ast() {
        let start_node = ast::NodePattern {
            name: Some("user"),
            label: Some("User"),
            properties: None,
        };
        let end_node = ast::NodePattern {
            name: Some("company"),
            label: Some("Company"),
            properties: None,
        };
        let relationship = ast::RelationshipPattern {
            name: Some("works_at"),
            direction: ast::Direction::Outgoing,
            labels: Some(vec!["WORKS_AT"]),
            properties: None,
            variable_length: None,
        };

        let ast_connected_pattern = ast::ConnectedPattern {
            start_node: Rc::new(RefCell::new(start_node)),
            relationship,
            end_node: Rc::new(RefCell::new(end_node)),
        };
        let logical_connected_pattern = ConnectedPattern::from(ast_connected_pattern);

        assert_eq!(
            logical_connected_pattern.start_node.name,
            Some("user".to_string())
        );
        assert_eq!(
            logical_connected_pattern.start_node.label,
            Some("User".to_string())
        );
        assert_eq!(
            logical_connected_pattern.end_node.name,
            Some("company".to_string())
        );
        assert_eq!(
            logical_connected_pattern.end_node.label,
            Some("Company".to_string())
        );
        assert_eq!(
            logical_connected_pattern.relationship.name,
            Some("works_at".to_string())
        );
        assert_eq!(
            logical_connected_pattern.relationship.labels,
            Some(vec!["WORKS_AT".to_string()])
        );
        assert_eq!(
            logical_connected_pattern.relationship.direction,
            Direction::Outgoing
        );
    }

    #[test]
    fn test_path_pattern_node_from_ast() {
        let ast_node = ast::NodePattern {
            name: Some("customer"),
            label: Some("Customer"),
            properties: None,
        };
        let ast_path_pattern = ast::PathPattern::Node(ast_node);
        let logical_path_pattern = PathPattern::from(ast_path_pattern);

        match logical_path_pattern {
            PathPattern::Node(node) => {
                assert_eq!(node.name, Some("customer".to_string()));
                assert_eq!(node.label, Some("Customer".to_string()));
            }
            _ => panic!("Expected Node pattern"),
        }
    }

    #[test]
    fn test_logical_expr_from_expression_variable() {
        // Test star variable
        let ast_star = ast::Expression::Variable("*");
        let logical_star = LogicalExpr::from(ast_star);
        assert_eq!(logical_star, LogicalExpr::Star);

        // Test regular variable
        let ast_var = ast::Expression::Variable("product");
        let logical_var = LogicalExpr::from(ast_var);
        match logical_var {
            LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "product"),
            _ => panic!("Expected TableAlias"),
        }
    }

    #[test]
    fn test_logical_expr_from_expression_list() {
        let ast_list = ast::Expression::List(vec![
            ast::Expression::Literal(ast::Literal::String("admin")),
            ast::Expression::Literal(ast::Literal::String("user")),
            ast::Expression::Literal(ast::Literal::String("guest")),
        ]);
        let logical_list = LogicalExpr::from(ast_list);

        match logical_list {
            LogicalExpr::List(items) => {
                assert_eq!(items.len(), 3);

                match &items[0] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "admin"),
                    _ => panic!("Expected string literal"),
                }
                match &items[1] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "user"),
                    _ => panic!("Expected string literal"),
                }
                match &items[2] {
                    LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "guest"),
                    _ => panic!("Expected string literal"),
                }
            }
            _ => panic!("Expected List"),
        }
    }

    #[test]
    fn test_display_implementations() {
        // Test TableAlias display
        let table_alias = TableAlias("customer".to_string());
        assert_eq!(format!("{}", table_alias), "customer");

        // Test ColumnAlias display
        let column_alias = ColumnAlias("full_name".to_string());
        assert_eq!(format!("{}", column_alias), "full_name");

        // Test Column display
        let column = Column("email_address".to_string());
        assert_eq!(format!("{}", column), "email_address");

        // Test Literal display implementations
        assert_eq!(format!("{}", Literal::Integer(12345)), "12345");
        assert_eq!(format!("{}", Literal::Float(99.99)), "99.99");
        assert_eq!(format!("{}", Literal::Boolean(true)), "true");
        assert_eq!(format!("{}", Literal::Boolean(false)), "false");
        assert_eq!(
            format!("{}", Literal::String("Hello World".to_string())),
            "Hello World"
        );
        assert_eq!(format!("{}", Literal::Null), "null");
    }

    #[test]
    fn test_aggregate_function_classification() {
        let agg_functions = ["count", "min", "max", "avg", "sum", "collect"];

        for func_name in &agg_functions {
            let ast_function_call = ast::FunctionCall {
                name: func_name.to_string(),
                args: vec![ast::Expression::Variable("revenue")],
            };
            let logical_expr = LogicalExpr::from(ast_function_call);

            match logical_expr {
                LogicalExpr::AggregateFnCall(agg_fn) => {
                    assert_eq!(agg_fn.name, *func_name);
                }
                _ => panic!("Expected aggregate function for {}", func_name),
            }
        }

        // Test non-aggregate function
        let ast_scalar_function = ast::FunctionCall {
            name: "substring".to_string(),
            args: vec![
                ast::Expression::Variable("description"),
                ast::Expression::Literal(ast::Literal::Integer(1)),
                ast::Expression::Literal(ast::Literal::Integer(10)),
            ],
        };
        let logical_expr = LogicalExpr::from(ast_scalar_function);

        match logical_expr {
            LogicalExpr::ScalarFnCall(scalar_fn) => {
                assert_eq!(scalar_fn.name, "substring");
                assert_eq!(scalar_fn.args.len(), 3);
            }
            _ => panic!("Expected scalar function"),
        }
    }
}
