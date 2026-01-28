//! Logical Expression Types
//!
//! This module defines the core expression types used throughout the query planner.
//! These are intermediate representations between the parsed AST and generated SQL.
//!
//! # Module Organization
//!
//! - **mod.rs** (this file): Type definitions and Display implementations
//! - **ast_conversion.rs**: `From`/`TryFrom` implementations for AST conversion
//! - **expression_rewriter.rs**: Property mapping and expression transformation
//! - **combinators.rs**: Helper functions for combining predicates (AND/OR)
//! - **visitors.rs**: Visitor pattern for expression traversal
//! - **errors.rs**: Error types

use crate::query_planner::logical_plan::LogicalPlan;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

// Import serde_arc module for serialization
use crate::utils::serde_arc;

// =============================================================================
// Sub-modules
// =============================================================================

mod ast_conversion; // AST to LogicalExpr conversions (From/TryFrom impls)
pub mod combinators;
pub mod errors;
pub mod expression_rewriter;
pub mod visitors;

/// Type of graph entity (node or relationship).
/// Used in CteEntityRef to indicate what kind of entity is being referenced.
#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum EntityType {
    Node,
    Relationship,
}

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

    /// Reduce expression: fold list into single value
    /// reduce(acc = init, x IN list | expr)
    ReduceExpr(ReduceExpr),

    /// Map literal: {key1: value1, key2: value2}
    /// Used in duration({days: 5}), point({x: 1, y: 2}), etc.
    MapLiteral(Vec<(String, LogicalExpr)>),

    /// Label expression: variable:Label
    /// Returns true if the variable has the specified label
    /// Example: message:Comment evaluates to boolean
    LabelExpression {
        variable: String,
        label: String,
    },

    /// Pattern count: size((n)-[:REL]->())
    /// Counts the number of matches for a relationship pattern
    /// Generates a correlated COUNT(*) subquery
    PatternCount(PatternCount),

    /// Lambda expression: param -> body
    /// Used in ClickHouse array functions
    /// Example: x -> x > 5
    Lambda(LambdaExpr),

    /// Pattern comprehension: [(pattern) WHERE condition | projection]
    /// Returns a list of projected values from matched patterns
    /// Will be rewritten to OPTIONAL MATCH + collect() during query planning
    PatternComprehension(PatternComprehensionExpr),

    /// Array subscript: array[index]
    /// Access element at specified index (1-based in Cypher, converted to 0-based for ClickHouse)
    /// Example: labels(n)[1], list[0], [1,2,3][2]
    ArraySubscript {
        array: Box<LogicalExpr>,
        index: Box<LogicalExpr>,
    },

    /// Array slicing: array[from..to]
    /// Extract subarray from index 'from' to 'to' (0-based, inclusive in Cypher)
    /// Both bounds are optional: [..3], [2..], [..]
    /// Example: list[0..5], collect(n)[..10], [1,2,3,4,5][2..4]
    ArraySlicing {
        array: Box<LogicalExpr>,
        from: Option<Box<LogicalExpr>>,
        to: Option<Box<LogicalExpr>>,
    },

    /// CTE Entity Reference: A node or relationship that was exported through a WITH clause.
    /// Unlike TableAlias which refers to entities from MATCH, CteEntityRef knows:
    /// - Which CTE contains the entity's data
    /// - The original alias and entity type (Node/Relationship)
    /// - What columns are available (prefixed in the CTE)
    ///
    /// Example: MATCH (a:User) WITH a RETURN a
    /// In RETURN, 'a' becomes CteEntityRef pointing to the CTE that contains a's columns.
    /// The renderer expands this to SELECT all a_* columns from the CTE.
    CteEntityRef(CteEntityRef),
}

/// Pattern count for size() on patterns
/// Represents size((n)-[:REL]->()) which counts pattern matches
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PatternCount {
    /// The pattern to count
    pub pattern: PathPattern,
}

/// Reduce expression for folding a list into a single value
/// Syntax: reduce(accumulator = initial, variable IN list | expression)
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ReduceExpr {
    /// Name of the accumulator variable
    pub accumulator: String,
    /// Initial value for the accumulator
    pub initial_value: Box<LogicalExpr>,
    /// Iteration variable name
    pub variable: String,
    /// List to iterate over
    pub list: Box<LogicalExpr>,
    /// Expression evaluated for each element
    pub expression: Box<LogicalExpr>,
}

/// EXISTS subquery for checking pattern existence
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ExistsSubquery {
    /// The logical plan representing the EXISTS pattern
    #[serde(with = "serde_arc")]
    pub subplan: Arc<LogicalPlan>,
}

/// Lambda expression for ClickHouse array functions
/// Example: x -> x > 5, (x, y) -> x + y
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LambdaExpr {
    /// Parameter names (one or more)
    pub params: Vec<String>,
    /// Body expression (can reference params)
    pub body: Box<LogicalExpr>,
}

/// Pattern comprehension: returns list of values from pattern matches
/// Example: [(user)-[:FOLLOWS]->(follower) WHERE follower.active | follower.name]
/// This will be rewritten during query planning to:
///   OPTIONAL MATCH (user)-[:FOLLOWS]->(follower) WHERE follower.active
///   RETURN collect(follower.name)
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PatternComprehensionExpr {
    /// The graph pattern to match
    pub pattern: PathPattern,
    /// Optional WHERE clause for filtering
    pub where_clause: Option<Box<LogicalExpr>>,
    /// Expression to project for each match
    pub projection: Box<LogicalExpr>,
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

/// Reference to a node or relationship that was exported through a CTE (WITH clause).
///
/// When a node/relationship passes through WITH, its properties become prefixed columns
/// in a CTE. CteEntityRef captures this information so the renderer can:
/// 1. Expand bare alias references (e.g., `RETURN a`) to all entity columns
/// 2. Resolve property access (e.g., `a.name`) to the correct CTE column
///
/// Example:
/// ```text
/// MATCH (a:User) WITH a RETURN a
/// ```
/// After WITH, 'a' becomes:
/// ```text
/// CteEntityRef {
///     cte_name: "with_a_cte_1",
///     alias: "a",
///     entity_type: Node,
///     columns: ["a_user_id", "a_name", "a_email", ...]
/// }
/// ```
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CteEntityRef {
    /// Name of the CTE containing the entity's data (e.g., "with_a_cte_1")
    pub cte_name: String,
    /// Original alias of the entity (e.g., "a")  
    pub alias: String,
    /// Type of entity: Node or Relationship
    pub entity_type: EntityType,
    /// List of column names available in the CTE (prefixed with alias_)
    /// e.g., ["a_user_id", "a_name", "a_email"]
    pub columns: Vec<String>,
}

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
    RegexMatch, // =~ (regex match)
    And,
    Or,
    In,
    NotIn,
    StartsWith, // STARTS WITH
    EndsWith,   // ENDS WITH
    Contains,   // CONTAINS
    Not,
    Distinct,
    IsNull,
    IsNotNull,
}

impl Operator {
    /// Returns true if this operator produces a boolean result and can be extracted as a filter.
    /// Arithmetic operators (Addition, Subtraction, etc.) return false because they produce
    /// numeric results and should not be extracted as standalone filters.
    pub fn is_filter_extractable(&self) -> bool {
        match self {
            // Arithmetic operators - NOT filter extractable
            Operator::Addition
            | Operator::Subtraction
            | Operator::Multiplication
            | Operator::Division
            | Operator::ModuloDivision
            | Operator::Exponentiation => false,

            // Comparison and boolean operators - filter extractable
            Operator::Equal
            | Operator::NotEqual
            | Operator::LessThan
            | Operator::GreaterThan
            | Operator::LessThanEqual
            | Operator::GreaterThanEqual
            | Operator::RegexMatch
            | Operator::And
            | Operator::Or
            | Operator::In
            | Operator::NotIn
            | Operator::StartsWith
            | Operator::EndsWith
            | Operator::Contains
            | Operator::Not
            | Operator::Distinct
            | Operator::IsNull
            | Operator::IsNotNull => true,
        }
    }
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
    pub label: Option<String>, // Primary label - kept for backward compatibility
    pub labels: Option<Vec<String>>, // Multi-label support (GraphRAG polymorphic nodes)
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
    pub value: LogicalExpr,
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

// =============================================================================
// Display Implementations
// =============================================================================

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
        let logical_operator_app = OperatorApplication::try_from(ast_operator_app).unwrap();

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
        let logical_expr = LogicalExpr::try_from(ast_function_call).unwrap();

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
        let logical_expr = LogicalExpr::try_from(ast_function_call).unwrap();

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
            labels: Some(vec!["Person"]),
            properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
                key: "department",
                value: ast::Expression::Literal(ast::Literal::String("Engineering")),
            })]),
        };
        let logical_node_pattern = NodePattern::try_from(ast_node_pattern).unwrap();

        assert_eq!(logical_node_pattern.name, Some("employee".to_string()));
        assert_eq!(logical_node_pattern.label, Some("Person".to_string()));
        assert!(logical_node_pattern.properties.is_some());

        let properties = logical_node_pattern.properties.unwrap();
        assert_eq!(properties.len(), 1);

        match &properties[0] {
            Property::PropertyKV(kv) => {
                assert_eq!(kv.key, "department");
                assert_eq!(
                    kv.value,
                    LogicalExpr::Literal(Literal::String("Engineering".to_string()))
                );
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
        let logical_relationship_pattern =
            RelationshipPattern::try_from(ast_relationship_pattern).unwrap();

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
                assert_eq!(kv.value, LogicalExpr::Literal(Literal::Integer(2020)));
            }
            _ => panic!("Expected PropertyKV"),
        }
    }

    #[test]
    fn test_connected_pattern_from_ast() {
        let start_node = ast::NodePattern {
            name: Some("user"),
            labels: Some(vec!["User"]),
            properties: None,
        };
        let end_node = ast::NodePattern {
            name: Some("company"),
            labels: Some(vec!["Company"]),
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
        let logical_connected_pattern = ConnectedPattern::try_from(ast_connected_pattern).unwrap();

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
            labels: Some(vec!["Customer"]),
            properties: None,
        };
        let ast_path_pattern = ast::PathPattern::Node(ast_node);
        let logical_path_pattern = PathPattern::try_from(ast_path_pattern).unwrap();

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
        let logical_star = LogicalExpr::try_from(ast_star).unwrap();
        assert_eq!(logical_star, LogicalExpr::Star);

        // Test regular variable
        let ast_var = ast::Expression::Variable("product");
        let logical_var = LogicalExpr::try_from(ast_var).unwrap();
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
        let logical_list = LogicalExpr::try_from(ast_list).unwrap();

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
            let logical_expr = LogicalExpr::try_from(ast_function_call).unwrap();

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
        let logical_expr = LogicalExpr::try_from(ast_scalar_function).unwrap();

        match logical_expr {
            LogicalExpr::ScalarFnCall(scalar_fn) => {
                assert_eq!(scalar_fn.name, "substring");
                assert_eq!(scalar_fn.args.len(), 3);
            }
            _ => panic!("Expected scalar function"),
        }
    }

    #[test]
    fn test_ch_aggregate_function_classification() {
        // Test ch. prefixed ClickHouse aggregate functions are classified as aggregates
        let ch_agg_functions = [
            "ch.uniq",
            "ch.quantile",
            "ch.topK",
            "ch.groupArray",
            "ch.argMax",
        ];

        for func_name in &ch_agg_functions {
            let ast_function_call = ast::FunctionCall {
                name: func_name.to_string(),
                args: vec![ast::Expression::Variable("user_id")],
            };
            let logical_expr = LogicalExpr::try_from(ast_function_call).unwrap();

            match logical_expr {
                LogicalExpr::AggregateFnCall(agg_fn) => {
                    assert_eq!(agg_fn.name, *func_name);
                }
                _ => panic!("Expected aggregate function for {}", func_name),
            }
        }

        // Test ch. prefixed scalar functions remain scalars
        let ch_scalar_functions = ["ch.cityHash64", "ch.JSONExtract", "ch.upper"];

        for func_name in &ch_scalar_functions {
            let ast_function_call = ast::FunctionCall {
                name: func_name.to_string(),
                args: vec![ast::Expression::Variable("email")],
            };
            let logical_expr = LogicalExpr::try_from(ast_function_call).unwrap();

            match logical_expr {
                LogicalExpr::ScalarFnCall(scalar_fn) => {
                    assert_eq!(scalar_fn.name, *func_name);
                }
                _ => panic!("Expected scalar function for {}", func_name),
            }
        }
    }
}

impl LogicalExpr {
    /// Check if this expression contains correlated subqueries (NOT PathPattern or EXISTS)
    /// Such expressions must go in WHERE clause, not JOIN ON (ClickHouse limitation)
    /// Returns true for patterns like: NOT (a)-[:REL]-(b) or EXISTS((a)-[:REL]-(b))
    pub fn contains_not_path_pattern(&self) -> bool {
        match self {
            LogicalExpr::OperatorApplicationExp(op_app) => {
                // Check if this is a NOT operator
                if op_app.operator == Operator::Not {
                    // Check if any operand is a PathPattern
                    for operand in &op_app.operands {
                        if matches!(operand, LogicalExpr::PathPattern(_)) {
                            return true;
                        }
                    }
                }
                // Recursively check operands for nested NOT PathPattern or EXISTS
                for operand in &op_app.operands {
                    if operand.contains_not_path_pattern() {
                        return true;
                    }
                }
                false
            }
            // EXISTS subquery also generates correlated subquery
            LogicalExpr::ExistsSubquery(_) => true,
            // Pattern count like size((n)-[:REL]->()) also generates correlated subquery
            LogicalExpr::PatternCount(_) => true,
            _ => false,
        }
    }
}

#[test]
fn test_pattern_comprehension_error_instead_of_panic() {
    use crate::open_cypher_parser;

    // Test that PatternComprehension now returns an error instead of panicking
    let pattern_comprehension = open_cypher_parser::ast::Expression::PatternComprehension(
        open_cypher_parser::ast::PatternComprehension {
            pattern: Box::new(open_cypher_parser::ast::PathPattern::Node(
                open_cypher_parser::ast::NodePattern {
                    name: Some("n"),
                    labels: None,
                    properties: None,
                },
            )),
            where_clause: None,
            projection: Box::new(open_cypher_parser::ast::Expression::Variable("n")),
        },
    );

    // This should return an error, not panic
    match LogicalExpr::try_from(pattern_comprehension) {
        Ok(_) => panic!("PatternComprehension should have failed!"),
        Err(errors::LogicalExprError::PatternComprehensionNotRewritten) => {
            // Success - we got the expected error instead of a panic
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}
