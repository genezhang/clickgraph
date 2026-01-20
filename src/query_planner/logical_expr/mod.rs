use crate::{
    clickhouse_query_generator::{
        is_ch_passthrough_aggregate, CH_AGG_PREFIX, CH_PASSTHROUGH_PREFIX,
    },
    open_cypher_parser::{self},
    query_planner::logical_plan::LogicalPlan,
};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

// Import serde_arc module for serialization
#[path = "../../utils/serde_arc.rs"]
mod serde_arc;

pub mod errors;

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
            open_cypher_parser::ast::PathPattern::ShortestPath(inner) => {
                // Recursively convert the inner pattern and wrap it
                Ok(PathPattern::ShortestPath(Box::new(PathPattern::try_from(
                    *inner,
                )?)))
            }
            open_cypher_parser::ast::PathPattern::AllShortestPaths(inner) => {
                // Recursively convert the inner pattern and wrap it
                Ok(PathPattern::AllShortestPaths(Box::new(
                    PathPattern::try_from(*inner)?,
                )))
            }
        }
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::NodePattern<'a>> for NodePattern {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::NodePattern<'a>) -> Result<Self, Self::Error> {
        // Convert Vec<&str> to Vec<String>
        let labels_vec = value.labels.map(|ls| {
            ls.into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        });
        // Set label to first element for backward compatibility
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

impl<'a> TryFrom<open_cypher_parser::ast::ExistsSubquery<'a>> for ExistsSubquery {
    type Error = errors::LogicalExprError;

    fn try_from(exists: open_cypher_parser::ast::ExistsSubquery<'a>) -> Result<Self, Self::Error> {
        use crate::query_planner::logical_plan::{Filter, GraphNode, GraphRel, LogicalPlan};
        use open_cypher_parser::ast::PathPattern as AstPathPattern;

        // Convert the pattern to a logical plan structure
        // The EXISTS pattern gets converted to a subplan that can be checked for existence
        let pattern = exists.pattern;

        // Build the logical plan from the pattern based on its type
        let base_plan = match pattern {
            AstPathPattern::Node(node) => {
                // Single node pattern - use Empty for now (will be resolved during planning)
                Arc::new(LogicalPlan::GraphNode(GraphNode {
                    input: Arc::new(LogicalPlan::Empty),
                    alias: node.name.unwrap_or("").to_string(),
                    label: node.first_label().map(|s| s.to_string()),
                    is_denormalized: false,
                    projected_columns: None,
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
                // For shortest path patterns, recursively convert the inner pattern
                let inner_exists = open_cypher_parser::ast::ExistsSubquery {
                    pattern: *inner,
                    where_clause: None,
                };
                return ExistsSubquery::try_from(inner_exists);
            }
        };

        // If there's a WHERE clause, add a filter
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
                    // TODO revisit this
                    // LogicalExpr::Variable(s.to_string())
                    Ok(LogicalExpr::TableAlias(TableAlias(s.to_string())))
                }
            }
            Expression::Parameter(s) => Ok(LogicalExpr::Parameter(s.to_string())),
            Expression::List(exprs) => {
                let logical_exprs = exprs
                    .into_iter()
                    .map(|e| Self::try_from(e))
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
            Expression::ExistsExpression(exists) => {
                // Convert the EXISTS pattern to a logical plan
                // The pattern needs to be converted to a scan + filter structure
                Ok(LogicalExpr::ExistsSubquery(ExistsSubquery::try_from(
                    *exists,
                )?))
            }
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
            Expression::PatternComprehension(pc) => {
                // Pattern comprehensions should be rewritten during query planning
                // before reaching this point. If we get here, it's a bug.
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
