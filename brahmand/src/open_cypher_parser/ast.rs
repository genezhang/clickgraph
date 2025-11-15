#![allow(dead_code)]

use std::{cell::RefCell, fmt, rc::Rc};

#[derive(Debug, PartialEq, Clone)]
pub struct OpenCypherQueryAst<'a> {
    pub use_clause: Option<UseClause<'a>>,
    pub match_clause: Option<MatchClause<'a>>,
    pub optional_match_clauses: Vec<OptionalMatchClause<'a>>,
    pub call_clause: Option<CallClause<'a>>,
    pub with_clause: Option<WithClause<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
    pub create_clause: Option<CreateClause<'a>>,
    pub set_clause: Option<SetClause<'a>>,
    pub remove_clause: Option<RemoveClause<'a>>,
    pub delete_clause: Option<DeleteClause<'a>>,
    pub return_clause: Option<ReturnClause<'a>>,
    pub order_by_clause: Option<OrderByClause<'a>>,
    pub skip_clause: Option<SkipClause>,
    pub limit_clause: Option<LimitClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UseClause<'a> {
    pub database_name: &'a str,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MatchClause<'a> {
    pub path_patterns: Vec<PathPattern<'a>>,
    pub path_variable: Option<&'a str>,  // For: MATCH p = (pattern)
}

#[derive(Debug, PartialEq, Clone)]
pub struct OptionalMatchClause<'a> {
    pub path_patterns: Vec<PathPattern<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateClause<'a> {
    pub path_patterns: Vec<PathPattern<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SetClause<'a> {
    pub set_items: Vec<OperatorApplication<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RemoveClause<'a> {
    pub remove_items: Vec<PropertyAccess<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteClause<'a> {
    pub is_detach: bool,
    pub delete_items: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause<'a> {
    pub conditions: Expression<'a>, //OperatorApplication<'a>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReturnClause<'a> {
    pub return_items: Vec<ReturnItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReturnItem<'a> {
    pub expression: Expression<'a>,
    pub alias: Option<&'a str>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WithClause<'a> {
    pub with_items: Vec<WithItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CallClause<'a> {
    pub procedure_name: &'a str,
    pub arguments: Vec<CallArgument<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CallArgument<'a> {
    pub name: &'a str,
    pub value: Expression<'a>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WithItem<'a> {
    pub expression: Expression<'a>,
    pub alias: Option<&'a str>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByClause<'a> {
    pub order_by_items: Vec<OrderByItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByItem<'a> {
    pub expression: Expression<'a>,
    pub order: OrerByOrder,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrerByOrder {
    Asc,
    Desc,
}

impl From<OrerByOrder> for String {
    fn from(value: OrerByOrder) -> String {
        match value {
            OrerByOrder::Asc => "ASC".to_string(),
            OrerByOrder::Desc => "DESC".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SkipClause {
    pub skip_item: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LimitClause {
    pub limit_item: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub enum PathPattern<'a> {
    Node(NodePattern<'a>),                       //  Standalone nodes `(a)`
    ConnectedPattern(Vec<ConnectedPattern<'a>>), // Nodes with relationships `(a)-[:REL]->(b)`
    ShortestPath(Box<PathPattern<'a>>),          // shortestPath((a)-[*]-(b))
    AllShortestPaths(Box<PathPattern<'a>>),      // allShortestPaths((a)-[*]-(b))
}

#[derive(Debug, PartialEq, Clone)]
pub struct NodePattern<'a> {
    pub name: Option<&'a str>,                 // `a` in `(a:Person)`
    pub label: Option<&'a str>,                // `Person` in `(a:Person)`
    pub properties: Option<Vec<Property<'a>>>, // `{name: "Charlie Sheen"}`
}

#[derive(Debug, PartialEq, Clone)]
pub enum Property<'a> {
    PropertyKV(PropertyKVPair<'a>),
    Param(&'a str),
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyKVPair<'a> {
    pub key: &'a str,
    pub value: Expression<'a>,
}

// #[derive(Debug, PartialEq, Clone)]
// pub struct ConnectedPattern<'a> {
//     pub start_node: &'a NodePattern<'a>,           // `(a)`
//     pub relationship: RelationshipPattern<'a>, // `-[:REL]->`
//     pub end_node: &'a NodePattern<'a>,             // `(b)`
// }

#[derive(Debug, PartialEq, Clone)]
pub struct ConnectedPattern<'a> {
    pub start_node: Rc<RefCell<NodePattern<'a>>>,
    pub relationship: RelationshipPattern<'a>,
    pub end_node: Rc<RefCell<NodePattern<'a>>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct RelationshipPattern<'a> {
    pub name: Option<&'a str>,
    pub direction: Direction,
    pub labels: Option<Vec<&'a str>>,  // Support multiple labels: [:TYPE1|TYPE2]
    pub properties: Option<Vec<Property<'a>>>,
    pub variable_length: Option<VariableLengthSpec>,
}

/// Represents variable-length path specifications like *1..3, *..5, *2, *
#[derive(Debug, PartialEq, Clone)]
pub struct VariableLengthSpec {
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

impl VariableLengthSpec {
    /// Create a fixed-length spec: *2 becomes min=2, max=2
    pub fn fixed(hops: u32) -> Self {
        Self {
            min_hops: Some(hops),
            max_hops: Some(hops),
        }
    }

    /// Create a range spec: *1..3 becomes min=1, max=3
    pub fn range(min: u32, max: u32) -> Self {
        Self {
            min_hops: Some(min),
            max_hops: Some(max),
        }
    }

    /// Create an upper-bounded spec: *..5 becomes min=1, max=5
    pub fn max_only(max: u32) -> Self {
        Self {
            min_hops: Some(1),
            max_hops: Some(max),
        }
    }

    /// Create an unbounded spec: * becomes min=1, max=None (unlimited)
    pub fn unbounded() -> Self {
        Self {
            min_hops: Some(1),
            max_hops: None,
        }
    }

    /// Check if this is a fixed-length relationship (single hop)
    pub fn is_single_hop(&self) -> bool {
        matches!(
            (self.min_hops, self.max_hops),
            (Some(1), Some(1)) | (None, None)
        )
    }

    /// Get effective minimum hops (defaults to 1)
    pub fn effective_min_hops(&self) -> u32 {
        self.min_hops.unwrap_or(1)
    }

    /// Check if there's an upper bound
    pub fn has_max_bound(&self) -> bool {
        self.max_hops.is_some()
    }
    
    /// Validate the variable-length specification
    /// Returns Ok(()) if valid, Err with descriptive message if invalid
    pub fn validate(&self) -> Result<(), String> {
        // Check for invalid range where min > max
        if let (Some(min), Some(max)) = (self.min_hops, self.max_hops) {
            if min > max {
                return Err(format!(
                    "Invalid variable-length range: minimum hops ({}) cannot be greater than maximum hops ({}). \
                     Use *{}..{} instead of *{}..{}.",
                    min, max, max, min, min, max
                ));
            }
            
            // Check for zero in range (special case - 0 hops means same node)
            if min == 0 || max == 0 {
                return Err(
                    "Invalid variable-length range: hop count cannot be 0. \
                     Variable-length paths must have at least 1 hop. \
                     If you want to match the same node, use a simple node pattern like (n) instead of a relationship pattern."
                        .to_string()
                );
            }
            
            // Warn about very large ranges (potential performance issue)
            if max > 100 {
                // Note: This is just a warning, not an error - we still allow it
                eprintln!(
                    "Warning: Variable-length path with maximum {} hops may have performance implications. \
                     Consider using a smaller maximum or adding additional WHERE clause filters.",
                    max
                );
            }
        }
        
        // Check for zero in unbounded spec
        if let Some(min) = self.min_hops {
            if min == 0 {
                return Err(
                    "Invalid variable-length range: hop count cannot be 0. \
                     Variable-length paths must have at least 1 hop."
                        .to_string()
                );
            }
        }
        
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Direction {
    Incoming, // `<-`
    Outgoing, // `->`
    Either,   // `-`
}

impl From<Direction> for String {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Incoming => "incoming".to_string(),
            Direction::Outgoing | Direction::Either => "outgoing".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Literal<'a> {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(&'a str),
    Null,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Operator {
    // binary
    Addition,         // +
    Subtraction,      // -
    Multiplication,   // +
    Division,         // /
    ModuloDivision,   // %
    Exponentiation,   // ^
    Equal,            // =
    NotEqual,         // <>
    LessThan,         // <
    GreaterThan,      // >
    LessThanEqual,    // <=
    GreaterThanEqual, // >=
    And,
    Or,
    In, // IN [...]
    NotIn,
    // unary
    Not,
    Distinct, // e.g distinct name
    // post fix
    IsNull,    // e.g. city IS NULL
    IsNotNull, // e.g. city IS NOT NULL
}

impl From<Operator> for String {
    fn from(value: Operator) -> Self {
        match value {
            Operator::Addition => "+".to_string(),
            Operator::Subtraction => "-".to_string(),
            Operator::Multiplication => "*".to_string(),
            Operator::Division => "/".to_string(),
            Operator::ModuloDivision => "%".to_string(),
            Operator::Exponentiation => "^".to_string(),
            Operator::Equal => "=".to_string(),
            Operator::NotEqual => "!=".to_string(),
            Operator::LessThan => "<".to_string(),
            Operator::GreaterThan => ">".to_string(),
            Operator::LessThanEqual => "<=".to_string(),
            Operator::GreaterThanEqual => ">=".to_string(),
            Operator::And => "AND".to_string(),
            Operator::Or => "OR".to_string(),
            Operator::In => "IN".to_string(),
            Operator::NotIn => "NOT IN".to_string(),
            Operator::Not => "NOT".to_string(),
            Operator::Distinct => "DISTINCT".to_string(),
            Operator::IsNull => "IS NULL".to_string(),
            Operator::IsNotNull => "IS NOT NULL".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct OperatorApplication<'a> {
    pub operator: Operator,
    pub operands: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyAccess<'a> {
    pub base: &'a str,
    pub key: &'a str,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FunctionCall<'a> {
    // pub name: &'a str,
    pub name: String,
    pub args: Vec<Expression<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Case<'a> {
    /// Expression for simple CASE (CASE x WHEN ...), None for searched CASE
    pub expr: Option<Box<Expression<'a>>>,
    /// WHEN conditions and THEN expressions
    pub when_then: Vec<(Expression<'a>, Expression<'a>)>,
    /// Optional ELSE expression
    pub else_expr: Option<Box<Expression<'a>>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression<'a> {
    /// A literal, such as a number, string, boolean, or null.
    Literal(Literal<'a>),

    /// A variable (e.g. n, x, or even backtick-quoted names).
    Variable(&'a str),

    /// A parameter, such as `$param` or `$0`.
    Parameter(&'a str),

    // A list literal: a vector of expressions.
    List(Vec<Expression<'a>>),

    // A function call, e.g. length(p) or nodes(p).
    FunctionCallExp(FunctionCall<'a>),

    // Property access. In Cypher you have both static and dynamic property accesses.
    // This variant uses a boxed base expression and a boxed key expression.
    PropertyAccessExp(PropertyAccess<'a>),

    // An operator application, e.g. 1 + 2 or 3 < 4.
    // The operator itself could be another enum.
    OperatorApplicationExp(OperatorApplication<'a>),

    // A path-pattern, for instance: (a)-[]->()<-[]-(b)
    PathPattern(PathPattern<'a>),
    /// A CASE expression.
    /// `expr` is used for the simple CASE (e.g. CASE x WHEN ...), and if absent, it's the searched CASE.
    Case(Case<'a>),
}

impl fmt::Display for Expression<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for OpenCypherQueryAst<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "OpenCypherQueryAst")?;
        if let Some(ref u) = self.use_clause {
            writeln!(f, "├── UseClause: {:#?}", u)?;
        }
        if let Some(ref m) = self.match_clause {
            writeln!(f, "├── MatchClause: {:#?}", m)?;
        }
        if !self.optional_match_clauses.is_empty() {
            for (i, opt_match) in self.optional_match_clauses.iter().enumerate() {
                writeln!(f, "├── OptionalMatchClause[{}]: {:#?}", i, opt_match)?;
            }
        }
        if let Some(ref w) = self.with_clause {
            writeln!(f, "├── WithClause: {:#?}", w)?;
        }
        if let Some(ref w) = self.where_clause {
            writeln!(f, "├── WhereClause: {:#?}", w)?;
        }
        if let Some(ref c) = self.create_clause {
            writeln!(f, "├── CreateClause: {:#?}", c)?;
        }
        if let Some(ref s) = self.set_clause {
            writeln!(f, "├── SetClause: {:#?}", s)?;
        }
        if let Some(ref r) = self.remove_clause {
            writeln!(f, "├── RemoveClause: {:#?}", r)?;
        }
        if let Some(ref d) = self.delete_clause {
            writeln!(f, "├── DeleteClause: {:#?}", d)?;
        }
        if let Some(ref r) = self.return_clause {
            writeln!(f, "├── ReturnClause: {:#?}", r)?;
        }
        if let Some(ref o) = self.order_by_clause {
            writeln!(f, "├── OrderByClause: {:#?}", o)?;
        }
        if let Some(ref s) = self.skip_clause {
            writeln!(f, "├── SkipClause: {:#?}", s)?;
        }
        if let Some(ref l) = self.limit_clause {
            writeln!(f, "└── LimitClause: {:#?}", l)?;
        }
        Ok(())
    }
}
