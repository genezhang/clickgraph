use std::{cell::RefCell, fmt, rc::Rc};

/// Type of UNION operation
#[derive(Debug, PartialEq, Clone)]
pub enum UnionType {
    /// UNION - removes duplicates
    Distinct,
    /// UNION ALL - keeps duplicates
    All,
}

/// A complete Cypher statement - either a regular query or a standalone procedure call
#[derive(Debug, PartialEq, Clone)]
pub enum CypherStatement<'a> {
    /// Regular query with optional UNION clauses
    Query {
        query: OpenCypherQueryAst<'a>,
        union_clauses: Vec<UnionClause<'a>>,
    },
    /// Standalone procedure call (e.g., CALL db.labels())
    ProcedureCall(StandaloneProcedureCall<'a>),
}

/// A UNION clause combining queries
#[derive(Debug, PartialEq, Clone)]
pub struct UnionClause<'a> {
    pub union_type: UnionType,
    pub query: OpenCypherQueryAst<'a>,
}

/// Standalone procedure call for system/metadata queries
/// Examples: CALL db.labels(), CALL dbms.components()
#[derive(Debug, PartialEq, Clone)]
pub struct StandaloneProcedureCall<'a> {
    /// Procedure name (can include dots, e.g., "db.labels", "dbms.components")
    pub procedure_name: &'a str,
    /// Optional arguments (for procedures that take parameters)
    pub arguments: Vec<Expression<'a>>,
    /// Optional YIELD clause to select specific return fields
    pub yield_items: Option<Vec<&'a str>>,
}

/// Enum representing a reading clause - either MATCH or OPTIONAL MATCH
/// This allows interleaved MATCH and OPTIONAL MATCH clauses in any order
#[derive(Debug, PartialEq, Clone)]
pub enum ReadingClause<'a> {
    Match(MatchClause<'a>),
    OptionalMatch(OptionalMatchClause<'a>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct OpenCypherQueryAst<'a> {
    pub use_clause: Option<UseClause<'a>>,
    pub match_clauses: Vec<MatchClause<'a>>, // Support multiple MATCH clauses in sequence
    pub optional_match_clauses: Vec<OptionalMatchClause<'a>>,
    /// Unified reading clauses that preserve the order of MATCH and OPTIONAL MATCH
    /// When populated, this takes precedence over match_clauses and optional_match_clauses
    pub reading_clauses: Vec<ReadingClause<'a>>,
    pub call_clause: Option<CallClause<'a>>,
    pub unwind_clauses: Vec<UnwindClause<'a>>, // Support multiple UNWIND clauses for cartesian product
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
    pub path_patterns: Vec<(Option<&'a str>, PathPattern<'a>)>, // Vec of (optional path_var, pattern)
    pub where_clause: Option<WhereClause<'a>>, // Optional WHERE clause per MATCH (OpenCypher grammar compliant)
}

#[derive(Debug, PartialEq, Clone)]
pub struct OptionalMatchClause<'a> {
    pub path_patterns: Vec<PathPattern<'a>>,
    pub where_clause: Option<WhereClause<'a>>,
}

/// UNWIND clause: transforms an array/list into individual rows
/// Example: UNWIND [1, 2, 3] AS x
/// Example: UNWIND r.items AS item
#[derive(Debug, PartialEq, Clone)]
pub struct UnwindClause<'a> {
    /// The expression to unwind (must evaluate to an array/list)
    pub expression: Expression<'a>,
    /// The alias for each unwound element
    pub alias: &'a str,
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
    pub distinct: bool,
    pub return_items: Vec<ReturnItem<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReturnItem<'a> {
    pub expression: Expression<'a>,
    pub alias: Option<&'a str>,
    /// Original text of the expression from the query, used as default alias when no explicit AS is provided
    /// This preserves the exact user input including spacing, matching Neo4j's behavior
    pub original_text: Option<&'a str>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WithClause<'a> {
    pub with_items: Vec<WithItem<'a>>,
    /// Optional DISTINCT modifier
    pub distinct: bool,
    /// Optional ORDER BY clause - part of WITH syntax per OpenCypher spec
    pub order_by: Option<OrderByClause<'a>>,
    /// Optional SKIP clause - part of WITH syntax per OpenCypher spec
    pub skip: Option<SkipClause>,
    /// Optional LIMIT clause - part of WITH syntax per OpenCypher spec
    pub limit: Option<LimitClause>,
    /// Optional WHERE clause after WITH - filters the intermediate result
    pub where_clause: Option<WhereClause<'a>>,
    /// Optional subsequent UNWIND clause after WITH (for WITH ... UNWIND chaining)
    pub subsequent_unwind: Option<UnwindClause<'a>>,
    /// Optional subsequent MATCH clause after WITH (for WITH ... MATCH chaining)
    pub subsequent_match: Option<Box<MatchClause<'a>>>,
    /// Optional subsequent OPTIONAL MATCH clauses after WITH
    pub subsequent_optional_matches: Vec<OptionalMatchClause<'a>>,
    /// Optional subsequent WITH clause for chained WITH...MATCH...WITH patterns
    /// This enables: MATCH ... WITH a MATCH ... WITH a, b MATCH ... RETURN ...
    pub subsequent_with: Option<Box<WithClause<'a>>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CallClause<'a> {
    pub procedure_name: &'a str,
    pub arguments: Vec<CallArgument<'a>>,
    /// Optional YIELD clause to select specific return fields
    pub yield_items: Option<Vec<&'a str>>,
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
    pub labels: Option<Vec<&'a str>>, // `Person` in `(a:Person)` or `Person|Post` in `(a:Person|Post)`
    pub properties: Option<Vec<Property<'a>>>, // `{name: "Charlie Sheen"}`
}

impl<'a> NodePattern<'a> {
    /// Helper to get the first label (for single-label compatibility)
    pub fn first_label(&self) -> Option<&'a str> {
        self.labels.as_ref().and_then(|l| l.first()).copied()
    }

    /// Check if node has a specific label
    pub fn has_label(&self, label: &str) -> bool {
        self.labels
            .as_ref()
            .map(|labels| labels.contains(&label))
            .unwrap_or(false)
    }
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
    pub labels: Option<Vec<&'a str>>, // Support multiple labels: [:TYPE1|TYPE2]
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

    /// Create a lower-bounded spec: *2.. becomes min=2, max=None (unbounded)
    pub fn min_only(min: u32) -> Self {
        Self {
            min_hops: Some(min),
            max_hops: None,
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
            // Zero is allowed for shortest path self-loops like *0..
            // but we warn about it since it's unusual
            if min == 0 || max == 0 {
                crate::debug_print!(
                    "Note: Variable-length path with 0 hops matches the same node. \
                     This is typically used with shortest path functions for self-loops."
                );
            }

            // Warn about very large ranges (potential performance issue)
            if max > 100 {
                // Note: This is just a warning, not an error - we still allow it
                crate::debug_print!(
                    "Warning: Variable-length path with maximum {} hops may have performance implications. \
                     Consider using a smaller maximum or adding additional WHERE clause filters.",
                    max
                );
            }
        }

        // Check for zero in unbounded spec
        // Zero is allowed for shortest path self-loops like *0..
        if let Some(min) = self.min_hops {
            if min == 0 {
                crate::debug_print!(
                    "Note: Variable-length path with 0 hops matches the same node. \
                     This is typically used with shortest path functions for self-loops."
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
    RegexMatch,       // =~ (regex match)
    And,
    Or,
    In, // IN [...]
    NotIn,
    // String predicates
    StartsWith, // STARTS WITH
    EndsWith,   // ENDS WITH
    Contains,   // CONTAINS
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
            Operator::RegexMatch => "=~".to_string(),
            Operator::And => "AND".to_string(),
            Operator::Or => "OR".to_string(),
            Operator::In => "IN".to_string(),
            Operator::NotIn => "NOT IN".to_string(),
            Operator::StartsWith => "STARTS WITH".to_string(),
            Operator::EndsWith => "ENDS WITH".to_string(),
            Operator::Contains => "CONTAINS".to_string(),
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
    /// EXISTS subquery expression: EXISTS { (pattern) } or EXISTS { MATCH (pattern) WHERE ... }
    /// Evaluates to true if the pattern matches at least one result
    ExistsExpression(Box<ExistsSubquery<'a>>),
    /// Reduce expression: reduce(acc = init, x IN list | expr)
    /// Folds a list into a single value using an accumulator
    ReduceExp(ReduceExpression<'a>),
    /// Map literal: {key1: value1, key2: value2}
    /// Used in duration({days: 5}), point({x: 1, y: 2}), etc.
    MapLiteral(Vec<(&'a str, Expression<'a>)>),
    /// Label expression: variable:Label
    /// Returns true if the variable has the specified label
    /// Example: message:Comment, n:Person
    LabelExpression {
        variable: &'a str,
        label: &'a str,
    },
    /// Lambda expression: param -> body
    /// Used in ClickHouse array functions like arrayFilter, arrayMap
    /// Example: x -> x > 5, (x, y) -> x + y
    Lambda(LambdaExpression<'a>),
    /// Pattern comprehension: [(pattern) WHERE condition | projection]
    /// Returns a list of projected values from matched patterns
    /// Example: [(user)-[:FOLLOWS]->(follower) WHERE follower.active | follower.name]
    PatternComprehension(PatternComprehension<'a>),
    /// Array subscript: array[index]
    /// Access element at specified index (1-based in Cypher)
    /// Example: labels(n)[1], list[0], [1,2,3][2]
    ArraySubscript {
        array: Box<Expression<'a>>,
        index: Box<Expression<'a>>,
    },
    /// Array slicing: array[from..to]
    /// Extract subarray from index 'from' to 'to' (inclusive, 0-based in Cypher)
    /// Both bounds are optional: [..3], [2..], [..]
    /// Example: list[0..5], collect(n)[..10], [1,2,3,4,5][2..4]
    ArraySlicing {
        array: Box<Expression<'a>>,
        from: Option<Box<Expression<'a>>>,
        to: Option<Box<Expression<'a>>>,
    },
}

/// Lambda expression for ClickHouse array functions
/// Examples:
///   x -> x > 5
///   (x, y) -> x + y
///   elem -> elem.field = 'value'
#[derive(Debug, PartialEq, Clone)]
pub struct LambdaExpression<'a> {
    /// Parameter names (single or multiple)
    pub params: Vec<&'a str>,
    /// Body expression (can reference params)
    pub body: Box<Expression<'a>>,
}

/// EXISTS subquery: checks if a pattern exists
/// Examples:
///   EXISTS { (u)-[:FOLLOWS]->(:User) }
///   EXISTS { MATCH (u)-[:FOLLOWS]->(f) WHERE f.active = true }
#[derive(Debug, PartialEq, Clone)]
pub struct ExistsSubquery<'a> {
    /// The pattern to check for existence
    pub pattern: PathPattern<'a>,
    /// Optional WHERE clause for filtering the pattern
    pub where_clause: Option<Box<WhereClause<'a>>>,
}

/// Reduce expression: aggregates list elements into a single value
/// Syntax: reduce(accumulator = initial, variable IN list | expression)
/// Examples:
///   reduce(total = 0, x IN [1, 2, 3] | total + x) => 6
///   reduce(s = '', name IN names | s + name + ', ') => 'Alice, Bob, '
#[derive(Debug, PartialEq, Clone)]
pub struct ReduceExpression<'a> {
    /// Name of the accumulator variable (e.g., "total")
    pub accumulator: &'a str,
    /// Initial value for the accumulator (e.g., 0)
    pub initial_value: Box<Expression<'a>>,
    /// Iteration variable name (e.g., "x")
    pub variable: &'a str,
    /// List expression to iterate over
    pub list: Box<Expression<'a>>,
    /// Expression to compute for each element (can reference both accumulator and variable)
    pub expression: Box<Expression<'a>>,
}

/// Pattern comprehension: generates a list from pattern matches
/// Syntax: [(pattern) WHERE condition | projection]
/// Examples:
///   [(user)-[:FOLLOWS]->(f) | f.name] => ['Alice', 'Bob', 'Charlie']
///   [(a)-[:KNOWS]->(b) WHERE b.age > 25 | b.name] => ['Dave', 'Eve']
///   [(n)-[r]->(m) | r.weight] => [1.5, 2.0, 3.7]
#[derive(Debug, PartialEq, Clone)]
pub struct PatternComprehension<'a> {
    /// The graph pattern to match (e.g., (user)-[:FOLLOWS]->(follower))
    pub pattern: Box<PathPattern<'a>>,
    /// Optional WHERE clause for filtering matches
    pub where_clause: Option<Box<Expression<'a>>>,
    /// Expression to project for each match (e.g., follower.name)
    pub projection: Box<Expression<'a>>,
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
        if !self.match_clauses.is_empty() {
            for (i, m) in self.match_clauses.iter().enumerate() {
                writeln!(f, "├── MatchClause[{}]: {:#?}", i, m)?;
            }
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
