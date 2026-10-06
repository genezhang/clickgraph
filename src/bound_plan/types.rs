//! Types of the bound plan (P-4c S3, `docs/design/EXPLICIT_SCOPE.md` §4.3–4.5).
//!
//! Every variable occurrence the binder resolves becomes a [`VarId`]: one per
//! binding, unique in the statement. Expressions in the bound plan are
//! [`LogicalExpr`]s in which every variable has been renamed to its
//! [`VarId::name`] (`v1`, `v2`, ...). Because every name in a bound expression
//! is generated, a name IS its binding: no lookup after binding can confuse
//! two variables that the query spelled the same (a re-bound name after WITH,
//! a shadowing comprehension variable). The user's spelling is kept in the
//! binding table for result column names.

use std::collections::BTreeSet;

use crate::query_planner::logical_expr::LogicalExpr;

/// A binding: one variable, as introduced by one clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VarId(pub u32);

impl VarId {
    /// The generated name every reference to this binding is renamed to.
    pub fn name(self) -> String {
        format!("v{}", self.0)
    }
}

impl std::fmt::Display for VarId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BindingKind {
    /// A node. `labels` is the set of labels the node can have, after label
    /// inference (§4.7); an empty set means the pattern cannot match.
    Node { labels: BTreeSet<String> },
    /// A relationship; with `length` it is a variable-length relationship,
    /// which binds a LIST of relationships. An empty `types` set means no
    /// relationship can match; for a segment with minimum length 0 the
    /// zero-hop match (end = start) still stands.
    Rel {
        types: BTreeSet<String>,
        length: Option<(u32, Option<u32>)>,
    },
    /// A path variable (`p = ...`).
    Path,
    /// Any other value (a projected expression, an UNWIND element, a
    /// comprehension or reduce variable).
    Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BindingSource {
    /// Introduced by the pattern of the clause with this index.
    Pattern { clause: usize },
    /// Introduced by a WITH / RETURN projection; `of` is the binding a bare
    /// variable item passes through (`WITH a` / `WITH a AS b`).
    Projection { clause: usize, of: Option<VarId> },
    /// Introduced by UNWIND.
    Unwind { clause: usize },
    /// A comprehension, reduce or lambda variable (local to an expression).
    Local,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    pub id: VarId,
    /// The user's name; `None` for an anonymous pattern element.
    pub name: Option<String>,
    pub kind: BindingKind,
    /// Introduced by OPTIONAL MATCH, or carried from such a binding.
    pub nullable: bool,
    pub source: BindingSource,
}

/// The variables visible at a point of a query: user name -> binding, in the
/// order they became visible (the order `*` expands to).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Scope {
    entries: Vec<(String, VarId)>,
}

impl Scope {
    pub fn lookup(&self, name: &str) -> Option<VarId> {
        self.entries
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, v)| *v)
    }
    pub fn insert(&mut self, name: &str, var: VarId) {
        if let Some(e) = self.entries.iter_mut().find(|(n, _)| n == name) {
            e.1 = var;
        } else {
            self.entries.push((name.to_string(), var));
        }
    }
    pub fn entries(&self) -> &[(String, VarId)] {
        &self.entries
    }
}

/// A MATCH or OPTIONAL MATCH pattern: its comma-separated parts.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundPattern {
    pub parts: Vec<PatternPart>,
}

/// One path of a pattern: `nodes[i] -rels[i]- nodes[i+1]`.
#[derive(Debug, Clone, PartialEq)]
pub struct PatternPart {
    pub path_var: Option<VarId>,
    pub shortest: Option<ShortestMode>,
    pub nodes: Vec<PatNode>,
    pub rels: Vec<PatRel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortestMode {
    Shortest,
    AllShortest,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatNode {
    pub var: VarId,
    /// Labels written on this occurrence (`(a:User|Post)`: alternatives).
    pub labels: Vec<String>,
    /// Inline property map, values bound (`{name: $n}`).
    pub props: Vec<(String, LogicalExpr)>,
    /// The variable was already bound before this clause (an identity tie to
    /// the input).
    pub bound_before: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelDirection {
    /// `(a)-[r]->(b)`: r goes from the left node to the right node.
    Right,
    /// `(a)<-[r]-(b)`.
    Left,
    /// `(a)-[r]-(b)`.
    Either,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatRel {
    pub var: VarId,
    pub types: Vec<String>,
    pub direction: RelDirection,
    pub length: Option<(u32, Option<u32>)>,
    pub props: Vec<(String, LogicalExpr)>,
    pub bound_before: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: LogicalExpr,
    pub descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionKind {
    With,
    Return,
}

/// One projected column.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjItem {
    /// The binding this column becomes in the output scope.
    pub var: VarId,
    /// The column name (the alias, the variable, or the expression text).
    pub name: String,
    /// Bound in the INPUT scope.
    pub expr: LogicalExpr,
    /// Contains an aggregate function.
    pub aggregate: bool,
}

/// WITH / RETURN: project (and aggregate), then sort, skip, limit, filter.
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    pub kind: ProjectionKind,
    pub distinct: bool,
    pub items: Vec<ProjItem>,
    /// Bound in the projected scope extended by the input scope (when the
    /// projection neither aggregates nor is DISTINCT); references to a
    /// projected expression are rewritten to its item's variable.
    pub order_by: Vec<SortKey>,
    pub skip: Option<i64>,
    pub limit: Option<i64>,
    /// A WITH's WHERE, evaluated after the LIMIT (#1311). Same scope as
    /// `order_by`.
    pub filter: Option<LogicalExpr>,
}

impl Projection {
    pub fn aggregates(&self) -> bool {
        self.items.iter().any(|i| i.aggregate)
    }
}

/// The bound plan: a tree of operators over bindings.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundOp {
    /// One empty record (the start of every query).
    Unit,
    Match {
        input: Box<BoundOp>,
        optional: bool,
        pattern: BoundPattern,
        /// The clause's WHERE (for OPTIONAL MATCH: decides the match, never
        /// drops input records).
        predicate: Option<LogicalExpr>,
        /// Variables this clause introduces.
        introduces: Vec<VarId>,
    },
    Unwind {
        input: Box<BoundOp>,
        expr: LogicalExpr,
        var: VarId,
    },
    Project {
        input: Box<BoundOp>,
        projection: Projection,
    },
    /// A free-standing ORDER BY.
    Sort {
        input: Box<BoundOp>,
        keys: Vec<SortKey>,
    },
    /// A free-standing SKIP / OFFSET.
    Skip { input: Box<BoundOp>, count: i64 },
    /// A free-standing LIMIT.
    Limit { input: Box<BoundOp>, count: i64 },
    /// Cypher UNION: arms are complete queries. Columns are matched by NAME
    /// (Neo4j: `RETURN 1 AS a, 2 AS b UNION RETURN 2 AS b, 1 AS a` is valid);
    /// `arm_columns[i]` lists arm i's result bindings in the output order.
    Union {
        arms: Vec<BoundOp>,
        arm_columns: Vec<Vec<VarId>>,
        all: bool,
    },
}

/// The result of binding a statement.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundStatement {
    pub plan: BoundOp,
    /// Every binding of the statement, indexed by `VarId.0`.
    pub bindings: Vec<Binding>,
    /// The result columns: (name, binding), in order.
    pub columns: Vec<(String, VarId)>,
}

impl BoundStatement {
    pub fn binding(&self, var: VarId) -> &Binding {
        &self.bindings[var.0 as usize]
    }
}

/// Why a statement could not be bound.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BindError {
    /// The query is invalid Cypher (Neo4j rejects it too).
    #[error("Variable `{0}` not defined")]
    UndefinedVariable(String),
    #[error("Variable `{0}` already declared")]
    AlreadyDeclared(String),
    #[error("Type mismatch: `{name}` is a {bound}, used as a {used}")]
    TypeMismatch {
        name: String,
        bound: &'static str,
        used: &'static str,
    },
    #[error("Expression in {0} must be aliased (use AS)")]
    MissingAlias(&'static str),
    #[error("Multiple result columns with the same name `{0}`")]
    DuplicateColumn(String),
    #[error("{0}")]
    Invalid(String),
    /// Valid Cypher this binder does not handle yet: the caller falls back to
    /// the legacy pipeline (or fails loudly once it is gone).
    #[error("not supported by the bound plan yet: {0}")]
    Unsupported(String),
}

impl BindError {
    pub fn is_unsupported(&self) -> bool {
        matches!(self, BindError::Unsupported(_))
    }
}
