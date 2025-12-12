use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, sync::Arc};
use std::sync::atomic::{AtomicU32, Ordering};

// Import serde_arc modules for serialization
#[path = "../../utils/serde_arc.rs"]
mod serde_arc;
#[path = "../../utils/serde_arc_vec.rs"]
mod serde_arc_vec;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::ast::{
        Expression as CypherExpression, OrderByItem as CypherOrderByItem,
        OrerByOrder as CypherOrerByOrder, ReturnItem as CypherReturnItem, WithItem,
    },
    query_planner::{
        logical_expr::{
            ColumnAlias, Direction, Literal, LogicalExpr, Operator, OperatorApplication,
        },
        transformed::Transformed,
    },
};

use crate::{
    open_cypher_parser::ast::{CypherStatement, OpenCypherQueryAst, UnionType as AstUnionType},
    query_planner::logical_plan::plan_builder::LogicalPlanResult,
};

use super::plan_ctx::PlanCtx;

pub mod errors;
// pub mod logical_plan;
mod filter_view;
mod match_clause;
mod optional_match_clause;
mod order_by_clause;
pub mod plan_builder;
mod projection_view;
mod return_clause;
mod skip_n_limit_clause;
mod unwind_clause;
mod view_scan;
mod where_clause;
mod with_clause;

pub use view_scan::ViewScan;

pub fn evaluate_query(
    query_ast: OpenCypherQueryAst<'_>,
    schema: &GraphSchema,
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    plan_builder::build_logical_plan(&query_ast, schema, tenant_id, view_parameter_values)
}

/// Evaluate a complete Cypher statement which may contain UNION clauses
pub fn evaluate_cypher_statement(
    statement: CypherStatement<'_>,
    schema: &GraphSchema,
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    // If no union clauses, just evaluate the single query
    if statement.union_clauses.is_empty() {
        return evaluate_query(statement.query, schema, tenant_id, view_parameter_values);
    }
    
    // Build logical plans for all queries
    let mut all_plans: Vec<Arc<LogicalPlan>> = Vec::new();
    #[allow(unused_assignments)]
    let mut combined_ctx: Option<PlanCtx> = None;
    
    // First query
    let (first_plan, first_ctx) = plan_builder::build_logical_plan(
        &statement.query, 
        schema, 
        tenant_id.clone(), 
        view_parameter_values.clone()
    )?;
    all_plans.push(first_plan);
    combined_ctx = Some(first_ctx);
    
    // Track the union type (all must be the same for simplicity, or we use the first UNION's type)
    let union_type = if let Some(first_union) = statement.union_clauses.first() {
        match first_union.union_type {
            AstUnionType::All => UnionType::All,
            AstUnionType::Distinct => UnionType::Distinct,
        }
    } else {
        UnionType::All
    };
    
    // Build plans for each union clause
    for union_clause in statement.union_clauses {
        let (plan, ctx) = plan_builder::build_logical_plan(
            &union_clause.query, 
            schema, 
            tenant_id.clone(), 
            view_parameter_values.clone()
        )?;
        all_plans.push(plan);
        // Merge the context from this union branch into combined context
        if let Some(ref mut combined) = combined_ctx {
            combined.merge(ctx);
        }
    }
    
    // Create Union logical plan
    let union_plan = Arc::new(LogicalPlan::Union(Union {
        inputs: all_plans,
        union_type,
    }));
    
    Ok((union_plan, combined_ctx.unwrap()))
}

/// Global counter for generating simple, human-readable aliases like t1, t2, t3...
static ALIAS_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Generate a simple, human-readable alias for anonymous nodes/edges.
/// Returns "t1", "t2", "t3", etc. Much easier to read than UUID hex strings!
pub fn generate_id() -> String {
    let n = ALIAS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("t{}", n)
}

/// Reset the alias counter (useful for testing to get predictable aliases)
#[allow(dead_code)]
pub fn reset_alias_counter() {
    ALIAS_COUNTER.store(1, Ordering::SeqCst);
}

static CTE_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Generate a simple, human-readable CTE name.
/// Returns "cte1", "cte2", "cte3", etc. Much shorter than UUID strings!
pub fn generate_cte_id() -> String {
    let n = CTE_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("cte{}", n)
}

/// Reset the CTE counter (useful for testing to get predictable CTE names)
#[allow(dead_code)]
pub fn reset_cte_counter() {
    CTE_COUNTER.store(1, Ordering::SeqCst);
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum LogicalPlan {
    Empty,

    Scan(Scan),

    #[serde(with = "serde_arc")]
    ViewScan(Arc<ViewScan>),

    GraphNode(GraphNode),

    GraphRel(GraphRel),

    Filter(Filter),

    Projection(Projection),

    GroupBy(GroupBy),

    OrderBy(OrderBy),

    Skip(Skip),

    Limit(Limit),

    Cte(Cte),

    GraphJoins(GraphJoins),

    Union(Union),

    PageRank(PageRank),

    /// UNWIND clause: transforms array values into individual rows
    /// Maps to ClickHouse ARRAY JOIN
    Unwind(Unwind),

    /// Cartesian product (CROSS JOIN) of two disconnected patterns
    /// Used when WITH...MATCH or OPTIONAL MATCH patterns don't share aliases
    CartesianProduct(CartesianProduct),

    /// WITH clause - creates a scope/materialization boundary between query segments.
    /// This is NOT just a projection - it has bridging semantics and contains
    /// ORDER BY, SKIP, LIMIT, WHERE as part of its syntax (per OpenCypher grammar).
    WithClause(WithClause),
}

/// Cartesian product of two disconnected graph patterns.
/// Generated when:
/// 1. `MATCH (a) WITH a MATCH (b)` - subsequent MATCH doesn't share aliases
/// 2. `MATCH (a) OPTIONAL MATCH (b)` - optional pattern doesn't connect
/// 
/// Translates to CROSS JOIN in SQL (or LEFT JOIN for OPTIONAL).
/// When a join_condition is present, it becomes the ON clause.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CartesianProduct {
    /// The left/base pattern (e.g., from WITH clause)
    #[serde(with = "serde_arc")]
    pub left: Arc<LogicalPlan>,
    /// The right/new pattern (e.g., subsequent MATCH)
    #[serde(with = "serde_arc")]
    pub right: Arc<LogicalPlan>,
    /// Whether this is optional (from OPTIONAL MATCH)
    /// When true, generates LEFT JOIN instead of CROSS JOIN
    pub is_optional: bool,
    /// Join condition extracted from WHERE clause when filter references both sides
    /// e.g., WHERE ip1.ip = ip2.ip becomes the ON clause for the join
    pub join_condition: Option<LogicalExpr>,
}

/// WITH clause as defined in OpenCypher grammar.
/// Creates a materialization/scope boundary between query segments.
///
/// OpenCypher syntax:
/// ```text
/// WITH [DISTINCT] <return items> [ORDER BY ...] [SKIP n] [LIMIT m] [WHERE ...]
/// ```
///
/// Key semantics:
/// - **Boundary**: Analyzers (like BidirectionalUnion) should NOT cross this boundary
/// - **Scope**: Only `exported_aliases` are visible to downstream clauses
/// - **Materialization**: Maps to SQL CTE in rendering
///
/// This is fundamentally different from Projection (RETURN):
/// - WITH has ORDER BY, SKIP, LIMIT, WHERE as part of its syntax
/// - WITH bridges to continuation (next MATCH/RETURN)
/// - WITH creates scope isolation
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct WithClause {
    /// The query segment BEFORE this WITH (input to be projected/filtered)
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,

    /// The projection items (what WITH exports)
    pub items: Vec<ProjectionItem>,

    /// DISTINCT modifier (WITH DISTINCT ...)
    pub distinct: bool,

    /// ORDER BY clause - part of WITH syntax, not a separate node
    /// Applied to intermediate result before passing to continuation
    pub order_by: Option<Vec<OrderByItem>>,

    /// SKIP clause - part of WITH syntax
    pub skip: Option<u64>,

    /// LIMIT clause - part of WITH syntax
    pub limit: Option<u64>,

    /// WHERE clause after WITH - filters the intermediate result
    /// This is different from WHERE after MATCH (which filters the pattern)
    pub where_clause: Option<LogicalExpr>,

    /// Exported aliases - what's visible to downstream clauses.
    /// Derived from items but stored explicitly for easy boundary checking.
    /// E.g., `WITH a, b.name AS name` exports ["a", "name"]
    pub exported_aliases: Vec<String>,
}

impl WithClause {
    /// Create a new WithClause with just the essential fields
    pub fn new(input: Arc<LogicalPlan>, items: Vec<ProjectionItem>) -> Self {
        let exported_aliases = Self::extract_exported_aliases(&items);
        Self {
            input,
            items,
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases,
        }
    }

    /// Create a WithClause with DISTINCT
    pub fn with_distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self
    }

    /// Add ORDER BY to the WithClause
    pub fn with_order_by(mut self, order_by: Vec<OrderByItem>) -> Self {
        self.order_by = Some(order_by);
        self
    }

    /// Add SKIP to the WithClause
    pub fn with_skip(mut self, skip: u64) -> Self {
        self.skip = Some(skip);
        self
    }

    /// Add LIMIT to the WithClause
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Add WHERE clause to the WithClause
    pub fn with_where(mut self, predicate: LogicalExpr) -> Self {
        self.where_clause = Some(predicate);
        self
    }

    /// Extract exported alias names from projection items
    fn extract_exported_aliases(items: &[ProjectionItem]) -> Vec<String> {
        items
            .iter()
            .filter_map(|item| {
                // First check for explicit alias (e.g., `b.name AS name`)
                if let Some(ref alias) = item.col_alias {
                    return Some(alias.0.clone()); // ColumnAlias is a tuple struct
                }
                // Otherwise try to extract from expression
                Self::extract_alias_from_expr(&item.expression)
            })
            .collect()
    }

    /// Extract alias from a LogicalExpr, handling nested expressions like DISTINCT
    fn extract_alias_from_expr(expr: &LogicalExpr) -> Option<String> {
        match expr {
            LogicalExpr::TableAlias(ta) => Some(ta.0.clone()),
            LogicalExpr::PropertyAccessExp(pa) => Some(pa.table_alias.0.clone()),
            LogicalExpr::Column(col) => Some(col.0.clone()),
            // Handle DISTINCT wrapping: DISTINCT friend -> friend
            LogicalExpr::OperatorApplicationExp(op_app) => {
                if op_app.operator == crate::query_planner::logical_expr::Operator::Distinct {
                    // DISTINCT wraps a single operand - extract from it
                    op_app.operands.first().and_then(|inner| Self::extract_alias_from_expr(inner))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Check if a given alias is exported by this WITH clause
    pub fn exports_alias(&self, alias: &str) -> bool {
        self.exported_aliases.iter().any(|a| a == alias)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Scan {
    pub table_alias: Option<String>,
    pub table_name: Option<String>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub alias: String,
    /// The node label (e.g., "User", "Airport")
    /// Needed for denormalized property mapping during SQL generation
    pub label: Option<String>,
    /// True if this node is denormalized onto an edge table (set by optimizer)
    /// When true, RenderPlan should skip creating CTEs/JOINs for this node
    #[serde(default)]
    pub is_denormalized: bool,
}

/// Represents a relationship pattern in a graph query.
/// 
/// # IMPORTANT: Left/Right Convention
/// 
/// The `left` and `right` fields follow a **normalized source/target convention**:
/// - `left` is ALWAYS the **source** node (connects to relationship's `from_id`)
/// - `right` is ALWAYS the **target** node (connects to relationship's `to_id`)
/// 
/// This normalization happens during parsing based on the arrow direction:
/// - For `(a)-[:R]->(b)` (Outgoing): left=a (source), right=b (target)
/// - For `(a)<-[:R]-(b)` (Incoming): left=b (source), right=a (target) ← nodes are SWAPPED!
/// 
/// The `direction` field records the original syntactic direction, but for JOIN
/// generation, always use:
/// - `left_connection` connects to `from_id`
/// - `right_connection` connects to `to_id`
/// 
/// Do NOT use direction-based branching for from_id/to_id selection in JOIN logic!
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct GraphRel {
    /// Source node (connects to relationship's from_id)
    #[serde(with = "serde_arc")]
    pub left: Arc<LogicalPlan>,
    #[serde(with = "serde_arc")]
    pub center: Arc<LogicalPlan>,
    /// Target node (connects to relationship's to_id)
    #[serde(with = "serde_arc")]
    pub right: Arc<LogicalPlan>,
    pub alias: String,
    /// Original syntactic direction (for display/debug only, not for JOIN logic)
    pub direction: Direction,
    /// Alias of source node (connects to from_id)
    pub left_connection: String,
    /// Alias of target node (connects to to_id)
    pub right_connection: String,
    pub is_rel_anchor: bool,
    pub variable_length: Option<VariableLengthSpec>,
    pub shortest_path_mode: Option<ShortestPathMode>,
    pub path_variable: Option<String>, // For: MATCH p = pattern, stores "p"
    pub where_predicate: Option<LogicalExpr>, // WHERE clause predicates for filter placement in CTEs
    pub labels: Option<Vec<String>>, // Relationship type labels for [:TYPE1|TYPE2] patterns
    pub is_optional: Option<bool>, // For OPTIONAL MATCH: marks this relationship as optional (LEFT JOIN)
    pub anchor_connection: Option<String>, // For OPTIONAL MATCH: the connection from base MATCH (keeps WHERE filters)
}

/// Mode for shortest path queries
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ShortestPathMode {
    /// shortestPath() - return one shortest path
    Shortest,
    /// allShortestPaths() - return all paths with minimum length
    AllShortest,
}

/// Specification for variable-length path relationships
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct VariableLengthSpec {
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

impl Default for VariableLengthSpec {
    /// Default is a single hop (normal relationship)
    fn default() -> Self {
        Self {
            min_hops: Some(1),
            max_hops: Some(1),
        }
    }
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

    /// Check if this is a single-hop relationship (normal relationship)
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

    /// Check if this is an exact hop count (e.g., *2, *3, *5)
    /// Returns Some(n) if min == max == n, None otherwise
    pub fn exact_hop_count(&self) -> Option<u32> {
        match (self.min_hops, self.max_hops) {
            (Some(min), Some(max)) if min == max => Some(min),
            _ => None,
        }
    }

    /// Check if this requires a range (not exact hop count)
    pub fn is_range(&self) -> bool {
        self.exact_hop_count().is_none()
    }
}

impl From<crate::open_cypher_parser::ast::VariableLengthSpec> for VariableLengthSpec {
    fn from(ast_spec: crate::open_cypher_parser::ast::VariableLengthSpec) -> Self {
        Self {
            min_hops: ast_spec.min_hops,
            max_hops: ast_spec.max_hops,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Cte {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub name: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Union {
    #[serde(with = "serde_arc_vec")]
    pub inputs: Vec<Arc<LogicalPlan>>,
    pub union_type: UnionType,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum UnionType {
    Distinct,
    All,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PageRank {
    pub graph_name: Option<String>,
    pub iterations: usize,
    pub damping_factor: f64,
    pub node_labels: Option<Vec<String>>,
    pub relationship_types: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct GraphJoins {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,

    /// DEPRECATED: These pre-computed joins are incorrect for multi-hop patterns.
    /// Only used as fallback for extract_from(). The correct approach is to call
    /// input.extract_joins() which handles nested GraphRel recursively.
    /// TODO: Remove this field in future refactor after validating all tests pass.
    pub joins: Vec<Join>,

    /// Aliases that came from OPTIONAL MATCH clauses (for correct FROM table selection)
    pub optional_aliases: std::collections::HashSet<String>,

    /// The computed anchor table (FROM clause table)
    /// Computed during join reordering in graph_join_inference
    /// None = denormalized pattern (use relationship table directly)
    pub anchor_table: Option<String>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct Join {
    pub table_name: String,
    pub table_alias: String,
    pub joining_on: Vec<OperatorApplication>,
    pub join_type: JoinType,
    /// Pre-filter for LEFT JOINs (applied inside subquery form)
    /// Used for OPTIONAL MATCH WHERE predicates that reference only the optional alias
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pre_filter: Option<LogicalExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub enum JoinType {
    #[default]
    Join,
    Inner,
    Left,
    Right,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Filter {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub predicate: LogicalExpr,
}

/// Distinguishes between WITH and RETURN projections.
/// Both use the same <return statement body> in OpenCypher grammar,
/// but differ in position: WITH is intermediate, RETURN is terminal.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ProjectionKind {
    /// Created by WITH clause - intermediate projection
    With,
    /// Created by RETURN clause - final projection
    Return,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Projection {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
    /// Indicates whether this projection comes from WITH or RETURN clause
    pub kind: ProjectionKind,
    /// Whether DISTINCT should be applied to results
    pub distinct: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct GroupBy {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub expressions: Vec<LogicalExpr>,
    /// HAVING clause for post-aggregation filtering
    /// Filters that reference projection aliases (aggregation results) go here
    pub having_clause: Option<LogicalExpr>,
    /// Whether this GroupBy forms a materialization boundary (must become CTE/subquery).
    /// Set to true when:
    /// - This is a WITH clause with aggregation followed by another MATCH
    /// - GraphJoinInference should NOT merge joins across this boundary
    #[serde(default)]
    pub is_materialization_boundary: bool,
    /// The alias exposed by this boundary (e.g., "f" in `WITH f, count(*) AS cnt`)
    /// Used for scoping: outer query references this alias from CTE
    #[serde(default)]
    pub exposed_alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ProjectionItem {
    pub expression: LogicalExpr,
    pub col_alias: Option<ColumnAlias>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OrderBy {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub items: Vec<OrderByItem>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Skip {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub count: i64,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Limit {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub count: i64,
}

/// UNWIND clause: transforms array values into individual rows
/// Maps to ClickHouse ARRAY JOIN
/// 
/// Example: UNWIND r.items AS item
/// Generates: ARRAY JOIN r.items AS item
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Unwind {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    /// The expression to unwind (must be an array type)
    pub expression: LogicalExpr,
    /// The alias for each unwound element
    pub alias: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expression: LogicalExpr,
    pub order: OrderByOrder,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum OrderByOrder {
    Asc,
    Desc,
}

impl Filter {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Filter(Filter {
                    input: new_input.clone(),
                    predicate: self.predicate.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl Projection {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        crate::debug_println!("DEBUG Projection::rebuild_or_clone: self.distinct = {}", self.distinct);
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Projection(Projection {
                    input: new_input.clone(),
                    items: self.items.clone(),
                    kind: self.kind.clone(),
                    distinct: self.distinct,
                });
                crate::debug_println!("DEBUG Projection::rebuild_or_clone: Created new Projection with distinct = {}", self.distinct);
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => {
                crate::debug_println!("DEBUG Projection::rebuild_or_clone: No transformation, returning old plan");
                Transformed::No(old_plan.clone())
            }
        }
    }
}

impl GroupBy {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::GroupBy(GroupBy {
                    input: new_input.clone(),
                    expressions: self.expressions.clone(),
                    having_clause: self.having_clause.clone(),
                    is_materialization_boundary: self.is_materialization_boundary,
                    exposed_alias: self.exposed_alias.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl OrderBy {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::OrderBy(OrderBy {
                    input: new_input.clone(),
                    items: self.items.clone(),
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl Skip {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Skip(Skip {
                    input: new_input.clone(),
                    count: self.count,
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl Limit {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_node = LogicalPlan::Limit(Limit {
                    input: new_input.clone(),
                    count: self.count,
                });
                Transformed::Yes(Arc::new(new_node))
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl GraphNode {
    // pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, self_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_graph_node = LogicalPlan::GraphNode(GraphNode {
                    input: new_input.clone(),
                    // self_plan: self_tf.get_plan(),
                    alias: self.alias.clone(),
                    label: self.label.clone(),
                    is_denormalized: self.is_denormalized,
                });
                Transformed::Yes(Arc::new(new_graph_node))
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl GraphRel {
    pub fn rebuild_or_clone(
        &self,
        left_tf: Transformed<Arc<LogicalPlan>>,
        center_tf: Transformed<Arc<LogicalPlan>>,
        right_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        let left_changed = left_tf.is_yes();
        let right_changed = right_tf.is_yes();
        let center_changed = center_tf.is_yes();

        if left_changed | right_changed | center_changed {
            let new_graph_rel = LogicalPlan::GraphRel(GraphRel {
                left: left_tf.get_plan(),
                center: center_tf.get_plan(),
                right: right_tf.get_plan(),
                alias: self.alias.clone(),
                left_connection: self.left_connection.clone(),
                right_connection: self.right_connection.clone(),
                direction: self.direction.clone(),
                // is_anchor_graph_rel: self.is_anchor_graph_rel,
                is_rel_anchor: self.is_rel_anchor,
                variable_length: self.variable_length.clone(),
                shortest_path_mode: self.shortest_path_mode.clone(),
                path_variable: self.path_variable.clone(),
                where_predicate: self.where_predicate.clone(),
                labels: self.labels.clone(),
                is_optional: self.is_optional,
                anchor_connection: self.anchor_connection.clone(),
            });
            Transformed::Yes(Arc::new(new_graph_rel))
        } else {
            Transformed::No(old_plan.clone())
        }
    }
}

impl Cte {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                // if new input is empty then remove the CTE
                if matches!(new_input.as_ref(), LogicalPlan::Empty) {
                    Transformed::Yes(new_input.clone())
                } else {
                    let new_node = LogicalPlan::Cte(Cte {
                        input: new_input.clone(),
                        name: self.name.clone(),
                    });
                    Transformed::Yes(Arc::new(new_node))
                }
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl GraphJoins {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        match input_tf {
            Transformed::Yes(new_input) => {
                let new_graph_joins = LogicalPlan::GraphJoins(GraphJoins {
                    input: new_input.clone(),
                    joins: self.joins.clone(),
                    optional_aliases: self.optional_aliases.clone(),
                    anchor_table: self.anchor_table.clone(),
                });
                Transformed::Yes(Arc::new(new_graph_joins))
            }
            Transformed::No(_) => Transformed::No(old_plan.clone()),
        }
    }
}

impl Union {
    pub fn rebuild_or_clone(
        &self,
        inputs_tf: Vec<Transformed<Arc<LogicalPlan>>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        // iterate over inputs_tf vec and check if any one of them is transformed.
        // If yes then break the iteration and club all inputs irrespective of transformation status.
        // If no then return the old plan.
        let mut is_transformed = false;
        for input_tf in &inputs_tf {
            if input_tf.is_yes() {
                is_transformed = true;
                break;
            }
        }
        if is_transformed {
            let new_inputs: Vec<Arc<LogicalPlan>> =
                inputs_tf.into_iter().map(|tf| tf.get_plan()).collect();
            let new_union = LogicalPlan::Union(Union {
                inputs: new_inputs,
                union_type: self.union_type.clone(),
            });
            Transformed::Yes(Arc::new(new_union))
        } else {
            Transformed::No(old_plan.clone())
        }
    }
}

impl<'a> From<CypherReturnItem<'a>> for ProjectionItem {
    fn from(value: CypherReturnItem<'a>) -> Self {
        // Infer alias from expression if not explicitly provided with AS
        let inferred_alias = if value.alias.is_none() {
            match &value.expression {
                // For property access like "u.name", use "u.name" as alias (Neo4j behavior)
                // Neo4j returns qualified names by default: RETURN u.name → column "u.name"
                CypherExpression::PropertyAccessExp(prop_access) => {
                    Some(format!("{}.{}", prop_access.base, prop_access.key))
                }
                // For simple variables like "u", use "u" as alias
                CypherExpression::Variable(var) => Some(var.to_string()),
                // For function calls, could infer from function name, but keep None for now
                _ => None,
            }
        } else {
            None
        };

        ProjectionItem {
            expression: value.expression.into(),
            col_alias: value
                .alias
                .map(|alias| ColumnAlias(alias.to_string()))
                .or_else(|| inferred_alias.map(ColumnAlias)),
            // belongs_to_table: None, // This will be set during planning phase
        }
    }
}

impl<'a> From<WithItem<'a>> for ProjectionItem {
    fn from(value: WithItem<'a>) -> Self {
        ProjectionItem {
            expression: value.expression.into(),
            col_alias: value.alias.map(|alias| ColumnAlias(alias.to_string())),
        }
    }
}

impl<'a> From<CypherOrderByItem<'a>> for OrderByItem {
    fn from(value: CypherOrderByItem<'a>) -> Self {
        OrderByItem {
            expression: if let CypherExpression::Variable(var) = value.expression {
                LogicalExpr::ColumnAlias(ColumnAlias(var.to_string()))
            } else {
                value.expression.into()
            },
            order: match value.order {
                CypherOrerByOrder::Asc => OrderByOrder::Asc,
                CypherOrerByOrder::Desc => OrderByOrder::Desc,
            },
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_tree(f, "", true, true)
    }
}

impl LogicalPlan {
    pub fn get_empty_match_plan() -> Self {
        LogicalPlan::Projection(Projection {
            input: Arc::new(LogicalPlan::Filter(Filter {
                input: Arc::new(LogicalPlan::Empty),
                predicate: LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::Literal(Literal::Integer(1)),
                        LogicalExpr::Literal(Literal::Integer(0)),
                    ],
                }),
            })),
            items: vec![ProjectionItem {
                expression: LogicalExpr::Literal(Literal::Integer(1)),
                col_alias: None,
            }],
            kind: ProjectionKind::Return,
            distinct: false,
        })
    }
}

impl LogicalPlan {
    fn fmt_with_tree(
        &self,
        f: &mut fmt::Formatter<'_>,
        prefix: &str,
        is_last: bool,
        is_root: bool,
    ) -> fmt::Result {
        let (branch, next_prefix) = if is_last {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };

        if is_root {
            writeln!(f, "\n{}", self.variant_name())?;
        } else {
            writeln!(f, "{}{}{}", prefix, branch, self.variant_name())?;
        }

        let mut children: Vec<&LogicalPlan> = vec![];
        match self {
            LogicalPlan::GraphNode(graph_node) => {
                children.push(&graph_node.input);
                // children.push(&graph_node.self_plan);
            }
            LogicalPlan::GraphRel(graph_rel) => {
                children.push(&graph_rel.left);
                children.push(&graph_rel.center);
                children.push(&graph_rel.right);
            }
            LogicalPlan::Filter(filter) => {
                children.push(&filter.input);
            }
            LogicalPlan::Projection(proj) => {
                children.push(&proj.input);
            }
            LogicalPlan::GraphJoins(graph_join) => {
                children.push(&graph_join.input);
            }
            LogicalPlan::OrderBy(order_by) => {
                children.push(&order_by.input);
            }
            LogicalPlan::Skip(skip) => {
                children.push(&skip.input);
            }
            LogicalPlan::Limit(limit) => {
                children.push(&limit.input);
            }
            LogicalPlan::GroupBy(group_by) => {
                children.push(&group_by.input);
            }
            LogicalPlan::Cte(cte) => {
                children.push(&cte.input);
            }
            LogicalPlan::Union(union) => {
                for input in &union.inputs {
                    children.push(input);
                }
            }
            LogicalPlan::PageRank(_) => {
                // PageRank is a leaf node - no children to traverse
            }
            LogicalPlan::Unwind(unwind) => {
                children.push(&unwind.input);
            }
            LogicalPlan::CartesianProduct(cp) => {
                children.push(&cp.left);
                children.push(&cp.right);
            }
            LogicalPlan::WithClause(with_clause) => {
                children.push(&with_clause.input);
            }
            LogicalPlan::ViewScan(_) => {
                // ViewScan is a leaf node - no children to traverse
            }
            LogicalPlan::Empty => {
                // Empty is a leaf node - no children to traverse
            }
            LogicalPlan::Scan(_) => {
                // Scan is a leaf node - no children to traverse
            }
        }

        let n = children.len();
        for (i, child) in children.into_iter().enumerate() {
            child.fmt_with_tree(f, &format!("{}{}", prefix, next_prefix), i + 1 == n, false)?;
        }
        Ok(())
    }

    fn variant_name(&self) -> String {
        match self {
            LogicalPlan::GraphNode(graph_node) => format!("Node({})", graph_node.alias),
            LogicalPlan::GraphRel(graph_rel) => format!(
                "GraphRel({:?})(is_rel_anchor: {:?})",
                graph_rel.direction, graph_rel.is_rel_anchor
            ),
            LogicalPlan::Scan(scan) => format!("scan({:?})", scan.table_alias),
            LogicalPlan::Empty => "".to_string(),
            LogicalPlan::Filter(_) => "Filter".to_string(),
            LogicalPlan::Projection(_) => "Projection".to_string(),
            LogicalPlan::OrderBy(_) => "OrderBy".to_string(),
            LogicalPlan::Skip(_) => "Skip".to_string(),
            LogicalPlan::Limit(_) => "Limit".to_string(),
            LogicalPlan::GroupBy(_) => "GroupBy".to_string(),
            LogicalPlan::Cte(cte) => format!("Cte({})", cte.name),
            LogicalPlan::GraphJoins(_) => "GraphJoins".to_string(),
            LogicalPlan::Union(_) => "Union".to_string(),
            LogicalPlan::PageRank(pagerank) => format!(
                "PageRank(iterations: {}, damping: {:.2})",
                pagerank.iterations, pagerank.damping_factor
            ),
            LogicalPlan::Unwind(unwind) => format!("Unwind(alias: {})", unwind.alias),
            LogicalPlan::ViewScan(scan) => format!("ViewScan({:?})", scan.source_table),
            LogicalPlan::CartesianProduct(cp) => format!("CartesianProduct(optional: {})", cp.is_optional),
            LogicalPlan::WithClause(wc) => format!("WithClause(items: {}, distinct: {})", wc.items.len(), wc.distinct),
        }
    }

    /// Check if the logical plan tree contains any variable-length paths
    pub fn contains_variable_length_path(&self) -> bool {
        match self {
            LogicalPlan::GraphRel(graph_rel) => {
                // Check if this GraphRel has variable_length
                if graph_rel.variable_length.is_some() {
                    return true;
                }
                // Recursively check children
                graph_rel.left.contains_variable_length_path()
                    || graph_rel.center.contains_variable_length_path()
                    || graph_rel.right.contains_variable_length_path()
            }
            LogicalPlan::GraphNode(graph_node) => graph_node.input.contains_variable_length_path(),
            LogicalPlan::Filter(filter) => filter.input.contains_variable_length_path(),
            LogicalPlan::Projection(proj) => proj.input.contains_variable_length_path(),
            LogicalPlan::GraphJoins(joins) => joins.input.contains_variable_length_path(),
            LogicalPlan::OrderBy(order_by) => order_by.input.contains_variable_length_path(),
            LogicalPlan::Skip(skip) => skip.input.contains_variable_length_path(),
            LogicalPlan::Limit(limit) => limit.input.contains_variable_length_path(),
            LogicalPlan::GroupBy(group_by) => group_by.input.contains_variable_length_path(),
            LogicalPlan::Cte(cte) => cte.input.contains_variable_length_path(),
            LogicalPlan::Union(union) => union
                .inputs
                .iter()
                .any(|input| input.contains_variable_length_path()),
            LogicalPlan::Unwind(unwind) => unwind.input.contains_variable_length_path(),
            LogicalPlan::CartesianProduct(cp) => {
                cp.left.contains_variable_length_path()
                    || cp.right.contains_variable_length_path()
            }
            LogicalPlan::WithClause(with_clause) => {
                with_clause.input.contains_variable_length_path()
            }
            // Leaf nodes
            LogicalPlan::Scan(_)
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::Empty
            | LogicalPlan::PageRank(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::query_planner::logical_expr::{
        Column, Literal, LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias,
    };
    // use crate::open_cypher_parser::ast;

    #[test]
    fn test_filter_rebuild_or_clone_with_transformation() {
        let original_input = Arc::new(LogicalPlan::Empty);
        let new_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("employees".to_string()),
            table_name: Some("employee_table".to_string()),
        }));

        let filter = Filter {
            input: original_input.clone(),
            predicate: LogicalExpr::Literal(Literal::Boolean(true)),
        };

        let old_plan = Arc::new(LogicalPlan::Filter(filter.clone()));
        let input_transformed = Transformed::Yes(new_input.clone());

        let result = filter.rebuild_or_clone(input_transformed, old_plan.clone());

        match result {
            Transformed::Yes(new_plan) => match new_plan.as_ref() {
                LogicalPlan::Filter(new_filter) => {
                    assert_eq!(new_filter.input, new_input);
                    assert_eq!(
                        new_filter.predicate,
                        LogicalExpr::Literal(Literal::Boolean(true))
                    );
                }
                _ => panic!("Expected Filter plan"),
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_filter_rebuild_or_clone_without_transformation() {
        let input = Arc::new(LogicalPlan::Empty);
        let filter = Filter {
            input: input.clone(),
            predicate: LogicalExpr::Literal(Literal::Boolean(true)),
        };

        let old_plan = Arc::new(LogicalPlan::Filter(filter.clone()));
        let input_not_transformed = Transformed::No(input.clone());

        let result = filter.rebuild_or_clone(input_not_transformed, old_plan.clone());

        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, old_plan);
            }
            _ => panic!("Expected no transformation"),
        }
    }

    #[test]
    fn test_projection_rebuild_or_clone_with_transformation() {
        let original_input = Arc::new(LogicalPlan::Empty);
        let new_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("customers".to_string()),
            table_name: Some("customer_table".to_string()),
        }));

        let projection_items = vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("customer".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
            }),
            col_alias: None,
        }];

        let projection = Projection {
            input: original_input.clone(),
            items: projection_items.clone(),
            kind: ProjectionKind::Return,
            distinct: false,
        };

        let old_plan = Arc::new(LogicalPlan::Projection(projection.clone()));
        let input_transformed = Transformed::Yes(new_input.clone());

        let result = projection.rebuild_or_clone(input_transformed, old_plan.clone());

        match result {
            Transformed::Yes(new_plan) => match new_plan.as_ref() {
                LogicalPlan::Projection(new_projection) => {
                    assert_eq!(new_projection.input, new_input);
                    assert_eq!(new_projection.items.len(), 1);
                }
                _ => panic!("Expected Projection plan"),
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_graph_node_rebuild_or_clone() {
        let original_input = Arc::new(LogicalPlan::Empty);
        let new_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("users".to_string()),
            table_name: Some("user_table".to_string()),
        }));

        let graph_node = GraphNode {
            input: original_input.clone(),
            alias: "person".to_string(),
            label: None,
            is_denormalized: false,
        };

        let old_plan = Arc::new(LogicalPlan::GraphNode(graph_node.clone()));
        let input_transformed = Transformed::Yes(new_input.clone());

        let result = graph_node.rebuild_or_clone(input_transformed, old_plan.clone());

        match result {
            Transformed::Yes(new_plan) => match new_plan.as_ref() {
                LogicalPlan::GraphNode(new_graph_node) => {
                    assert_eq!(new_graph_node.input, new_input);
                    assert_eq!(new_graph_node.alias, "person");
                }
                _ => panic!("Expected GraphNode plan"),
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_graph_rel_rebuild_or_clone() {
        let left_plan = Arc::new(LogicalPlan::Empty);
        let center_plan = Arc::new(LogicalPlan::Empty);
        let right_plan = Arc::new(LogicalPlan::Empty);
        let new_left_plan = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("users".to_string()),
            table_name: Some("user_table".to_string()),
        }));

        let graph_rel = GraphRel {
            left: left_plan.clone(),
            center: center_plan.clone(),
            right: right_plan.clone(),
            alias: "works_for".to_string(),
            direction: Direction::Outgoing,
            left_connection: "employee_id".to_string(),
            right_connection: "company_id".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: None,
            is_optional: None,
            anchor_connection: None,
        };

        let old_plan = Arc::new(LogicalPlan::GraphRel(graph_rel.clone()));
        let left_transformed = Transformed::Yes(new_left_plan.clone());
        let center_not_transformed = Transformed::No(center_plan.clone());
        let right_not_transformed = Transformed::No(right_plan.clone());

        let result = graph_rel.rebuild_or_clone(
            left_transformed,
            center_not_transformed,
            right_not_transformed,
            old_plan.clone(),
        );

        match result {
            Transformed::Yes(new_plan) => match new_plan.as_ref() {
                LogicalPlan::GraphRel(new_graph_rel) => {
                    assert_eq!(new_graph_rel.left, new_left_plan);
                    assert_eq!(new_graph_rel.center, center_plan);
                    assert_eq!(new_graph_rel.right, right_plan);
                    assert_eq!(new_graph_rel.alias, "works_for");
                }
                _ => panic!("Expected GraphRel plan"),
            },
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_cte_rebuild_or_clone_with_empty_input() {
        let original_input = Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some("temp".to_string()),
            table_name: Some("temp_table".to_string()),
        }));
        let empty_input = Arc::new(LogicalPlan::Empty);

        let cte = Cte {
            input: original_input.clone(),
            name: "temp_results".to_string(),
        };

        let old_plan = Arc::new(LogicalPlan::Cte(cte.clone()));
        let input_transformed = Transformed::Yes(empty_input.clone());

        let result = cte.rebuild_or_clone(input_transformed, old_plan.clone());

        match result {
            Transformed::Yes(new_plan) => {
                // When input is empty, CTE should be removed and return the empty plan
                assert_eq!(new_plan, empty_input);
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_projection_item_from_ast() {
        let ast_return_item = CypherReturnItem {
            expression: CypherExpression::Variable("customer_name"),
            alias: Some("full_name"),
        };

        let projection_item = ProjectionItem::from(ast_return_item);

        match projection_item.expression {
            LogicalExpr::TableAlias(alias) => assert_eq!(alias.0, "customer_name"),
            _ => panic!("Expected TableAlias"),
        }
        assert_eq!(
            projection_item.col_alias,
            Some(ColumnAlias("full_name".to_string()))
        );
    }

    #[test]
    fn test_order_by_item_from_ast() {
        let ast_order_item = CypherOrderByItem {
            expression: CypherExpression::Variable("price"),
            order: CypherOrerByOrder::Desc,
        };

        let order_by_item = OrderByItem::from(ast_order_item);

        match order_by_item.expression {
            LogicalExpr::ColumnAlias(alias) => assert_eq!(alias.0, "price"),
            _ => panic!("Expected ColumnAlias"),
        }
        assert_eq!(order_by_item.order, OrderByOrder::Desc);
    }

    #[test]
    fn test_complex_logical_plan_structure() {
        // Create a complex plan: Projection -> Filter -> GraphNode -> Scan
        let scan = LogicalPlan::Scan(Scan {
            table_alias: Some("users".to_string()),
            table_name: Some("user_accounts".to_string()),
        });

        let graph_node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(scan),
            alias: "user".to_string(),
            label: None,
            is_denormalized: false,
        });

        let filter = LogicalPlan::Filter(Filter {
            input: Arc::new(graph_node),
            predicate: LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column("age".to_string()),
                    }),
                    LogicalExpr::Literal(Literal::Integer(18)),
                ],
            }),
        });

        let projection = LogicalPlan::Projection(Projection {
            input: Arc::new(filter),
            items: vec![
                ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column("email".to_string()),
                    }),
                    col_alias: Some(ColumnAlias("email_address".to_string())),
                },
                ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column("first_name".to_string()),
                    }),
                    col_alias: None,
                },
            ],
            kind: ProjectionKind::Return,
            distinct: false,
        });

        // Verify the structure
        match projection {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.items.len(), 2);
                match proj.input.as_ref() {
                    LogicalPlan::Filter(filter_node) => match filter_node.input.as_ref() {
                        LogicalPlan::GraphNode(graph_node) => {
                            assert_eq!(graph_node.alias, "user");
                            match graph_node.input.as_ref() {
                                LogicalPlan::Scan(scan_node) => {
                                    assert_eq!(scan_node.table_alias, Some("users".to_string()));
                                    assert_eq!(
                                        scan_node.table_name,
                                        Some("user_accounts".to_string())
                                    );
                                }
                                _ => panic!("Expected Scan at bottom"),
                            }
                        }
                        _ => panic!("Expected GraphNode"),
                    },
                    _ => panic!("Expected Filter"),
                }
            }
            _ => panic!("Expected Projection at top"),
        }
    }
}
