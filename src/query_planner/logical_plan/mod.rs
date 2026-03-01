//! Logical Plan representation for Cypher queries.
//!
//! This module defines the core data structures representing a Cypher query
//! as an intermediate representation (IR) between parsed AST and generated SQL.
//!
//! # Architecture Overview
//!
//! ```text
//! Cypher Query → AST → LogicalPlan → SQL Query
//!                       ^^^^^^^^^^^
//!                       This module
//! ```
//!
//! # Key Components
//!
//! ## Core Types
//! - [`LogicalPlan`] - Main enum representing all query plan nodes
//! - [`ViewScan`] - Table scan with optional predicate pushdown
//! - [`GraphNode`] / [`GraphRel`] - Graph pattern nodes and relationships
//! - [`Projection`] - SELECT clause items
//! - [`Filter`] - WHERE clause conditions
//! - [`GroupBy`] - Aggregation and grouping
//! - [`Cte`] - Common Table Expression (WITH clause in SQL sense)
//! - [`CartesianProduct`] - CROSS JOIN for disconnected patterns
//! - [`Union`] - UNION/UNION ALL for combined result sets
//!
//! ## Clause Processing Submodules
//! - `match_clause` - MATCH pattern to scan/join translation
//! - `optional_match_clause` - OPTIONAL MATCH (LEFT JOIN) handling
//! - `with_clause` - WITH clause scope/projection boundaries
//! - `return_clause` - RETURN projection handling
//! - `where_clause` - WHERE condition processing
//! - `order_by_clause` - ORDER BY generation
//! - `skip_n_limit_clause` - SKIP/LIMIT pagination
//! - `unwind_clause` - UNWIND array expansion
//!
//! # Plan Building
//!
//! Plans are built via [`plan_builder::build_logical_plan`] which:
//! 1. Processes Cypher clauses in order
//! 2. Builds nodes from inner to outer (scans → joins → filters → projections)
//! 3. Tracks planning context via [`PlanCtx`]
//!
//! # Example Plan Structure
//!
//! ```text
//! MATCH (u:User)-[f:FOLLOWS]->(friend)
//! WHERE u.active = true
//! RETURN friend.name
//!
//! → Projection(friend.name)
//!     └─ Filter(u.active = true)
//!         └─ GraphRel(f:FOLLOWS)
//!             ├─ left: ViewScan(users AS u)
//!             ├─ center: ViewScan(follows AS f)
//!             └─ right: ViewScan(users AS friend)
//! ```
//!
//! # ID Generation
//!
//! For anonymous patterns, unique IDs are generated via:
//! - [`generate_id`] - Simple incrementing aliases (t1, t2, t3...)
//! - [`generate_cte_id`] - CTE names (cte1, cte2, cte3...)

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::{collections::HashMap, fmt, sync::Arc};

// Import serde_arc modules for serialization
use crate::utils::serde_arc;
use crate::utils::serde_arc_vec;

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
pub use errors::LogicalPlanError;
// pub mod logical_plan;
mod filter_view;
pub mod match_clause; // Public for schema_inference to access ViewScan generation functions
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
    max_inferred_types: Option<usize>,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    plan_builder::build_logical_plan(
        &query_ast,
        schema,
        tenant_id,
        view_parameter_values,
        max_inferred_types,
    )
}

/// Helper function to detect empty or filtered UNION branches
///
/// Track C filters branches to 0 types, but creates different "empty" representations:
/// - Explicit: LogicalPlan::Empty (for nodes filtered to 0 types)
/// - Implicit: GraphRel{labels: None} (for relationships filtered to 0 types)
///
/// This function detects both forms and recursively checks wrapped plans.
fn is_empty_or_filtered_branch(plan: &LogicalPlan) -> bool {
    match plan {
        // Explicit empty
        LogicalPlan::Empty => true,

        // Implicit empty: relationship filtered to 0 types by Track C
        // Check both None and empty vector cases for consistency with analyzer checks
        LogicalPlan::GraphRel(rel)
            if rel.labels.as_ref().is_none_or(|labels| labels.is_empty()) =>
        {
            log::debug!(
                "Detected filtered GraphRel (labels=None or empty) for alias '{}'",
                rel.alias
            );
            true
        }

        // Check if wrapped plan contains Empty
        // BUT: GraphNode(input=Empty) with no label is a TypeInference placeholder,
        // not a filtered-out branch. Only treat as empty if label was explicitly set.
        LogicalPlan::GraphNode(node) => {
            matches!(node.input.as_ref(), LogicalPlan::Empty) && node.label.is_some()
        }

        // Recursively check wrapped plans (common UNION branch structures)
        // Includes all wrapper types that could appear in UNION branches
        LogicalPlan::Projection(proj) => is_empty_or_filtered_branch(&proj.input),
        LogicalPlan::Filter(f) => is_empty_or_filtered_branch(&f.input),
        LogicalPlan::GraphJoins(joins) => is_empty_or_filtered_branch(&joins.input),
        LogicalPlan::Limit(limit) => is_empty_or_filtered_branch(&limit.input),
        LogicalPlan::Skip(skip) => is_empty_or_filtered_branch(&skip.input),
        LogicalPlan::OrderBy(order) => is_empty_or_filtered_branch(&order.input),
        LogicalPlan::GroupBy(group) => is_empty_or_filtered_branch(&group.input),

        // Not empty
        _ => false,
    }
}

/// Evaluate a complete Cypher statement which may contain UNION clauses
pub fn evaluate_cypher_statement(
    statement: CypherStatement<'_>,
    schema: &GraphSchema,
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
    max_inferred_types: Option<usize>,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    // Handle standalone procedure calls
    match statement {
        CypherStatement::ProcedureCall(proc_call) => {
            // Procedure calls should be handled separately
            Err(LogicalPlanError::QueryPlanningError(format!(
                "Standalone procedure call '{}' should be handled by procedures module, not query planner",
                proc_call.procedure_name
            )))
        }
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            // If no union clauses, just evaluate the single query
            if union_clauses.is_empty() {
                return evaluate_query(
                    query,
                    schema,
                    tenant_id,
                    view_parameter_values,
                    max_inferred_types,
                );
            }

            // Build logical plans for all queries
            let mut all_plans: Vec<Arc<LogicalPlan>> = Vec::new();
            #[allow(unused_assignments)]
            let mut combined_ctx: Option<PlanCtx> = None;

            // Use the module-level is_empty_or_filtered_branch (no need for duplicate)

            // First query
            let (first_plan, first_ctx) = plan_builder::build_logical_plan(
                &query,
                schema,
                tenant_id.clone(),
                view_parameter_values.clone(),
                max_inferred_types,
            )?;
            // Only add non-empty branches
            if !is_empty_or_filtered_branch(&first_plan) {
                all_plans.push(first_plan);
                combined_ctx = Some(first_ctx);
            } else {
                log::info!(
                    "🔀 UNION first branch filtered to 0 types by Track C - skipping empty branch"
                );
                // Don't use empty branch's context as base — it has incomplete variable info
                // (e.g., relationship aliases registered with empty labels).
                // The first non-empty branch's context will be used instead.
                combined_ctx = None;
            }

            // Track the union type (all must be the same for simplicity, or we use the first UNION's type)
            let union_type = if let Some(first_union) = union_clauses.first() {
                match first_union.union_type {
                    AstUnionType::All => UnionType::All,
                    AstUnionType::Distinct => UnionType::Distinct,
                }
            } else {
                UnionType::All
            };

            // Build plans for each union clause
            for union_clause in union_clauses {
                let (plan, ctx) = plan_builder::build_logical_plan(
                    &union_clause.query,
                    schema,
                    tenant_id.clone(),
                    view_parameter_values.clone(),
                    max_inferred_types,
                )?;

                // Track C Property Optimization: Skip empty branches
                // When Track C filters a branch to 0 matching types, detect and skip it
                // This handles both explicit Empty and implicit GraphRel{labels: None}
                if !is_empty_or_filtered_branch(plan.as_ref()) {
                    all_plans.push(plan);
                    // Merge the context from this union branch into combined context
                    if let Some(ref mut combined) = combined_ctx {
                        combined.merge(ctx);
                    } else {
                        // First non-empty branch becomes the base context
                        combined_ctx = Some(ctx);
                    }
                } else {
                    log::info!(
                        "🔀 UNION branch filtered to 0 types by Track C - skipping empty branch"
                    );
                }
            }

            // Handle different scenarios based on non-empty branch count
            let union_plan = match all_plans.len() {
                0 => {
                    // All branches filtered to 0 types - return empty result
                    log::info!("🔀 All UNION branches empty - returning Empty plan (0 rows)");
                    Arc::new(LogicalPlan::Empty)
                }
                1 => {
                    // Only one branch has data - no UNION needed
                    log::info!("🔀 Only 1 non-empty UNION branch - skipping UNION wrapper");
                    all_plans.into_iter().next().unwrap()
                }
                _ => {
                    // Multiple branches with data - create UNION
                    log::info!(
                        "🔀 Creating UNION with {} non-empty branches",
                        all_plans.len()
                    );
                    Arc::new(LogicalPlan::Union(Union {
                        inputs: all_plans,
                        union_type,
                    }))
                }
            };

            let final_ctx = combined_ctx.ok_or_else(|| {
                LogicalPlanError::QueryPlanningError(
                    "Failed to merge plan contexts for UNION".to_string(),
                )
            })?;
            Ok((union_plan, final_ctx))
        }
    }
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

/// Reset all global counters for deterministic SQL generation.
/// Call at the start of each query to ensure identical input produces identical output.
pub fn reset_all_counters() {
    ALIAS_COUNTER.store(1, Ordering::SeqCst);
    CTE_COUNTER.store(1, Ordering::SeqCst);
}

static CTE_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Generate a simple, human-readable CTE name.
/// Returns "cte1", "cte2", "cte3", etc. Much shorter than UUID strings!
pub fn generate_cte_id() -> String {
    let n = CTE_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("cte{}", n)
}

/// Helper function for the common rebuild_or_clone pattern used across LogicalPlan variants.
///
/// This consolidates the duplicated logic that appears in 14+ rebuild_or_clone() methods:
/// - If transformation occurred, build new node with updated children via the provided closure
/// - If no transformation, return the old plan unchanged
///
/// # Arguments
/// * `is_transformed` - Whether any child transformation occurred
/// * `old_plan` - The original plan to return if no transformation occurred
/// * `builder` - Closure that constructs the new LogicalPlan variant with transformed children
#[inline]
fn handle_rebuild_or_clone<F>(
    is_transformed: bool,
    old_plan: Arc<LogicalPlan>,
    builder: F,
) -> Transformed<Arc<LogicalPlan>>
where
    F: FnOnce() -> Arc<LogicalPlan>,
{
    if is_transformed {
        Transformed::Yes(builder())
    } else {
        Transformed::No(old_plan)
    }
}

/// Helper for multi-child rebuild pattern (e.g., GraphRel with left/center/right).
/// Returns true if any child transformation occurred.
fn any_transformed(transformations: &[&Transformed<Arc<LogicalPlan>>]) -> bool {
    transformations.iter().any(|tf| tf.is_yes())
}

/// Aggregation type for pattern comprehensions
#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum AggregationType {
    Count,
    GroupArray,
    Sum,
    Avg,
    Min,
    Max,
}

/// Position of a correlation variable within the pattern hop chain
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PatternPosition {
    /// Start node of hop N (0-indexed)
    StartOfHop(usize),
    /// End node of hop N (0-indexed)
    EndOfHop(usize),
}

/// Info about a single correlation variable (outer scope variable referenced in pattern)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorrelationVarInfo {
    pub var_name: String,
    pub label: String,
    pub pattern_position: PatternPosition,
}

/// Serializable representation of a single hop in a connected pattern
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectedPatternInfo {
    pub start_label: Option<String>,
    pub start_alias: Option<String>,
    pub rel_type: Option<String>,
    pub rel_alias: Option<String>,
    pub direction: Direction,
    pub end_label: Option<String>,
    pub end_alias: Option<String>,
}

/// Metadata for a pattern comprehension extracted during logical planning.
/// Consumed at render time to generate CTE + LEFT JOIN SQL (simple cases)
/// or inline correlated subqueries (multi-hop/multi-correlation cases).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatternComprehensionMeta {
    /// Correlation variable from outer scope (e.g., "a" in `[(a)--() | 1]`)
    pub correlation_var: String,
    /// Label of the correlation variable's node (e.g., "User")
    pub correlation_label: String,
    /// Relationship direction: Outgoing, Incoming, or Either (both)
    pub direction: crate::open_cypher_parser::ast::Direction,
    /// Optional relationship type filter (e.g., "FOLLOWS")
    pub rel_types: Option<Vec<String>>,
    /// Aggregation function to apply
    pub agg_type: AggregationType,
    /// The alias assigned to this pattern comprehension result in the WITH clause
    pub result_alias: String,
    /// Label of the target node (e.g., "User" in `[(a)-[:FOLLOWS]->(b:User) | b.name]`)
    pub target_label: Option<String>,
    /// Property name from the projection (e.g., "name" in `| b.name`)
    pub target_property: Option<String>,
    /// ALL outer variables correlated from pattern (multi-correlation support)
    pub correlation_vars: Vec<CorrelationVarInfo>,
    /// Full multi-hop pattern chain (serializable form of ConnectedPattern)
    pub pattern_hops: Vec<ConnectedPatternInfo>,
    /// WHERE clause inside the pattern comprehension (serialized as LogicalExpr)
    pub where_clause: Option<LogicalExpr>,
    /// DFS order position for matching count(*) placeholders
    pub position_index: usize,
    /// Optional list constraint for list comprehension patterns.
    /// When set, the pattern comprehension was derived from `size([p IN list WHERE pattern])`.
    /// Contains (iteration_variable, list_expression_alias) — the correlated subquery
    /// should add `has(list_cte_column, edge_id)` instead of a direct correlation.
    pub list_constraint: Option<ListConstraint>,
}

/// Constraint from a list comprehension: `[p IN posts WHERE pattern]`
/// The iteration variable `p` maps to elements of the `posts` array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListConstraint {
    /// The iteration variable name (e.g., "p")
    pub variable: String,
    /// The list expression alias (e.g., "posts")
    pub list_alias: String,
    /// The source node label for the list elements (e.g., "Post" from collect(post:Post))
    pub source_label: Option<String>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum LogicalPlan {
    Empty,

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

    /// The CTE name assigned to this WITH clause (populated by CteSchemaResolver)
    /// Used by CteReferencePopulator to build available_ctes map
    pub cte_name: Option<String>,

    /// CTE references map: alias → CTE name
    /// Populated by analyzer to resolve which variables come from previous CTEs.
    /// Example: {"b": "with_a_b_cte"} means variable `b` comes from CTE `with_a_b_cte`.
    /// This allows render phase to be "dumb" - no searching, just lookup.
    pub cte_references: std::collections::HashMap<String, String>,

    /// Pattern comprehensions extracted during logical planning.
    /// Each entry describes a pattern comprehension that needs to be rendered as
    /// a CTE + LEFT JOIN at SQL generation time.
    pub pattern_comprehensions: Vec<PatternComprehensionMeta>,
}

impl WithClause {
    /// Validate that all projection items either have explicit aliases or can have aliases extracted.
    /// Complex expressions (aggregations, arithmetic, function calls) REQUIRE explicit aliases.
    fn validate_items(items: &[ProjectionItem]) -> Result<(), errors::LogicalPlanError> {
        for item in items {
            // Check if item has explicit alias or can extract one
            let has_alias = item.col_alias.is_some()
                || Self::extract_alias_from_expr(&item.expression).is_some();

            if !has_alias {
                // Item has no extractable alias - this is an error
                let expr_str = format!("{:?}", item.expression); // Use debug format for now
                return Err(errors::LogicalPlanError::WithClauseValidation(
                    format!("Expression without alias: `{}`. Complex expressions (aggregations, arithmetic, function calls) require explicit aliases. Use 'AS alias_name'.", expr_str)
                ));
            }
        }
        Ok(())
    }

    /// Create a new WithClause with just the essential fields.
    /// Returns an error if any item lacks a required alias.
    pub fn new(
        input: Arc<LogicalPlan>,
        items: Vec<ProjectionItem>,
    ) -> Result<Self, errors::LogicalPlanError> {
        // Validate items before proceeding
        Self::validate_items(&items)?;

        let exported_aliases = Self::extract_exported_aliases(&items);
        Ok(Self {
            input,
            items,
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases,
            cte_name: None,
            cte_references: std::collections::HashMap::new(),
            pattern_comprehensions: Vec::new(),
        })
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
                    op_app
                        .operands
                        .first()
                        .and_then(Self::extract_alias_from_expr)
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
    /// Pre-computed projected columns for this node (computed by GraphJoinInference analyzer)
    /// Format: Vec<(graph_property_name, db_column_qualified)>
    /// Examples:
    /// - Base table: vec![("name", "person.firstName"), ("age", "person.age")]
    /// - CTE reference: vec![("name", "with_p_cte_1.name")]
    /// - Denormalized: vec![("code", "flights.Origin"), ("city", "flights.OriginCity")]
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub projected_columns: Option<Vec<(String, String)>>,

    /// **NEW (Feb 2026)**: Node type candidates for multi-type inference
    /// When TypeInference finds an untyped node like `(n)`, it infers all
    /// possible node types from schema. CTE generation creates UNION of node tables.
    /// The `label` field contains the first type for backward compatibility.
    /// Example: Some(vec!["User", "Post", "ZeekLog"])
    /// Scope: THIS node only (not query-level)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub node_types: Option<Vec<String>>,
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

    /// CTE references for node connections
    /// Maps node alias → CTE name for left_connection and right_connection
    /// Example: {"b": "with_a_b_cte_1"} means left_connection="b" comes from CTE
    /// Allows renderer to generate: a_b.b_user_id instead of b.user_id in JOINs
    pub cte_references: std::collections::HashMap<String, String>,

    /// **NEW (Feb 2026)**: Pattern type combinations for multi-type inference
    /// When TypeInference finds ambiguous nodes in this pattern, it generates
    /// all valid (from_label, rel_type, to_label) combinations from schema.
    /// CTE generation creates UNION of full JOINs for each combination.
    /// Example: [(User, FOLLOWS, User), (User, AUTHORED, Post)]
    /// Scope: THIS pattern only (not query-level)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub pattern_combinations: Option<Vec<crate::query_planner::plan_ctx::TypeCombination>>,

    /// Set to true by BidirectionalUnion when this GraphRel was split from Direction::Either
    /// Allows downstream (CTE extraction) to know this was originally undirected
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub was_undirected: Option<bool>,
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

    /// Pre-computed joins from the planning phase (join_generation module).
    /// This is the single source of truth for join conditions and ordering.
    /// The rendering phase converts these to render_plan::Join 1:1.
    pub joins: Vec<Join>,

    /// Aliases that came from OPTIONAL MATCH clauses (for correct FROM table selection)
    pub optional_aliases: std::collections::HashSet<String>,

    /// The computed anchor table (FROM clause table)
    /// Computed during join reordering in graph_join_inference
    /// None = denormalized pattern (use relationship table directly)
    pub anchor_table: Option<String>,

    /// CTE references: Maps alias → CTE name for aliases exported from WITH clauses
    /// Used by render phase to resolve anchor table names correctly
    pub cte_references: std::collections::HashMap<String, String>,

    /// Cross-table correlation predicates from CartesianProduct.join_condition
    /// These predicates reference aliases from different graph patterns (e.g., WITH...MATCH)
    /// Used by renderer to generate proper JOIN conditions for CTEs
    /// Example: WHERE src2.ip = source_ip becomes JOIN ON condition
    pub correlation_predicates: Vec<LogicalExpr>,
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
    /// The ID column name from the source/left side of the relationship (if this is a relationship join)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub from_id_column: Option<String>,
    /// The ID column name from the target/right side of the relationship (if this is a relationship join)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to_id_column: Option<String>,
    /// For VLP joins, the original GraphRel for CTE generation
    #[serde(skip)]
    pub graph_rel: Option<Arc<GraphRel>>,
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

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Projection {
    #[serde(with = "serde_arc")]
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
    /// Indicates whether this projection comes from RETURN clause.
    /// Always Return since WITH clauses use the separate WithClause node.
    /// Whether DISTINCT should be applied to results
    pub distinct: bool,
    /// Pattern comprehension metadata for CTE+JOIN generation at render time.
    /// Populated when RETURN clause contains pattern comprehensions like `size([(a)--() | 1])`.
    #[serde(default)]
    pub pattern_comprehensions: Vec<PatternComprehensionMeta>,
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
    /// The label/type of elements being unwound (e.g., "Person", "Post")
    /// Used for property resolution when unwinding collected nodes
    /// Example: collect(u:Person) → UNWIND → user:Person
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub label: Option<String>,
    /// Tuple structure metadata for unwound arrays from collect(node)
    /// Maps property names to their positions in the tuple
    /// Example: [("city", 1), ("country", 2), ("email", 3), ...]
    /// Used to convert user.name → user.5 (tuple index access)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tuple_properties: Option<Vec<(String, usize)>>,
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

impl Unwind {
    /// Create a new Unwind node from an existing one, preserving all metadata
    /// while modifying the input plan. This ensures metadata like tuple_properties
    /// is automatically carried forward during query transformations.
    ///
    /// # Example
    /// ```rust
    /// use clickgraph::query_planner::logical_plan::{Unwind, LogicalPlan};
    /// use clickgraph::query_planner::logical_expr::{LogicalExpr, Literal};
    /// use std::sync::Arc;
    ///
    /// // Create a sample Unwind node
    /// let old_unwind = Unwind {
    ///     input: Arc::new(LogicalPlan::get_empty_match_plan()),
    ///     expression: LogicalExpr::Literal(Literal::String("test".to_string())),
    ///     alias: "test_alias".to_string(),
    ///     label: Some("test_label".to_string()),
    ///     tuple_properties: Some(vec![]),
    /// };
    ///
    /// // Create a transformed input plan
    /// let transformed_input = Arc::new(LogicalPlan::get_empty_match_plan());
    ///
    /// let new_unwind = old_unwind.with_new_input(transformed_input);
    /// // tuple_properties, label, expression, alias all preserved
    /// ```
    pub fn with_new_input(&self, new_input: Arc<LogicalPlan>) -> Self {
        Unwind {
            input: new_input,
            expression: self.expression.clone(),
            alias: self.alias.clone(),
            label: self.label.clone(),
            tuple_properties: self.tuple_properties.clone(),
        }
    }

    /// Create a new Unwind node with a different expression, preserving metadata
    pub fn with_new_expression(&self, new_expr: LogicalExpr) -> Self {
        Unwind {
            input: self.input.clone(),
            expression: new_expr,
            alias: self.alias.clone(),
            label: self.label.clone(),
            tuple_properties: self.tuple_properties.clone(),
        }
    }

    /// Standard rebuild_or_clone pattern used throughout the codebase.
    /// Automatically preserves all metadata fields.
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::Unwind(
                self.with_new_input(input_tf.get_plan()),
            ))
        })
    }
}

impl Filter {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::Filter(Filter {
                input: input_tf.get_plan(),
                predicate: self.predicate.clone(),
            }))
        })
    }
}

impl Projection {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::Projection(Projection {
                input: input_tf.get_plan(),
                items: self.items.clone(),
                distinct: self.distinct,
                pattern_comprehensions: self.pattern_comprehensions.clone(),
            }))
        })
    }
}

impl GroupBy {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::GroupBy(GroupBy {
                input: input_tf.get_plan(),
                expressions: self.expressions.clone(),
                having_clause: self.having_clause.clone(),
                is_materialization_boundary: self.is_materialization_boundary,
                exposed_alias: self.exposed_alias.clone(),
            }))
        })
    }
}

impl OrderBy {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::OrderBy(OrderBy {
                input: input_tf.get_plan(),
                items: self.items.clone(),
            }))
        })
    }
}

impl Skip {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::Skip(Skip {
                input: input_tf.get_plan(),
                count: self.count,
            }))
        })
    }
}

impl Limit {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::Limit(Limit {
                input: input_tf.get_plan(),
                count: self.count,
            }))
        })
    }
}

impl GraphNode {
    // pub fn rebuild_or_clone(&self, input_tf: Transformed<Arc<LogicalPlan>>, self_tf: Transformed<Arc<LogicalPlan>>, old_plan: Arc<LogicalPlan>) -> Transformed<Arc<LogicalPlan>> {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::GraphNode(GraphNode {
                input: input_tf.get_plan(),
                alias: self.alias.clone(),
                label: self.label.clone(),
                is_denormalized: self.is_denormalized,
                projected_columns: self.projected_columns.clone(),
                node_types: self.node_types.clone(),
            }))
        })
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
        if any_transformed(&[&left_tf, &center_tf, &right_tf]) {
            Transformed::Yes(Arc::new(LogicalPlan::GraphRel(GraphRel {
                left: left_tf.get_plan(),
                center: center_tf.get_plan(),
                right: right_tf.get_plan(),
                alias: self.alias.clone(),
                left_connection: self.left_connection.clone(),
                right_connection: self.right_connection.clone(),
                direction: self.direction.clone(),
                is_rel_anchor: self.is_rel_anchor,
                variable_length: self.variable_length.clone(),
                shortest_path_mode: self.shortest_path_mode.clone(),
                path_variable: self.path_variable.clone(),
                where_predicate: self.where_predicate.clone(),
                labels: self.labels.clone(),
                is_optional: self.is_optional,
                anchor_connection: self.anchor_connection.clone(),
                cte_references: std::collections::HashMap::new(),
                pattern_combinations: self.pattern_combinations.clone(),
                was_undirected: self.was_undirected,
            })))
        } else {
            Transformed::No(old_plan)
        }
    }
}

impl Cte {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            let new_input = input_tf.get_plan();
            // If new input is empty then remove the CTE
            if matches!(new_input.as_ref(), LogicalPlan::Empty) {
                new_input
            } else {
                Arc::new(LogicalPlan::Cte(Cte {
                    input: new_input,
                    name: self.name.clone(),
                }))
            }
        })
    }
}

impl GraphJoins {
    pub fn rebuild_or_clone(
        &self,
        input_tf: Transformed<Arc<LogicalPlan>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        handle_rebuild_or_clone(input_tf.is_yes(), old_plan, || {
            Arc::new(LogicalPlan::GraphJoins(GraphJoins {
                input: input_tf.get_plan(),
                joins: self.joins.clone(),
                optional_aliases: self.optional_aliases.clone(),
                anchor_table: self.anchor_table.clone(),
                cte_references: self.cte_references.clone(),
                correlation_predicates: vec![],
            }))
        })
    }
}

impl Union {
    pub fn rebuild_or_clone(
        &self,
        inputs_tf: Vec<Transformed<Arc<LogicalPlan>>>,
        old_plan: Arc<LogicalPlan>,
    ) -> Transformed<Arc<LogicalPlan>> {
        // Check if any input was transformed
        let is_transformed = inputs_tf.iter().any(|tf| tf.is_yes());

        if is_transformed {
            let new_inputs: Vec<Arc<LogicalPlan>> =
                inputs_tf.into_iter().map(|tf| tf.get_plan()).collect();
            Transformed::Yes(Arc::new(LogicalPlan::Union(Union {
                inputs: new_inputs,
                union_type: self.union_type.clone(),
            })))
        } else {
            Transformed::No(old_plan)
        }
    }
}

impl<'a> From<CypherReturnItem<'a>> for ProjectionItem {
    fn from(value: CypherReturnItem<'a>) -> Self {
        // Determine the column alias using this priority:
        // 1. Explicit AS alias (highest priority)
        // 2. Original text from the query (preserves user input exactly)
        // 3. Inferred from expression structure (fallback for backward compatibility)
        let col_alias = if let Some(explicit_alias) = value.alias {
            // Explicit AS alias takes precedence
            Some(ColumnAlias(explicit_alias.to_string()))
        } else if let Some(original_text) = value.original_text {
            // Use captured original expression text (Neo4j behavior)
            Some(ColumnAlias(original_text.to_string()))
        } else {
            // Fallback: infer from expression structure
            match &value.expression {
                // For property access like "u.name", use "u.name" as alias (Neo4j behavior)
                // Neo4j returns qualified names by default: RETURN u.name → column "u.name"
                CypherExpression::PropertyAccessExp(prop_access) => Some(ColumnAlias(format!(
                    "{}.{}",
                    prop_access.base, prop_access.key
                ))),
                // For simple variables like "u", use "u" as alias
                CypherExpression::Variable(var) => Some(ColumnAlias(var.to_string())),
                // For other expressions, no default alias
                _ => None,
            }
        };

        ProjectionItem {
            expression: LogicalExpr::try_from(value.expression)
                .expect("Failed to convert RETURN expression - invalid Cypher syntax"),
            col_alias,
        }
    }
}

impl<'a> TryFrom<WithItem<'a>> for ProjectionItem {
    type Error = crate::query_planner::logical_expr::errors::LogicalExprError;

    fn try_from(value: WithItem<'a>) -> Result<Self, Self::Error> {
        Ok(ProjectionItem {
            expression: LogicalExpr::try_from(value.expression)?,
            col_alias: value.alias.map(|alias| ColumnAlias(alias.to_string())),
        })
    }
}

impl<'a> TryFrom<CypherOrderByItem<'a>> for OrderByItem {
    type Error = crate::query_planner::logical_expr::errors::LogicalExprError;

    fn try_from(value: CypherOrderByItem<'a>) -> Result<Self, Self::Error> {
        Ok(OrderByItem {
            expression: if let CypherExpression::Variable(var) = value.expression {
                LogicalExpr::ColumnAlias(ColumnAlias(var.to_string()))
            } else {
                LogicalExpr::try_from(value.expression)?
            },
            order: match value.order {
                CypherOrerByOrder::Asc => OrderByOrder::Asc,
                CypherOrerByOrder::Desc => OrderByOrder::Desc,
            },
        })
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
            distinct: false,
            pattern_comprehensions: vec![],
        })
    }

    /// Check if this plan represents an optional pattern (from OPTIONAL MATCH).
    /// Returns true if the plan contains a GraphRel or GraphJoins marked as optional.
    /// This is used to determine proper anchor selection when combining required and
    /// optional patterns via CartesianProduct.
    pub fn is_optional_pattern(&self) -> bool {
        match self {
            // GraphRel with is_optional=Some(true) is an optional pattern
            LogicalPlan::GraphRel(rel) => rel.is_optional.unwrap_or(false),
            // GraphJoins can have optional=true from the underlying GraphRel
            LogicalPlan::GraphJoins(joins) => joins.input.is_optional_pattern(),
            // Recursively check through wrapper nodes
            LogicalPlan::GraphNode(node) => node.input.is_optional_pattern(),
            LogicalPlan::Filter(filter) => filter.input.is_optional_pattern(),
            LogicalPlan::Projection(proj) => proj.input.is_optional_pattern(),
            LogicalPlan::GroupBy(gb) => gb.input.is_optional_pattern(),
            LogicalPlan::OrderBy(ob) => ob.input.is_optional_pattern(),
            LogicalPlan::Skip(skip) => skip.input.is_optional_pattern(),
            LogicalPlan::Limit(limit) => limit.input.is_optional_pattern(),
            LogicalPlan::Cte(cte) => cte.input.is_optional_pattern(),
            LogicalPlan::Unwind(u) => u.input.is_optional_pattern(),
            // CartesianProduct: check if both sides are optional
            // (If either side is required, the overall pattern isn't purely optional)
            LogicalPlan::CartesianProduct(cp) => {
                cp.left.is_optional_pattern() && (cp.is_optional || cp.right.is_optional_pattern())
            }
            // Empty and other leaf nodes are not optional
            _ => false,
        }
    }

    /// Check if this plan tree contains a Union node at any depth.
    pub fn has_union_anywhere(&self) -> bool {
        match self {
            LogicalPlan::Union(_) => true,
            LogicalPlan::Limit(l) => l.input.has_union_anywhere(),
            LogicalPlan::Skip(s) => s.input.has_union_anywhere(),
            LogicalPlan::OrderBy(o) => o.input.has_union_anywhere(),
            LogicalPlan::Filter(f) => f.input.has_union_anywhere(),
            LogicalPlan::Projection(p) => p.input.has_union_anywhere(),
            LogicalPlan::GroupBy(gb) => gb.input.has_union_anywhere(),
            LogicalPlan::GraphJoins(gj) => gj.input.has_union_anywhere(),
            LogicalPlan::GraphNode(gn) => gn.input.has_union_anywhere(),
            LogicalPlan::GraphRel(gr) => {
                gr.left.has_union_anywhere()
                    || gr.center.has_union_anywhere()
                    || gr.right.has_union_anywhere()
            }
            LogicalPlan::CartesianProduct(cp) => {
                cp.left.has_union_anywhere() || cp.right.has_union_anywhere()
            }
            LogicalPlan::WithClause(wc) => wc.input.has_union_anywhere(),
            LogicalPlan::Unwind(u) => u.input.has_union_anywhere(),
            _ => false,
        }
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
            LogicalPlan::CartesianProduct(cp) => {
                format!("CartesianProduct(optional: {})", cp.is_optional)
            }
            LogicalPlan::WithClause(wc) => format!(
                "WithClause(items: {}, distinct: {})",
                wc.items.len(),
                wc.distinct
            ),
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
                cp.left.contains_variable_length_path() || cp.right.contains_variable_length_path()
            }
            LogicalPlan::WithClause(with_clause) => {
                with_clause.input.contains_variable_length_path()
            }
            // Leaf nodes
            LogicalPlan::ViewScan(_) | LogicalPlan::Empty | LogicalPlan::PageRank(_) => false,
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
        let new_input = Arc::new(LogicalPlan::Empty);

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
        let new_input = Arc::new(LogicalPlan::Empty);

        let projection_items = vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("customer".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "name".to_string(),
                ),
            }),
            col_alias: None,
        }];

        let projection = Projection {
            input: original_input.clone(),
            items: projection_items.clone(),
            distinct: false,
            pattern_comprehensions: vec![],
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
        let new_input = Arc::new(LogicalPlan::Empty);

        let graph_node = GraphNode {
            input: original_input.clone(),
            alias: "person".to_string(),
            label: None,
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
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
        let new_left_plan = Arc::new(LogicalPlan::Empty);

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
            cte_references: std::collections::HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
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
        let original_input = Arc::new(LogicalPlan::Empty);
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
            original_text: None,
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

        let order_by_item = OrderByItem::try_from(ast_order_item).unwrap();

        match order_by_item.expression {
            LogicalExpr::ColumnAlias(alias) => assert_eq!(alias.0, "price"),
            _ => panic!("Expected ColumnAlias"),
        }
        assert_eq!(order_by_item.order, OrderByOrder::Desc);
    }

    #[test]
    fn test_complex_logical_plan_structure() {
        // Create a complex plan: Projection -> Filter -> GraphNode -> Empty
        let scan = LogicalPlan::Empty;

        let graph_node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(scan),
            alias: "user".to_string(),
            label: None,
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        });

        let filter = LogicalPlan::Filter(Filter {
            input: Arc::new(graph_node),
            predicate: LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::GreaterThan,
                operands: vec![
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            "age".to_string(),
                        ),
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
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            "email".to_string(),
                        ),
                    }),
                    col_alias: Some(ColumnAlias("email_address".to_string())),
                },
                ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias("user".to_string()),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            "first_name".to_string(),
                        ),
                    }),
                    col_alias: None,
                },
            ],
            distinct: false,
            pattern_comprehensions: vec![],
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
                                LogicalPlan::Empty => {
                                    // Empty is the leaf node now
                                }
                                _ => panic!("Expected Empty at bottom"),
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
