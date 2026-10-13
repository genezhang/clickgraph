pub mod cte_generation;
pub mod cte_manager;
pub(crate) mod expression_utils;
mod filter_pipeline;
pub(crate) mod filter_sql;
mod from_table;
mod view_table_ref;

use render_expr::{ColumnAlias, OperatorApplication, RenderExpr};

pub use cte_generation::CteGenerationContext;
pub use cte_manager::{CteError, CteGenerationResult, CteManager};
pub use filter_pipeline::CategorizedFilters;
pub use from_table::FromTable;
pub use view_table_ref::ViewTableRef;

/// Maximum recursion depth for plan/expression tree traversal functions.
/// Prevents stack overflow and OOM from pathologically deep or cyclic plan trees.
/// Normal queries rarely exceed depth 50; 200 is generous while protecting against DoS.
pub const MAX_TRAVERSAL_DEPTH: usize = 200;

/// Maximum total number of nodes allowed in a plan tree.
/// Prevents OOM from plan width explosion (e.g., UNION distribution through deep trees
/// can create N^D nodes where N = branch count, D = depth).
/// Normal queries produce plans with < 100 nodes; 10,000 is generous.
pub const MAX_PLAN_NODES: usize = 10_000;

use serde::{Deserialize, Serialize};
use std::fmt;

pub mod errors;
pub mod render_expr;
pub mod write_render;

pub use write_render::{DeleteOp, InsertOp, RowSource, UpdateOp, WriteRenderPlan};

pub trait ToSql {
    fn to_sql(&self) -> String;
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RenderPlan {
    pub ctes: CteItems,
    pub select: SelectItems,
    pub from: FromTableItem,
    pub joins: JoinItems,
    pub array_join: ArrayJoinItem,
    pub filters: FilterItems,
    pub group_by: GroupByExpressions,
    pub having_clause: Option<RenderExpr>, // HAVING clause for post-aggregation filtering
    pub order_by: OrderByItems,
    pub skip: SkipItem,
    pub limit: LimitItem,
    pub union: UnionItems,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SelectItems {
    pub items: Vec<SelectItem>,
    pub distinct: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SelectItem {
    pub expression: RenderExpr,
    pub col_alias: Option<ColumnAlias>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct FromTableItem(pub Option<ViewTableRef>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct FilterItems(pub Option<RenderExpr>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct GroupByExpressions(pub Vec<RenderExpr>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct JoinItems(pub Vec<Join>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Join {
    pub table_name: String,
    pub table_alias: String,
    pub joining_on: Vec<OperatorApplication>,
    pub join_type: JoinType,
    /// Read the table with `FINAL` (its merged rows: `JOIN t AS a FINAL`),
    /// where the dialect has it (P-4c S11k).
    #[serde(default)]
    pub use_final: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Join,
    Inner,
    Left,
    Right,
}

/// ARRAY JOIN items for ClickHouse
/// Maps from Cypher UNWIND clauses (supports multiple for cartesian product)
///
/// Example: `UNWIND [1,2] AS x UNWIND [10,20] AS y`
/// Generates: `ARRAY JOIN [1,2] AS x ARRAY JOIN [10,20] AS y`
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct ArrayJoinItem(pub Vec<ArrayJoin>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ArrayJoin {
    /// The expression to array join (must be an array type)
    pub expression: RenderExpr,
    /// The alias for each unwound element
    pub alias: String,
    /// The alias of each element's position in its list, a number that
    /// increases along the list (the lowering's UNWIND, which keeps list
    /// order): read with the list, which is evaluated once.
    #[serde(default)]
    pub position: Option<String>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CteItems(pub Vec<Cte>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum CteContent {
    Structured(Box<RenderPlan>),
    RawSql(String),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Cte {
    pub cte_name: String,
    pub content: CteContent,
    pub is_recursive: bool,
}

impl Cte {
    pub fn new(cte_name: String, content: CteContent, is_recursive: bool) -> Self {
        Self {
            cte_name,
            content,
            is_recursive,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct UnionItems(pub Option<Union>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Union {
    pub input: Vec<RenderPlan>,
    pub union_type: UnionType,
    /// True when this comes from an explicit Cypher `UNION` clause: each
    /// branch is an independent complete query, so aggregation / GROUP BY /
    /// ORDER BY / LIMIT apply WITHIN each branch and must never be hoisted
    /// over the union (#487). False for planner-internal unions (multi-label
    /// scans, denormalized from/to, bidirectional expansion) where outer
    /// aggregation applies OVER the combined branches.
    #[serde(default)]
    pub is_cypher_union: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum UnionType {
    Distinct,
    All,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LimitItem(pub Option<i64>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SkipItem(pub Option<i64>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OrderByItems(pub Vec<OrderByItem>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expression: RenderExpr,
    pub order: OrderByOrder,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum OrderByOrder {
    Asc,
    Desc,
}

impl fmt::Display for RenderPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "---- RenderPlan ----")?;
        writeln!(f, "\nCTEs: {:?}", self.ctes)?;
        writeln!(f, "\nSELECT: {:?}", self.select)?;
        writeln!(f, "\nFROM: {:?}", self.from)?;
        writeln!(f, "\nJOINS: {:?}", self.joins)?;
        writeln!(f, "\nARRAY JOIN: {:?}", self.array_join)?;
        writeln!(f, "\nFILTERS: {:?}", self.filters)?;
        writeln!(f, "\nGROUP BY: {:?}", self.group_by)?;
        writeln!(f, "\nHAVING: {:?}", self.having_clause)?;
        writeln!(f, "\nORDER BY: {:?}", self.order_by)?;
        writeln!(f, "\nLIMIT: {:?}", self.limit)?;
        writeln!(f, "\nSKIP: {:?}", self.skip)?;
        writeln!(f, "-------------------")
    }
}
