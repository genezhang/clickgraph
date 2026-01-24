pub mod alias_resolver;
pub mod cte_extraction;
pub mod cte_generation;
pub mod cte_manager;
mod expression_utils;
mod feature_flags;
mod filter_builder;
mod filter_pipeline;
mod from_builder;
mod from_table;
mod group_by_builder;
mod join_builder;
mod plan_builder_helpers;
mod plan_builder_utils;
mod properties_builder;
pub mod property_expansion;
mod select_builder;
pub mod types;
pub mod utils;
mod view_table_ref;

use errors::RenderBuildError;
use render_expr::{ColumnAlias, OperatorApplication, RenderExpr};
use std::cell::RefCell;

pub use cte_generation::CteGenerationContext;
pub use cte_manager::{
    CteColumnMetadata, CteError, CteGenerationResult, CteManager, CteStrategy, VlpEndpointInfo,
};
pub use filter_pipeline::CategorizedFilters;
pub use from_table::FromTable;
pub use view_table_ref::ViewTableRef;

// Thread-local storage for CTE column registry during render phase
// This allows select_builder and filter_builder to resolve property access expressions
// for WITH-exported variables to their CTE output column names
thread_local! {
    static CTE_COLUMN_REGISTRY_CONTEXT: RefCell<Option<CteColumnRegistry>> = RefCell::new(None);
}

/// Set the CTE column registry for the current render phase
/// This should be called before rendering a logical plan that references WITH-exported variables
pub fn set_cte_column_registry(registry: CteColumnRegistry) {
    CTE_COLUMN_REGISTRY_CONTEXT.with(|cell| {
        *cell.borrow_mut() = Some(registry);
    });
}

/// Get the CTE column registry for property resolution
pub fn get_cte_column_registry() -> Option<CteColumnRegistry> {
    CTE_COLUMN_REGISTRY_CONTEXT.with(|cell| cell.borrow().clone())
}

/// Clear the CTE column registry after rendering is complete
pub fn clear_cte_column_registry() {
    CTE_COLUMN_REGISTRY_CONTEXT.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

// Denormalized edge alias mapping: maps target node alias to edge alias
// For denormalized edges (e.g., AUTHORED where posts_bench is both edge and target node),
// when we skip the second JOIN, we need to map the target node alias to the edge alias
// so property resolution works correctly
thread_local! {
    static DENORMALIZED_EDGE_ALIASES: RefCell<std::collections::HashMap<String, String>> = 
        RefCell::new(std::collections::HashMap::new());
}

/// Register an alias mapping for denormalized edges
/// Maps target_node_alias → edge_alias (e.g., "d" → "r2")
pub fn register_denormalized_alias(target_node_alias: &str, edge_alias: &str) {
    DENORMALIZED_EDGE_ALIASES.with(|cell| {
        cell.borrow_mut().insert(target_node_alias.to_string(), edge_alias.to_string());
    });
}

/// Look up the edge alias for a target node alias (if denormalized)
pub fn get_denormalized_alias_mapping(target_node_alias: &str) -> Option<String> {
    DENORMALIZED_EDGE_ALIASES.with(|cell| {
        cell.borrow().get(target_node_alias).cloned()
    })
}

/// Clear all denormalized alias mappings after rendering is complete
pub fn clear_denormalized_aliases() {
    DENORMALIZED_EDGE_ALIASES.with(|cell| {
        cell.borrow_mut().clear();
    });
}

use crate::query_planner::join_context::{
    VLP_CTE_FROM_ALIAS, VLP_END_ID_COLUMN, VLP_START_ID_COLUMN,
};
use crate::query_planner::logical_plan::{
    Join as LogicalJoin, JoinType as LogicalJoinType, OrderByItem as LogicalOrderByItem,
    OrderByOrder as LogicalOrderByOrder, UnionType as LogicalUnionType,
};

use serde::{Deserialize, Serialize};
use std::fmt;

pub mod errors;
pub mod plan_builder;
pub mod render_expr;
pub mod render_plan;
pub mod view_plan;

pub use render_plan::CteColumnRegistry;

#[cfg(test)]
mod tests;

pub trait ToSql {
    fn to_sql(&self) -> String;
}

/// Convert a LogicalPlan to a RenderPlan
pub fn logical_plan_to_render_plan(
    logical_plan: crate::query_planner::logical_plan::LogicalPlan,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<RenderPlan, errors::RenderBuildError> {
    use plan_builder::RenderPlanBuilder;
    logical_plan.to_render_plan(schema)
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
    /// Fixed path information for simple (non-VLP) path patterns
    /// Contains path variable name and hop count for queries like:
    /// MATCH p = (a)-[:T]->(b) RETURN length(p)
    pub fixed_path_info: Option<FixedPathMetadata>,
    /// Per-query CTE column registry - maps (alias, property) to CTE output column name
    /// Used during SQL rendering to resolve property accesses from WITH clauses
    #[serde(skip)] // Don't serialize, recreate during rendering
    pub cte_column_registry: CteColumnRegistry,
}

/// Metadata for simple/fixed path patterns (non-VLP)
/// Used to render path functions like length(p), nodes(p), relationships(p)
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct FixedPathMetadata {
    /// Path variable name from Cypher (e.g., "p")
    pub path_variable: String,
    /// Number of relationships/hops in the path (e.g., 1 for (a)-[r]->(b))
    pub hop_count: u32,
    /// List of node table aliases in order: [start_alias, intermediate1, ..., end_alias]
    pub node_aliases: Vec<String>,
    /// List of relationship aliases (e.g., ["r"])
    pub rel_aliases: Vec<String>,
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
    /// Pre-filter for LEFT JOIN subquery form:
    /// `LEFT JOIN (SELECT * FROM table WHERE pre_filter) AS alias ON ...`
    /// This is used for schema filters and OPTIONAL MATCH WHERE clauses
    /// to ensure correct LEFT JOIN semantics (filter BEFORE join, not after).
    pub pre_filter: Option<RenderExpr>,
    /// For relationship tables: the source node ID column name (e.g., "Person1Id", "from_id")
    /// Used for NULL checks: `r IS NULL` → `r.from_id IS NULL`
    /// Extracted during planning from schema lookups, NOT from joining_on (to avoid circular logic)
    pub from_id_column: Option<String>,
    /// For relationship tables: the target node ID column name (e.g., "Person2Id", "to_id")
    pub to_id_column: Option<String>,
    /// For VLP joins, the original GraphRel for CTE generation
    #[serde(skip)]
    pub graph_rel: Option<std::sync::Arc<crate::query_planner::logical_plan::GraphRel>>,
}

impl Join {
    /// Get the relationship ID column for NULL checks.
    /// Returns from_id_column if set (populated during planning from schema),
    /// otherwise extracts from JOIN condition as fallback.
    /// For `LEFT JOIN Person_knows_Person AS k ON k.Person1Id = a.id`,
    /// returns "Person1Id".
    pub fn get_relationship_id_column(&self) -> Option<String> {
        // First priority: use from_id_column if explicitly set during planning
        if let Some(ref col) = self.from_id_column {
            return Some(col.clone());
        }

        // Fallback: extract from joining_on condition
        if let Some(first_condition) = self.joining_on.first() {
            if first_condition.operands.len() >= 2 {
                // Check if first operand is PropertyAccess with our table_alias
                if let RenderExpr::PropertyAccessExp(prop) = &first_condition.operands[0] {
                    if prop.table_alias.0 == self.table_alias {
                        return Some(prop.column.raw().to_string());
                    }
                }
                // Sometimes the order is reversed: other.column = alias.column
                if let RenderExpr::PropertyAccessExp(prop) = &first_condition.operands[1] {
                    if prop.table_alias.0 == self.table_alias {
                        return Some(prop.column.raw().to_string());
                    }
                }
            }
        }
        None
    }
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
/// Example: UNWIND [1,2] AS x UNWIND [10,20] AS y
/// Generates: ARRAY JOIN [1,2] AS x ARRAY JOIN [10,20] AS y
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Default)]
pub struct ArrayJoinItem(pub Vec<ArrayJoin>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ArrayJoin {
    /// The expression to array join (must be an array type)
    pub expression: RenderExpr,
    /// The alias for each unwound element
    pub alias: String,
}

impl TryFrom<LogicalJoinType> for JoinType {
    type Error = RenderBuildError;

    fn try_from(value: LogicalJoinType) -> Result<Self, Self::Error> {
        let join_type = match value {
            LogicalJoinType::Join => JoinType::Join,
            LogicalJoinType::Inner => JoinType::Inner,
            LogicalJoinType::Left => JoinType::Left,
            LogicalJoinType::Right => JoinType::Right,
        };
        Ok(join_type)
    }
}

impl TryFrom<LogicalJoin> for Join {
    type Error = RenderBuildError;

    fn try_from(value: LogicalJoin) -> Result<Self, Self::Error> {
        // Convert pre_filter from LogicalExpr to RenderExpr if present
        let pre_filter = if let Some(logical_pre_filter) = value.pre_filter {
            RenderExpr::try_from(logical_pre_filter).ok()
        } else {
            None
        };

        let join = Join {
            table_alias: value.table_alias,
            table_name: value.table_name,
            joining_on: value
                .joining_on
                .clone()
                .into_iter()
                .map(OperatorApplication::try_from)
                .collect::<Result<Vec<OperatorApplication>, RenderBuildError>>()?,
            join_type: value.join_type.clone().try_into()?,
            pre_filter,
            from_id_column: value.from_id_column,
            to_id_column: value.to_id_column,
            graph_rel: None,
        };
        Ok(join)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CteItems(pub Vec<Cte>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum CteContent {
    Structured(RenderPlan),
    RawSql(String),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Cte {
    pub cte_name: String,
    pub content: CteContent,
    pub is_recursive: bool,
    // VLP endpoint information (only populated for VLP CTEs)
    pub vlp_start_alias: Option<String>, // Internal alias used in VLP CTE (e.g., "start_node")
    pub vlp_end_alias: Option<String>,   // Internal alias used in VLP CTE (e.g., "end_node")
    pub vlp_start_table: Option<String>, // Start node table name (e.g., "ldbc.Message")
    pub vlp_end_table: Option<String>,   // End node table name (e.g., "ldbc.Post")
    pub vlp_cypher_start_alias: Option<String>, // Original Cypher alias for start node (e.g., "m")
    pub vlp_cypher_end_alias: Option<String>, // Original Cypher alias for end node (e.g., "p")
    // 🔧 FIX: Store actual ID columns from relationship schema (not node schema)
    // For zeek DNS: start_id_col = "id.orig_h", end_id_col = "query" (from DNS_REQUESTED relationship)
    // NOT Domain.node_id = "name" (that's the logical name, not the column)
    pub vlp_start_id_col: Option<String>, // Actual ID column for start node JOIN (from rel.from_id)
    pub vlp_end_id_col: Option<String>,   // Actual ID column for end node JOIN (from rel.to_id)
    // Path variable name from Cypher (e.g., "p" in MATCH p = (a)-[*]->(b))
    // Used for rewriting path functions like length(p) → hop_count
    pub vlp_path_variable: Option<String>,
    /// Column metadata for deterministic column lookup (from CteGenerationResult)
    /// This eliminates heuristic column guessing in plan_builder_utils.rs
    pub columns: Vec<CteColumnMetadata>,
    /// The table alias to use when referencing this CTE's columns (e.g., "t" for VLP CTEs)
    pub from_alias: Option<String>,
}

impl Cte {
    /// Create a new non-VLP CTE
    pub fn new(cte_name: String, content: CteContent, is_recursive: bool) -> Self {
        Self {
            cte_name,
            content,
            is_recursive,
            vlp_start_alias: None,
            vlp_end_alias: None,
            vlp_start_table: None,
            vlp_end_table: None,
            vlp_cypher_start_alias: None,
            vlp_cypher_end_alias: None,
            vlp_start_id_col: None,
            vlp_end_id_col: None,
            vlp_path_variable: None,
            columns: Vec::new(),
            from_alias: None,
        }
    }

    /// Create a new VLP CTE with endpoint information
    pub fn new_vlp(
        cte_name: String,
        content: CteContent,
        is_recursive: bool,
        start_alias: String,
        end_alias: String,
        start_table: String,
        end_table: String,
        cypher_start_alias: String,
        cypher_end_alias: String,
        start_id_col: String, // 🔧 FIX: Add ID columns from relationship schema
        end_id_col: String,
        path_variable: Option<String>, // Path variable name from Cypher (e.g., "p")
    ) -> Self {
        Self {
            cte_name,
            content,
            is_recursive,
            vlp_start_alias: Some(start_alias),
            vlp_end_alias: Some(end_alias),
            vlp_start_table: Some(start_table),
            vlp_end_table: Some(end_table),
            vlp_cypher_start_alias: Some(cypher_start_alias.clone()),
            vlp_cypher_end_alias: Some(cypher_end_alias.clone()),
            vlp_start_id_col: Some(start_id_col.clone()),
            vlp_end_id_col: Some(end_id_col.clone()),
            vlp_path_variable: path_variable,
            // Generate basic VLP column metadata from the endpoint info
            columns: vec![
                CteColumnMetadata {
                    cte_column_name: VLP_START_ID_COLUMN.to_string(),
                    cypher_alias: cypher_start_alias.clone(),
                    cypher_property: start_id_col.clone(),
                    db_column: start_id_col.clone(),
                    is_id_column: true,
                    vlp_position: Some(cte_manager::VlpColumnPosition::Start),
                },
                CteColumnMetadata {
                    cte_column_name: VLP_END_ID_COLUMN.to_string(),
                    cypher_alias: cypher_end_alias.clone(),
                    cypher_property: end_id_col.clone(),
                    db_column: end_id_col.clone(),
                    is_id_column: true,
                    vlp_position: Some(cte_manager::VlpColumnPosition::End),
                },
            ],
            from_alias: Some(VLP_CTE_FROM_ALIAS.to_string()), // VLP CTEs use standard FROM alias
        }
    }

    /// Create a new VLP CTE with full column metadata from CteGenerationResult
    pub fn new_vlp_with_columns(
        cte_name: String,
        content: CteContent,
        is_recursive: bool,
        start_alias: String,
        end_alias: String,
        start_table: String,
        end_table: String,
        cypher_start_alias: String,
        cypher_end_alias: String,
        start_id_col: String,
        end_id_col: String,
        path_variable: Option<String>,
        columns: Vec<CteColumnMetadata>,
        from_alias: String,
    ) -> Self {
        Self {
            cte_name,
            content,
            is_recursive,
            vlp_start_alias: Some(start_alias),
            vlp_end_alias: Some(end_alias),
            vlp_start_table: Some(start_table),
            vlp_end_table: Some(end_table),
            vlp_cypher_start_alias: Some(cypher_start_alias),
            vlp_cypher_end_alias: Some(cypher_end_alias),
            vlp_start_id_col: Some(start_id_col),
            vlp_end_id_col: Some(end_id_col),
            vlp_path_variable: path_variable,
            columns,
            from_alias: Some(from_alias),
        }
    }

    /// Get the ID column metadata for a given Cypher alias
    pub fn get_id_column_for_alias(&self, alias: &str) -> Option<&CteColumnMetadata> {
        self.columns
            .iter()
            .find(|c| c.cypher_alias == alias && c.is_id_column)
    }

    /// Get all columns for a given Cypher alias  
    pub fn get_columns_for_alias(&self, alias: &str) -> Vec<&CteColumnMetadata> {
        self.columns
            .iter()
            .filter(|c| c.cypher_alias == alias)
            .collect()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct UnionItems(pub Option<Union>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Union {
    pub input: Vec<RenderPlan>,
    pub union_type: UnionType,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum UnionType {
    Distinct,
    All,
}

#[derive(Debug, PartialEq, Clone)]
pub struct InSubquery {
    pub expr: RenderExpr,
    pub subplan: SubquerySubPlan,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SubquerySubPlan {
    pub select: SelectItems,
    pub from: FromTable,
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

impl TryFrom<LogicalOrderByItem> for OrderByItem {
    type Error = RenderBuildError;

    fn try_from(value: LogicalOrderByItem) -> Result<Self, Self::Error> {
        let order_by_item = OrderByItem {
            expression: value.expression.try_into()?,
            order: value.order.try_into()?,
        };
        Ok(order_by_item)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum OrderByOrder {
    Asc,
    Desc,
}

impl TryFrom<LogicalUnionType> for UnionType {
    type Error = RenderBuildError;

    fn try_from(value: LogicalUnionType) -> Result<Self, Self::Error> {
        let union_type = match value {
            LogicalUnionType::Distinct => UnionType::Distinct,
            LogicalUnionType::All => UnionType::All,
        };
        Ok(union_type)
    }
}

impl TryFrom<LogicalOrderByOrder> for OrderByOrder {
    type Error = RenderBuildError;

    fn try_from(value: LogicalOrderByOrder) -> Result<Self, Self::Error> {
        let order_by = match value {
            LogicalOrderByOrder::Asc => OrderByOrder::Asc,
            LogicalOrderByOrder::Desc => OrderByOrder::Desc,
        };
        Ok(order_by)
    }
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
