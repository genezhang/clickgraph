mod view_table_ref;
mod from_table;

use errors::RenderBuildError;
use render_expr::{ColumnAlias, OperatorApplication, RenderExpr};

pub use view_table_ref::ViewTableRef;
pub use from_table::FromTable;

use crate::query_planner::logical_plan::{
    Join as LogicalJoin, JoinType as LogicalJoinType, OrderByItem as LogicalOrderByItem,
    OrderByOrder as LogicalOrderByOrder, UnionType as LogicalUnionType,
};

use std::fmt;
use serde::{Deserialize, Serialize};

pub mod errors;
pub mod plan_builder;
pub mod render_expr;
pub mod view_plan;

#[cfg(test)]
mod tests;



pub trait ToSql {
    fn to_sql(&self) -> String;
}

/// Convert a LogicalPlan to a RenderPlan
pub fn logical_plan_to_render_plan(logical_plan: crate::query_planner::logical_plan::LogicalPlan) -> Result<RenderPlan, errors::RenderBuildError> {
    use plan_builder::RenderPlanBuilder;
    logical_plan.to_render_plan()
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RenderPlan {
    pub ctes: CteItems,
    pub select: SelectItems,
    pub from: FromTableItem,
    pub joins: JoinItems,
    pub filters: FilterItems,
    pub group_by: GroupByExpressions,
    pub order_by: OrderByItems,
    pub skip: SkipItem,
    pub limit: LimitItem,
    pub union: UnionItems,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SelectItems(pub Vec<SelectItem>);

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
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Join,
    Inner,
    Left,
    Right,
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
        writeln!(f, "\nFILTERS: {:?}", self.filters)?;
        writeln!(f, "\nGROUP BY: {:?}", self.group_by)?;
        writeln!(f, "\nORDER BY: {:?}", self.order_by)?;
        writeln!(f, "\nLIMIT: {:?}", self.limit)?;
        writeln!(f, "\nSKIP: {:?}", self.skip)?;
        writeln!(f, "-------------------")
    }
}
