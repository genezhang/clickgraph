//! SKIP and LIMIT clause processing.
//!
//! Handles Cypher's pagination clauses for result set limiting.
//!
//! # SQL Translation
//!
//! ```text
//! SKIP 10 LIMIT 20
//! → OFFSET 10 LIMIT 20
//! ```
//!
//! These clauses remain outside UNION operations, applying to combined results.

use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::{LimitClause, SkipClause},
    query_planner::logical_plan::{Limit, LogicalPlan, Skip},
};

pub fn evaluate_skip_clause(skip_clause: &SkipClause, plan: Arc<LogicalPlan>) -> Arc<LogicalPlan> {
    // Skip/Limit stay OUTSIDE Union - they apply to the combined result
    Arc::new(LogicalPlan::Skip(Skip {
        input: plan,
        count: skip_clause.skip_item,
    }))
}

pub fn evaluate_limit_clause(
    limit_clause: &LimitClause,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    // Skip/Limit stay OUTSIDE Union - they apply to the combined result
    Arc::new(LogicalPlan::Limit(Limit {
        input: plan,
        count: limit_clause.limit_item,
    }))
}
