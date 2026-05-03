//! Write guard — centralised admission checks for write `LogicalPlan` variants.
//!
//! Per the embedded-writes design (Phase 0):
//! - **Decision 0.1**: Writes are only admitted when the executor is the
//!   embedded chdb backend. Server mode and SQL-only/remote bindings reject.
//! - **Decision 0.3**: Source-backed (read-only) targets are rejected at plan
//!   time (also enforced upstream in `write_clause_builder`; replicated here
//!   for defence-in-depth).
//! - **Decision 0.6**: FK-edge writes are rejected (also enforced upstream).
//!
//! Call sites: the embedded `Connection::query()` should call
//! `ensure_write_target_writable` after the planner produces a `LogicalPlan`
//! and before the renderer executes it.
//!
//! The HTTP/Bolt server retains its existing reject at
//! `src/server/handlers.rs:1356` as a separate defence-in-depth check —
//! this guard does not replace it.

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::logical_plan::{CreatePattern, LogicalPlan},
};

use thiserror::Error;

/// Identifies the execution backend the planner is producing SQL for.
///
/// Only `EmbeddedChdb` may execute write `LogicalPlan` variants. The other
/// kinds will be rejected by `ensure_write_target_writable` if a write
/// variant appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorKind {
    /// Embedded chdb (`Database::new(...)` with the `embedded` feature).
    /// Writes are permitted.
    EmbeddedChdb,
    /// Translation-only mode (`Database::sql_only(...)`). No execution.
    SqlOnly,
    /// External ClickHouse via HTTP (`Database::new_remote(...)`). Writes rejected.
    Remote,
    /// HTTP / Bolt server. Writes rejected.
    Server,
}

#[derive(Debug, Clone, Error, PartialEq)]
pub enum WriteGuardError {
    #[error(
        "Cypher write clauses (CREATE/SET/DELETE/REMOVE) are only supported in embedded chdb mode; \
         this connection is `{0:?}`. Use `Database::new(schema, SystemConfig)` (with the `embedded` \
         feature) to enable writes."
    )]
    ExecutorNotWritable(ExecutorKind),

    #[error(
        "Cannot write to label `{label}`: it resolves to a source-backed (read-only) table. \
         Source-backed schemas (Parquet/S3/Iceberg/Delta) are read-only by design."
    )]
    SourceBackedTarget { label: String },

    #[error(
        "Cannot write to relationship type `{rel_type}`: it is an FK-edge schema, which is not \
         writable in v1. Declare a standard edge-table schema (separate edge table with \
         from_id/to_id columns) to enable writes."
    )]
    FkEdgeTarget { rel_type: String },

    #[error("Unknown node label `{0}` referenced in write clause")]
    UnknownNodeLabel(String),

    #[error("Unknown relationship type `{0}` referenced in write clause")]
    UnknownRelationshipType(String),
}

/// Walk the plan tree, and if any write variant is present, enforce decisions
/// 0.1, 0.3 and 0.6.
///
/// Read-only plans pass through silently (no traversal cost beyond a single
/// dispatch on the root variant).
pub fn ensure_write_target_writable(
    plan: &LogicalPlan,
    schema: &GraphSchema,
    executor: ExecutorKind,
) -> Result<(), WriteGuardError> {
    if !plan_contains_write(plan) {
        return Ok(());
    }
    if executor != ExecutorKind::EmbeddedChdb {
        return Err(WriteGuardError::ExecutorNotWritable(executor));
    }
    check_writes_recursive(plan, schema)
}

/// Cheap structural check: does this plan tree contain any write variant?
fn plan_contains_write(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Create(_)
        | LogicalPlan::SetProperties(_)
        | LogicalPlan::Delete(_)
        | LogicalPlan::Remove(_) => true,

        LogicalPlan::GraphNode(gn) => plan_contains_write(&gn.input),
        LogicalPlan::GraphRel(gr) => {
            plan_contains_write(&gr.left)
                || plan_contains_write(&gr.center)
                || plan_contains_write(&gr.right)
        }
        LogicalPlan::Filter(f) => plan_contains_write(&f.input),
        LogicalPlan::Projection(p) => plan_contains_write(&p.input),
        LogicalPlan::GroupBy(gb) => plan_contains_write(&gb.input),
        LogicalPlan::OrderBy(ob) => plan_contains_write(&ob.input),
        LogicalPlan::Skip(s) => plan_contains_write(&s.input),
        LogicalPlan::Limit(l) => plan_contains_write(&l.input),
        LogicalPlan::Cte(cte) => plan_contains_write(&cte.input),
        LogicalPlan::GraphJoins(gj) => plan_contains_write(&gj.input),
        LogicalPlan::Union(u) => u.inputs.iter().any(|i| plan_contains_write(i)),
        LogicalPlan::Unwind(uw) => plan_contains_write(&uw.input),
        LogicalPlan::CartesianProduct(cp) => {
            plan_contains_write(&cp.left) || plan_contains_write(&cp.right)
        }
        LogicalPlan::WithClause(wc) => plan_contains_write(&wc.input),

        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => false,
    }
}

fn check_writes_recursive(plan: &LogicalPlan, schema: &GraphSchema) -> Result<(), WriteGuardError> {
    match plan {
        LogicalPlan::Create(c) => {
            for pattern in &c.patterns {
                check_create_pattern(pattern, schema)?;
            }
            check_writes_recursive(&c.input, schema)
        }
        LogicalPlan::SetProperties(sp) => check_writes_recursive(&sp.input, schema),
        LogicalPlan::Delete(d) => check_writes_recursive(&d.input, schema),
        LogicalPlan::Remove(r) => check_writes_recursive(&r.input, schema),

        LogicalPlan::GraphNode(gn) => check_writes_recursive(&gn.input, schema),
        LogicalPlan::GraphRel(gr) => {
            check_writes_recursive(&gr.left, schema)?;
            check_writes_recursive(&gr.center, schema)?;
            check_writes_recursive(&gr.right, schema)
        }
        LogicalPlan::Filter(f) => check_writes_recursive(&f.input, schema),
        LogicalPlan::Projection(p) => check_writes_recursive(&p.input, schema),
        LogicalPlan::GroupBy(gb) => check_writes_recursive(&gb.input, schema),
        LogicalPlan::OrderBy(ob) => check_writes_recursive(&ob.input, schema),
        LogicalPlan::Skip(s) => check_writes_recursive(&s.input, schema),
        LogicalPlan::Limit(l) => check_writes_recursive(&l.input, schema),
        LogicalPlan::Cte(cte) => check_writes_recursive(&cte.input, schema),
        LogicalPlan::GraphJoins(gj) => check_writes_recursive(&gj.input, schema),
        LogicalPlan::Unwind(uw) => check_writes_recursive(&uw.input, schema),
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                check_writes_recursive(input, schema)?;
            }
            Ok(())
        }
        LogicalPlan::CartesianProduct(cp) => {
            check_writes_recursive(&cp.left, schema)?;
            check_writes_recursive(&cp.right, schema)
        }
        LogicalPlan::WithClause(wc) => check_writes_recursive(&wc.input, schema),

        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => Ok(()),
    }
}

fn check_create_pattern(
    pattern: &CreatePattern,
    schema: &GraphSchema,
) -> Result<(), WriteGuardError> {
    match pattern {
        CreatePattern::Node(node) => {
            let node_schema = schema
                .node_schema_opt(&node.label)
                .ok_or_else(|| WriteGuardError::UnknownNodeLabel(node.label.clone()))?;
            if node_schema.source.is_some() {
                return Err(WriteGuardError::SourceBackedTarget {
                    label: node.label.clone(),
                });
            }
        }
        CreatePattern::Rel(rel) => {
            let rel_schema = schema
                .get_relationships_schema_opt(&rel.rel_type)
                .ok_or_else(|| WriteGuardError::UnknownRelationshipType(rel.rel_type.clone()))?;
            if rel_schema.source.is_some() {
                return Err(WriteGuardError::SourceBackedTarget {
                    label: rel.rel_type.clone(),
                });
            }
            if rel_schema.is_fk_edge {
                return Err(WriteGuardError::FkEdgeTarget {
                    rel_type: rel.rel_type.clone(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_plan::{Create, CreateNode, Delete};
    use std::sync::Arc;

    fn empty_schema() -> GraphSchema {
        GraphSchema::build(
            0,
            "test".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        )
    }

    #[test]
    fn read_only_plan_passes_in_any_executor() {
        let plan = LogicalPlan::Empty;
        let schema = empty_schema();
        for kind in [
            ExecutorKind::EmbeddedChdb,
            ExecutorKind::SqlOnly,
            ExecutorKind::Remote,
            ExecutorKind::Server,
        ] {
            assert!(ensure_write_target_writable(&plan, &schema, kind).is_ok());
        }
    }

    #[test]
    fn write_plan_rejected_outside_embedded() {
        let plan = LogicalPlan::Delete(Delete {
            input: Arc::new(LogicalPlan::Empty),
            targets: vec!["a".to_string()],
            detach: false,
        });
        let schema = empty_schema();
        for kind in [
            ExecutorKind::SqlOnly,
            ExecutorKind::Remote,
            ExecutorKind::Server,
        ] {
            let err = ensure_write_target_writable(&plan, &schema, kind).unwrap_err();
            assert!(matches!(err, WriteGuardError::ExecutorNotWritable(_)));
        }
    }

    #[test]
    fn unknown_label_rejected_in_embedded() {
        let plan = LogicalPlan::Create(Create {
            input: Arc::new(LogicalPlan::Empty),
            patterns: vec![CreatePattern::Node(CreateNode {
                alias: Some("a".to_string()),
                label: "DoesNotExist".to_string(),
                properties: vec![],
            })],
        });
        let schema = empty_schema();
        let err =
            ensure_write_target_writable(&plan, &schema, ExecutorKind::EmbeddedChdb).unwrap_err();
        assert!(matches!(err, WriteGuardError::UnknownNodeLabel(_)));
    }
}
