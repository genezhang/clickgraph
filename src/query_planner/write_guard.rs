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
//! Validation scope per write variant:
//! - **`CREATE`**: every `CreateNode` / `CreateRel` is validated (label/type
//!   resolves, schema is writable, edge is not FK-edge).
//! - **`SET` / `DELETE` / `REMOVE`**: targets are bound aliases from the
//!   preceding read pipeline. The guard walks `input` to resolve each alias's
//!   label statically and re-checks 0.3 / 0.6 on the resolved schema. When a
//!   label cannot be resolved at this stage (e.g., bound across a WITH
//!   boundary), validation is deferred to the renderer / executor.
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
        "Cannot write to node label `{label}`: it resolves to a source-backed (read-only) table. \
         Source-backed schemas (Parquet/S3/Iceberg/Delta) are read-only by design."
    )]
    SourceBackedNode { label: String },

    #[error(
        "Cannot write to relationship type `{rel_type}`: it resolves to a source-backed \
         (read-only) table. Source-backed schemas (Parquet/S3/Iceberg/Delta) are read-only by design."
    )]
    SourceBackedRelationship { rel_type: String },

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
/// The implementation makes two passes: a structural check for any write
/// variant (whole-tree walk, but cheap — leaves return immediately and there
/// is no cloning), then full schema-level validation only when writes are
/// found. Read-only plans pay only the first pass.
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

/// Structural check: does this plan tree contain any write variant?
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
        LogicalPlan::SetProperties(sp) => {
            for item in &sp.items {
                check_alias_target_writable(&item.target_alias, &sp.input, schema)?;
            }
            check_writes_recursive(&sp.input, schema)
        }
        LogicalPlan::Delete(d) => {
            for alias in &d.targets {
                check_alias_target_writable(alias, &d.input, schema)?;
            }
            check_writes_recursive(&d.input, schema)
        }
        LogicalPlan::Remove(r) => {
            for item in &r.items {
                check_alias_target_writable(&item.target_alias, &r.input, schema)?;
            }
            check_writes_recursive(&r.input, schema)
        }

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
                return Err(WriteGuardError::SourceBackedNode {
                    label: node.label.clone(),
                });
            }
        }
        CreatePattern::Rel(rel) => {
            let rel_schema = schema
                .get_relationships_schema_opt(&rel.rel_type)
                .ok_or_else(|| WriteGuardError::UnknownRelationshipType(rel.rel_type.clone()))?;
            if rel_schema.source.is_some() {
                return Err(WriteGuardError::SourceBackedRelationship {
                    rel_type: rel.rel_type.clone(),
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

/// For SET / DELETE / REMOVE: locate the alias in the input plan, then if its
/// label / relationship type is statically resolvable, re-check decisions
/// 0.3 and 0.6. Aliases whose label cannot be resolved at this stage (e.g.,
/// bound across a WITH boundary) are deferred to downstream validation.
fn check_alias_target_writable(
    alias: &str,
    input: &LogicalPlan,
    schema: &GraphSchema,
) -> Result<(), WriteGuardError> {
    if let Some(label) = find_alias_node_label(alias, input) {
        if let Some(node_schema) = schema.node_schema_opt(&label) {
            if node_schema.source.is_some() {
                return Err(WriteGuardError::SourceBackedNode { label });
            }
        }
        return Ok(());
    }
    if let Some(rel_type) = find_alias_rel_type(alias, input) {
        if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
            if rel_schema.source.is_some() {
                return Err(WriteGuardError::SourceBackedRelationship { rel_type });
            }
            if rel_schema.is_fk_edge {
                return Err(WriteGuardError::FkEdgeTarget { rel_type });
            }
        }
    }
    Ok(())
}

fn find_alias_node_label(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(n) if n.alias == alias => n.label.clone(),
        LogicalPlan::GraphNode(n) => find_alias_node_label(alias, &n.input),
        LogicalPlan::GraphRel(r) => find_alias_node_label(alias, &r.left)
            .or_else(|| find_alias_node_label(alias, &r.center))
            .or_else(|| find_alias_node_label(alias, &r.right)),
        LogicalPlan::Filter(f) => find_alias_node_label(alias, &f.input),
        LogicalPlan::Projection(p) => find_alias_node_label(alias, &p.input),
        LogicalPlan::GroupBy(gb) => find_alias_node_label(alias, &gb.input),
        LogicalPlan::OrderBy(ob) => find_alias_node_label(alias, &ob.input),
        LogicalPlan::Skip(s) => find_alias_node_label(alias, &s.input),
        LogicalPlan::Limit(l) => find_alias_node_label(alias, &l.input),
        LogicalPlan::Cte(c) => find_alias_node_label(alias, &c.input),
        LogicalPlan::GraphJoins(gj) => find_alias_node_label(alias, &gj.input),
        LogicalPlan::Unwind(u) => find_alias_node_label(alias, &u.input),
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .find_map(|p| find_alias_node_label(alias, p)),
        LogicalPlan::CartesianProduct(cp) => find_alias_node_label(alias, &cp.left)
            .or_else(|| find_alias_node_label(alias, &cp.right)),
        LogicalPlan::WithClause(wc) => find_alias_node_label(alias, &wc.input),
        LogicalPlan::Create(c) => find_alias_node_label(alias, &c.input),
        LogicalPlan::SetProperties(sp) => find_alias_node_label(alias, &sp.input),
        LogicalPlan::Delete(d) => find_alias_node_label(alias, &d.input),
        LogicalPlan::Remove(r) => find_alias_node_label(alias, &r.input),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => None,
    }
}

fn find_alias_rel_type(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(r) if r.alias == alias => {
            r.labels.as_ref().and_then(|l| l.first().cloned())
        }
        LogicalPlan::GraphRel(r) => find_alias_rel_type(alias, &r.left)
            .or_else(|| find_alias_rel_type(alias, &r.center))
            .or_else(|| find_alias_rel_type(alias, &r.right)),
        LogicalPlan::GraphNode(n) => find_alias_rel_type(alias, &n.input),
        LogicalPlan::Filter(f) => find_alias_rel_type(alias, &f.input),
        LogicalPlan::Projection(p) => find_alias_rel_type(alias, &p.input),
        LogicalPlan::GroupBy(gb) => find_alias_rel_type(alias, &gb.input),
        LogicalPlan::OrderBy(ob) => find_alias_rel_type(alias, &ob.input),
        LogicalPlan::Skip(s) => find_alias_rel_type(alias, &s.input),
        LogicalPlan::Limit(l) => find_alias_rel_type(alias, &l.input),
        LogicalPlan::Cte(c) => find_alias_rel_type(alias, &c.input),
        LogicalPlan::GraphJoins(gj) => find_alias_rel_type(alias, &gj.input),
        LogicalPlan::Unwind(u) => find_alias_rel_type(alias, &u.input),
        LogicalPlan::Union(u) => u.inputs.iter().find_map(|p| find_alias_rel_type(alias, p)),
        LogicalPlan::CartesianProduct(cp) => {
            find_alias_rel_type(alias, &cp.left).or_else(|| find_alias_rel_type(alias, &cp.right))
        }
        LogicalPlan::WithClause(wc) => find_alias_rel_type(alias, &wc.input),
        LogicalPlan::Create(c) => find_alias_rel_type(alias, &c.input),
        LogicalPlan::SetProperties(sp) => find_alias_rel_type(alias, &sp.input),
        LogicalPlan::Delete(d) => find_alias_rel_type(alias, &d.input),
        LogicalPlan::Remove(r) => find_alias_rel_type(alias, &r.input),
        LogicalPlan::Empty | LogicalPlan::ViewScan(_) | LogicalPlan::PageRank(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::graph_catalog::schema_types::SchemaType;
    use crate::graph_catalog::{NodeIdSchema, NodeSchema, RelationshipSchema};
    use crate::query_planner::logical_expr::Direction;
    use crate::query_planner::logical_plan::{
        Create, CreateNode, CreateRel, Delete, GraphNode, GraphRel, SetItem, SetProperties,
    };
    use std::sync::Arc;

    fn empty_schema() -> GraphSchema {
        GraphSchema::build(
            0,
            "test".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        )
    }

    fn person_node_with_source(source: Option<&str>) -> NodeSchema {
        NodeSchema {
            database: "test".to_string(),
            table_name: "person".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::String),
            property_mappings: [(
                "name".to_string(),
                PropertyValue::Column("name".to_string()),
            )]
            .into_iter()
            .collect(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
            source: source.map(|s| s.to_string()),
            property_types: std::collections::HashMap::new(),
            id_generation: None,
        }
    }

    fn knows_rel(is_fk: bool) -> RelationshipSchema {
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "knows".to_string(),
            column_names: vec!["from_id".to_string(), "to_id".to_string()],
            from_node: "Person".to_string(),
            to_node: "Person".to_string(),
            from_node_table: "person".to_string(),
            to_node_table: "person".to_string(),
            from_id: Identifier::from("from_id"),
            to_id: Identifier::from("to_id"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: std::collections::HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: is_fk,
            constraints: None,
            edge_id_types: None,
            source: None,
            property_types: std::collections::HashMap::new(),
        }
    }

    fn schema_with_person(source: Option<&str>) -> GraphSchema {
        let mut nodes = std::collections::HashMap::new();
        nodes.insert("Person".to_string(), person_node_with_source(source));
        GraphSchema::build(
            1,
            "test".to_string(),
            nodes,
            std::collections::HashMap::new(),
        )
    }

    fn schema_with_fk_edge() -> GraphSchema {
        let mut nodes = std::collections::HashMap::new();
        nodes.insert("Person".to_string(), person_node_with_source(None));
        let mut rels = std::collections::HashMap::new();
        rels.insert("KNOWS::Person::Person".to_string(), knows_rel(true));
        GraphSchema::build(1, "test".to_string(), nodes, rels)
    }

    fn graph_node_alias(alias: &str, label: Option<&str>) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: alias.to_string(),
            label: label.map(|s| s.to_string()),
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        }))
    }

    fn graph_rel(
        alias: &str,
        rel_type: &str,
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
    ) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphRel(GraphRel {
            left,
            center: Arc::new(LogicalPlan::Empty),
            right,
            alias: alias.to_string(),
            direction: Direction::Outgoing,
            left_connection: "a".to_string(),
            right_connection: "b".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec![rel_type.to_string()]),
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
        }))
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

    #[test]
    fn create_rel_against_source_backed_emits_relationship_error() {
        let mut nodes = std::collections::HashMap::new();
        nodes.insert("Person".to_string(), person_node_with_source(None));
        let mut rel = knows_rel(false);
        rel.source = Some("table_function:numbers(10)".to_string());
        let mut rels = std::collections::HashMap::new();
        rels.insert("KNOWS::Person::Person".to_string(), rel);
        let schema = GraphSchema::build(1, "test".to_string(), nodes, rels);

        let plan = LogicalPlan::Create(Create {
            input: Arc::new(LogicalPlan::Empty),
            patterns: vec![CreatePattern::Rel(CreateRel {
                alias: None,
                rel_type: "KNOWS".to_string(),
                direction: Direction::Outgoing,
                start_alias: "a".to_string(),
                end_alias: "b".to_string(),
                properties: vec![],
            })],
        });
        let err =
            ensure_write_target_writable(&plan, &schema, ExecutorKind::EmbeddedChdb).unwrap_err();
        assert!(
            matches!(err, WriteGuardError::SourceBackedRelationship { .. }),
            "got {:?}",
            err
        );
    }

    // SET on a bound alias whose schema is source-backed must be rejected by
    // the guard (Decision 0.3 enforcement on SET, not just CREATE).
    #[test]
    fn set_against_source_backed_alias_rejected() {
        let schema = schema_with_person(Some("table_function:numbers(10)"));
        let plan = LogicalPlan::SetProperties(SetProperties {
            input: graph_node_alias("a", Some("Person")),
            items: vec![SetItem {
                target_alias: "a".to_string(),
                property: "name".to_string(),
                value: crate::query_planner::logical_expr::LogicalExpr::Literal(
                    crate::query_planner::logical_expr::Literal::String("x".to_string()),
                ),
            }],
        });
        let err =
            ensure_write_target_writable(&plan, &schema, ExecutorKind::EmbeddedChdb).unwrap_err();
        assert!(
            matches!(err, WriteGuardError::SourceBackedNode { .. }),
            "got {:?}",
            err
        );
    }

    // DELETE on a relationship alias whose type is FK-edge must be rejected.
    #[test]
    fn delete_against_fk_edge_alias_rejected() {
        let schema = schema_with_fk_edge();
        let input = graph_rel(
            "r",
            "KNOWS",
            graph_node_alias("a", Some("Person")),
            graph_node_alias("b", Some("Person")),
        );
        let plan = LogicalPlan::Delete(Delete {
            input,
            targets: vec!["r".to_string()],
            detach: false,
        });
        let err =
            ensure_write_target_writable(&plan, &schema, ExecutorKind::EmbeddedChdb).unwrap_err();
        assert!(
            matches!(err, WriteGuardError::FkEdgeTarget { .. }),
            "got {:?}",
            err
        );
    }
}
