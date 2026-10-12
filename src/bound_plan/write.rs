//! The embedded write path on the bound plan (S13,
//! `docs/design/EXPLICIT_SCOPE.md`): a statement of SET / REMOVE / DELETE
//! after a read part, planned as the lightweight UPDATE / DELETE statements
//! of [`WriteRenderPlan`].
//!
//! Each operation's node is updated or deleted by its identity: the
//! identities of the node in the read part's records
//! ([`lower::write::lower_identities`]), one statement per label the node
//! can have, reading that label's identities. A SET value is computed from
//! the row it updates ([`lower::write::lower_write_value`]); REMOVE sets the
//! property to NULL; DETACH DELETE first deletes the relationships of every
//! standard edge table at either end of the label.
//!
//! Admission (the read-only rules of the embedded-writes design): a
//! source-backed label (Parquet / S3 / Iceberg / Delta) is read-only; a
//! node is written in its own table, by a single-column identity.

use std::collections::HashMap;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema};
use crate::render_plan::render_expr::{Literal, RenderExpr};
use crate::render_plan::write_render::{DeleteOp, RowSource, UpdateOp, WriteRenderPlan};
use crate::sql_generator::emitters::clickhouse::to_sql_query::render_plan_to_sql_plain;

use super::lower::write::{lower_identities, lower_write_value, IdentityRows};
use super::lower::LowerOptions;
use super::types::{BindingKind, BoundWrite, BoundWriteStatement, VarId};
use super::{bind_write_statement, BindError};

/// Why a write statement was not planned on the bound plan.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WritePlanError {
    /// A statement this planner does not handle yet (CREATE, an update
    /// followed by RETURN, …): the caller's other path.
    #[error("{0}")]
    Unsupported(String),
    /// A statement that cannot be executed (invalid, or a read-only
    /// target).
    #[error("{0}")]
    Rejected(String),
}

/// Plan the write statement `cypher` (comments stripped).
pub fn plan_write(cypher: &str, schema: &GraphSchema) -> Result<WriteRenderPlan, WritePlanError> {
    let (rest, stmt) = crate::open_cypher_parser::clause_list::parse_clause_statement(cypher)
        .map_err(|e| WritePlanError::Unsupported(format!("not parsed: {e:?}")))?;
    if !rest.trim().trim_end_matches(';').trim().is_empty() {
        return Err(WritePlanError::Unsupported("unparsed text".to_string()));
    }
    let bound = bind_write_statement(&stmt, schema).map_err(|e| match e {
        BindError::Unsupported(_) | BindError::NotARead(_) => {
            WritePlanError::Unsupported(e.to_string())
        }
        e => WritePlanError::Rejected(e.to_string()),
    })?;
    Planner {
        stmt: &bound,
        schema,
        options: LowerOptions::default(),
        identities: HashMap::new(),
    }
    .plan()
}

/// One SET / REMOVE of a node: its property, and its value (REMOVE: none).
type Assignment<'e> = (
    String,
    Option<&'e crate::query_planner::logical_expr::LogicalExpr>,
);

struct Planner<'a> {
    stmt: &'a BoundWriteStatement,
    schema: &'a GraphSchema,
    options: LowerOptions,
    /// Each node's identity rows, lowered once.
    identities: HashMap<VarId, Option<IdentityRows>>,
}

impl<'a> Planner<'a> {
    fn plan(mut self) -> Result<WriteRenderPlan, WritePlanError> {
        // SET / REMOVE of one node: one UPDATE per label, its assignments
        // in order (a property assigned twice: the last), nodes in order of
        // their first operation; then the DELETEs.
        let mut updates: Vec<(VarId, Vec<Assignment<'_>>)> = Vec::new();
        let mut deletes: Vec<(VarId, bool)> = Vec::new();
        for w in &self.stmt.writes {
            let (var, property, value) = match w {
                BoundWrite::Set {
                    var,
                    property,
                    value,
                } => (*var, property, Some(value)),
                BoundWrite::Remove { var, property } => (*var, property, None),
                BoundWrite::Delete { var, detach } => {
                    match deletes.iter_mut().find(|(v, _)| v == var) {
                        Some(d) => d.1 |= *detach,
                        None => deletes.push((*var, *detach)),
                    }
                    continue;
                }
            };
            match updates.iter_mut().find(|(v, _)| *v == var) {
                Some((_, items)) => items.push((property.clone(), value)),
                None => updates.push((var, vec![(property.clone(), value)])),
            }
        }
        let mut ops = Vec::new();
        for (var, items) in updates {
            for label in self.labels(var)? {
                let node = self.writable(&label)?;
                let Some(source) = self.source(var, &label, false)? else {
                    continue;
                };
                let mut assignments: Vec<(String, RenderExpr)> = Vec::new();
                for (property, value) in &items {
                    let column = property_column(node, property).ok_or_else(|| {
                        WritePlanError::Rejected(format!(
                            "SET: property `{property}` of `{label}` cannot be mapped to a \
                             writable column"
                        ))
                    })?;
                    let value = match value {
                        Some(v) => RenderExpr::Raw(
                            crate::sql_generator::emitters::clickhouse::to_sql_query::render_expr_to_sql_plain(
                                &lower_write_value(
                                    &self.stmt.read,
                                    self.schema,
                                    &self.options,
                                    var,
                                    &label,
                                    node,
                                    &node.table_name,
                                    v,
                                )
                                // A row's UPDATE computes no other value:
                                // refused (the legacy planner's SQL fails).
                                .map_err(|e| WritePlanError::Rejected(e.to_string()))?,
                            ),
                        ),
                        None => RenderExpr::Literal(Literal::Null),
                    };
                    match assignments.iter_mut().find(|(c, _)| *c == column) {
                        Some(a) => a.1 = value,
                        None => assignments.push((column, value)),
                    }
                }
                ops.push(WriteRenderPlan::Update(UpdateOp {
                    database: node.database.clone(),
                    table: node.table_name.clone(),
                    assignments,
                    id_column: id_column(node, &label)?,
                    source,
                }));
            }
        }
        for (var, detach) in deletes {
            for label in self.labels(var)? {
                let node = self.writable(&label)?;
                let Some(source) = self.source(var, &label, true)? else {
                    continue;
                };
                if detach {
                    ops.extend(detach_deletes(self.schema, &label, &source)?);
                }
                ops.push(WriteRenderPlan::Delete(DeleteOp {
                    database: node.database.clone(),
                    table: node.table_name.clone(),
                    id_column: id_column(node, &label)?,
                    source,
                }));
            }
        }
        Ok(match ops.len() {
            1 => ops.remove(0),
            _ => WriteRenderPlan::Sequence(ops),
        })
    }

    /// The labels node `var` can have.
    fn labels(&self, var: VarId) -> Result<Vec<String>, WritePlanError> {
        match &self.stmt.read.binding(var).kind {
            BindingKind::Node { labels } => Ok(labels.iter().cloned().collect()),
            _ => Err(WritePlanError::Rejected(
                "internal: a write target that is not a node".to_string(),
            )),
        }
    }

    /// The schema of `label`, refused when it is not writable.
    fn writable(&self, label: &str) -> Result<&'a NodeSchema, WritePlanError> {
        let Some(node) = self.schema.node_schema_opt(label) else {
            return Err(WritePlanError::Rejected(format!(
                "unknown node label `{label}`"
            )));
        };
        if node.source.is_some() {
            return Err(WritePlanError::Rejected(format!(
                "Cannot write to node label `{label}`: it resolves to a source-backed \
                 (read-only) table. Source-backed schemas (Parquet/S3/Iceberg/Delta) are \
                 read-only by design."
            )));
        }
        if !node.is_standard_own_table() {
            return Err(WritePlanError::Rejected(format!(
                "Cannot write to node label `{label}`: its nodes are not rows of their own table"
            )));
        }
        Ok(node)
    }

    /// The identities of `var` of `label` in the read part's records, as a
    /// subquery of one column; `None` when the node matches nothing. A node
    /// of several labels: the identities of `label`'s (its rows' label
    /// column). For a DELETE whose subquery has CTEs, a
    /// [`RowSource::Read`]: ClickHouse's lightweight DELETE reads its
    /// subquery's tables in the target's database, where a CTE is unknown
    /// (an UPDATE and a SELECT read it), so the executor reads the ids
    /// first.
    fn source(
        &mut self,
        var: VarId,
        label: &str,
        delete: bool,
    ) -> Result<Option<RowSource>, WritePlanError> {
        if !self.identities.contains_key(&var) {
            let rows = lower_identities(&self.stmt.read, self.schema, &self.options, var)
                .map_err(|e| WritePlanError::Unsupported(e.to_string()))?;
            self.identities.insert(var, rows);
        }
        let Some(rows) = &self.identities[&var] else {
            return Ok(None);
        };
        let ids = &rows.columns[usize::from(rows.labeled)..];
        let [id] = ids else {
            return Err(WritePlanError::Rejected(format!(
                "a write to `{label}`: composite primary keys are not supported"
            )));
        };
        let inner = render_plan_to_sql_plain(rows.plan.clone());
        let label_filter = match rows.labeled {
            true => format!(
                " WHERE {} = '{}'",
                rows.columns[0],
                label.replace('\'', "''")
            ),
            false => String::new(),
        };
        let sql = format!("SELECT {id} FROM ({inner}) AS __cg_write_ids{label_filter}");
        Ok(Some(match delete && !rows.plan.ctes.0.is_empty() {
            true => RowSource::Read(sql),
            false => RowSource::Sql(sql),
        }))
    }
}

/// The node table's column of `property`: a column mapping, or a column of
/// the table by that name; an expression-mapped property is not writable.
fn property_column(node: &NodeSchema, property: &str) -> Option<String> {
    match node.property_mappings.get(property) {
        Some(PropertyValue::Column(c)) => Some(c.clone()),
        Some(PropertyValue::Expression(_)) => None,
        None => node
            .column_names
            .iter()
            .any(|c| c == property)
            .then(|| property.to_string()),
    }
}

fn id_column(node: &NodeSchema, label: &str) -> Result<String, WritePlanError> {
    node.node_id
        .column_or_error()
        .map(str::to_string)
        .map_err(|e| {
            WritePlanError::Rejected(format!(
                "a write to `{label}`: composite primary keys are not supported ({e})"
            ))
        })
}

/// DETACH DELETE: the relationships of every standard edge table with
/// `label` at an end, one DELETE per end column (deduplicated by table).
fn detach_deletes(
    schema: &GraphSchema,
    label: &str,
    source: &RowSource,
) -> Result<Vec<WriteRenderPlan>, WritePlanError> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut rels: Vec<_> = schema.get_relationships_schemas().iter().collect();
    rels.sort_by(|a, b| a.0.cmp(b.0));
    for (_, rel) in rels {
        // An FK edge's relationship is gone with either node.
        if rel.is_node_row_reference() || (rel.from_node != label && rel.to_node != label) {
            continue;
        }
        // Deleted by an end's id alone: every row of the table is one
        // relationship between nodes of its declared labels (else another
        // label's relationships of the same id would go too).
        if !rel.is_plain_edge_table() || !rel.has_fixed_endpoint_labels() {
            return Err(WritePlanError::Rejected(format!(
                "DETACH DELETE of `{label}`: the relationships of table `{}` cannot be told \
                 apart by the node's id alone (a polymorphic table, or one embedding node \
                 properties)",
                rel.table_name
            )));
        }
        if !seen.insert((rel.database.clone(), rel.table_name.clone())) {
            continue;
        }
        if rel.source.is_some() {
            return Err(WritePlanError::Rejected(format!(
                "DETACH DELETE: relationships of `{label}` are in a source-backed (read-only) \
                 table `{}`",
                rel.table_name
            )));
        }
        for (end, at_end) in [
            (&rel.from_id, rel.from_node == label),
            (&rel.to_id, rel.to_node == label),
        ] {
            if !at_end {
                continue;
            }
            let column = end.as_single().map_err(|_| {
                WritePlanError::Rejected(format!(
                    "DETACH DELETE: a composite end on `{}` is not supported",
                    rel.table_name
                ))
            })?;
            out.push(WriteRenderPlan::Delete(DeleteOp {
                database: rel.database.clone(),
                table: rel.table_name.clone(),
                id_column: column.to_string(),
                source: source.clone(),
            }));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse_query_generator::write_to_sql::write_render_to_sql;

    fn social() -> GraphSchema {
        crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../../schemas/test/social_integration.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap()
    }

    fn sql(q: &str) -> Vec<String> {
        write_render_to_sql(&plan_write(q, &social()).unwrap())
            .into_iter()
            .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect()
    }

    /// A SET value is computed from the row it updates (its properties
    /// qualified by the table's name: an UPDATE has no alias), by the
    /// node's identities in the read part's records.
    #[test]
    fn set_reads_the_row_it_updates() {
        let got = sql("MATCH (u:User) WHERE u.user_id = 1 SET u.age = u.age + 1, u.name = 'x'");
        assert_eq!(got.len(), 1);
        assert!(
            got[0].starts_with(
                "UPDATE `test_integration`.`users_test` SET `age` = users_test.age + 1, \
                 `full_name` = 'x' WHERE `user_id` IN (SELECT __cg_id0 FROM (SELECT DISTINCT \
                 v0.user_id AS \"__cg_id0\" FROM test_integration.users_test AS v0 WHERE"
            ),
            "{got:?}"
        );
        // REMOVE: NULL; a property assigned twice: the last.
        let got = sql("MATCH (u:User) WHERE u.user_id = 1 SET u.age = 1 REMOVE u.age");
        assert!(got[0].contains("SET `age` = NULL WHERE"), "{got:?}");
    }

    /// DETACH DELETE: each standard edge table's relationships at either
    /// end, then the node (an FK edge is the other node's row: kept). A
    /// node of several labels: one per label, its ids read first (the
    /// labels' union is a CTE, which a lightweight DELETE cannot read).
    #[test]
    fn delete_and_detach() {
        let plan = plan_write("MATCH (u:User {user_id: 2}) DETACH DELETE u", &social()).unwrap();
        let got: Vec<String> = write_render_to_sql(&plan)
            .iter()
            .map(|s| s.split(" WHERE ").next().unwrap_or_default().to_string())
            .collect();
        assert_eq!(
            got,
            [
                "DELETE FROM `test_integration`.`user_follows_test`",
                "DELETE FROM `test_integration`.`user_follows_test`",
                "DELETE FROM `test_integration`.`post_likes_test`",
                "DELETE FROM `test_integration`.`users_test`",
            ]
        );
        let WriteRenderPlan::Sequence(ops) = plan_write("MATCH (n) DELETE n", &social()).unwrap()
        else {
            panic!("one DELETE per label");
        };
        assert_eq!(ops.len(), 2);
        for op in ops {
            let WriteRenderPlan::Delete(d) = op else {
                panic!("a DELETE");
            };
            assert!(matches!(d.source, RowSource::Read(_)), "{d:?}");
        }
        // One label: its subquery, read by the DELETE.
        let WriteRenderPlan::Delete(d) = plan_write("MATCH (u:User) DELETE u", &social()).unwrap()
        else {
            panic!("a DELETE");
        };
        assert!(matches!(d.source, RowSource::Sql(_)), "{d:?}");
    }

    #[test]
    fn refusals() {
        let schema = social();
        for (q, why) in [
            (
                "MATCH (u:User)-[:FOLLOWS]->(f:User) SET u.name = f.name",
                "another variable",
            ),
            (
                "MATCH (u:User)-[r:FOLLOWS]->(f:User) DELETE r",
                "relationship",
            ),
            ("MATCH (u:User) SET u.nope_xyz = 1", "writable column"),
        ] {
            let err = plan_write(q, &schema).unwrap_err().to_string();
            assert!(err.contains(why), "{q}: {err}");
        }
        // Not planned here yet (the legacy planner's): CREATE, RETURN.
        for q in [
            "CREATE (u:User {user_id: 99})",
            "MATCH (u:User) SET u.age = 1 RETURN u.age",
        ] {
            assert!(
                matches!(plan_write(q, &schema), Err(WritePlanError::Unsupported(_))),
                "{q}"
            );
        }
    }
}
