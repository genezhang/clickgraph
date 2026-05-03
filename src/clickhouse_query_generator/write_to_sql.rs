//! `WriteRenderPlan` → ClickHouse SQL.
//!
//! Emits chdb-compatible SQL aligned with the patterns already used by
//! `clickgraph-embedded::write_helpers`:
//!
//! - `INSERT INTO `db`.`table` (cols) VALUES (row1), (row2), ...`
//! - `UPDATE `db`.`table` SET col = expr WHERE id_col IN (subquery)`
//!   (lightweight; no `SETTINGS` clause at query time — the table must have
//!   been created with `enable_block_number_column=1, enable_block_offset_column=1`,
//!   which Phase 3 wires into `data_loader.rs`).
//! - `DELETE FROM `db`.`table` WHERE id_col IN (subquery)` (lightweight).
//! - `Sequence` flattens to one statement per element, separated by `\n;\n`
//!   so the executor can split and run each in turn.
//!
//! No `SETTINGS mutations_sync = …` is emitted — Decision 0.7 explicitly
//! ruled out the mutation path.

use crate::render_plan::write_render::{DeleteOp, InsertOp, RowSource, UpdateOp, WriteRenderPlan};
use crate::render_plan::{RenderPlan, ToSql};

/// Render a `WriteRenderPlan` to one or more SQL statements, in execution
/// order. Returned vector contains the statements; the executor (Phase 3)
/// runs each in sequence.
pub fn write_render_to_sql(plan: &WriteRenderPlan) -> Vec<String> {
    let mut out = Vec::new();
    push_sql(plan, &mut out);
    out
}

fn push_sql(plan: &WriteRenderPlan, out: &mut Vec<String>) {
    match plan {
        WriteRenderPlan::Insert(op) => out.push(insert_sql(op)),
        WriteRenderPlan::Update(op) => out.push(update_sql(op)),
        WriteRenderPlan::Delete(op) => out.push(delete_sql(op)),
        WriteRenderPlan::Sequence(seq) => {
            for inner in seq {
                push_sql(inner, out);
            }
        }
    }
}

fn insert_sql(op: &InsertOp) -> String {
    let cols = op
        .columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect::<Vec<_>>()
        .join(", ");

    let rows: Vec<String> = op
        .rows
        .iter()
        .map(|row| {
            let values = row
                .iter()
                .map(render_expr_inline)
                .collect::<Vec<_>>()
                .join(", ");
            format!("({})", values)
        })
        .collect();

    format!(
        "INSERT INTO `{}`.`{}` ({}) VALUES {}",
        op.database,
        op.table,
        cols,
        rows.join(", ")
    )
}

fn update_sql(op: &UpdateOp) -> String {
    let assignments = op
        .assignments
        .iter()
        .map(|(col, expr)| format!("`{}` = {}", col, render_expr_inline(expr)))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "UPDATE `{}`.`{}` SET {} WHERE `{}` IN {}",
        op.database,
        op.table,
        assignments,
        op.id_column,
        render_id_source(&op.source),
    )
}

fn delete_sql(op: &DeleteOp) -> String {
    format!(
        "DELETE FROM `{}`.`{}` WHERE `{}` IN {}",
        op.database,
        op.table,
        op.id_column,
        render_id_source(&op.source),
    )
}

fn render_id_source(source: &RowSource) -> String {
    match source {
        RowSource::Subquery(plan) => format!("({})", render_subquery(plan.as_ref())),
        RowSource::Ids(ids) => {
            let parts = ids.iter().map(render_expr_inline).collect::<Vec<_>>();
            format!("({})", parts.join(", "))
        }
    }
}

fn render_subquery(plan: &RenderPlan) -> String {
    super::to_sql_query::render_plan_to_sql(plan.clone(), 10)
}

/// Render a `RenderExpr` to an inline SQL fragment suitable for INSERT
/// VALUES, SET assignments, and IN-list literals. We deliberately keep this
/// narrow — write payloads are simple expressions (literals, parameters,
/// scalar function calls) and complex constructs (aggregates, subqueries)
/// don't appear here.
fn render_expr_inline(expr: &crate::render_plan::render_expr::RenderExpr) -> String {
    use crate::render_plan::render_expr::{Literal, RenderExpr};
    match expr {
        RenderExpr::Literal(Literal::String(s)) => format!("'{}'", s.replace('\'', "''")),
        RenderExpr::Literal(Literal::Integer(i)) => i.to_string(),
        RenderExpr::Literal(Literal::Float(f)) => f.to_string(),
        RenderExpr::Literal(Literal::Boolean(b)) => b.to_string(),
        RenderExpr::Literal(Literal::Null) => "NULL".to_string(),
        RenderExpr::Parameter(name) => format!("${}", name),
        RenderExpr::Raw(raw) => raw.clone(),
        RenderExpr::List(items) => {
            let parts = items.iter().map(render_expr_inline).collect::<Vec<_>>();
            format!("[{}]", parts.join(", "))
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            let args = fn_call
                .args
                .iter()
                .map(render_expr_inline)
                .collect::<Vec<_>>();
            format!("{}({})", fn_call.name, args.join(", "))
        }
        RenderExpr::Column(c) => c.raw().to_string(),
        RenderExpr::TableAlias(a) => a.0.clone(),
        RenderExpr::ColumnAlias(a) => a.0.clone(),
        // Fall back to the read-side renderer for anything we don't handle
        // explicitly. Conservative: write payloads should be simple, but if
        // we encounter something unexpected, at least produce *some* SQL
        // rather than a placeholder.
        _ => expr.to_sql(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{Literal, RenderExpr};

    fn lit_string(s: &str) -> RenderExpr {
        RenderExpr::Literal(Literal::String(s.to_string()))
    }

    fn lit_int(i: i64) -> RenderExpr {
        RenderExpr::Literal(Literal::Integer(i))
    }

    #[test]
    fn insert_single_row() {
        let op = InsertOp {
            database: "test".into(),
            table: "person".into(),
            columns: vec!["id".into(), "name".into(), "age".into()],
            rows: vec![vec![lit_string("u1"), lit_string("Alice"), lit_int(30)]],
        };
        assert_eq!(
            insert_sql(&op),
            "INSERT INTO `test`.`person` (`id`, `name`, `age`) VALUES ('u1', 'Alice', 30)"
        );
    }

    #[test]
    fn insert_multi_row() {
        let op = InsertOp {
            database: "test".into(),
            table: "person".into(),
            columns: vec!["id".into()],
            rows: vec![vec![lit_string("u1")], vec![lit_string("u2")]],
        };
        assert_eq!(
            insert_sql(&op),
            "INSERT INTO `test`.`person` (`id`) VALUES ('u1'), ('u2')"
        );
    }

    #[test]
    fn delete_with_literal_ids() {
        let op = DeleteOp {
            database: "test".into(),
            table: "person".into(),
            id_column: "id".into(),
            source: RowSource::Ids(vec![lit_string("u1"), lit_string("u2")]),
        };
        assert_eq!(
            delete_sql(&op),
            "DELETE FROM `test`.`person` WHERE `id` IN ('u1', 'u2')"
        );
    }

    #[test]
    fn update_with_literal_ids() {
        let op = UpdateOp {
            database: "test".into(),
            table: "person".into(),
            assignments: vec![("age".into(), lit_int(31))],
            id_column: "id".into(),
            source: RowSource::Ids(vec![lit_string("u1")]),
        };
        assert_eq!(
            update_sql(&op),
            "UPDATE `test`.`person` SET `age` = 31 WHERE `id` IN ('u1')"
        );
    }

    #[test]
    fn sequence_emits_each_statement() {
        let op = WriteRenderPlan::Sequence(vec![
            WriteRenderPlan::Delete(DeleteOp {
                database: "t".into(),
                table: "knows".into(),
                id_column: "from_id".into(),
                source: RowSource::Ids(vec![lit_string("u1")]),
            }),
            WriteRenderPlan::Delete(DeleteOp {
                database: "t".into(),
                table: "person".into(),
                id_column: "id".into(),
                source: RowSource::Ids(vec![lit_string("u1")]),
            }),
        ]);
        let sql = write_render_to_sql(&op);
        assert_eq!(sql.len(), 2);
        assert!(sql[0].contains("knows"));
        assert!(sql[1].contains("person"));
    }

    #[test]
    fn string_escape_in_insert() {
        let op = InsertOp {
            database: "t".into(),
            table: "p".into(),
            columns: vec!["name".into()],
            rows: vec![vec![lit_string("O'Brien")]],
        };
        let sql = insert_sql(&op);
        assert!(
            sql.contains("'O''Brien'"),
            "expected escaped quote, got: {}",
            sql
        );
    }
}
