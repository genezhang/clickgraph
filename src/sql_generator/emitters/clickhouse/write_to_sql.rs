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
//! - `Sequence` flattens to a `Vec<String>` with one statement per element,
//!   in execution order; the executor (Phase 3) runs each in turn.
//!
//! No `SETTINGS mutations_sync = …` is emitted — Decision 0.7 explicitly
//! ruled out the mutation path.

use crate::render_plan::write_render::{DeleteOp, InsertOp, RowSource, UpdateOp, WriteRenderPlan};

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
        // Checked by the executor before any statement: no statement.
        WriteRenderPlan::Check(_) => {}
    }
}

/// The SELECT that finds a [`CheckOp`]'s violation: a row when its ids are
/// not empty (with `within`: when one is in that table's column).
pub fn check_sql(op: &crate::render_plan::write_render::CheckOp) -> String {
    let ids = render_id_source(&op.source);
    match &op.within {
        Some(t) => {
            let unless: String = t
                .unless
                .iter()
                .map(|(c, s)| format!(" AND NOT `{c}` IN {}", render_id_source(s)))
                .collect();
            format!(
                "SELECT 1 AS hit FROM `{}`.`{}` WHERE `{}` IN {ids}{unless} LIMIT 1",
                t.database, t.table, t.column
            )
        }
        None => format!("SELECT 1 AS hit FROM {ids} AS __cg_ids LIMIT 1"),
    }
}

fn insert_sql(op: &InsertOp) -> String {
    let cols = op
        .columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect::<Vec<_>>()
        .join(", ");
    if let Some(RowSource::Sql(select) | RowSource::Read(select)) = &op.select {
        return format!(
            "INSERT INTO `{}`.`{}` ({cols}) SELECT * FROM ({select}) AS __cg_rows",
            op.database, op.table
        );
    }

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

/// Render a probe SQL that counts the rows a `DeleteOp` would affect,
/// without mutating anything. Used by `clickgraph-embedded` to attach
/// accurate `nodes_deleted` / `relationships_deleted` counters when the
/// statement also carries a RETURN clause (Phase 5d): the lightweight
/// DELETE itself returns no affected-row count, so we run the same
/// `IN (subquery)` against `count(*)` first.
pub fn probe_delete_count_sql(op: &DeleteOp) -> String {
    format!(
        "SELECT count() AS n FROM `{}`.`{}` WHERE `{}` IN {}",
        op.database,
        op.table,
        op.id_column,
        render_id_source(&op.source),
    )
}

/// Probe SQL counting the rows an `InsertOp` of a SELECT inserts.
pub fn probe_insert_count_sql(select: &str) -> String {
    format!("SELECT count() AS n FROM ({select}) AS __cg_rows")
}

/// Probe SQL for an `UpdateOp` — same pattern as `probe_delete_count_sql`.
pub fn probe_update_count_sql(op: &UpdateOp) -> String {
    format!(
        "SELECT count() AS n FROM `{}`.`{}` WHERE `{}` IN {}",
        op.database,
        op.table,
        op.id_column,
        render_id_source(&op.source),
    )
}

fn render_id_source(source: &RowSource) -> String {
    match source {
        RowSource::Sql(sql) | RowSource::Read(sql) => format!("({sql})"),
        RowSource::Ids(ids) => {
            let parts = ids.iter().map(render_expr_inline).collect::<Vec<_>>();
            format!("({})", parts.join(", "))
        }
    }
}

/// Render a `RenderExpr` to an inline SQL fragment suitable for INSERT
/// VALUES, SET assignments, and IN-list literals. We deliberately keep this
/// narrow — write payloads are simple expressions (literals, parameters,
/// scalar function calls) and complex constructs (aggregates, subqueries)
/// don't appear here.
fn render_expr_inline(expr: &crate::render_plan::render_expr::RenderExpr) -> String {
    use crate::render_plan::render_expr::{Literal, RenderExpr};
    match expr {
        // The literal's text as the query wrote it (a value read from
        // elsewhere is held so, `sql_literal::value_as_cypher_text`).
        RenderExpr::Literal(Literal::String(s)) => {
            crate::utils::sql_literal::cypher_string_to_sql_literal(s)
        }
        RenderExpr::Literal(Literal::Integer(i)) => i.to_string(),
        RenderExpr::Literal(Literal::Float(f)) => f.to_string(),
        RenderExpr::Literal(Literal::Boolean(b)) => b.to_string(),
        RenderExpr::Literal(Literal::Null) => "NULL".to_string(),
        RenderExpr::Parameter(name) => format!("${}", name),
        RenderExpr::Raw(raw) => raw.clone(),
        RenderExpr::List(items) => {
            let parts = items.iter().map(render_expr_inline).collect::<Vec<_>>();
            crate::sql_generator::function_mapper::current_function_mapper()
                .array_literal(&parts.join(", "))
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
        // The shared printer for anything else. The bound write plan
        // (`bound_plan/write.rs`) builds only `Raw` texts and literals; a
        // property access prints its column as given (the lowering resolves
        // columns, the printer does not).
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
            select: None,
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
            select: None,
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
            relationships: false,
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
                relationships: false,
            }),
            WriteRenderPlan::Delete(DeleteOp {
                database: "t".into(),
                table: "person".into(),
                id_column: "id".into(),
                source: RowSource::Ids(vec![lit_string("u1")]),
                relationships: false,
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
            select: None,
        };
        let sql = insert_sql(&op);
        assert!(
            sql.contains("'O''Brien'"),
            "expected escaped quote, got: {}",
            sql
        );
    }

    /// A string literal holds its text as the query wrote it: an escaped
    /// quote is one quote, and a value read from elsewhere
    /// (`value_as_cypher_text`) keeps every character, a backslash too.
    #[test]
    fn string_literals_are_spelled_as_cypher_text() {
        let sql = |s: &str| render_expr_inline(&lit_string(s));
        assert_eq!(sql("it\\'s"), "'it''s'");
        let value = crate::utils::sql_literal::value_as_cypher_text("a\\'b");
        assert_eq!(sql(&value), "'a\\\\''b'");
    }
}
