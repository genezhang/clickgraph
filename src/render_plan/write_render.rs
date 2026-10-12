//! Write-side render plan: data model produced by Phase 2 from
//! `LogicalPlan::Create / SetProperties / Delete / Remove`, consumed by
//! `clickhouse_query_generator::write_to_sql`.
//!
//! Writes are terminal — they do not compose into SELECT / JOIN structure
//! the way reads do. So instead of extending `RenderPlan` with optional
//! write fields, we model writes as a separate enum. The executor (Phase 3)
//! decides which builder to call based on whether the planner produced a
//! write variant at the top level.
//!
//! ## Lightweight semantics (per Decision 0.7)
//!
//! All three operation kinds map to ClickHouse synchronous primitives:
//! - `Insert` → `INSERT INTO db.table (cols) VALUES (rows)`
//! - `Update` → `UPDATE db.table SET col = expr WHERE id IN (...)`
//!   (lightweight; relies on the table being created with
//!   `enable_block_number_column` / `enable_block_offset_column` — Phase 3
//!   wires this in `data_loader.rs`).
//! - `Delete` → `DELETE FROM db.table WHERE id IN (...)` (lightweight).
//!
//! ## DETACH DELETE
//!
//! Modeled as `Sequence(vec![Delete(rel1_from), Delete(rel1_to), ..., Delete(node)])`.
//! The builder walks the schema to find every relationship table that
//! references the node label and emits one DELETE per side that points at
//! the node (one `WHERE from_id IN (ids)` for tables where the node is the
//! source, one `WHERE to_id IN (ids)` for tables where it is the target),
//! followed by the node DELETE itself. Two DELETEs per side rather than a
//! single `from_id OR to_id` predicate so the lightweight DELETE path can
//! use a separate `IN` filter on each indexed column.

use serde::{Deserialize, Serialize};

use super::render_expr::RenderExpr;
use super::RenderPlan;

/// Top-level write plan. Always produced via the dedicated write-render
/// builder; never reached by the read renderer.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum WriteRenderPlan {
    /// Single-table INSERT.
    Insert(InsertOp),
    /// Single-table lightweight UPDATE.
    Update(UpdateOp),
    /// Single-table lightweight DELETE.
    Delete(DeleteOp),
    /// Ordered list of write ops executed as a sequence. Used for:
    /// - `CREATE` patterns that produce multiple INSERTs (nodes + rels).
    /// - `DETACH DELETE` (per-rel-table DELETEs, then the node DELETE).
    /// - Multi-item `SET` / `REMOVE` that updates multiple aliases at once.
    Sequence(Vec<WriteRenderPlan>),
    /// A condition checked before any statement runs (no SQL statement).
    Check(CheckOp),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct InsertOp {
    pub database: String,
    pub table: String,
    /// Columns named explicitly in the INSERT. Columns omitted (e.g., the
    /// node ID column when the schema's DDL has `DEFAULT generateUUIDv4()`)
    /// will be filled by chdb.
    pub columns: Vec<String>,
    /// One inner Vec per row to insert. Each value is rendered via
    /// `RenderExpr::to_sql`. Length must equal `columns.len()`.
    pub rows: Vec<Vec<RenderExpr>>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct UpdateOp {
    pub database: String,
    pub table: String,
    /// `(column, expression)` pairs for the SET list.
    pub assignments: Vec<(String, RenderExpr)>,
    /// Primary-key column on the target table (used in the WHERE clause).
    pub id_column: String,
    /// Source of IDs to update.
    pub source: RowSource,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DeleteOp {
    pub database: String,
    pub table: String,
    /// Column to filter on. For relationship-table DETACH cleanup this is
    /// often `from_id` or `to_id`; for node DELETE it's the node ID column.
    pub id_column: String,
    pub source: RowSource,
    /// It deletes relationships (DETACH DELETE's cleanup), not nodes: the
    /// counter it adds to.
    #[serde(default)]
    pub relationships: bool,
}

/// A condition of a write statement, checked before any of its statements
/// runs: the write fails with `message` when `source` holds an id (with
/// `within`: an id that is in that table's column).
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CheckOp {
    pub source: RowSource,
    pub within: Option<CheckTable>,
    pub message: String,
}

/// A table column a [`CheckOp`] looks the ids up in.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CheckTable {
    pub database: String,
    pub table: String,
    pub column: String,
    /// Rows the statement deletes (another node's DETACH DELETE: their
    /// column in the ids): not counted.
    #[serde(default)]
    pub unless: Vec<(String, RowSource)>,
}

/// Where the IDs in the WHERE clause of `Update` / `Delete` come from.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum RowSource {
    /// Read pipeline rendered as a subquery yielding a single ID column.
    /// The subquery is rendered via the regular `clickhouse_query_generator`,
    /// then wrapped as `WHERE id_col IN (<subquery>)`.
    Subquery(Box<RenderPlan>),
    /// A rendered SELECT of a single ID column (the bound plan's,
    /// `bound_plan::write`). Wrapped as `WHERE id_col IN (<sql>)`.
    Sql(String),
    /// A rendered SELECT of a single ID column that a lightweight DELETE
    /// cannot read (it has CTEs, whose names the DELETE resolves in its
    /// target's database): the executor reads the ids first and deletes by
    /// them ([`RowSource::Ids`]). A probe (`SELECT count()`) or an UPDATE
    /// reads it as [`RowSource::Sql`].
    Read(String),
    /// Direct list of literal ID expressions.
    /// Wrapped as `WHERE id_col IN (id1, id2, ...)`.
    Ids(Vec<RenderExpr>),
}
