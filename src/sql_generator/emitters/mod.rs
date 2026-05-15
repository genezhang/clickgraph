//! Per-dialect SQL emitters.
//!
//! Each subdirectory hosts the full SQL generation code for one target
//! database. Phase 0.3 moved the existing ClickHouse code (formerly
//! `crate::clickhouse_query_generator`) here; the planned Databricks
//! emitter will land next to it as a sibling module.
//!
//! The old `crate::clickhouse_query_generator` path is preserved as a
//! transparent re-export in `lib.rs` so call sites can migrate at their
//! own pace.

pub mod clickhouse;
