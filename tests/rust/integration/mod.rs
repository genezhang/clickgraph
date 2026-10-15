//! Integration tests - Tests that require ClickHouse or other external dependencies
//!
//! These tests verify that components work together correctly with real dependencies.

mod binder_corpus;
mod clause_list_parity;
mod corpus_sweep;
#[cfg(feature = "databricks")]
mod databricks_introspect_tests;
mod metrics_endpoint_tests;
mod outcome_loop_example_tests;
mod sql_generation_handler_comment_tests;
mod translate_seam_endpoint_tests;
mod with_cte_column_refs;
