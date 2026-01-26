//! Integration tests - Tests that require ClickHouse or other external dependencies
//!
//! These tests verify that components work together correctly with real dependencies.

mod complex_feature_tests;
mod cte_column_aliasing_tests;
mod parameter_function_test;
mod path_variable_tests;
mod with_where_having_tests;
