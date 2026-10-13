//! The naming conventions of a variable-length path's CTE (the path
//! generator, `sql_generator/emitters/clickhouse/variable_length_cte.rs`,
//! and the code that reads its columns). The legacy planner's join context
//! that lived here went in P-4c S14c5.

/// Default FROM alias used for VLP CTEs in outer query.
/// Example: `FROM vlp_u1_u2 AS t` - the "t" is this constant.
///
/// All code generating or referencing VLP CTEs MUST use this constant.
pub const VLP_CTE_FROM_ALIAS: &str = "t";

/// Column name for the start node ID in VLP CTEs.
/// Example: `SELECT ... start_node.id AS start_id ...` in recursive CTE.
///
/// All code generating or referencing VLP CTEs MUST use this constant.
pub const VLP_START_ID_COLUMN: &str = "start_id";

/// Column name for the end node ID in VLP CTEs.
/// Example: `SELECT ... end_node.id AS end_id ...` in recursive CTE.
///
/// All code generating or referencing VLP CTEs MUST use this constant.
pub const VLP_END_ID_COLUMN: &str = "end_id";
