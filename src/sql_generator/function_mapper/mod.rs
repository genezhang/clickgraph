//! Function-name mapping per dialect.
//!
//! `render_plan` and helpers construct synthetic SQL expressions that
//! reference function names directly (e.g. `groupArray`, `arrayElement`).
//! This module funnels those names through a `FunctionMapper` trait so
//! Phase 1 can swap in a Databricks/Spark SQL implementation without
//! grepping for string literals across the planner.
//!
//! ## Status
//! Two mappers ship: `ClickhouseFunctionMapper` (production) and
//! `DatabricksFunctionMapper` (Phase 1.0 spike — exercised by unit tests
//! but not yet reached from emission). `current_function_mapper()`
//! still returns ClickHouse unconditionally because the dialect isn't
//! plumbed through call sites yet; use [`for_dialect`] when you need
//! an explicit one. Phase 1.1 will route call sites through
//! [`for_dialect`] with the active dialect.

pub(crate) mod clickhouse;
pub(crate) mod databricks;

/// Returns the dialect-specific name for built-in SQL functions used by
/// `render_plan` and downstream emission helpers.
///
/// All methods return `&'static str`. IR construction sites use the bare
/// name (it's later wrapped as `name(args)` by the SQL emitter); raw
/// SQL emission sites compose `format!("{name}({args})", ...)` directly.
/// This shape works for ClickHouse function-call syntax and for the
/// function-call cast aliases Spark/Databricks also accepts (`string(x)`,
/// `bigint(x)`, etc.). If a dialect ever needs full `CAST(x AS T)` syntax
/// or a structural rewrite (e.g. `arrayJoin` → `LATERAL VIEW explode`),
/// that becomes a dedicated method then — not a global redesign.
pub(crate) trait FunctionMapper: Send + Sync {
    /// Collect into a list aggregate. CH: `groupArray`. Spark: `collect_list`.
    fn collect_list(&self) -> &'static str;

    /// 1-based array indexing. CH: `arrayElement`. Spark: `element_at`.
    fn array_element(&self) -> &'static str;

    /// Conditional count. CH: `countIf`. Spark: `count_if` (DBR 13.1+).
    fn count_if(&self) -> &'static str;

    /// Count of array elements matching a predicate.
    /// CH: `arrayCount`. Spark needs structural rewriting to
    /// `size(filter(...))` — the planned Databricks emitter handles this
    /// at the call site once dialect is plumbed through (Phase 1).
    fn array_count(&self) -> &'static str;

    /// Extract JSON field as a string. CH: `JSONExtractString`.
    /// Spark: `get_json_object`.
    fn json_extract_string(&self) -> &'static str;

    /// Cast to 64-bit signed integer. CH: `toInt64`. Spark: `bigint`
    /// (works as a function-call cast alias).
    fn cast_int64(&self) -> &'static str;

    /// Cast to 8-bit unsigned integer. CH: `toUInt8`. Spark has no
    /// unsigned integer — the planned Databricks emitter will widen to
    /// `tinyint` (signed) since the values used here fit.
    fn cast_uint8(&self) -> &'static str;

    /// Cast to string. CH: `toString`. Spark: `string` (function-call alias).
    fn cast_string(&self) -> &'static str;

    /// Concatenate two arrays. CH: `arrayConcat`. Spark: `concat`
    /// (overloaded for arrays).
    fn array_concat(&self) -> &'static str;

    /// Test array membership. CH: `has`. Spark: `array_contains`.
    /// Used for cycle detection in VLP recursive CTEs.
    fn array_contains(&self) -> &'static str;

    /// Empty `Array(String)` literal with explicit cast. CH:
    /// `CAST([] AS Array(String))`. Spark: `CAST(array() AS ARRAY<STRING>)`.
    /// Returned as a full snippet (not a function name) because the array
    /// literal syntax and the element-type spelling both diverge.
    fn empty_string_array_cast(&self) -> &'static str;

    /// Empty `Array(Int64)` literal with explicit cast. CH:
    /// `CAST([] AS Array(Int64))`. Spark: `CAST(array() AS ARRAY<BIGINT>)`.
    fn empty_int64_array_cast(&self) -> &'static str;

    /// Array literal with the given comma-separated elements. CH:
    /// `[a, b, c]`. Spark: `array(a, b, c)`. Empty input (`""`) yields
    /// `[]` / `array()` respectively. Returned as `String` because the
    /// elements aren't known to the trait — callers join their own
    /// rendered expressions and pass them here.
    fn array_literal(&self, elems: &str) -> String;
}

/// Returns the function mapper for the active SQL dialect, read from the
/// task-local [`QueryContext`].
///
/// Outside a task-local scope (notably unit tests), this defaults to
/// ClickHouse — matching the historical hard-coded behavior so existing
/// tests don't need to opt in to a context.
///
/// The trait itself stays `pub(crate)`: external code can't name it, so
/// it can't implement it either, and the simpler visibility rule is
/// enough for now.
///
/// [`QueryContext`]: crate::server::query_context::QueryContext
pub(crate) fn current_function_mapper() -> &'static dyn FunctionMapper {
    for_dialect(crate::server::query_context::get_current_dialect())
}

/// Returns the function mapper for an explicit dialect.
///
/// `current_function_mapper()` delegates here with `ClickHouse`; Phase 1.1
/// will route call sites through this accessor with the active dialect once
/// it's plumbed through the rendering pipeline. Unsupported dialects panic
/// at the boundary rather than silently falling back, matching
/// `emitter_for`.
pub(crate) fn for_dialect(
    dialect: crate::sql_generator::SqlDialect,
) -> &'static dyn FunctionMapper {
    use crate::sql_generator::SqlDialect;
    static CLICKHOUSE: clickhouse::ClickhouseFunctionMapper = clickhouse::ClickhouseFunctionMapper;
    static DATABRICKS: databricks::DatabricksFunctionMapper = databricks::DatabricksFunctionMapper;
    match dialect {
        SqlDialect::ClickHouse => &CLICKHOUSE,
        SqlDialect::Databricks => &DATABRICKS,
        d => unimplemented!("FunctionMapper for dialect {:?} is not yet implemented", d),
    }
}
