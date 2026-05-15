//! Function-name mapping per dialect.
//!
//! `render_plan` and helpers construct synthetic SQL expressions that
//! reference function names directly (e.g. `groupArray`, `arrayElement`).
//! This module funnels those names through a `FunctionMapper` trait so
//! Phase 1 can swap in a Databricks/Spark SQL implementation without
//! grepping for string literals across the planner.
//!
//! ## Phase 0.2 status
//! Only `ClickhouseFunctionMapper` is implemented. `current_function_mapper`
//! returns it unconditionally; Phase 1 will replace this with a dialect-aware
//! accessor once the dialect is plumbed through call sites.

pub(crate) mod clickhouse;

/// Returns the dialect-specific name and casting syntax for built-in SQL
/// functions used by `render_plan` and downstream emission helpers.
///
/// Methods returning `&'static str` give the canonical function name —
/// callers compose `name(args...)` themselves. Methods returning `String`
/// emit a complete fragment because the *shape* differs between dialects
/// (e.g. ClickHouse `toInt64(x)` vs Spark SQL `CAST(x AS BIGINT)`).
pub(crate) trait FunctionMapper: Send + Sync {
    /// Collect into a list aggregate. CH: `groupArray`. Spark: `collect_list`.
    fn collect_list(&self) -> &'static str;

    /// 1-based array indexing. CH: `arrayElement`. Spark: `element_at`.
    fn array_element(&self) -> &'static str;

    /// Conditional count. CH: `countIf`. Spark: `count_if` (DBR 13.1+).
    fn count_if(&self) -> &'static str;

    /// Count of array elements matching a predicate.
    /// CH: `arrayCount`. Spark: requires `size(filter(...))` wrapping —
    /// the helper [`array_count_call`](Self::array_count_call) takes care
    /// of the dialect difference.
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
}

/// The default function mapper for the current build.
///
/// `pub(crate)` because the trait is internal; once dialect selection is
/// plumbed through `render_plan` (Phase 1), call sites will receive the
/// mapper from the active emitter rather than this static accessor.
pub(crate) fn current_function_mapper() -> &'static dyn FunctionMapper {
    static CLICKHOUSE: clickhouse::ClickhouseFunctionMapper = clickhouse::ClickhouseFunctionMapper;
    &CLICKHOUSE
}
