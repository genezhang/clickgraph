//! Databricks / Spark SQL `FunctionMapper`.
//!
//! This is the second consumer of the [`FunctionMapper`] trait, added as
//! Phase 1.0 of the DeltaGraph refactor. Its purpose is to validate the
//! shape of the trait by exercising every method against a real second
//! dialect. The Databricks SQL emitter itself is not yet wired in;
//! `current_function_mapper()` still returns ClickHouse. Phase 1.1 will
//! plumb the dialect through call sites so this mapper gets used at
//! emission time.
//!
//! ### Spelling references
//! - <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin>
//! - Apache Spark SQL function reference
//!
//! ### Known structural gap: `array_count`
//!
//! ClickHouse's `arrayCount(arr, x -> pred)` has no equivalent single
//! function in Spark — the idiom is `size(filter(arr, x -> pred))`.
//! That's a *structural* rewrite, not a name swap, so
//! `FunctionMapper::array_count(&self) -> &'static str` can't express
//! it. This method panics; the two call sites in
//! `plan_builder_utils.rs` branch on dialect and build the
//! `size(filter(...))` form directly when running against Databricks
//! (added in Phase 1.1).
//!
//! ### Resolved gap (Phase 1.1): `json_extract_string`
//!
//! CH's `JSONExtractString(blob, 'field')` takes a bare field name;
//! Spark's `get_json_object(blob, '$.field')` takes a JSONPath. The
//! mapper returns `"get_json_object"`; the call site in
//! `select_builder.rs` prepends `$.` to the field name when the active
//! dialect is Databricks. The mapper-level structural gap is gone —
//! only the *argument shape* differs, and that lives at the call site.

use super::FunctionMapper;

pub(crate) struct DatabricksFunctionMapper;

impl FunctionMapper for DatabricksFunctionMapper {
    fn collect_list(&self) -> &'static str {
        "collect_list"
    }

    fn array_element(&self) -> &'static str {
        // Spark/Databricks: element_at(array, index) is 1-based, matching CH's arrayElement.
        "element_at"
    }

    fn count_if(&self) -> &'static str {
        // Databricks Runtime 13.1+ ships count_if. Older runtimes need
        // SUM(CASE WHEN ... THEN 1 ELSE 0 END) — flagged here so a future
        // version-aware mapper can fork.
        "count_if"
    }

    fn array_count(&self) -> &'static str {
        // Structural mismatch — see module docs. No single function name.
        unimplemented!(
            "DatabricksFunctionMapper::array_count: Spark has no `arrayCount(arr, pred)`; \
             use `size(filter(arr, pred))` at the call site. This panics so callers can't \
             silently emit broken SQL — Phase 1.1 will rework the API or the call site."
        );
    }

    fn json_extract_string(&self) -> &'static str {
        // The function name is a clean swap; the *argument* needs JSONPath
        // shape (`$.field` instead of `field`). The call site in
        // `select_builder.rs` does that rewrite — see Phase 1.1 module docs.
        "get_json_object"
    }

    fn cast_int64(&self) -> &'static str {
        // Spark function-call cast alias for BIGINT.
        "bigint"
    }

    fn cast_uint8(&self) -> &'static str {
        // Spark has no unsigned integer type. The values cast here fit in a
        // signed tinyint (1-byte, -128..127), which matches the range
        // ClickHouse's UInt8 uses in this codebase (small enum-like values).
        // Documented as a deliberate widening, not a bug.
        "tinyint"
    }

    fn cast_string(&self) -> &'static str {
        "string"
    }

    fn array_concat(&self) -> &'static str {
        // Spark's `concat` is overloaded for arrays — same call shape as CH's
        // `arrayConcat(a, b)`.
        "concat"
    }

    fn array_contains(&self) -> &'static str {
        "array_contains"
    }

    fn empty_string_array_cast(&self) -> &'static str {
        "CAST(array() AS ARRAY<STRING>)"
    }

    fn empty_int64_array_cast(&self) -> &'static str {
        "CAST(array() AS ARRAY<BIGINT>)"
    }
}

#[cfg(test)]
mod tests {
    use crate::sql_generator::function_mapper::for_dialect;
    use crate::sql_generator::SqlDialect;

    /// Lock in the Spark/Databricks spellings. If any of these change we
    /// want a failing test, not a silent regression in generated SQL.
    #[test]
    fn databricks_spellings() {
        let m = for_dialect(SqlDialect::Databricks);
        assert_eq!(m.collect_list(), "collect_list");
        assert_eq!(m.array_element(), "element_at");
        assert_eq!(m.count_if(), "count_if");
        assert_eq!(m.json_extract_string(), "get_json_object");
        assert_eq!(m.cast_int64(), "bigint");
        assert_eq!(m.cast_uint8(), "tinyint");
        assert_eq!(m.cast_string(), "string");
        assert_eq!(m.array_concat(), "concat");
        assert_eq!(m.array_contains(), "array_contains");
        assert_eq!(
            m.empty_string_array_cast(),
            "CAST(array() AS ARRAY<STRING>)"
        );
        assert_eq!(m.empty_int64_array_cast(), "CAST(array() AS ARRAY<BIGINT>)");
    }

    /// Documented structural gap: `array_count` has no clean Spark mapping.
    /// The panic is intentional — the two call sites in
    /// `plan_builder_utils.rs` branch on dialect and build
    /// `size(filter(...))` directly when Databricks is active, so they
    /// never reach this method.
    #[test]
    #[should_panic(expected = "Spark has no `arrayCount")]
    fn databricks_array_count_panics() {
        let m = for_dialect(SqlDialect::Databricks);
        m.array_count();
    }

    /// Outside a task-local scope `current_function_mapper()` defaults to
    /// ClickHouse — the historical hardcoded behavior. Tests that don't
    /// opt into a context keep emitting CH SQL.
    #[test]
    fn current_mapper_defaults_to_clickhouse_outside_scope() {
        assert_eq!(
            super::super::current_function_mapper().cast_string(),
            "toString"
        );
    }

    /// Inside a task-local scope with `dialect: Databricks`,
    /// `current_function_mapper()` returns the Databricks mapper. This is
    /// the Phase 1.1 plumbing — once Phase 1.2's emitter sets the dialect
    /// in the context, every `current_function_mapper()` call site flips
    /// to Databricks spellings automatically.
    #[tokio::test]
    async fn current_mapper_follows_task_local_dialect() {
        use crate::server::query_context::{with_query_context, QueryContext};

        let ctx = QueryContext {
            dialect: SqlDialect::Databricks,
            ..QueryContext::default()
        };
        let s = with_query_context(ctx, async {
            super::super::current_function_mapper().cast_string()
        })
        .await;
        assert_eq!(s, "string");
    }
}
