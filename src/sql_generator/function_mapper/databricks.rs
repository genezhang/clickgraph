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
//! ### Known structural gaps (panic with TODO until Phase 1.1)
//!
//! **`array_count`**: ClickHouse's `arrayCount(arr, x -> pred)` has no
//! equivalent single function in Spark — the idiom is
//! `size(filter(arr, x -> pred))`. That's a *structural* rewrite, not a
//! name swap, so `FunctionMapper::array_count(&self) -> &'static str`
//! can't express it.
//!
//! **`json_extract_string`**: CH's `JSONExtractString(blob, 'field')` takes
//! a bare field name; Spark's `get_json_object(blob, '$.field')` takes a
//! JSONPath. Same name shape but the second argument needs structural
//! rewriting. Returning `"get_json_object"` would silently emit broken
//! SQL.
//!
//! Both panic with a TODO so callers can't accidentally produce wrong
//! output. Phase 1.1 will pick a strategy — likely widening the call
//! sites in `select_builder.rs` and `plan_builder_utils.rs` rather than
//! widening the trait, since each gap has only one call site and the
//! structural choice belongs there.

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
        // Structural mismatch — see module docs. CH passes the property as a
        // bare field name (`'OriginCityName'`); Spark's `get_json_object`
        // wants a JSONPath (`'$.OriginCityName'`). The call site builds the
        // function args, so it must do the rewrite. Panicking here prevents
        // silent breakage if a future caller forgets.
        unimplemented!(
            "DatabricksFunctionMapper::json_extract_string: Spark's `get_json_object` \
             requires a JSONPath argument (`$.field`) but the existing call site passes a \
             bare field name. Phase 1.1 must rewrite the argument at the call site before \
             this mapping can be used."
        );
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
    /// The panic is intentional — Phase 1.1 must either widen the trait or
    /// rewrite the single call site to build `size(filter(...))` directly.
    #[test]
    #[should_panic(expected = "Spark has no `arrayCount")]
    fn databricks_array_count_panics() {
        let m = for_dialect(SqlDialect::Databricks);
        m.array_count();
    }

    /// Documented structural gap: `get_json_object` needs a JSONPath
    /// argument; the call site passes a bare field name. Phase 1.1 must
    /// rewrite the argument before the mapping can be used.
    #[test]
    #[should_panic(expected = "requires a JSONPath argument")]
    fn databricks_json_extract_string_panics() {
        let m = for_dialect(SqlDialect::Databricks);
        m.json_extract_string();
    }

    /// `current_function_mapper()` still returns ClickHouse — Phase 1.1
    /// hasn't plumbed dialect through call sites yet.
    #[test]
    fn current_mapper_still_clickhouse() {
        assert_eq!(
            super::super::current_function_mapper().cast_string(),
            "toString"
        );
    }
}
