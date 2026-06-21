//! Databricks / Spark SQL `FunctionMapper`.
//!
//! This is the second consumer of the [`FunctionMapper`] trait, added as
//! Phase 1.0 of the DeltaGraph refactor. Its purpose is to validate the
//! shape of the trait by exercising every method against a real second
//! dialect. Phase 1.1 wired the dialect through the rendering pipeline
//! via the task-local `QueryContext`: `current_function_mapper()` now
//! reads from there and defaults to ClickHouse outside a scope. The
//! Databricks SQL emitter itself (`DatabricksEmitter::emit`) still isn't
//! implemented — that's Phase 1.2.
//!
//! ### Spelling references
//! - <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin>
//! - Apache Spark SQL function reference
//!
//! ### Known structural gap: `array_count`
//!
//! ClickHouse's `arrayCount(x -> pred, arr)` (predicate first) has no
//! equivalent single function in Spark — the idiom is
//! `size(filter(arr, x -> pred))` (arg order reversed, and wrapped in
//! `size`). That's a *structural* rewrite, not a name swap, so
//! `FunctionMapper::array_count(&self) -> &'static str` can't express
//! it. This method panics; the two call sites in
//! `plan_builder_utils.rs` go through the helper
//! `emit_array_count_call` which branches on dialect and builds the
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

    fn min_if(&self, val: &str, cond: &str) -> String {
        // Spark has no `minIf`. `min` ignores NULLs in ANSI SQL, so a
        // `CASE WHEN cond THEN val END` (no ELSE → NULL) reproduces
        // `minIf(val, cond)` exactly: matching rows contribute `val`,
        // non-matching rows contribute NULL and get dropped by `min`.
        format!("min(CASE WHEN {cond} THEN {val} END)")
    }

    fn min_or_null(&self) -> &'static str {
        // Spark's `min` already returns NULL on empty input (ANSI), so the
        // CH-only `minOrNull` quirk doesn't apply — plain `min` is the
        // direct equivalent.
        "min"
    }

    fn array_count(&self) -> &'static str {
        // Structural mismatch — see module docs. No single function name.
        // Phase 1.1 resolved this at the call sites via
        // `emit_array_count_call` in `plan_builder_utils.rs`; the panic
        // stays as a tripwire for any future caller that doesn't go
        // through the helper.
        unimplemented!(
            "DatabricksFunctionMapper::array_count: Spark has no `arrayCount(pred, arr)`; \
             use `size(filter(arr, pred))` at the call site (see emit_array_count_call). \
             This panics so callers can't silently emit broken SQL."
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

    fn cast_uint16(&self) -> &'static str {
        // Spark has no unsigned 16-bit type. Widening to signed
        // smallint (16-bit, max 32767) is conceptually closest but
        // unsafe in this codebase: `max_hops` is a `u32` overridable
        // via `CLICKGRAPH_VLP_MAX_HOPS` with no upper bound, so a
        // signed 16-bit cast could wrap. Widen to signed `int`
        // (32-bit) instead — it matches the actual u32 source range
        // (modulo the high bit, which no realistic hop count reaches)
        // and removes the overflow tripwire entirely.
        "int"
    }

    fn cast_float64(&self) -> &'static str {
        "double"
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

    fn int64_array_cast(&self, expr: &str) -> String {
        format!("CAST({expr} AS ARRAY<BIGINT>)")
    }

    fn array_literal(&self, elems: &str) -> String {
        format!("array({elems})")
    }

    fn tuple_constructor(&self) -> &'static str {
        // Spark's `struct(a, b, c)` is the analogue of CH's `tuple()`:
        // element-wise ordering and equality match.
        "struct"
    }

    fn quote_alias(&self, name: &str) -> String {
        // Spark parses `"foo"` as a string literal — backticks are the
        // only valid identifier quote. Spark escapes `` ` `` inside a
        // backtick-quoted identifier by doubling it. Aliases inferred
        // from raw return text can contain backticks, so escape before
        // wrapping. `quote_identifier` uses backticks for both dialects
        // so this stays consistent with that.
        format!("`{}`", name.replace('`', "``"))
    }

    fn cast_as(&self, expr: &str, type_name: &str) -> String {
        // Spark/ANSI CAST(expr AS TYPE) with an unquoted type keyword.
        format!("CAST({} AS {})", expr, type_name)
    }

    fn array_slice(&self, arr: &str, offset: &str, length: Option<&str>) -> String {
        // Spark slice(arr, start, length) requires a length, and ERRORS on a
        // negative one — whereas CH's 2-arg arraySlice(arr, offset) silently
        // returns empty when offset is past the end. So the computed
        // rest-from-offset length, size(arr) - offset + 1, is floored at 0 with
        // greatest(...) to preserve CH's empty-on-out-of-bounds behavior.
        // (Note: `arr` is evaluated twice here; fine for column/literal arrays.)
        match length {
            Some(l) => format!("slice({}, {}, {})", arr, offset, l),
            None => format!(
                "slice({}, {}, greatest(size({}) - ({}) + 1, 0))",
                arr, offset, arr, offset
            ),
        }
    }

    fn epoch_millis_to_timestamp(&self, expr: &str) -> String {
        // Spark TIMESTAMP from epoch milliseconds. Verified on Databricks SQL:
        // unix_millis(timestamp_millis(x)) round-trips exactly under UTC.
        format!("timestamp_millis({})", expr)
    }

    fn timestamp_to_epoch_millis(&self, expr: &str) -> String {
        format!("unix_millis({})", expr)
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
        assert_eq!(
            m.min_if("int(hop)", "node_id = 14"),
            "min(CASE WHEN node_id = 14 THEN int(hop) END)"
        );
        assert_eq!(m.min_or_null(), "min");
        assert_eq!(m.json_extract_string(), "get_json_object");
        assert_eq!(m.cast_int64(), "bigint");
        assert_eq!(m.cast_uint8(), "tinyint");
        assert_eq!(m.cast_uint16(), "int");
        assert_eq!(m.cast_float64(), "double");
        assert_eq!(m.cast_string(), "string");
        assert_eq!(m.array_concat(), "concat");
        assert_eq!(m.array_contains(), "array_contains");
        assert_eq!(m.epoch_millis_to_timestamp("x"), "timestamp_millis(x)");
        assert_eq!(m.timestamp_to_epoch_millis("x"), "unix_millis(x)");
        assert_eq!(
            m.empty_string_array_cast(),
            "CAST(array() AS ARRAY<STRING>)"
        );
        assert_eq!(m.empty_int64_array_cast(), "CAST(array() AS ARRAY<BIGINT>)");
        assert_eq!(m.int64_array_cast("x"), "CAST(x AS ARRAY<BIGINT>)");
        assert_eq!(m.array_literal(""), "array()");
        assert_eq!(m.array_literal("a, b"), "array(a, b)");
        assert_eq!(m.tuple_constructor(), "struct");
        assert_eq!(m.quote_alias("b.id"), "`b.id`");
        // Embedded backticks must be doubled, not left raw — otherwise
        // an alias like `` x`y `` would prematurely close the quote.
        assert_eq!(m.quote_alias("x`y"), "`x``y`");
        // ANSI CAST syntax with unquoted type keyword.
        assert_eq!(m.cast_as("''", "STRING"), "CAST('' AS STRING)");
        assert_eq!(m.cast_as("NULL", "BIGINT"), "CAST(NULL AS BIGINT)");
        // slice requires a length; the 2-arg CH form computes one.
        assert_eq!(m.array_slice("arr", "2", Some("3")), "slice(arr, 2, 3)");
        assert_eq!(
            m.array_slice("arr", "2", None),
            "slice(arr, 2, greatest(size(arr) - (2) + 1, 0))"
        );
    }

    /// Documented structural gap: `array_count` has no clean Spark mapping.
    /// The panic is intentional — the two call sites in
    /// `plan_builder_utils.rs` branch on dialect and build
    /// `size(filter(...))` directly when Databricks is active, so they
    /// never reach this method.
    #[test]
    #[should_panic(expected = "Spark has no `arrayCount(pred, arr)`")]
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
