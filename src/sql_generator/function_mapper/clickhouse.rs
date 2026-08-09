//! ClickHouse `FunctionMapper` — the canonical names used by the existing
//! `clickhouse_query_generator` SQL emission path.

use super::FunctionMapper;

pub(crate) struct ClickhouseFunctionMapper;

impl FunctionMapper for ClickhouseFunctionMapper {
    fn collect_list(&self) -> &'static str {
        "groupArray"
    }

    fn array_element(&self) -> &'static str {
        "arrayElement"
    }

    fn array_element_or_null(&self, arr: &str, idx: &str) -> String {
        // CH `arr[i]`/`arrayElement` return the element type's default (0, '')
        // for an out-of-range index; `arrayElementOrNull` returns NULL instead,
        // matching openCypher `list[i]` out-of-bounds semantics. In-bounds and
        // negative (from-the-end) indices behave identically to `arrayElement`.
        format!("arrayElementOrNull({}, {})", arr, idx)
    }

    fn count_if(&self) -> &'static str {
        "countIf"
    }

    fn any(&self) -> &'static str {
        "any"
    }

    fn min_if(&self, val: &str, cond: &str) -> String {
        format!("minIf({val}, {cond})")
    }

    fn min_or_null(&self) -> &'static str {
        "minOrNull"
    }

    fn array_count(&self) -> &'static str {
        "arrayCount"
    }

    fn json_extract_string(&self) -> &'static str {
        "JSONExtractString"
    }

    fn cast_int64(&self) -> &'static str {
        "toInt64"
    }

    fn cast_uint8(&self) -> &'static str {
        "toUInt8"
    }

    fn cast_uint16(&self) -> &'static str {
        "toUInt16"
    }

    fn cast_float64(&self) -> &'static str {
        "toFloat64"
    }

    fn cast_int64_or_null(&self, expr: &str) -> String {
        // CH `toInt64OrNull` returns NULL on parse failure (String arg only).
        format!("toInt64OrNull({})", expr)
    }

    fn cast_float64_or_null(&self, expr: &str) -> String {
        format!("toFloat64OrNull({})", expr)
    }

    fn cast_string(&self) -> &'static str {
        "toString"
    }

    fn to_string_float(&self, expr: &str) -> String {
        // CH `toString(toFloat64(3))` returns "3" (drops the decimal), and a
        // whole-valued float literal has already rendered as the integer SQL
        // literal `3`. Append `.0` iff the string form is a bare integer with no
        // `.`/`e`/`inf`/`nan` — matched by `^-?[0-9]+$`. Fractional, scientific,
        // NULL, and inf/nan forms pass through `toString` untouched. The caller
        // gates this on `RenderType::Float`, so an integer-valued *float* is the
        // only thing that reaches the `concat` arm. `expr` is pre-rendered SQL.
        // #1055.
        format!(
            "if(match(toString({e}), '^-?[0-9]+$'), concat(toString({e}), '.0'), toString({e}))",
            e = expr
        )
    }

    fn cast_bool(&self, expr: &str) -> String {
        // Nullable(Bool) preserves NULL (bare `CAST(NULL>2 AS Bool)` throws
        // Code 70) and is physically UInt8, so this changes only the wire
        // display (1/0 → true/false), never downstream semantics. #1057.
        format!("CAST({expr} AS Nullable(Bool))")
    }

    fn array_concat(&self) -> &'static str {
        "arrayConcat"
    }

    fn array_contains(&self) -> &'static str {
        "has"
    }

    fn integer_division(&self) -> &'static str {
        // Truncates toward zero: intDiv(-7, 2) = -3, matching Neo4j.
        "intDiv"
    }

    fn empty_string_array_cast(&self) -> &'static str {
        "CAST([] AS Array(String))"
    }

    fn empty_int64_array_cast(&self) -> &'static str {
        "CAST([] AS Array(Int64))"
    }

    fn int64_array_cast(&self, expr: &str) -> String {
        format!("CAST({expr} AS Array(Int64))")
    }

    fn array_literal(&self, elems: &str) -> String {
        format!("[{elems}]")
    }

    fn tuple_constructor(&self) -> &'static str {
        "tuple"
    }

    fn quote_alias(&self, name: &str) -> String {
        // CH escapes `"` inside a double-quoted identifier by doubling it.
        // Aliases inferred from raw return text can contain quotes
        // (e.g., `RETURN 'a"b'` derives an alias from the literal),
        // and naive wrapping would produce malformed SQL.
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn cast_as(&self, expr: &str, type_name: &str) -> String {
        // ClickHouse function-call CAST with a quoted type string.
        format!("CAST({}, '{}')", expr, type_name)
    }

    fn array_slice(&self, arr: &str, offset: &str, length: Option<&str>) -> String {
        match length {
            Some(l) => format!("arraySlice({}, {}, {})", arr, offset, l),
            None => format!("arraySlice({}, {})", arr, offset),
        }
    }

    fn array_length(&self, arr: &str) -> String {
        // CH `length` is overloaded for arrays and strings alike.
        format!("length({})", arr)
    }

    fn epoch_millis_to_timestamp(&self, expr: &str) -> String {
        format!("fromUnixTimestamp64Milli({})", expr)
    }

    fn timestamp_to_epoch_millis(&self, expr: &str) -> String {
        format!("toUnixTimestamp64Milli({})", expr)
    }

    fn json_row_object(&self, columns: &str) -> String {
        format!("formatRowNoNewline('JSONEachRow', {})", columns)
    }

    fn try_parse_int128(&self, expr: &str) -> String {
        format!("toInt128OrNull({})", expr)
    }

    fn id_order_key_nulls_clause(&self) -> &'static str {
        // No-op for CH — NULL already sorts last for both ASC and DESC by
        // default — but explicit for parity with Databricks (#556).
        " NULLS LAST"
    }

    fn order_by_nulls_clause(&self, descending: bool) -> &'static str {
        // CH sorts NULL last for BOTH ASC and DESC by default. Neo4j wants
        // nulls-last on ASC (already matches → stay bare) and nulls-first on
        // DESC (needs an explicit override). #1065.
        if descending {
            " NULLS FIRST"
        } else {
            ""
        }
    }

    fn percentile_aggregate(&self, expr: &str, percentile: &str, continuous: bool) -> String {
        if continuous {
            // percentileCont = linear interpolation. ClickHouse quantiles are
            // parametric aggregates: the percentile is a leading parameter,
            // `quantile...(p)(expr)`, NOT an argument (#639).
            // `quantileExactInclusive` matches Neo4j's percentileCont algorithm
            // (floatIdx = p*(n-1), interpolate) exactly — verified live across
            // odd/even/single/duplicate datasets at p ∈ {0, .25, .5, .75, .9, 1}.
            format!("quantileExactInclusive({percentile})({expr})")
        } else {
            // percentileDisc = nearest actual value at Neo4j's index convention:
            // 1-based idx = greatest(1, ceil(p * n)), n = non-null count. NO
            // ClickHouse quantile variant reproduces this — quantileExact,
            // quantileExactLow and quantileExactHigh all use a different
            // rounding and return the wrong element for a large fraction of
            // inputs (e.g. [10,20,30,40]@0.25 → Neo4j 10 but quantileExact* 20).
            // So build the exact index form by hand over the sorted value array.
            // Verified: 0 mismatches vs the Neo4j formula across n=1..40 ×
            // p=0.05..0.95, and live against the endpoint corpus (#639).
            //
            // `arrayElementOrNull` (not `arrayElement`): on an EMPTY group the
            // index is 1 but the array is empty, and bare `arrayElement([], 1)`
            // returns the type default (0) — a silent wrong value. `…OrNull`
            // returns NULL there, matching percentileCont, `median`, and Neo4j
            // (percentile over an empty set is null). Identical to `arrayElement`
            // on every non-empty input (verified).
            format!(
                "arrayElementOrNull(arraySort(groupArray({expr})), greatest(1, toUInt32(ceil({percentile} * count({expr})))))"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClickhouseFunctionMapper;
    use super::FunctionMapper;

    #[test]
    fn quote_alias_escapes_embedded_double_quotes() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.quote_alias("b.id"), "\"b.id\"");
        assert_eq!(m.quote_alias("x\"y"), "\"x\"\"y\"");
    }

    #[test]
    fn cast_as_uses_clickhouse_function_call_form() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.cast_as("''", "String"), "CAST('', 'String')");
        assert_eq!(
            m.cast_as("NULL", "Nullable(Int64)"),
            "CAST(NULL, 'Nullable(Int64)')"
        );
    }

    #[test]
    fn cast_bool_uses_nullable_bool_for_null_safety() {
        // #1057: Nullable(Bool), not bare Bool — CAST(NULL>2 AS Bool) throws
        // Code 70. Nullable preserves NULL and prints true/false.
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.cast_bool("u.user_id > 2"),
            "CAST(u.user_id > 2 AS Nullable(Bool))"
        );
    }

    #[test]
    fn to_string_float_appends_dot_zero_only_for_integer_valued_forms() {
        // #1055: a whole-valued float keeps `.0` (Neo4j toString(3.0)="3.0"); the
        // `match ^-?[0-9]+$` guard adds `.0` only when the string form is a bare
        // integer, so fractional/scientific/NULL/inf/nan pass through untouched.
        // The caller gates this on RenderType::Float, so only floats reach here.
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.to_string_float("3"),
            "if(match(toString(3), '^-?[0-9]+$'), concat(toString(3), '.0'), toString(3))"
        );
        assert_eq!(
            m.to_string_float("toFloat64(x)"),
            "if(match(toString(toFloat64(x)), '^-?[0-9]+$'), \
             concat(toString(toFloat64(x)), '.0'), toString(toFloat64(x)))"
        );
    }

    #[test]
    fn order_by_nulls_clause_only_overrides_desc() {
        // #1065: CH default is nulls-last for both directions. Neo4j wants
        // nulls-last on ASC (already matches → bare) and nulls-first on DESC.
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.order_by_nulls_clause(false), "");
        assert_eq!(m.order_by_nulls_clause(true), " NULLS FIRST");
    }

    #[test]
    fn array_slice_keeps_clickhouse_2_and_3_arg_forms() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.array_slice("a", "2", Some("3")), "arraySlice(a, 2, 3)");
        assert_eq!(m.array_slice("a", "2", None), "arraySlice(a, 2)");
    }

    #[test]
    fn array_length_uses_clickhouse_overloaded_length() {
        // CH `length` works on arrays and strings alike; used to normalize
        // negative list-slice bounds.
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.array_length("arr"), "length(arr)");
    }

    #[test]
    fn array_element_or_null_uses_clickhouse_null_returning_accessor() {
        // CH `arr[i]`/`arrayElement` return the type default (0/'') on
        // out-of-bounds; `arrayElementOrNull` returns NULL, matching the
        // openCypher `list[i]` contract.
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.array_element_or_null("[10, 20, 30]", "11"),
            "arrayElementOrNull([10, 20, 30], 11)"
        );
        assert_eq!(
            m.array_element_or_null("names", "if((0 - 1) >= 0, (0 - 1)+1, (0 - 1))"),
            "arrayElementOrNull(names, if((0 - 1) >= 0, (0 - 1)+1, (0 - 1)))"
        );
    }

    #[test]
    fn in_list_predicate_renders_paren_value_list() {
        // Value-list `x IN (a, b)` — never `x IN [array]` (a heterogeneous CH
        // array literal fails NO_COMMON_TYPE; SQL IN coerces per-element). Empty
        // list collapses to the constant predicate.
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.in_list_predicate("x", &["1".into(), "2".into()], false),
            "x IN (1, 2)"
        );
        assert_eq!(
            m.in_list_predicate("x", &["1".into(), "2".into()], true),
            "x NOT IN (1, 2)"
        );
        assert_eq!(
            m.in_list_predicate("u.country", &["u.city".into(), "'USA'".into()], false),
            "u.country IN (u.city, 'USA')"
        );
        assert_eq!(m.in_list_predicate("x", &[], false), "FALSE");
        assert_eq!(m.in_list_predicate("x", &[], true), "TRUE");
    }

    #[test]
    fn power_uses_ansi_power_call_form() {
        // ClickHouse has no infix `^` (Code 62 SYNTAX_ERROR); the exponentiation
        // operator must render as the ANSI `POWER(base, exp)` call.
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.power("2", "3"), "POWER(2, 3)");
        assert_eq!(m.power("n.user_id", "2"), "POWER(n.user_id, 2)");
    }

    #[test]
    fn epoch_millis_timestamp_roundtrip_uses_clickhouse_functions() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.epoch_millis_to_timestamp("x"),
            "fromUnixTimestamp64Milli(x)"
        );
        assert_eq!(
            m.timestamp_to_epoch_millis("x"),
            "toUnixTimestamp64Milli(x)"
        );
    }

    #[test]
    fn json_row_object_uses_format_row_no_newline() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.json_row_object("a.x AS x, a.y AS y"),
            "formatRowNoNewline('JSONEachRow', a.x AS x, a.y AS y)"
        );
    }

    #[test]
    fn min_if_emits_native_clickhouse_form() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.min_if("toUInt16(hop)", "node_id = 14"),
            "minIf(toUInt16(hop), node_id = 14)"
        );
    }

    #[test]
    fn min_or_null_uses_clickhouse_specific_name() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.min_or_null(), "minOrNull");
    }

    #[test]
    fn percentile_aggregate_uses_parametric_quantile_forms() {
        let m = ClickhouseFunctionMapper;
        // Cont = linear interpolation → parametric quantileExactInclusive
        // (percentile in a leading parameter, not an argument) (#639).
        assert_eq!(
            m.percentile_aggregate("t.x", "0.9", true),
            "quantileExactInclusive(0.9)(t.x)"
        );
        // Disc = nearest value at Neo4j's 1-based index greatest(1, ceil(p*n)).
        // No CH quantile builtin matches this, so it's a hand-built array-index
        // form over the sorted values.
        assert_eq!(
            m.percentile_aggregate("t.x", "0.9", false),
            "arrayElementOrNull(arraySort(groupArray(t.x)), greatest(1, toUInt32(ceil(0.9 * count(t.x)))))"
        );
    }
}
