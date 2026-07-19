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

    fn count_if(&self) -> &'static str {
        "countIf"
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

    fn cast_string(&self) -> &'static str {
        "toString"
    }

    fn array_concat(&self) -> &'static str {
        "arrayConcat"
    }

    fn array_contains(&self) -> &'static str {
        "has"
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
    fn array_slice_keeps_clickhouse_2_and_3_arg_forms() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.array_slice("a", "2", Some("3")), "arraySlice(a, 2, 3)");
        assert_eq!(m.array_slice("a", "2", None), "arraySlice(a, 2)");
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
