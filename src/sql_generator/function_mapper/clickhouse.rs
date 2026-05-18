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
}
