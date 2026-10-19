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

/// A text of the VARIANT `x` that two values share exactly when Cypher's
/// DISTINCT takes them as equal: a number (`n:`) by its value, as a Cypher
/// number (a 64-bit integer or a double): an integral one within BIGINT as
/// its integer (`1` = `1.0` = a decimal `1.0`, `-0.0` = `0`), any other as
/// its double (`0.0001` written either way); a boolean (`b:`); any other
/// scalar by its type and text (a date is no string); a list (`l:`) or map
/// (`m:`, keys sorted, as a VARIANT object holds them) by its elements'
/// texts, two levels deep and by its JSON below; NULL one group (`:`).
fn variant_key(x: &str, depth: usize) -> String {
    const DEPTH: usize = 2;
    let ty = format!("schema_of_variant({x})");
    let d = format!("variant_get({x}, '$', 'DOUBLE')");
    let (list, map) = if depth < DEPTH {
        let (e, k, v) = (
            format!("__cg_e{depth}"),
            format!("__cg_k{depth}"),
            format!("__cg_v{depth}"),
        );
        (
            format!(
                "concat('l:', to_json(transform(CAST({x} AS ARRAY<VARIANT>), {e} -> {})))",
                variant_key(&e, depth + 1)
            ),
            format!(
                "concat('m:', to_json(transform_values(CAST({x} AS MAP<STRING, VARIANT>), \
                 ({k}, {v}) -> {})))",
                variant_key(&v, depth + 1)
            ),
        )
    } else {
        let json = format!("concat('c:', to_json({x}))");
        (json.clone(), json)
    };
    format!(
        "CASE WHEN {x} IS NULL OR {ty} = 'VOID' THEN ':' \
         WHEN {ty} = 'BOOLEAN' THEN concat('b:', CAST({x} AS STRING)) \
         WHEN {ty} RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' \
         THEN concat('n:', CAST(CAST({x} AS BIGINT) AS STRING)) \
         WHEN {ty} RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', \
         CASE WHEN {d} = floor({d}) AND abs({d}) < 9.2E18 \
         THEN CAST(CAST({d} AS BIGINT) AS STRING) ELSE CAST({d} AS STRING) END) \
         WHEN {ty} RLIKE '^ARRAY' THEN {list} \
         WHEN {ty} RLIKE '^OBJECT' THEN {map} \
         ELSE concat({ty}, ':', CAST({x} AS STRING)) END"
    )
}

/// The Databricks `CypherUnion::order_key`: the value's type and JSON text,
/// timestamps to the microsecond (`to_json`'s default keeps milliseconds).
/// A STRUCT (a node's or relationship's tuple) casts to no VARIANT, so not
/// `variant_key`; the type tells apart values of one text in a typed column
/// (a date and its string). Inside a VARIANT (`typeof` is `variant`) a date
/// and its string still share a key: their order among themselves is not
/// fixed.
fn order_key(v: &str) -> String {
    format!(
        "concat(typeof({v}), ':', to_json(named_struct('v', {v}), \
         map('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSSXXX', \
         'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS')))"
    )
}

pub(crate) struct DatabricksFunctionMapper;

impl FunctionMapper for DatabricksFunctionMapper {
    fn array_element_or_null(&self, arr: &str, idx: &str) -> String {
        // Spark `element_at(arr, i)` fails on an out-of-bounds index under
        // ANSI mode (Spark 4's and Databricks' default); `try_element_at` is
        // NULL there, as openCypher's `list[i]`. The index is never 0 (a
        // Cypher index `i >= 0` is `i + 1`, a negative one is as it is).
        format!("try_element_at({}, {})", arr, idx)
    }

    fn count_if(&self) -> &'static str {
        // Databricks Runtime 13.1+ ships count_if. Older runtimes need
        // SUM(CASE WHEN ... THEN 1 ELSE 0 END) — flagged here so a future
        // version-aware mapper can fork.
        "count_if"
    }

    fn any(&self) -> &'static str {
        "any_value"
    }

    fn arrays_overlap(&self) -> &'static str {
        "arrays_overlap"
    }

    fn cast_int64(&self) -> &'static str {
        // Spark function-call cast alias for BIGINT.
        "bigint"
    }

    fn cast_float64(&self) -> &'static str {
        "double"
    }

    fn cast_int64_or_null(&self, expr: &str) -> String {
        // Spark `bigint(...)` already returns NULL on parse failure (non-ANSI
        // cast), so the OrNull form is just the plain cast — byte-identical to
        // `cast_int64`. #880.
        format!("bigint({})", expr)
    }

    fn cast_float64_or_null(&self, expr: &str) -> String {
        format!("double({})", expr)
    }

    fn cast_string(&self) -> &'static str {
        "string"
    }

    fn to_string_float(&self, expr: &str) -> String {
        // Neo4j `toString(3.0)` = "3.0". Spark keeps a double's decimal in its
        // string form (`string(3.0d)` = "3.0"), BUT a whole-valued float LITERAL
        // has already rendered as an integer SQL literal (`3.0` → `3`), so a bare
        // `string(3)` would drop the decimal. Force the double cast first so the
        // value is unambiguously a double before stringifying: `string(double(3))`
        // = "3.0", `string(double(3.5))` = "3.5"; identity on an already-double
        // arg. The caller gates on `RenderType::Float`. (DeltaGraph is beta — the
        // CH path is live-verified; this Spark form is verified by construction,
        // pending live-warehouse confirmation.) #1055.
        format!("string(double({}))", expr)
    }

    fn null_safe_equal(&self, a: &str, b: &str) -> String {
        format!("({a} <=> {b})")
    }

    fn logical_xor(&self, a: &str, b: &str) -> String {
        // Spark has no boolean XOR; of two booleans it is `<>`, which is
        // NULL with a NULL operand.
        format!("(({a}) <> ({b}))")
    }

    fn cast_bool(&self, expr: &str) -> String {
        // Spark BOOLEAN is already nullable and prints true/false, so a plain
        // ANSI cast suffices — no OrNull dance needed. #1057.
        format!("CAST({expr} AS BOOLEAN)")
    }

    fn array_concat(&self) -> &'static str {
        // Spark's `concat` is overloaded for arrays — same call shape as CH's
        // `arrayConcat(a, b)`.
        "concat"
    }

    fn in_list_expression(&self, value: &str, list: &str, negated: bool) -> Option<String> {
        // NULL when the list is, or holds a NULL and not the value, as
        // Cypher's `IN`.
        let not = if negated { "NOT " } else { "" };
        Some(format!("{not}array_contains({list}, {value})"))
    }

    fn array_contains(&self) -> &'static str {
        "array_contains"
    }

    fn integer_division(&self) -> &'static str {
        // Spark `div(a, b)` is integer division truncating toward zero
        // (div(-7, 2) = -3), matching Neo4j and CH `intDiv`.
        "div"
    }

    fn empty_string_array_cast(&self) -> &'static str {
        "CAST(array() AS ARRAY<STRING>)"
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
        // wrapping. `quote_identifier` routes special-char identifiers
        // here too, so backticks stay consistent across both.
        format!("`{}`", name.replace('`', "``"))
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

    fn array_length(&self, arr: &str) -> String {
        // Spark reserves `length` for strings/binary; arrays need `size`.
        format!("size({})", arr)
    }

    fn epoch_millis_to_timestamp(&self, expr: &str) -> String {
        // Spark TIMESTAMP from epoch milliseconds. Verified on Databricks SQL:
        // unix_millis(timestamp_millis(x)) round-trips exactly under UTC.
        format!("timestamp_millis({})", expr)
    }

    fn timestamp_to_epoch_millis(&self, expr: &str) -> String {
        format!("unix_millis({})", expr)
    }

    fn shortest_path_search(&self) -> Option<super::ShortestPathSearch> {
        // Spark gives each step of a recursive CTE the rows of the step
        // before, as ClickHouse does, but takes one recursive term (its
        // steps are one parenthesized UNION ALL), no recursive reference in
        // an aggregate (no early stop, no grouped step), and no recursive
        // CTE read by another's subquery.
        Some(super::ShortestPathSearch {
            depth: "INT",
            flag: "INT",
            // Overflow fails under ANSI mode rather than wrapping.
            count: "DECIMAL(38, 0)",
            copies: |count| format!("array_repeat(0, CAST({count} AS INT))"),
            distinct_list: |x| format!("collect_set({x})"),
            recursive_steps: |steps| format!("({})", steps.join("\n    UNION ALL\n    ")),
            stops_early: false,
            grouped_steps: false,
            // A recursive CTE reading the search (`IN`) is refused there.
            nested_recursion: false,
        })
    }

    fn one_type_guard(&self, columns: &[String]) -> Option<String> {
        // Each definition's values are their own column: the value is their
        // `coalesce` when every column's type is the same up to integer
        // width and string representation (`typeof`, constant over a
        // query), else an error. Spark raises it on a row read: rows of
        // values of different types fail, no rows are no rows.
        let class = |c: &str| {
            // A Spark string literal unescapes `\\(` to `\(`; a collated
            // string is a string.
            format!(
                r"regexp_replace(regexp_replace(typeof({c}), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string')"
            )
        };
        let value = format!("coalesce({})", columns.join(", "));
        let same: Vec<String> = columns[1..]
            .iter()
            .map(|c| format!("{} = {}", class(&columns[0]), class(c)))
            .collect();
        if same.is_empty() {
            return Some(value);
        }
        Some(format!(
            "CASE WHEN {} THEN {value} ELSE raise_error(\
             'ClickGraph: a property has different types on different labels or types') END",
            same.join(" AND ")
        ))
    }

    fn cypher_union(&self) -> Option<super::CypherUnion> {
        Some(super::CypherUnion {
            // A VARIANT holds each value with its own type.
            any_type: |v| format!("CAST({v} AS VARIANT)"),
            // A text of the value's kind and value (Spark groups no VARIANT):
            // `variant_key`.
            distinct_key: |v| variant_key(&format!("CAST({v} AS VARIANT)"), 0),
            order_key,
            any: |v| format!("any_value({v})"),
            row_number: |keys| format!("row_number() OVER (ORDER BY {keys})"),
        })
    }

    fn lists(&self) -> Option<super::Lists> {
        Some(super::Lists {
            // `filter` keeps the elements whose predicate is true.
            filter: |x, p, l| format!("filter({l}, {x} -> {p})"),
            map: |x, e, l| format!("transform({l}, {x} -> {e})"),
            // `collect_list` skips NULLs (the absent rows); the pairs are
            // sorted by their key alone: a value need not be orderable (a
            // map is not).
            ordered_collect: |v, k, present| {
                format!(
                    "transform(array_sort(collect_list(CASE WHEN {present} THEN \
                     named_struct('k', {k}, 'v', {v}) END), (__cg_l, __cg_r) -> \
                     CASE WHEN __cg_l.k < __cg_r.k THEN -1 WHEN __cg_l.k > __cg_r.k THEN 1 \
                     ELSE 0 END), __cg_t -> __cg_t.v)"
                )
            },
            collect_if: |v, present| format!("collect_list(CASE WHEN {present} THEN {v} END)"),
            // A struct of one NULL is not NULL.
            collect_all: |v| {
                format!("transform(collect_list(named_struct('v', {v})), __cg_t -> __cg_t.v)")
            },
            // By the values' keys (`order_key`): a value need not be
            // orderable (a map is not).
            fixed_order: |l| {
                let (l_key, r_key) = (order_key("__cg_sl"), order_key("__cg_sr"));
                format!(
                    "array_sort({l}, (__cg_sl, __cg_sr) -> CASE WHEN {l_key} < {r_key} THEN -1 \
                     WHEN {l_key} > {r_key} THEN 1 ELSE 0 END)"
                )
            },
            or_empty: Some(|l| format!("coalesce({l}, array())")),
            // `struct(a.x, …)` names its fields after their columns: the
            // fields are named by position to be read by it.
            tuple: |fields| {
                let fields: Vec<String> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| format!("'_{}', {f}", i + 1))
                    .collect();
                format!("named_struct({})", fields.join(", "))
            },
            element: |t, i| format!("({t})._{i}"),
            is_null: |v| format!("isnull({v})"),
            // An aggregate has one row also over none.
            empty_of: |t, table, alias| {
                format!("(SELECT collect_list({t}) FROM {table} AS {alias} WHERE false)")
            },
            // `array_distinct` keeps each element where it first occurs.
            distinct: |l| format!("array_distinct({l})"),
            contains: |l, x| format!("array_contains({l}, {x})"),
            intersect: |a, b| format!("array_intersect({a}, {b})"),
            sorted: |l| format!("array_sort({l})"),
            null_list: |c, l| format!("CASE WHEN {c} THEN NULL ELSE {l} END"),
            null_as_empty: false,
            aggregate_if: |f, v, c| format!("{f}(CASE WHEN {c} THEN {v} END)"),
            fingerprint: |v| format!("xxhash64({v})"),
            // A VARIANT object's keys, sorted (its order).
            map_keys: |m| format!("map_keys(CAST({m} AS MAP<STRING, VARIANT>))"),
            // A VARIANT object: each value keeps its type, maps of any keys
            // are of one type, and a map is a value as any other (a value of
            // a map, a UNION's column). Spark groups, compares and sorts no
            // VARIANT: a map is grouped and made distinct by its key
            // (`CypherUnion::distinct_key`). An empty list's elements are of
            // no type, which no VARIANT holds.
            map_of: |entries| {
                if entries.is_empty() {
                    return Some(
                        "to_variant_object(CAST(map() AS MAP<STRING, VARIANT>))".to_string(),
                    );
                }
                if entries.iter().any(|e| e.holds_empty_list) {
                    return None;
                }
                let pairs: Vec<String> = entries
                    .iter()
                    .map(|e| {
                        let k = e.key.replace('\\', "\\\\").replace('\'', "\\'");
                        format!("'{k}', CAST({} AS VARIANT)", e.value)
                    })
                    .collect();
                Some(format!("to_variant_object(map({}))", pairs.join(", ")))
            },
        })
    }

    fn unwind(&self) -> Option<super::Unwind> {
        Some(super::Unwind {
            one_row: "(SELECT 1)",
            // `explode` of a map, or `arrays_zip` of one, fails the query.
            list_only: |list| list.to_string(),
            row_number: |keys| format!("row_number() OVER (ORDER BY {keys})"),
            // Spark numbers rows of an ordered window only.
            any_row_number: "row_number() OVER (ORDER BY 1)",
            is_null: |v| format!("isnull({v})"),
            value_list: |v| format!("CASE WHEN isnull({v}) THEN array() ELSE array({v}) END"),
        })
    }

    fn array_join_clause(&self, lists: &[(String, String, Option<String>)]) -> String {
        // Spark has no ARRAY JOIN: `explode` repeats a row per element (none
        // for an empty or NULL list), `posexplode` with its position (from
        // 0), each reading the list once (a list read twice could differ:
        // `rand()`). Lists read in step are zipped first; their positions
        // are counted over the zipped list.
        match lists {
            [(list, alias, None)] => format!("LATERAL VIEW explode({list}) AS {alias}\n"),
            [(list, alias, Some(p))] => {
                format!("LATERAL VIEW posexplode({list}) AS {p}, {alias}\n")
            }
            _ => {
                let l: Vec<&str> = lists.iter().map(|(l, _, _)| l.as_str()).collect();
                let a: Vec<&str> = lists.iter().map(|(_, a, _)| a.as_str()).collect();
                let zipped = format!("arrays_zip({})", l.join(", "));
                match lists.iter().find_map(|(_, _, p)| p.as_deref()) {
                    None => format!("LATERAL VIEW inline({zipped}) AS {}\n", a.join(", ")),
                    Some(p) => format!(
                        "LATERAL VIEW posexplode({zipped}) AS {p}, __cg_zipped\n\
                         LATERAL VIEW inline(array(__cg_zipped)) AS {}\n",
                        a.join(", ")
                    ),
                }
            }
        }
    }

    fn list_size(&self, list: &str) -> String {
        // `length` is of a string only; `size` of NULL is -1 unless ANSI.
        format!("array_size({list})")
    }

    fn any_non_null(&self, value: &str) -> String {
        format!("any_value({value}, true)")
    }

    fn one_value(&self, value: &str, message: &str) -> Option<String> {
        // `raise_error` is evaluated on the groups of several values only;
        // its NULL takes the value's type. DISTINCT's keys (`variant_key`: a
        // VARIANT has no DISTINCT of its own) of the non-NULL values differ
        // when their least and greatest do: `min` / `max` (a `count(DISTINCT)`
        // per property expands the rows once each, review).
        let key = (self.cypher_union()?.distinct_key)(value);
        let key = format!("CASE WHEN {value} IS NOT NULL THEN {key} END");
        let message = message.replace('\\', "\\\\").replace('\'', "\\'");
        Some(format!(
            "CASE WHEN min({key}) <> max({key}) THEN raise_error('{message}') ELSE {} END",
            self.any_non_null(value)
        ))
    }

    fn graph_values(&self) -> Option<super::GraphValues> {
        fn struct_text(tuple: &str, arity: usize) -> String {
            let fields: Vec<String> = (1..=arity).map(|i| format!("_{i}: STRING")).collect();
            format!(
                "to_json(CAST({tuple} AS STRUCT<{}>), map('ignoreNullFields', 'false'))",
                fields.join(", ")
            )
        }
        // An element is a VARIANT object: objects of different keys and
        // value types share the type, so one `ARRAY<VARIANT>` holds nodes
        // and relationships of any label, as a path does. A value that is
        // an object or a list is already a VARIANT, or casts to one.
        fn map(entries: &[(String, String)]) -> String {
            if entries.is_empty() {
                return "CAST(map() AS MAP<STRING, VARIANT>)".to_string();
            }
            let pairs: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{k}, CAST({v} AS VARIANT)"))
                .collect();
            format!("map({})", pairs.join(", "))
        }
        Some(super::GraphValues {
            object: |entries| format!("to_variant_object({})", map(entries)),
            // A VARIANT cast of NULL is SQL NULL.
            object_without_nulls: |entries| {
                format!(
                    "to_variant_object(map_filter({}, (__k, __v) -> __v IS NOT NULL))",
                    map(entries)
                )
            },
            list: |items| {
                if items.is_empty() {
                    "CAST(array() AS ARRAY<VARIANT>)".to_string()
                } else {
                    format!("array({})", items.join(", "))
                }
            },
            concat: |lists| match lists {
                [one] => one.clone(),
                _ => format!("concat({})", lists.join(", ")),
            },
            tail: |list| format!("filter({list}, (__x, __i) -> __i > 0)"),
            reverse: |list| format!("reverse({list})"),
            // The lists are of one length.
            interleave: |rels, nodes| {
                format!(
                    "flatten(transform({rels}, (__r, __i) -> \
                     array(__r, element_at({nodes}, __i + 1))))"
                )
            },
            // `size(NULL)` is -1 unless ANSI mode.
            length: |list| format!("array_size({list})"),
            text: |parts| format!("concat({})", parts.join(", ")),
            to_text: |x| format!("CAST({x} AS STRING)"),
            // `CAST(struct AS STRING)` quotes no part: each part's text in a
            // JSON object of positional fields, its NULLs kept, as
            // `tuple_value_text` spells a struct of the parts.
            tuple_text: |parts| {
                let fields: Vec<String> = parts
                    .iter()
                    .enumerate()
                    .map(|(i, p)| format!("'_{}', CAST({p} AS STRING)", i + 1))
                    .collect();
                format!(
                    "to_json(named_struct({}), map('ignoreNullFields', 'false'))",
                    fields.join(", ")
                )
            },
            // A struct cast to one of strings is cast field by field, by
            // position.
            tuple_value_text: struct_text,
            null_if: |cond, value| {
                format!("CASE WHEN {cond} THEN NULL ELSE CAST({value} AS VARIANT) END")
            },
            // `any_value` skips NULLs only when asked, as ClickHouse's `any`.
            any: |value| format!("any_value({value}, true)"),
            texts: |items| {
                if items.is_empty() {
                    "CAST(array() AS ARRAY<STRING>)".to_string()
                } else {
                    format!("array({})", items.join(", "))
                }
            },
            prefixed_texts: |prefix, list, arity| {
                let text = match arity {
                    1 => "CAST(__x AS STRING)".to_string(),
                    n => struct_text("__x", n),
                };
                format!("transform({list}, __x -> concat({prefix}, {text}))")
            },
            empty_if: |cond, list| format!("CASE WHEN {cond} THEN array() ELSE {list} END"),
        })
    }

    fn string_literal_quote_escape(&self) -> &'static str {
        "\\'"
    }

    fn order_by_nulls_clause(&self, descending: bool) -> &'static str {
        // Spark/Databricks defaults to NULLS FIRST for ASC and NULLS LAST for
        // DESC (ANSI). Neo4j is the opposite on BOTH: nulls-last on ASC,
        // nulls-first on DESC — so each direction needs an explicit override
        // (neither Spark default matches). #1065.
        if descending {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        }
    }

    fn percentile_aggregate(&self, expr: &str, percentile: &str, continuous: bool) -> String {
        if continuous {
            // Spark `percentile(expr, p)` does linear interpolation, matching
            // Neo4j percentileCont. It's a documented plain-function aggregate
            // (comma-arg form), unlike `percentile_cont`/`percentile_disc`
            // which are `WITHIN GROUP (ORDER BY …)` ordered-set aggregates.
            format!("percentile({expr}, {percentile})")
        } else {
            // percentileDisc = nearest actual value at Neo4j's 1-based index
            // `greatest(1, ceil(p * n))`. Spark's `percentile_disc` uses the
            // `WITHIN GROUP` ordered-set syntax (not a comma-arg function) AND a
            // different index convention, so mirror the ClickHouse hand-built
            // exact-index form over the sorted value array for identical
            // cross-dialect semantics (#639). `try_element_at` (1-based) returns
            // NULL on an empty group instead of throwing under ANSI mode —
            // matching ClickHouse `arrayElementOrNull` and Neo4j (null over an
            // empty set). `array_sort` is ascending; `collect_list` drops NULLs.
            format!(
                "try_element_at(array_sort(collect_list({expr})), greatest(1, cast(ceil({percentile} * count({expr})) as int)))"
            )
        }
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
        // element_at fails out of bounds under ANSI mode (Spark 4's and
        // Databricks' default): the NULL-returning accessor is try_element_at.
        assert_eq!(
            m.array_element_or_null("array(10, 20, 30)", "11"),
            "try_element_at(array(10, 20, 30), 11)"
        );
        assert_eq!(m.count_if(), "count_if");
        assert_eq!(m.cast_int64(), "bigint");
        assert_eq!(m.cast_float64(), "double");
        assert_eq!(m.cast_string(), "string");
        assert_eq!(m.cast_bool("x > 2"), "CAST(x > 2 AS BOOLEAN)");
        // #1065: Spark defaults NULLS FIRST/ASC + NULLS LAST/DESC (ANSI) —
        // Neo4j is the opposite on both, so each direction is explicit.
        assert_eq!(m.order_by_nulls_clause(false), " NULLS LAST");
        assert_eq!(m.order_by_nulls_clause(true), " NULLS FIRST");
        assert_eq!(m.array_concat(), "concat");
        assert_eq!(m.array_contains(), "array_contains");
        assert_eq!(m.integer_division(), "div");
        // Exponentiation uses the shared ANSI `POWER(base, exp)` default —
        // Spark/Databricks has no infix `^` either.
        assert_eq!(m.power("2", "3"), "POWER(2, 3)");
        assert_eq!(m.epoch_millis_to_timestamp("x"), "timestamp_millis(x)");
        assert_eq!(m.timestamp_to_epoch_millis("x"), "unix_millis(x)");
        assert_eq!(
            m.empty_string_array_cast(),
            "CAST(array() AS ARRAY<STRING>)"
        );
        assert_eq!(m.array_literal(""), "array()");
        assert_eq!(m.array_literal("a, b"), "array(a, b)");
        assert_eq!(m.tuple_constructor(), "struct");
        assert_eq!(m.quote_alias("b.id"), "`b.id`");
        // Embedded backticks must be doubled, not left raw — otherwise
        // an alias like `` x`y `` would prematurely close the quote.
        assert_eq!(m.quote_alias("x`y"), "`x``y`");
        // ANSI CAST syntax with unquoted type keyword.
        // slice requires a length; the 2-arg CH form computes one.
        assert_eq!(m.array_slice("arr", "2", Some("3")), "slice(arr, 2, 3)");
        assert_eq!(
            m.array_slice("arr", "2", None),
            "slice(arr, 2, greatest(size(arr) - (2) + 1, 0))"
        );
        // Spark arrays need `size` (its `length` is string/binary-only), unlike
        // ClickHouse's overloaded `length`; used to normalize negative slice bounds.
        assert_eq!(m.array_length("arr"), "size(arr)");
        // #639: Spark percentileCont = `percentile(expr, p)` (interpolated).
        // percentileDisc has no matching comma-arg builtin (Spark's
        // `percentile_disc` uses WITHIN GROUP + a different index), so it's the
        // hand-built sorted-array index form, mirroring ClickHouse.
        assert_eq!(
            m.percentile_aggregate("t.x", "0.9", true),
            "percentile(t.x, 0.9)"
        );
        assert_eq!(
            m.percentile_aggregate("t.x", "0.9", false),
            "try_element_at(array_sort(collect_list(t.x)), greatest(1, cast(ceil(0.9 * count(t.x)) as int)))"
        );
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
