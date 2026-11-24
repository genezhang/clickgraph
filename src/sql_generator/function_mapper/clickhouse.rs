//! ClickHouse `FunctionMapper` — the canonical names used by the existing
//! `clickhouse_query_generator` SQL emission path.

use super::FunctionMapper;

/// A `Dynamic` value's container type as the scalar branch of
/// [`distinct_key`] classes its values (#1482): numbers of any width are one
/// class, the string-like types another, `Date32` is a `Date`, and a
/// `Nullable` / `LowCardinality` wrapper does not count. Equal values of
/// `Array(Int32)` and `Array(Int64)`, `Array(String)` and
/// `Array(LowCardinality(String))`, or `Map(String, Int32)` and
/// `Map(String, Int64)` therefore stay one group (a `UNION` of two sources
/// with different storage types), while a `Date` and a `String` stay two.
///
/// A wrapper is removed with its parenthesis (stripping only the prefix
/// leaves a stray `)` and misses `Array(Nullable(Int32))` against
/// `Array(UInt64)`); it runs twice for `LowCardinality(Nullable(T))`.
fn container_type(ty: &str) -> String {
    let replace = |t: String, pattern: &str, with: &str| {
        format!("replaceRegexpAll({t}, '{pattern}', '{with}')")
    };
    let wrapper = r"(Nullable|LowCardinality)\\(([A-Za-z0-9]+(?:\\([^()]*\\))?)\\)";
    let t = replace(
        ty.to_string(),
        r"Decimal[0-9]*\\([0-9, ]*\\)|U?Int[0-9]+|BFloat16|Float[0-9]+",
        "Number",
    );
    let t = replace(
        t,
        r"FixedString\\([0-9]+\\)|Enum(8|16)?\\([^)]*\\)|UUID|IPv[46]",
        "String",
    );
    let t = replace(t, "Date32", "Date");
    let t = replace(t, wrapper, r"\\2");
    replace(t, wrapper, r"\\2")
}

/// How many levels below a container the distinct key looks into (#1482):
/// eight maps in maps, say, each keyed by what is in it. A container deeper is
/// keyed by its type and its text, as a typed container is. Only a container
/// that needs it ([`keyed_by_elements`]) pays for the levels.
const KEY_LEVELS: usize = 8;

/// The ClickHouse `CypherUnion::distinct_key`: the value's kind and text.
///
/// A container whose type hides what its elements are, or prints them by
/// storage type ([`keyed_by_elements`]), is keyed by its elements
/// ([`deep_key`]); every other value is [`plain_key`], which looks at no
/// element. The gate is outside every lambda: ClickHouse evaluates a branch
/// of `multiIf` only for the rows that take it there, and not inside a
/// lambda, so a scalar (or a long typed list) costs what it did.
fn distinct_key(v: &str) -> String {
    format!(
        "multiIf({}, {}, {})",
        keyed_by_elements(&value_type_of(v)),
        deep_key(v),
        plain_key(v)
    )
}

/// `v`'s type, a `LowCardinality` one as its inner type (`Float64`
/// 1.5 and `LowCardinality(Float64)` 1.5 are one value).
fn value_type_of(v: &str) -> String {
    format!(
        "replaceRegexpOne(dynamicType(CAST({v}, 'Dynamic')), \
         '^LowCardinality\\\\((.*)\\\\)$', '\\\\1')"
    )
}

/// Whether the container of type `ty` is keyed by its elements: its type
/// hides them (`Dynamic`, `Variant`) or prints them by storage type
/// (`DateTime`, `Time`), and it is a map or a list (a list of lists is its
/// type and its text: a `Dynamic` is cast to `Array(Dynamic)`, which
/// ClickHouse does of an array of one dimension only).
fn keyed_by_elements(ty: &str) -> String {
    // (`startsWith` and `multiSearchAny`, not a regex: this is read of every
    // value. A type name follows `(` or a space: an `Enum` label such as
    // `'Timeout'` is not one.)
    format!(
        "((startsWith({ty}, 'Map') OR (startsWith({ty}, 'Array') \
         AND NOT startsWith({ty}, 'Array(Array('))) \
         AND multiSearchAny({ty}, ['(Dynamic', ' Dynamic', '(Variant', ' Variant', \
         '(DateTime', ' DateTime', '(Time', ' Time']))"
    )
}

/// The kind of value `v`, as [`plain_key`] has it. `values`: a map's kind has
/// its values' types too (a map of `Dynamic` values, which [`deep_key`] reads
/// for itself, has none to give).
fn plain_kind(v: &str, values: bool) -> String {
    let ty = value_type_of(v);
    let container = container_type(&ty);
    // `Int[0-9]`: not an `Interval…`.
    let number = format!("match({ty}, '^(U?Int[0-9]|Float|BFloat|Decimal)')");
    let string = format!("match({ty}, '^(String|FixedString|Enum|UUID|IPv|LowCardinality)')");
    let zone = format!("extract({ty}, '''([^'']*)''')");
    // `mapValues` of a raw `Dynamic` map is `[]` for a typed map and
    // Code 43 for anything that is not one, and `multiIf` analyses
    // every branch: the Dynamic value goes through `dynamicElement`
    // (safe on any kind) and the map's own type joins the key, which
    // separates typed maps whose value list is empty.
    // The container's type and each value's are classed by
    // [`container_type`]: the same value of two storage types is one key.
    let map = match values {
        true => {
            let value_type = container_type("dynamicType(CAST(x, 'Dynamic'))");
            format!(
                "concat('m:', {container}, ':', arrayStringConcat(arrayMap(x -> \
                 {value_type}, CAST(mapValues(dynamicElement(\
                 CAST({v}, 'Dynamic'), 'Map(String, Dynamic)')), 'Array(Dynamic)')), ','))"
            )
        }
        false => format!("concat('m:', {container}, ':')"),
    };
    format!(
        "multiIf({ty} = 'None', '', {ty} = 'Bool', 'b', {number}, 'n', \
         startsWith({ty}, 'Map'), {map}, \
         match({ty}, '^(Array|Tuple)'), concat('c:', {container}), {string}, 's', \
         {ty} = 'Date32', 'Date', startsWith({ty}, 'DateTime'), \
         concat('DateTime ', if({zone} = '', timezone(), {zone})), \
         startsWith({ty}, 'Time'), 'Time', {ty})"
    )
}

/// The text of value `v`, as [`plain_key`] has it.
fn plain_text(v: &str) -> String {
    let ty = value_type_of(v);
    let number = format!("match({ty}, '^(U?Int[0-9]|Float|BFloat|Decimal)')");
    let clock = format!(
        "replaceRegexpOne(replaceRegexpOne(toString({v}), \
         '(\\\\.[0-9]*[1-9])0+$', '\\\\1'), '\\\\.0+$', '')"
    );
    // NULL of a value of another type, not an error: only a
    // DateTime is cast (`accurateCastOrNull` of a NaN or an
    // infinite Float throws, and a `multiIf` branch is computed
    // for every row).
    let instant = format!(
        "ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(\
         if(startsWith({ty}, 'DateTime'), CAST({v}, 'Dynamic'), NULL), \
         'DateTime64(6, ''UTC'')'))), '')"
    );
    // `toString` drops a FixedString's trailing zero bytes,
    // which the output prints: padded back to its width.
    format!(
        "multiIf({number} AND toString({v}) = '-0', '0', \
         startsWith({ty}, 'FixedString'), \
         rightPad(toString({v}), toUInt64OrZero(extract({ty}, '[0-9]+')), '\\0'), \
         startsWith({ty}, 'DateTime'), concat({clock}, ' ', {instant}), \
         startsWith({ty}, 'Time'), {clock}, \
         toString({v}))"
    )
}

/// The key of a value that looks at no element of a container: its kind and
/// text, a container being its classed type and its text. (A container whose
/// elements are looked at is [`deep_key`]'s.)
fn plain_key(v: &str) -> String {
    format!("tuple({}, {})", plain_kind(v, true), plain_text(v))
}

/// The elements of container `n` that is [`keyed_by_elements`], as an
/// `Array(Dynamic)`: a map's keys and values in turn, a list's elements; none
/// for any other value. (A `Dynamic` is cast to a container only where it is
/// one: a cast of a scalar is an error, and an `if` is evaluated for every
/// row. The other rows hold a one-element container, which is never read, and
/// the `identity` keeps ClickHouse from moving the cast into the two arms of
/// the `if`, which would cast the scalars too.)
fn elements_of(n: &str) -> String {
    // The node's type and whether it is looked into are computed once, as the
    // parameters of a lambda over one-element arrays.
    let ty = value_type_of(n);
    let deep = keyed_by_elements(&ty);
    // A map as its `(key, value)` pairs.
    let pairs = format!(
        "CAST(identity(if(d AND startsWith(t, 'Map'), CAST({n}, 'Dynamic'), \
         CAST(map('', 0), 'Dynamic'))), 'Array(Tuple(String, Dynamic))')"
    );
    let list = format!(
        "CAST(identity(if(d AND startsWith(t, 'Array'), CAST({n}, 'Dynamic'), \
         CAST([0], 'Dynamic'))), 'Array(Dynamic)')"
    );
    format!(
        "arrayMap((t, d) -> multiIf(d AND startsWith(t, 'Map'), arrayFlatten(arrayMap(p -> \
         [CAST(p.1, 'Dynamic'), p.2], {pairs})), \
         d AND startsWith(t, 'Array'), {list}, \
         arrayPopBack([CAST(0, 'Dynamic')])), [{ty}], [{deep}])[1]"
    )
}

/// The key of a container that is [`keyed_by_elements`] (#1482): a date in a
/// map in a list is a date, not the string of its text, and the same instant
/// is one value whatever it is stored in.
///
/// The container's tree, level by level: each node as its kind, its text and
/// how many elements it has (a container that is itself keyed by its
/// elements has neither kind nor text of its own: they are its elements').
/// The elements of one level are the next level's nodes, so the lambda is the
/// same for every level, and the key is the same size however deep it looks
/// ([`KEY_LEVELS`]); a level order with the number of elements of each node
/// is one tree only. A node on the last level is [`plain_key`], whatever it
/// holds.
fn deep_key(v: &str) -> String {
    let n = "n";
    let ty = value_type_of(n);
    let deep = keyed_by_elements(&ty);
    // A node's token: its kind, its text and its number of elements `kids`.
    // (Not bound to a lambda's parameters as `elements_of`'s are: a lambda
    // over one-element arrays per node costs more per row than the repeated
    // type costs ClickHouse to plan.)
    let token = format!(
        "toString(tuple(if({deep} AND i < {KEY_LEVELS}, if(startsWith({ty}, 'Map'), 'm', 'c'), \
         {kind}), if({deep} AND i < {KEY_LEVELS}, '', {text}), length(kids)))",
        kind = plain_kind(n, false),
        text = plain_text(n),
    );
    // The last level's nodes are not opened (`i` is the last level's index).
    let levels = format!(
        "arrayFold((acc, i) -> arrayMap(ch -> (arrayFlatten(ch), \
         arrayConcat(acc.2, arrayMap((n, kids) -> {token}, acc.1, ch))), \
         [arrayMap(n -> if(i < {KEY_LEVELS}, {elements}, arrayPopBack([CAST(0, 'Dynamic')])), \
         acc.1)])[1], range({KEY_LEVELS} + 1), \
         ([CAST({v}, 'Dynamic')], CAST([], 'Array(String)')))",
        elements = elements_of(n)
    );
    format!(
        "arrayMap(f -> tuple(if(startsWith({root}, 'Map'), 'm', 'c'), toString(f.2)), \
         [{levels}])[1]",
        root = value_type_of(v)
    )
}

/// #1491: `ORDER BY` of a list whose elements are not one comparable type.
/// Neo4j compares lists element by element, each by its type's rank and then
/// by its value in that type; ClickHouse refuses such a list in `ORDER BY`
/// (Code 44/386). `Array(Dynamic)` lets one lambda see each element's own
/// type, and an array of `(rank, value, text)` tuples compares
/// lexicographically, so `[3]` sorts before `[12]` — a text key puts `[12]`
/// first.
fn mixed_list_order_key(list: &str) -> String {
    format!(
        "arrayMap(x -> {}, CAST({list}, 'Array(Dynamic)'))",
        order_element_key("x")
    )
}

/// One element's orderable key: Neo4j's type rank (numbers greatest, NULL
/// last), then the value in its own type (a number numerically, a
/// string/date lexicographically), then the text as a tie-break.
///
/// A list, map or tuple inside the list is ordered by its text (a map has no
/// order of its own: `ORDER BY` of a map is refused), after its type rank.
///
/// A number's value is two components: its `Float64` (which orders values
/// of any magnitude), then its exact `Decimal256` (which breaks a float tie
/// without dropping digits: `0.1234567891239` and `0.1234567891231` are
/// different doubles). Values beyond Decimal256's range leave the second 0
/// and fall back to the text, which orders digits of one length.
fn order_element_key(v: &str) -> String {
    let ty = format!(
        "replaceRegexpOne(dynamicType(CAST({v}, 'Dynamic')), \
         '^LowCardinality\\\\((.*)\\\\)$', '\\\\1')"
    );
    let number = format!("match({ty}, '^(U?Int[0-9]|Float|BFloat|Decimal)')");
    let string = format!("match({ty}, '^(String|FixedString|Enum|UUID|IPv)')");
    let zone = format!("extract({ty}, '''([^'']*)''')");
    format!(
        "tuple(multiIf({ty} = 'None', 255, startsWith({ty}, 'Map'), 1, \
         startsWith({ty}, 'Array') OR startsWith({ty}, 'Tuple'), 4, \
         startsWith({ty}, 'DateTime'), if({zone} = '', 8, 7), \
         {ty} = 'Date32' OR {ty} = 'Date', 9, \
         startsWith({ty}, 'Time'), if({zone} = '', 11, 10), \
         {string}, 13, {ty} = 'Bool', 14, {number}, 15, 16), \
         multiIf({number}, ifNull(toFloat64OrNull(toString({v})), 0), \
         {ty} = 'Bool', toUInt8(toString({v}) = 'true'), 0), \
         multiIf({number}, ifNull(toDecimal256OrNull(toString({v}), 18), 0), 0), \
         if({ty} = 'Bool' OR {ty} = 'None', '', toString({v})))"
    )
}

pub(crate) struct ClickhouseFunctionMapper;

impl FunctionMapper for ClickhouseFunctionMapper {
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

    fn any_non_null(&self, value: &str, is_complex: bool) -> String {
        if is_complex {
            // Array/Map/Tuple/Dynamic cannot be inside Nullable on 25.8;
            // `anyIf` returns the type's default for an all-NULL group.
            format!("anyIf({value}, NOT isNull({value}))")
        } else {
            // `any` skips the NULLs of a `Nullable` column but not a
            // `Variant`'s NULL (#1426). `-OrNull`: NULL for all-NULL.
            format!("anyIfOrNull({value}, NOT isNull({value}))")
        }
    }

    fn one_value(&self, value: &str, message: &str) -> Option<String> {
        // `uniqUpTo(1)` counts up to two distinct keys in constant memory;
        // of the hashes of DISTINCT's keys (`-0.0` is `0.0`, a FixedString
        // its padded text, integers of any width one number). `throwIf` is
        // 0 unless it throws: the element of a one-element array keeps the
        // value's type.
        let key = (self.cypher_union()?.distinct_key)(value);
        let message = message.replace('\\', "\\\\").replace('\'', "\\'");
        Some(format!(
            "arrayElement([{}], 1 + throwIf(uniqUpToIf(1)(cityHash64({key}), NOT isNull({value})) > 1, '{message}'))",
            self.any_non_null(value, false)
        ))
    }

    fn arrays_overlap(&self) -> &'static str {
        "hasAny"
    }

    fn cast_int64(&self) -> &'static str {
        "toInt64"
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

    fn day_or_time_text(&self, expr: &str) -> String {
        // `toStartOfDay` / `toDate` wrap outside 1970-2106 (review finding).
        format!("replaceRegexpOne(toString({expr}), ' 00:00:00(\\\\.0+)?$', '')")
    }

    fn replace_text(&self, expr: &str, search: &str, replacement: &str) -> String {
        format!("replaceAll({expr}, {search}, {replacement})")
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
        //
        // `expr` is textually repeated 3× (match + concat + else). This is safe
        // for a nondeterministic arg (e.g. `toString(2 ^ rand())`) ONLY because
        // ClickHouse applies common-subexpression elimination to identical
        // nondeterministic subexpressions (`randCanonical() = randCanonical()` →
        // 1) under both the old and new analyzer — so all three copies observe
        // the SAME draw. Were that to change, the `match` could test one draw
        // while `concat` appends `.0` to a different, fractional draw (malformed
        // `X.5.0`). If a future CH drops that CSE, wrap the arg in a CTE/subquery
        // column and reference it here instead of re-emitting the fragment.
        format!(
            "if(match(toString({e}), '^-?[0-9]+$'), concat(toString({e}), '.0'), toString({e}))",
            e = expr
        )
    }

    fn null_safe_equal(&self, a: &str, b: &str) -> String {
        format!("isNotDistinctFrom({a}, {b})")
    }

    fn to_string_datetime(&self, expr: &str) -> String {
        // #1068: swap only the separator, keeping the fraction and the
        // zone-less (localdatetime) form ClickHouse's toString spells.
        format!("replace(toString({expr}), ' ', 'T')")
    }

    fn logical_xor(&self, a: &str, b: &str) -> String {
        // `xor` is three-valued: NULL with a NULL operand.
        format!("xor({a}, {b})")
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

    fn array_literal(&self, elems: &str) -> String {
        format!("[{elems}]")
    }

    fn mixed_array_literal(&self, elems: &str) -> Option<String> {
        // `[1, 'a']` of literals is one constant array with no type (Code
        // 386); `array(1, 'a')` is a call, which makes a `Variant` (#1491).
        Some(format!("array({elems})"))
    }

    fn tuple_constructor(&self) -> &'static str {
        "tuple"
    }

    fn final_modifier(&self) -> &'static str {
        " FINAL"
    }

    fn all_columns_settings(&self) -> &'static str {
        " SETTINGS asterisk_include_alias_columns = 1, asterisk_include_materialized_columns = 1"
    }

    fn quote_alias(&self, name: &str) -> String {
        // CH escapes `"` inside a double-quoted identifier by doubling it.
        // Aliases inferred from raw return text can contain quotes
        // (e.g., `RETURN 'a"b'` derives an alias from the literal),
        // and naive wrapping would produce malformed SQL. A backslash
        // escapes the next character there too, so it is doubled: bare,
        // `"a\"` would not end the identifier (#1489: `` AS `a\` ``).
        format!("\"{}\"", name.replace('\\', "\\\\").replace('"', "\"\""))
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

    fn shortest_path_search(&self) -> Option<super::ShortestPathSearch> {
        Some(super::ShortestPathSearch {
            depth: "UInt32",
            flag: "UInt8",
            count: "UInt256",
            copies: |count| format!("range(accurateCast({count}, 'UInt64'))"),
            distinct_list: |x| format!("groupUniqArray({x})"),
            recursive_steps: |steps| steps.join("\n    UNION ALL\n    "),
            stops_early: true,
            grouped_steps: true,
            nested_recursion: true,
        })
    }

    fn one_type_guard(&self, columns: &[String]) -> Option<String> {
        // A union's column takes the arms' common type, converting values
        // (`Bool` and `UInt8`, `Float32` to `Float64`, `Date` to
        // `DateTime`), or with none a `Variant` (`use_variant_as_common_type`,
        // on by default), whose NULLs `count` counts and whose `8` and `8.0`
        // DISTINCT keeps apart. Each definition's values are their own
        // column: the value is their `coalesce`, cast to its own type when
        // every column's type is the same up to integer width, string
        // representation and nullability (which change no value), else to a
        // type name that does not exist: an error while the query is
        // analysed, rows or none.
        let class = |c: &str| {
            format!(
                "replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName({c}), \
                 'Nullable\\\\(|LowCardinality\\\\(|\\\\)', ''), '^U?Int\\\\d+$', 'Int'), \
                 '^(String|FixedString\\\\(\\\\d+)$', 'String')"
            )
        };
        let value = format!("coalesce({})", columns.join(", "));
        let mut same: Vec<String> = columns[1..]
            .iter()
            .map(|c| format!("{} = {}", class(&columns[0]), class(c)))
            .collect();
        // Keep the Variant check for mixes such as Int64 vs UInt64, but skip it
        // when every column is itself a complex (Array/Map/Tuple/Dynamic) type,
        // whose coalesce is also a Variant but reads correctly. (#1554)
        let complex_pred = columns
            .iter()
            .map(|c| {
                format!(
                    "(toTypeName({c}) LIKE 'Array(%' OR toTypeName({c}) LIKE 'Map(%' \
                     OR toTypeName({c}) LIKE 'Tuple(%' OR toTypeName({c}) LIKE 'Dynamic(%' \
                     OR toTypeName({c}) LIKE 'Variant(%')"
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        same.push(format!(
            "(toTypeName({value}) NOT LIKE 'Variant(%' OR ({complex_pred}))"
        ));
        Some(format!(
            "CAST({value}, if({}, toTypeName({value}), \
             'ClickGraph_property_has_different_types_on_different_labels_or_types'))",
            same.join(" AND ")
        ))
    }
    fn unwind(&self) -> Option<super::Unwind> {
        Some(super::Unwind {
            one_row: "system.one",
            // An identity of arrays that refuses a map, which `ARRAY JOIN`
            // reads as a list of its entries.
            list_only: |list| format!("arrayConcat({list})"),
            row_number: |keys| format!("row_number() OVER (ORDER BY {keys})"),
            any_row_number: "row_number() OVER ()",
            is_null: |v| format!("isNull({v})"),
            value_list: |v| format!("if(isNull({v}), [], [{v}])"),
        })
    }

    fn cypher_union(&self) -> Option<super::CypherUnion> {
        Some(super::CypherUnion {
            // A `Dynamic` column holds each value with its own type; the
            // common type of the arms' `Dynamic` columns is `Dynamic`.
            any_type: |v| format!("CAST({v}, 'Dynamic')"),
            // The value's kind and text: values of different kinds are
            // different (`1`, `'1'`, `true`, a date and its text), numbers
            // equal by value (`1` = `1.0`, an `Int64` = a `UInt8`, `-0.0` =
            // `0`), and inside a list a string is quoted. A value the output
            // shows as a string (FixedString, Enum, UUID) is of the string
            // kind; another is of its type's (#1432): a `Date32` is a
            // `Date`, a `DateTime64` a `DateTime` of its zone (the session's
            // when the type names none), its fraction without trailing
            // zeros, then its instant in microseconds (two instants of a DST
            // fall-back hour print alike; the text has the rest). Not JSON:
            // `toJSONString` quotes a 64-bit integer as a string under
            // `output_format_json_quote_64bit_integers`. NULL is one group.
            distinct_key,
            scalar_key: plain_key,
            // Sortable as it is (a tuple of strings), and equal only for
            // values DISTINCT takes as one.
            order_key: distinct_key,
            any: |v| format!("any({v})"),
            row_number: |keys| format!("row_number() OVER (ORDER BY {keys})"),
        })
    }

    fn lists(&self) -> Option<super::Lists> {
        Some(super::Lists {
            // `arrayFilter` keeps the elements whose predicate is not 0 and
            // not NULL.
            filter: |x, p, l| format!("arrayFilter({x} -> {p}, {l})"),
            map: |x, e, l| format!("arrayMap({x} -> {e}, {l})"),
            ordered_collect: |v, k, present| {
                format!(
                    "arrayMap(t -> t.2, arraySort(t -> t.1, groupArrayIf(({k}, {v}), {present})))"
                )
            },
            collect_if: |v, present| format!("groupArrayIf({v}, {present})"),
            // `groupArray` skips NULLs; a one-element tuple of one is not
            // NULL.
            collect_all: |v| format!("arrayMap(t -> t.1, groupArray(tuple({v})))"),
            // Every type compares (a `Map`, a `Dynamic` by its type first).
            fixed_order: |l| format!("arraySort({l})"),
            mixed_order: Some(mixed_list_order_key),
            // An unmatched row's `Array` column is its default, `[]`, also
            // under `join_use_nulls = 1`.
            or_empty: None,
            tuple: |fields| format!("tuple({})", fields.join(", ")),
            element: |t, i| format!("tupleElement({t}, {i})"),
            is_null: |v| format!("isNull({v})"),
            // An aggregate has one row also over none.
            empty_of: |t, table, alias| {
                format!("(SELECT groupArray({t}) FROM {table} AS {alias} WHERE 0)")
            },
            distinct: |l| format!("arrayDistinct({l})"),
            contains: |l, x| format!("has({l}, {x})"),
            intersect: |a, b| format!("arrayIntersect({a}, {b})"),
            sorted: |l| format!("arraySort({l})"),
            // No `Nullable(Array)`: a `Dynamic` holds NULL and the array.
            null_list: |c, l| format!("if({c}, NULL, CAST({l}, 'Dynamic'))"),
            null_as_empty: true,
            aggregate_if: |f, v, c| format!("{f}If({v}, {c})"),
            fingerprint: |v| format!("cityHash64({v})"),
            map_keys: |m| format!("mapKeys({m})"),
            // A `Map(String, Dynamic)`, as a graph value's (S6c): each value
            // keeps its type, maps of any keys are of one type, and the
            // output prints it as an object.
            map_of: |entries| {
                if entries.is_empty() {
                    // `CAST(map(), 'Map(String, Dynamic)')` fails: an empty
                    // map's value type has no variant to cast from.
                    return Some(
                        "CAST(CAST(map(), 'Map(String, String)'), 'Map(String, Dynamic)')"
                            .to_string(),
                    );
                }
                let pairs: Vec<String> = entries
                    .iter()
                    .map(|e| {
                        let k = e.key.replace('\\', "\\\\").replace('\'', "\\'");
                        format!("'{k}', CAST({}, 'Dynamic')", e.value)
                    })
                    .collect();
                Some(format!("map({})", pairs.join(", ")))
            },
        })
    }

    fn graph_values(&self) -> Option<super::GraphValues> {
        // An element is a `Map(String, Dynamic)`: maps of different keys
        // and value types share it, so one array holds nodes and
        // relationships of any label. JSON output prints a map as an object
        // and a `Dynamic` as the value it holds.
        fn object(entries: &[(String, String)]) -> String {
            if entries.is_empty() {
                // `CAST(map(), 'Map(String, Dynamic)')` fails: an empty
                // map's value type has no variant to cast from.
                return "CAST(CAST(map(), 'Map(String, String)'), 'Map(String, Dynamic)')"
                    .to_string();
            }
            let pairs: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{k}, CAST({v}, 'Dynamic')"))
                .collect();
            format!("map({})", pairs.join(", "))
        }
        Some(super::GraphValues {
            object,
            object_without_nulls: |entries| {
                format!(
                    "mapFilter((__k, __v) -> __v IS NOT NULL, {})",
                    object(entries)
                )
            },
            list: |items| {
                if items.is_empty() {
                    "CAST([], 'Array(Map(String, Dynamic))')".to_string()
                } else {
                    format!("[{}]", items.join(", "))
                }
            },
            concat: |lists| match lists {
                [one] => one.clone(),
                _ => format!("arrayConcat({})", lists.join(", ")),
            },
            tail: |list| format!("arraySlice({list}, 2)"),
            reverse: |list| format!("arrayReverse({list})"),
            interleave: |rels, nodes| {
                format!("arrayFlatten(arrayMap((__r, __n) -> [__r, __n], {rels}, {nodes}))")
            },
            length: |list| format!("length({list})"),
            text: |parts| format!("concat({})", parts.join(", ")),
            to_text: |x| format!("toString({x})"),
            // A string part is quoted.
            tuple_text: |parts| format!("toString(tuple({}))", parts.join(", ")),
            tuple_value_text: |tuple, _| format!("toString({tuple})"),
            null_if: |cond, value| format!("if({cond}, NULL, CAST({value}, 'Dynamic'))"),
            any: |value| format!("any({value})"),
            texts: |items| {
                if items.is_empty() {
                    "CAST([], 'Array(String)')".to_string()
                } else {
                    format!("[{}]", items.join(", "))
                }
            },
            prefixed_texts: |prefix, list, _| {
                format!("arrayMap(__x -> concat({prefix}, toString(__x)), {list})")
            },
            empty_if: |cond, list| format!("if({cond}, [], {list})"),
        })
    }

    fn string_literal_quote_escape(&self) -> &'static str {
        "''"
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
    /// #1482: a container's type is classed in the `DISTINCT` key, not spelled:
    /// the key of `Array(Int32)` and `Array(Int64)` (or `Nullable` and
    /// `LowCardinality` elements, a `Map` of either width) is one, which a
    /// `UNION` of two storage types relies on. The class is used for the list /
    /// tuple branch, the map's own type and each map value's type, in the plain
    /// key, and for a nested container's type in the key by elements.
    #[test]
    fn a_containers_type_in_the_distinct_key_is_classed_not_spelled_1482() {
        let key = super::plain_key("v");
        assert_eq!(key.matches("'Number')").count(), 3, "{key}");
        assert_eq!(key.matches("'String')").count(), 3, "{key}");
        assert!(key.contains("'Date')"), "{key}");
        // A wrapper goes with its parenthesis (a stray `)` would miss
        // `Array(Nullable(Int32))` against `Array(UInt64)`), twice.
        assert_eq!(
            key.matches(r"(Nullable|LowCardinality)\\(([A-Za-z0-9]+(?:\\([^()]*\\))?)\\)")
                .count(),
            6,
            "{key}"
        );
        assert!(key.contains("concat('c:', replaceRegexpAll("), "{key}");
        assert!(key.contains("concat('m:', replaceRegexpAll("), "{key}");
        // The key of a container looked into classes its elements' types too.
        let deep = super::deep_key("v");
        assert!(deep.contains("concat('c:', replaceRegexpAll("), "{deep}");
        assert!(deep.contains("'Number')"), "{deep}");
    }

    /// #1482: a container whose type hides its elements (`Dynamic`,
    /// `Variant`) or prints them by storage type (`DateTime`) is keyed by its
    /// elements, `KEY_LEVELS` levels of them; every other value is its plain
    /// key, and which is chosen outside any lambda (a branch of `multiIf` is
    /// evaluated only for its rows, and not inside a lambda).
    #[test]
    fn a_container_that_hides_its_elements_is_keyed_by_them_1482() {
        let key = super::distinct_key("v");
        assert!(key.starts_with("multiIf(((startsWith("), "{key}");
        assert!(
            key.ends_with(&format!(", {})", super::plain_key("v"))),
            "{key}"
        );
        let deep = super::deep_key("v");
        assert!(key.contains(&deep), "{key}");
        assert!(
            deep.contains(&format!("range({} + 1)", super::KEY_LEVELS)),
            "{deep}"
        );
        // A `Dynamic` is cast to a container only where it is one, through an
        // `identity` (ClickHouse moves a cast into the arms of an `if`, and
        // casts the scalars too): a list, and a map as its pairs.
        for target in ["'Array(Dynamic)'", "'Array(Tuple(String, Dynamic))'"] {
            assert!(deep.contains(target), "{target}: {deep}");
        }
        assert_eq!(deep.matches("CAST(identity(if(").count(), 2, "{deep}");
        for part in ["Dynamic", "Variant", "DateTime", "Time"] {
            // A type name (after `(` or a space), not an `Enum` label.
            for lead in ["(", " "] {
                assert!(
                    key.contains(&format!("'{lead}{part}'")),
                    "{part} is a type to look into: {key}"
                );
            }
        }
    }
    use super::ClickhouseFunctionMapper;
    use super::FunctionMapper;

    #[test]
    fn quote_alias_escapes_embedded_double_quotes() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.quote_alias("b.id"), "\"b.id\"");
        assert_eq!(m.quote_alias("x\"y"), "\"x\"\"y\"");
        // A backslash is doubled: `"a\"` would not end the identifier.
        assert_eq!(m.quote_alias(r"a\"), r#""a\\""#);
        assert_eq!(m.quote_alias(r#"a\"b"#), r#""a\\""b""#);
    }

    /// #1432: a date is not a string of its text, nor an interval a number;
    /// a `Date32` is a `Date`, a `DateTime64` a `DateTime` of its zone.
    #[test]
    fn a_distinct_key_tells_temporal_values_by_type() {
        let Some(u) = ClickhouseFunctionMapper.cypher_union() else {
            panic!("ClickHouse has a Cypher UNION");
        };
        let key = (u.distinct_key)("x");
        let ty = r"replaceRegexpOne(dynamicType(CAST(x, 'Dynamic')), '^LowCardinality\\((.*)\\)$', '\\1')";
        let zone = format!("extract({ty}, '''([^'']*)''')");
        for part in [
            format!("match({ty}, '^(U?Int[0-9]|Float|BFloat|Decimal)'), 'n'"),
            format!("match({ty}, '^(String|FixedString|Enum|UUID|IPv|LowCardinality)'), 's'"),
            format!("{ty} = 'Date32', 'Date'"),
            format!(
                "startsWith({ty}, 'DateTime'), \
                 concat('DateTime ', if({zone} = '', timezone(), {zone}))"
            ),
            format!("startsWith({ty}, 'Time'), 'Time', {ty})"),
            r"replaceRegexpOne(replaceRegexpOne(toString(x), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', '')"
                .to_string(),
            format!(
                "ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(if(startsWith(\
                 {ty}, 'DateTime'), CAST(x, 'Dynamic'), NULL), 'DateTime64(6, ''UTC'')'))), '')"
            ),
        ] {
            assert!(key.contains(&part), "missing `{part}` in\n{key}");
        }
    }

    /// #1430: the one-type guard keeps its two checks. The class comparison
    /// treats every integer width as one class, so `Int64` and `UInt64` pass
    /// it, and their `coalesce` is a `Variant` of two values: that is refused
    /// (`NOT LIKE 'Variant(%'`), unless every column is itself a complex type
    /// (`Array` / `Map` / `Tuple` / `Dynamic` / `Variant`), whose `coalesce`
    /// is a `Variant` that reads correctly. One scalar column among them
    /// leaves the check in force.
    #[test]
    fn one_type_guard_refuses_a_variant_unless_every_column_is_complex_1430() {
        let guard = ClickhouseFunctionMapper
            .one_type_guard(&["a".to_string(), "b".to_string()])
            .expect("ClickHouse guards a property of several columns");
        // The class comparison is there, as before.
        assert!(guard.contains("'^U?Int\\\\d+$', 'Int'"), "{guard}");
        // The Variant check, exempt only where both columns are complex.
        let complex = |c: &str| {
            format!(
                "(toTypeName({c}) LIKE 'Array(%' OR toTypeName({c}) LIKE 'Map(%' \
                 OR toTypeName({c}) LIKE 'Tuple(%' OR toTypeName({c}) LIKE 'Dynamic(%' \
                 OR toTypeName({c}) LIKE 'Variant(%')"
            )
        };
        let check = format!(
            "(toTypeName(coalesce(a, b)) NOT LIKE 'Variant(%' OR ({} AND {}))",
            complex("a"),
            complex("b")
        );
        assert!(guard.contains(&check), "{guard}");
        // A mismatch is the nonexistent type, an error at analysis.
        assert!(
            guard.ends_with(
                "toTypeName(coalesce(a, b)), \
                 'ClickGraph_property_has_different_types_on_different_labels_or_types'))"
            ),
            "{guard}"
        );
        // A scalar column is not exempt: no `Array` pattern stands alone.
        assert!(
            !guard.contains("NOT LIKE 'Variant(%' OR (toTypeName(a) LIKE"),
            "{guard}"
        );
    }

    /// #1490: a complex value (`Array` / `Map` / `Tuple` / `Dynamic`) cannot be
    /// inside `Nullable` on ClickHouse 25.8, so its aggregate is `anyIf`;
    /// every other is `anyIfOrNull`, which is NULL for a group of NULLs only.
    #[test]
    fn any_non_null_is_any_if_for_a_complex_value_1490() {
        let m = ClickhouseFunctionMapper;
        assert_eq!(m.any_non_null("v", true), "anyIf(v, NOT isNull(v))");
        assert_eq!(m.any_non_null("v", false), "anyIfOrNull(v, NOT isNull(v))");
    }

    /// #1491: the order key of a mixed list ranks by type and orders numbers
    /// numerically (`[3]` before `[12]`), unlike `distinct_key`, which would
    /// spell them as text.
    #[test]
    fn a_mixed_list_order_key_orders_by_type_then_value() {
        let Some(lists) = ClickhouseFunctionMapper.lists() else {
            panic!("ClickHouse lists");
        };
        let key = (lists.mixed_order.expect("ClickHouse orders mixed lists"))("[1, 'a']");
        assert!(key.starts_with("arrayMap(x -> tuple(multiIf("), "{key}");
        assert!(key.ends_with("CAST([1, 'a'], 'Array(Dynamic)'))"), "{key}");
        // A number is ordered by its float, then by its exact value (#1491
        // review: scale 9 merged floats closer than 1e-9).
        assert!(key.contains("toFloat64OrNull(toString(x))"), "{key}");
        assert!(key.contains("toDecimal256OrNull(toString(x), 18)"), "{key}");
        assert!(!key.contains("toDecimal256OrNull(toString(x), 9)"), "{key}");
        assert!(
            key.contains("'^(U?Int[0-9]|Float|BFloat|Decimal)'), 15"),
            "{key}"
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
    fn to_string_datetime_spells_iso_t_separator() {
        // #1068: Neo4j toString(datetime) = "2024-03-15T10:30:00"; CH's
        // toString spells a space. `replace` keeps the fraction and the
        // zone-less (localdatetime) form, swapping only the separator.
        let m = ClickhouseFunctionMapper;
        assert_eq!(
            m.to_string_datetime("parseDateTime64BestEffort('2024-03-15T10:30:00', 3)"),
            "replace(toString(parseDateTime64BestEffort('2024-03-15T10:30:00', 3)), ' ', 'T')"
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
