/// Neo4j to ClickHouse Function Registry
///
/// Maps Neo4j function names to ClickHouse equivalents with optional argument transformations.
use std::collections::HashMap;

/// Wrap a temporal extraction argument so a downstream `year()`/`month()`/etc.
/// sees a real DateTime / TIMESTAMP rather than a raw epoch-millis BIGINT.
///
/// Schemas that store timestamps as Int64 epoch milliseconds (e.g., LDBC SNB)
/// need conversion before extraction. Dialect-aware:
///   - ClickHouse: `fromUnixTimestamp64Milli(arg)` -> DateTime64
///   - Databricks: `timestamp_millis(arg)` -> TIMESTAMP
///
/// Skips wrapping when the argument is already a datetime expression to
/// avoid double-conversion.
fn wrap_epoch_millis_arg(args: &[String]) -> Vec<String> {
    use crate::server::query_context::get_current_dialect;
    use crate::sql_generator::SqlDialect;
    if args.is_empty() {
        return args.to_vec();
    }
    let arg = &args[0];
    let dialect = get_current_dialect();
    let already_datetime = match dialect {
        SqlDialect::Databricks => {
            arg.contains("timestamp_millis")
                || arg.contains("to_timestamp")
                || arg.contains("from_unixtime")
                || arg.contains("current_timestamp")
        }
        _ => {
            arg.contains("parseDateTime64BestEffort")
                || arg.contains("fromUnixTimestamp64Milli")
                || arg.contains("now64")
                || arg.contains("now()")
                || arg.contains("toDateTime")
        }
    };
    if already_datetime {
        args.to_vec()
    } else {
        // Reuse the single source of truth for the epoch-millis -> timestamp wrap
        // (CH `fromUnixTimestamp64Milli`, Spark `timestamp_millis`) so it can't
        // drift from `render_interval_arithmetic`. (The `already_datetime`
        // detection above is still a local copy — a deferred follow-up.)
        vec![
            crate::sql_generator::function_mapper::current_function_mapper()
                .epoch_millis_to_timestamp(arg),
        ]
    }
}

/// Argument transformation: maps SQL-string args to (possibly rewritten) SQL-string args.
pub type ArgTransform = fn(&[String]) -> Vec<String>;

/// Function mapping entry
#[derive(Clone)]
pub struct FunctionMapping {
    /// Neo4j function name (lowercase for lookup)
    #[allow(dead_code)]
    pub neo4j_name: &'static str,
    /// ClickHouse function name. Also used as the fallback for any
    /// dialect that doesn't override (most SQL aggregates and scalar
    /// functions are spelled the same across dialects).
    pub clickhouse_name: &'static str,
    /// Databricks / Spark SQL name when it differs from the CH name.
    /// `None` means "use `clickhouse_name`" — appropriate for the many
    /// ANSI-shaped functions (count, sum, min, max, etc.). Setting this
    /// to `Some(...)` is how Phase 1.5 routes a registry entry through
    /// the dialect layer.
    pub databricks_name: Option<&'static str>,
    /// Optional argument transformation function
    /// Takes SQL string args, returns transformed SQL string args
    pub arg_transform: Option<ArgTransform>,
}

impl FunctionMapping {
    /// Returns the function name for the given dialect, falling back to
    /// `clickhouse_name` when the dialect has no explicit override.
    pub fn name_for(&self, dialect: crate::sql_generator::SqlDialect) -> &'static str {
        match dialect {
            crate::sql_generator::SqlDialect::Databricks => {
                self.databricks_name.unwrap_or(self.clickhouse_name)
            }
            _ => self.clickhouse_name,
        }
    }
}

/// Get function mapping for a Neo4j function name
pub fn get_function_mapping(neo4j_fn: &str) -> Option<FunctionMapping> {
    let fn_lower = neo4j_fn.to_lowercase();
    FUNCTION_MAPPINGS.get(fn_lower.as_str()).cloned()
}

// Static function mapping table
lazy_static::lazy_static! {
    static ref FUNCTION_MAPPINGS: HashMap<&'static str, FunctionMapping> = {
        let mut m = HashMap::new();

        // ===== DATETIME FUNCTIONS =====

        // datetime() -> parseDateTime64BestEffort(arg) with millisecond precision
        // Returns DateTime64(3) type
        m.insert("datetime", FunctionMapping {
            neo4j_name: "datetime",
            clickhouse_name: "parseDateTime64BestEffort",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.is_empty() {
                    // datetime() with no args returns current timestamp
                    vec!["now64(3)".to_string()]
                } else {
                    // datetime(string) parses ISO8601/various formats to DateTime64
                    vec![args[0].clone(), "3".to_string()] // 3 = millisecond precision
                }
            }),
        });

        // toUnixTimestampMillis() — datetime string -> epoch millis BIGINT.
        // CH:    toUnixTimestamp64Milli(parseDateTime64BestEffort(arg, 3))
        // Spark: unix_millis(to_timestamp(arg))
        m.insert("tounixtimestampmillis", FunctionMapping {
            neo4j_name: "toUnixTimestampMillis",
            clickhouse_name: "toUnixTimestamp64Milli",
            databricks_name: Some("unix_millis"),
            arg_transform: Some(|args| {
                use crate::server::query_context::get_current_dialect;
                use crate::sql_generator::SqlDialect;
                let databricks = matches!(get_current_dialect(), SqlDialect::Databricks);
                if args.is_empty() {
                    let now = if databricks { "current_timestamp()" } else { "now64(3)" };
                    return vec![now.to_string()];
                }
                let wrapped = if databricks {
                    format!("to_timestamp({})", args[0])
                } else {
                    format!("parseDateTime64BestEffort({}, 3)", args[0])
                };
                vec![wrapped]
            }),
        });

        // date() -> CH toDate(arg) / Spark to_date(arg). No-arg = current date:
        // CH today() / Spark current_date(). Spark has no toDate/today().
        m.insert("date", FunctionMapping {
            neo4j_name: "date",
            clickhouse_name: "toDate",
            databricks_name: Some("to_date"),
            arg_transform: Some(|args| {
                if args.is_empty() {
                    use crate::server::query_context::get_current_dialect;
                    use crate::sql_generator::SqlDialect;
                    let now = if matches!(get_current_dialect(), SqlDialect::Databricks) {
                        "current_date()"
                    } else {
                        "today()"
                    };
                    vec![now.to_string()]
                } else {
                    vec![args[0].clone()]
                }
            }),
        });

        // timestamp() -> toUnixTimestamp(arg) or toUnixTimestamp(now()) if no args
        m.insert("timestamp", FunctionMapping {
            neo4j_name: "timestamp",
            clickhouse_name: "toUnixTimestamp",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.is_empty() {
                    vec!["now()".to_string()]
                } else {
                    vec![args[0].clone()]
                }
            }),
        });

        // ===== STRING FUNCTIONS =====

        // toUpper() -> upper()
        m.insert("toupper", FunctionMapping {
            neo4j_name: "toUpper",
            clickhouse_name: "upper",
            databricks_name: None,
            arg_transform: None,
        });

        // toLower() -> lower()
        m.insert("tolower", FunctionMapping {
            neo4j_name: "toLower",
            clickhouse_name: "lower",
            databricks_name: None,
            arg_transform: None,
        });

        // trim() -> trim(arg). Bare trim(str) removes leading/trailing whitespace
        // on both CH and Spark. The old arg_transform emitted `trim(BOTH arg)`
        // (missing the `' ' FROM`), which is invalid SQL and 500'd on CH.
        m.insert("trim", FunctionMapping {
            neo4j_name: "trim",
            clickhouse_name: "trim",
            databricks_name: None,
            arg_transform: None,
        });

        // substring(str, start [, length]) -> substring(str, start+1, length)
        // Note: Neo4j is 0-indexed, ClickHouse is 1-indexed
        m.insert("substring", FunctionMapping {
            neo4j_name: "substring",
            clickhouse_name: "substring",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.len() == 2 {
                    // substring(str, start) - take rest of string
                    vec![args[0].clone(), format!("({}) + 1", args[1])]
                } else if args.len() == 3 {
                    // substring(str, start, length)
                    vec![args[0].clone(), format!("({}) + 1", args[1]), args[2].clone()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // size() -> length() for both strings and arrays
        m.insert("size", FunctionMapping {
            neo4j_name: "size",
            clickhouse_name: "length",
            // CH `length` is overloaded for arrays AND strings, but Spark splits
            // them: `length` is string/binary-only (rejects arrays) and `size`
            // is collection-only (rejects strings). The static name therefore
            // can't be right for both — `length` is the string-safe default and
            // the ScalarFnCall render site upgrades it to Spark `size` when the
            // argument is a detected collection (see `databricks_size_name`).
            databricks_name: None,
            arg_transform: None,
        });

        // Cypher split(str, delim):
        //   CH    -> splitByChar(delim, str)   [name + args swapped]
        //   Spark -> split(str, delim)         [name change only, Cypher arg order]
        // The swap is ClickHouse-only, so the arg_transform reads the active
        // dialect. (Spark `split` treats arg 2 as a regex, whereas Cypher/CH
        // split is literal; equivalent for the single-char, non-regex-meta
        // delimiters Cypher `split` is typically used with.)
        m.insert("split", FunctionMapping {
            neo4j_name: "split",
            clickhouse_name: "splitByChar",
            databricks_name: Some("split"),
            arg_transform: Some(|args| {
                let dialect = crate::server::query_context::get_current_dialect();
                if dialect == crate::sql_generator::SqlDialect::ClickHouse && args.len() >= 2 {
                    // CH splitByChar(delim, str): swap from Cypher split(str, delim).
                    vec![args[1].clone(), args[0].clone()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // replace(str, search, replacement) -> replaceAll(str, search, replacement)
        m.insert("replace", FunctionMapping {
            neo4j_name: "replace",
            clickhouse_name: "replaceAll",
            databricks_name: Some("replace"), // Spark literal 3-arg replace
            arg_transform: None,
        });

        // reverse(str) -> reverse(str)
        m.insert("reverse", FunctionMapping {
            neo4j_name: "reverse",
            clickhouse_name: "reverse",
            databricks_name: None,
            arg_transform: None,
        });

        // left(str, length) -> substring(str, 1, length)
        m.insert("left", FunctionMapping {
            neo4j_name: "left",
            clickhouse_name: "substring",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.len() >= 2 {
                    vec![args[0].clone(), "1".to_string(), args[1].clone()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // right(str, length) -> substring(str, -length)
        m.insert("right", FunctionMapping {
            neo4j_name: "right",
            clickhouse_name: "substring",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.len() >= 2 {
                    vec![args[0].clone(), format!("-({})", args[1])]
                } else {
                    args.to_vec()
                }
            }),
        });

        // ===== MATH FUNCTIONS =====

        // abs() -> abs() [1:1 mapping]
        m.insert("abs", FunctionMapping {
            neo4j_name: "abs",
            clickhouse_name: "abs",
            databricks_name: None,
            arg_transform: None,
        });

        // ceil() -> ceil() [1:1 mapping]
        m.insert("ceil", FunctionMapping {
            neo4j_name: "ceil",
            clickhouse_name: "ceil",
            databricks_name: None,
            arg_transform: None,
        });

        // floor() -> floor() [1:1 mapping]
        m.insert("floor", FunctionMapping {
            neo4j_name: "floor",
            clickhouse_name: "floor",
            databricks_name: None,
            arg_transform: None,
        });

        // round() -> round() [1:1 mapping]
        m.insert("round", FunctionMapping {
            neo4j_name: "round",
            clickhouse_name: "round",
            databricks_name: None,
            arg_transform: None,
        });

        // sqrt() -> sqrt() [1:1 mapping]
        m.insert("sqrt", FunctionMapping {
            neo4j_name: "sqrt",
            clickhouse_name: "sqrt",
            databricks_name: None,
            arg_transform: None,
        });

        // rand() -> rand() / 4294967295.0 (normalize to 0.0-1.0)
        m.insert("rand", FunctionMapping {
            neo4j_name: "rand",
            clickhouse_name: "rand",
            databricks_name: None,
            arg_transform: Some(|_args| {
                // Neo4j rand() returns 0.0-1.0
                // ClickHouse rand() returns UInt32
                vec!["rand() / 4294967295.0".to_string()]
            }),
        });

        // sign() -> sign() [1:1 mapping]
        m.insert("sign", FunctionMapping {
            neo4j_name: "sign",
            clickhouse_name: "sign",
            databricks_name: None,
            arg_transform: None,
        });

        // ===== LIST FUNCTIONS =====

        // head(list) -> arrayElement(list, 1) [first element]
        m.insert("head", FunctionMapping {
            neo4j_name: "head",
            clickhouse_name: "arrayElement",
            databricks_name: Some("element_at"), // Spark 1-based element access
            arg_transform: Some(|args| {
                if !args.is_empty() {
                    vec![args[0].clone(), "1".to_string()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // tail(list) = all but first. CH arraySlice(list, 2) (2-arg, rest from
        // offset). Spark slice requires a length, so emit slice(list, 2, size(list) - 1).
        m.insert("tail", FunctionMapping {
            neo4j_name: "tail",
            clickhouse_name: "arraySlice",
            databricks_name: Some("slice"),
            arg_transform: Some(|args| {
                use crate::server::query_context::get_current_dialect;
                use crate::sql_generator::SqlDialect;
                if args.is_empty() {
                    return args.to_vec();
                }
                let list = args[0].clone();
                if matches!(get_current_dialect(), SqlDialect::Databricks) {
                    // Floor at 0: slice errors on negative length (empty list).
                    vec![
                        list.clone(),
                        "2".to_string(),
                        format!("greatest(size({}) - 1, 0)", list),
                    ]
                } else {
                    vec![list, "2".to_string()]
                }
            }),
        });

        // last(list) -> arrayElement(list, -1) [last element]
        m.insert("last", FunctionMapping {
            neo4j_name: "last",
            clickhouse_name: "arrayElement",
            databricks_name: Some("element_at"), // Spark element_at supports -1 (last)
            arg_transform: Some(|args| {
                if !args.is_empty() {
                    vec![args[0].clone(), "-1".to_string()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // range(start, end [, step]) — Cypher range is INCLUSIVE of `end`.
        //   CH `range(start, end [, step])` is EXCLUSIVE of `end`  -> bump end +1
        //     (range(1,5) gave [1,2,3,4], must be [1,2,3,4,5]; silently wrong).
        //   Spark has no `range`; `sequence(start, end [, step])` is inclusive -> use as-is.
        m.insert("range", FunctionMapping {
            neo4j_name: "range",
            clickhouse_name: "range",
            databricks_name: Some("sequence"),
            arg_transform: Some(|args| {
                use crate::server::query_context::get_current_dialect;
                use crate::sql_generator::SqlDialect;
                // Spark sequence() is already inclusive — leave args untouched.
                if matches!(get_current_dialect(), SqlDialect::Databricks) {
                    return args.to_vec();
                }
                // ClickHouse range() is exclusive of `end`; make it inclusive by
                // bumping the end bound (2nd arg) by 1. Works for the 2-arg and
                // 3-arg (step) ascending forms.
                if args.len() >= 2 {
                    let mut out = args.to_vec();
                    out[1] = format!("({}) + 1", args[1]);
                    out
                } else {
                    args.to_vec()
                }
            }),
        });

        // ===== TYPE CONVERSION FUNCTIONS =====

        // toInteger() -> toInt64() (CH) / bigint() (Spark)
        m.insert("tointeger", FunctionMapping {
            neo4j_name: "toInteger",
            clickhouse_name: "toInt64",
            databricks_name: Some("bigint"),
            arg_transform: None,
        });

        // toFloat() -> toFloat64() (CH) / double() (Spark)
        m.insert("tofloat", FunctionMapping {
            neo4j_name: "toFloat",
            clickhouse_name: "toFloat64",
            databricks_name: Some("double"),
            arg_transform: None,
        });

        // toString() -> toString() (CH) / string() (Spark)
        m.insert("tostring", FunctionMapping {
            neo4j_name: "toString",
            clickhouse_name: "toString",
            databricks_name: Some("string"),
            arg_transform: None,
        });

        // toBoolean() -> toBool() (CH) / boolean() (Spark). Both accept string
        // ('true'/'false') and numeric args; the old if(arg,1,0) form broke on
        // string inputs (CH: "Illegal type String ... of function if").
        m.insert("toboolean", FunctionMapping {
            neo4j_name: "toBoolean",
            clickhouse_name: "toBool",
            databricks_name: Some("boolean"),
            arg_transform: None,
        });

        // ===== AGGREGATION FUNCTIONS =====

        // collect() -> groupArray() (CH) / collect_list() (Spark/Databricks)
        // First Phase 1.5 entry to actually exercise `databricks_name` —
        // the dialect-aware accessor falls back to `clickhouse_name` for
        // every other entry (most aggregates are spelled the same).
        m.insert("collect", FunctionMapping {
            neo4j_name: "collect",
            clickhouse_name: "groupArray",
            databricks_name: Some("collect_list"),
            arg_transform: None,
        });

        // ===== TRIGONOMETRIC FUNCTIONS =====

        // sin() -> sin() [1:1 mapping]
        m.insert("sin", FunctionMapping {
            neo4j_name: "sin",
            clickhouse_name: "sin",
            databricks_name: None,
            arg_transform: None,
        });

        // cos() -> cos() [1:1 mapping]
        m.insert("cos", FunctionMapping {
            neo4j_name: "cos",
            clickhouse_name: "cos",
            databricks_name: None,
            arg_transform: None,
        });

        // tan() -> tan() [1:1 mapping]
        m.insert("tan", FunctionMapping {
            neo4j_name: "tan",
            clickhouse_name: "tan",
            databricks_name: None,
            arg_transform: None,
        });

        // asin() -> asin() [1:1 mapping]
        m.insert("asin", FunctionMapping {
            neo4j_name: "asin",
            clickhouse_name: "asin",
            databricks_name: None,
            arg_transform: None,
        });

        // acos() -> acos() [1:1 mapping]
        m.insert("acos", FunctionMapping {
            neo4j_name: "acos",
            clickhouse_name: "acos",
            databricks_name: None,
            arg_transform: None,
        });

        // atan() -> atan() [1:1 mapping]
        m.insert("atan", FunctionMapping {
            neo4j_name: "atan",
            clickhouse_name: "atan",
            databricks_name: None,
            arg_transform: None,
        });

        // atan2(y, x) -> atan2(y, x) [1:1 mapping]
        m.insert("atan2", FunctionMapping {
            neo4j_name: "atan2",
            clickhouse_name: "atan2",
            databricks_name: None,
            arg_transform: None,
        });

        // ===== ADDITIONAL MATH FUNCTIONS =====

        // exp() -> exp() [1:1 mapping]
        m.insert("exp", FunctionMapping {
            neo4j_name: "exp",
            clickhouse_name: "exp",
            databricks_name: None,
            arg_transform: None,
        });

        // log() -> log() [natural logarithm, 1:1 mapping]
        m.insert("log", FunctionMapping {
            neo4j_name: "log",
            clickhouse_name: "log",
            databricks_name: None,
            arg_transform: None,
        });

        // log10() -> log10() [1:1 mapping]
        m.insert("log10", FunctionMapping {
            neo4j_name: "log10",
            clickhouse_name: "log10",
            databricks_name: None,
            arg_transform: None,
        });

        // pi() -> pi() [1:1 mapping]
        m.insert("pi", FunctionMapping {
            neo4j_name: "pi",
            clickhouse_name: "pi",
            databricks_name: None,
            arg_transform: None,
        });

        // e() -> e() [1:1 mapping]
        m.insert("e", FunctionMapping {
            neo4j_name: "e",
            clickhouse_name: "e",
            databricks_name: None,
            arg_transform: None,
        });

        // pow(base, exp) / ^ -> pow(base, exp) [1:1 mapping]
        m.insert("pow", FunctionMapping {
            neo4j_name: "pow",
            clickhouse_name: "pow",
            databricks_name: None,
            arg_transform: None,
        });

        // ===== ADDITIONAL STRING FUNCTIONS =====

        // ltrim() -> CH trimLeft() / Spark ltrim(). Spark has no trimLeft.
        m.insert("ltrim", FunctionMapping {
            neo4j_name: "lTrim",
            clickhouse_name: "trimLeft",
            databricks_name: Some("ltrim"),
            arg_transform: None,
        });

        // rtrim() -> CH trimRight() / Spark rtrim(). Spark has no trimRight.
        m.insert("rtrim", FunctionMapping {
            neo4j_name: "rTrim",
            clickhouse_name: "trimRight",
            databricks_name: Some("rtrim"),
            arg_transform: None,
        });

        // ===== ADDITIONAL AGGREGATION FUNCTIONS =====

        // stDev() -> stddevSamp() [sample standard deviation]
        m.insert("stdev", FunctionMapping {
            neo4j_name: "stDev",
            clickhouse_name: "stddevSamp",
            databricks_name: Some("stddev_samp"),
            arg_transform: None,
        });

        // stDevP() -> stddevPop() [population standard deviation]
        m.insert("stdevp", FunctionMapping {
            neo4j_name: "stDevP",
            clickhouse_name: "stddevPop",
            databricks_name: Some("stddev_pop"),
            arg_transform: None,
        });

        // percentileCont(percentile) -> quantile(percentile)
        // Note: Neo4j syntax is percentileCont(expr, percentile)
        // ClickHouse syntax is quantile(percentile)(expr) - parametric aggregate
        // We'll use simpler quantileExact for now which takes (expr)
        m.insert("percentilecont", FunctionMapping {
            neo4j_name: "percentileCont",
            clickhouse_name: "median",  // median = quantile(0.5), closest simple equivalent
            databricks_name: None,
            arg_transform: Some(|args| {
                // percentileCont(expr, 0.5) -> median(expr)
                // For other percentiles, user needs to use quantile directly
                if !args.is_empty() {
                    vec![args[0].clone()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // percentileDisc(percentile) -> similar to percentileCont but discrete
        m.insert("percentiledisc", FunctionMapping {
            neo4j_name: "percentileDisc",
            clickhouse_name: "median",
            databricks_name: None,
            arg_transform: Some(|args| {
                if !args.is_empty() {
                    vec![args[0].clone()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // ===== PREDICATE/NULL FUNCTIONS =====

        // coalesce(a, b, ...) -> coalesce(a, b, ...) [1:1 mapping]
        m.insert("coalesce", FunctionMapping {
            neo4j_name: "coalesce",
            clickhouse_name: "coalesce",
            databricks_name: None,
            arg_transform: None,
        });

        // nullIf(a, b) -> nullIf(a, b) [1:1 mapping]
        m.insert("nullif", FunctionMapping {
            neo4j_name: "nullIf",
            clickhouse_name: "nullIf",
            databricks_name: None,
            arg_transform: None,
        });

        // ===== ADDITIONAL LIST FUNCTIONS =====

        // keys(map) -> mapKeys(map) - get keys from a map
        m.insert("keys", FunctionMapping {
            neo4j_name: "keys",
            clickhouse_name: "mapKeys",
            databricks_name: None,
            arg_transform: None,
        });

        // ===== VECTOR/SIMILARITY FUNCTIONS =====
        // These map Neo4j GDS similarity functions to ClickHouse distance functions
        // Note: ClickHouse requires pre-computed embedding vectors (Array(Float32))

        // gds.similarity.cosine(v1, v2) -> 1 - cosineDistance(v1, v2)
        // Returns similarity (0-1) where 1 = identical, 0 = orthogonal
        m.insert("gds.similarity.cosine", FunctionMapping {
            neo4j_name: "gds.similarity.cosine",
            clickhouse_name: "cosineDistance",
            databricks_name: None,
            arg_transform: Some(|args| {
                // Wrap in (1 - distance) to convert distance to similarity
                if args.len() >= 2 {
                    vec![format!("1 - cosineDistance({}, {})", args[0], args[1])]
                } else {
                    args.to_vec()
                }
            }),
        });

        // gds.similarity.euclidean(v1, v2) -> 1 / (1 + L2Distance(v1, v2))
        // Returns similarity (0-1) where 1 = identical
        m.insert("gds.similarity.euclidean", FunctionMapping {
            neo4j_name: "gds.similarity.euclidean",
            clickhouse_name: "L2Distance",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.len() >= 2 {
                    vec![format!("1 / (1 + L2Distance({}, {}))", args[0], args[1])]
                } else {
                    args.to_vec()
                }
            }),
        });

        // gds.similarity.euclideanDistance(v1, v2) -> L2Distance(v1, v2)
        // Returns raw Euclidean distance
        m.insert("gds.similarity.euclideandistance", FunctionMapping {
            neo4j_name: "gds.similarity.euclideanDistance",
            clickhouse_name: "L2Distance",
            databricks_name: None,
            arg_transform: None,
        });

        // vector.similarity.cosine(v1, v2) -> 1 - cosineDistance(v1, v2)
        // Neo4j 5.x vector similarity function
        m.insert("vector.similarity.cosine", FunctionMapping {
            neo4j_name: "vector.similarity.cosine",
            clickhouse_name: "cosineDistance",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.len() >= 2 {
                    vec![format!("1 - cosineDistance({}, {})", args[0], args[1])]
                } else {
                    args.to_vec()
                }
            }),
        });

        // ===== ADDITIONAL LIST/ARRAY FUNCTIONS =====

        // reduce() - complex, needs special handling but add placeholder
        // Note: ClickHouse has arrayReduce() but syntax differs significantly

        // filter() -> arrayFilter() [list comprehension style]
        // Neo4j: [x IN list WHERE x > 0] or filter(x IN list WHERE x > 0)
        // ClickHouse: arrayFilter(x -> x > 0, list)
        // This requires special AST handling, placeholder for now

        // extract() -> arrayMap() for extracting properties
        // Neo4j: [x IN list | x.prop] or extract(x IN list | x.prop)
        // ClickHouse: arrayMap(x -> x.prop, list)
        // This requires special AST handling, placeholder for now

        // all() -> arrayAll() - check if all elements match predicate
        m.insert("all", FunctionMapping {
            neo4j_name: "all",
            clickhouse_name: "arrayAll",
            databricks_name: None,
            arg_transform: None, // Requires special handling for predicate syntax
        });

        // any() -> arrayExists() - check if any element matches predicate
        m.insert("any", FunctionMapping {
            neo4j_name: "any",
            clickhouse_name: "arrayExists",
            databricks_name: None,
            arg_transform: None, // Requires special handling for predicate syntax
        });

        // none() -> NOT arrayExists() - check if no element matches predicate
        m.insert("none", FunctionMapping {
            neo4j_name: "none",
            clickhouse_name: "arrayExists",
            databricks_name: None,
            arg_transform: Some(|args| {
                // Will need to wrap with NOT in the caller
                args.to_vec()
            }),
        });

        // single() -> check exactly one element matches
        // ClickHouse: arrayCount(...) = 1
        m.insert("single", FunctionMapping {
            neo4j_name: "single",
            clickhouse_name: "arrayCount",
            databricks_name: None,
            arg_transform: None, // Caller needs to add = 1
        });

        // isEmpty(list) -> empty(list) or length(list) = 0
        m.insert("isempty", FunctionMapping {
            neo4j_name: "isEmpty",
            clickhouse_name: "empty",
            databricks_name: None,
            arg_transform: None,
        });

        // ===== ADDITIONAL TEMPORAL FUNCTIONS =====

        // duration() - Neo4j duration type
        // ClickHouse doesn't have a direct equivalent, use interval arithmetic
        // duration({days: 5}) -> toIntervalDay(5)
        // This requires special handling for map arguments

        // localdatetime() -> now() or parseDateTime64BestEffort()
        m.insert("localdatetime", FunctionMapping {
            neo4j_name: "localdatetime",
            clickhouse_name: "now64",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.is_empty() {
                    vec!["3".to_string()] // millisecond precision
                } else {
                    vec![format!("parseDateTime64BestEffort({}, 3)", args[0])]
                }
            }),
        });

        // localtime() -> toTime(now()) - time without timezone
        m.insert("localtime", FunctionMapping {
            neo4j_name: "localtime",
            clickhouse_name: "toTime",
            databricks_name: None,
            arg_transform: Some(|args| {
                if args.is_empty() {
                    vec!["now()".to_string()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // date.truncate() -> toStartOfDay/Week/Month/Year
        // Neo4j: date.truncate('week', date)
        // ClickHouse: toStartOfWeek(date)
        // Requires special handling for unit argument

        // datetime.truncate() -> similar
        // date.statement() functions handled specially

        // ===== DATE/TIME EXTRACTION FUNCTIONS =====

        // date().year, datetime().month, etc. are property accesses
        // But Neo4j also has explicit functions:

        // Temporal extraction. Spark's year/month/.../quarter accept a
        // TIMESTAMP directly; `wrap_epoch_millis_arg` chooses the dialect's
        // BIGINT→TIMESTAMP wrapper (CH: fromUnixTimestamp64Milli,
        // Spark: timestamp_millis). Per-component name mapping below; same
        // name in both dialects when names match.

        // year(datetime) -> CH: toYear, Spark: year
        m.insert("year", FunctionMapping {
            neo4j_name: "year",
            clickhouse_name: "toYear",
            databricks_name: Some("year"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // month(datetime) -> CH: toMonth, Spark: month
        m.insert("month", FunctionMapping {
            neo4j_name: "month",
            clickhouse_name: "toMonth",
            databricks_name: Some("month"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // day(datetime) -> CH: toDayOfMonth, Spark: dayofmonth
        m.insert("day", FunctionMapping {
            neo4j_name: "day",
            clickhouse_name: "toDayOfMonth",
            databricks_name: Some("dayofmonth"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // hour(datetime) -> CH: toHour, Spark: hour
        m.insert("hour", FunctionMapping {
            neo4j_name: "hour",
            clickhouse_name: "toHour",
            databricks_name: Some("hour"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // minute(datetime) -> CH: toMinute, Spark: minute
        m.insert("minute", FunctionMapping {
            neo4j_name: "minute",
            clickhouse_name: "toMinute",
            databricks_name: Some("minute"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // second(datetime) -> CH: toSecond, Spark: second
        m.insert("second", FunctionMapping {
            neo4j_name: "second",
            clickhouse_name: "toSecond",
            databricks_name: Some("second"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // dayOfWeek(datetime) -> CH: toDayOfWeek (1=Monday..7=Sunday, ISO)
        //                        Spark: dayofweek (1=Sunday..7=Saturday) — different!
        // Direct name swap would silently shift the result by one day; needs a
        // structural rewrite like `weekday(x) + 1` to preserve ISO semantics.
        // Until that lands, fall through to `toDayOfWeek` on Spark so the gap
        // surfaces as UNRESOLVED_ROUTINE rather than silently-wrong data.
        m.insert("dayofweek", FunctionMapping {
            neo4j_name: "dayOfWeek",
            clickhouse_name: "toDayOfWeek",
            databricks_name: None,
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // dayOfYear(datetime) -> CH: toDayOfYear, Spark: dayofyear
        m.insert("dayofyear", FunctionMapping {
            neo4j_name: "dayOfYear",
            clickhouse_name: "toDayOfYear",
            databricks_name: Some("dayofyear"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // quarter(datetime) -> CH: toQuarter, Spark: quarter
        m.insert("quarter", FunctionMapping {
            neo4j_name: "quarter",
            clickhouse_name: "toQuarter",
            databricks_name: Some("quarter"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // week(datetime) -> CH: toISOWeek, Spark: weekofyear
        m.insert("week", FunctionMapping {
            neo4j_name: "week",
            clickhouse_name: "toISOWeek",
            databricks_name: Some("weekofyear"),
            arg_transform: Some(wrap_epoch_millis_arg),
        });

        // ===== ADDITIONAL STRING FUNCTIONS =====

        // startsWith(str, prefix) -> startsWith(str, prefix) [1:1]
        m.insert("startswith", FunctionMapping {
            neo4j_name: "startsWith",
            clickhouse_name: "startsWith",
            databricks_name: None,
            arg_transform: None,
        });

        // endsWith(str, suffix) -> endsWith(str, suffix) [1:1]
        m.insert("endswith", FunctionMapping {
            neo4j_name: "endsWith",
            clickhouse_name: "endsWith",
            databricks_name: None,
            arg_transform: None,
        });

        // tuple(a, b, ...) -> CH `tuple`, Spark `struct` (element-wise equality).
        m.insert("tuple", FunctionMapping {
            neo4j_name: "tuple",
            clickhouse_name: "tuple",
            databricks_name: Some("struct"),
            arg_transform: None,
        });

        // contains(str, search) -> position(str, search) > 0 (caller adds the > 0)
        m.insert("contains", FunctionMapping {
            neo4j_name: "contains",
            clickhouse_name: "position",
            databricks_name: None,
            arg_transform: Some(|args| {
                // contains(str, search) -> position(str, search) > 0
                // (caller adds the > 0). Spark's position(substr, str) reverses
                // the arg order, so swap the two operands on Databricks.
                use crate::server::query_context::get_current_dialect;
                use crate::sql_generator::SqlDialect;
                if args.len() == 2 && matches!(get_current_dialect(), SqlDialect::Databricks) {
                    vec![args[1].clone(), args[0].clone()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // normalize(str) -> normalizeUTF8NFC(str)
        m.insert("normalize", FunctionMapping {
            neo4j_name: "normalize",
            clickhouse_name: "normalizeUTF8NFC",
            databricks_name: None,
            arg_transform: None,
        });

        // valueType(value) - returns type name, no direct CH equivalent
        // ClickHouse: toTypeName(value)
        m.insert("valuetype", FunctionMapping {
            neo4j_name: "valueType",
            clickhouse_name: "toTypeName",
            databricks_name: None,
            arg_transform: None,
        });

        // ===== ADDITIONAL AGGREGATION FUNCTIONS =====

        // avg() -> avg() [1:1]
        m.insert("avg", FunctionMapping {
            neo4j_name: "avg",
            clickhouse_name: "avg",
            databricks_name: None,
            arg_transform: None,
        });

        // sum() -> sum() [1:1]
        m.insert("sum", FunctionMapping {
            neo4j_name: "sum",
            clickhouse_name: "sum",
            databricks_name: None,
            arg_transform: None,
        });

        // min() -> min() [1:1]
        m.insert("min", FunctionMapping {
            neo4j_name: "min",
            clickhouse_name: "min",
            databricks_name: None,
            arg_transform: None,
        });

        // max() -> max() [1:1]
        m.insert("max", FunctionMapping {
            neo4j_name: "max",
            clickhouse_name: "max",
            databricks_name: None,
            arg_transform: None,
        });

        // count() -> count() [1:1]
        m.insert("count", FunctionMapping {
            neo4j_name: "count",
            clickhouse_name: "count",
            databricks_name: None,
            arg_transform: None,
        });

        // anyLast() — internal IR-level aggregate used by property_expansion
        // to pick a non-deterministic value of a non-grouped column. CH ships
        // `anyLast`; Spark's `any_value()` (3.4+) has matching semantics.
        m.insert("anylast", FunctionMapping {
            neo4j_name: "anyLast",
            clickhouse_name: "anyLast",
            databricks_name: Some("any_value"),
            arg_transform: None,
        });

        // countIf(predicate) — conditional count. CH: countIf, Spark: count_if (DBR 13.1+).
        m.insert("countif", FunctionMapping {
            neo4j_name: "countIf",
            clickhouse_name: "countIf",
            databricks_name: Some("count_if"),
            arg_transform: None,
        });

        // ===== SPATIAL FUNCTIONS (basic) =====
        // Note: Full spatial support would require more extensive work

        // point.distance(p1, p2) -> geoDistance for lat/lon
        // Neo4j: point.distance(point({longitude: x1, latitude: y1}), point({longitude: x2, latitude: y2}))
        // ClickHouse: geoDistance(lon1, lat1, lon2, lat2)
        // Requires special handling to extract coordinates from point()

        // ===== ADDITIONAL TYPE FUNCTIONS =====

        // type(relationship) - handled specially in code
        // labels(node) - handled specially in code

        // id(node/relationship) - Neo4j internal integer ID
        // The actual ID is computed at result transformation time from element_id.
        // Here we return 0 as a placeholder that won't break SQL execution.
        // The result transformer uses the node's element_id to compute the proper ID.
        m.insert("id", FunctionMapping {
            neo4j_name: "id",
            clickhouse_name: "toInt64",  // toInt64(0) = 0 placeholder (CH) / bigint(0) (Spark)
            databricks_name: Some("bigint"),
            arg_transform: Some(|_args| {
                // Return 0 as placeholder - actual ID computed from element_id at result time
                vec!["0".to_string()]
            }),
        });

        m
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_functions() {
        assert!(get_function_mapping("toUpper").is_some());
        assert!(get_function_mapping("TOUPPER").is_some()); // Case insensitive
        assert!(get_function_mapping("size").is_some());

        let mapping = get_function_mapping("toUpper")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "upper");
    }

    #[test]
    fn test_math_functions() {
        assert!(get_function_mapping("abs").is_some());
        assert!(get_function_mapping("sqrt").is_some());

        let mapping =
            get_function_mapping("ceil").expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "ceil");
    }

    #[test]
    fn test_datetime_functions() {
        assert!(get_function_mapping("datetime").is_some());
        assert!(get_function_mapping("date").is_some());

        let mapping = get_function_mapping("timestamp")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "toUnixTimestamp");
    }

    #[test]
    fn test_arg_transformations() {
        let mapping = get_function_mapping("split")
            .expect("get_function_mapping failed for function in test");
        assert!(mapping.arg_transform.is_some());

        // split(str, delim) -> splitByChar(delim, str)
        let transform = mapping
            .arg_transform
            .expect("arg_transform should exist for this function");
        let args = vec!["'hello,world'".to_string(), "','".to_string()];
        let result = transform(&args);
        assert_eq!(result, vec!["','", "'hello,world'"]);
    }

    #[test]
    fn test_unsupported_function() {
        assert!(get_function_mapping("unknownFunction").is_none());
    }

    #[test]
    fn test_trig_functions() {
        // Test all trig functions exist
        assert!(get_function_mapping("sin").is_some());
        assert!(get_function_mapping("cos").is_some());
        assert!(get_function_mapping("tan").is_some());
        assert!(get_function_mapping("asin").is_some());
        assert!(get_function_mapping("acos").is_some());
        assert!(get_function_mapping("atan").is_some());
        assert!(get_function_mapping("atan2").is_some());

        // Verify 1:1 mappings
        let mapping =
            get_function_mapping("sin").expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "sin");
    }

    #[test]
    fn test_additional_math_functions() {
        assert!(get_function_mapping("exp").is_some());
        assert!(get_function_mapping("log").is_some());
        assert!(get_function_mapping("log10").is_some());
        assert!(get_function_mapping("pi").is_some());
        assert!(get_function_mapping("e").is_some());
        assert!(get_function_mapping("pow").is_some());

        let mapping =
            get_function_mapping("exp").expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "exp");
    }

    #[test]
    fn test_trim_functions() {
        assert!(get_function_mapping("ltrim").is_some());
        assert!(get_function_mapping("rtrim").is_some());

        let mapping = get_function_mapping("ltrim")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "trimLeft");

        let mapping = get_function_mapping("rtrim")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "trimRight");
    }

    #[test]
    fn test_aggregation_functions() {
        assert!(get_function_mapping("stdev").is_some());
        assert!(get_function_mapping("stdevp").is_some());
        assert!(get_function_mapping("percentilecont").is_some());
        assert!(get_function_mapping("percentiledisc").is_some());

        let mapping = get_function_mapping("stdev")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "stddevSamp");

        let mapping = get_function_mapping("stdevp")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "stddevPop");
    }

    #[test]
    fn test_predicate_functions() {
        assert!(get_function_mapping("coalesce").is_some());
        assert!(get_function_mapping("nullif").is_some());

        let mapping = get_function_mapping("coalesce")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "coalesce");
    }

    #[test]
    fn test_vector_similarity_functions() {
        // Vector/GDS similarity functions
        assert!(get_function_mapping("gds.similarity.cosine").is_some());
        assert!(get_function_mapping("gds.similarity.euclidean").is_some());
        assert!(get_function_mapping("gds.similarity.euclideandistance").is_some());
        assert!(get_function_mapping("vector.similarity.cosine").is_some());

        // Test cosine similarity transformation
        let mapping = get_function_mapping("gds.similarity.cosine")
            .expect("get_function_mapping failed for function in test");
        assert!(mapping.arg_transform.is_some());
        let transform = mapping
            .arg_transform
            .expect("arg_transform should exist for this function");
        let args = vec!["v1".to_string(), "v2".to_string()];
        let result = transform(&args);
        assert!(result[0].contains("1 - cosineDistance"));
    }

    #[test]
    fn test_temporal_extraction_functions() {
        // Date/time extraction functions
        assert!(get_function_mapping("year").is_some());
        assert!(get_function_mapping("month").is_some());
        assert!(get_function_mapping("day").is_some());
        assert!(get_function_mapping("hour").is_some());
        assert!(get_function_mapping("minute").is_some());
        assert!(get_function_mapping("second").is_some());
        assert!(get_function_mapping("dayofweek").is_some());
        assert!(get_function_mapping("dayofyear").is_some());
        assert!(get_function_mapping("quarter").is_some());
        assert!(get_function_mapping("week").is_some());

        let mapping =
            get_function_mapping("year").expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "toYear");
    }

    #[test]
    fn test_additional_string_functions() {
        assert!(get_function_mapping("startswith").is_some());
        assert!(get_function_mapping("endswith").is_some());
        assert!(get_function_mapping("contains").is_some());
        assert!(get_function_mapping("normalize").is_some());
        assert!(get_function_mapping("valuetype").is_some());

        let mapping = get_function_mapping("startswith")
            .expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "startsWith");
    }

    #[test]
    fn test_core_aggregation_functions() {
        assert!(get_function_mapping("avg").is_some());
        assert!(get_function_mapping("sum").is_some());
        assert!(get_function_mapping("min").is_some());
        assert!(get_function_mapping("max").is_some());
        assert!(get_function_mapping("count").is_some());

        let mapping =
            get_function_mapping("avg").expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "avg");
    }

    #[test]
    fn test_list_predicate_functions() {
        assert!(get_function_mapping("all").is_some());
        assert!(get_function_mapping("any").is_some());
        assert!(get_function_mapping("none").is_some());
        assert!(get_function_mapping("single").is_some());
        assert!(get_function_mapping("isempty").is_some());

        let mapping =
            get_function_mapping("any").expect("get_function_mapping failed for function in test");
        assert_eq!(mapping.clickhouse_name, "arrayExists");
    }

    #[test]
    fn test_total_function_count() {
        // Count total functions in registry
        let test_functions = [
            // Original functions (25)
            "datetime",
            "date",
            "timestamp",
            "toupper",
            "tolower",
            "trim",
            "substring",
            "size",
            "split",
            "replace",
            "reverse",
            "left",
            "right",
            "abs",
            "ceil",
            "floor",
            "round",
            "sqrt",
            "rand",
            "sign",
            "head",
            "tail",
            "last",
            "range",
            "tointeger",
            "tofloat",
            "tostring",
            "toboolean",
            "collect",
            // Trig/math (13)
            "sin",
            "cos",
            "tan",
            "asin",
            "acos",
            "atan",
            "atan2",
            "exp",
            "log",
            "log10",
            "pi",
            "e",
            "pow",
            // String (2)
            "ltrim",
            "rtrim",
            // Aggregation (4)
            "stdev",
            "stdevp",
            "percentilecont",
            "percentiledisc",
            // Predicate (2)
            "coalesce",
            "nullif",
            // Map (1)
            "keys",
            // Vector/similarity (4)
            "gds.similarity.cosine",
            "gds.similarity.euclidean",
            "gds.similarity.euclideandistance",
            "vector.similarity.cosine",
            // List predicates (5)
            "all",
            "any",
            "none",
            "single",
            "isempty",
            // Temporal extraction (12)
            "localdatetime",
            "localtime",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "dayofweek",
            "dayofyear",
            "quarter",
            "week",
            // Additional string (5)
            "startswith",
            "endswith",
            "contains",
            "normalize",
            "valuetype",
            // Core aggregation (5)
            "avg",
            "sum",
            "min",
            "max",
            "count",
        ];

        let mut count = 0;
        for func in test_functions.iter() {
            if get_function_mapping(func).is_some() {
                count += 1;
            } else {
                eprintln!("Missing function: {}", func);
            }
        }

        // Should have 73+ functions now
        assert!(count >= 70, "Expected at least 70 functions, got {}", count);
    }
}
