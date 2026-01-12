/// Neo4j to ClickHouse Function Registry
///
/// Maps Neo4j function names to ClickHouse equivalents with optional argument transformations.
use std::collections::HashMap;

/// Function mapping entry
#[derive(Clone)]
pub struct FunctionMapping {
    /// Neo4j function name (lowercase for lookup)
    #[allow(dead_code)]
    pub neo4j_name: &'static str,
    /// ClickHouse function name
    pub clickhouse_name: &'static str,
    /// Optional argument transformation function
    /// Takes SQL string args, returns transformed SQL string args
    pub arg_transform: Option<fn(&[String]) -> Vec<String>>,
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

        // toUnixTimestampMillis() -> toUnixTimestamp64Milli(parseDateTime64BestEffort(arg))
        // Converts datetime string to Unix timestamp in milliseconds (Int64)
        // For schemas that store dates as Int64 milliseconds (e.g., LDBC)
        m.insert("tounixtimestampmillis", FunctionMapping {
            neo4j_name: "toUnixTimestampMillis",
            clickhouse_name: "toUnixTimestamp64Milli",
            arg_transform: Some(|args| {
                if args.is_empty() {
                    // No args: return current time in milliseconds
                    vec!["now64(3)".to_string()]
                } else {
                    // Parse string and convert to milliseconds since epoch
                    vec![format!("parseDateTime64BestEffort({}, 3)", args[0])]
                }
            }),
        });

        // date() -> toDate(arg) or today() if no args
        m.insert("date", FunctionMapping {
            neo4j_name: "date",
            clickhouse_name: "toDate",
            arg_transform: Some(|args| {
                if args.is_empty() {
                    vec!["today()".to_string()]
                } else {
                    vec![args[0].clone()]
                }
            }),
        });

        // timestamp() -> toUnixTimestamp(arg) or toUnixTimestamp(now()) if no args
        m.insert("timestamp", FunctionMapping {
            neo4j_name: "timestamp",
            clickhouse_name: "toUnixTimestamp",
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
            arg_transform: None,
        });

        // toLower() -> lower()
        m.insert("tolower", FunctionMapping {
            neo4j_name: "toLower",
            clickhouse_name: "lower",
            arg_transform: None,
        });

        // trim() -> trim(BOTH ' ' FROM arg) - ClickHouse trim removes all whitespace by default
        m.insert("trim", FunctionMapping {
            neo4j_name: "trim",
            clickhouse_name: "trim",
            arg_transform: Some(|args| {
                // ClickHouse: trim(BOTH str)
                vec![format!("BOTH {}", args[0])]
            }),
        });

        // substring(str, start [, length]) -> substring(str, start+1, length)
        // Note: Neo4j is 0-indexed, ClickHouse is 1-indexed
        m.insert("substring", FunctionMapping {
            neo4j_name: "substring",
            clickhouse_name: "substring",
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
            arg_transform: None,
        });

        // split(str, delimiter) -> splitByChar(delimiter, str) [ARGS SWAPPED!]
        m.insert("split", FunctionMapping {
            neo4j_name: "split",
            clickhouse_name: "splitByChar",
            arg_transform: Some(|args| {
                if args.len() >= 2 {
                    // Swap: split(str, delim) -> splitByChar(delim, str)
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
            arg_transform: None,
        });

        // reverse(str) -> reverse(str)
        m.insert("reverse", FunctionMapping {
            neo4j_name: "reverse",
            clickhouse_name: "reverse",
            arg_transform: None,
        });

        // left(str, length) -> substring(str, 1, length)
        m.insert("left", FunctionMapping {
            neo4j_name: "left",
            clickhouse_name: "substring",
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
            arg_transform: None,
        });

        // ceil() -> ceil() [1:1 mapping]
        m.insert("ceil", FunctionMapping {
            neo4j_name: "ceil",
            clickhouse_name: "ceil",
            arg_transform: None,
        });

        // floor() -> floor() [1:1 mapping]
        m.insert("floor", FunctionMapping {
            neo4j_name: "floor",
            clickhouse_name: "floor",
            arg_transform: None,
        });

        // round() -> round() [1:1 mapping]
        m.insert("round", FunctionMapping {
            neo4j_name: "round",
            clickhouse_name: "round",
            arg_transform: None,
        });

        // sqrt() -> sqrt() [1:1 mapping]
        m.insert("sqrt", FunctionMapping {
            neo4j_name: "sqrt",
            clickhouse_name: "sqrt",
            arg_transform: None,
        });

        // rand() -> rand() / 4294967295.0 (normalize to 0.0-1.0)
        m.insert("rand", FunctionMapping {
            neo4j_name: "rand",
            clickhouse_name: "rand",
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
            arg_transform: None,
        });

        // ===== LIST FUNCTIONS =====

        // head(list) -> arrayElement(list, 1) [first element]
        m.insert("head", FunctionMapping {
            neo4j_name: "head",
            clickhouse_name: "arrayElement",
            arg_transform: Some(|args| {
                if !args.is_empty() {
                    vec![args[0].clone(), "1".to_string()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // tail(list) -> arraySlice(list, 2) [all but first]
        m.insert("tail", FunctionMapping {
            neo4j_name: "tail",
            clickhouse_name: "arraySlice",
            arg_transform: Some(|args| {
                if !args.is_empty() {
                    vec![args[0].clone(), "2".to_string()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // last(list) -> arrayElement(list, -1) [last element]
        m.insert("last", FunctionMapping {
            neo4j_name: "last",
            clickhouse_name: "arrayElement",
            arg_transform: Some(|args| {
                if !args.is_empty() {
                    vec![args[0].clone(), "-1".to_string()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // range(start, end [, step]) -> range(start, end [, step])
        m.insert("range", FunctionMapping {
            neo4j_name: "range",
            clickhouse_name: "range",
            arg_transform: None,
        });

        // ===== TYPE CONVERSION FUNCTIONS =====

        // toInteger() -> toInt64()
        m.insert("tointeger", FunctionMapping {
            neo4j_name: "toInteger",
            clickhouse_name: "toInt64",
            arg_transform: None,
        });

        // toFloat() -> toFloat64()
        m.insert("tofloat", FunctionMapping {
            neo4j_name: "toFloat",
            clickhouse_name: "toFloat64",
            arg_transform: None,
        });

        // toString() -> toString()
        m.insert("tostring", FunctionMapping {
            neo4j_name: "toString",
            clickhouse_name: "toString",
            arg_transform: None,
        });

        // toBoolean() -> if(arg, 1, 0) - ClickHouse doesn't have native boolean conversion
        m.insert("toboolean", FunctionMapping {
            neo4j_name: "toBoolean",
            clickhouse_name: "if",
            arg_transform: Some(|args| {
                if !args.is_empty() {
                    vec![args[0].clone(), "1".to_string(), "0".to_string()]
                } else {
                    args.to_vec()
                }
            }),
        });

        // ===== AGGREGATION FUNCTIONS =====

        // collect() -> groupArray() [collect elements into array]
        m.insert("collect", FunctionMapping {
            neo4j_name: "collect",
            clickhouse_name: "groupArray",
            arg_transform: None,
        });

        // ===== TRIGONOMETRIC FUNCTIONS =====

        // sin() -> sin() [1:1 mapping]
        m.insert("sin", FunctionMapping {
            neo4j_name: "sin",
            clickhouse_name: "sin",
            arg_transform: None,
        });

        // cos() -> cos() [1:1 mapping]
        m.insert("cos", FunctionMapping {
            neo4j_name: "cos",
            clickhouse_name: "cos",
            arg_transform: None,
        });

        // tan() -> tan() [1:1 mapping]
        m.insert("tan", FunctionMapping {
            neo4j_name: "tan",
            clickhouse_name: "tan",
            arg_transform: None,
        });

        // asin() -> asin() [1:1 mapping]
        m.insert("asin", FunctionMapping {
            neo4j_name: "asin",
            clickhouse_name: "asin",
            arg_transform: None,
        });

        // acos() -> acos() [1:1 mapping]
        m.insert("acos", FunctionMapping {
            neo4j_name: "acos",
            clickhouse_name: "acos",
            arg_transform: None,
        });

        // atan() -> atan() [1:1 mapping]
        m.insert("atan", FunctionMapping {
            neo4j_name: "atan",
            clickhouse_name: "atan",
            arg_transform: None,
        });

        // atan2(y, x) -> atan2(y, x) [1:1 mapping]
        m.insert("atan2", FunctionMapping {
            neo4j_name: "atan2",
            clickhouse_name: "atan2",
            arg_transform: None,
        });

        // ===== ADDITIONAL MATH FUNCTIONS =====

        // exp() -> exp() [1:1 mapping]
        m.insert("exp", FunctionMapping {
            neo4j_name: "exp",
            clickhouse_name: "exp",
            arg_transform: None,
        });

        // log() -> log() [natural logarithm, 1:1 mapping]
        m.insert("log", FunctionMapping {
            neo4j_name: "log",
            clickhouse_name: "log",
            arg_transform: None,
        });

        // log10() -> log10() [1:1 mapping]
        m.insert("log10", FunctionMapping {
            neo4j_name: "log10",
            clickhouse_name: "log10",
            arg_transform: None,
        });

        // pi() -> pi() [1:1 mapping]
        m.insert("pi", FunctionMapping {
            neo4j_name: "pi",
            clickhouse_name: "pi",
            arg_transform: None,
        });

        // e() -> e() [1:1 mapping]
        m.insert("e", FunctionMapping {
            neo4j_name: "e",
            clickhouse_name: "e",
            arg_transform: None,
        });

        // pow(base, exp) / ^ -> pow(base, exp) [1:1 mapping]
        m.insert("pow", FunctionMapping {
            neo4j_name: "pow",
            clickhouse_name: "pow",
            arg_transform: None,
        });

        // ===== ADDITIONAL STRING FUNCTIONS =====

        // ltrim() -> trimLeft()
        m.insert("ltrim", FunctionMapping {
            neo4j_name: "lTrim",
            clickhouse_name: "trimLeft",
            arg_transform: None,
        });

        // rtrim() -> trimRight()
        m.insert("rtrim", FunctionMapping {
            neo4j_name: "rTrim",
            clickhouse_name: "trimRight",
            arg_transform: None,
        });

        // ===== ADDITIONAL AGGREGATION FUNCTIONS =====

        // stDev() -> stddevSamp() [sample standard deviation]
        m.insert("stdev", FunctionMapping {
            neo4j_name: "stDev",
            clickhouse_name: "stddevSamp",
            arg_transform: None,
        });

        // stDevP() -> stddevPop() [population standard deviation]
        m.insert("stdevp", FunctionMapping {
            neo4j_name: "stDevP",
            clickhouse_name: "stddevPop",
            arg_transform: None,
        });

        // percentileCont(percentile) -> quantile(percentile)
        // Note: Neo4j syntax is percentileCont(expr, percentile)
        // ClickHouse syntax is quantile(percentile)(expr) - parametric aggregate
        // We'll use simpler quantileExact for now which takes (expr)
        m.insert("percentilecont", FunctionMapping {
            neo4j_name: "percentileCont",
            clickhouse_name: "median",  // median = quantile(0.5), closest simple equivalent
            arg_transform: Some(|args| {
                // percentileCont(expr, 0.5) -> median(expr)
                // For other percentiles, user needs to use quantile directly
                if args.len() >= 1 {
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
            arg_transform: Some(|args| {
                if args.len() >= 1 {
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
            arg_transform: None,
        });

        // nullIf(a, b) -> nullIf(a, b) [1:1 mapping]
        m.insert("nullif", FunctionMapping {
            neo4j_name: "nullIf",
            clickhouse_name: "nullIf",
            arg_transform: None,
        });

        // ===== ADDITIONAL LIST FUNCTIONS =====

        // keys(map) -> mapKeys(map) - get keys from a map
        m.insert("keys", FunctionMapping {
            neo4j_name: "keys",
            clickhouse_name: "mapKeys",
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
            arg_transform: None,
        });

        // vector.similarity.cosine(v1, v2) -> 1 - cosineDistance(v1, v2)
        // Neo4j 5.x vector similarity function
        m.insert("vector.similarity.cosine", FunctionMapping {
            neo4j_name: "vector.similarity.cosine",
            clickhouse_name: "cosineDistance",
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
            arg_transform: None, // Requires special handling for predicate syntax
        });

        // any() -> arrayExists() - check if any element matches predicate
        m.insert("any", FunctionMapping {
            neo4j_name: "any",
            clickhouse_name: "arrayExists",
            arg_transform: None, // Requires special handling for predicate syntax
        });

        // none() -> NOT arrayExists() - check if no element matches predicate
        m.insert("none", FunctionMapping {
            neo4j_name: "none",
            clickhouse_name: "arrayExists",
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
            arg_transform: None, // Caller needs to add = 1
        });

        // isEmpty(list) -> empty(list) or length(list) = 0
        m.insert("isempty", FunctionMapping {
            neo4j_name: "isEmpty",
            clickhouse_name: "empty",
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

        // year(datetime) -> toYear(datetime)
        m.insert("year", FunctionMapping {
            neo4j_name: "year",
            clickhouse_name: "toYear",
            arg_transform: None,
        });

        // month(datetime) -> toMonth(datetime)
        m.insert("month", FunctionMapping {
            neo4j_name: "month",
            clickhouse_name: "toMonth",
            arg_transform: None,
        });

        // day(datetime) -> toDayOfMonth(datetime)
        m.insert("day", FunctionMapping {
            neo4j_name: "day",
            clickhouse_name: "toDayOfMonth",
            arg_transform: None,
        });

        // hour(datetime) -> toHour(datetime)
        m.insert("hour", FunctionMapping {
            neo4j_name: "hour",
            clickhouse_name: "toHour",
            arg_transform: None,
        });

        // minute(datetime) -> toMinute(datetime)
        m.insert("minute", FunctionMapping {
            neo4j_name: "minute",
            clickhouse_name: "toMinute",
            arg_transform: None,
        });

        // second(datetime) -> toSecond(datetime)
        m.insert("second", FunctionMapping {
            neo4j_name: "second",
            clickhouse_name: "toSecond",
            arg_transform: None,
        });

        // dayOfWeek(datetime) -> toDayOfWeek(datetime)
        m.insert("dayofweek", FunctionMapping {
            neo4j_name: "dayOfWeek",
            clickhouse_name: "toDayOfWeek",
            arg_transform: None,
        });

        // dayOfYear(datetime) -> toDayOfYear(datetime)
        m.insert("dayofyear", FunctionMapping {
            neo4j_name: "dayOfYear",
            clickhouse_name: "toDayOfYear",
            arg_transform: None,
        });

        // quarter(datetime) -> toQuarter(datetime)
        m.insert("quarter", FunctionMapping {
            neo4j_name: "quarter",
            clickhouse_name: "toQuarter",
            arg_transform: None,
        });

        // week(datetime) -> toISOWeek(datetime)
        m.insert("week", FunctionMapping {
            neo4j_name: "week",
            clickhouse_name: "toISOWeek",
            arg_transform: None,
        });

        // ===== ADDITIONAL STRING FUNCTIONS =====

        // startsWith(str, prefix) -> startsWith(str, prefix) [1:1]
        m.insert("startswith", FunctionMapping {
            neo4j_name: "startsWith",
            clickhouse_name: "startsWith",
            arg_transform: None,
        });

        // endsWith(str, suffix) -> endsWith(str, suffix) [1:1]
        m.insert("endswith", FunctionMapping {
            neo4j_name: "endsWith",
            clickhouse_name: "endsWith",
            arg_transform: None,
        });

        // contains(str, search) -> position(str, search) > 0 or like
        // ClickHouse has positionCaseInsensitive too
        m.insert("contains", FunctionMapping {
            neo4j_name: "contains",
            clickhouse_name: "position",
            arg_transform: Some(|args| {
                // contains(str, search) -> position(str, search) > 0
                // Caller needs to handle the > 0 comparison
                args.to_vec()
            }),
        });

        // normalize(str) -> normalizeUTF8NFC(str)
        m.insert("normalize", FunctionMapping {
            neo4j_name: "normalize",
            clickhouse_name: "normalizeUTF8NFC",
            arg_transform: None,
        });

        // valueType(value) - returns type name, no direct CH equivalent
        // ClickHouse: toTypeName(value)
        m.insert("valuetype", FunctionMapping {
            neo4j_name: "valueType",
            clickhouse_name: "toTypeName",
            arg_transform: None,
        });

        // ===== ADDITIONAL AGGREGATION FUNCTIONS =====

        // avg() -> avg() [1:1]
        m.insert("avg", FunctionMapping {
            neo4j_name: "avg",
            clickhouse_name: "avg",
            arg_transform: None,
        });

        // sum() -> sum() [1:1]
        m.insert("sum", FunctionMapping {
            neo4j_name: "sum",
            clickhouse_name: "sum",
            arg_transform: None,
        });

        // min() -> min() [1:1]
        m.insert("min", FunctionMapping {
            neo4j_name: "min",
            clickhouse_name: "min",
            arg_transform: None,
        });

        // max() -> max() [1:1]
        m.insert("max", FunctionMapping {
            neo4j_name: "max",
            clickhouse_name: "max",
            arg_transform: None,
        });

        // count() -> count() [1:1]
        m.insert("count", FunctionMapping {
            neo4j_name: "count",
            clickhouse_name: "count",
            arg_transform: None,
        });

        // ===== SPATIAL FUNCTIONS (basic) =====
        // Note: Full spatial support would require more extensive work

        // point.distance(p1, p2) -> geoDistance for lat/lon
        // Neo4j: point.distance(point({longitude: x1, latitude: y1}), point({longitude: x2, latitude: y2}))
        // ClickHouse: geoDistance(lon1, lat1, lon2, lat2)
        // Requires special handling to extract coordinates from point()

        // ===== ADDITIONAL TYPE FUNCTIONS =====

        // type(relationship) - handled specially in code, but add placeholder
        // id(node) - handled specially in code
        // labels(node) - handled specially in code

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

        let mapping = get_function_mapping("toUpper").unwrap();
        assert_eq!(mapping.clickhouse_name, "upper");
    }

    #[test]
    fn test_math_functions() {
        assert!(get_function_mapping("abs").is_some());
        assert!(get_function_mapping("sqrt").is_some());

        let mapping = get_function_mapping("ceil").unwrap();
        assert_eq!(mapping.clickhouse_name, "ceil");
    }

    #[test]
    fn test_datetime_functions() {
        assert!(get_function_mapping("datetime").is_some());
        assert!(get_function_mapping("date").is_some());

        let mapping = get_function_mapping("timestamp").unwrap();
        assert_eq!(mapping.clickhouse_name, "toUnixTimestamp");
    }

    #[test]
    fn test_arg_transformations() {
        let mapping = get_function_mapping("split").unwrap();
        assert!(mapping.arg_transform.is_some());

        // split(str, delim) -> splitByChar(delim, str)
        let transform = mapping.arg_transform.unwrap();
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
        let mapping = get_function_mapping("sin").unwrap();
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

        let mapping = get_function_mapping("exp").unwrap();
        assert_eq!(mapping.clickhouse_name, "exp");
    }

    #[test]
    fn test_trim_functions() {
        assert!(get_function_mapping("ltrim").is_some());
        assert!(get_function_mapping("rtrim").is_some());

        let mapping = get_function_mapping("ltrim").unwrap();
        assert_eq!(mapping.clickhouse_name, "trimLeft");

        let mapping = get_function_mapping("rtrim").unwrap();
        assert_eq!(mapping.clickhouse_name, "trimRight");
    }

    #[test]
    fn test_aggregation_functions() {
        assert!(get_function_mapping("stdev").is_some());
        assert!(get_function_mapping("stdevp").is_some());
        assert!(get_function_mapping("percentilecont").is_some());
        assert!(get_function_mapping("percentiledisc").is_some());

        let mapping = get_function_mapping("stdev").unwrap();
        assert_eq!(mapping.clickhouse_name, "stddevSamp");

        let mapping = get_function_mapping("stdevp").unwrap();
        assert_eq!(mapping.clickhouse_name, "stddevPop");
    }

    #[test]
    fn test_predicate_functions() {
        assert!(get_function_mapping("coalesce").is_some());
        assert!(get_function_mapping("nullif").is_some());

        let mapping = get_function_mapping("coalesce").unwrap();
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
        let mapping = get_function_mapping("gds.similarity.cosine").unwrap();
        assert!(mapping.arg_transform.is_some());
        let transform = mapping.arg_transform.unwrap();
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

        let mapping = get_function_mapping("year").unwrap();
        assert_eq!(mapping.clickhouse_name, "toYear");
    }

    #[test]
    fn test_additional_string_functions() {
        assert!(get_function_mapping("startswith").is_some());
        assert!(get_function_mapping("endswith").is_some());
        assert!(get_function_mapping("contains").is_some());
        assert!(get_function_mapping("normalize").is_some());
        assert!(get_function_mapping("valuetype").is_some());

        let mapping = get_function_mapping("startswith").unwrap();
        assert_eq!(mapping.clickhouse_name, "startsWith");
    }

    #[test]
    fn test_core_aggregation_functions() {
        assert!(get_function_mapping("avg").is_some());
        assert!(get_function_mapping("sum").is_some());
        assert!(get_function_mapping("min").is_some());
        assert!(get_function_mapping("max").is_some());
        assert!(get_function_mapping("count").is_some());

        let mapping = get_function_mapping("avg").unwrap();
        assert_eq!(mapping.clickhouse_name, "avg");
    }

    #[test]
    fn test_list_predicate_functions() {
        assert!(get_function_mapping("all").is_some());
        assert!(get_function_mapping("any").is_some());
        assert!(get_function_mapping("none").is_some());
        assert!(get_function_mapping("single").is_some());
        assert!(get_function_mapping("isempty").is_some());

        let mapping = get_function_mapping("any").unwrap();
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
