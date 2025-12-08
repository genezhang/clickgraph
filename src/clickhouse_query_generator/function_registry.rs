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

        // datetime() -> parseDateTime64BestEffort(arg, 3, 'UTC')
        m.insert("datetime", FunctionMapping {
            neo4j_name: "datetime",
            clickhouse_name: "parseDateTime64BestEffort",
            arg_transform: Some(|args| {
                if args.is_empty() {
                    // datetime() with no args returns current timestamp
                    vec!["now64(3)".to_string()]
                } else {
                    // datetime(string) parses ISO8601
                    vec![args[0].clone(), "3".to_string()]
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
    fn test_total_function_count() {
        // Count total functions in registry
        let test_functions = [
            // Original functions (25)
            "datetime", "date", "timestamp",
            "toupper", "tolower", "trim", "substring", "size", "split", "replace", "reverse", "left", "right",
            "abs", "ceil", "floor", "round", "sqrt", "rand", "sign",
            "head", "tail", "last", "range",
            "tointeger", "tofloat", "tostring", "toboolean",
            "collect",
            // New functions (18)
            "sin", "cos", "tan", "asin", "acos", "atan", "atan2",
            "exp", "log", "log10", "pi", "e", "pow",
            "ltrim", "rtrim",
            "stdev", "stdevp", "percentilecont", "percentiledisc",
            "coalesce", "nullif",
            "keys",
        ];
        
        let mut count = 0;
        for func in test_functions.iter() {
            if get_function_mapping(func).is_some() {
                count += 1;
            }
        }
        
        // Should have at least 40 functions now
        assert!(count >= 40, "Expected at least 40 functions, got {}", count);
    }
}
