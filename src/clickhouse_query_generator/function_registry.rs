/// Neo4j to ClickHouse Function Registry
///
/// Maps Neo4j function names to ClickHouse equivalents with optional argument transformations.
use std::collections::HashMap;

/// Function mapping entry
#[derive(Clone)]
pub struct FunctionMapping {
    /// Neo4j function name (lowercase for lookup)
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
}
