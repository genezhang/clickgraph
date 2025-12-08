use super::errors::ClickhouseQueryGeneratorError;
use super::function_registry::get_function_mapping;
use super::to_sql::ToSql;
/// Neo4j Function Translator
///
/// Translates Neo4j function calls to ClickHouse SQL equivalents
use crate::query_planner::logical_expr::ScalarFnCall;

/// Prefix for ClickHouse pass-through functions
/// Usage: ch::functionName(args) -> functionName(args) passed directly to ClickHouse
const CH_PASSTHROUGH_PREFIX: &str = "ch::";

/// Translate a Neo4j scalar function call to ClickHouse SQL
pub fn translate_scalar_function(
    fn_call: &ScalarFnCall,
) -> Result<String, ClickhouseQueryGeneratorError> {
    let fn_name = &fn_call.name;
    
    // Check for ClickHouse pass-through prefix (ch::)
    if fn_name.starts_with(CH_PASSTHROUGH_PREFIX) {
        return translate_ch_passthrough(fn_call);
    }
    
    let fn_name_lower = fn_name.to_lowercase();

    // Look up function mapping
    match get_function_mapping(&fn_name_lower) {
        Some(mapping) => {
            // Convert arguments to SQL
            let args_sql: Result<Vec<String>, _> =
                fn_call.args.iter().map(|e| e.to_sql()).collect();

            let args_sql = args_sql.map_err(|e| {
                ClickhouseQueryGeneratorError::SchemaError(format!(
                    "Failed to convert function arguments to SQL: {}",
                    e
                ))
            })?;

            // Apply argument transformation if provided
            let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                transform_fn(&args_sql)
            } else {
                args_sql
            };

            // Generate ClickHouse function call
            Ok(format!(
                "{}({})",
                mapping.clickhouse_name,
                transformed_args.join(", ")
            ))
        }
        None => {
            // Function not mapped - try direct passthrough with warning
            log::warn!(
                "Neo4j function '{}' is not mapped to ClickHouse. Attempting direct passthrough. \
                 This may fail if ClickHouse doesn't support this function name.",
                fn_call.name
            );

            // Convert arguments and attempt passthrough
            let args_sql: Result<Vec<String>, _> =
                fn_call.args.iter().map(|e| e.to_sql()).collect();

            let args_sql = args_sql.map_err(|e| {
                ClickhouseQueryGeneratorError::SchemaError(format!(
                    "Failed to convert function arguments to SQL: {}",
                    e
                ))
            })?;

            Ok(format!("{}({})", fn_call.name, args_sql.join(", ")))
        }
    }
}

/// Translate a ClickHouse pass-through function (ch:: prefix)
/// 
/// The ch:: prefix allows direct access to any ClickHouse function without
/// requiring a Neo4j mapping. Arguments still undergo property mapping and
/// parameter substitution.
/// 
/// # Examples
/// ```cypher
/// // Scalar functions
/// RETURN ch::cityHash64(u.email) AS hash
/// RETURN ch::JSONExtractString(u.metadata, 'field') AS field
/// 
/// // URL functions
/// RETURN ch::domain(u.url) AS domain
/// 
/// // IP functions  
/// RETURN ch::IPv4NumToString(u.ip) AS ip_str
/// 
/// // Geo functions
/// RETURN ch::greatCircleDistance(lat1, lon1, lat2, lon2) AS distance
/// ```
fn translate_ch_passthrough(
    fn_call: &ScalarFnCall,
) -> Result<String, ClickhouseQueryGeneratorError> {
    // Strip the ch:: prefix to get the raw ClickHouse function name
    let ch_fn_name = &fn_call.name[CH_PASSTHROUGH_PREFIX.len()..];
    
    if ch_fn_name.is_empty() {
        return Err(ClickhouseQueryGeneratorError::SchemaError(
            "ch:: prefix requires a function name (e.g., ch::cityHash64)".to_string()
        ));
    }
    
    // Convert arguments to SQL (this preserves property mapping)
    let args_sql: Result<Vec<String>, _> =
        fn_call.args.iter().map(|e| e.to_sql()).collect();
    
    let args_sql = args_sql.map_err(|e| {
        ClickhouseQueryGeneratorError::SchemaError(format!(
            "Failed to convert ch::{} arguments to SQL: {}",
            ch_fn_name, e
        ))
    })?;
    
    log::debug!(
        "ClickHouse pass-through: ch::{}({}) -> {}({})",
        ch_fn_name,
        fn_call.args.iter().map(|a| format!("{:?}", a)).collect::<Vec<_>>().join(", "),
        ch_fn_name,
        args_sql.join(", ")
    );
    
    // Generate ClickHouse function call directly
    Ok(format!("{}({})", ch_fn_name, args_sql.join(", ")))
}

/// Check if a function uses ClickHouse pass-through (ch:: prefix)
pub fn is_ch_passthrough(fn_name: &str) -> bool {
    fn_name.starts_with(CH_PASSTHROUGH_PREFIX)
}

/// Check if a function is supported (has a mapping)
pub fn is_function_supported(fn_name: &str) -> bool {
    get_function_mapping(fn_name).is_some()
}

/// Get list of all supported Neo4j functions
pub fn get_supported_functions() -> Vec<&'static str> {
    // This would need to be updated when we add lazy_static iteration
    // For now, return a static list
    vec![
        // DateTime
        "datetime",
        "date",
        "timestamp",
        // String
        "toUpper",
        "toLower",
        "trim",
        "substring",
        "size",
        "split",
        "replace",
        "reverse",
        "left",
        "right",
        // Math
        "abs",
        "ceil",
        "floor",
        "round",
        "sqrt",
        "rand",
        "sign",
        // List
        "head",
        "tail",
        "last",
        "range",
        // Type Conversion
        "toInteger",
        "toFloat",
        "toString",
        "toBoolean",
        // Aggregation
        "collect",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Literal, LogicalExpr};

    #[test]
    fn test_translate_simple_function() {
        // toUpper('hello') -> upper('hello')
        let fn_call = ScalarFnCall {
            name: "toUpper".to_string(),
            args: vec![LogicalExpr::Literal(Literal::String("hello".to_string()))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "upper('hello')");
    }

    #[test]
    fn test_translate_math_function() {
        // abs(-5) -> abs(-5)
        let fn_call = ScalarFnCall {
            name: "abs".to_string(),
            args: vec![LogicalExpr::Literal(Literal::Integer(-5))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "abs(-5)");
    }

    #[test]
    fn test_translate_function_with_transformation() {
        // left('hello', 3) -> substring('hello', 1, 3)
        let fn_call = ScalarFnCall {
            name: "left".to_string(),
            args: vec![
                LogicalExpr::Literal(Literal::String("hello".to_string())),
                LogicalExpr::Literal(Literal::Integer(3)),
            ],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "substring('hello', 1, 3)");
    }

    #[test]
    fn test_unsupported_function_passthrough() {
        // unknownFunc(arg) -> unknownFunc(arg) with warning
        let fn_call = ScalarFnCall {
            name: "unknownFunc".to_string(),
            args: vec![LogicalExpr::Literal(Literal::Integer(42))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "unknownFunc(42)");
    }

    #[test]
    fn test_is_function_supported() {
        assert!(is_function_supported("toUpper"));
        assert!(is_function_supported("TOUPPER")); // Case insensitive
        assert!(is_function_supported("abs"));
        assert!(!is_function_supported("unknownFunc"));
    }

    #[test]
    fn test_get_supported_functions() {
        let supported = get_supported_functions();
        assert!(supported.contains(&"toUpper"));
        assert!(supported.contains(&"abs"));
        assert!(supported.contains(&"datetime"));
        assert!(supported.len() >= 20); // Should have 20+ functions
    }

    // ===== ClickHouse Pass-through Tests =====

    #[test]
    fn test_ch_passthrough_simple() {
        // ch::cityHash64('test') -> cityHash64('test')
        let fn_call = ScalarFnCall {
            name: "ch::cityHash64".to_string(),
            args: vec![LogicalExpr::Literal(Literal::String("test".to_string()))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "cityHash64('test')");
    }

    #[test]
    fn test_ch_passthrough_multiple_args() {
        // ch::substring('hello', 2, 3) -> substring('hello', 2, 3)
        let fn_call = ScalarFnCall {
            name: "ch::substring".to_string(),
            args: vec![
                LogicalExpr::Literal(Literal::String("hello".to_string())),
                LogicalExpr::Literal(Literal::Integer(2)),
                LogicalExpr::Literal(Literal::Integer(3)),
            ],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "substring('hello', 2, 3)");
    }

    #[test]
    fn test_ch_passthrough_json_function() {
        // ch::JSONExtractString(data, 'field') -> JSONExtractString(data, 'field')
        let fn_call = ScalarFnCall {
            name: "ch::JSONExtractString".to_string(),
            args: vec![
                LogicalExpr::Literal(Literal::String(r#"{"name":"Alice"}"#.to_string())),
                LogicalExpr::Literal(Literal::String("name".to_string())),
            ],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, r#"JSONExtractString('{"name":"Alice"}', 'name')"#);
    }

    #[test]
    fn test_ch_passthrough_no_args() {
        // ch::now() -> now()
        let fn_call = ScalarFnCall {
            name: "ch::now".to_string(),
            args: vec![],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "now()");
    }

    #[test]
    fn test_ch_passthrough_empty_name_error() {
        // ch:: (empty) -> error
        let fn_call = ScalarFnCall {
            name: "ch::".to_string(),
            args: vec![],
        };

        let result = translate_scalar_function(&fn_call);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a function name"));
    }

    #[test]
    fn test_is_ch_passthrough() {
        assert!(is_ch_passthrough("ch::cityHash64"));
        assert!(is_ch_passthrough("ch::JSONExtract"));
        assert!(!is_ch_passthrough("cityHash64"));
        assert!(!is_ch_passthrough("toUpper"));
        assert!(!is_ch_passthrough("CH::test")); // Case sensitive
    }
}
