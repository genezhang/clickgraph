/// Neo4j Function Translator
/// 
/// Translates Neo4j function calls to ClickHouse SQL equivalents

use crate::query_planner::logical_expr::ScalarFnCall;
use super::function_registry::get_function_mapping;
use super::to_sql::ToSql;
use super::errors::ClickhouseQueryGeneratorError;

/// Translate a Neo4j scalar function call to ClickHouse SQL
pub fn translate_scalar_function(fn_call: &ScalarFnCall) -> Result<String, ClickhouseQueryGeneratorError> {
    let fn_name_lower = fn_call.name.to_lowercase();
    
    // Look up function mapping
    match get_function_mapping(&fn_name_lower) {
        Some(mapping) => {
            // Convert arguments to SQL
            let args_sql: Result<Vec<String>, _> = fn_call.args
                .iter()
                .map(|e| e.to_sql())
                .collect();
            
            let args_sql = args_sql.map_err(|e| {
                ClickhouseQueryGeneratorError::SchemaError(
                    format!("Failed to convert function arguments to SQL: {}", e)
                )
            })?;
            
            // Apply argument transformation if provided
            let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                transform_fn(&args_sql)
            } else {
                args_sql
            };
            
            // Generate ClickHouse function call
            Ok(format!("{}({})", mapping.clickhouse_name, transformed_args.join(", ")))
        }
        None => {
            // Function not mapped - try direct passthrough with warning
            log::warn!(
                "Neo4j function '{}' is not mapped to ClickHouse. Attempting direct passthrough. \
                 This may fail if ClickHouse doesn't support this function name.",
                fn_call.name
            );
            
            // Convert arguments and attempt passthrough
            let args_sql: Result<Vec<String>, _> = fn_call.args
                .iter()
                .map(|e| e.to_sql())
                .collect();
            
            let args_sql = args_sql.map_err(|e| {
                ClickhouseQueryGeneratorError::SchemaError(
                    format!("Failed to convert function arguments to SQL: {}", e)
                )
            })?;
            
            Ok(format!("{}({})", fn_call.name, args_sql.join(", ")))
        }
    }
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
        "datetime", "date", "timestamp",
        // String
        "toUpper", "toLower", "trim", "substring", "size", 
        "split", "replace", "reverse", "left", "right",
        // Math
        "abs", "ceil", "floor", "round", "sqrt", "rand", "sign",
        // List
        "head", "tail", "last", "range",
        // Type Conversion
        "toInteger", "toFloat", "toString", "toBoolean",
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
}
