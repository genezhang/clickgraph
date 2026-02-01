//! RETURN clause evaluator for procedure results
//!
//! This module handles evaluation of RETURN clauses when they appear after procedure calls.
//! Example: `CALL db.labels() YIELD label RETURN {name:'labels', data:COLLECT(label)} AS result`
//!
//! The evaluator:
//! 1. Takes raw procedure results: [{label: "User"}, {label: "Post"}, ...]
//! 2. Evaluates RETURN expressions (COLLECT, maps, arrays, etc.)
//! 3. Returns transformed results: [{result: {name: "labels", data: ["User", "Post", ...]}}]

use crate::open_cypher_parser::ast::{Expression, FunctionCall, Literal, ReturnClause, ReturnItem};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Result type for expression evaluation
type EvalResult = Result<JsonValue, String>;

/// Context for expression evaluation
/// Contains the current record being processed
#[derive(Debug, Clone)]
pub struct EvalContext {
    /// Current record (variable name -> value)
    pub variables: HashMap<String, JsonValue>,
}

impl EvalContext {
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    pub fn with_variables(variables: HashMap<String, JsonValue>) -> Self {
        Self { variables }
    }

    pub fn set(&mut self, name: String, value: JsonValue) {
        self.variables.insert(name, value);
    }

    pub fn get(&self, name: &str) -> Option<&JsonValue> {
        self.variables.get(name)
    }
}

/// Apply RETURN clause transformations to procedure results
///
/// # Arguments
/// * `results` - Raw procedure results (vector of records)
/// * `return_clause` - RETURN clause AST to evaluate
///
/// # Returns
/// * Transformed results according to RETURN clause
pub fn apply_return_clause(
    results: Vec<HashMap<String, JsonValue>>,
    return_clause: &ReturnClause,
) -> Result<Vec<HashMap<String, JsonValue>>, String> {
    // Check if any return item uses aggregation
    let has_aggregation = return_clause
        .return_items
        .iter()
        .any(|item| has_aggregation_in_expr(&item.expression));

    if has_aggregation {
        // Apply aggregations: all results -> single aggregated record
        apply_aggregated_return(&results, return_clause)
    } else {
        // Apply non-aggregated return: transform each record independently
        apply_row_by_row_return(&results, return_clause)
    }
}

/// Check if an expression contains aggregation functions
fn has_aggregation_in_expr(expr: &Expression) -> bool {
    match expr {
        Expression::FunctionCallExp(func) => {
            let func_name_upper = func.name.to_uppercase();
            // Check if it's an aggregation function
            if matches!(
                func_name_upper.as_str(),
                "COLLECT" | "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            ) {
                return true;
            }
            // Check arguments recursively
            func.args.iter().any(has_aggregation_in_expr)
        }
        Expression::MapLiteral(pairs) => pairs.iter().any(|(_, expr)| has_aggregation_in_expr(expr)),
        Expression::List(exprs) => exprs.iter().any(has_aggregation_in_expr),
        Expression::PropertyAccessExp(_prop_access) => {
            // PropertyAccess has base and key as strings, not expressions
            false
        }
        Expression::OperatorApplicationExp(op) => {
            // Check all operands
            op.operands.iter().any(has_aggregation_in_expr)
        }
        Expression::ArraySlicing { array, from, to } => {
            has_aggregation_in_expr(array)
                || from.as_ref().map_or(false, |e| has_aggregation_in_expr(e))
                || to.as_ref().map_or(false, |e| has_aggregation_in_expr(e))
        }
        Expression::ArraySubscript { array, index } => {
            has_aggregation_in_expr(array) || has_aggregation_in_expr(index)
        }
        _ => false,
    }
}

/// Apply aggregated RETURN: all records -> single record with aggregated values
fn apply_aggregated_return(
    results: &[HashMap<String, JsonValue>],
    return_clause: &ReturnClause,
) -> Result<Vec<HashMap<String, JsonValue>>, String> {
    let mut output_record = HashMap::new();

    // Evaluate each return item with access to all records
    for return_item in &return_clause.return_items {
        // Evaluate the expression across all records
        let value = evaluate_aggregation_expr(&return_item.expression, results)?;

        // Determine output field name
        let field_name = return_item
            .alias
            .map(|s| s.to_string())
            .or_else(|| return_item.original_text.map(|s| s.to_string()))
            .unwrap_or_else(|| "?column?".to_string());

        output_record.insert(field_name, value);
    }

    // Return single aggregated record
    Ok(vec![output_record])
}

/// Apply non-aggregated RETURN: transform each record independently
fn apply_row_by_row_return(
    results: &[HashMap<String, JsonValue>],
    return_clause: &ReturnClause,
) -> Result<Vec<HashMap<String, JsonValue>>, String> {
    let mut output_results = Vec::new();

    for input_record in results {
        let mut output_record = HashMap::new();

        // Create context with current record's variables
        let context = EvalContext::with_variables(input_record.clone());

        // Evaluate each return item
        for return_item in &return_clause.return_items {
            let value = evaluate_expr(&return_item.expression, &context)?;

            // Determine output field name
            let field_name = return_item
                .alias
                .map(|s| s.to_string())
                .or_else(|| return_item.original_text.map(|s| s.to_string()))
                .unwrap_or_else(|| "?column?".to_string());

            output_record.insert(field_name, value);
        }

        output_results.push(output_record);
    }

    Ok(output_results)
}

/// Evaluate an expression with aggregation (has access to all records)
fn evaluate_aggregation_expr(
    expr: &Expression,
    all_records: &[HashMap<String, JsonValue>],
) -> EvalResult {
    match expr {
        Expression::FunctionCallExp(func) => {
            let func_name_upper = func.name.to_uppercase();

            match func_name_upper.as_str() {
                "COLLECT" => {
                    if func.args.len() != 1 {
                        return Err(format!(
                            "COLLECT expects exactly 1 argument, got {}",
                            func.args.len()
                        ));
                    }

                    // Collect values from all records
                    let mut collected = Vec::new();
                    for record in all_records {
                        let context = EvalContext::with_variables(record.clone());
                        let value = evaluate_expr(&func.args[0], &context)?;
                        collected.push(value);
                    }

                    Ok(JsonValue::Array(collected))
                }
                "COUNT" => {
                    // COUNT(*) or COUNT(expr)
                    Ok(JsonValue::Number(all_records.len().into()))
                }
                _ => {
                    // Non-aggregation function in aggregation context
                    // Evaluate it as if we have access to first record (for constants/literals)
                    if all_records.is_empty() {
                        evaluate_expr(expr, &EvalContext::new())
                    } else {
                        let context = EvalContext::with_variables(all_records[0].clone());
                        evaluate_expr(expr, &context)
                    }
                }
            }
        }
        Expression::MapLiteral(pairs) => {
            // Evaluate each value in the map
            let mut map = serde_json::Map::new();
            for (key, value_expr) in pairs {
                let value = evaluate_aggregation_expr(value_expr, all_records)?;
                map.insert(key.to_string(), value);
            }
            Ok(JsonValue::Object(map))
        }
        Expression::ArraySlicing { array, from, to } => {
            // First evaluate the array expression (which might be aggregation)
            let array_value = evaluate_aggregation_expr(array, all_records)?;

            // Then apply slicing
            apply_array_slicing(array_value, from.as_deref(), to.as_deref(), all_records)
        }
        _ => {
            // For other expressions, evaluate in context of first record (or empty context)
            if all_records.is_empty() {
                evaluate_expr(expr, &EvalContext::new())
            } else {
                let context = EvalContext::with_variables(all_records[0].clone());
                evaluate_expr(expr, &context)
            }
        }
    }
}

/// Evaluate a non-aggregation expression with a single record context
fn evaluate_expr(expr: &Expression, context: &EvalContext) -> EvalResult {
    match expr {
        Expression::Literal(lit) => evaluate_literal(lit),
        Expression::Variable(var_name) => context
            .get(var_name)
            .cloned()
            .ok_or_else(|| format!("Variable not found: {}", var_name)),
        Expression::MapLiteral(pairs) => {
            let mut map = serde_json::Map::new();
            for (key, value_expr) in pairs {
                let value = evaluate_expr(value_expr, context)?;
                map.insert(key.to_string(), value);
            }
            Ok(JsonValue::Object(map))
        }
        Expression::List(exprs) => {
            let mut list = Vec::new();
            for expr in exprs {
                list.push(evaluate_expr(expr, context)?);
            }
            Ok(JsonValue::Array(list))
        }
        Expression::PropertyAccessExp(prop_access) => {
            // PropertyAccess is base.key where both are strings
            // Look up base variable, then access its property
            let base_value = context
                .get(prop_access.base)
                .cloned()
                .ok_or_else(|| format!("Variable not found: {}", prop_access.base))?;

            // Extract property from object
            if let JsonValue::Object(obj) = base_value {
                return obj
                    .get(prop_access.key)
                    .cloned()
                    .ok_or_else(|| format!("Property not found: {}", prop_access.key));
            }

            Err(format!(
                "Cannot access property {} on non-object", prop_access.key
            ))
        }
        Expression::FunctionCallExp(func) => {
            Err(format!("Function calls in non-aggregation context not yet supported: {}", func.name))
        }
        _ => Err(format!("Expression type not yet supported: {:?}", expr)),
    }
}

/// Evaluate a literal expression
fn evaluate_literal(lit: &Literal) -> EvalResult {
    match lit {
        Literal::String(s) => Ok(JsonValue::String(s.to_string())),
        Literal::Integer(i) => Ok(JsonValue::Number((*i).into())),
        Literal::Float(f) => {
            let num = serde_json::Number::from_f64(*f)
                .ok_or_else(|| format!("Invalid float: {}", f))?;
            Ok(JsonValue::Number(num))
        }
        Literal::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Literal::Null => Ok(JsonValue::Null),
        _ => Err(format!("Unsupported literal type: {:?}", lit)),
    }
}

/// Apply array slicing [from..to]
fn apply_array_slicing(
    array_value: JsonValue,
    from: Option<&Expression>,
    to: Option<&Expression>,
    all_records: &[HashMap<String, JsonValue>],
) -> EvalResult {
    let JsonValue::Array(arr) = array_value else {
        return Err("Array slicing can only be applied to arrays".to_string());
    };

    // Evaluate from and to bounds
    let from_idx = if let Some(from_expr) = from {
        let from_val = evaluate_aggregation_expr(from_expr, all_records)?;
        if let JsonValue::Number(n) = from_val {
            n.as_u64().unwrap_or(0) as usize
        } else {
            return Err("Array slice 'from' index must be a number".to_string());
        }
    } else {
        0 // Default: start from beginning
    };

    let to_idx = if let Some(to_expr) = to {
        let to_val = evaluate_aggregation_expr(to_expr, all_records)?;
        if let JsonValue::Number(n) = to_val {
            n.as_u64().unwrap_or(arr.len() as u64) as usize
        } else {
            return Err("Array slice 'to' index must be a number".to_string());
        }
    } else {
        arr.len() // Default: go to end
    };

    // Apply slicing (Cypher uses inclusive bounds, 0-based indexing for slicing)
    let from_clamped = from_idx.min(arr.len());
    let to_clamped = to_idx.min(arr.len());

    if from_clamped <= to_clamped {
        Ok(JsonValue::Array(
            arr[from_clamped..to_clamped].to_vec(),
        ))
    } else {
        Ok(JsonValue::Array(vec![]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser;

    #[test]
    fn test_simple_variable_return() {
        // RETURN label
        let query = "RETURN label";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        
        if let crate::open_cypher_parser::ast::CypherStatement::Query { query, .. } = stmt {
            let return_clause = query.return_clause.as_ref().unwrap();
            
            let input = vec![
                HashMap::from([("label".to_string(), JsonValue::String("User".to_string()))]),
                HashMap::from([("label".to_string(), JsonValue::String("Post".to_string()))]),
            ];
            
            let result = apply_return_clause(input, return_clause).unwrap();
            assert_eq!(result.len(), 2);
            assert_eq!(result[0].get("label"), Some(&JsonValue::String("User".to_string())));
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_collect_aggregation() {
        // RETURN COLLECT(label)
        let query = "RETURN COLLECT(label) AS labels";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        
        if let crate::open_cypher_parser::ast::CypherStatement::Query { query, .. } = stmt {
            let return_clause = query.return_clause.as_ref().unwrap();
            
            let input = vec![
                HashMap::from([("label".to_string(), JsonValue::String("User".to_string()))]),
                HashMap::from([("label".to_string(), JsonValue::String("Post".to_string()))]),
            ];
            
            let result = apply_return_clause(input, return_clause).unwrap();
            assert_eq!(result.len(), 1); // Aggregation returns single record
            
            let labels_value = result[0].get("labels").unwrap();
            assert!(labels_value.is_array());
            assert_eq!(labels_value.as_array().unwrap().len(), 2);
        } else {
            panic!("Expected Query statement");
        }
    }

    #[test]
    fn test_map_with_collect() {
        // RETURN {name:'labels', data:COLLECT(label)}
        let query = "RETURN {name:'labels', data:COLLECT(label)} AS result";
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(query).unwrap();
        
        if let crate::open_cypher_parser::ast::CypherStatement::Query { query, .. } = stmt {
            let return_clause = query.return_clause.as_ref().unwrap();
            
            let input = vec![
                HashMap::from([("label".to_string(), JsonValue::String("User".to_string()))]),
                HashMap::from([("label".to_string(), JsonValue::String("Post".to_string()))]),
            ];
            
            let result = apply_return_clause(input, return_clause).unwrap();
            assert_eq!(result.len(), 1);
            
            let result_value = result[0].get("result").unwrap();
            assert!(result_value.is_object());
            
            let obj = result_value.as_object().unwrap();
            assert_eq!(obj.get("name"), Some(&JsonValue::String("labels".to_string())));
            assert!(obj.get("data").unwrap().is_array());
            assert_eq!(obj.get("data").unwrap().as_array().unwrap().len(), 2);
        } else {
            panic!("Expected Query statement");
        }
    }
}
