//! RETURN clause evaluator for procedure results
//!
//! This module handles evaluation of RETURN clauses when they appear after procedure calls.
//! Example: `CALL db.labels() YIELD label RETURN {name:'labels', data:COLLECT(label)} AS result`
//!
//! The evaluator:
//! 1. Takes raw procedure results: [{label: "User"}, {label: "Post"}, ...]
//! 2. Evaluates RETURN expressions (COLLECT, maps, arrays, etc.)
//! 3. Returns transformed results: [{result: {name: "labels", data: ["User", "Post", ...]}}]

use crate::open_cypher_parser::ast::{
    Expression, Literal, Operator, OperatorApplication, OrderByClause, OrerByOrder, ReturnClause,
    UnwindClause,
};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
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

impl Default for EvalContext {
    fn default() -> Self {
        Self::new()
    }
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
    let has_aggregation = return_aggregates(return_clause);

    if has_aggregation {
        // One record for all the rows: a value read outside an aggregate would
        // be a grouping key, which this does not evaluate.
        if return_clause
            .return_items
            .iter()
            .any(|item| reads_row_outside_aggregate(&item.expression))
        {
            return Err(
                "Not supported: a grouping key beside an aggregate after a procedure call"
                    .to_string(),
            );
        }
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
        Expression::MapLiteral(pairs) => {
            pairs.iter().any(|(_, expr)| has_aggregation_in_expr(expr))
        }
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
                || from.as_ref().is_some_and(|e| has_aggregation_in_expr(e))
                || to.as_ref().is_some_and(|e| has_aggregation_in_expr(e))
        }
        Expression::ArraySubscript { array, index } => {
            has_aggregation_in_expr(array) || has_aggregation_in_expr(index)
        }
        _ => false,
    }
}

/// Whether an expression reads the current row outside an aggregate call.
fn reads_row_outside_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Variable(_) | Expression::PropertyAccessExp(_) => true,
        Expression::FunctionCallExp(func) => {
            !matches!(
                func.name.to_uppercase().as_str(),
                "COLLECT" | "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            ) && func.args.iter().any(reads_row_outside_aggregate)
        }
        Expression::MapLiteral(pairs) => pairs.iter().any(|(_, e)| reads_row_outside_aggregate(e)),
        Expression::List(exprs) => exprs.iter().any(reads_row_outside_aggregate),
        Expression::OperatorApplicationExp(op) => {
            op.operands.iter().any(reads_row_outside_aggregate)
        }
        Expression::ArraySlicing { array, from, to } => {
            reads_row_outside_aggregate(array)
                || from.as_deref().is_some_and(reads_row_outside_aggregate)
                || to.as_deref().is_some_and(reads_row_outside_aggregate)
        }
        Expression::ArraySubscript { array, index } => {
            reads_row_outside_aggregate(array) || reads_row_outside_aggregate(index)
        }
        _ => false,
    }
}

/// The value of an aggregate's one argument for each record; a NULL is
/// skipped, as Cypher's aggregates skip it.
fn aggregate_arguments(
    name: &str,
    args: &[Expression],
    all_records: &[HashMap<String, JsonValue>],
) -> Result<Vec<JsonValue>, String> {
    let [arg] = args else {
        return Err(format!(
            "{name} expects exactly 1 argument, got {}",
            args.len()
        ));
    };
    let mut values = Vec::new();
    for record in all_records {
        let value = evaluate_expr(arg, &EvalContext::with_variables(record.clone()))?;
        if !value.is_null() {
            values.push(value);
        }
    }
    Ok(values)
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
                "COLLECT" => Ok(JsonValue::Array(aggregate_arguments(
                    "COLLECT",
                    &func.args,
                    all_records,
                )?)),
                "COUNT" => {
                    let count = match func.args.as_slice() {
                        [] | [Expression::Variable("*")] => all_records.len(),
                        args => aggregate_arguments("COUNT", args, all_records)?.len(),
                    };
                    Ok(JsonValue::Number(count.into()))
                }
                "SUM" | "AVG" | "MIN" | "MAX" => Err(format!(
                    "Not supported: {} after a procedure call",
                    func.name
                )),
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
                "Cannot access property {} on non-object",
                prop_access.key
            ))
        }
        Expression::OperatorApplicationExp(op) => evaluate_operator(op, context),
        Expression::FunctionCallExp(func) => Err(format!(
            "Function calls in non-aggregation context not yet supported: {}",
            func.name
        )),
        _ => Err(format!("Expression type not yet supported: {:?}", expr)),
    }
}

/// Keep the records a WHERE condition is true for (false and NULL drop one).
pub fn apply_where(
    results: Vec<HashMap<String, JsonValue>>,
    condition: &Expression,
) -> Result<Vec<HashMap<String, JsonValue>>, String> {
    let mut kept = Vec::new();
    for record in results {
        let context = EvalContext::with_variables(record.clone());
        match evaluate_expr(condition, &context)? {
            JsonValue::Bool(true) => kept.push(record),
            JsonValue::Bool(false) | JsonValue::Null => {}
            other => return Err(format!("WHERE expects a boolean, got {other}")),
        }
    }
    Ok(kept)
}

/// One record per element of the UNWIND's list for each record, the element
/// bound to its alias; none for an empty list or NULL; the record itself with
/// a value that is not a list.
pub fn apply_unwind(
    results: Vec<HashMap<String, JsonValue>>,
    unwind: &UnwindClause,
) -> Result<Vec<HashMap<String, JsonValue>>, String> {
    let mut out = Vec::new();
    for record in results {
        if record.contains_key(unwind.alias) {
            return Err(format!("Variable `{}` already declared", unwind.alias));
        }
        let context = EvalContext::with_variables(record.clone());
        let elements = match evaluate_expr(&unwind.expression, &context)? {
            JsonValue::Array(items) => items,
            JsonValue::Null => Vec::new(),
            other => vec![other],
        };
        for element in elements {
            let mut row = record.clone();
            row.insert(unwind.alias.to_string(), element);
            out.push(row);
        }
    }
    Ok(out)
}

/// Whether a RETURN aggregates (one record for all the rows).
pub fn return_aggregates(return_clause: &ReturnClause) -> bool {
    return_clause
        .return_items
        .iter()
        .any(|item| has_aggregation_in_expr(&item.expression))
}

/// Sort the RETURN's records by ORDER BY, whose expressions read the
/// returned columns and, when `inputs` holds the record each one was
/// returned from (a RETURN that does not aggregate), its variables. By
/// Cypher's orderability: strings, booleans, numbers, then NULL ascending.
pub fn apply_order_by(
    results: Vec<HashMap<String, JsonValue>>,
    inputs: Option<Vec<HashMap<String, JsonValue>>>,
    order_by: &OrderByClause,
) -> Result<Vec<HashMap<String, JsonValue>>, String> {
    let mut inputs = inputs.map(|i| i.into_iter());
    let mut keyed = Vec::new();
    for record in results {
        let mut variables = inputs.as_mut().and_then(|i| i.next()).unwrap_or_default();
        variables.extend(record.clone());
        let context = EvalContext::with_variables(variables);
        let keys = order_by
            .order_by_items
            .iter()
            .map(|item| evaluate_expr(&item.expression, &context))
            .collect::<Result<Vec<_>, _>>()?;
        if keys.iter().any(|k| k.is_array() || k.is_object()) {
            return Err(
                "Not supported: ordering by a list or a map after a procedure call".to_string(),
            );
        }
        keyed.push((keys, record));
    }
    keyed.sort_by(|(a, _), (b, _)| {
        for (item, (a, b)) in order_by.order_by_items.iter().zip(a.iter().zip(b)) {
            let o = orderability(a, b);
            let o = match item.order {
                OrerByOrder::Asc => o,
                OrerByOrder::Desc => o.reverse(),
            };
            if o.is_ne() {
                return o;
            }
        }
        Ordering::Equal
    });
    Ok(keyed.into_iter().map(|(_, record)| record).collect())
}

/// Cypher's ascending orderability of two scalar values (strings before
/// booleans before numbers, NULL last).
fn orderability(a: &JsonValue, b: &JsonValue) -> Ordering {
    let rank = |v: &JsonValue| match v {
        JsonValue::String(_) => 0,
        JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 2,
        _ => 3,
    };
    match compare(a, b) {
        Ok(Some(o)) => o,
        _ => rank(a).cmp(&rank(b)),
    }
}

/// A boolean, comparison or string-predicate operator, by Cypher's rules:
/// NULL where an operand is NULL or the operands are not comparable.
fn evaluate_operator(op: &OperatorApplication, context: &EvalContext) -> EvalResult {
    let operands = op
        .operands
        .iter()
        .map(|e| evaluate_expr(e, context))
        .collect::<Result<Vec<_>, _>>()?;
    let truth = |v: &JsonValue| match v {
        JsonValue::Bool(b) => Ok(Some(*b)),
        JsonValue::Null => Ok(None),
        other => Err(format!("{:?} expects a boolean, got {other}", op.operator)),
    };
    let value = |b: Option<bool>| b.map_or(JsonValue::Null, JsonValue::Bool);
    let unary = || match operands.as_slice() {
        [a] => Ok(a),
        _ => Err(format!("{:?} expects 1 operand", op.operator)),
    };
    let binary = || match operands.as_slice() {
        [a, b] => Ok((a, b)),
        _ => Err(format!("{:?} expects 2 operands", op.operator)),
    };
    let order = |want: fn(Ordering) -> bool| -> EvalResult {
        let (a, b) = binary()?;
        Ok(value(compare(a, b)?.map(want)))
    };
    let strings = |test: fn(&str, &str) -> bool| -> EvalResult {
        Ok(match binary()? {
            (JsonValue::String(a), JsonValue::String(b)) => JsonValue::Bool(test(a, b)),
            _ => JsonValue::Null,
        })
    };
    match op.operator {
        Operator::And | Operator::Or => {
            let (decisive, other) = match op.operator {
                Operator::And => (false, true),
                _ => (true, false),
            };
            let truths = operands.iter().map(truth).collect::<Result<Vec<_>, _>>()?;
            Ok(if truths.contains(&Some(decisive)) {
                JsonValue::Bool(decisive)
            } else if truths.contains(&None) {
                JsonValue::Null
            } else {
                JsonValue::Bool(other)
            })
        }
        Operator::Not => Ok(value(truth(unary()?)?.map(|b| !b))),
        Operator::IsNull => Ok(JsonValue::Bool(unary()?.is_null())),
        Operator::IsNotNull => Ok(JsonValue::Bool(!unary()?.is_null())),
        Operator::Equal => {
            let (a, b) = binary()?;
            Ok(value(equal(a, b)))
        }
        Operator::NotEqual => {
            let (a, b) = binary()?;
            Ok(value(equal(a, b).map(|e| !e)))
        }
        Operator::LessThan => order(|o| o == Ordering::Less),
        Operator::GreaterThan => order(|o| o == Ordering::Greater),
        Operator::LessThanEqual => order(|o| o != Ordering::Greater),
        Operator::GreaterThanEqual => order(|o| o != Ordering::Less),
        Operator::In | Operator::NotIn => {
            let (a, list) = binary()?;
            let found = match list {
                JsonValue::Null => None,
                JsonValue::Array(items) => {
                    let matches: Vec<_> = items.iter().map(|item| equal(a, item)).collect();
                    if matches.contains(&Some(true)) {
                        Some(true)
                    } else if matches.contains(&None) {
                        None
                    } else {
                        Some(false)
                    }
                }
                other => return Err(format!("IN expects a list, got {other}")),
            };
            Ok(value(match op.operator {
                Operator::In => found,
                _ => found.map(|f| !f),
            }))
        }
        Operator::StartsWith => strings(|a, b| a.starts_with(b)),
        Operator::EndsWith => strings(|a, b| a.ends_with(b)),
        Operator::Contains => strings(|a, b| a.contains(b)),
        other => Err(format!(
            "Not supported: the operator {} after a procedure call",
            String::from(other)
        )),
    }
}

/// Cypher equality: NULL if either side is NULL (or a NULL inside lists or
/// maps decides it), false for values of different types.
fn equal(a: &JsonValue, b: &JsonValue) -> Option<bool> {
    let all = |pairs: Vec<Option<bool>>| {
        if pairs.contains(&Some(false)) {
            Some(false)
        } else if pairs.contains(&None) {
            None
        } else {
            Some(true)
        }
    };
    match (a, b) {
        (JsonValue::Null, _) | (_, JsonValue::Null) => None,
        (JsonValue::Number(_), JsonValue::Number(_)) => compare(a, b).ok()?.map(|o| o.is_eq()),
        (JsonValue::Array(x), JsonValue::Array(y)) => {
            if x.len() != y.len() {
                return Some(false);
            }
            all(x.iter().zip(y).map(|(x, y)| equal(x, y)).collect())
        }
        (JsonValue::Object(x), JsonValue::Object(y)) => {
            if x.len() != y.len() || x.keys().any(|k| !y.contains_key(k)) {
                return Some(false);
            }
            all(x.iter().map(|(k, v)| equal(v, &y[k])).collect())
        }
        _ => Some(a == b),
    }
}

/// Cypher ordering of two values: NULL (`None`) if either is NULL or they
/// are of different types; lists and maps are refused.
fn compare(a: &JsonValue, b: &JsonValue) -> Result<Option<Ordering>, String> {
    Ok(match (a, b) {
        (JsonValue::Number(x), JsonValue::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(x), Some(y)) => Some(x.cmp(&y)),
            _ => match (x.as_f64(), y.as_f64()) {
                (Some(x), Some(y)) => x.partial_cmp(&y),
                _ => None,
            },
        },
        (JsonValue::String(x), JsonValue::String(y)) => Some(x.cmp(y)),
        (JsonValue::Bool(x), JsonValue::Bool(y)) => Some(x.cmp(y)),
        (
            JsonValue::Array(_) | JsonValue::Object(_),
            JsonValue::Array(_) | JsonValue::Object(_),
        ) => {
            return Err("Not supported: ordering lists or maps after a procedure call".to_string())
        }
        _ => None,
    })
}

/// Evaluate a literal expression
fn evaluate_literal(lit: &Literal) -> EvalResult {
    match lit {
        Literal::String(s) => Ok(JsonValue::String(s.to_string())),
        Literal::Integer(i) => Ok(JsonValue::Number((*i).into())),
        Literal::Float(f) => {
            let num =
                serde_json::Number::from_f64(*f).ok_or_else(|| format!("Invalid float: {}", f))?;
            Ok(JsonValue::Number(num))
        }
        Literal::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Literal::Null => Ok(JsonValue::Null),
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
        Ok(JsonValue::Array(arr[from_clamped..to_clamped].to_vec()))
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
            assert_eq!(
                result[0].get("label"),
                Some(&JsonValue::String("User".to_string()))
            );
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
            assert_eq!(
                obj.get("name"),
                Some(&JsonValue::String("labels".to_string()))
            );
            assert!(obj.get("data").unwrap().is_array());
            assert_eq!(obj.get("data").unwrap().as_array().unwrap().len(), 2);
        } else {
            panic!("Expected Query statement");
        }
    }
}
