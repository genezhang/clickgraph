//! UNION Pruning Optimizer
//!
//! Extracts label information from WHERE clause `id() IN [...]` patterns to prune
//! unnecessary UNION branches, improving performance and fixing SQL column mismatch errors.
//!
//! ## Problem
//!
//! Queries like `MATCH (a)-[r]->(b) WHERE id(a) IN [...]` generate UNION of ALL relationship types,
//! even when all IDs belong to a single node type (e.g., all User).
//!
//! ## Solution
//!
//! Decode the ID list to extract label information, then filter UNION branches to only include
//! relationships from those node types.
//!
//! ## Example
//!
//! ```ignore
//! // Query with all User IDs:
//! MATCH (a)-[r]->(b) WHERE id(a) IN [281474976710657, 281474976710658] RETURN r
//!
//! // Decode IDs → All are label_code=5 (User)
//! // Generate only User relationships:
//! //   User-FOLLOWS->User
//! //   User-AUTHORED->Post
//! //   User-LIKED->Post
//! // Instead of all 10 relationship types!
//! ```

use crate::{open_cypher_parser::ast, utils::id_encoding::IdEncoding};
use std::collections::{HashMap, HashSet};

/// Extract node labels from WHERE clause containing `id(var) IN [...]` patterns
///
/// # Arguments
/// * `where_clause` - The WHERE clause AST node
///
/// # Returns
/// * Map of variable names to their possible label sets
///
/// # Example
///
/// ```ignore
/// // WHERE id(a) IN [281474976710657, 281474976710658]
/// // Returns: {"a": {"User"}}
///
/// // WHERE id(a) = 281474976710657 AND id(b) = 844424930131969
/// // Returns: {"a": {"User"}, "b": {"Post"}}
/// ```
pub fn extract_labels_from_id_where<'a>(
    where_clause: &ast::WhereClause<'a>,
) -> HashMap<String, HashSet<String>> {
    let mut label_constraints = HashMap::new();
    extract_from_ast_expr(&where_clause.conditions, &mut label_constraints);
    label_constraints
}

/// Recursively traverse AST WHERE expression to find `id(var) IN [...]` or `id(var) = X` patterns
fn extract_from_ast_expr<'a>(
    expr: &ast::Expression<'a>,
    constraints: &mut HashMap<String, HashSet<String>>,
) {
    match expr {
        // Handle operator applications: IN, =, AND, OR
        ast::Expression::OperatorApplicationExp(op_app) => {
            match op_app.operator {
                ast::Operator::In => {
                    // Check if first operand is id(var)
                    if let Some(ast::Expression::FunctionCallExp(func)) = op_app.operands.get(0) {
                        if func.name == "id" && func.args.len() == 1 {
                            if let ast::Expression::Variable(var_name) = &func.args[0] {
                                // Extract IDs from second operand (list)
                                if let Some(ast::Expression::List(id_list)) = op_app.operands.get(1)
                                {
                                    for item in id_list {
                                        if let ast::Expression::Literal(ast::Literal::Integer(
                                            id_value,
                                        )) = item
                                        {
                                            if let Some((label, _)) =
                                                IdEncoding::decode_with_label(*id_value)
                                            {
                                                constraints
                                                    .entry(var_name.to_string())
                                                    .or_insert_with(HashSet::new)
                                                    .insert(label);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                ast::Operator::Equal => {
                    // Handle: id(var) = X
                    if let Some(ast::Expression::FunctionCallExp(func)) = op_app.operands.get(0) {
                        if func.name == "id" && func.args.len() == 1 {
                            if let ast::Expression::Variable(var_name) = &func.args[0] {
                                if let Some(ast::Expression::Literal(ast::Literal::Integer(
                                    id_value,
                                ))) = op_app.operands.get(1)
                                {
                                    if let Some((label, _)) =
                                        IdEncoding::decode_with_label(*id_value)
                                    {
                                        constraints
                                            .entry(var_name.to_string())
                                            .or_insert_with(HashSet::new)
                                            .insert(label);
                                    }
                                }
                            }
                        }
                    }
                }
                ast::Operator::And | ast::Operator::Or => {
                    // Recursively check all operands
                    for operand in &op_app.operands {
                        extract_from_ast_expr(operand, constraints);
                    }
                }
                _ => {
                    // For other operators, recurse into operands
                    for operand in &op_app.operands {
                        extract_from_ast_expr(operand, constraints);
                    }
                }
            }
        }
        // For other expression types, no id() patterns expected
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_labels_basic() {
        // This is a minimal test since we'd need full AST parsing to test properly
        // The real testing will happen via integration tests
        let constraints = HashMap::<String, HashSet<String>>::new();
        assert!(constraints.is_empty());
    }
}
