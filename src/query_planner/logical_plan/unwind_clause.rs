use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::UnwindClause,
    query_planner::{
        logical_plan::{LogicalPlan, Unwind},
        logical_expr::LogicalExpr,
        plan_ctx::PlanCtx,
    },
};

/// Evaluate an UNWIND clause and wrap the current plan with an Unwind node
/// 
/// UNWIND transforms array values into individual rows.
/// In ClickHouse, this maps to ARRAY JOIN.
/// 
/// Example:
/// ```cypher
/// MATCH (n:Node)
/// UNWIND n.items AS item
/// RETURN n.id, item
/// ```
/// 
/// Generates:
/// ```sql
/// SELECT n.id, item
/// FROM nodes AS n
/// ARRAY JOIN n.items AS item
/// ```
pub fn evaluate_unwind_clause(
    unwind_clause: &UnwindClause,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> Arc<LogicalPlan> {
    // Convert the Cypher expression to a LogicalExpr
    let expression = LogicalExpr::from(unwind_clause.expression.clone());
    
    // Register the UNWIND alias as a projection alias
    // This allows subsequent clauses (WHERE, RETURN) to reference it
    let alias_expr = LogicalExpr::TableAlias(crate::query_planner::logical_expr::TableAlias(
        unwind_clause.alias.to_string()
    ));
    plan_ctx.register_projection_alias(unwind_clause.alias.to_string(), alias_expr);
    
    let unwind = Unwind {
        input: plan,
        expression,
        alias: unwind_clause.alias.to_string(),
    };
    
    Arc::new(LogicalPlan::Unwind(unwind))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_cypher_parser::ast::{Expression, Literal, PropertyAccess};
    use crate::graph_catalog::graph_schema::GraphSchema;
    use std::collections::HashMap;

    fn create_test_plan_ctx() -> PlanCtx {
        let schema = GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
            HashMap::new(),
        );
        PlanCtx::new(std::sync::Arc::new(schema))
    }

    #[test]
    fn test_evaluate_unwind_literal_list() {
        // Create a simple UNWIND clause with a literal list
        let unwind_clause = UnwindClause {
            expression: Expression::List(vec![
                Expression::Literal(Literal::Integer(1)),
                Expression::Literal(Literal::Integer(2)),
                Expression::Literal(Literal::Integer(3)),
            ]),
            alias: "x",
        };

        let input_plan = Arc::new(LogicalPlan::Empty);
        let mut plan_ctx = create_test_plan_ctx();
        let result = evaluate_unwind_clause(&unwind_clause, input_plan, &mut plan_ctx);

        match result.as_ref() {
            LogicalPlan::Unwind(unwind) => {
                assert_eq!(unwind.alias, "x");
                // Check the expression is a list
                match &unwind.expression {
                    LogicalExpr::List(_) => (),
                    _ => panic!("Expected list expression, got {:?}", unwind.expression),
                }
                // Verify the alias was registered as a projection alias
                assert!(plan_ctx.is_projection_alias("x"));
            }
            _ => panic!("Expected Unwind plan"),
        }
    }

    #[test]
    fn test_evaluate_unwind_property_access() {
        // Create an UNWIND clause with property access: UNWIND r.items AS item
        let unwind_clause = UnwindClause {
            expression: Expression::PropertyAccessExp(PropertyAccess {
                base: "r",
                key: "items",
            }),
            alias: "item",
        };

        let input_plan = Arc::new(LogicalPlan::Empty);
        let mut plan_ctx = create_test_plan_ctx();
        let result = evaluate_unwind_clause(&unwind_clause, input_plan, &mut plan_ctx);

        match result.as_ref() {
            LogicalPlan::Unwind(unwind) => {
                assert_eq!(unwind.alias, "item");
                // Check the expression is a property access
                match &unwind.expression {
                    LogicalExpr::PropertyAccessExp(pa) => {
                        // column is a PropertyValue enum
                        match &pa.column {
                            crate::graph_catalog::expression_parser::PropertyValue::Column(col) => {
                                assert_eq!(col, "items");
                            }
                            _ => panic!("Expected Column property value"),
                        }
                    }
                    _ => panic!("Expected property access expression, got {:?}", unwind.expression),
                }
            }
            _ => panic!("Expected Unwind plan"),
        }
    }
}
