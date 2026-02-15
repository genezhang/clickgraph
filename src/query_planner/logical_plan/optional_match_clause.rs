//! OPTIONAL MATCH clause processing.
//!
//! Handles Cypher's OPTIONAL MATCH which provides LEFT JOIN semantics -
//! all rows from the base pattern are preserved, with NULL values
//! where optional patterns don't match.
//!
//! # SQL Translation
//!
//! ```text
//! MATCH (a) OPTIONAL MATCH (a)-[:FOLLOWS]->(b)
//! → SELECT ... FROM a LEFT JOIN follows ON ... LEFT JOIN b ON ...
//! ```
//!
//! # Implementation
//!
//! 1. Sets optional mode flag in [`PlanCtx`]
//! 2. Processes patterns via standard MATCH logic
//! 3. Aliases are auto-marked as optional for JOIN generation
//! 4. Restores normal mode after processing

use std::sync::Arc;

use crate::{
    open_cypher_parser::ast,
    query_planner::{
        logical_plan::{plan_builder::LogicalPlanResult, LogicalPlan},
        plan_ctx::PlanCtx,
    },
};

/// Evaluate an OPTIONAL MATCH clause
///
/// OPTIONAL MATCH uses LEFT JOIN semantics - all rows from the input are preserved,
/// with NULL values for unmatched optional patterns.
///
/// Strategy:
/// 1. Set the optional match mode flag in PlanCtx
/// 2. Process patterns using regular MATCH logic (which now auto-marks aliases as optional)
/// 3. GraphJoinInference will generate LEFT JOINs for optional aliases
/// 4. Restore normal mode after processing
pub fn evaluate_optional_match_clause<'a>(
    optional_match_clause: &ast::OptionalMatchClause<'a>,
    input_plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::debug!(
        "OPTIONAL_MATCH: evaluate_optional_match_clause called with {} path patterns",
        optional_match_clause.path_patterns.len()
    );

    // SIMPLE FIX: Set the optional match mode flag BEFORE processing patterns
    // This will automatically mark all new aliases as optional during planning
    plan_ctx.set_optional_match_mode(true);

    crate::debug_print!("🔔 DEBUG OPTIONAL_MATCH: Enabled optional match mode");

    // Create a temporary MatchClause from the OptionalMatchClause
    // This allows us to reuse the existing match clause logic
    let temp_match_clause = ast::MatchClause {
        path_patterns: optional_match_clause
            .path_patterns
            .iter()
            .map(|p| (None, p.clone())) // Wrap each pattern with None for path_variable
            .collect(),
        where_clause: None, // WHERE clause handled separately for OPTIONAL MATCH
    };

    // Process the patterns using the _with_optional variant and pass is_optional=true
    // This ensures GraphRel structures are created with is_optional=Some(true)
    use crate::query_planner::logical_plan::match_clause::evaluate_match_clause_with_optional;
    let mut plan =
        evaluate_match_clause_with_optional(&temp_match_clause, input_plan, plan_ctx, true)?;

    // Restore normal mode
    plan_ctx.set_optional_match_mode(false);

    crate::debug_print!(
        "🔕 DEBUG OPTIONAL_MATCH: Disabled optional match mode, plan type: {:?}",
        std::mem::discriminant(&*plan)
    );

    // If there's a WHERE clause specific to this OPTIONAL MATCH,
    // it should be applied as part of the JOIN condition, not as a final filter
    if let Some(where_clause) = &optional_match_clause.where_clause {
        // Store the WHERE clause in the plan context for later processing
        // During SQL generation, this will become part of the LEFT JOIN ON condition
        // For now, we'll add it as a regular filter
        // TODO: Properly handle WHERE clauses in OPTIONAL MATCH
        use crate::query_planner::logical_plan::where_clause::evaluate_where_clause;
        plan = evaluate_where_clause(where_clause, plan)?;
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::schema_types::SchemaType;
    use crate::graph_catalog::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema};
    use crate::open_cypher_parser::ast;

    /// Create a test graph schema with User nodes and FOLLOWS relationships
    fn setup_test_graph_schema() -> GraphSchema {
        use crate::graph_catalog::expression_parser::PropertyValue;
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create User node schema
        let user_node = NodeSchema {
            database: "test_db".to_string(),
            table_name: "users".to_string(),
            column_names: vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
                "status".to_string(),
                "user_id".to_string(),
            ],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: [
                (
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                ),
                ("age".to_string(), PropertyValue::Column("age".to_string())),
                (
                    "status".to_string(),
                    PropertyValue::Column("status".to_string()),
                ),
                (
                    "user_id".to_string(),
                    PropertyValue::Column("user_id".to_string()),
                ),
                (
                    "full_name".to_string(),
                    PropertyValue::Column("name".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        };
        nodes.insert("User".to_string(), user_node);

        // Create FOLLOWS relationship schema
        let follows_rel = RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "follows".to_string(),
            column_names: vec!["from_id".to_string(), "to_id".to_string()],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
            from_id: Identifier::from("from_id"),
            to_id: Identifier::from("to_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        };
        relationships.insert("FOLLOWS::User::User".to_string(), follows_rel);

        GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
    }

    #[test]
    fn test_evaluate_optional_match_simple_node() {
        let optional_match = ast::OptionalMatchClause {
            path_patterns: vec![ast::PathPattern::Node(ast::NodePattern {
                name: Some("a"),
                labels: Some(vec!["User"]),
                properties: None,
            })],
            where_clause: None,
        };

        let input_plan = Arc::new(LogicalPlan::Empty);

        // Set up test schema for the test
        let graph_schema = setup_test_graph_schema();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));

        let result = evaluate_optional_match_clause(&optional_match, input_plan, &mut plan_ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluate_optional_match_with_where() {
        let optional_match = ast::OptionalMatchClause {
            path_patterns: vec![ast::PathPattern::Node(ast::NodePattern {
                name: Some("a"),
                labels: Some(vec!["User"]),
                properties: None,
            })],
            where_clause: Some(ast::WhereClause {
                conditions: ast::Expression::OperatorApplicationExp(ast::OperatorApplication {
                    operator: ast::Operator::GreaterThan,
                    operands: vec![
                        ast::Expression::PropertyAccessExp(ast::PropertyAccess {
                            base: "a",
                            key: "age",
                        }),
                        ast::Expression::Literal(ast::Literal::Integer(25)),
                    ],
                }),
            }),
        };

        let input_plan = Arc::new(LogicalPlan::Empty);

        // Set up test schema for the test
        let graph_schema = setup_test_graph_schema();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));

        let result = evaluate_optional_match_clause(&optional_match, input_plan, &mut plan_ctx);
        assert!(result.is_ok());
    }
}
