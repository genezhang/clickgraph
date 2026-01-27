use serde::{Deserialize, Serialize};
use std::cell::RefCell;

use super::plan_builder::RenderPlanBuilder;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::render_plan::RenderPlan;

use crate::query_planner::logical_expr::LogicalExpr;

use crate::query_planner::logical_expr::{
    AggregateFnCall as LogicalAggregateFnCall, Column as LogicalColumn,
    ColumnAlias as LogicalColumnAlias, ConnectedPattern, CteEntityRef as LogicalCteEntityRef,
    Direction, EntityType, ExistsSubquery as LogicalExistsSubquery,
    InSubquery as LogicalInSubquery, Literal as LogicalLiteral, LogicalCase,
    Operator as LogicalOperator, OperatorApplication as LogicalOperatorApplication, PathPattern,
    PropertyAccess as LogicalPropertyAccess, ScalarFnCall as LogicalScalarFnCall,
    TableAlias as LogicalTableAlias,
};
use crate::query_planner::logical_plan::LogicalPlan;

use super::errors::RenderBuildError;

// Re-export schema name accessors from the unified query context
// See server/query_context.rs for the task_local! implementation with .scope() support
pub use crate::server::query_context::{
    clear_current_schema_name, get_current_schema_name, set_current_schema_name,
};

/// Generate SQL for an EXISTS subquery directly from the logical plan
/// This is a simplified approach that generates basic EXISTS SQL
fn generate_exists_sql(exists: &LogicalExistsSubquery) -> Result<String, RenderBuildError> {
    use crate::server::GLOBAL_SCHEMAS;

    // Try to extract pattern info from the subplan
    // The subplan is typically a GraphRel representing a relationship pattern
    match exists.subplan.as_ref() {
        // For WITH clauses and other complex plans, use the full render pipeline
        LogicalPlan::WithClause(_)
        | LogicalPlan::GraphJoins(_)
        | LogicalPlan::CartesianProduct(_) => {
            use crate::clickhouse_query_generator::to_sql_query::render_plan_to_sql;
            use crate::render_plan::plan_builder::RenderPlanBuilder;

            // Get schema from GLOBAL_SCHEMAS
            let schemas_lock = GLOBAL_SCHEMAS.get();
            let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());
            let schema = schemas_guard
                .as_ref()
                .and_then(|guard| guard.values().next())
                .ok_or_else(|| {
                    RenderBuildError::InvalidRenderPlan(
                        "No schema available for EXISTS subquery".to_string(),
                    )
                })?;

            // Convert logical plan to render plan using the full pipeline
            let render_plan = exists.subplan.to_render_plan(schema)?;

            // Generate SQL from render plan
            let sql = render_plan_to_sql(render_plan, 10); // Use default max_cte_depth

            Ok(sql)
        }
        LogicalPlan::GraphRel(graph_rel) => {
            // Get the relationship type
            let rel_type = graph_rel
                .labels
                .as_ref()
                .and_then(|l| l.first())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string());

            // Get the start node alias (the correlated variable)
            let start_alias = &graph_rel.left_connection;

            // Try to get schema for relationship lookup
            // GLOBAL_SCHEMAS is OnceCell<RwLock<HashMap<String, GraphSchema>>>
            // CRITICAL FIX: Use the current schema name from thread-local storage
            // instead of searching all schemas. This fixes EXISTS with multi-schema support.
            let schemas_lock = GLOBAL_SCHEMAS.get();
            let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());

            // First try to get the schema by name from thread-local
            let current_schema_name = get_current_schema_name();
            let schema = schemas_guard.as_ref().and_then(|guard| {
                if let Some(ref name) = current_schema_name {
                    // Use the specific schema if we have a name
                    guard.get(name)
                } else {
                    // Fallback: search all schemas for one that has this relationship type
                    // This is less reliable but maintains backward compatibility
                    guard
                        .values()
                        .find(|s| s.get_relationships_schema_opt(&rel_type).is_some())
                }
            });

            // Look up the relationship table and columns using public accessors
            if let Some(schema) = schema {
                if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                    // CRITICAL FIX: Use fully qualified table name (database.table) for EXISTS
                    // This fixes "Unknown table expression" errors when table name alone isn't enough
                    let qualified_table =
                        format!("{}.{}", rel_schema.database, rel_schema.table_name);
                    let from_col = &rel_schema.from_id; // from_id is the FK column

                    // Get the start node's ID column from its label
                    let start_id_sql =
                        if let LogicalPlan::GraphNode(start_node) = graph_rel.left.as_ref() {
                            if let Some(label) = &start_node.label {
                                let node_schema =
                                    schema.node_schema_opt(label).ok_or_else(|| {
                                        RenderBuildError::NodeSchemaNotFound(label.clone())
                                    })?;
                                node_schema.node_id.sql_tuple(start_alias)
                            } else {
                                // No label - infer from relationship schema
                                let node_type = &rel_schema.from_node;
                                let node_schema =
                                    schema.node_schema_opt(node_type).ok_or_else(|| {
                                        RenderBuildError::NodeSchemaNotFound(node_type.clone())
                                    })?;
                                node_schema.node_id.sql_tuple(start_alias)
                            }
                        } else {
                            // Not a GraphNode - error, can't infer
                            return Err(RenderBuildError::InvalidRenderPlan(
                                "EXISTS pattern left side is not a GraphNode".to_string(),
                            ));
                        };

                    // Generate the EXISTS SQL
                    // EXISTS (SELECT 1 FROM database.edge_table WHERE edge_table.from_id = outer.node_id)
                    // Note: Use unqualified table_name in WHERE clause column reference
                    return Ok(format!(
                        "SELECT 1 FROM {} WHERE {}.{} = {}",
                        qualified_table, rel_schema.table_name, from_col, start_id_sql
                    ));
                }
            }

            // No schema found - this is an error
            Err(RenderBuildError::InvalidRenderPlan(format!(
                "Cannot generate EXISTS pattern: relationship schema '{}' not found. \
                         Please define this relationship in your YAML schema configuration.",
                rel_type
            )))
        }
        _ => {
            // For other plan types, this is unsupported
            Err(RenderBuildError::UnsupportedFeature(
                "EXISTS pattern with non-GraphRel subplan".to_string(),
            ))
        }
    }
}

/// Generate SQL for multi-hop pattern count (size() with multiple relationships)
///
/// For `size((tag)<-[:HAS_TAG]-(message:Message)-[:HAS_CREATOR]->(person))` generates:
/// ```sql
/// (SELECT COUNT(*)
///  FROM Message_hasTag_Tag AS r1
///  INNER JOIN Message_hasCreator_Person AS r2 ON r1.MessageId = r2.MessageId
///  WHERE r1.TagId = tag.id)
/// ```
fn generate_multi_hop_pattern_count_sql(
    connected_patterns: &[ConnectedPattern],
    start_alias: &str,
) -> Result<String, RenderBuildError> {
    use crate::server::GLOBAL_SCHEMAS;

    // Get the first relationship type to find the correct schema
    let first_rel_type = connected_patterns
        .first()
        .and_then(|conn| conn.relationship.labels.as_ref())
        .and_then(|labels| labels.first())
        .ok_or_else(|| {
            RenderBuildError::InvalidRenderPlan(
                "Multi-hop pattern missing relationship type".to_string(),
            )
        })?;

    // CRITICAL FIX: Use the current schema name from thread-local storage
    // instead of searching all schemas. This fixes multi-hop EXISTS with multi-schema support.
    let schemas_lock = GLOBAL_SCHEMAS.get();
    let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());

    // First try to get the schema by name from thread-local
    let current_schema_name = get_current_schema_name();
    let schema = schemas_guard
        .as_ref()
        .and_then(|guard| {
            if let Some(ref name) = current_schema_name {
                // Use the specific schema if we have a name
                guard.get(name)
            } else {
                // Fallback: search all schemas for one that has this relationship type
                guard
                    .values()
                    .find(|s| s.get_relationships_schema_opt(first_rel_type).is_some())
            }
        })
        .ok_or_else(|| {
            RenderBuildError::InvalidRenderPlan(format!(
                "Schema not found for multi-hop pattern with relationship '{}'",
                first_rel_type
            ))
        })?;

    // First relationship connects to start node (correlated)
    let first_conn = &connected_patterns[0];
    let first_rel_type = first_conn
        .relationship
        .labels
        .as_ref()
        .and_then(|l| l.first())
        .ok_or_else(|| {
            RenderBuildError::InvalidRenderPlan(
                "Multi-hop pattern missing relationship type".to_string(),
            )
        })?;

    let first_rel_schema = schema
        .get_relationships_schema_opt(first_rel_type)
        .ok_or_else(|| {
            RenderBuildError::InvalidRenderPlan(format!(
                "Relationship schema '{}' not found for multi-hop pattern",
                first_rel_type
            ))
        })?;

    // Get start node ID for correlation
    // Try explicit label first, then infer from relationship schema
    let start_node_label = if let Some(label) = first_conn.start_node.label.as_ref() {
        label.clone()
    } else {
        // Infer from relationship schema based on direction
        match first_conn.relationship.direction {
            Direction::Outgoing => first_rel_schema.from_node.clone(),
            Direction::Incoming => first_rel_schema.to_node.clone(),
            _ => first_rel_schema.from_node.clone(), // default
        }
    };

    let start_node_schema = schema
        .node_schema_opt(&start_node_label)
        .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(start_node_label.clone()))?;
    let start_id_sql = start_node_schema.node_id.sql_tuple(start_alias);

    // Build FROM clause with JOINs
    let first_table = if !first_rel_schema.database.is_empty() {
        format!(
            "{}.{}",
            first_rel_schema.database, first_rel_schema.table_name
        )
    } else {
        first_rel_schema.table_name.clone()
    };

    let mut from_clause = format!("{} AS r1", first_table);
    let mut where_conditions = Vec::new();

    // Add correlation condition for first relationship
    let first_col = match first_conn.relationship.direction {
        Direction::Outgoing => &first_rel_schema.from_id,
        Direction::Incoming => &first_rel_schema.to_id,
        _ => &first_rel_schema.from_id, // default
    };
    where_conditions.push(format!("r1.{} = {}", first_col, start_id_sql));

    // Add subsequent relationships as JOINs
    for (idx, conn) in connected_patterns.iter().enumerate().skip(1) {
        let rel_type = conn
            .relationship
            .labels
            .as_ref()
            .and_then(|l| l.first())
            .ok_or_else(|| {
                RenderBuildError::InvalidRenderPlan(
                    "Multi-hop pattern missing relationship type".to_string(),
                )
            })?;

        let rel_schema = schema
            .get_relationships_schema_opt(rel_type)
            .ok_or_else(|| {
                RenderBuildError::InvalidRenderPlan(format!(
                    "Relationship schema '{}' not found for multi-hop pattern",
                    rel_type
                ))
            })?;

        let table = if !rel_schema.database.is_empty() {
            format!("{}.{}", rel_schema.database, rel_schema.table_name)
        } else {
            rel_schema.table_name.clone()
        };

        let curr_alias = format!("r{}", idx + 1);
        let prev_alias = format!("r{}", idx);

        // Determine join condition based on how patterns connect
        // The end of previous pattern should match start of current pattern
        let prev_conn = &connected_patterns[idx - 1];

        // Previous pattern's end column (where it points TO)
        let prev_end_col = match prev_conn.relationship.direction {
            Direction::Outgoing => {
                &schema
                    .get_relationships_schema_opt(
                        prev_conn
                            .relationship
                            .labels
                            .as_ref()
                            .unwrap()
                            .first()
                            .unwrap(),
                    )
                    .unwrap()
                    .to_id
            }
            Direction::Incoming => {
                &schema
                    .get_relationships_schema_opt(
                        prev_conn
                            .relationship
                            .labels
                            .as_ref()
                            .unwrap()
                            .first()
                            .unwrap(),
                    )
                    .unwrap()
                    .from_id
            }
            _ => {
                &schema
                    .get_relationships_schema_opt(
                        prev_conn
                            .relationship
                            .labels
                            .as_ref()
                            .unwrap()
                            .first()
                            .unwrap(),
                    )
                    .unwrap()
                    .to_id
            }
        };

        // Current pattern's start column (where it points FROM)
        let curr_start_col = match conn.relationship.direction {
            Direction::Outgoing => &rel_schema.from_id,
            Direction::Incoming => &rel_schema.to_id,
            _ => &rel_schema.from_id,
        };

        from_clause.push_str(&format!(
            " INNER JOIN {} AS {} ON {}.{} = {}.{}",
            table, curr_alias, prev_alias, prev_end_col, curr_alias, curr_start_col
        ));
    }

    // Build final SQL
    let where_clause = if where_conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conditions.join(" AND "))
    };

    Ok(format!(
        "(SELECT COUNT(*) FROM {}{})",
        from_clause, where_clause
    ))
}

/// Generate SQL for a pattern count (size() on patterns)
///
/// For `size((n)-[:REL]->())` pattern, generates:
/// ```sql
/// (SELECT COUNT(*) FROM rel_table WHERE rel_table.from_id = n.id)
/// ```
fn generate_pattern_count_sql(pattern: &PathPattern) -> Result<String, RenderBuildError> {
    use crate::server::GLOBAL_SCHEMAS;

    match pattern {
        PathPattern::ConnectedPattern(connected_patterns) => {
            if connected_patterns.is_empty() {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Empty connected pattern in size()".to_string(),
                ));
            }

            // Get the first connection for start node correlation
            let conn = &connected_patterns[0];

            // Get the start node alias (the correlated variable - connects to outer scope)
            let start_alias = conn.start_node.name.as_ref().ok_or_else(|| {
                RenderBuildError::InvalidRenderPlan(
                    "size() pattern requires named start node".to_string(),
                )
            })?;

            // For multi-hop patterns, we need to join multiple relationships
            if connected_patterns.len() > 1 {
                // Handle multi-hop pattern by generating JOINs
                return generate_multi_hop_pattern_count_sql(connected_patterns, start_alias);
            }

            // Single-hop pattern from here on
            // Get the end node alias (can be anonymous/None)
            let end_alias = conn.end_node.name.as_ref().map(|s| s.to_string());

            // Get relationship type
            let rel_type = conn
                .relationship
                .labels
                .as_ref()
                .and_then(|l| l.first())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string());

            // Determine direction
            let is_undirected = matches!(conn.relationship.direction, Direction::Either);

            // Try to get schema for relationship lookup
            // CRITICAL FIX: Search all schemas for one that has this relationship type
            // instead of hardcoding "default". This fixes size() with multi-schema support.
            let schemas_lock = GLOBAL_SCHEMAS.get();
            let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());
            let schema = schemas_guard.as_ref().and_then(|guard| {
                // Try each schema until we find one with this relationship type
                guard
                    .values()
                    .find(|s| s.get_relationships_schema_opt(&rel_type).is_some())
            });

            // Look up the relationship table and columns
            if let Some(schema) = schema {
                if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                    let table_name = &rel_schema.table_name;
                    let full_table = if !rel_schema.database.is_empty() {
                        format!("{}.{}", rel_schema.database, table_name)
                    } else {
                        table_name.clone()
                    };
                    let from_col = &rel_schema.from_id;
                    let to_col = &rel_schema.to_id;

                    // Get the start node's ID column
                    // First try the explicit label from the pattern, then fall back to relationship schema
                    let start_id_sql = if let Some(label) = &conn.start_node.label {
                        let node_schema = schema
                            .node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        node_schema.node_id.sql_tuple(start_alias)
                    } else {
                        // No label in pattern - infer from relationship's from_node
                        let node_type = &rel_schema.from_node;
                        let node_schema =
                            schema.node_schema_opt(node_type).ok_or_else(|| {
                                RenderBuildError::NodeSchemaNotFound(node_type.clone())
                            })?;
                        node_schema.node_id.sql_tuple(start_alias)
                    };

                    // Get end node's ID column
                    let end_id_sql = if let Some(label) = &conn.end_node.label {
                        let node_schema = schema
                            .node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        end_alias
                            .as_ref()
                            .map(|alias| node_schema.node_id.sql_tuple(alias))
                            .unwrap_or_default()
                    } else {
                        // No label in pattern - infer from relationship's to_node
                        let node_type = &rel_schema.to_node;
                        let node_schema =
                            schema.node_schema_opt(node_type).ok_or_else(|| {
                                RenderBuildError::NodeSchemaNotFound(node_type.clone())
                            })?;
                        end_alias
                            .as_ref()
                            .map(|alias| node_schema.node_id.sql_tuple(alias))
                            .unwrap_or_default()
                    };

                    // Generate COUNT SQL based on direction
                    // NOTE: For size() patterns, named end nodes are INTERNAL to the pattern,
                    // not correlated with outer scope. We only correlate on the start node.
                    // Examples:
                    //   size((tag)<-[:HAS_INTEREST]-(person:Person))
                    //     => COUNT all Person nodes connected to tag, regardless of which persons
                    //   size((tag)<-[:HAS_TAG]-(message:Message))
                    //     => COUNT all Message nodes connected to tag, not correlating on specific message
                    let count_sql = match (is_undirected, &conn.relationship.direction) {
                        (false, Direction::Outgoing) => {
                            // Directed outgoing: start_node -> end_node
                            format!(
                                "(SELECT COUNT(*) FROM {} WHERE {}.{} = {})",
                                full_table, table_name, from_col, start_id_sql
                            )
                        }
                        (false, Direction::Incoming) => {
                            // Directed incoming: start_node <- end_node
                            format!(
                                "(SELECT COUNT(*) FROM {} WHERE {}.{} = {})",
                                full_table, table_name, to_col, start_id_sql
                            )
                        }
                        (true, _) | (false, Direction::Either) => {
                            // Undirected: count both directions from start node
                            format!(
                                "(SELECT COUNT(*) FROM {} WHERE {}.{} = {} OR {}.{} = {})",
                                full_table,
                                table_name,
                                from_col,
                                start_id_sql,
                                table_name,
                                to_col,
                                start_id_sql
                            )
                        }
                    };

                    return Ok(count_sql);
                }
            }

            // No schema found - this is an error, not a fallback scenario
            Err(RenderBuildError::InvalidRenderPlan(
                format!("Cannot generate size() pattern count: relationship schema '{}' not found. \
                         Please define this relationship in your YAML schema configuration with proper \
                         from_node, to_node, and ID column mappings.", rel_type)
            ))
        }
        PathPattern::Node(_) => Err(RenderBuildError::InvalidRenderPlan(
            "size() pattern with single node is not supported".to_string(),
        )),
        PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
            Err(RenderBuildError::InvalidRenderPlan(
                "size() pattern with shortest path is not supported".to_string(),
            ))
        }
    }
}

/// Generate NOT EXISTS SQL for a PathPattern (negative pattern matching / anti-join)
///
/// For `NOT (a)-[:REL]-(b)` pattern, generates:
/// ```sql
/// NOT EXISTS (
///     SELECT 1 FROM rel_table
///     WHERE (rel_table.from_id = a.id AND rel_table.to_id = b.id)
///        OR (rel_table.from_id = b.id AND rel_table.to_id = a.id)  -- for undirected
/// )
/// ```
fn generate_not_exists_from_path_pattern(
    pattern: &PathPattern,
) -> Result<String, RenderBuildError> {
    use crate::server::GLOBAL_SCHEMAS;

    match pattern {
        PathPattern::ConnectedPattern(connected_patterns) => {
            if connected_patterns.is_empty() {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Empty connected pattern in NOT expression".to_string(),
                ));
            }

            // Handle single-hop pattern (most common case for anti-join)
            let conn = &connected_patterns[0];

            // Get the start and end node aliases (end node can be anonymous)
            let start_alias = conn.start_node.name.as_ref().ok_or_else(|| {
                RenderBuildError::InvalidRenderPlan(
                    "NOT pattern requires named start node".to_string(),
                )
            })?;
            // End alias is optional - if None, we only check the from_id
            let end_alias = conn.end_node.name.as_ref();

            // Get the relationship type
            let rel_type = conn
                .relationship
                .labels
                .as_ref()
                .and_then(|l| l.first())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string());

            // Get direction
            let is_undirected = conn.relationship.direction == Direction::Either;

            // Try to get schema for relationship lookup
            // CRITICAL FIX: Search all schemas for one that has this relationship type
            // instead of hardcoding "default". This fixes size() with multi-schema support.
            let schemas_lock = GLOBAL_SCHEMAS.get();
            let schemas_guard = schemas_lock.and_then(|lock| lock.try_read().ok());
            let schema = schemas_guard.as_ref().and_then(|guard| {
                // Try each schema until we find one with this relationship type
                guard
                    .values()
                    .find(|s| s.get_relationships_schema_opt(&rel_type).is_some())
            });

            // Look up the relationship table and columns
            if let Some(schema) = schema {
                if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                    let db_name = &rel_schema.database;
                    let table_name = &rel_schema.table_name;
                    let full_table = format!("{}.{}", db_name, table_name);
                    let from_col = &rel_schema.from_id;
                    let to_col = &rel_schema.to_id;

                    // Get the node ID columns from their labels or infer from relationship schema
                    let start_id_sql = if let Some(label) = &conn.start_node.label {
                        let node_schema = schema
                            .node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        node_schema.node_id.sql_tuple(start_alias)
                    } else {
                        // Infer from relationship's from_node
                        let node_type = &rel_schema.from_node;
                        let node_schema =
                            schema.node_schema_opt(node_type).ok_or_else(|| {
                                RenderBuildError::NodeSchemaNotFound(node_type.clone())
                            })?;
                        node_schema.node_id.sql_tuple(start_alias)
                    };

                    let end_id_sql = if let Some(label) = &conn.end_node.label {
                        let node_schema = schema
                            .node_schema_opt(label)
                            .ok_or_else(|| RenderBuildError::NodeSchemaNotFound(label.clone()))?;
                        end_alias
                            .map(|alias| node_schema.node_id.sql_tuple(alias))
                            .unwrap_or_default()
                    } else {
                        // Infer from relationship's to_node
                        let node_type = &rel_schema.to_node;
                        let node_schema =
                            schema.node_schema_opt(node_type).ok_or_else(|| {
                                RenderBuildError::NodeSchemaNotFound(node_type.clone())
                            })?;
                        end_alias
                            .map(|alias| node_schema.node_id.sql_tuple(alias))
                            .unwrap_or_default()
                    };

                    // Generate the NOT EXISTS SQL
                    let exists_sql = match (end_alias, is_undirected) {
                        // Anonymous end node: just check if any relationship exists from start node
                        (None, false) => {
                            // Directed with anonymous end: check FROM or TO based on direction
                            match conn.relationship.direction {
                                Direction::Outgoing => format!(
                                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {})",
                                    full_table, table_name, from_col, start_id_sql
                                ),
                                Direction::Incoming => format!(
                                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {})",
                                    full_table, table_name, to_col, start_id_sql
                                ),
                                _ => {
                                    format!(
                                    "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {} OR {}.{} = {})",
                                    full_table,
                                    table_name, from_col, start_id_sql,
                                    table_name, to_col, start_id_sql
                                )
                                }
                            }
                        }
                        (None, true) => {
                            // Undirected with anonymous end: check either direction
                            format!(
                                "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {} OR {}.{} = {})",
                                full_table,
                                table_name,
                                from_col,
                                start_id_sql,
                                table_name,
                                to_col,
                                start_id_sql
                            )
                        }
                        (Some(end), true) => {
                            // Named end node, undirected: check both directions
                            format!(
                                "NOT EXISTS (SELECT 1 FROM {} WHERE ({}.{} = {} AND {}.{} = {}) OR ({}.{} = {} AND {}.{} = {}))",
                                full_table,
                                // Direction 1: start -> end
                                table_name, from_col, start_id_sql,
                                table_name, to_col, end_id_sql,
                                // Direction 2: end -> start
                                table_name, from_col, end_id_sql,
                                table_name, to_col, start_id_sql
                            )
                        }
                        (Some(end), false) => {
                            // Named end node, directed: check single direction
                            let (from_match_sql, to_match_sql) = match conn.relationship.direction {
                                Direction::Outgoing => (start_id_sql.clone(), end_id_sql.clone()),
                                Direction::Incoming => (end_id_sql.clone(), start_id_sql.clone()),
                                _ => (start_id_sql.clone(), end_id_sql.clone()),
                            };
                            format!(
                                "NOT EXISTS (SELECT 1 FROM {} WHERE {}.{} = {} AND {}.{} = {})",
                                full_table,
                                table_name,
                                from_col,
                                from_match_sql,
                                table_name,
                                to_col,
                                to_match_sql
                            )
                        }
                    };

                    return Ok(exists_sql);
                }
            }

            // NO FALLBACK - schema is required!
            return Err(RenderBuildError::InvalidRenderPlan(format!(
                "INTERNAL ERROR: Relationship type '{}' not found in schema for EXISTS pattern. This should have been caught during query planning.",
                rel_type
            )));
        }
        PathPattern::Node(_) => Err(RenderBuildError::InvalidRenderPlan(
            "NOT pattern with single node is not supported".to_string(),
        )),
        PathPattern::ShortestPath(_) | PathPattern::AllShortestPaths(_) => {
            Err(RenderBuildError::InvalidRenderPlan(
                "NOT pattern with shortest path is not supported".to_string(),
            ))
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum RenderExpr {
    Literal(Literal),

    /// Raw SQL expression as a string
    Raw(String),

    Star,

    TableAlias(TableAlias),

    ColumnAlias(ColumnAlias),

    Column(Column),

    Parameter(String),

    List(Vec<RenderExpr>),

    AggregateFnCall(AggregateFnCall),

    ScalarFnCall(ScalarFnCall),

    PropertyAccessExp(PropertyAccess),

    OperatorApplicationExp(OperatorApplication),

    Case(RenderCase),

    InSubquery(InSubquery),

    /// EXISTS subquery expression - checks if a pattern exists
    ExistsSubquery(ExistsSubquery),

    /// Reduce expression: fold list into single value
    ReduceExpr(ReduceExpr),

    /// Map literal: {key1: value1, key2: value2}
    /// Used in duration({days: 5}), point({x: 1, y: 2}), etc.
    MapLiteral(Vec<(String, RenderExpr)>),

    /// Pattern count: pre-rendered SQL for size((n)-[:REL]->())
    PatternCount(PatternCount),

    /// Array subscript: array[index]
    /// Access element at specified index (1-based in Cypher, 0-based in ClickHouse)
    ArraySubscript {
        array: Box<RenderExpr>,
        index: Box<RenderExpr>,
    },

    /// Array slicing: array[from..to]
    /// Extract subarray from index 'from' to 'to' (0-based, inclusive in Cypher)
    ArraySlicing {
        array: Box<RenderExpr>,
        from: Option<Box<RenderExpr>>,
        to: Option<Box<RenderExpr>>,
    },

    /// CTE Entity Reference: A node or relationship exported through a WITH clause
    /// Contains information needed to expand the reference to all its properties
    CteEntityRef(CteEntityRef),
}

/// CTE Entity Reference for render plan
/// Represents a node or relationship that was exported through a WITH clause
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CteEntityRef {
    /// Name of the CTE containing the entity's data (e.g., "with_u_cte_1")
    pub cte_name: String,
    /// Original alias of the entity (e.g., "u")
    pub alias: String,
    /// Type of entity: Node or Relationship  
    pub entity_type: EntityType,
    /// List of column names available in the CTE (prefixed with alias_)
    /// e.g., ["u_user_id", "u_name", "u_email"]
    pub columns: Vec<String>,
}

/// Pattern count for size() on patterns
/// Contains pre-rendered SQL for a correlated COUNT(*) subquery
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PatternCount {
    /// Pre-rendered SQL for the pattern count subquery
    pub sql: String,
}

/// Reduce expression for folding a list into a single value
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ReduceExpr {
    /// Name of the accumulator variable
    pub accumulator: String,
    /// Initial value for the accumulator
    pub initial_value: Box<RenderExpr>,
    /// Iteration variable name
    pub variable: String,
    /// List to iterate over
    pub list: Box<RenderExpr>,
    /// Expression evaluated for each element
    pub expression: Box<RenderExpr>,
}

/// EXISTS subquery for render plan
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ExistsSubquery {
    /// Pre-rendered SQL for the EXISTS subquery
    /// This is generated during conversion since EXISTS patterns
    /// don't fit the normal query structure (no select items)
    pub sql: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct InSubquery {
    pub expr: Box<RenderExpr>,
    pub subplan: Box<RenderPlan>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RenderCase {
    /// Expression for simple CASE (CASE x WHEN ...), None for searched CASE
    pub expr: Option<Box<RenderExpr>>,
    /// WHEN conditions and THEN expressions
    pub when_then: Vec<(RenderExpr, RenderExpr)>,
    /// Optional ELSE expression
    pub else_expr: Option<Box<RenderExpr>>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TableAlias(pub String);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnAlias(pub String);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Column(pub PropertyValue);

impl Column {
    pub fn raw(&self) -> &str {
        self.0.raw()
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Operator {
    Addition,
    Subtraction,
    Multiplication,
    Division,
    ModuloDivision,
    Exponentiation,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanEqual,
    GreaterThanEqual,
    RegexMatch, // =~ (regex match)
    And,
    Or,
    In,
    NotIn,
    StartsWith, // STARTS WITH
    EndsWith,   // ENDS WITH
    Contains,   // CONTAINS
    Not,
    Distinct,
    IsNull,
    IsNotNull,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OperatorApplication {
    pub operator: Operator,
    pub operands: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PropertyAccess {
    pub table_alias: TableAlias,
    pub column: PropertyValue, // Use PropertyValue directly
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ScalarFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct AggregateFnCall {
    pub name: String,
    pub args: Vec<RenderExpr>,
}

impl TryFrom<LogicalExpr> for RenderExpr {
    type Error = RenderBuildError;

    fn try_from(expr: LogicalExpr) -> Result<Self, Self::Error> {
        println!(
            "DEBUG TryFrom RenderExpr: Converting LogicalExpr discriminant={:?}",
            std::mem::discriminant(&expr)
        );
        let expression = match expr {
            LogicalExpr::Literal(lit) => {
                crate::debug_println!("DEBUG TryFrom: Converting Literal variant");
                RenderExpr::Literal(lit.try_into()?)
            }
            LogicalExpr::Raw(raw) => RenderExpr::Raw(raw),
            LogicalExpr::Star => RenderExpr::Star,
            LogicalExpr::TableAlias(alias) => RenderExpr::TableAlias(alias.try_into()?),
            LogicalExpr::ColumnAlias(alias) => RenderExpr::ColumnAlias(alias.try_into()?),
            LogicalExpr::Column(col) => RenderExpr::Column(col.try_into()?),
            LogicalExpr::Parameter(s) => RenderExpr::Parameter(s),
            LogicalExpr::List(exprs) => RenderExpr::List(
                exprs
                    .into_iter()
                    .map(RenderExpr::try_from)
                    .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
            ),
            LogicalExpr::AggregateFnCall(agg) => RenderExpr::AggregateFnCall(agg.try_into()?),
            LogicalExpr::ScalarFnCall(fn_call) => RenderExpr::ScalarFnCall(fn_call.try_into()?),
            LogicalExpr::PropertyAccessExp(pa) => RenderExpr::PropertyAccessExp(pa.try_into()?),
            LogicalExpr::OperatorApplicationExp(op) => {
                // Special case: NOT (PathPattern) -> NOT EXISTS subquery
                if op.operator == LogicalOperator::Not && op.operands.len() == 1 {
                    if let LogicalExpr::PathPattern(ref pattern) = op.operands[0] {
                        let not_exists_sql = generate_not_exists_from_path_pattern(pattern)?;
                        return Ok(RenderExpr::Raw(not_exists_sql));
                    }
                }
                RenderExpr::OperatorApplicationExp(op.try_into()?)
            }
            LogicalExpr::InSubquery(subq) => RenderExpr::InSubquery(subq.try_into()?),
            LogicalExpr::Case(case) => RenderExpr::Case(case.try_into()?),
            LogicalExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
                array: Box::new(RenderExpr::try_from(*array)?),
                index: Box::new(RenderExpr::try_from(*index)?),
            },
            LogicalExpr::ArraySlicing { array, from, to } => RenderExpr::ArraySlicing {
                array: Box::new(RenderExpr::try_from(*array)?),
                from: from
                    .map(|f| RenderExpr::try_from(*f))
                    .transpose()?
                    .map(Box::new),
                to: to
                    .map(|t| RenderExpr::try_from(*t))
                    .transpose()?
                    .map(Box::new),
            },
            LogicalExpr::ExistsSubquery(exists) => {
                // For EXISTS subqueries, generate SQL directly since they don't fit
                // the normal RenderPlan structure (no select items needed)
                let sql = generate_exists_sql(&exists)?;
                RenderExpr::ExistsSubquery(ExistsSubquery { sql })
            }
            LogicalExpr::ReduceExpr(reduce) => {
                // Convert LogicalExpr::ReduceExpr to RenderExpr::ReduceExpr
                RenderExpr::ReduceExpr(ReduceExpr {
                    accumulator: reduce.accumulator,
                    initial_value: Box::new(RenderExpr::try_from(*reduce.initial_value)?),
                    variable: reduce.variable,
                    list: Box::new(RenderExpr::try_from(*reduce.list)?),
                    expression: Box::new(RenderExpr::try_from(*reduce.expression)?),
                })
            }
            LogicalExpr::MapLiteral(entries) => {
                // Convert map literal - preserve key-value pairs
                let converted_entries: Result<Vec<(String, RenderExpr)>, RenderBuildError> =
                    entries
                        .into_iter()
                        .map(|(k, v)| Ok((k, RenderExpr::try_from(v)?)))
                        .collect();
                RenderExpr::MapLiteral(converted_entries?)
            }
            LogicalExpr::LabelExpression { variable, label } => {
                // LabelExpression should have been resolved at analysis time
                // If it reaches here, return false (unknown label)
                log::warn!(
                    "LabelExpression {}:{} reached RenderExpr conversion - returning false",
                    variable,
                    label
                );
                RenderExpr::Literal(Literal::Boolean(false))
            }
            LogicalExpr::PatternCount(pc) => {
                // Generate the pattern count SQL (correlated COUNT(*) subquery)
                let sql = generate_pattern_count_sql(&pc.pattern)?;
                RenderExpr::PatternCount(PatternCount { sql })
            }
            LogicalExpr::Lambda(lambda) => {
                // Lambda expressions are rendered directly to ClickHouse lambda syntax
                // Format: param -> body or (param1, param2) -> body
                let params_str = if lambda.params.len() == 1 {
                    lambda.params[0].clone()
                } else {
                    format!("({})", lambda.params.join(", "))
                };
                let body_sql = RenderExpr::try_from(*lambda.body)?.to_sql();
                let lambda_sql = format!("{} -> {}", params_str, body_sql);
                RenderExpr::Raw(lambda_sql)
            }
            LogicalExpr::CteEntityRef(cte_ref) => {
                // CteEntityRef represents a node/relationship from a CTE
                // For TryFrom conversion, we create a placeholder that select_builder will expand
                // The alias references the CTE table with all its prefixed columns
                log::info!(
                    "CteEntityRef '{}' from CTE '{}' reached TryFrom - expanding in select_builder",
                    cte_ref.alias,
                    cte_ref.cte_name
                );
                // Return as TableAlias pointing to the CTE - actual column expansion
                // happens in select_builder.rs where we have access to WITH clause metadata
                RenderExpr::CteEntityRef(CteEntityRef {
                    cte_name: cte_ref.cte_name,
                    alias: cte_ref.alias,
                    entity_type: cte_ref.entity_type,
                    columns: cte_ref.columns,
                })
            }
            // PathPattern is not present in RenderExpr
            _ => unimplemented!("Conversion for this LogicalExpr variant is not implemented"),
        };
        println!(
            "DEBUG TryFrom RenderExpr: Successfully converted to discriminant={:?}",
            std::mem::discriminant(&expression)
        );
        Ok(expression)
    }
}

impl TryFrom<LogicalInSubquery> for InSubquery {
    type Error = RenderBuildError;

    fn try_from(value: LogicalInSubquery) -> Result<Self, Self::Error> {
        // InSubquery needs schema but TryFrom doesn't provide it
        // Use empty schema as fallback (this is rarely used feature)
        use crate::graph_catalog::graph_schema::GraphSchema;
        let empty_schema = GraphSchema::build(
            1,
            "default".to_string(),
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        );
        let sub_plan = value.subplan.clone().to_render_plan(&empty_schema)?;
        let in_sub_query = InSubquery {
            expr: Box::new((value.expr.as_ref().clone()).try_into()?),
            subplan: Box::new(sub_plan),
        };
        Ok(in_sub_query)
    }
}

impl TryFrom<LogicalLiteral> for Literal {
    type Error = RenderBuildError;

    fn try_from(lit: LogicalLiteral) -> Result<Self, Self::Error> {
        let literal = match lit {
            LogicalLiteral::Integer(i) => Literal::Integer(i),
            LogicalLiteral::Float(f) => Literal::Float(f),
            LogicalLiteral::Boolean(b) => Literal::Boolean(b),
            LogicalLiteral::String(s) => Literal::String(s),
            LogicalLiteral::Null => Literal::Null,
        };
        Ok(literal)
    }
}

impl TryFrom<LogicalTableAlias> for TableAlias {
    type Error = RenderBuildError;

    fn try_from(alias: LogicalTableAlias) -> Result<Self, Self::Error> {
        Ok(TableAlias(alias.0))
    }
}

impl TryFrom<LogicalColumnAlias> for ColumnAlias {
    type Error = RenderBuildError;

    fn try_from(alias: LogicalColumnAlias) -> Result<Self, Self::Error> {
        Ok(ColumnAlias(alias.0))
    }
}

impl TryFrom<LogicalColumn> for Column {
    type Error = RenderBuildError;

    fn try_from(col: LogicalColumn) -> Result<Self, Self::Error> {
        Ok(Column(PropertyValue::Column(col.0.clone())))
    }
}

impl TryFrom<LogicalPropertyAccess> for PropertyAccess {
    type Error = RenderBuildError;

    fn try_from(pa: LogicalPropertyAccess) -> Result<Self, Self::Error> {
        Ok(PropertyAccess {
            table_alias: pa.table_alias.try_into()?,
            column: pa.column, // Pass through PropertyValue
        })
    }
}

impl TryFrom<LogicalOperatorApplication> for OperatorApplication {
    type Error = RenderBuildError;

    fn try_from(op: LogicalOperatorApplication) -> Result<Self, Self::Error> {
        let op_app = OperatorApplication {
            operator: op.operator.try_into()?,
            operands: op
                .operands
                .into_iter()
                .map(RenderExpr::try_from)
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
        };
        Ok(op_app)
    }
}

impl TryFrom<LogicalOperator> for Operator {
    type Error = RenderBuildError;

    fn try_from(value: LogicalOperator) -> Result<Self, Self::Error> {
        let operator = match value {
            LogicalOperator::Addition => Operator::Addition,
            LogicalOperator::Subtraction => Operator::Subtraction,
            LogicalOperator::Multiplication => Operator::Multiplication,
            LogicalOperator::Division => Operator::Division,
            LogicalOperator::ModuloDivision => Operator::ModuloDivision,
            LogicalOperator::Exponentiation => Operator::Exponentiation,
            LogicalOperator::Equal => Operator::Equal,
            LogicalOperator::NotEqual => Operator::NotEqual,
            LogicalOperator::LessThan => Operator::LessThan,
            LogicalOperator::GreaterThan => Operator::GreaterThan,
            LogicalOperator::LessThanEqual => Operator::LessThanEqual,
            LogicalOperator::GreaterThanEqual => Operator::GreaterThanEqual,
            LogicalOperator::RegexMatch => Operator::RegexMatch,
            LogicalOperator::And => Operator::And,
            LogicalOperator::Or => Operator::Or,
            LogicalOperator::In => Operator::In,
            LogicalOperator::NotIn => Operator::NotIn,
            LogicalOperator::StartsWith => Operator::StartsWith,
            LogicalOperator::EndsWith => Operator::EndsWith,
            LogicalOperator::Contains => Operator::Contains,
            LogicalOperator::Not => Operator::Not,
            LogicalOperator::Distinct => Operator::Distinct,
            LogicalOperator::IsNull => Operator::IsNull,
            LogicalOperator::IsNotNull => Operator::IsNotNull,
        };
        Ok(operator)
    }
}

impl TryFrom<LogicalScalarFnCall> for ScalarFnCall {
    type Error = RenderBuildError;

    fn try_from(fc: LogicalScalarFnCall) -> Result<Self, Self::Error> {
        let scalar_fn = ScalarFnCall {
            name: fc.name,
            args: fc
                .args
                .into_iter()
                .map(RenderExpr::try_from)
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
        };
        Ok(scalar_fn)
    }
}

impl TryFrom<LogicalAggregateFnCall> for AggregateFnCall {
    type Error = RenderBuildError;

    fn try_from(agg: LogicalAggregateFnCall) -> Result<Self, Self::Error> {
        // Special case: count(node_variable) should become count(*)
        // When counting a graph node (e.g., count(friend)), the argument is a TableAlias
        // which doesn't exist as a column name inside subqueries. Convert to count(*).
        //
        // Special case: collect(node_variable) should NOT be converted yet
        // This requires knowledge of the node's properties which isn't available here.
        // The conversion to groupArray(tuple(...)) happens during WITH projection expansion
        // in plan_builder.rs where we have access to the schema.
        // For now, pass through TableAlias args as-is for collect().
        let converted_args: Vec<RenderExpr> =
            if agg.name.to_lowercase() == "count" && agg.args.len() == 1 {
                match &agg.args[0] {
                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(_) => {
                        // count(node_var) -> count(*)
                        vec![RenderExpr::Star]
                    }
                    _ => agg
                        .args
                        .into_iter()
                        .map(RenderExpr::try_from)
                        .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
                }
            } else if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                match &agg.args[0] {
                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                        // collect(node_var) - keep TableAlias for now
                        // Will be expanded in plan_builder when we have schema context
                        vec![RenderExpr::TableAlias(TableAlias(alias.0.clone()))]
                    }
                    _ => agg
                        .args
                        .into_iter()
                        .map(RenderExpr::try_from)
                        .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?,
                }
            } else {
                agg.args
                    .into_iter()
                    .map(RenderExpr::try_from)
                    .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?
            };

        let agg_fn = AggregateFnCall {
            name: agg.name,
            args: converted_args,
        };
        Ok(agg_fn)
    }
}

impl TryFrom<LogicalCase> for RenderCase {
    type Error = RenderBuildError;

    fn try_from(case: LogicalCase) -> Result<Self, Self::Error> {
        let expr = if let Some(e) = case.expr {
            Some(Box::new(RenderExpr::try_from(*e)?))
        } else {
            None
        };

        let when_then = case
            .when_then
            .into_iter()
            .map(|(when, then)| Ok((RenderExpr::try_from(when)?, RenderExpr::try_from(then)?)))
            .collect::<Result<Vec<(RenderExpr, RenderExpr)>, RenderBuildError>>()?;

        let else_expr = if let Some(e) = case.else_expr {
            Some(Box::new(RenderExpr::try_from(*e)?))
        } else {
            None
        };

        Ok(RenderCase {
            expr,
            when_then,
            else_expr,
        })
    }
}
