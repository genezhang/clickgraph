use std::{collections::HashMap, sync::Arc, time::Instant};

use axum::{extract::State, http::StatusCode, response::Json};

use crate::{
    clickhouse_query_generator, open_cypher_parser,
    query_planner::{self, types::QueryType},
    render_plan::plan_builder::RenderPlanBuilder,
};

use super::{
    graph_catalog,
    models::{
        ErrorDetails, SqlGenerationError, SqlGenerationMetadata, SqlGenerationRequest,
        SqlGenerationResponse,
    },
    query_cache::QueryCacheKey,
    AppState, GLOBAL_QUERY_CACHE,
};

/// Handler for POST /query/sql - Generate SQL without execution (production API)
pub async fn sql_generation_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<SqlGenerationRequest>,
) -> Result<Json<SqlGenerationResponse>, (StatusCode, Json<SqlGenerationError>)> {
    let start_time = Instant::now();

    // Validate target database - only ClickHouse is currently supported
    if !payload.target_database.is_supported() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(SqlGenerationError {
                cypher_query: payload.query.clone(),
                error: format!(
                    "Unsupported target database: '{}'. Currently only 'clickhouse' is supported.",
                    payload.target_database.as_str()
                ),
                error_type: "UnsupportedDialectError".to_string(),
                error_details: Some(ErrorDetails {
                    position: None,
                    line: None,
                    column: None,
                    hint: Some(
                        "Supported dialects: clickhouse. Future: postgresql, duckdb, mysql, sqlite"
                            .to_string(),
                    ),
                }),
            }),
        ));
    }

    // Parse and validate schema name
    // First, do a quick parse to extract USE clause if present
    let clean_query = payload.query.trim();
    let schema_name = if clean_query.to_uppercase().starts_with("USE ") {
        // Quick extraction of schema name from USE clause
        match open_cypher_parser::parse_cypher_statement(clean_query) {
            Ok((_, statement)) => match statement {
                open_cypher_parser::ast::CypherStatement::Query { query, .. } => {
                    if let Some(ref use_clause) = query.use_clause {
                        use_clause.database_name
                    } else {
                        payload.schema_name.as_deref().unwrap_or("default")
                    }
                }
                _ => payload.schema_name.as_deref().unwrap_or("default"),
            },
            Err(_) => payload.schema_name.as_deref().unwrap_or("default"),
        }
    } else {
        payload.schema_name.as_deref().unwrap_or("default")
    };

    // Check query cache first
    let cache_key = QueryCacheKey::new(&payload.query, schema_name);

    let mut cache_status = "MISS";
    let cached_sql = if let Some(cache) = GLOBAL_QUERY_CACHE.get() {
        if let Some(sql) = cache.get(&cache_key) {
            cache_status = "HIT";
            Some(sql)
        } else {
            None
        }
    } else {
        None
    };

    // If we have cached SQL, return it immediately
    if let Some(ch_query) = cached_sql {
        let mut sql_statements = Vec::new();

        // Add SET ROLE if specified
        if let Some(role) = &payload.role {
            sql_statements.push(format!("SET ROLE {}", role));
        }

        // Add the cached query
        sql_statements.push(ch_query);

        let elapsed = start_time.elapsed();

        return Ok(Json(SqlGenerationResponse {
            cypher_query: payload.query.clone(),
            target_database: payload.target_database.as_str().to_string(),
            sql: sql_statements,
            parameters: payload.parameters.clone(),
            view_parameters: payload.view_parameters.clone(),
            role: payload.role.clone(),
            metadata: SqlGenerationMetadata {
                query_type: "unknown".to_string(),
                cache_status: cache_status.to_string(),
                parse_time_ms: 0.0,
                planning_time_ms: 0.0,
                sql_generation_time_ms: 0.0,
                total_time_ms: elapsed.as_secs_f64() * 1000.0,
            },
            logical_plan: None,
            dialect_notes: None,
        }));
    }

    // Get graph schema
    let graph_schema = match graph_catalog::get_graph_schema_by_name(schema_name).await {
        Ok(schema) => schema,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(SqlGenerationError {
                    cypher_query: payload.query.clone(),
                    error: format!("Schema error: {}", e),
                    error_type: "SchemaError".to_string(),
                    error_details: Some(ErrorDetails {
                        position: None,
                        line: None,
                        column: None,
                        hint: Some("Available schemas can be listed via GET /schemas".to_string()),
                    }),
                }),
            ));
        }
    };

    // Clean query (remove CYPHER prefix if present)
    let clean_query = payload.query.trim();
    let clean_query = if clean_query.to_uppercase().starts_with("CYPHER") {
        clean_query
            .split_once(char::is_whitespace)
            .map_or(clean_query, |(_, rest)| rest)
    } else {
        clean_query
    };

    // Phase 1: Parse query (support UNION ALL)
    let parse_start = Instant::now();
    let cypher_statement = match open_cypher_parser::parse_cypher_statement(clean_query) {
        Ok((_, stmt)) => stmt,
        Err(e) => {
            let _parse_time = parse_start.elapsed().as_secs_f64() * 1000.0;
            return Err((
                StatusCode::BAD_REQUEST,
                Json(SqlGenerationError {
                    cypher_query: payload.query.clone(),
                    error: format!("{}", e),
                    error_type: "ParseError".to_string(),
                    error_details: Some(ErrorDetails {
                        position: None,
                        line: None,
                        column: None,
                        hint: Some(
                            "Check Cypher syntax. See docs/wiki/Cypher-Language-Reference.md"
                                .to_string(),
                        ),
                    }),
                }),
            ));
        }
    };
    let parse_time = parse_start.elapsed().as_secs_f64() * 1000.0;

    // Extract the first query for query_type detection
    // For UNION queries, all branches should have the same type
    let first_query = match &cypher_statement {
        open_cypher_parser::ast::CypherStatement::Query { query, .. } => query,
        open_cypher_parser::ast::CypherStatement::ProcedureCall(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(SqlGenerationError {
                    cypher_query: payload.query.clone(),
                    error: "Procedure calls not supported in SQL generation endpoint".to_string(),
                    error_type: "UnsupportedQuery".to_string(),
                    error_details: None,
                }),
            ));
        }
    };

    let query_type = query_planner::get_query_type(first_query);
    let query_type_str = match query_type {
        QueryType::Read => "read",
        QueryType::Ddl => "ddl",
        QueryType::Update => "update",
        QueryType::Delete => "delete",
        QueryType::Call => "call",
        QueryType::Procedure => "procedure",
    }
    .to_string();

    let is_read = query_type == QueryType::Read;
    let is_call = query_type == QueryType::Call;

    let (ch_query, logical_plan_str, planning_time, sql_gen_time): (
        String,
        Option<String>,
        f64,
        f64,
    ) = if is_call {
        // Handle CALL queries (like PageRank)
        // Note: CALL with UNION doesn't make sense, so we use the first query
        let planning_start = Instant::now();
        let logical_plan =
            match query_planner::evaluate_call_query(first_query.clone(), &graph_schema) {
                Ok(plan) => plan,
                Err(e) => {
                    let _planning_time = planning_start.elapsed().as_secs_f64() * 1000.0;
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(SqlGenerationError {
                            cypher_query: payload.query.clone(),
                            error: format!("{}", e),
                            error_type: "PlanningError".to_string(),
                            error_details: None,
                        }),
                    ));
                }
            };
        let planning_time = planning_start.elapsed().as_secs_f64() * 1000.0;

        let sql_gen_start = Instant::now();
        let ch_sql = match &logical_plan {
            crate::query_planner::logical_plan::LogicalPlan::PageRank(pagerank) => {
                use crate::clickhouse_query_generator::pagerank::{
                    PageRankConfig, PageRankGenerator,
                };

                let config = PageRankConfig {
                    iterations: pagerank.iterations,
                    damping_factor: pagerank.damping_factor,
                    convergence_threshold: None,
                };

                let generator = PageRankGenerator::new(
                    &graph_schema,
                    config,
                    pagerank.graph_name.clone(),
                    pagerank.node_labels.clone(),
                    pagerank.relationship_types.clone(),
                );
                match generator.generate_pagerank_sql() {
                    Ok(sql) => sql,
                    Err(e) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(SqlGenerationError {
                                cypher_query: payload.query.clone(),
                                error: format!("{}", e),
                                error_type: "SqlGenerationError".to_string(),
                                error_details: None,
                            }),
                        ));
                    }
                }
            }
            _ => {
                return Err((
                    StatusCode::NOT_IMPLEMENTED,
                    Json(SqlGenerationError {
                        cypher_query: payload.query.clone(),
                        error: "Unsupported CALL query type".to_string(),
                        error_type: "NotImplementedError".to_string(),
                        error_details: None,
                    }),
                ));
            }
        };
        let sql_gen_time = sql_gen_start.elapsed().as_secs_f64() * 1000.0;

        let plan_str = if payload.include_plan.unwrap_or(false) {
            Some(format!("{:#?}", logical_plan))
        } else {
            None
        };

        (ch_sql, plan_str, planning_time, sql_gen_time)
    } else if is_read {
        // Phase 2: Plan query
        let planning_start = Instant::now();

        // Convert view_parameters from Option<HashMap<String, Value>> to Option<HashMap<String, String>>
        let view_parameter_values: Option<HashMap<String, String>> = payload
            .view_parameters
            .as_ref()
            .map(|params: &HashMap<String, serde_json::Value>| {
                params
                    .iter()
                    .map(|(k, v): (&String, &serde_json::Value)| {
                        let string_value = match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            _ => v.to_string(),
                        };
                        (k.clone(), string_value)
                    })
                    .collect()
            });

        let (logical_plan, plan_ctx) = match query_planner::logical_plan::evaluate_cypher_statement(
            cypher_statement,
            &graph_schema,
            None, // tenant_id not needed for SQL generation
            view_parameter_values,
            None, // max_inferred_types
        ) {
            Ok(result) => result,
            Err(e) => {
                let _planning_time = planning_start.elapsed().as_secs_f64() * 1000.0;
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(SqlGenerationError {
                        cypher_query: payload.query.clone(),
                        error: format!("{}", e),
                        error_type: "PlanningError".to_string(),
                        error_details: None,
                    }),
                ));
            }
        };
        let planning_time = planning_start.elapsed().as_secs_f64() * 1000.0;

        // Phase 3: Render plan generation - use _with_ctx to pass VLP endpoint information
        let render_start = Instant::now();
        let render_plan = match logical_plan.to_render_plan_with_ctx(&graph_schema, Some(&plan_ctx), None)
        {
            Ok(plan) => plan,
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(SqlGenerationError {
                        cypher_query: payload.query.clone(),
                        error: format!("{}", e),
                        error_type: "RenderError".to_string(),
                        error_details: None,
                    }),
                ));
            }
        };

        // Phase 4: SQL generation
        let ch_query: String =
            clickhouse_query_generator::generate_sql(render_plan, app_state.config.max_cte_depth);
        let sql_gen_time = render_start.elapsed().as_secs_f64() * 1000.0;

        let plan_str = if payload.include_plan.unwrap_or(false) {
            Some(format!("{:#?}", logical_plan))
        } else {
            None
        };

        (ch_query, plan_str, planning_time, sql_gen_time)
    } else {
        // DDL/Update/Delete operations not supported
        return Err((
            StatusCode::BAD_REQUEST,
            Json(SqlGenerationError {
                cypher_query: payload.query.clone(),
                error: "ClickGraph is read-only. Write operations (CREATE, SET, DELETE, MERGE) are not supported.".to_string(),
                error_type: "ReadOnlyError".to_string(),
                error_details: Some(ErrorDetails {
                    position: None,
                    line: None,
                    column: None,
                    hint: Some("Use ClickHouse INSERT/UPDATE for data modifications".to_string()),
                }),
            }),
        ));
    };

    // Store in cache
    if let Some(cache) = GLOBAL_QUERY_CACHE.get() {
        cache.insert(cache_key, ch_query.clone());
    }

    // Build SQL statements array
    let mut sql_statements = Vec::new();

    // Add SET ROLE if specified
    if let Some(role) = &payload.role {
        sql_statements.push(format!("SET ROLE {}", role));
    }

    // Add the main query
    sql_statements.push(ch_query);

    let total_time = start_time.elapsed().as_secs_f64() * 1000.0;

    Ok(Json(SqlGenerationResponse {
        cypher_query: payload.query.clone(),
        target_database: payload.target_database.as_str().to_string(),
        sql: sql_statements,
        parameters: payload.parameters.clone(),
        view_parameters: payload.view_parameters.clone(),
        role: payload.role.clone(),
        metadata: SqlGenerationMetadata {
            query_type: query_type_str,
            cache_status: cache_status.to_string(),
            parse_time_ms: parse_time,
            planning_time_ms: planning_time,
            sql_generation_time_ms: sql_gen_time,
            total_time_ms: total_time,
        },
        logical_plan: logical_plan_str,
        dialect_notes: None, // Future: Add ClickHouse-specific optimization hints
    }))
}
