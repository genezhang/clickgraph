use std::{collections::HashMap, sync::Arc, time::Instant};

use axum::{extract::State, http::StatusCode, response::Json};

use crate::{
    open_cypher_parser,
    query_planner::{self, types::QueryType},
    server::query_context::{
        attach_current_table_stats, set_current_schema, set_current_schema_name,
        with_query_context, QueryContext,
    },
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
    // Like `/query`: the whole request runs in its own task-local query
    // context (schema, dialect, table stats, per-query alias counters).
    // Without it, translation fell back to process-global state, and
    // concurrent requests rewound each other's generated aliases.
    with_query_context(
        QueryContext::new(None),
        sql_generation_handler_inner(app_state, payload),
    )
    .await
}

async fn sql_generation_handler_inner(
    app_state: Arc<AppState>,
    payload: SqlGenerationRequest,
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
    // First, do a quick parse to extract USE clause if present.
    // Strip comments before parsing (#516 made parse_cypher_statement
    // all-consuming — a trailing `//`/`/* */` comment must not be mistaken
    // for garbage input).
    let stripped_for_use_check = open_cypher_parser::strip_comments(&payload.query);
    let clean_query = stripped_for_use_check.trim();
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
    // Scoped to this endpoint, and to the view parameters the SQL is planned
    // with (they change parameterized-view SQL).
    let vp_strings = view_parameter_strings(&payload);
    let cache_key =
        QueryCacheKey::with_view_scope(&payload.query, schema_name, None, vp_strings.as_ref())
            .for_route("query_sql");

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

        // Add the cached query.
        // #1314: SQL returned for external execution carries the semantic
        // session settings (join_use_nulls) in the statement itself.
        sql_statements.push(crate::sql_generator::portable_sql(
            &ch_query,
            crate::server::query_context::get_current_dialect(),
        ));

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
    // Same per-query setup as `/query`: downstream code reads the schema from
    // the context, and stats-informed planning needs the snapshot before
    // planning.
    set_current_schema_name(Some(schema_name.to_string()));
    set_current_schema(Arc::new(graph_schema.clone()));
    attach_current_table_stats(&graph_schema).await;

    // Clean query (remove CYPHER prefix if present), then strip comments
    // before parsing (#516 made parse_cypher_statement all-consuming — a
    // trailing `//`/`/* */` comment must not be mistaken for garbage input).
    let clean_query = payload.query.trim();
    let clean_query = if clean_query.to_uppercase().starts_with("CYPHER") {
        clean_query
            .split_once(char::is_whitespace)
            .map_or(clean_query, |(_, rest)| rest)
    } else {
        clean_query
    };
    let stripped_query = open_cypher_parser::strip_comments(clean_query);
    let clean_query = stripped_query.trim();

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
        open_cypher_parser::ast::CypherStatement::CopyTo(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(SqlGenerationError {
                    cypher_query: payload.query.clone(),
                    error: "COPY TO statements not supported in SQL generation endpoint"
                        .to_string(),
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
            match query_planner::evaluate_call_query((**first_query).clone(), &graph_schema) {
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

        // The same conversion the cache key used.
        let view_parameter_values = vp_strings.clone();

        // Same pre-pass and pipeline as `/query`, so this endpoint returns
        // the SQL `/query` would run: the id() rewrite, then the translate
        // seam (which runs the analyzer passes this endpoint used to skip).
        use crate::query_planner::ast_transform;
        use crate::server::bolt_protocol::id_mapper::IdMapper;
        let mut id_mapper = IdMapper::new();
        id_mapper.set_scope(Some(schema_name.to_string()), None);
        let ast_arena = ast_transform::StringArena::new();
        let (cypher_statement, _label_constraints) = ast_transform::transform_id_functions(
            &ast_arena,
            cypher_statement,
            &id_mapper,
            Some(&graph_schema),
        );

        let translation = match crate::translate::translate_read(
            cypher_statement,
            &graph_schema,
            crate::translate::ReadOptions {
                view_parameter_values,
                max_cte_depth: app_state.config.max_cte_depth,
                ..Default::default()
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                let error_type = match e {
                    crate::translate::TranslateError::Planning(_) => "PlanningError",
                    crate::translate::TranslateError::Render(_) => "RenderError",
                };
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(SqlGenerationError {
                        cypher_query: payload.query.clone(),
                        error: format!("{}", e),
                        error_type: error_type.to_string(),
                        error_details: None,
                    }),
                ));
            }
        };
        let planning_time = translation.timings.planning.as_secs_f64() * 1000.0;
        let sql_gen_time = (translation.timings.render + translation.timings.sql_generation)
            .as_secs_f64()
            * 1000.0;
        let crate::translate::ReadTranslation {
            sql: ch_query,
            logical_plan,
            ..
        } = translation;

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
    // #1314: SQL returned for external execution carries the semantic
    // session settings (join_use_nulls) in the statement itself.
    sql_statements.push(crate::sql_generator::portable_sql(
        &ch_query,
        crate::server::query_context::get_current_dialect(),
    ));

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

/// The request's view parameters as strings: used for BOTH the cache key and
/// planning, so the two can never disagree.
fn view_parameter_strings(payload: &SqlGenerationRequest) -> Option<HashMap<String, String>> {
    payload.view_parameters.as_ref().map(|params| {
        params
            .iter()
            .map(|(k, v)| {
                let string_value = match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => v.to_string(),
                };
                (k.clone(), string_value)
            })
            .collect()
    })
}
