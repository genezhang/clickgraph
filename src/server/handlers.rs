use std::{collections::HashMap, sync::Arc, time::Instant};

use axum::{
    extract::State,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncBufReadExt;

use crate::{
    clickhouse_query_generator,
    graph_catalog::graph_schema::GraphSchemaElement,
    graph_catalog::{DraftOptions, DraftRequest, EdgeHint, FkEdgeHint, NodeHint, SchemaDiscovery},
    open_cypher_parser::{self, ast::CypherStatement},
    query_planner::{self, types::QueryType},
    render_plan::plan_builder::RenderPlanBuilder,
};

use super::{
    graph_catalog,
    models::{OutputFormat, QueryRequest, SqlOnlyResponse},
    parameter_substitution, query_cache,
    query_context::{with_query_context, QueryContext},
    AppState, GLOBAL_QUERY_CACHE,
};

/// Merge view_parameters and query parameters into a single HashMap
///
/// Both view_parameters and parameters can contain values that need to be substituted
/// in the SQL template. View parameters (like tenant_id) and query parameters (like $userId)
/// are merged, with query parameters taking precedence in case of conflicts.
fn merge_parameters(
    query_params: &Option<std::collections::HashMap<String, Value>>,
    view_params: &Option<std::collections::HashMap<String, Value>>,
) -> Option<std::collections::HashMap<String, Value>> {
    match (query_params, view_params) {
        (None, None) => None,
        (Some(p), None) => Some(p.clone()),
        (None, Some(v)) => Some(v.clone()),
        (Some(p), Some(v)) => {
            let mut merged = v.clone();
            merged.extend(p.clone()); // Query params override view params
            Some(merged)
        }
    }
}

/// Performance metrics for query execution
#[derive(Debug, Clone)]
pub struct QueryPerformanceMetrics {
    pub total_time: f64,
    pub parse_time: f64,
    pub planning_time: f64,
    pub render_time: f64,
    pub sql_generation_time: f64,
    pub execution_time: f64,
    pub query_type: String,
    pub sql_queries_count: usize,
    pub result_rows: Option<usize>,
}

impl Default for QueryPerformanceMetrics {
    fn default() -> Self {
        Self {
            total_time: 0.0,
            parse_time: 0.0,
            planning_time: 0.0,
            render_time: 0.0,
            sql_generation_time: 0.0,
            execution_time: 0.0,
            query_type: "unknown".to_string(),
            sql_queries_count: 0,
            result_rows: None,
        }
    }
}

/// Response for SHOW DATABASES command (Neo4j browser compatibility)
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseListResponse {
    pub databases: Vec<serde_json::Value>,
}

impl QueryPerformanceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn log_performance(&self, query: &str) {
        log::info!(
            "Query performance - Total: {:.3}ms, Parse: {:.3}ms, Planning: {:.3}ms, Render: {:.3}ms, SQL Gen: {:.3}ms, Exec: {:.3}ms, Type: {}, Queries: {}, Rows: {}",
            self.total_time * 1000.0,
            self.parse_time * 1000.0,
            self.planning_time * 1000.0,
            self.render_time * 1000.0,
            self.sql_generation_time * 1000.0,
            self.execution_time * 1000.0,
            self.query_type,
            self.sql_queries_count,
            self.result_rows
                .map_or("N/A".to_string(), |r| r.to_string())
        );

        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "Performance breakdown for query: {}",
                query.chars().take(100).collect::<String>()
            );
        }
    }

    pub fn to_headers(&self) -> Vec<(String, String)> {
        vec![
            (
                "X-Query-Total-Time".to_string(),
                format!("{:.3}ms", self.total_time * 1000.0),
            ),
            (
                "X-Query-Parse-Time".to_string(),
                format!("{:.3}ms", self.parse_time * 1000.0),
            ),
            (
                "X-Query-Planning-Time".to_string(),
                format!("{:.3}ms", self.planning_time * 1000.0),
            ),
            (
                "X-Query-Render-Time".to_string(),
                format!("{:.3}ms", self.render_time * 1000.0),
            ),
            (
                "X-Query-SQL-Gen-Time".to_string(),
                format!("{:.3}ms", self.sql_generation_time * 1000.0),
            ),
            (
                "X-Query-Execution-Time".to_string(),
                format!("{:.3}ms", self.execution_time * 1000.0),
            ),
            ("X-Query-Type".to_string(), self.query_type.clone()),
            (
                "X-Query-SQL-Count".to_string(),
                self.sql_queries_count.to_string(),
            ),
        ]
    }
}

/// Simple health check endpoint
pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "service": "clickgraph",
        "status": "healthy",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

/// Simple test endpoint
pub async fn simple_test_handler() -> impl IntoResponse {
    println!("DEBUG: simple_test_handler called!");
    "Hello from simple test"
}

pub async fn query_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<QueryRequest>,
) -> Result<impl IntoResponse, impl IntoResponse> {
    let start_time = Instant::now();
    let metrics = QueryPerformanceMetrics::new();

    log::debug!("Query handler called with query: {}", payload.query);

    // Extract all needed fields from payload BEFORE any partial moves
    // Use clone() or take() to avoid partial move issues
    let output_format = payload.format.clone().unwrap_or(OutputFormat::JSONEachRow);
    let sql_only = payload.sql_only.unwrap_or(false);
    let query_string = payload.query.clone();
    let schema_name_param = payload.schema_name.clone();

    // Query cache integration - Strip CYPHER prefix FIRST
    // Extract replan option and clean query
    let replan_option = query_cache::ReplanOption::from_query_prefix(&query_string)
        .unwrap_or(query_cache::ReplanOption::Default);
    let clean_query_with_comments = query_cache::ReplanOption::strip_prefix(&query_string);

    // Strip SQL-style comments (-- and /* */) before parsing
    let clean_query_string = open_cypher_parser::strip_comments(clean_query_with_comments);
    let clean_query = clean_query_string.clone();

    // Handle SHOW DATABASES early (special case for Neo4j browser compatibility)
    let clean_upper = clean_query.trim().to_uppercase();
    if clean_upper.starts_with("SHOW DATABASES") {
        log::info!("📊 SHOW DATABASES query detected - returning available schemas");

        // Use shared SHOW DATABASES implementation
        let databases_result = crate::procedures::show_databases::execute_show_databases();

        let databases: Vec<serde_json::Value> = match databases_result {
            Ok(db_list) => db_list
                .into_iter()
                .map(|db| serde_json::to_value(db).unwrap())
                .collect(),
            Err(e) => {
                log::error!("Failed to execute SHOW DATABASES: {}", e);
                vec![]
            }
        };

        let response = DatabaseListResponse { databases };
        return Ok(Json(response).into_response());
    }

    // Intercept apoc.meta.schema MCP queries that use UNWIND + map projection.
    // The procedure executor can handle simple CALL, but the MCP query pattern
    // (UNWIND keys(value) AS key WITH key, value[key] ...) cannot be parsed/executed.
    // Simple CALL apoc.meta.schema() falls through to the normal procedure dispatch below.
    if clean_upper.contains("APOC.META.SCHEMA") && clean_upper.contains("UNWIND") {
        log::info!("Detected apoc.meta.schema MCP query — short-circuiting with unwound results");

        // Determine schema: payload param > USE clause > "default"
        let schema_name = schema_name_param
            .clone()
            .or_else(|| extract_schema_from_use_clause(&clean_query))
            .unwrap_or_else(|| "default".to_string());

        let schema_guard = crate::server::GLOBAL_SCHEMAS.get().ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Schema registry not initialized".to_string(),
            )
        })?;
        let schemas = schema_guard.read().await;
        let schema = schemas.get(&schema_name).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("Schema not found: {}", schema_name),
            )
        })?;

        match crate::procedures::apoc_meta_schema::execute_unwound(schema) {
            Ok(results) => {
                let response_json = crate::procedures::executor::format_as_json(results);
                return Ok(Json(response_json).into_response());
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("apoc.meta.schema execution failed: {}", e),
                ));
            }
        }
    }

    // Handle procedure calls early (before query context)
    // Parse to check if it's a procedure call or procedure-only query
    let (_is_procedure, is_union, proc_name_opt) =
        if let Ok((_, parsed_stmt)) = open_cypher_parser::parse_cypher_statement(&clean_query) {
            log::debug!("Parse succeeded for query: {}", &clean_query);

            // Check if it's a procedure-only statement
            let proc_check = crate::procedures::is_procedure_only_statement(&parsed_stmt);

            // Check if it's a procedure UNION
            let union_check = crate::procedures::is_procedure_union_query(&parsed_stmt);
            log::debug!(
                "Procedure check: {}, Union check: {}",
                proc_check,
                union_check
            );

            // Extract procedure name for standalone procedures (non-UNION)
            let proc_name = if proc_check && !union_check {
                match &parsed_stmt {
                    CypherStatement::ProcedureCall(proc_call) => {
                        Some(proc_call.procedure_name.to_string())
                    }
                    CypherStatement::Query { query, .. } => query
                        .call_clause
                        .as_ref()
                        .map(|cc| cc.procedure_name.to_string()),
                }
            } else {
                None
            };

            (proc_check, union_check, proc_name)
        } else {
            log::debug!("Parse FAILED for query: {}", &clean_query);
            (false, false, None)
        };

    if is_union {
        log::info!("Executing UNION ALL of procedures");

        // Extract procedure names BEFORE any async calls
        let proc_names = match crate::procedures::extract_procedure_names_from_union(&clean_query) {
            Ok(names) => names,
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("Failed to parse UNION query: {}", e),
                ));
            }
        };

        let registry = crate::procedures::ProcedureRegistry::new();
        let schema_name = schema_name_param.unwrap_or_else(|| "default".to_string());

        // Now execute (no lifetimes involved)
        let results =
            crate::procedures::execute_procedure_union(proc_names, &schema_name, &registry).await;

        match results {
            Ok(r) => {
                let response_json = crate::procedures::executor::format_as_json(r);
                return Ok(Json(response_json).into_response());
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Procedure union execution failed: {}", e),
                ));
            }
        }
    }

    if let Some(proc_name) = proc_name_opt {
        log::info!("Executing procedure: {}", proc_name);

        let registry = crate::procedures::ProcedureRegistry::new();
        let schema_name = schema_name_param.unwrap_or_else(|| "default".to_string());

        // Check if procedure exists
        if !registry.contains(&proc_name) {
            return Err((
                StatusCode::NOT_FOUND,
                format!("Unknown procedure: {}", proc_name),
            ));
        }

        // Execute procedure
        let results = crate::procedures::executor::execute_procedure_by_name(
            &proc_name,
            &schema_name,
            &registry,
        )
        .await;

        match results {
            Ok(r) => {
                let response_json = crate::procedures::executor::format_as_json(r);
                return Ok(Json(response_json).into_response());
            }
            Err(e) => {
                let msg = e.to_string();
                // Map known client-side errors (e.g., unknown schema) to 4xx instead of 500.
                // This keeps INTERNAL_SERVER_ERROR reserved for genuine server failures.
                if msg.contains("Schema not found") {
                    return Err((
                        StatusCode::NOT_FOUND,
                        format!("Procedure execution failed: {}", msg),
                    ));
                }
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Procedure execution failed: {}", msg),
                ));
            }
        }
    }

    // 🔧 FIX: Validate query syntax FIRST before schema lookup
    // This prevents misleading "Schema not found" errors when query has syntax errors
    // Quick syntax validation (doesn't need full planning)
    // Note: Use parse_cypher_statement to support UNION ALL queries
    let schema_name = match open_cypher_parser::parse_cypher_statement(&clean_query) {
        Ok((_, statement)) => {
            // Parse succeeded - extract schema name from USE clause
            match statement {
                open_cypher_parser::ast::CypherStatement::Query { query, .. } => {
                    if let Some(ref use_clause) = query.use_clause {
                        use_clause.database_name.to_string()
                    } else {
                        // No USE clause - use request parameter or "default"
                        schema_name_param.unwrap_or_else(|| "default".to_string())
                    }
                }
                open_cypher_parser::ast::CypherStatement::ProcedureCall(_) => {
                    // Procedure calls don't have USE clauses
                    schema_name_param.unwrap_or_else(|| "default".to_string())
                }
            }
        }
        Err(e) => {
            // ❌ PARSE ERROR: Return immediately with clear error message
            // Don't proceed to schema lookup (which would give misleading "Schema not found")
            log::error!("Query parse failed during schema extraction: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "Query syntax error: {}. Check Cypher syntax before proceeding.",
                    e
                ),
            ));
        }
    };

    log::debug!(
        "Using schema: {} ({})",
        schema_name,
        if payload.schema_name.is_none() && !clean_query.to_uppercase().contains("USE ") {
            "explicit default - no USE clause"
        } else {
            "from query or parameter"
        }
    );

    // ✅ TASK-LOCAL CONTEXT: Wrap ALL query processing in with_query_context()
    // This creates an isolated per-task context that is:
    // - Automatically available to ALL phases (planning, rendering, SQL generation)
    // - Isolated from concurrent queries on the same OS thread
    // - Automatically cleaned up when the task completes
    let context = QueryContext::new(Some(schema_name.clone()));

    with_query_context(context, async move {
        query_handler_inner(
            app_state,
            payload,
            schema_name,
            clean_query,
            output_format,
            sql_only,
            replan_option,
            start_time,
            metrics,
        )
        .await
    })
    .await
}

/// Inner query handler logic - runs within task-local context
async fn query_handler_inner(
    app_state: Arc<AppState>,
    payload: QueryRequest,
    schema_name: String,
    clean_query: String,
    output_format: OutputFormat,
    sql_only: bool,
    replan_option: query_cache::ReplanOption,
    start_time: Instant,
    mut metrics: QueryPerformanceMetrics,
) -> Result<Response, (StatusCode, String)> {
    // Convert view_parameters to String values for cache key
    let vp_strings: Option<HashMap<String, String>> =
        payload.view_parameters.as_ref().map(|params| {
            params
                .iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), s)
                })
                .collect()
        });
    let cache_key = query_cache::QueryCacheKey::with_view_scope(
        &clean_query,
        &schema_name,
        payload.tenant_id.as_deref(),
        vp_strings.as_ref(),
    );
    let mut cache_status = "MISS";

    // Try cache lookup (unless replan=force)
    let cached_sql = if replan_option != query_cache::ReplanOption::Force {
        if let Some(cache) = GLOBAL_QUERY_CACHE.get() {
            if let Some(sql) = cache.get(&cache_key) {
                log::debug!("Cache HIT for query");
                cache_status = "HIT";
                Some(sql)
            } else {
                log::debug!("Cache MISS for query");
                None
            }
        } else {
            None
        }
    } else {
        if replan_option == query_cache::ReplanOption::Force {
            log::debug!("Cache BYPASS (replan=force)");
            cache_status = "BYPASS";
        }
        None
    };

    // If cache hit, substitute parameters and return early
    if let Some(sql_template) = cached_sql {
        log::info!("Using cached SQL template");

        // Merge view_parameters and query parameters for substitution
        let all_params = merge_parameters(&payload.parameters, &payload.view_parameters);

        // Substitute parameters if provided
        let final_sql = if let Some(params) = &all_params {
            match parameter_substitution::substitute_parameters(&sql_template, params) {
                Ok(sql) => sql,
                Err(e) => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("Parameter substitution error: {}", e),
                    ));
                }
            }
        } else {
            sql_template
        };

        // Check for unsubstituted $param placeholders before executing
        if let Some(missing_param) =
            parameter_substitution::find_unsubstituted_parameter(&final_sql)
        {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Missing required parameter: '{}'. Parameterized views require view_parameters to be provided.", missing_param),
            ));
        }

        // If SQL-only mode, return SQL without executing
        if sql_only {
            let sql_response = Json(SqlOnlyResponse {
                cypher_query: payload.query.clone(),
                generated_sql: final_sql.clone(),
                execution_mode: "sql_only".to_string(),
            });

            let mut response = sql_response.into_response();
            if let Ok(cache_header) = axum::http::HeaderValue::try_from(cache_status) {
                response
                    .headers_mut()
                    .insert("X-Query-Cache-Status", cache_header);
            }
            return Ok(response);
        }

        // Execute query and return
        let ch_sql_queries = vec![final_sql];
        let execution_start = Instant::now();
        let response = execute_cte_queries(
            app_state,
            ch_sql_queries,
            output_format,
            all_params, // Use merged parameters
            payload.role.clone(),
        )
        .await;
        metrics.execution_time = execution_start.elapsed().as_secs_f64();

        let elapsed = start_time.elapsed();
        metrics.total_time = elapsed.as_secs_f64();

        match response {
            Ok(mut resp) => {
                log::info!("✓ Query succeeded (cached) in {:.2}ms", elapsed.as_millis());

                // Add cache status header to response
                let headers = resp.headers_mut();
                headers.insert("X-Query-Cache-Status", HeaderValue::from_static("HIT"));

                return Ok(resp);
            }
            Err(e) => return Err(e),
        }
    }

    let (ch_sql_queries, maybe_schema_elem, is_read, query_type_str) = {
        // ✅ FAIL LOUDLY: If schema not found, return clear error (no silent fallback)
        let graph_schema = match graph_catalog::get_graph_schema_by_name(&schema_name).await {
            Ok(schema) => schema,
            Err(e) => {
                let available = graph_catalog::list_available_schemas().await;
                log::error!(
                    "Schema '{}' not found. Available schemas: {:?}",
                    schema_name,
                    available
                );
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("{} Available schemas: {:?}", e, available),
                ));
            }
        };

        // Set the resolved schema in task-local context so all downstream
        // code can access it via get_current_schema() without GLOBAL_SCHEMAS lookups
        crate::server::query_context::set_current_schema(std::sync::Arc::new(graph_schema.clone()));

        // Phase 1: Parse query with UNION support
        // IMPORTANT: Parse the CLEAN query without CYPHER prefix
        let parse_start = Instant::now();
        let parsed_stmt = match open_cypher_parser::parse_cypher_statement(&clean_query) {
            Ok((_remaining, stmt)) => stmt,
            Err(e) => {
                metrics.parse_time = parse_start.elapsed().as_secs_f64();
                log::error!("Query parse failed: {:?}", e);
                // Return 400 for parse errors (both sql_only and normal mode)
                return Err((StatusCode::BAD_REQUEST, format!("Parse error: {}", e)));
            }
        };

        // Phase 1.5: Transform id() functions (same as Bolt protocol does)
        // This converts id(alias) = N to proper property comparisons
        // NOTE: HTTP is stateless, so we create a temporary IdMapper per request
        use crate::query_planner::ast_transform;
        use crate::server::bolt_protocol::id_mapper::IdMapper;

        let mut id_mapper = IdMapper::new();
        id_mapper.set_scope(Some(schema_name.clone()), None); // HTTP has no tenant_id
        let ast_arena = ast_transform::StringArena::new();
        let (cypher_statement, _label_constraints) = ast_transform::transform_id_functions(
            &ast_arena,
            parsed_stmt,
            &id_mapper,
            Some(&graph_schema),
        );

        metrics.parse_time = parse_start.elapsed().as_secs_f64();

        let query_type = query_planner::get_statement_query_type(&cypher_statement);
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

        if is_call {
            // Handle CALL queries (like PageRank) - use first query's AST
            let query_ast = match &cypher_statement {
                CypherStatement::Query { query, .. } => query,
                CypherStatement::ProcedureCall(_) => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "Standalone procedure calls should not reach this path".to_string(),
                    ));
                }
            };
            let logical_plan =
                match query_planner::evaluate_call_query(query_ast.clone(), &graph_schema) {
                    Ok(plan) => plan,
                    Err(e) => {
                        // Return 400 for call planning errors (both sql_only and normal mode)
                        return Err((
                            StatusCode::BAD_REQUEST,
                            format!("CALL planning error: {}", e),
                        ));
                    }
                };

            // For CALL queries, we need to generate SQL directly from the logical plan
            // Since PageRank generates complete SQL, we'll use a special approach
            let ch_sql = match &logical_plan {
                crate::query_planner::logical_plan::LogicalPlan::PageRank(pagerank) => {
                    // Generate PageRank SQL directly
                    use crate::clickhouse_query_generator::pagerank::PageRankConfig;
                    use crate::clickhouse_query_generator::pagerank::PageRankGenerator;

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
                            // Return 500 for PageRank SQL generation errors
                            return Err((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("PageRank SQL generation error: {}", e),
                            ));
                        }
                    }
                }
                _ => {
                    // For other CALL queries (not implemented yet)
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "Unsupported CALL query type".to_string(),
                    ));
                }
            };

            // If SQL-only mode, return the SQL without executing
            if sql_only {
                let sql_response = SqlOnlyResponse {
                    cypher_query: payload.query.clone(),
                    generated_sql: ch_sql.clone(),
                    execution_mode: "sql_only".to_string(),
                };
                return Ok(Json(sql_response).into_response());
            }

            (vec![ch_sql], None, true, query_type_str)
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

            log::debug!(
                "Handler: view_parameters from request: {:?}",
                payload.view_parameters
            );
            log::debug!(
                "Handler: converted view_parameter_values: {:?}",
                view_parameter_values
            );

            // Reset global counters for deterministic SQL generation
            crate::query_planner::logical_plan::reset_all_counters();

            let (logical_plan, plan_ctx) = match query_planner::evaluate_read_statement(
                cypher_statement,
                &graph_schema,
                payload.tenant_id.clone(),
                view_parameter_values,
                payload.max_inferred_types,
            ) {
                Ok(result) => result,
                Err(e) => {
                    metrics.planning_time = planning_start.elapsed().as_secs_f64();
                    // Return 400 for planning errors (both sql_only and normal mode)
                    return Err((StatusCode::BAD_REQUEST, format!("Planning error: {}", e)));
                }
            };
            metrics.planning_time = planning_start.elapsed().as_secs_f64();

            // Phase 3: Render plan generation
            let render_start = Instant::now();

            // Schema context is already set via with_query_context() at handler entry
            // Use to_render_plan_with_ctx to pass analysis-phase metadata (VLP endpoints, etc.)

            let render_plan =
                match logical_plan.to_render_plan_with_ctx(&graph_schema, Some(&plan_ctx), None) {
                    Ok(plan) => plan,
                    Err(e) => {
                        metrics.render_time = render_start.elapsed().as_secs_f64();
                        // Return 500 for render errors (internal error)
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Render error: {}", e),
                        ));
                    }
                };
            metrics.render_time = render_start.elapsed().as_secs_f64();

            // Phase 4: SQL generation
            let sql_generation_start = Instant::now();
            let ch_query = clickhouse_query_generator::generate_sql(
                render_plan,
                app_state.config.max_cte_depth,
            );
            metrics.sql_generation_time = sql_generation_start.elapsed().as_secs_f64();
            crate::debug_println!("\n ch_query \n {} \n", ch_query);

            // Store in cache (even in sql_only mode for future use)
            if let Some(cache) = GLOBAL_QUERY_CACHE.get() {
                cache.insert(cache_key.clone(), ch_query.clone());
                log::debug!("Stored SQL template in cache");
            }

            // If SQL-only mode, return the SQL without executing
            if sql_only {
                let sql_response = Json(SqlOnlyResponse {
                    cypher_query: payload.query.clone(),
                    generated_sql: ch_query.clone(),
                    execution_mode: "sql_only".to_string(),
                });

                // Add cache status header
                let mut response = sql_response.into_response();
                if let Ok(cache_header) = axum::http::HeaderValue::try_from(cache_status) {
                    response
                        .headers_mut()
                        .insert("X-Query-Cache-Status", cache_header);
                }
                return Ok(response);
            }

            (vec![ch_query], None, true, query_type_str)
        } else {
            // DDL operations not supported - ClickGraph is read-only
            return Err((
                StatusCode::BAD_REQUEST,
                "DDL operations (CREATE/SET/DELETE) not supported. ClickGraph is a read-only query engine. Use YAML schemas to define graph views.".to_string(),
            ));
        }
    };

    // Phase 5: Execute query
    let execution_start = Instant::now();
    let sql_queries_count = ch_sql_queries.len();

    // Merge view_parameters and query parameters for substitution
    let all_params = merge_parameters(&payload.parameters, &payload.view_parameters);

    let response = if is_read {
        execute_cte_queries(
            app_state,
            ch_sql_queries,
            output_format,
            all_params, // Use merged parameters
            payload.role.clone(),
        )
        .await
    } else {
        ddl_handler(
            app_state.clickhouse_client.clone(),
            ch_sql_queries,
            maybe_schema_elem,
        )
        .await
    };
    metrics.execution_time = execution_start.elapsed().as_secs_f64();

    // Complete metrics collection
    metrics.total_time = start_time.elapsed().as_secs_f64();
    metrics.query_type = query_type_str;
    metrics.sql_queries_count = sql_queries_count;

    // Extract result count if available (for read queries)
    if let Ok(ref resp) = response {
        if let Some(result_count) = extract_result_count(resp) {
            metrics.result_rows = Some(result_count);
        }
    }

    // Log performance metrics
    metrics.log_performance(&payload.query);

    // Add performance headers to response
    match response {
        Ok(mut resp) => {
            let headers = metrics.to_headers();
            for (key, value) in headers {
                if let (Ok(header_name), Ok(header_value)) = (
                    axum::http::HeaderName::try_from(key),
                    axum::http::HeaderValue::try_from(value),
                ) {
                    resp.headers_mut().insert(header_name, header_value);
                }
            }

            // Add cache status header
            if let Ok(cache_header) = axum::http::HeaderValue::try_from(cache_status) {
                resp.headers_mut()
                    .insert("X-Query-Cache-Status", cache_header);
            }

            Ok(resp)
        }
        Err(e) => Err(e),
    }
}

// pub async fn query_handler_old(
//     State(app_state): State<Arc<AppState>>,
//     Json(payload): Json<QueryRequest>,
// ) -> Result<Response, (StatusCode, String)> {
//     let instant = Instant::now();

//     let graph_schema = graph_meta::get_graph_schema().await;

//     let output_format = payload.format.unwrap_or(OutputFormat::JSONEachRow);

//     // parse cypher query
//     let cypher_ast = open_cypher_parser::parse_query(&payload.query).map_err(|e| {
//         (
//             axum::http::StatusCode::INTERNAL_SERVER_ERROR,
//             format!("Brahmand Error: {}", e),
//         )
//     })?;

//     let mut traversal_mode = TraversalMode::Cte;

//     if let Some(mode) = payload.mode {
//         traversal_mode = mode
//     }

//     // TODO convert this error to axum error with proper message. Expose the module name in traces but not to users
//     let (query_type, ch_sql_queries, graph_schema_element_opt) =
//         query_planner::evaluate_query(cypher_ast, &traversal_mode, &graph_schema).map_err(|e| {
//             (
//                 axum::http::StatusCode::INTERNAL_SERVER_ERROR,
//                 format!("Brahmand Error: {}", e),
//             )
//         })?;
//         // query_engine::evaluate_query(cypher_ast, &traversal_mode, &graph_schema).map_err(|e| {
//         //     (
//         //         axum::http::StatusCode::INTERNAL_SERVER_ERROR,
//         //         format!("Brahmand Error: {}", e),
//         //     )
//         // })?;
//     if query_type == QueryType::Ddl {
//         return ddl_handler(
//             app_state.clickhouse_client.clone(),
//             ch_sql_queries,
//             graph_schema_element_opt,
//         )
//         .await;
//     }

//     if traversal_mode == TraversalMode::Cte {
//         execute_cte_queries(app_state, ch_sql_queries, output_format, instant).await
//     } else {
//         execute_temp_table_queries(app_state, ch_sql_queries, output_format, instant).await
//     }
// }

async fn execute_cte_queries(
    app_state: Arc<AppState>,
    ch_sql_queries: Vec<String>,
    output_format: OutputFormat,
    parameters: Option<std::collections::HashMap<String, Value>>,
    role: Option<String>,
) -> Result<Response, (StatusCode, String)> {
    let ch_query_string = ch_sql_queries.join(" ");

    // Substitute parameters if provided
    let final_sql = if let Some(params) = parameters {
        match parameter_substitution::substitute_parameters(&ch_query_string, &params) {
            Ok(sql) => sql,
            Err(e) => {
                log::error!("Parameter substitution failed: {}", e);
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("Parameter substitution error: {}", e),
                ));
            }
        }
    } else {
        ch_query_string.clone()
    };

    // Check for unsubstituted $param placeholders before executing
    if let Some(missing_param) = parameter_substitution::find_unsubstituted_parameter(&final_sql) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Missing required parameter: '{}'. Parameterized views require view_parameters to be provided.", missing_param),
        ));
    }

    // Log full SQL for debugging (especially helpful when ClickHouse truncates errors)
    log::debug!("Executing SQL:\n{}", final_sql);

    // Get role-based connection from pool
    // Note: We use role-specific connection pools instead of SET ROLE for better performance
    // and to avoid race conditions. The connection pool maintains separate pools per role.
    let client = app_state.connection_pool.get_client(role.as_deref()).await;

    if output_format == OutputFormat::Pretty
        || output_format == OutputFormat::PrettyCompact
        || output_format == OutputFormat::Csv
        || output_format == OutputFormat::CSVWithNames
    {
        let mut lines = client
            .query(&final_sql)
            .fetch_bytes(output_format)
            .map_err(|e| {
                // Log full SQL on error for debugging
                log::error!(
                    "ClickHouse query failed. SQL was:\n{}\nError: {}",
                    final_sql,
                    e
                );
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Clickhouse Error: {}", e),
                )
            })?
            .lines();

        let mut rows: Vec<String> = vec![];
        while let Some(line) = lines.next_line().await.map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })? {
            // let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
            rows.push(line);
        }

        let text = rows.join("\n");

        let mut response = (StatusCode::OK, text).into_response();
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        Ok(response)
    } else {
        let mut lines = client
            .query(&final_sql)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| {
                // Log full SQL on error for debugging
                log::error!(
                    "ClickHouse query failed. SQL was:\n{}\nError: {}",
                    final_sql,
                    e
                );
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Clickhouse Error: {}", e),
                )
            })?
            .lines();

        let mut rows: Vec<Value> = vec![];
        while let Some(line) = lines.next_line().await.map_err(|e| {
            // Log full SQL on error for debugging
            log::error!(
                "ClickHouse response parsing failed. SQL was:\n{}\nError: {}",
                final_sql,
                e
            );
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })? {
            let value: serde_json::Value = serde_json::de::from_str(&line).map_err(|e| {
                log::error!("Failed to parse JSON from ClickHouse response: {}", e);
                log::error!("Invalid JSON line: {}", line);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Invalid JSON from ClickHouse: {}", e),
                )
            })?;
            rows.push(value);
        }

        // Wrap results in an object with "results" key for consistency with Neo4j format
        let response_obj = serde_json::json!({
            "results": rows
        });

        Ok(Json(response_obj).into_response())
    }
}

// async fn execute_temp_table_queries(
//     app_state: Arc<AppState>,
//     mut ch_sql_queries: Vec<String>,
//     output_format: OutputFormat,
//     instant: Instant,
// ) -> Result<Response, (StatusCode, String)> {
//     let session_id = Uuid::new_v4();
//     let ch_client = app_state
//         .clickhouse_client
//         .clone()
//         .with_option("session_id", session_id);
//     let last_query = ch_sql_queries.pop().unwrap();

//     for ch_query in ch_sql_queries {
//         println!("\n ch_query -> {:?}", ch_query);
//         let ch_client = app_state
//             .clickhouse_client
//             .clone()
//             .with_option("session_id", session_id);
//         ch_client
//             .query(&ch_query)
//             .execute()
//             .await
//             .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
//     }

//     if output_format == OutputFormat::Pretty
//         || output_format == OutputFormat::PrettyCompact
//         || output_format == OutputFormat::Csv
//         || output_format == OutputFormat::CSVWithNames
//     {
//         let mut lines = ch_client
//             .query(&last_query)
//             .fetch_bytes(output_format)
//             .map_err(|e| {
//                 (
//                     axum::http::StatusCode::INTERNAL_SERVER_ERROR,
//                     format!("Clickhouse Error: {}", e),
//                 )
//             })?
//             .lines();

//         let mut rows: Vec<String> = vec![];
//         while let Some(line) = lines.next_line().await.map_err(|e| {
//             (
//                 axum::http::StatusCode::INTERNAL_SERVER_ERROR,
//                 format!("Clickhouse Error: {}", e),
//             )
//         })? {
//             // let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
//             rows.push(line);
//         }

//         let now = Instant::now();
//         let elapsed = now.duration_since(instant).as_secs_f64();
//         let elapsed_rounded = (elapsed * 1000.0).round() / 1000.0;
//         rows.push(format!("\nElapsed: {} sec", elapsed_rounded));

//         let text = rows.join("\n");

//         let mut response = (StatusCode::OK, text).into_response();
//         response
//             .headers_mut()
//             .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
//         Ok(response)
//     } else {
//         // Execute the arbitrary ClickHouse query.
//         let mut lines = ch_client
//             .query(&last_query)
//             .fetch_bytes("JSONEachRow")
//             .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
//             .lines();

//         let mut rows: Vec<Value> = vec![];
//         while let Some(line) = lines
//             .next_line()
//             .await
//             .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
//         {
//             let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
//             rows.push(value);
//         }

//         Ok(Json(rows).into_response())
//     }
// }

pub async fn ddl_handler(
    clickhouse_client: Client,
    ch_sql_queries: Vec<String>,
    graph_schema_element_opt: Option<Vec<GraphSchemaElement>>,
) -> Result<Response, (StatusCode, String)> {
    // // parse cypher query
    // let cypher_ast = open_cypher_parser::parse_query(&payload.query).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // let (query_type,ch_sql_queries) = query_engine::evaluate_ddl_query(cypher_ast).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let graph_schema_element: Vec<GraphSchemaElement> =
        graph_schema_element_opt.ok_or_else(|| {
            log::error!("Missing graph schema element in DDL query execution");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error: Missing schema element for DDL query".to_string(),
            )
        })?;

    graph_catalog::validate_schema(&graph_schema_element)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("ClickGraph Error: {}", e),
            )
        })?;

    for ch_query in ch_sql_queries {
        crate::debug_println!("\n ch_query -> {:?}", ch_query);
        let ch_client = clickhouse_client
            .clone()
            .with_option("wait_end_of_query", "1");

        ch_client.query(&ch_query).execute().await.map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })?;
    }

    // Now that DDL is applied successfully, add graph schema element into the schema and update the graph meta table here

    graph_catalog::add_to_schema(clickhouse_client.clone(), graph_schema_element)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })?;

    graph_catalog::refresh_global_schema(clickhouse_client)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })?;

    let mut response = (StatusCode::OK, "DDL applied successfully".to_string()).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));

    Ok(response)
}

/// Extract result count from a response for performance metrics
/// Note: This is a simplified implementation. In a production system,
/// you might want to track result counts during query execution.

// Multi-schema management endpoints - NEW

#[derive(Deserialize)]
pub struct LoadSchemaRequest {
    pub schema_name: String,
    pub config_content: String, // YAML content as string
    pub validate_schema: Option<bool>,
}

#[derive(Serialize)]
pub struct SchemaInfo {
    pub name: String,
    pub node_count: usize,
    pub relationship_count: usize,
}

pub async fn list_schemas_handler(
    State(_app_state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let schema_names = graph_catalog::list_available_schemas().await;
    log::debug!("Found {} schemas: {:?}", schema_names.len(), schema_names);
    let mut schemas_info = Vec::new();

    for name in schema_names {
        if let Ok(schema) = graph_catalog::get_graph_schema_by_name(&name).await {
            let node_count = schema.all_node_schemas().len();
            let relationship_count = schema.get_relationships_schemas().len();
            schemas_info.push(SchemaInfo {
                name,
                node_count,
                relationship_count,
            });
        }
    }

    Ok(Json(serde_json::json!({
        "schemas": schemas_info
    })))
}

pub async fn load_schema_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<LoadSchemaRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let validate_schema = payload.validate_schema.unwrap_or(false);

    match graph_catalog::load_schema_from_content(
        &payload.schema_name,
        &payload.config_content,
        Some(app_state.clickhouse_client.clone()),
        validate_schema,
    )
    .await
    {
        Ok(_) => {
            // Invalidate cache entries for this schema
            if let Some(cache) = GLOBAL_QUERY_CACHE.get() {
                cache.invalidate_schema(&payload.schema_name);
                log::info!("Cache invalidated for schema: {}", payload.schema_name);
            }

            Ok(Json(serde_json::json!({
                "message": format!("Schema '{}' loaded successfully", payload.schema_name),
                "schema_name": payload.schema_name
            })))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to load schema: {}", e)
            })),
        )),
    }
}

pub async fn get_schema_handler(
    axum::extract::Path(schema_name): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match graph_catalog::get_graph_schema_by_name(&schema_name).await {
        Ok(schema) => {
            let node_count = schema.all_node_schemas().len();
            let relationship_count = schema.get_relationships_schemas().len();

            Ok(Json(serde_json::json!({
                "schema_name": schema_name,
                "node_types": node_count,
                "relationship_types": relationship_count,
                "nodes": schema.all_node_schemas().keys().collect::<Vec<_>>(),
                "relationships": schema.get_relationships_schemas().keys().collect::<Vec<_>>()
            })))
        }
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": e
            })),
        )),
    }
}
/// Extract a schema name from a leading `USE <schema>` clause in a Cypher query.
///
/// This lightweight text-based extraction mirrors the normal path's USE handling
/// (which relies on the parser AST) for interceptions that run before parsing.
fn extract_schema_from_use_clause(query: &str) -> Option<String> {
    let trimmed = query.trim_start();
    if !trimmed
        .get(..4)
        .map(|s| s.eq_ignore_ascii_case("USE "))
        .unwrap_or(false)
    {
        return None;
    }

    let after_use = trimmed[3..].trim_start();
    if after_use.is_empty() {
        return None;
    }

    let mut end = after_use.len();
    for (i, ch) in after_use.char_indices() {
        if ch.is_whitespace() || ch == ';' {
            end = i;
            break;
        }
    }

    let schema = after_use[..end].trim_matches('`').trim();
    if schema.is_empty() {
        None
    } else {
        Some(schema.to_string())
    }
}

fn extract_result_count(_response: &axum::response::Response) -> Option<usize> {
    // TODO: Implement proper result count extraction
    // This would require either:
    // 1. Modifying the query execution to track row counts
    // 2. Parsing the response body (complex with streaming)
    // For now, we return None
    None
}

// Schema discovery endpoints

#[derive(Deserialize)]
pub struct IntrospectRequest {
    pub database: String,
}

pub async fn introspect_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<IntrospectRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    log::info!("Introspecting database: {}", payload.database);

    // Validate database name to prevent SQL injection
    if !payload
        .database
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        log::error!("Invalid database name: {}", payload.database);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Invalid database name" })),
        ));
    }

    let response =
        SchemaDiscovery::introspect(&app_state.clickhouse_client, &payload.database).await;

    match response {
        Ok(resp) => Ok(Json(serde_json::to_value(resp).unwrap())),
        Err(e) => {
            log::error!("Introspect failed: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e })),
            ))
        }
    }
}

#[derive(Deserialize)]
pub struct DiscoverPromptRequest {
    pub database: String,
}

pub async fn discover_prompt_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<DiscoverPromptRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    log::info!("Generating discovery prompt for: {}", payload.database);

    // Validate database name to prevent SQL injection
    if !payload
        .database
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Invalid database name" })),
        ));
    }

    // Introspect the database
    let introspect_result =
        SchemaDiscovery::introspect(&app_state.clickhouse_client, &payload.database).await;

    match introspect_result {
        Ok(resp) => {
            let prompt_response = crate::graph_catalog::llm_prompt::format_discovery_prompt(
                &resp.database,
                &resp.tables,
            );
            Ok(Json(serde_json::to_value(prompt_response).unwrap()))
        }
        Err(e) => {
            log::error!("Introspect failed for discover-prompt: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": e })),
            ))
        }
    }
}

#[derive(Deserialize)]
pub struct DraftRequestPayload {
    pub database: String,
    pub schema_name: String,
    pub nodes: Vec<NodeHint>,
    pub edges: Option<Vec<EdgeHint>>,
    pub fk_edges: Option<Vec<FkEdgeHint>>,
    pub options: Option<DraftOptions>,
}

pub async fn draft_handler(
    State(_app_state): State<Arc<AppState>>,
    Json(payload): Json<DraftRequestPayload>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    log::info!("Generating draft for schema: {}", payload.schema_name);

    let request = DraftRequest {
        database: payload.database,
        schema_name: payload.schema_name,
        nodes: payload.nodes,
        edges: payload.edges.unwrap_or_default(),
        fk_edges: payload.fk_edges.unwrap_or_default(),
        options: payload.options,
    };

    let yaml = SchemaDiscovery::generate_draft(&request);

    Ok(Json(serde_json::json!({
        "yaml": yaml,
        "message": "Review and edit the YAML before loading with /schemas/load"
    })))
}
