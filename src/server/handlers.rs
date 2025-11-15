use std::{sync::Arc, time::Instant};

use axum::{
    Json,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncBufReadExt;

use crate::{
    clickhouse_query_generator,
    graph_catalog::graph_schema::GraphSchemaElement,
    open_cypher_parser::{self},
    query_planner::{self, types::QueryType},
    render_plan::plan_builder::RenderPlanBuilder,
};

use super::{
    AppState, GLOBAL_QUERY_CACHE, graph_catalog,
    models::{OutputFormat, QueryRequest, SqlOnlyResponse},
    parameter_substitution, query_cache,
};

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

impl QueryPerformanceMetrics {
    pub fn new() -> Self {
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
    println!("DEBUG: health_check handler called!");
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
    let mut metrics = QueryPerformanceMetrics::new();

    log::debug!("Query handler called with query: {}", payload.query);

    let output_format = payload.format.unwrap_or(OutputFormat::JSONEachRow);
    let sql_only = payload.sql_only.unwrap_or(false);

    // Query cache integration - Strip CYPHER prefix FIRST
    // Extract replan option and clean query
    let replan_option = query_cache::ReplanOption::from_query_prefix(&payload.query)
        .unwrap_or(query_cache::ReplanOption::Default);
    let clean_query = query_cache::ReplanOption::strip_prefix(&payload.query);

    // Pre-parse to check for USE clause (minimal parse just to extract database selection)
    // IMPORTANT: Parse the CLEAN query without CYPHER prefix
    let schema_name = if let Ok(ast) = open_cypher_parser::parse_query(clean_query) {
        if let Some(ref use_clause) = ast.use_clause {
            use_clause.database_name
        } else {
            payload.schema_name.as_deref().unwrap_or("default")
        }
    } else {
        payload.schema_name.as_deref().unwrap_or("default")
    };

    log::debug!("Using schema: {}", schema_name);

    // Generate cache key
    let cache_key = query_cache::QueryCacheKey::new(clean_query, schema_name);
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

        // Substitute parameters if provided
        let final_sql = if let Some(params) = &payload.parameters {
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
        let response =
            execute_cte_queries(app_state, ch_sql_queries, output_format, payload.parameters).await;
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
        let graph_schema = match graph_catalog::get_graph_schema_by_name(schema_name).await {
            Ok(schema) => schema,
            Err(e) => {
                return Err((StatusCode::BAD_REQUEST, format!("Schema error: {}", e)));
            }
        };

        // Phase 1: Parse query
        // IMPORTANT: Parse the CLEAN query without CYPHER prefix
        let parse_start = Instant::now();
        let cypher_ast = match open_cypher_parser::parse_query(clean_query) {
            Ok(ast) => ast,
            Err(e) => {
                metrics.parse_time = parse_start.elapsed().as_secs_f64();
                log::error!("Query parse failed: {:?}", e);
                if sql_only {
                    let error_response = SqlOnlyResponse {
                        cypher_query: payload.query.clone(),
                        generated_sql: format!("PARSE_ERROR: {}", e),
                        execution_mode: "sql_only_with_parse_error".to_string(),
                    };
                    return Ok(Json(error_response).into_response());
                } else {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Brahmand Error: {}", e),
                    ));
                }
            }
        };
        metrics.parse_time = parse_start.elapsed().as_secs_f64();

        let query_type = query_planner::get_query_type(&cypher_ast);
        let query_type_str = match query_type {
            QueryType::Read => "read",
            QueryType::Ddl => "ddl",
            QueryType::Update => "update",
            QueryType::Delete => "delete",
            QueryType::Call => "call",
        }
        .to_string();

        let is_read = query_type == QueryType::Read;
        let is_call = query_type == QueryType::Call;

        if is_call {
            // Handle CALL queries (like PageRank)
            let logical_plan = match query_planner::evaluate_call_query(cypher_ast, &graph_schema) {
                Ok(plan) => plan,
                Err(e) => {
                    if sql_only {
                        let error_response = SqlOnlyResponse {
                            cypher_query: payload.query.clone(),
                            generated_sql: format!("CALL_PLANNING_ERROR: {}", e),
                            execution_mode: "sql_only_with_call_error".to_string(),
                        };
                        return Ok(Json(error_response).into_response());
                    } else {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Brahmand Error: {}", e),
                        ));
                    }
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
                        iterations: pagerank.iterations as usize,
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
                            if sql_only {
                                let error_response = SqlOnlyResponse {
                                    cypher_query: payload.query.clone(),
                                    generated_sql: format!("PAGERANK_SQL_ERROR: {}", e),
                                    execution_mode: "sql_only_with_pagerank_error".to_string(),
                                };
                                return Ok(Json(error_response).into_response());
                            } else {
                                return Err((
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("Brahmand Error: {}", e),
                                ));
                            }
                        }
                    }
                }
                _ => {
                    // For other CALL queries (not implemented yet)
                    if sql_only {
                        let error_response = SqlOnlyResponse {
                            cypher_query: payload.query.clone(),
                            generated_sql: "UNSUPPORTED_CALL_QUERY".to_string(),
                            execution_mode: "sql_only_unsupported_call".to_string(),
                        };
                        return Ok(Json(error_response).into_response());
                    } else {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Unsupported CALL query type".to_string(),
                        ));
                    }
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
            let logical_plan = match query_planner::evaluate_read_query(cypher_ast, &graph_schema) {
                Ok(plan) => plan,
                Err(e) => {
                    metrics.planning_time = planning_start.elapsed().as_secs_f64();
                    if sql_only {
                        let error_response = SqlOnlyResponse {
                            cypher_query: payload.query.clone(),
                            generated_sql: format!("PLANNING_ERROR: {}", e),
                            execution_mode: "sql_only_with_planning_error".to_string(),
                        };
                        return Ok(Json(error_response).into_response());
                    } else {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Brahmand Error: {}", e),
                        ));
                    }
                }
            };
            metrics.planning_time = planning_start.elapsed().as_secs_f64();

            // Phase 3: Render plan generation
            let render_start = Instant::now();
            let render_plan = match logical_plan.to_render_plan(&graph_schema) {
                Ok(plan) => plan,
                Err(e) => {
                    metrics.render_time = render_start.elapsed().as_secs_f64();
                    if sql_only {
                        let error_response = SqlOnlyResponse {
                            cypher_query: payload.query.clone(),
                            generated_sql: format!("RENDER_ERROR: {}", e),
                            execution_mode: "sql_only_with_render_error".to_string(),
                        };
                        return Ok(Json(error_response).into_response());
                    } else {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Brahmand Error: {}", e),
                        ));
                    }
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
            println!("\n ch_query \n {} \n", ch_query);

            // Store in cache (even in sql_only mode for future use)
            if let Some(cache) = GLOBAL_QUERY_CACHE.get() {
                cache.insert(cache_key.clone(), ch_query.clone());
                log::debug!("Stored SQL template in cache");
            }

            // If SQL-only mode, return the SQL without executing
            if sql_only {
                let mut sql_response = Json(SqlOnlyResponse {
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
                format!(
                    "DDL operations (CREATE/SET/DELETE) not supported. ClickGraph is a read-only query engine. Use YAML schemas to define graph views."
                ),
            ));
        }
    };

    // Phase 5: Execute query
    let execution_start = Instant::now();
    let sql_queries_count = ch_sql_queries.len();
    let response = if is_read {
        execute_cte_queries(app_state, ch_sql_queries, output_format, payload.parameters).await
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

    // Log full SQL for debugging (especially helpful when ClickHouse truncates errors)
    log::debug!("Executing SQL:\n{}", final_sql);

    if output_format == OutputFormat::Pretty
        || output_format == OutputFormat::PrettyCompact
        || output_format == OutputFormat::Csv
        || output_format == OutputFormat::CSVWithNames
    {
        let mut lines = app_state
            .clickhouse_client
            .clone()
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
        let mut lines = app_state
            .clickhouse_client
            .clone()
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
            let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
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

    let graph_schema_element: Vec<GraphSchemaElement> = graph_schema_element_opt.unwrap();

    graph_catalog::validate_schema(&graph_schema_element)
        .await
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Brahmand Error: {}", e),
            )
        })?;

    for ch_query in ch_sql_queries {
        println!("\n ch_query -> {:?}", ch_query);
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
    println!("DEBUG: list_schemas_handler called");
    let schema_names = graph_catalog::list_available_schemas().await;
    println!(
        "DEBUG: Found {} schemas: {:?}",
        schema_names.len(),
        schema_names
    );
    let mut schemas_info = Vec::new();

    for name in schema_names {
        if let Ok(schema) = graph_catalog::get_graph_schema_by_name(&name).await {
            let node_count = schema.get_nodes_schemas().len();
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
            let node_count = schema.get_nodes_schemas().len();
            let relationship_count = schema.get_relationships_schemas().len();

            Ok(Json(serde_json::json!({
                "schema_name": schema_name,
                "node_types": node_count,
                "relationship_types": relationship_count,
                "nodes": schema.get_nodes_schemas().keys().collect::<Vec<_>>(),
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
fn extract_result_count(_response: &axum::response::Response) -> Option<usize> {
    // TODO: Implement proper result count extraction
    // This would require either:
    // 1. Modifying the query execution to track row counts
    // 2. Parsing the response body (complex with streaming)
    // For now, we return None
    None
}
