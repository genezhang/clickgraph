use std::{sync::Arc, time::Instant};

use axum::{
    Json,
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use clickhouse::Client;
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
    AppState, graph_catalog,
    models::{OutputFormat, QueryRequest, SqlOnlyResponse},
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
            self.result_rows.map_or("N/A".to_string(), |r| r.to_string())
        );

        if log::log_enabled!(log::Level::Debug) {
            log::debug!("Performance breakdown for query: {}", query.chars().take(100).collect::<String>());
        }
    }

    pub fn to_headers(&self) -> Vec<(String, String)> {
        vec![
            ("X-Query-Total-Time".to_string(), format!("{:.3}ms", self.total_time * 1000.0)),
            ("X-Query-Parse-Time".to_string(), format!("{:.3}ms", self.parse_time * 1000.0)),
            ("X-Query-Planning-Time".to_string(), format!("{:.3}ms", self.planning_time * 1000.0)),
            ("X-Query-Render-Time".to_string(), format!("{:.3}ms", self.render_time * 1000.0)),
            ("X-Query-SQL-Gen-Time".to_string(), format!("{:.3}ms", self.sql_generation_time * 1000.0)),
            ("X-Query-Execution-Time".to_string(), format!("{:.3}ms", self.execution_time * 1000.0)),
            ("X-Query-Type".to_string(), self.query_type.clone()),
            ("X-Query-SQL-Count".to_string(), self.sql_queries_count.to_string()),
        ]
    }
}

/// Simple health check endpoint
pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({
        "status": "healthy",
        "service": "clickgraph",
        "version": env!("CARGO_PKG_VERSION")
    })))
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

        let (ch_sql_queries, maybe_schema_elem, is_read, query_type_str) = {
        let graph_schema = graph_catalog::get_graph_schema().await;

        // Phase 1: Parse query
        let parse_start = Instant::now();
        let cypher_ast = match open_cypher_parser::parse_query(&payload.query) {
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
        }.to_string();

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
                    use crate::clickhouse_query_generator::pagerank::PageRankGenerator;
                    use crate::clickhouse_query_generator::pagerank::PageRankConfig;
                    
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
                        pagerank.relationship_types.clone()
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
            let render_plan = match logical_plan.to_render_plan() {
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
            let ch_query = clickhouse_query_generator::generate_sql(render_plan, app_state.config.max_cte_depth);
            metrics.sql_generation_time = sql_generation_start.elapsed().as_secs_f64();
            println!("\n ch_query \n {} \n", ch_query);
            
            // If SQL-only mode, return the SQL without executing
            if sql_only {
                let sql_response = SqlOnlyResponse {
                    cypher_query: payload.query.clone(),
                    generated_sql: ch_query.clone(),
                    execution_mode: "sql_only".to_string(),
                };
                return Ok(Json(sql_response).into_response());
            }
            
            (vec![ch_query], None, true, query_type_str)
        } else {
            let (queries, schema_elem) =
                clickhouse_query_generator::generate_ddl_query(cypher_ast, &graph_schema).map_err(
                    |e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Brahmand Error: {}", e),
                        )
                    },
                )?;
            (queries, Some(schema_elem), false, query_type_str)
        }
    };

    // Phase 5: Execute query
    let execution_start = Instant::now();
    let sql_queries_count = ch_sql_queries.len();
    let response = if is_read {
        execute_cte_queries(app_state, ch_sql_queries, output_format).await
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
                    axum::http::HeaderValue::try_from(value)
                ) {
                    resp.headers_mut().insert(header_name, header_value);
                }
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
) -> Result<Response, (StatusCode, String)> {
    let ch_query_string = ch_sql_queries.join(" ");
    
    // Log full SQL for debugging (especially helpful when ClickHouse truncates errors)
    log::debug!("Executing SQL:\n{}", ch_query_string);

    if output_format == OutputFormat::Pretty
        || output_format == OutputFormat::PrettyCompact
        || output_format == OutputFormat::Csv
        || output_format == OutputFormat::CSVWithNames
    {
        let mut lines = app_state
            .clickhouse_client
            .clone()
            .query(&ch_query_string)
            .fetch_bytes(output_format)
            .map_err(|e| {
                // Log full SQL on error for debugging
                log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", ch_query_string, e);
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
            .query(&ch_query_string)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| {
                // Log full SQL on error for debugging
                log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", ch_query_string, e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Clickhouse Error: {}", e),
                )
            })?
            .lines();

        let mut rows: Vec<Value> = vec![];
        while let Some(line) = lines.next_line().await.map_err(|e| {
                // Log full SQL on error for debugging
                log::error!("ClickHouse response parsing failed. SQL was:\n{}\nError: {}", ch_query_string, e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Clickhouse Error: {}", e),
            )
        })? {
            let value: serde_json::Value = serde_json::de::from_str(&line).unwrap();
            rows.push(value);
        }

        Ok(Json(rows).into_response())
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

    // println!("IN DDL HANDLER GLOBAL_GRAPH_SCHEMA {:?}",GLOBAL_GRAPH_SCHEMA.get());
    Ok(response)
}

/// Extract result count from a response for performance metrics
/// Note: This is a simplified implementation. In a production system,
/// you might want to track result counts during query execution.
fn extract_result_count(_response: &axum::response::Response) -> Option<usize> {
    // TODO: Implement proper result count extraction
    // This would require either:
    // 1. Modifying the query execution to track row counts
    // 2. Parsing the response body (complex with streaming)
    // For now, we return None
    None
}
