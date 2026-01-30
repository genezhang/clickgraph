//! Bolt Protocol Message Handler
//!
//! This module processes incoming Bolt messages and generates appropriate responses.
//! It handles the complete Bolt protocol state machine and integrates with
//! Brahmand's query processing pipeline.

use clickhouse::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::auth::{AuthToken, AuthenticatedUser, Authenticator};
use super::errors::{BoltError, BoltResult};
use super::messages::{signatures, BoltMessage};
use super::result_transformer::extract_return_metadata;
use super::{BoltConfig, BoltContext, ConnectionState};

use crate::clickhouse_query_generator;
use crate::open_cypher_parser;
use crate::query_planner;
use crate::render_plan::plan_builder::RenderPlanBuilder;
use crate::server::{graph_catalog, parameter_substitution};

/// Helper macro for safe mutex locking with proper error handling
macro_rules! lock_context {
    ($mutex:expr) => {
        $mutex.lock().map_err(|e| {
            log::error!("Mutex poisoning detected in Bolt handler: {}", e);
            BoltError::mutex_poisoned(format!("Connection state synchronization failed: {}", e))
        })?
    };
}

/// Bolt protocol message handler
pub struct BoltHandler {
    /// Connection context
    context: Arc<Mutex<BoltContext>>,
    /// Server configuration
    config: Arc<BoltConfig>,
    /// Authenticator
    authenticator: Authenticator,
    /// Current authenticated user
    authenticated_user: Option<AuthenticatedUser>,
    /// ClickHouse client for query execution
    clickhouse_client: Client,
    /// Cached query results for streaming
    cached_results: Option<Vec<Vec<Value>>>,
}

impl BoltHandler {
    /// Create a new Bolt message handler
    pub fn new(
        context: Arc<Mutex<BoltContext>>,
        config: Arc<BoltConfig>,
        clickhouse_client: Client,
    ) -> Self {
        BoltHandler {
            context,
            config: config.clone(),
            authenticator: Authenticator::new(config.enable_auth, config.default_user.clone()),
            authenticated_user: None,
            clickhouse_client,
            cached_results: None,
        }
    }

    /// Handle a Bolt message and return response messages
    pub async fn handle_message(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::debug!("Handling Bolt message: {}", message.type_name());

        match message.signature {
            signatures::HELLO => self.handle_hello(message).await,
            signatures::LOGON => self.handle_logon(message).await,
            signatures::LOGOFF => self.handle_logoff(message).await,
            signatures::GOODBYE => self.handle_goodbye(message).await,
            signatures::RESET => self.handle_reset(message).await,
            signatures::RUN => self.handle_run(message).await,
            signatures::PULL => self.handle_pull(message).await,
            signatures::DISCARD => self.handle_discard(message).await,
            signatures::BEGIN => self.handle_begin(message).await,
            signatures::COMMIT => self.handle_commit(message).await,
            signatures::ROLLBACK => self.handle_rollback(message).await,
            _ => {
                log::warn!("Unhandled Bolt message type: {}", message.type_name());
                Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    format!("Unhandled message type: {}", message.type_name()),
                )])
            }
        }
    }

    /// Handle HELLO message (Bolt 5.1+: no auth, just connection initialization)
    async fn handle_hello(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        let (current_state, negotiated_version) = {
            let context = lock_context!(self.context);
            let version = match &context.state {
                ConnectionState::Negotiated(v) => *v,
                _ => 0,
            };
            (context.state.clone(), version)
        };

        if !matches!(current_state, ConnectionState::Negotiated(_)) {
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Request.Invalid".to_string(),
                "HELLO message received in invalid state".to_string(),
            )]);
        }

        // Determine if this is Bolt 5.1+ (authentication moved to LOGON)
        let is_bolt_51_plus = negotiated_version >= 0x00000501;

        // DEBUG: Log HELLO message structure
        log::debug!("HELLO message has {} fields", message.fields.len());
        for (i, field) in message.fields.iter().enumerate() {
            log::debug!(
                "  HELLO Field[{}]: {}",
                i,
                serde_json::to_string(field).unwrap_or_else(|_| "?".to_string())
            );
        }

        if is_bolt_51_plus {
            // Bolt 5.1+: HELLO just initializes connection, auth happens in LOGON
            log::info!("HELLO received (Bolt 5.1+), awaiting LOGON for authentication");

            // Update context to AUTHENTICATION state
            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Authentication(negotiated_version));
            }

            // Create success response with server information
            let mut metadata = HashMap::new();
            metadata.insert(
                "server".to_string(),
                Value::String(self.config.server_agent.clone()),
            );
            metadata.insert(
                "connection_id".to_string(),
                Value::String("bolt-1".to_string()),
            );

            // Add server capabilities
            let mut hints = HashMap::new();
            hints.insert("utc_patch".to_string(), Value::Bool(false));
            hints.insert("patch_bolt".to_string(), Value::Bool(false));
            metadata.insert(
                "hints".to_string(),
                Value::Object(serde_json::Map::from_iter(hints)),
            );

            Ok(vec![BoltMessage::success(metadata)])
        } else {
            // Bolt 4.x and earlier: HELLO includes authentication
            // Debug: log HELLO message fields
            log::debug!("HELLO message has {} fields", message.fields.len());
            for (i, field) in message.fields.iter().enumerate() {
                log::debug!(
                    "  Field[{}]: {}",
                    i,
                    serde_json::to_string(field).unwrap_or_else(|_| "ERROR".to_string())
                );
            }

            let auth_token = message.extract_auth_token().unwrap_or_default();

            // Extract database selection (Neo4j 4.0+ multi-database support)
            let database = message.extract_database();
            log::debug!("Extracted database from HELLO: {:?}", database);

            // Parse authentication token
            let token = AuthToken::from_hello_fields(&auth_token)?;

            // Authenticate user
            match self.authenticator.authenticate(&token) {
                Ok(user) => {
                    self.authenticated_user = Some(user.clone());

                    // Update context
                    {
                        let mut context = lock_context!(self.context);
                        context.set_user(user.username.clone());
                        context.schema_name = database.clone();
                        context.set_state(ConnectionState::Ready);
                    }

                    // Log database selection
                    if let Some(ref db) = database {
                        log::info!("Bolt connection using database/schema: {}", db);
                    } else {
                        log::info!("Bolt connection using default schema");
                    }

                    // Create success response with server information
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "server".to_string(),
                        Value::String(self.config.server_agent.clone()),
                    );
                    metadata.insert(
                        "connection_id".to_string(),
                        Value::String("bolt-1".to_string()),
                    );

                    // Add server capabilities
                    let mut hints = HashMap::new();
                    hints.insert("utc_patch".to_string(), Value::Bool(false));
                    hints.insert("patch_bolt".to_string(), Value::Bool(false));
                    metadata.insert(
                        "hints".to_string(),
                        Value::Object(serde_json::Map::from_iter(hints)),
                    );

                    log::info!("Bolt authentication successful for user: {}", user.username);
                    Ok(vec![BoltMessage::success(metadata)])
                }
                Err(auth_error) => {
                    log::warn!("Bolt authentication failed: {}", auth_error);

                    // Update context to failed state
                    {
                        let mut context = lock_context!(self.context);
                        context.set_state(ConnectionState::Failed);
                    }

                    Ok(vec![BoltMessage::failure(
                        auth_error.error_code().to_string(),
                        auth_error.to_string(),
                    )])
                }
            }
        }
    }

    /// Handle LOGON message (Bolt 5.1+ authentication)
    async fn handle_logon(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        let current_state = {
            let context = lock_context!(self.context);
            context.state.clone()
        };

        // LOGON can only be processed in AUTHENTICATION state (Bolt 5.1+)
        if !matches!(current_state, ConnectionState::Authentication(_)) {
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Request.Invalid".to_string(),
                format!(
                    "LOGON message received in invalid state: {:?}",
                    current_state
                ),
            )]);
        }

        // Debug: log LOGON message fields
        log::debug!("LOGON message has {} fields", message.fields.len());
        for (i, field) in message.fields.iter().enumerate() {
            log::debug!(
                "  Field[{}]: {}",
                i,
                serde_json::to_string(field).unwrap_or_else(|_| "ERROR".to_string())
            );
        }

        // Extract authentication token from LOGON message
        // Handle empty LOGON (auth-less mode for Bolt 5.x)
        let auth_token = if message.fields.is_empty() {
            log::info!("Empty LOGON message received - using auth-less mode");
            HashMap::new() // Empty auth = no authentication required
        } else {
            message.extract_logon_auth().ok_or_else(|| {
                BoltError::invalid_message("Missing authentication data in LOGON message")
            })?
        };

        log::debug!(
            "Extracted auth token: {:?}",
            auth_token.keys().collect::<Vec<_>>()
        );

        // Parse authentication token
        let token = AuthToken::from_hello_fields(&auth_token)?;

        // Authenticate user
        match self.authenticator.authenticate(&token) {
            Ok(user) => {
                self.authenticated_user = Some(user.clone());

                // Extract database from auth_token if present (Bolt 5.1+ can include db in LOGON)
                let mut database = auth_token
                    .get("db")
                    .or_else(|| auth_token.get("database"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                // If no database specified, use the first loaded schema (if any)
                if database.is_none() {
                    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                        let schemas = schemas_lock.read().await;
                        // Find first non-default schema
                        let first_schema = schemas
                            .keys()
                            .find(|k| *k != "default")
                            .or_else(|| schemas.keys().next())
                            .cloned();
                        if let Some(schema_name) = first_schema {
                            log::info!(
                                "No database specified in LOGON, using first loaded schema: {}",
                                schema_name
                            );
                            database = Some(schema_name);
                        }
                    }
                }

                // Update context
                {
                    let mut context = lock_context!(self.context);
                    context.set_user(user.username.clone());
                    context.schema_name = database.clone();
                    context.set_state(ConnectionState::Ready);
                }

                // Log database selection
                if let Some(ref db) = database {
                    log::info!("Bolt LOGON successful, using database/schema: {}", db);
                } else {
                    log::info!("Bolt LOGON successful, using default schema");
                }

                // Create success response
                let metadata = HashMap::new();
                log::info!("Bolt authentication successful for user: {}", user.username);
                Ok(vec![BoltMessage::success(metadata)])
            }
            Err(auth_error) => {
                log::warn!("Bolt LOGON failed: {}", auth_error);

                // Update context to failed state
                {
                    let mut context = lock_context!(self.context);
                    context.set_state(ConnectionState::Failed);
                }

                Ok(vec![BoltMessage::failure(
                    auth_error.error_code().to_string(),
                    auth_error.to_string(),
                )])
            }
        }
    }

    /// Handle LOGOFF message (Bolt 5.1+ - log out and return to authentication state)
    async fn handle_logoff(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state - LOGOFF can only be called in READY state
        let current_state = {
            let context = lock_context!(self.context);
            context.state.clone()
        };

        if !matches!(current_state, ConnectionState::Ready) {
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Request.Invalid".to_string(),
                format!(
                    "LOGOFF message received in invalid state: {:?}",
                    current_state
                ),
            )]);
        }

        // Clear authentication
        let username = self.authenticated_user.as_ref().map(|u| u.username.clone());
        self.authenticated_user = None;

        // Get negotiated version to restore proper authentication state
        let negotiated_version = match current_state {
            ConnectionState::Ready => {
                // Get from context if we stored it
                0x00000501 // Default to 5.1 if we're handling LOGOFF
            }
            _ => 0x00000501,
        };

        // Update context to AUTHENTICATION state
        {
            let mut context = lock_context!(self.context);
            context.set_user(String::new());
            context.schema_name = None;
            context.set_state(ConnectionState::Authentication(negotiated_version));
        }

        if let Some(user) = username {
            log::info!("Bolt LOGOFF successful for user: {}", user);
        }

        // Return success
        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle GOODBYE message (connection termination)
    async fn handle_goodbye(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::info!("Received GOODBYE message, closing connection");

        // Update context
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Failed);
        }

        // No response needed for GOODBYE
        Ok(vec![])
    }

    /// Handle RESET message (connection reset)
    async fn handle_reset(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::info!("Resetting Bolt connection");

        // Reset connection state but keep authentication
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Ready);
            context.tx_id = None; // Clear any active transaction
        }

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle RUN message (execute Cypher query)
    async fn handle_run(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !context.is_ready() {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "RUN message received in invalid state".to_string(),
                )]);
            }
        }

        // Extract query and parameters
        let query = message
            .extract_query()
            .ok_or_else(|| BoltError::invalid_message("RUN message missing query"))?;

        let parameters = message.extract_parameters().unwrap_or_default();

        // Get selected schema from context, or from RUN message metadata
        let (schema_name, tenant_id, role, view_parameters) = {
            let context = lock_context!(self.context);

            // Debug: log RUN message fields
            log::debug!("RUN message has {} fields", message.fields.len());
            for (i, field) in message.fields.iter().enumerate() {
                log::debug!(
                    "  Field[{}]: {}",
                    i,
                    serde_json::to_string(field).unwrap_or_else(|_| "<<unparseable>>".to_string())
                );
            }

            // Check if RUN message specifies a database (Bolt 4.x)
            let schema_name = if let Some(run_db) = message.extract_run_database() {
                log::info!("✅ RUN message contains database: {}", run_db);
                if run_db != context.schema_name.as_deref().unwrap_or("default") {
                    log::debug!(
                        "RUN message overriding schema: {} -> {}",
                        context.schema_name.as_deref().unwrap_or("default"),
                        run_db
                    );
                }
                Some(run_db)
            } else {
                log::debug!(
                    "RUN message does NOT contain database field, using context schema: {:?}",
                    context.schema_name
                );
                context.schema_name.clone()
            };

            // Extract tenant_id from RUN message metadata (Phase 2)
            let tenant_id = message.extract_run_tenant_id();
            if let Some(ref tid) = tenant_id {
                log::debug!("✅ RUN message contains tenant_id: {}", tid);
            }

            // Extract role from RUN message metadata (Phase 2 RBAC)
            let role = message.extract_run_role();
            if let Some(ref r) = role {
                log::debug!("✅ RUN message contains role: {}", r);
            }

            // Extract view_parameters from RUN message metadata (Phase 2 Multi-tenancy)
            let view_parameters = message.extract_run_view_parameters();
            if let Some(ref vp) = view_parameters {
                log::debug!("✅ RUN message contains view_parameters: {:?}", vp);
            }

            (schema_name, tenant_id, role, view_parameters)
        };

        log::info!("Executing Cypher query: {}", query);
        if let Some(ref schema) = schema_name {
            log::debug!("Query execution using schema: {}", schema);
        } else {
            log::debug!("Query execution using schema: default");
        }

        // Parse and execute the query
        match self
            .execute_cypher_query(
                query,
                parameters,
                schema_name,
                tenant_id,
                role,
                view_parameters,
            )
            .await
        {
            Ok(result_metadata) => {
                // Update context to streaming state
                {
                    let mut context = lock_context!(self.context);
                    context.set_state(ConnectionState::Streaming);
                }

                // Return success with query metadata
                Ok(vec![BoltMessage::success(result_metadata)])
            }
            Err(query_error) => {
                log::error!("Query execution failed: {}", query_error);
                Ok(vec![BoltMessage::failure(
                    query_error.error_code().to_string(),
                    query_error.to_string(),
                )])
            }
        }
    }

    /// Handle PULL message (fetch query results)
    async fn handle_pull(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !matches!(context.state, ConnectionState::Streaming) {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "PULL message received in invalid state".to_string(),
                )]);
            }
        }

        // Stream the cached results as RECORD messages
        let mut messages = Vec::new();

        if let Some(rows) = self.cached_results.take() {
            log::debug!("Streaming {} rows via Bolt RECORD messages", rows.len());

            // Send each row as a RECORD message
            for row in rows {
                messages.push(BoltMessage::record(row));
            }
        }

        // Send SUCCESS with completion metadata
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), Value::String("r".to_string()));
        metadata.insert("has_more".to_string(), Value::Bool(false));
        metadata.insert("t_last".to_string(), Value::Number(0.into()));

        messages.push(BoltMessage::success(metadata));

        // Update context back to ready state
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Ready);
        }

        Ok(messages)
    }

    /// Handle DISCARD message (discard query results)
    async fn handle_discard(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !matches!(context.state, ConnectionState::Streaming) {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "DISCARD message received in invalid state".to_string(),
                )]);
            }
        }

        log::debug!("Discarding query results");

        // Update context back to ready state
        {
            let mut context = lock_context!(self.context);
            context.set_state(ConnectionState::Ready);
        }

        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), Value::String("r".to_string()));

        Ok(vec![BoltMessage::success(metadata)])
    }

    /// Handle BEGIN message (start transaction)
    async fn handle_begin(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !context.is_ready() {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "BEGIN message received in invalid state".to_string(),
                )]);
            }
        }

        // Extract database from BEGIN message extra field (Bolt 4.0+)
        if let Some(db) = message.extract_begin_database() {
            let mut context = lock_context!(self.context);
            if context.schema_name.as_deref() != Some(&db) {
                log::debug!(
                    "BEGIN message overriding schema: {:?} -> {}",
                    context.schema_name,
                    db
                );
                context.schema_name = Some(db);
            }
        }

        // Generate transaction ID
        let tx_id = format!("tx-{}", chrono::Utc::now().timestamp_millis());

        // Update context with transaction
        {
            let mut context = lock_context!(self.context);
            context.tx_id = Some(tx_id.clone());
        }

        log::info!("Started transaction: {}", tx_id);

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle COMMIT message (commit transaction)
    async fn handle_commit(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify we're in a transaction
        let tx_id = {
            let mut context = lock_context!(self.context);
            if let Some(tx_id) = context.tx_id.take() {
                tx_id
            } else {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Transaction.TransactionNotFound".to_string(),
                    "No active transaction to commit".to_string(),
                )]);
            }
        };

        log::info!("Committed transaction: {}", tx_id);

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle ROLLBACK message (rollback transaction)
    async fn handle_rollback(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify we're in a transaction
        let tx_id = {
            let mut context = lock_context!(self.context);
            if let Some(tx_id) = context.tx_id.take() {
                tx_id
            } else {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Transaction.TransactionNotFound".to_string(),
                    "No active transaction to rollback".to_string(),
                )]);
            }
        };

        log::info!("Rolled back transaction: {}", tx_id);

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Execute a Cypher query and return result metadata
    async fn execute_cypher_query(
        &mut self,
        query: &str,
        parameters: HashMap<String, Value>,
        schema_name: Option<String>,
        tenant_id: Option<String>,
        role: Option<String>,
        view_parameters: Option<HashMap<String, String>>,
    ) -> BoltResult<HashMap<String, Value>> {
        // Parse and extract schema name synchronously (no await points, so Rc<RefCell<>> is safe)
        let (effective_schema, query_type_check) = {
            // Parse the Cypher query
            let parsed_query = match open_cypher_parser::parse_query(query) {
                Ok(ast) => ast,
                Err(parse_error) => {
                    return Err(BoltError::query_error(format!(
                        "Query parsing failed: {}",
                        parse_error
                    )));
                }
            };

            // Determine schema_name
            let effective_schema = if let Some(ref use_clause) = parsed_query.use_clause {
                use_clause.database_name.to_string()
            } else {
                schema_name.as_deref().unwrap_or("default").to_string()
            };

            // Get query type
            let query_type = query_planner::get_query_type(&parsed_query);

            (effective_schema, query_type)
        }; // parsed_query is dropped here!

        // Check query type
        if query_type_check != query_planner::types::QueryType::Read {
            return Err(BoltError::query_error(
                "Only read queries are currently supported via Bolt protocol".to_string(),
            ));
        }

        log::debug!("Query execution using schema: {}", effective_schema);

        // NOW we can await - no more Rc<RefCell<>> held
        let graph_schema = match graph_catalog::get_graph_schema_by_name(&effective_schema).await {
            Ok(schema) => schema,
            Err(e) => {
                return Err(BoltError::query_error(format!("Schema error: {}", e)));
            }
        };

        // Re-parse for planning (yes, inefficient, but ensures Send safety)
        let parsed_query_for_planning = open_cypher_parser::parse_query(query)
            .map_err(|e| BoltError::query_error(format!("Query re-parse failed: {}", e)))?;

        // Generate logical plan (returns both plan and context with VLP metadata)
        let (logical_plan, plan_ctx) = match query_planner::evaluate_read_query(
            parsed_query_for_planning,
            &graph_schema,
            tenant_id,
            view_parameters,
        ) {
            Ok(result) => result,
            Err(e) => {
                return Err(BoltError::query_error(format!(
                    "Query planning failed: {}",
                    e
                )));
            }
        };
        // parsed_query is now dropped - no more Rc<RefCell<>> held!

        // Extract return metadata for result transformation
        let return_metadata = match extract_return_metadata(&logical_plan, &plan_ctx) {
            Ok(metadata) => metadata,
            Err(e) => {
                log::warn!("Failed to extract return metadata: {}", e);
                Vec::new() // Fall back to no transformation
            }
        };
        let has_graph_objects = return_metadata.iter().any(|m| {
            matches!(
                m.item_type,
                super::result_transformer::ReturnItemType::Node { .. }
                    | super::result_transformer::ReturnItemType::Relationship { .. }
                    | super::result_transformer::ReturnItemType::Path
            )
        });

        // Generate render plan - use _with_ctx to pass VLP endpoint information
        let render_plan = match logical_plan.to_render_plan_with_ctx(&graph_schema, Some(&plan_ctx))
        {
            Ok(plan) => plan,
            Err(e) => {
                return Err(BoltError::query_error(format!(
                    "Render plan generation failed: {}",
                    e
                )));
            }
        };

        // Generate ClickHouse SQL
        let max_cte_depth = 1000; // Use default from config
        let ch_sql = clickhouse_query_generator::generate_sql(render_plan, max_cte_depth);

        // Substitute parameters if provided
        let final_sql = if !parameters.is_empty() {
            match parameter_substitution::substitute_parameters(&ch_sql, &parameters) {
                Ok(sql) => sql,
                Err(e) => {
                    return Err(BoltError::query_error(format!(
                        "Parameter substitution failed: {}",
                        e
                    )));
                }
            }
        } else {
            ch_sql.clone()
        };

        log::debug!("Executing SQL: {}", final_sql);

        // Apply role for ClickHouse RBAC (Phase 2)
        if let Some(ref role_name) = role {
            crate::server::clickhouse_client::set_role(&self.clickhouse_client, role_name)
                .await
                .map_err(|e| {
                    BoltError::query_error(format!(
                        "Failed to set ClickHouse role: {}. Ensure role is granted to user.",
                        e
                    ))
                })?;
        }

        // Execute the query and fetch results as JSON bytes
        use tokio::io::AsyncBufReadExt;
        let result_reader = self
            .clickhouse_client
            .query(&final_sql)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| {
                BoltError::query_error(format!("ClickHouse query execution failed: {}", e))
            })?;

        // Parse JSON results line by line
        let mut rows = Vec::new();
        let mut field_names = Vec::new();

        let mut lines = result_reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<serde_json::Value>(&line) {
                Ok(Value::Object(obj)) => {
                    // Extract field names from first row
                    if field_names.is_empty() {
                        field_names = obj.keys().cloned().collect();
                    }

                    // Extract field values in consistent order
                    let mut row_fields = Vec::new();
                    for field_name in &field_names {
                        row_fields.push(obj.get(field_name).cloned().unwrap_or(Value::Null));
                    }
                    rows.push(row_fields);
                }
                _ => {
                    log::warn!("Unexpected JSON format in result row: {}", line);
                }
            }
        }

        // Transform results if we have graph objects (nodes, relationships, paths)
        if has_graph_objects {
            let mut transformed_rows = Vec::new();
            for row in &rows {
                // Convert row Vec back to HashMap for transformation
                let mut row_map = HashMap::new();
                for (i, field_name) in field_names.iter().enumerate() {
                    if let Some(value) = row.get(i) {
                        row_map.insert(field_name.clone(), value.clone());
                    }
                }

                match super::result_transformer::transform_row(
                    row_map,
                    &return_metadata,
                    &graph_schema,
                ) {
                    Ok(transformed) => transformed_rows.push(transformed),
                    Err(e) => {
                        log::warn!("Failed to transform row to graph objects: {}", e);
                        // Fall back to original row on error
                        transformed_rows.push(row.clone());
                    }
                }
            }
            rows = transformed_rows;
            
            // Update field names to match transformed structure
            field_names = return_metadata
                .iter()
                .map(|m| m.field_name.clone())
                .collect();
        }

        // Cache the results for streaming in PULL
        self.cached_results = Some(rows);

        // Return SUCCESS with metadata
        let mut metadata = HashMap::new();
        metadata.insert(
            "fields".to_string(),
            Value::Array(
                field_names
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        );
        metadata.insert("t_first".to_string(), Value::Number(0.into()));
        metadata.insert("qid".to_string(), Value::Number(1.into()));

        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_handler() -> BoltHandler {
        let context = Arc::new(Mutex::new(BoltContext::new()));
        let config = Arc::new(BoltConfig::default());
        // Create a test ClickHouse client (won't be used in unit tests)
        let clickhouse_client = clickhouse::Client::default().with_url("http://localhost:8123");
        BoltHandler::new(context, config, clickhouse_client)
    }

    #[tokio::test]
    async fn test_hello_message_handling() {
        let mut handler = create_test_handler();

        // Set context to negotiated state
        {
            let mut context = handler.context.lock().unwrap();
            context.set_version(super::super::BOLT_VERSION_4_4);
        }

        let auth_token = HashMap::from([("scheme".to_string(), Value::String("none".to_string()))]);

        let hello = BoltMessage::hello("TestClient/1.0".to_string(), auth_token);
        let responses = handler.handle_message(hello).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].signature, signatures::SUCCESS);
    }

    #[tokio::test]
    async fn test_reset_message_handling() {
        let mut handler = create_test_handler();

        let reset = BoltMessage::reset();
        let responses = handler.handle_message(reset).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].signature, signatures::SUCCESS);
    }

    #[tokio::test]
    async fn test_goodbye_message_handling() {
        let mut handler = create_test_handler();

        let goodbye = BoltMessage::goodbye();
        let responses = handler.handle_message(goodbye).await.unwrap();

        // GOODBYE should return no responses
        assert_eq!(responses.len(), 0);

        // Context should be set to failed state
        {
            let context = handler.context.lock().unwrap();
            assert_eq!(context.state, ConnectionState::Failed);
        }
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let mut handler = create_test_handler();

        // Set context to ready state
        {
            let mut context = handler.context.lock().unwrap();
            context.set_state(ConnectionState::Ready);
        }

        // Begin transaction
        let begin = BoltMessage::begin(None);
        let responses = handler.handle_message(begin).await.unwrap();
        assert_eq!(responses[0].signature, signatures::SUCCESS);

        // Verify transaction started
        {
            let context = handler.context.lock().unwrap();
            assert!(context.tx_id.is_some());
        }

        // Commit transaction
        let commit = BoltMessage::commit();
        let responses = handler.handle_message(commit).await.unwrap();
        assert_eq!(responses[0].signature, signatures::SUCCESS);

        // Verify transaction cleared
        {
            let context = handler.context.lock().unwrap();
            assert!(context.tx_id.is_none());
        }
    }
}
