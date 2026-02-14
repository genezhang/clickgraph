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
use super::messages::{signatures, BoltMessage, BoltValue};
use super::result_transformer::extract_return_metadata;
use super::{BoltConfig, BoltContext, ConnectionState};

use crate::clickhouse_query_generator;
use crate::open_cypher_parser;
use crate::query_planner;

/// Execution plan for procedure-only queries (extracted before async execution)
#[derive(Debug)]
enum ExecutionPlan {
    SimpleProcedure {
        proc_name: String,
    },
    ProcedureWithReturn {
        proc_name: String,
        // Store index to look up in parsed statement after async
    },
    Union {
        branches: Vec<ProcedureBranch>,
    },
}

/// Branch information for UNION execution
#[derive(Debug)]
struct ProcedureBranch {
    proc_name: String,
    has_return: bool,
}

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

/// Helper function to format BoltValue for logging
fn bolt_value_to_string(value: &BoltValue) -> String {
    match value {
        BoltValue::Json(v) => serde_json::to_string(v).unwrap_or_else(|_| format!("{:?}", v)),
        BoltValue::PackstreamBytes(bytes) => format!("<packstream: {} bytes>", bytes.len()),
    }
}

/// Pattern detection: `id(alias) IN $paramName` or `id(alias) = $paramName`
/// Substitute Cypher parameters into query string, keeping encoded IDs intact
/// This replaces $paramName with actual values so parser sees literals
/// Used for id() parameters to preserve encoded IDs for label extraction
/// This replaces only $paramName values used with id() so the parser sees literals
/// Used for id() parameters to preserve encoded IDs for label extraction
///
/// SECURITY:
/// - Uses regex with word boundaries to prevent partial matches ($id vs $ids)
/// - Escapes quotes and backslashes in string values
/// - Skips string literals to prevent injection
///
/// LIMITATION: This is a lexical approach that may have edge cases with complex
/// nested strings or comments. For production, consider AST-level parameter binding.
fn substitute_cypher_parameters(query: &str, parameters: &HashMap<String, Value>) -> String {
    use regex::Regex;

    // Helper to check if position is inside a string literal
    fn is_inside_string(query: &str, pos: usize) -> bool {
        let before = &query[..pos];
        let single_quotes = before.matches('\'').count();
        let double_quotes = before.matches('"').count();

        // Odd number of quotes means we're inside a string
        // This is simplified - doesn't handle escaped quotes perfectly
        (single_quotes % 2 == 1) || (double_quotes % 2 == 1)
    }

    fn escape_cypher_string(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for ch in s.chars() {
            match ch {
                '\'' => {
                    out.push('\\');
                    out.push('\'');
                }
                '\\' => {
                    out.push('\\');
                    out.push('\\');
                }
                _ => out.push(ch),
            }
        }
        out
    }

    fn value_to_cypher_literal(value: &Value) -> String {
        match value {
            Value::Array(arr) => {
                let values: Vec<String> =
                    arr.iter()
                        .map(|v| match v {
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => format!("'{}'", escape_cypher_string(s)),
                            other => serde_json::to_string(other)
                                .unwrap_or_else(|_| format!("{:?}", other)),
                        })
                        .collect();
                format!("[{}]", values.join(", "))
            }
            Value::Number(n) => n.to_string(),
            Value::String(s) => format!("'{}'", escape_cypher_string(s)),
            other => serde_json::to_string(other).unwrap_or_else(|_| format!("{:?}", other)),
        }
    }

    // Regex to find id()-based predicates with a parameter
    let re = Regex::new(
        r"(?i)\bid\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)\s*(?:IN|=)\s*\$([A-Za-z_][A-Za-z0-9_]*)",
    )
    .expect("regex for id() parameter substitution must compile");

    let mut result = query.to_string();
    let mut offset: isize = 0;

    // Collect all matches first to avoid modification-during-iteration issues
    let matches: Vec<_> = re.captures_iter(query).collect();

    for cap in matches {
        let full_match = cap.get(0).unwrap();
        let match_start = full_match.start();
        let param_name = &cap[1];

        // Skip if inside string literal
        if is_inside_string(query, match_start) {
            log::debug!(
                "⏭️  Skipping parameter ${} inside string literal",
                param_name
            );
            continue;
        }

        if let Some(param_value) = parameters.get(param_name) {
            let literal = value_to_cypher_literal(param_value);
            let pattern = format!("${}", param_name);
            let replacement = full_match.as_str().replacen(&pattern, &literal, 1);

            // Calculate position with offset adjustment
            let adjusted_start = (match_start as isize + offset) as usize;
            let adjusted_end = adjusted_start + full_match.as_str().len();

            result.replace_range(adjusted_start..adjusted_end, &replacement);

            // Update offset for subsequent replacements
            offset += replacement.len() as isize - full_match.as_str().len() as isize;
        }
    }

    log::debug!(
        "🔧 Parameter substitution: {} parameters processed",
        parameters.len()
    );
    result
}

/// Legacy function - replaced by substitute_cypher_parameters
/// Kept for potential future use but currently unused
#[allow(dead_code)]
fn decode_id_parameters(
    query: &str,
    mut parameters: HashMap<String, Value>,
) -> HashMap<String, Value> {
    use super::id_mapper::IdMapper;
    use regex::Regex;

    // Pattern: id(alias) IN $paramName
    let id_in_param = Regex::new(r"(?i)\bid\s*\(\s*\w+\s*\)\s+IN\s+\$(\w+)").unwrap();
    // Pattern: id(alias) = $paramName
    let id_eq_param = Regex::new(r"(?i)\bid\s*\(\s*\w+\s*\)\s*=\s*\$(\w+)").unwrap();

    // Collect parameter names used with id()
    let mut id_params: Vec<String> = Vec::new();

    for cap in id_in_param.captures_iter(query) {
        if let Some(param_name) = cap.get(1) {
            id_params.push(param_name.as_str().to_string());
        }
    }
    for cap in id_eq_param.captures_iter(query) {
        if let Some(param_name) = cap.get(1) {
            id_params.push(param_name.as_str().to_string());
        }
    }

    if id_params.is_empty() {
        return parameters;
    }

    log::info!(
        "🔧 decode_id_parameters: Found id() parameters: {:?}",
        id_params
    );

    // Decode each id() parameter
    for param_name in id_params {
        if let Some(value) = parameters.get_mut(&param_name) {
            match value {
                Value::Array(arr) => {
                    // Decode each element in the array
                    let decoded: Vec<Value> = arr
                        .iter()
                        .filter_map(|v| {
                            if let Some(encoded_id) = v.as_i64() {
                                // Use IdMapper to decode (tries cache first)
                                if let Some((_label, raw_value)) =
                                    IdMapper::decode_for_query(encoded_id)
                                {
                                    log::debug!(
                                        "  Decoded {} -> {} (from cache)",
                                        encoded_id,
                                        raw_value
                                    );
                                    // Try to parse as integer, fallback to string
                                    if let Ok(int_val) = raw_value.parse::<i64>() {
                                        return Some(Value::Number(int_val.into()));
                                    }
                                    return Some(Value::String(raw_value));
                                } else {
                                    // Fallback: extract raw_value directly from bit pattern
                                    // This handles cross-session IDs where cache doesn't have the mapping
                                    // Use 47-bit mask (matching the JS-safe encoding in id_encoding.rs)
                                    const ID_MASK: i64 = (1i64 << 47) - 1; // 0x7FFFFFFFFFFF
                                    let raw_value = encoded_id & ID_MASK;
                                    log::debug!(
                                        "  Decoded {} -> {} (from bit pattern)",
                                        encoded_id,
                                        raw_value
                                    );
                                    return Some(Value::Number(raw_value.into()));
                                }
                            }
                            // Keep original if not a number
                            Some(v.clone())
                        })
                        .collect();

                    log::info!(
                        "🔧 Decoded parameter '{}': {} values (from {} original)",
                        param_name,
                        decoded.len(),
                        arr.len()
                    );
                    *value = Value::Array(decoded);
                }
                Value::Number(n) => {
                    // Single value
                    if let Some(encoded_id) = n.as_i64() {
                        if let Some((_label, raw_value)) = IdMapper::decode_for_query(encoded_id) {
                            log::info!(
                                "🔧 Decoded parameter '{}': {} -> {} (from cache)",
                                param_name,
                                encoded_id,
                                raw_value
                            );
                            if let Ok(int_val) = raw_value.parse::<i64>() {
                                *value = Value::Number(int_val.into());
                            } else {
                                *value = Value::String(raw_value);
                            }
                        } else {
                            // Fallback: extract raw_value directly from bit pattern
                            // Use 47-bit mask (matching the JS-safe encoding in id_encoding.rs)
                            const ID_MASK: i64 = (1i64 << 47) - 1; // 0x7FFFFFFFFFFF
                            let raw_value = encoded_id & ID_MASK;
                            log::info!(
                                "🔧 Decoded parameter '{}': {} -> {} (from bit pattern)",
                                param_name,
                                encoded_id,
                                raw_value
                            );
                            *value = Value::Number(raw_value.into());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    parameters
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
    cached_results: Option<Vec<Vec<BoltValue>>>,
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
            signatures::ROUTE => self.handle_route(message).await,
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
        log::info!("🔍 HELLO message has {} fields", message.fields.len());
        for (i, field) in message.fields.iter().enumerate() {
            log::info!("  HELLO Field[{}]: {}", i, bolt_value_to_string(field));
        }

        if is_bolt_51_plus {
            // Bolt 5.1+: HELLO just initializes connection, auth happens in LOGON
            log::info!("HELLO received (Bolt 5.1+), awaiting LOGON for authentication");

            // Extract database from HELLO extra field (routing context)
            let database = message.extract_database();
            log::info!("📊 Extracted database from HELLO: {:?}", database);

            // Store database selection in context for later use in LOGON
            if let Some(ref db_name) = database {
                let mut context = lock_context!(self.context);
                context.schema_name = Some(db_name.clone());
                context.id_mapper.set_scope(Some(db_name.clone()), None);
                log::info!("Database/schema specified in HELLO: {}", db_name);
            }

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
                log::debug!("  Field[{}]: {}", i, bolt_value_to_string(field));
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
        log::info!("🔍 LOGON message has {} fields", message.fields.len());
        for (i, field) in message.fields.iter().enumerate() {
            log::info!("  LOGON Field[{}]: {}", i, bolt_value_to_string(field));
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

                // If no database in LOGON, check if it was set in HELLO (Bolt 5.1+)
                if database.is_none() {
                    let context = lock_context!(self.context);
                    database = context.schema_name.clone();
                    if database.is_some() {
                        log::debug!("Using database from HELLO: {:?}", database);
                    }
                }

                // If still no database specified, use the first loaded schema (if any)
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
                    context.id_mapper.set_scope(database.clone(), None);
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
            context.tenant_id = None;
            context.id_mapper.set_scope(None, None);
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

    /// Handle session commands like `CALL sys.set('key', 'value')` or
    /// `CALL dbms.setConfigValue('key', 'value')` (browser-friendly alias).
    /// These are intercepted before Cypher parsing and modify BoltContext state.
    /// Returns Some(response) if the query was a session command, None otherwise.
    async fn handle_session_command(
        &mut self,
        query: &str,
    ) -> BoltResult<Option<Vec<BoltMessage>>> {
        let trimmed = query.trim();
        let lower = trimmed.to_lowercase();

        // Accept both CALL sys.set(...) and CALL dbms.setConfigValue(...)
        let is_session_cmd =
            lower.starts_with("call sys.set") || lower.starts_with("call dbms.setconfigvalue");
        if !is_session_cmd {
            return Ok(None);
        }

        // Bare command without parens — usage error
        if !trimmed.contains('(') {
            return Ok(Some(vec![BoltMessage::failure(
                "Neo.ClientError.Statement.SyntaxError".to_string(),
                "Usage: CALL sys.set('key', 'value') or CALL dbms.setConfigValue('key', 'value')"
                    .to_string(),
            )]));
        }

        // Parse arguments from CALL sys.set(arg1, arg2)
        let inner = trimmed.trim_end_matches(')').trim_end_matches(';');
        let inner = if let Some(pos) = inner.find('(') {
            &inner[pos + 1..]
        } else {
            return Ok(Some(vec![BoltMessage::failure(
                "Neo.ClientError.Statement.SyntaxError".to_string(),
                "Usage: CALL dbms.setConfigValue('key', 'value')".to_string(),
            )]));
        };

        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        if parts.len() != 2 {
            return Ok(Some(vec![BoltMessage::failure(
                "Neo.ClientError.Statement.SyntaxError".to_string(),
                "Usage: CALL dbms.setConfigValue('key', 'value')".to_string(),
            )]));
        }

        let key = parts[0].trim().trim_matches('\'').trim_matches('"');
        let value = parts[1].trim().trim_matches('\'').trim_matches('"');

        log::info!("Session command: setting {} = {}", key, value);

        // Store in BoltContext metadata and update IdMapper scope
        {
            let mut context = lock_context!(self.context);
            context.metadata.insert(key.to_string(), value.to_string());

            // Special handling for tenant_id
            if key == "tenant_id" {
                context.tenant_id = Some(value.to_string());
                let schema = context.schema_name.clone();
                context.id_mapper.set_scope(schema, Some(value.to_string()));
            }

            // Set state to Streaming so PULL can deliver the cached result
            context.set_state(ConnectionState::Streaming);
        }

        // Return SUCCESS with fields metadata so browser can PULL the result
        let mut meta = HashMap::new();
        meta.insert(
            "fields".to_string(),
            Value::Array(vec![Value::String("result".to_string())]),
        );
        // Store confirmation message for PULL
        self.cached_results = Some(vec![vec![BoltValue::Json(Value::String(format!(
            "Session {} set to {}",
            key, value
        )))]]);

        Ok(Some(vec![BoltMessage::success(meta)]))
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

        // Handle EXPLAIN queries — browser sends "EXPLAIN <partial_query>" as autocomplete
        // probes while the user types. Return empty SUCCESS so probes don't show errors.
        // For fully-formed EXPLAIN queries, this also prevents unnecessary execution.
        if query.trim().to_lowercase().starts_with("explain ") {
            log::debug!("EXPLAIN query (returning empty plan): {}", query.trim());
            let mut meta = HashMap::new();
            meta.insert("fields".to_string(), Value::Array(vec![]));
            // Set state to Streaming so the subsequent PULL gets a clean completion
            {
                let mut context = lock_context!(self.context);
                context.set_state(ConnectionState::Streaming);
            }
            self.cached_results = Some(vec![]);
            return Ok(vec![BoltMessage::success(meta)]);
        }

        // Intercept session commands before Cypher parsing
        if let Some(response) = self.handle_session_command(&query).await? {
            return Ok(response);
        }

        let parameters = message.extract_parameters().unwrap_or_default();

        // Substitute Cypher parameters into query string (keeping encoded IDs)
        // This allows parser to see actual values as literals while preserving encoding
        // The AST transformer will then extract labels from encoded IDs for UNION pruning
        let query = substitute_cypher_parameters(query, &parameters);

        // NOTE: Do NOT rewrite the browser's directed relationship-fetch to undirected.
        // The browser's Path objects from the expand query already carry all relationship data.
        // Making it undirected causes Relationship objects with schema-direction start/end IDs
        // that reference nodes not in the browser's graph, crashing with "t.source is undefined".

        // Get selected schema from context, or from RUN message metadata
        let (schema_name, tenant_id, role, view_parameters) = {
            let context = lock_context!(self.context);

            // Debug: log RUN message fields
            log::info!("🔍 RUN message has {} fields", message.fields.len());
            for (i, field) in message.fields.iter().enumerate() {
                log::info!("  Field[{}]: {}", i, bolt_value_to_string(field));
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
                log::info!(
                    "⚠️  RUN message does NOT contain database field, using context schema: {:?}",
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

        // Update IdMapper scope for cross-session isolation
        {
            let mut context = lock_context!(self.context);
            if let Some(ref tid) = tenant_id {
                context.tenant_id = Some(tid.clone());
            }
            let scope_tenant = context.tenant_id.clone();
            context
                .id_mapper
                .set_scope(schema_name.clone(), scope_tenant);
        }

        log::info!("Executing Cypher query: {}", query);

        if let Some(ref schema) = schema_name {
            log::debug!("Query execution using schema: {}", schema);
        } else {
            log::debug!("Query execution using schema: default");
        }

        // Parse and execute the query with task-local schema context
        // Note: id() predicates with encoded values are decoded in FilterTagging pass
        use crate::server::query_context::{with_query_context, QueryContext};
        let ctx = QueryContext::new(schema_name.clone());
        match with_query_context(
            ctx,
            self.execute_cypher_query(
                &query,
                parameters,
                schema_name,
                tenant_id,
                role,
                view_parameters,
            ),
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
                let error_code = query_error.error_code().to_string();
                let error_message = query_error.to_string();
                log::error!("Query execution failed: {}", query_error);
                log::error!(
                    "Sending FAILURE: code='{}', message='{}'",
                    error_code,
                    error_message
                );

                // Don't update state - let client send RESET to recover
                // Setting to Failed would close the connection

                Ok(vec![BoltMessage::failure(error_code, error_message)])
            }
        }
    }

    /// Handle PULL message (fetch query results)
    async fn handle_pull(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = lock_context!(self.context);
            if !matches!(context.state, ConnectionState::Streaming) {
                // If we're not streaming, a FAILURE was likely already sent
                // Return IGNORED instead of sending another FAILURE
                log::debug!("PULL received in non-streaming state, returning IGNORED");
                return Ok(vec![BoltMessage::ignored()]);
            }
        }

        // Stream the cached results as RECORD messages
        let mut messages = Vec::new();

        if let Some(rows) = self.cached_results.take() {
            log::debug!("Streaming {} rows via Bolt RECORD messages", rows.len());

            // Send each row as a RECORD message
            for row in rows {
                // Row is already Vec<BoltValue> - pass directly
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
        log::debug!("BEGIN message has {} fields", message.fields.len());
        if !message.fields.is_empty() {
            log::debug!(
                "BEGIN Field[0]: {}",
                bolt_value_to_string(&message.fields[0])
            );
        }

        if let Some(db) = message.extract_begin_database() {
            log::info!("✅ BEGIN message contains database: {}", db);
            let mut context = lock_context!(self.context);
            if context.schema_name.as_deref() != Some(&db) {
                log::debug!(
                    "BEGIN message overriding schema: {:?} -> {}",
                    context.schema_name,
                    db
                );
                context.schema_name = Some(db.clone());
                let scope_tenant = context.tenant_id.clone();
                context.id_mapper.set_scope(Some(db), scope_tenant);
            }
        } else {
            log::debug!("BEGIN message does NOT contain database field");
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

    /// Handle ROUTE message (return routing table for database)
    /// ROUTE message format: ROUTE {routing_context} [bookmarks] {extra}
    /// where extra can contain {"db": "database_name"}
    async fn handle_route(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::info!("ROUTE message received");

        // Extract database from ROUTE message (field 2 - extra metadata)
        let database = if message.fields.len() >= 3 {
            if let BoltValue::Json(Value::Object(extra_map)) = &message.fields[2] {
                extra_map
                    .get("db")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                None
            }
        } else {
            None
        };

        let db_name = database.unwrap_or_else(|| "default".to_string());
        log::info!("ROUTE request for database: {}", db_name);

        // Verify schema exists
        let schema_exists = if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
            if let Ok(schemas) = schemas_lock.try_read() {
                schemas.contains_key(&db_name)
            } else {
                false
            }
        } else {
            false
        };

        if !schema_exists {
            log::warn!("ROUTE requested for non-existent database: {}", db_name);
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Database.DatabaseNotFound".to_string(),
                format!("Database '{}' not found", db_name),
            )]);
        }

        // Build routing table response
        // For ClickGraph (single server, no cluster), we return ourselves for all roles
        let server_address = format!("{}:{}", self.config.host, self.config.port);

        let mut routing_table = serde_json::Map::new();
        routing_table.insert("ttl".to_string(), Value::Number(300.into())); // 5 minutes TTL
        routing_table.insert("db".to_string(), Value::String(db_name));

        // Servers list: we are WRITE, READ, and ROUTE all in one
        let servers = serde_json::json!([
            {
                "role": "WRITE",
                "addresses": [server_address.clone()]
            },
            {
                "role": "READ",
                "addresses": [server_address.clone()]
            },
            {
                "role": "ROUTE",
                "addresses": [server_address]
            }
        ]);

        routing_table.insert("servers".to_string(), servers);

        // Return SUCCESS with routing table
        let mut metadata = HashMap::new();
        metadata.insert("rt".to_string(), Value::Object(routing_table));

        log::info!(
            "✅ Returning routing table for database: {}",
            metadata
                .get("rt")
                .and_then(|v| v.get("db"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        );

        Ok(vec![BoltMessage::success(metadata)])
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
        use crate::open_cypher_parser::ast::CypherStatement;

        // ============================================================
        // PHASE 1: Determine Schema (for id() transformation)
        // ============================================================

        // Parse once to extract schema name
        let effective_schema = match open_cypher_parser::parse_cypher_statement(query) {
            Ok((_, stmt)) => match stmt {
                CypherStatement::Query { query, .. } => {
                    if let Some(use_clause) = query.use_clause {
                        use_clause.database_name.to_string()
                    } else {
                        schema_name.as_deref().unwrap_or("default").to_string()
                    }
                }
                CypherStatement::ProcedureCall(_) => {
                    schema_name.as_deref().unwrap_or("default").to_string()
                }
            },
            Err(_) => schema_name.as_deref().unwrap_or("default").to_string(),
        };

        // Load the actual GraphSchema object for id() transformation.
        // Set schema name in task-local context so downstream code can use it.
        crate::server::query_context::set_current_schema_name(Some(effective_schema.clone()));
        let graph_schema = if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
            if let Ok(schemas) = schemas_lock.try_read() {
                schemas.get(&effective_schema).cloned()
            } else {
                None
            }
        } else {
            None
        };

        if graph_schema.is_some() {
            log::info!(
                "✅ Loaded schema '{}' for id() transformation",
                effective_schema
            );
        } else {
            log::warn!(
                "⚠️  Schema '{}' not found for id() transformation",
                effective_schema
            );
        }

        // ============================================================
        // PHASE 2: Parse and Transform (synchronous, single pass)
        // ============================================================

        // Parse Cypher statement for transformation
        let parsed_stmt = match open_cypher_parser::parse_cypher_statement(query) {
            Ok((_, stmt)) => stmt,
            Err(parse_error) => {
                return Err(BoltError::query_error(format!(
                    "Statement parsing failed: {}",
                    parse_error
                )));
            }
        };

        // Transform id() functions using IdMapper (AST-level transformation)
        // Clone IdMapper snapshot for transformation (read-only access)
        let id_mapper_snapshot = {
            let context = lock_context!(self.context);
            context.id_mapper.clone()
        };

        // Parse and transform in a limited scope to extract metadata
        let ast_arena = crate::query_planner::ast_transform::StringArena::new();
        let (is_procedure, _is_union, exec_plan, query_type, _label_constraints) = {
            // Transform id() functions with schema available
            let (transformed_stmt, label_constraints) =
                crate::query_planner::ast_transform::transform_id_functions(
                    &ast_arena,
                    parsed_stmt,
                    &id_mapper_snapshot,
                    graph_schema.as_ref(),
                );

            let is_procedure = crate::procedures::is_procedure_only_statement(&transformed_stmt);
            let is_union = crate::procedures::is_procedure_union_query(&transformed_stmt);

            log::debug!("Query execution using schema: {}", effective_schema);
            log::debug!(
                "Routing decision: is_procedure={}, is_union={}",
                is_procedure,
                is_union
            );

            // Extract execution plan for procedures
            let exec_plan = if is_procedure {
                Some(match &transformed_stmt {
                    CypherStatement::ProcedureCall(proc_call) => ExecutionPlan::SimpleProcedure {
                        proc_name: proc_call.procedure_name.to_string(),
                    },
                    CypherStatement::Query {
                        query: query_ast,
                        union_clauses,
                    } => {
                        if !union_clauses.is_empty() {
                            // Extract branch metadata
                            let mut branches = Vec::new();

                            // Main branch
                            if let Some(call_clause) = &query_ast.call_clause {
                                branches.push(ProcedureBranch {
                                    proc_name: call_clause.procedure_name.to_string(),
                                    has_return: query_ast.return_clause.is_some(),
                                });
                            }

                            // Union branches
                            for union_clause in union_clauses {
                                if let Some(call_clause) = &union_clause.query.call_clause {
                                    branches.push(ProcedureBranch {
                                        proc_name: call_clause.procedure_name.to_string(),
                                        has_return: union_clause.query.return_clause.is_some(),
                                    });
                                }
                            }

                            ExecutionPlan::Union { branches }
                        } else {
                            // Single procedure with possible RETURN
                            if let Some(call_clause) = &query_ast.call_clause {
                                if query_ast.return_clause.is_some() {
                                    ExecutionPlan::ProcedureWithReturn {
                                        proc_name: call_clause.procedure_name.to_string(),
                                    }
                                } else {
                                    ExecutionPlan::SimpleProcedure {
                                        proc_name: call_clause.procedure_name.to_string(),
                                    }
                                }
                            } else {
                                return Err(BoltError::query_error(
                                    "No call clause found".to_string(),
                                ));
                            }
                        }
                    }
                })
            } else {
                None
            };

            // Extract query type for regular queries
            let query_type = match &transformed_stmt {
                CypherStatement::Query {
                    query,
                    union_clauses,
                } => {
                    // Check main query type
                    let main_type = query_planner::get_query_type(query);

                    // For UNION queries, all branches must be Read queries
                    if !union_clauses.is_empty() {
                        // Check each union branch
                        for union_clause in union_clauses {
                            let branch_type = query_planner::get_query_type(&union_clause.query);
                            if branch_type != query_planner::types::QueryType::Read {
                                log::debug!("UNION branch has non-Read type: {:?}", branch_type);
                                return Err(BoltError::query_error(
                                    "Only read queries are currently supported via Bolt protocol"
                                        .to_string(),
                                ));
                            }
                        }
                    }

                    main_type
                }
                CypherStatement::ProcedureCall(_) => {
                    // Procedures are handled above, this shouldn't happen
                    query_planner::types::QueryType::Read // dummy value
                }
            };

            // transformed_stmt is dropped here at end of scope!
            (
                is_procedure,
                is_union,
                exec_plan,
                query_type,
                label_constraints,
            )
        };

        // ============================================================
        // PHASE 3: Route to Procedure or Regular Query Handler
        // ============================================================

        // Handle procedure-only queries (including UNION)
        if is_procedure {
            let exec_plan = exec_plan.expect("exec_plan must be Some when is_procedure=true");

            // Now execute based on exec_plan (no AST references remain)
            let registry = crate::procedures::ProcedureRegistry::new();

            let results = match exec_plan {
                ExecutionPlan::SimpleProcedure { proc_name } => {
                    log::info!("Executing simple procedure via Bolt: {}", proc_name);
                    crate::procedures::executor::execute_procedure_by_name(
                        &proc_name,
                        &effective_schema,
                        &registry,
                    )
                    .await
                    .map_err(|e| {
                        BoltError::query_error(format!("Procedure execution failed: {}", e))
                    })?
                }
                ExecutionPlan::ProcedureWithReturn { proc_name } => {
                    log::info!("Executing procedure with RETURN via Bolt: {}", proc_name);

                    // Execute procedure
                    let raw_results = crate::procedures::executor::execute_procedure_by_name(
                        &proc_name,
                        &effective_schema,
                        &registry,
                    )
                    .await
                    .map_err(BoltError::query_error)?;

                    // Parse original query to get RETURN clause (procedures don't have id() in RETURN)
                    let return_clause = match open_cypher_parser::parse_cypher_statement(query) {
                        Ok((_, CypherStatement::Query { query, .. })) => {
                            query.return_clause.ok_or_else(|| {
                                BoltError::query_error("Expected RETURN clause".to_string())
                            })?
                        }
                        _ => {
                            return Err(BoltError::query_error(
                                "Failed to parse RETURN clause".to_string(),
                            ))
                        }
                    };

                    // Apply RETURN clause
                    crate::procedures::return_evaluator::apply_return_clause(
                        raw_results,
                        &return_clause,
                    )
                    .map_err(|e| {
                        BoltError::query_error(format!("RETURN evaluation failed: {}", e))
                    })?
                }
                ExecutionPlan::Union { branches } => {
                    log::info!(
                        "Executing UNION of procedures via Bolt: {} branches",
                        branches.len()
                    );

                    let mut all_results = Vec::new();

                    // Execute each branch
                    for (idx, branch) in branches.iter().enumerate() {
                        let raw_results = crate::procedures::executor::execute_procedure_by_name(
                            &branch.proc_name,
                            &effective_schema,
                            &registry,
                        )
                        .await
                        .map_err(BoltError::query_error)?;

                        // Apply RETURN clause if branch has one
                        let transformed_results = if branch.has_return {
                            // Parse to get return clause for this branch (after await - safe)
                            let return_clause =
                                match open_cypher_parser::parse_cypher_statement(query) {
                                    Ok((
                                        _,
                                        CypherStatement::Query {
                                            query: main_q,
                                            union_clauses,
                                        },
                                    )) => {
                                        if idx == 0 {
                                            main_q.return_clause
                                        } else {
                                            union_clauses
                                                .get(idx - 1)
                                                .and_then(|uc| uc.query.return_clause.clone())
                                        }
                                    }
                                    _ => None,
                                };

                            if let Some(ref rc) = return_clause {
                                crate::procedures::return_evaluator::apply_return_clause(
                                    raw_results,
                                    rc,
                                )
                                .map_err(|e| {
                                    BoltError::query_error(format!(
                                        "RETURN evaluation failed: {}",
                                        e
                                    ))
                                })?
                            } else {
                                raw_results
                            }
                        } else {
                            raw_results
                        };

                        all_results.extend(transformed_results);
                    }

                    all_results
                }
            };

            // Convert to Bolt records
            let bolt_records: Vec<Vec<BoltValue>> = results
                .iter()
                .map(|record| {
                    let mut values: Vec<BoltValue> = Vec::new();
                    let mut keys: Vec<_> = record.keys().collect();
                    keys.sort();

                    for key in keys {
                        let json_value = &record[key];
                        values.push(BoltValue::Json(json_value.clone()));
                    }
                    values
                })
                .collect();

            // Cache results for PULL
            self.cached_results = Some(bolt_records);

            // Return metadata with field names
            let mut metadata = HashMap::new();
            if let Some(first_record) = results.first() {
                let mut keys: Vec<_> = first_record.keys().map(|k| k.to_string()).collect();
                keys.sort();

                metadata.insert(
                    "fields".to_string(),
                    Value::Array(keys.into_iter().map(Value::String).collect()),
                );
            } else {
                metadata.insert("fields".to_string(), Value::Array(vec![]));
            }
            metadata.insert("t_first".to_string(), Value::Number(0.into()));

            return Ok(metadata);
        }

        // Handle regular queries
        // Check query type
        if query_type != query_planner::types::QueryType::Read {
            return Err(BoltError::query_error(
                "Only read queries are currently supported via Bolt protocol".to_string(),
            ));
        }

        // Get graph schema (safe to await now - all Rc<RefCell<>> dropped)
        let graph_schema = match graph_catalog::get_graph_schema_by_name(&effective_schema).await {
            Ok(schema) => schema,
            Err(e) => {
                return Err(BoltError::query_error(format!("Schema error: {}", e)));
            }
        };

        // Set the resolved schema in task-local context so all downstream
        // code can access it via get_current_schema() without GLOBAL_SCHEMAS lookups
        crate::server::query_context::set_current_schema(std::sync::Arc::new(graph_schema.clone()));

        // Re-parse and transform for planning (after async boundary)
        // Note: This is unavoidable due to Rc<RefCell<>> in AST not being Send
        let parsed_stmt_for_planning = match open_cypher_parser::parse_cypher_statement(query) {
            Ok((_, stmt)) => stmt,
            Err(e) => {
                return Err(BoltError::query_error(format!("Re-parse failed: {}", e)));
            }
        };

        let id_mapper_snapshot = {
            let context = lock_context!(self.context);
            context.id_mapper.clone()
        };

        let (transformed_for_planning, label_constraints_from_second_pass) =
            crate::query_planner::ast_transform::transform_id_functions(
                &ast_arena, // Reuse same arena
                parsed_stmt_for_planning,
                &id_mapper_snapshot,
                Some(&graph_schema), // Pass schema for node_id property lookup
            );

        // Use label_constraints from the second pass (not first) since it has schema context
        log::info!(
            "🎯 Passing {} label constraints to query planner (from second pass)",
            label_constraints_from_second_pass.len()
        );

        // Generate logical plan using transformed statement
        let (logical_plan, mut plan_ctx) = match query_planner::evaluate_read_statement(
            transformed_for_planning,
            &graph_schema,
            tenant_id,
            view_parameters,
            Some(20), // max_inferred_types - increased for UNION branches
        ) {
            Ok(result) => result,
            Err(e) => {
                return Err(BoltError::query_error(format!(
                    "Query planning failed: {}",
                    e
                )));
            }
        };

        // Inject label constraints into plan_ctx for UNION pruning
        if !label_constraints_from_second_pass.is_empty() {
            plan_ctx.set_where_label_constraints(label_constraints_from_second_pass);
        }

        // transformed_for_planning is now dropped

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
                    | super::result_transformer::ReturnItemType::Path { .. }
                    | super::result_transformer::ReturnItemType::IdFunction { .. }
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

        // Substitute parameters in SQL (for non-id() parameters like $name, $age, etc.)
        // Note: id() parameters were already handled in Cypher query substitution (line 741)
        let final_sql = match parameter_substitution::substitute_parameters(&ch_sql, &parameters) {
            Ok(sql) => sql,
            Err(e) => {
                return Err(BoltError::query_error(format!(
                    "Parameter substitution failed: {}",
                    e
                )));
            }
        };

        log::info!("📊 Executing SQL: {}", final_sql);

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

        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
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
                                row_fields
                                    .push(obj.get(field_name).cloned().unwrap_or(Value::Null));
                            }
                            rows.push(row_fields);
                        }
                        _ => {
                            log::warn!("Unexpected JSON format in result row: {}", line);
                        }
                    }
                }
                Ok(None) => {
                    // End of stream - normal completion
                    break;
                }
                Err(e) => {
                    // ClickHouse error during result streaming (e.g., unknown column)
                    return Err(BoltError::query_error(format!(
                        "ClickHouse error while reading results: {}",
                        e
                    )));
                }
            }
        }

        // Transform results if we have graph objects (nodes, relationships, paths)
        if has_graph_objects {
            log::info!(
                "Transforming graph objects. Original field_names: {:?}, metadata items: {}",
                field_names,
                return_metadata.len()
            );

            let mut transformed_rows: Vec<Vec<BoltValue>> = Vec::new();

            // Get mutable access to id_mapper from context for session-scoped ID assignment
            let mut context = lock_context!(self.context);

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
                    &mut context.id_mapper,
                ) {
                    Ok(transformed) => {
                        log::debug!(
                            "Transformed row: {} fields → {} items",
                            field_names.len(),
                            transformed.len()
                        );
                        transformed_rows.push(transformed);
                    }
                    Err(e) => {
                        log::warn!("Failed to transform row to graph objects: {}", e);
                        // Fall back: produce one Null per metadata item to match field count
                        let fallback: Vec<BoltValue> = return_metadata
                            .iter()
                            .map(|_| BoltValue::Json(Value::Null))
                            .collect();
                        transformed_rows.push(fallback);
                    }
                }
            }

            // Release lock before caching
            drop(context);

            // Cache the transformed results
            self.cached_results = Some(transformed_rows);

            // Update field names to match transformed structure
            // Strip ".*" suffix for wildcard expansions (e.g., "a.*" → "a")
            field_names = return_metadata
                .iter()
                .map(|m| {
                    m.field_name
                        .strip_suffix(".*")
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| m.field_name.clone())
                })
                .collect();

            log::info!("After transformation: field_names: {:?}", field_names);
        } else {
            // No graph objects - wrap rows in BoltValue::Json and cache
            let wrapped_rows: Vec<Vec<BoltValue>> = rows
                .into_iter()
                .map(|row| row.into_iter().map(BoltValue::Json).collect())
                .collect();
            self.cached_results = Some(wrapped_rows);
        }

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
