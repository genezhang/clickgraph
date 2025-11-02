//! Bolt Protocol Message Handler
//!
//! This module processes incoming Bolt messages and generates appropriate responses.
//! It handles the complete Bolt protocol state machine and integrates with
//! Brahmand's query processing pipeline.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde_json::Value;

use super::{BoltContext, BoltConfig, ConnectionState};
use super::errors::{BoltError, BoltResult};
use super::messages::{BoltMessage, signatures};
use super::auth::{Authenticator, AuthToken, AuthenticatedUser};

use crate::open_cypher_parser;

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
}

impl BoltHandler {
    /// Create a new Bolt message handler
    pub fn new(context: Arc<Mutex<BoltContext>>, config: Arc<BoltConfig>) -> Self {
        BoltHandler {
            context,
            config: config.clone(),
            authenticator: Authenticator::new(config.enable_auth, config.default_user.clone()),
            authenticated_user: None,
        }
    }

    /// Handle a Bolt message and return response messages
    pub async fn handle_message(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::debug!("Handling Bolt message: {}", message.type_name());

        match message.signature {
            signatures::HELLO => self.handle_hello(message).await,
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

    /// Handle HELLO message (authentication)
    async fn handle_hello(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        let current_state = {
            let context = self.context.lock().unwrap();
            context.state.clone()
        };

        if !matches!(current_state, ConnectionState::Negotiated(_)) {
            return Ok(vec![BoltMessage::failure(
                "Neo.ClientError.Request.Invalid".to_string(),
                "HELLO message received in invalid state".to_string(),
            )]);
        }

        // Extract authentication token
        let auth_token = message.extract_auth_token()
            .unwrap_or_else(HashMap::new);

        // Extract database selection (Neo4j 4.0+ multi-database support)
        let database = message.extract_database();

        // Parse authentication token
        let token = AuthToken::from_hello_fields(&auth_token)?;

        // Authenticate user
        match self.authenticator.authenticate(&token) {
            Ok(user) => {
                self.authenticated_user = Some(user.clone());

                // Update context
                {
                    let mut context = self.context.lock().unwrap();
                    context.set_user(user.username.clone());
                    context.schema_name = database.clone();
                }

                // Log database selection
                if let Some(ref db) = database {
                    log::info!("Bolt connection using database/schema: {}", db);
                } else {
                    log::info!("Bolt connection using default schema");
                }

                // Create success response with server information
                let mut metadata = HashMap::new();
                metadata.insert("server".to_string(), Value::String(self.config.server_agent.clone()));
                metadata.insert("connection_id".to_string(), Value::String("bolt-1".to_string()));
                
                // Add server capabilities
                let mut hints = HashMap::new();
                hints.insert("utc_patch".to_string(), Value::Bool(false));
                hints.insert("patch_bolt".to_string(), Value::Bool(false));
                metadata.insert("hints".to_string(), Value::Object(serde_json::Map::from_iter(hints)));

                log::info!("Bolt authentication successful for user: {}", user.username);
                Ok(vec![BoltMessage::success(metadata)])
            }
            Err(auth_error) => {
                log::warn!("Bolt authentication failed: {}", auth_error);
                
                // Update context to failed state
                {
                    let mut context = self.context.lock().unwrap();
                    context.set_state(ConnectionState::Failed);
                }

                Ok(vec![BoltMessage::failure(
                    auth_error.error_code().to_string(),
                    auth_error.to_string(),
                )])
            }
        }
    }

    /// Handle GOODBYE message (connection termination)
    async fn handle_goodbye(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        log::info!("Received GOODBYE message, closing connection");
        
        // Update context
        {
            let mut context = self.context.lock().unwrap();
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
            let mut context = self.context.lock().unwrap();
            context.set_state(ConnectionState::Ready);
            context.tx_id = None; // Clear any active transaction
        }

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle RUN message (execute Cypher query)
    async fn handle_run(&mut self, message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = self.context.lock().unwrap();
            if !context.is_ready() {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "RUN message received in invalid state".to_string(),
                )]);
            }
        }

        // Extract query and parameters
        let query = message.extract_query()
            .ok_or_else(|| BoltError::invalid_message("RUN message missing query"))?;
            
        let parameters = message.extract_parameters()
            .unwrap_or_else(HashMap::new);

        // Get selected schema from context
        let schema_name = {
            let context = self.context.lock().unwrap();
            context.schema_name.clone()
        };

        log::info!("Executing Cypher query: {}", query);
        if let Some(ref schema) = schema_name {
            log::debug!("Using schema: {}", schema);
        }

        // Parse and execute the query
        match self.execute_cypher_query(query, parameters, schema_name).await {
            Ok(result_metadata) => {
                // Update context to streaming state
                {
                    let mut context = self.context.lock().unwrap();
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
            let context = self.context.lock().unwrap();
            if !matches!(context.state, ConnectionState::Streaming) {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "PULL message received in invalid state".to_string(),
                )]);
            }
        }

        // For now, return empty result set
        // In a full implementation, this would fetch actual query results
        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), Value::String("r".to_string()));
        metadata.insert("has_more".to_string(), Value::Bool(false));

        // Update context back to ready state
        {
            let mut context = self.context.lock().unwrap();
            context.set_state(ConnectionState::Ready);
        }

        Ok(vec![BoltMessage::success(metadata)])
    }

    /// Handle DISCARD message (discard query results)
    async fn handle_discard(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = self.context.lock().unwrap();
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
            let mut context = self.context.lock().unwrap();
            context.set_state(ConnectionState::Ready);
        }

        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), Value::String("r".to_string()));

        Ok(vec![BoltMessage::success(metadata)])
    }

    /// Handle BEGIN message (start transaction)
    async fn handle_begin(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify connection state
        {
            let context = self.context.lock().unwrap();
            if !context.is_ready() {
                return Ok(vec![BoltMessage::failure(
                    "Neo.ClientError.Request.Invalid".to_string(),
                    "BEGIN message received in invalid state".to_string(),
                )]);
            }
        }

        // Generate transaction ID
        let tx_id = format!("tx-{}", chrono::Utc::now().timestamp_millis());

        // Update context with transaction
        {
            let mut context = self.context.lock().unwrap();
            context.tx_id = Some(tx_id.clone());
        }

        log::info!("Started transaction: {}", tx_id);

        Ok(vec![BoltMessage::success(HashMap::new())])
    }

    /// Handle COMMIT message (commit transaction)
    async fn handle_commit(&mut self, _message: BoltMessage) -> BoltResult<Vec<BoltMessage>> {
        // Verify we're in a transaction
        let tx_id = {
            let mut context = self.context.lock().unwrap();
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
            let mut context = self.context.lock().unwrap();
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
        &self,
        query: &str,
        _parameters: HashMap<String, Value>,
        schema_name: Option<String>,
    ) -> BoltResult<HashMap<String, Value>> {
        // Log schema selection
        let schema = schema_name.as_deref().unwrap_or("default");
        log::debug!("Query execution using schema: {}", schema);

        // Parse the Cypher query using Brahmand's parser
        match open_cypher_parser::parse_query(query) {
            Ok(_parsed_query) => {
                // For now, just return success metadata
                // In a full implementation, this would:
                // 1. Transform parsed query to logical plan
                // 2. Optimize the plan
                // 3. Generate ClickHouse SQL using selected schema
                // 4. Execute the SQL
                // 5. Transform results back to graph format
                
                let mut metadata = HashMap::new();
                metadata.insert("fields".to_string(), Value::Array(vec![]));
                metadata.insert("t_first".to_string(), Value::Number(0.into()));
                metadata.insert("qid".to_string(), Value::Number(1.into()));
                
                Ok(metadata)
            }
            Err(parse_error) => {
                Err(BoltError::query_error(format!(
                    "Query parsing failed: {}",
                    parse_error
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_handler() -> BoltHandler {
        let context = Arc::new(Mutex::new(BoltContext::new()));
        let config = Arc::new(BoltConfig::default());
        BoltHandler::new(context, config)
    }

    #[tokio::test]
    async fn test_hello_message_handling() {
        let mut handler = create_test_handler();
        
        // Set context to negotiated state
        {
            let mut context = handler.context.lock().unwrap();
            context.set_version(super::super::BOLT_VERSION_4_4);
        }

        let auth_token = HashMap::from([
            ("scheme".to_string(), Value::String("none".to_string())),
        ]);

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
