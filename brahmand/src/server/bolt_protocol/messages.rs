//! Bolt Protocol Message Types
//!
//! This module defines the message types for the Neo4j Bolt protocol v4.4.
//! Messages are the primary communication unit between client and server.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Bolt message signatures (message types)
pub mod signatures {
    pub const HELLO: u8 = 0x01;
    pub const GOODBYE: u8 = 0x02;
    pub const RESET: u8 = 0x0F;
    pub const RUN: u8 = 0x10;
    pub const DISCARD: u8 = 0x2F;
    pub const PULL: u8 = 0x3F;
    pub const BEGIN: u8 = 0x11;
    pub const COMMIT: u8 = 0x12;
    pub const ROLLBACK: u8 = 0x13;
    pub const ROUTE: u8 = 0x66;
    
    // Response messages
    pub const SUCCESS: u8 = 0x70;
    pub const RECORD: u8 = 0x71;
    pub const IGNORED: u8 = 0x7E;
    pub const FAILURE: u8 = 0x7F;
}

/// Bolt message structure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoltMessage {
    /// Message signature (type)
    pub signature: u8,
    /// Message fields
    pub fields: Vec<Value>,
}

impl BoltMessage {
    /// Create a new Bolt message
    pub fn new(signature: u8, fields: Vec<Value>) -> Self {
        BoltMessage { signature, fields }
    }

    /// Create a HELLO message
    pub fn hello(user_agent: String, auth_token: HashMap<String, Value>) -> Self {
        BoltMessage::new(
            signatures::HELLO,
            vec![
                Value::Object(serde_json::Map::from_iter([
                    ("user_agent".to_string(), Value::String(user_agent)),
                ])),
                Value::Object(serde_json::Map::from_iter(auth_token)),
            ],
        )
    }

    /// Create a GOODBYE message
    pub fn goodbye() -> Self {
        BoltMessage::new(signatures::GOODBYE, vec![])
    }

    /// Create a RESET message
    pub fn reset() -> Self {
        BoltMessage::new(signatures::RESET, vec![])
    }

    /// Create a RUN message
    pub fn run(query: String, parameters: HashMap<String, Value>, extra: Option<HashMap<String, Value>>) -> Self {
        let mut fields = vec![
            Value::String(query),
            Value::Object(serde_json::Map::from_iter(parameters)),
        ];
        
        if let Some(extra_map) = extra {
            fields.push(Value::Object(serde_json::Map::from_iter(extra_map)));
        }
        
        BoltMessage::new(signatures::RUN, fields)
    }

    /// Create a PULL message
    pub fn pull(n: i64, qid: Option<i64>) -> Self {
        let mut extra = serde_json::Map::new();
        extra.insert("n".to_string(), Value::Number(n.into()));
        
        if let Some(qid) = qid {
            extra.insert("qid".to_string(), Value::Number(qid.into()));
        }
        
        BoltMessage::new(signatures::PULL, vec![Value::Object(extra)])
    }

    /// Create a DISCARD message
    pub fn discard(n: i64, qid: Option<i64>) -> Self {
        let mut extra = serde_json::Map::new();
        extra.insert("n".to_string(), Value::Number(n.into()));
        
        if let Some(qid) = qid {
            extra.insert("qid".to_string(), Value::Number(qid.into()));
        }
        
        BoltMessage::new(signatures::DISCARD, vec![Value::Object(extra)])
    }

    /// Create a BEGIN message
    pub fn begin(extra: Option<HashMap<String, Value>>) -> Self {
        let fields = if let Some(extra_map) = extra {
            vec![Value::Object(serde_json::Map::from_iter(extra_map))]
        } else {
            vec![Value::Object(serde_json::Map::new())]
        };
        
        BoltMessage::new(signatures::BEGIN, fields)
    }

    /// Create a COMMIT message
    pub fn commit() -> Self {
        BoltMessage::new(signatures::COMMIT, vec![])
    }

    /// Create a ROLLBACK message
    pub fn rollback() -> Self {
        BoltMessage::new(signatures::ROLLBACK, vec![])
    }

    /// Create a SUCCESS response message
    pub fn success(metadata: HashMap<String, Value>) -> Self {
        BoltMessage::new(
            signatures::SUCCESS,
            vec![Value::Object(serde_json::Map::from_iter(metadata))],
        )
    }

    /// Create a RECORD response message
    pub fn record(fields: Vec<Value>) -> Self {
        BoltMessage::new(signatures::RECORD, vec![Value::Array(fields)])
    }

    /// Create a FAILURE response message
    pub fn failure(code: String, message: String) -> Self {
        let metadata = HashMap::from([
            ("code".to_string(), Value::String(code)),
            ("message".to_string(), Value::String(message)),
        ]);
        
        BoltMessage::new(
            signatures::FAILURE,
            vec![Value::Object(serde_json::Map::from_iter(metadata))],
        )
    }

    /// Create an IGNORED response message
    pub fn ignored() -> Self {
        BoltMessage::new(signatures::IGNORED, vec![])
    }

    /// Get the message type name for debugging
    pub fn type_name(&self) -> &'static str {
        match self.signature {
            signatures::HELLO => "HELLO",
            signatures::GOODBYE => "GOODBYE",
            signatures::RESET => "RESET",
            signatures::RUN => "RUN",
            signatures::DISCARD => "DISCARD",
            signatures::PULL => "PULL",
            signatures::BEGIN => "BEGIN",
            signatures::COMMIT => "COMMIT",
            signatures::ROLLBACK => "ROLLBACK",
            signatures::ROUTE => "ROUTE",
            signatures::SUCCESS => "SUCCESS",
            signatures::RECORD => "RECORD",
            signatures::IGNORED => "IGNORED",
            signatures::FAILURE => "FAILURE",
            _ => "UNKNOWN",
        }
    }

    /// Check if this is a request message
    pub fn is_request(&self) -> bool {
        matches!(
            self.signature,
            signatures::HELLO
                | signatures::GOODBYE
                | signatures::RESET
                | signatures::RUN
                | signatures::DISCARD
                | signatures::PULL
                | signatures::BEGIN
                | signatures::COMMIT
                | signatures::ROLLBACK
                | signatures::ROUTE
        )
    }

    /// Check if this is a response message
    pub fn is_response(&self) -> bool {
        matches!(
            self.signature,
            signatures::SUCCESS | signatures::RECORD | signatures::IGNORED | signatures::FAILURE
        )
    }

    /// Extract authentication token from HELLO message
    pub fn extract_auth_token(&self) -> Option<HashMap<String, Value>> {
        if self.signature == signatures::HELLO && self.fields.len() >= 2 {
            if let Value::Object(auth_map) = &self.fields[1] {
                return Some(auth_map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            }
        }
        None
    }

    /// Extract query from RUN message
    pub fn extract_query(&self) -> Option<&str> {
        if self.signature == signatures::RUN && !self.fields.is_empty() {
            if let Value::String(query) = &self.fields[0] {
                return Some(query);
            }
        }
        None
    }

    /// Extract parameters from RUN message
    pub fn extract_parameters(&self) -> Option<HashMap<String, Value>> {
        if self.signature == signatures::RUN && self.fields.len() >= 2 {
            if let Value::Object(params_map) = &self.fields[1] {
                return Some(params_map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            }
        }
        None
    }
}

/// Bolt message chunk for streaming large messages
#[derive(Debug, Clone)]
pub struct BoltChunk {
    /// Chunk size (2 bytes, big-endian)
    pub size: u16,
    /// Chunk data
    pub data: Vec<u8>,
}

impl BoltChunk {
    /// Create a new chunk
    pub fn new(data: Vec<u8>) -> Self {
        BoltChunk {
            size: data.len() as u16,
            data,
        }
    }

    /// Create an end-of-message marker chunk
    pub fn end_marker() -> Self {
        BoltChunk {
            size: 0,
            data: vec![],
        }
    }

    /// Check if this is an end-of-message marker
    pub fn is_end_marker(&self) -> bool {
        self.size == 0 && self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hello_message_creation() {
        let auth_token = HashMap::from([
            ("scheme".to_string(), Value::String("basic".to_string())),
            ("principal".to_string(), Value::String("user".to_string())),
            ("credentials".to_string(), Value::String("password".to_string())),
        ]);

        let hello = BoltMessage::hello("Brahmand/1.0".to_string(), auth_token.clone());
        assert_eq!(hello.signature, signatures::HELLO);
        assert_eq!(hello.fields.len(), 2);
        
        let extracted_auth = hello.extract_auth_token();
        assert!(extracted_auth.is_some());
        assert_eq!(extracted_auth.unwrap(), auth_token);
    }

    #[test]
    fn test_run_message_creation() {
        let parameters = HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
        ]);

        let run = BoltMessage::run(
            "MATCH (n:Person {name: $name}) RETURN n".to_string(),
            parameters.clone(),
            None,
        );

        assert_eq!(run.signature, signatures::RUN);
        assert_eq!(run.extract_query(), Some("MATCH (n:Person {name: $name}) RETURN n"));
        assert_eq!(run.extract_parameters(), Some(parameters));
    }

    #[test]
    fn test_message_type_identification() {
        let success = BoltMessage::success(HashMap::new());
        assert!(success.is_response());
        assert!(!success.is_request());

        let hello = BoltMessage::hello("Test".to_string(), HashMap::new());
        assert!(hello.is_request());
        assert!(!hello.is_response());
    }

    #[test]
    fn test_failure_message() {
        let failure = BoltMessage::failure(
            "Neo.ClientError.Statement.SyntaxError".to_string(),
            "Invalid syntax".to_string(),
        );

        assert_eq!(failure.signature, signatures::FAILURE);
        assert_eq!(failure.type_name(), "FAILURE");
    }

    #[test]
    fn test_bolt_chunk() {
        let data = vec![1, 2, 3, 4];
        let chunk = BoltChunk::new(data.clone());
        assert_eq!(chunk.size, 4);
        assert_eq!(chunk.data, data);
        assert!(!chunk.is_end_marker());

        let end_chunk = BoltChunk::end_marker();
        assert_eq!(end_chunk.size, 0);
        assert!(end_chunk.is_end_marker());
    }
}
