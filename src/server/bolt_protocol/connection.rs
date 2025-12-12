//! Bolt Connection Management
//!
//! This module handles individual Bolt protocol connections, including
//! version negotiation, message parsing, and connection lifecycle management.

use bytes::Bytes;
use clickhouse::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{timeout, Duration};

use crate::packstream; // Our vendored packstream module

use super::errors::{BoltError, BoltResult};
use super::handler::BoltHandler;
use super::messages::{signatures, BoltChunk, BoltMessage};
use super::{BoltConfig, BoltContext, ConnectionState, SUPPORTED_VERSIONS};

/// Magic preamble for Bolt protocol
const BOLT_MAGIC_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Maximum message size (64KB by default)
/// Reserved for future use when message size validation is implemented
#[allow(dead_code)]
const DEFAULT_MAX_MESSAGE_SIZE: usize = 65536;

/// Bolt connection handler
pub struct BoltConnection<S> {
    /// Underlying TCP stream
    stream: S,
    /// Connection context
    context: Arc<Mutex<BoltContext>>,
    /// Server configuration
    config: Arc<BoltConfig>,
    /// Message handler
    handler: BoltHandler,
}

impl<S> BoltConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Create a new Bolt connection
    pub fn new(
        stream: S,
        context: Arc<Mutex<BoltContext>>,
        config: Arc<BoltConfig>,
        clickhouse_client: Client,
    ) -> Self {
        BoltConnection {
            stream,
            context: context.clone(),
            config: config.clone(),
            handler: BoltHandler::new(context, config, clickhouse_client),
        }
    }

    /// Handle the complete connection lifecycle
    pub async fn handle(mut self) -> BoltResult<()> {
        // Apply connection timeout
        let timeout_duration = Duration::from_secs(self.config.connection_timeout);

        timeout(timeout_duration, async {
            // Step 1: Perform handshake and version negotiation
            self.perform_handshake().await?;

            // Step 2: Handle messages until connection closes
            self.message_loop().await?;

            Ok::<(), BoltError>(())
        })
        .await
        .map_err(|_| BoltError::ConnectionTimeout {
            timeout_seconds: self.config.connection_timeout,
        })?
    }

    /// Perform Bolt protocol handshake and version negotiation
    async fn perform_handshake(&mut self) -> BoltResult<()> {
        // Read magic preamble (4 bytes)
        let mut preamble = [0u8; 4];
        self.stream.read_exact(&mut preamble).await?;

        if preamble != BOLT_MAGIC_PREAMBLE {
            return Err(BoltError::invalid_message(format!(
                "Invalid magic preamble: {:?}, expected: {:?}",
                preamble, BOLT_MAGIC_PREAMBLE
            )));
        }

        // Read client version preferences (4 versions * 4 bytes each = 16 bytes)
        let mut version_buffer = [0u8; 16];
        self.stream.read_exact(&mut version_buffer).await?;

        // Parse client versions (big-endian u32)
        let mut client_versions = Vec::new();
        for chunk in version_buffer.chunks_exact(4) {
            let version = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            if version != 0 {
                client_versions.push(version);
            }
        }

        // Log client version proposals
        log::info!("Client proposed {} Bolt versions:", client_versions.len());
        for (i, &version) in client_versions.iter().enumerate() {
            log::info!(
                "  [{}] {} (0x{:08X})",
                i,
                super::utils::version_to_string(version),
                version
            );
        }
        log::info!(
            "Server supports: {:?}",
            SUPPORTED_VERSIONS
                .iter()
                .map(|v| super::utils::version_to_string(*v))
                .collect::<Vec<_>>()
        );

        // Negotiate version
        let negotiated_version =
            super::utils::negotiate_version(&client_versions).ok_or_else(|| {
                BoltError::VersionNegotiationFailed {
                    client_versions: client_versions.clone(),
                    server_versions: SUPPORTED_VERSIONS.to_vec(),
                }
            })?;

        // Convert version to client's expected format before sending
        // Bolt 5.x uses swapped byte order: [reserved][range][minor][major]
        let version_to_send = if negotiated_version >= 0x00000500 {
            // Bolt 5.x: swap major/minor bytes
            let major = (negotiated_version >> 8) & 0xFF;
            let minor = negotiated_version & 0xFF;
            (minor << 8) | major // Swap to [00][00][minor][major]
        } else {
            // Bolt 4.x and earlier: use as-is
            negotiated_version
        };

        log::debug!(
            "Sending negotiated version: 0x{:08X} (internal: 0x{:08X})",
            version_to_send,
            negotiated_version
        );

        // Send negotiated version back to client
        self.stream
            .write_all(&version_to_send.to_be_bytes())
            .await?;
        self.stream.flush().await?;

        // Update context with negotiated version
        {
            let mut context = self.context.lock().unwrap();
            context.set_version(negotiated_version);
        }

        log::info!(
            "Bolt connection established with protocol version {}",
            super::utils::version_to_string(negotiated_version)
        );

        Ok(())
    }

    /// Main message processing loop
    async fn message_loop(&mut self) -> BoltResult<()> {
        loop {
            // Read next message
            let message = match self.read_message().await {
                Ok(msg) => msg,
                Err(BoltError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Client disconnected gracefully
                    log::info!("Bolt client disconnected");
                    break;
                }
                Err(e) => return Err(e),
            };

            log::debug!("Received Bolt message: {}", message.type_name());

            // Process message and get response
            let responses = self.handler.handle_message(message).await?;

            // Send responses back to client
            for response in responses {
                self.send_message(response).await?;
            }

            // Check if we should close the connection
            {
                let context = self.context.lock().unwrap();
                if matches!(context.state, ConnectionState::Failed) {
                    log::info!("Closing Bolt connection due to failed state");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Read a single Bolt message from the stream
    async fn read_message(&mut self) -> BoltResult<BoltMessage> {
        let mut chunks = Vec::new();

        // Read chunks until we get an end marker
        loop {
            // Read chunk size (2 bytes, big-endian)
            let mut size_bytes = [0u8; 2];
            self.stream.read_exact(&mut size_bytes).await?;
            let chunk_size = u16::from_be_bytes(size_bytes);

            if chunk_size == 0 {
                // End of message marker
                break;
            }

            // Check message size limit
            let total_size: usize = chunks.iter().map(|c: &BoltChunk| c.data.len()).sum();
            if total_size + chunk_size as usize > self.config.max_message_size {
                return Err(BoltError::MessageTooLarge {
                    size: total_size + chunk_size as usize,
                    max_size: self.config.max_message_size,
                });
            }

            // Read chunk data
            let mut chunk_data = vec![0u8; chunk_size as usize];
            self.stream.read_exact(&mut chunk_data).await?;

            chunks.push(BoltChunk::new(chunk_data));
        }

        // Combine all chunks into a single message buffer
        let mut message_data = Vec::new();
        for chunk in chunks {
            message_data.extend(chunk.data);
        }

        // Parse the message
        self.parse_message(message_data)
    }

    /// Send a Bolt message to the client
    async fn send_message(&mut self, message: BoltMessage) -> BoltResult<()> {
        // Serialize message to bytes
        let message_bytes = self.serialize_message(message)?;

        // Split into chunks if necessary
        let chunks = self.create_chunks(message_bytes);

        // Send each chunk
        for chunk in chunks {
            // Write chunk size (2 bytes, big-endian)
            self.stream.write_all(&chunk.size.to_be_bytes()).await?;

            // Write chunk data
            if !chunk.data.is_empty() {
                self.stream.write_all(&chunk.data).await?;
            }
        }

        // Send end-of-message marker (chunk size 0)
        self.stream.write_all(&[0x00, 0x00]).await?;
        self.stream.flush().await?;

        Ok(())
    }

    /// Parse message bytes into a BoltMessage
    fn parse_message(&self, data: Vec<u8>) -> BoltResult<BoltMessage> {
        if data.is_empty() {
            return Err(BoltError::invalid_message("Empty message"));
        }

        // Convert to Bytes for neo4rs packstream
        let bytes = Bytes::from(data);

        // PackStream Bolt messages are structures: 0xB[size] [signature] [field1] [field2] ...
        // We need to manually parse the structure wrapper to get signature and fields

        let cursor = bytes.clone();
        if cursor.is_empty() {
            return Err(BoltError::invalid_message("Empty message data"));
        }

        // Read structure marker
        let marker = cursor[0];
        if (marker & 0xF0) != 0xB0 {
            return Err(BoltError::invalid_message(format!(
                "Expected structure marker (0xB0-0xBF), got 0x{:02X}",
                marker
            )));
        }

        let field_count = (marker & 0x0F) as usize;

        // Read signature (message type)
        if cursor.len() < 2 {
            return Err(BoltError::invalid_message("Missing signature byte"));
        }
        let signature = cursor[1];

        // Parse fields based on message type
        // For now, we'll deserialize the remaining bytes as a tuple/list
        // The actual field structure depends on the message type

        match signature {
            signatures::HELLO => {
                // HELLO has 1 field: extra metadata map
                // We expect: Structure(1) { map }
                if field_count != 1 {
                    return Err(BoltError::invalid_message(format!(
                        "HELLO expects 1 field, got {}",
                        field_count
                    )));
                }

                // Parse the metadata map from bytes[2..]
                let field_bytes = Bytes::from(cursor[2..].to_vec());
                let metadata: HashMap<String, Value> = packstream::from_bytes(field_bytes)
                    .map_err(|e| {
                        BoltError::invalid_message(format!("Failed to parse HELLO metadata: {}", e))
                    })?;

                Ok(BoltMessage::new(
                    signature,
                    vec![Value::Object(serde_json::Map::from_iter(metadata))],
                ))
            }

            signatures::RUN => {
                // RUN has 2-3 fields: query string, parameters map, optional extra map
                if field_count < 2 || field_count > 3 {
                    return Err(BoltError::invalid_message(format!(
                        "RUN expects 2-3 fields, got {}",
                        field_count
                    )));
                }

                // Parse all fields from the remaining bytes
                let field_bytes = Bytes::from(cursor[2..].to_vec());

                // Parse as tuple: (String, HashMap<String, Value>, Option<HashMap<String, Value>>)
                let fields: (String, HashMap<String, serde_json::Value>) =
                    packstream::from_bytes(field_bytes.clone()).map_err(|e| {
                        BoltError::invalid_message(format!("Failed to parse RUN fields: {:?}", e))
                    })?;

                // Convert to BoltMessage values
                let values = vec![
                    Value::String(fields.0),                                         // query
                    Value::Object(serde_json::Map::from_iter(fields.1.into_iter())), // parameters
                ];

                // If field_count == 3, we'd parse the optional extra map here
                // For now, RUN with 2 fields is most common

                Ok(BoltMessage::new(signature, values))
            }

            signatures::PULL => {
                // PULL has 1 field: extra metadata map with 'n' field
                if field_count != 1 {
                    return Err(BoltError::invalid_message(format!(
                        "PULL expects 1 field, got {}",
                        field_count
                    )));
                }

                // Parse the metadata map
                let field_bytes = Bytes::from(cursor[2..].to_vec());
                let metadata: HashMap<String, Value> = packstream::from_bytes(field_bytes)
                    .map_err(|e| {
                        BoltError::invalid_message(format!("Failed to parse PULL metadata: {}", e))
                    })?;

                Ok(BoltMessage::new(
                    signature,
                    vec![Value::Object(serde_json::Map::from_iter(metadata))],
                ))
            }

            _ => {
                // For other messages, create empty field list for now
                Ok(BoltMessage::new(signature, vec![]))
            }
        }
    }

    /// Serialize a BoltMessage to bytes
    fn serialize_message(&self, message: BoltMessage) -> BoltResult<Vec<u8>> {
        // Bolt messages are PackStream structures: 0xB[field_count] [signature] [field1] [field2] ...
        let field_count = message.fields.len();

        if field_count > 15 {
            return Err(BoltError::invalid_message(format!(
                "Message has too many fields: {}. Maximum is 15 for tiny struct.",
                field_count
            )));
        }

        let mut bytes = Vec::new();

        // Write structure marker (0xB0 | field_count)
        bytes.push(0xB0 | (field_count as u8));

        // Write signature
        bytes.push(message.signature);

        // Serialize each field using PackStream
        for field in message.fields {
            let field_bytes = packstream::to_bytes(&field).map_err(|e| {
                BoltError::invalid_message(format!("Failed to serialize field: {:?}", e))
            })?;
            bytes.extend_from_slice(&field_bytes);
        }

        Ok(bytes)
    }

    /// Split message bytes into chunks
    fn create_chunks(&self, data: Vec<u8>) -> Vec<BoltChunk> {
        const CHUNK_SIZE: usize = 65535; // Maximum chunk size

        let mut chunks = Vec::new();

        for chunk_data in data.chunks(CHUNK_SIZE) {
            chunks.push(BoltChunk::new(chunk_data.to_vec()));
        }

        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite};

    // Mock stream for testing
    struct MockStream {
        read_data: Vec<u8>,
        read_pos: usize,
        write_data: Vec<u8>,
    }

    impl MockStream {
        fn new(read_data: Vec<u8>) -> Self {
            MockStream {
                read_data,
                read_pos: 0,
                write_data: Vec::new(),
            }
        }
    }

    impl AsyncRead for MockStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let remaining = self.read_data.len() - self.read_pos;
            let to_read = std::cmp::min(buf.remaining(), remaining);

            if to_read == 0 {
                return Poll::Ready(Ok(()));
            }

            let data = &self.read_data[self.read_pos..self.read_pos + to_read];
            buf.put_slice(data);
            self.read_pos += to_read;

            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MockStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            self.write_data.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_bolt_connection_creation() {
        let stream = MockStream::new(vec![]);
        let context = Arc::new(Mutex::new(BoltContext::new()));
        let config = Arc::new(BoltConfig::default());
        // Create a test ClickHouse client (won't be used in unit tests)
        let clickhouse_client = Client::default().with_url("http://localhost:8123");

        let _connection = BoltConnection::new(stream, context, config, clickhouse_client);
        // Just test that we can create the connection
        assert!(true);
    }

    #[test]
    fn test_chunk_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let chunk = BoltChunk::new(data.clone());
        assert_eq!(chunk.data, data);
        assert_eq!(chunk.size, 5);
    }

    #[test]
    fn test_magic_preamble() {
        assert_eq!(BOLT_MAGIC_PREAMBLE, [0x60, 0x60, 0xB0, 0x17]);
    }
}
