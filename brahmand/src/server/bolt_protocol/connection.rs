//! Bolt Connection Management
//!
//! This module handles individual Bolt protocol connections, including
//! version negotiation, message parsing, and connection lifecycle management.

use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{timeout, Duration};

use super::{BoltContext, BoltConfig, ConnectionState, SUPPORTED_VERSIONS};
use super::errors::{BoltError, BoltResult};
use super::messages::{BoltMessage, BoltChunk, signatures};
use super::handler::BoltHandler;

/// Magic preamble for Bolt protocol
const BOLT_MAGIC_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Maximum message size (64KB by default)
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
    pub fn new(stream: S, context: Arc<Mutex<BoltContext>>, config: Arc<BoltConfig>) -> Self {
        BoltConnection {
            stream,
            context: context.clone(),
            config: config.clone(),
            handler: BoltHandler::new(context, config),
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

        // Negotiate version
        let negotiated_version = super::utils::negotiate_version(&client_versions)
            .ok_or_else(|| BoltError::VersionNegotiationFailed {
                client_versions: client_versions.clone(),
                server_versions: SUPPORTED_VERSIONS.to_vec(),
            })?;

        // Send negotiated version back to client
        self.stream.write_all(&negotiated_version.to_be_bytes()).await?;
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

        // For now, implement a basic parser
        // In a full implementation, this would use the PackStream format
        // specified by the Bolt protocol
        
        let signature = data[0];
        
        // Simplified parsing - in reality this would be much more complex
        // using PackStream binary format
        match signature {
            signatures::HELLO => {
                // Parse HELLO message fields
                Ok(BoltMessage::new(signature, vec![
                    serde_json::Value::Object(serde_json::Map::new()),
                    serde_json::Value::Object(serde_json::Map::new()),
                ]))
            }
            signatures::RUN => {
                // Parse RUN message - simplified
                Ok(BoltMessage::new(signature, vec![
                    serde_json::Value::String("".to_string()),
                    serde_json::Value::Object(serde_json::Map::new()),
                ]))
            }
            _ => {
                // For other messages, create empty field list for now
                Ok(BoltMessage::new(signature, vec![]))
            }
        }
    }

    /// Serialize a BoltMessage to bytes
    fn serialize_message(&self, message: BoltMessage) -> BoltResult<Vec<u8>> {
        // For now, implement a basic serializer
        // In a full implementation, this would use the PackStream format
        
        let mut bytes = Vec::new();
        bytes.push(message.signature);
        
        // Simplified serialization - in reality this would be much more complex
        // using PackStream binary format
        
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
    use tokio::io::{AsyncRead, AsyncWrite};
    use std::pin::Pin;
    use std::task::{Context, Poll};

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

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_bolt_connection_creation() {
        let stream = MockStream::new(vec![]);
        let context = Arc::new(Mutex::new(BoltContext::new()));
        let config = Arc::new(BoltConfig::default());
        
        let connection = BoltConnection::new(stream, context, config);
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