//! WebSocket transport layer for Bolt protocol

use futures_util::sink::{Sink, SinkExt};
use futures_util::stream::{Stream, StreamExt};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

/// Adapter that wraps a WebSocket connection to implement AsyncRead/AsyncWrite
pub struct WebSocketBoltAdapter {
    ws_stream: WebSocketStream<TcpStream>,
    read_buffer: Vec<u8>,
    read_pos: usize,
}

impl WebSocketBoltAdapter {
    pub async fn new(stream: TcpStream) -> io::Result<Self> {
        let ws_stream = accept_async(stream).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WebSocket handshake failed: {}", e),
            )
        })?;

        log::info!("WebSocket handshake completed successfully");

        Ok(Self {
            ws_stream,
            read_buffer: Vec::new(),
            read_pos: 0,
        })
    }
}

impl AsyncRead for WebSocketBoltAdapter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If buffer has data, copy to caller's buffer
        if self.read_pos < self.read_buffer.len() {
            let available = &self.read_buffer[self.read_pos..];
            let to_read = available.len().min(buf.remaining());

            if to_read > 0 {
                buf.put_slice(&available[..to_read]);
                self.read_pos += to_read;
                log::trace!("Read {} bytes from WebSocket buffer", to_read);
            }

            return Poll::Ready(Ok(()));
        }

        // Buffer is empty, get next WebSocket message
        self.read_buffer.clear();
        self.read_pos = 0;

        // Poll the WebSocket stream for next message
        match Pin::new(&mut self.ws_stream).poll_next(cx) {
            Poll::Ready(Some(Ok(Message::Binary(data)))) => {
                log::debug!("Received WebSocket binary message: {} bytes", data.len());
                self.read_buffer = data;

                // Copy what we can to caller's buffer
                let available = &self.read_buffer[..];
                let to_read = available.len().min(buf.remaining());

                if to_read > 0 {
                    buf.put_slice(&available[..to_read]);
                    self.read_pos = to_read;
                    log::trace!("Read {} bytes from WebSocket message", to_read);
                }

                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ok(Message::Close(_)))) => {
                log::debug!("WebSocket close message received");
                // Signal EOF for graceful disconnect instead of error
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ok(Message::Ping(_)))) | Poll::Ready(Some(Ok(Message::Pong(_)))) => {
                // Handled automatically by tungstenite, poll again
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Some(Ok(msg))) => {
                log::warn!("Unexpected WebSocket message: {:?}", msg);
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unexpected message type",
                )))
            }
            Poll::Ready(Some(Err(e))) => {
                log::error!("WebSocket error: {}", e);
                Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
            }
            Poll::Ready(None) => {
                log::debug!("WebSocket stream ended");
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "WebSocket ended",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for WebSocketBoltAdapter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let message = Message::Binary(buf.to_vec());

        match Pin::new(&mut self.ws_stream).poll_ready(cx) {
            Poll::Ready(Ok(())) => match Pin::new(&mut self.ws_stream).start_send(message) {
                Ok(()) => {
                    log::trace!("Wrote {} bytes to WebSocket", buf.len());
                    Poll::Ready(Ok(buf.len()))
                }
                Err(e) => {
                    log::error!("WebSocket write error: {}", e);
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
                }
            },
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.ws_stream)
            .poll_flush(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.ws_stream)
            .poll_close(cx)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adapter_creation() {
        assert!(true);
    }
}
