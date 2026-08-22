// src/connection.rs
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use bytes::{BytesMut, Buf};
use std::io::Cursor;

// We import the Frame type from the mini-redis crate
use mini_redis::Frame;

// Define the Result type alias correctly
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            // Allocate 4KB buffer
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Read a single frame from the stream.
    /// Returns None if EOF is reached.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // 1. Attempt to parse a frame from the buffered data.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // 2. Not enough data in buffer. Read more from socket.
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // EOF: connection closed by peer
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    // Peer closed connection while we were waiting for a frame
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// Parse a frame from the current buffer without reading more data.
    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        // Create a Cursor to read from the buffer
        let mut buf = Cursor::new(&self.buffer[..]);

        // Use the PUBLIC Frame::parse method
        // It returns Err(FrameError::Incomplete) if the frame is not complete yet
        match Frame::parse(&mut buf) {
            Ok(frame) => {
                // Success!
                // Get the length of the frame consumed
                let len = buf.position() as usize;

                // Advance the buffer to discard the parsed frame
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            Err(mini_redis::frame::Error::Incomplete) => {
                // Not enough data yet.
                Ok(None)
            }
            Err(e) => {
                // Invalid frame data
                Err(e.into())
            }
        }
    }

    /// Write a frame to the connection.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => {
                return Err("Array frames not implemented in this tutorial step".into());
            }
        }

        // Flush the buffer to ensure data is sent to the socket
        self.stream.flush().await?;
        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> Result<()> {
        let s = val.to_string();
        self.stream.write_all(s.as_bytes()).await?;
        Ok(())
    }
}