// PostgreSQL wire protocol connection handler

use crate::{database::Database, transaction::manager::TransactionManager};
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{
    message::{Message, MessageType},
    protocol::ProtocolHandler,
    NetworkError, Result,
};

pub struct Connection {
    stream: TcpStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    protocol_handler: ProtocolHandler,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        database: Arc<Database>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
            protocol_handler: ProtocolHandler::new(database, transaction_manager),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        // Handle startup message first
        self.handle_startup().await?;

        // Main message loop
        loop {
            // Read data from socket
            let n = self.stream.read_buf(&mut self.read_buffer).await?;
            if n == 0 {
                return Err(NetworkError::ConnectionClosed);
            }

            // Process messages in the buffer
            while let Some(message) = self.try_read_message()? {
                match message {
                    Message::Terminate => {
                        return Ok(());
                    }
                    _ => {
                        eprintln!("DEBUG: Handling message in protocol handler");
                        let responses = self.protocol_handler.handle_message(message)?;
                        eprintln!("DEBUG: Got {} response messages", responses.len());
                        for response in responses {
                            self.send_message(response).await?;
                        }
                        eprintln!("DEBUG: All responses sent");
                    }
                }
            }
        }
    }

    async fn handle_startup(&mut self) -> Result<()> {
        // Read startup message using special format (no message type byte)
        loop {
            let n = self.stream.read_buf(&mut self.read_buffer).await?;
            if n == 0 {
                return Err(NetworkError::ConnectionClosed);
            }

            // Check for SSL request first
            if self.read_buffer.len() >= 8 {
                let len = i32::from_be_bytes([
                    self.read_buffer[0],
                    self.read_buffer[1],
                    self.read_buffer[2],
                    self.read_buffer[3],
                ]) as usize;
                
                if len == 8 && self.read_buffer.len() >= 8 {
                    let code = i32::from_be_bytes([
                        self.read_buffer[4],
                        self.read_buffer[5],
                        self.read_buffer[6],
                        self.read_buffer[7],
                    ]);
                    
                    if code == crate::network::SSL_REQUEST_CODE {
                        // Consume the SSL request
                        self.read_buffer.advance(8);
                        
                        // Send 'N' to indicate no SSL support
                        self.stream.write_all(b"N").await?;
                        self.stream.flush().await?;
                        
                        // Continue to read the actual startup message
                        continue;
                    }
                }
            }

            // Try to decode startup message
            if let Some(message) = Message::decode_startup(&mut self.read_buffer)? {
                match message {
                    Message::Startup {
                        version,
                        parameters,
                    } => {
                        let responses =
                            self.protocol_handler.handle_startup(version, parameters)?;
                        for response in responses {
                            self.send_message(response).await?;
                        }
                        return Ok(());
                    }
                    _ => {
                        return Err(NetworkError::InvalidMessage(
                            "Expected startup message".to_string(),
                        ))
                    }
                }
            }
            // If we don't have enough data yet, the loop will continue
        }
    }

    fn try_read_message(&mut self) -> Result<Option<Message>> {
        // Check if we have enough bytes for message type and length
        if self.read_buffer.len() < 5 {
            return Ok(None);
        }

        // Peek at message type and length
        let msg_type_byte = self.read_buffer[0];
        let length = u32::from_be_bytes([
            self.read_buffer[1],
            self.read_buffer[2],
            self.read_buffer[3],
            self.read_buffer[4],
        ]) as usize;

        // Check if we have the full message
        if self.read_buffer.len() < length + 1 {
            return Ok(None);
        }

        // Consume message type byte
        let _ = self.read_buffer.split_to(1);

        // Read length and body
        let mut msg_buf = self.read_buffer.split_to(length);
        let _ = msg_buf.split_to(4); // Skip length bytes

        // Decode message based on type byte
        let msg_type = match msg_type_byte {
            b'Q' => MessageType::Query,
            b'P' => MessageType::Parse,
            b'B' => MessageType::Bind,
            b'E' => MessageType::Execute,
            b'S' => MessageType::Sync,
            b'X' => MessageType::Terminate,
            b'C' => MessageType::Close,
            b'D' => MessageType::Describe,
            b'H' => MessageType::Flush,
            _ => return Err(NetworkError::InvalidMessage(format!("Unknown message type: {}", msg_type_byte as char))),
        };
        
        let message = Message::decode(msg_type, &mut msg_buf)?;
        Ok(Some(message))
    }

    async fn send_message(&mut self, message: Message) -> Result<()> {
        eprintln!("DEBUG: Sending message type: {:?}", std::mem::discriminant(&message));
        self.write_buffer.clear();
        message.encode(&mut self.write_buffer)?;
        eprintln!("DEBUG: Encoded message size: {} bytes", self.write_buffer.len());
        self.stream.write_all(&self.write_buffer).await?;
        self.stream.flush().await?;
        eprintln!("DEBUG: Message sent successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    async fn setup_test_server() -> (SocketAddr, Arc<Database>, Arc<TransactionManager>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let database = Arc::new(Database::create(&db_path).unwrap());
        let transaction_manager = Arc::new(TransactionManager::new());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        (addr, database, transaction_manager, temp_dir)
    }

    #[tokio::test]
    async fn test_connection_creation() {
        let (_addr, database, transaction_manager, _temp_dir) = setup_test_server().await;

        // This test just verifies that we can create a connection
        // Actual connection testing would require a full server setup
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _conn = Connection::new(stream, database, transaction_manager);
                // Connection created successfully
            }
        });
    }
}
