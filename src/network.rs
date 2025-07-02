// PostgreSQL wire protocol network module

pub mod connection;
pub mod message;
pub mod protocol;
pub mod server;

// Re-export commonly used types
pub use connection::Connection;
pub use message::{Message, MessageType};
pub use protocol::{ProtocolError, ProtocolHandler};
pub use server::Server;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),
}

pub type Result<T> = std::result::Result<T, NetworkError>;

// PostgreSQL protocol version
pub const PROTOCOL_VERSION_3: i32 = 196608; // 3.0

// SSL request code
pub const SSL_REQUEST_CODE: i32 = 80877103;

// Default PostgreSQL port
pub const DEFAULT_PORT: u16 = 5432;
