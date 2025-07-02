// PostgreSQL wire protocol TCP server

use crate::{database::Database, transaction::manager::TransactionManager};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use super::{connection::Connection, Result, DEFAULT_PORT};

pub struct Server {
    database: Arc<Database>,
    transaction_manager: Arc<TransactionManager>,
    max_connections: usize,
}

impl Server {
    pub fn new(
        database: Arc<Database>,
        transaction_manager: Arc<TransactionManager>,
        max_connections: usize,
    ) -> Self {
        Self {
            database,
            transaction_manager,
            max_connections,
        }
    }

    pub async fn run(&self, addr: Option<SocketAddr>) -> Result<()> {
        let addr = addr.unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], DEFAULT_PORT)));
        let listener = TcpListener::bind(addr).await?;

        println!("VibeDB server listening on {}", addr);
        println!("Ready to accept connections");

        // Connection limiter
        let connection_semaphore = Arc::new(Semaphore::new(self.max_connections));

        loop {
            let (stream, peer_addr) = listener.accept().await?;

            // Clone what we need for the spawned task
            let database = self.database.clone();
            let transaction_manager = self.transaction_manager.clone();
            let semaphore = connection_semaphore.clone();

            // Spawn a task to handle this connection
            tokio::spawn(async move {
                // Acquire connection permit
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        eprintln!("Failed to acquire connection permit");
                        return;
                    }
                };

                println!("New connection from {}", peer_addr);

                if let Err(e) = handle_connection(stream, database, transaction_manager).await {
                    eprintln!("Connection error from {}: {}", peer_addr, e);
                }

                println!("Connection closed from {}", peer_addr);
            });
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    database: Arc<Database>,
    transaction_manager: Arc<TransactionManager>,
) -> Result<()> {
    let mut connection = Connection::new(stream, database, transaction_manager);
    connection.run().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup_test_server() -> (Server, SocketAddr, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let database = Arc::new(Database::create(&db_path).unwrap());
        let transaction_manager = Arc::new(TransactionManager::new());

        let server = Server::new(database, transaction_manager, 10);
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));

        (server, addr, temp_dir)
    }

    #[tokio::test]
    async fn test_server_startup() {
        let (server, addr, _temp_dir) = setup_test_server().await;

        // Start server in background
        let server_task = tokio::spawn(async move { server.run(Some(addr)).await });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Server should be running
        assert!(!server_task.is_finished());

        // Clean shutdown
        server_task.abort();
    }

    #[tokio::test]
    async fn test_connection_limit() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let database = Arc::new(Database::create(&db_path).unwrap());
        let transaction_manager = Arc::new(TransactionManager::new());

        // Create server with only 2 max connections
        let server = Server::new(database, transaction_manager, 2);

        // This test just verifies the server can be created with connection limits
        assert_eq!(server.max_connections, 2);
    }
}
