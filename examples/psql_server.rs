//! Example PostgreSQL wire protocol server
//!
//! This example demonstrates how to start a VibeDB server that listens
//! on the PostgreSQL wire protocol and can be connected to using psql.
//!
//! Usage:
//!   cargo run --example psql_server
//!
//! Then connect with psql:
//!   psql -h localhost -p 5432 -U postgres testdb

use std::sync::Arc;
use vibedb::{
    access::DataType, database::Database, network::Server, transaction::manager::TransactionManager,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting VibeDB PostgreSQL wire protocol server...");

    // Create a test database
    let db_path = std::path::Path::new("vibedb_example.db");

    // Remove existing database file for a fresh start
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    // Create database and add some tables
    let database = {
        let mut db = Database::create(db_path)?;

        // Create a users table
        db.create_table_with_columns(
            "users",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("active", DataType::Boolean),
            ],
        )?;

        // Create a products table
        db.create_table_with_columns(
            "products",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("price", DataType::Int32), // Store price in cents
            ],
        )?;

        Arc::new(db)
    };

    let transaction_manager = Arc::new(TransactionManager::new());

    // Create and run the server
    let server = Server::new(database, transaction_manager, 100);

    println!("VibeDB server listening on localhost:5432");
    println!("Connect with: psql -h localhost -p 5432 -U postgres testdb");
    println!();
    println!("Available tables: users, products");
    println!("Example queries:");
    println!("  INSERT INTO users VALUES (1, 'Alice', true);");
    println!("  INSERT INTO products VALUES (1, 'Laptop', 99900);");
    println!("  SELECT * FROM users;");
    println!("  SELECT * FROM products;");
    println!();
    println!(
        "Note: CREATE TABLE is not supported via psql. Tables must be created programmatically."
    );
    println!("Press Ctrl+C to stop the server.");

    // Run the server
    server.run(None).await?;

    Ok(())
}
