//! Example demonstrating transaction SQL support in vibedb

use vibedb::database::Database;
use vibedb::session::{Session, QueryResult};
use std::sync::Arc;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    // Create database
    let db_path = Path::new("transaction_demo.db");
    let db = Arc::new(Database::create(db_path)?);
    
    // Create a session
    let mut session = Session::new(db.clone());
    
    println!("=== Transaction SQL Demo ===");
    
    // Test BEGIN TRANSACTION
    println!("\n1. Testing BEGIN TRANSACTION:");
    match session.execute_sql("BEGIN TRANSACTION") {
        Ok(result) => match result {
            QueryResult::Transaction { message } => println!("   Success: {}", message),
            _ => println!("   Unexpected result type"),
        },
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test alternate syntax
    println!("\n2. Testing BEGIN:");
    match session.execute_sql("BEGIN") {
        Ok(result) => match result {
            QueryResult::Transaction { message } => println!("   Success: {}", message),
            _ => println!("   Unexpected result type"),
        },
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test COMMIT
    println!("\n3. Testing COMMIT:");
    match session.execute_sql("COMMIT") {
        Ok(result) => match result {
            QueryResult::Transaction { message } => println!("   Success: {}", message),
            _ => println!("   Unexpected result type"),
        },
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test ROLLBACK
    println!("\n4. Testing ROLLBACK:");
    match session.execute_sql("ROLLBACK") {
        Ok(result) => match result {
            QueryResult::Transaction { message } => println!("   Success: {}", message),
            _ => println!("   Unexpected result type"),
        },
        Err(e) => println!("   Error: {}", e),
    }
    
    // Cleanup
    std::fs::remove_file(db_path).ok();
    
    println!("\nNote: Transaction functionality is now integrated with TransactionManager.");
    
    Ok(())
}