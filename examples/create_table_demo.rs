//! Example demonstrating CREATE TABLE SQL support in vibedb

use vibedb::database::Database;
use vibedb::session::{Session, QueryResult};
use vibedb::access::Value;
use std::sync::Arc;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    // Create database
    let db_path = Path::new("create_table_demo.db");
    let db = Arc::new(Database::create(db_path)?);
    
    // Create a session
    let session = Session::new(db.clone());
    
    println!("=== CREATE TABLE SQL Demo ===");
    
    // Test 1: Create a simple table
    println!("\n1. Creating employees table:");
    match session.execute_sql("CREATE TABLE employees (id INT, name VARCHAR(50), salary INT)") {
        Ok(QueryResult::CreateTable { table_name }) => {
            println!("   Success: Created table '{}'", table_name);
        }
        Ok(_) => println!("   Unexpected result type"),
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test 2: Insert data into the created table
    println!("\n2. Inserting data into employees:");
    let insert_queries = vec![
        "INSERT INTO employees VALUES (1, 'Alice', 50000)",
        "INSERT INTO employees VALUES (2, 'Bob', 60000)",
        "INSERT INTO employees VALUES (3, 'Charlie', 55000)",
    ];
    
    for query in insert_queries {
        match session.execute_sql(query) {
            Ok(QueryResult::Insert { table_name, row_count }) => {
                println!("   Inserted {} row(s) into {}", row_count, table_name);
            }
            Ok(_) => println!("   Unexpected result type"),
            Err(e) => println!("   Error: {}", e),
        }
    }
    
    // Test 3: Query the created table
    println!("\n3. Querying employees table:");
    match session.execute_sql("SELECT * FROM employees") {
        Ok(QueryResult::Select { schema, rows }) => {
            println!("   Columns: {:?}", schema.iter().map(|c| &c.name).collect::<Vec<_>>());
            println!("   Data:");
            for row in rows {
                let values = vibedb::access::deserialize_values(
                    &row.data,
                    &schema.iter().map(|c| c.data_type).collect::<Vec<_>>(),
                )?;
                println!("   {:?}", values);
            }
        }
        Ok(_) => println!("   Unexpected result type"),
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test 4: Create another table
    println!("\n4. Creating departments table:");
    match session.execute_sql("CREATE TABLE departments (dept_id INT, dept_name VARCHAR(100))") {
        Ok(QueryResult::CreateTable { table_name }) => {
            println!("   Success: Created table '{}'", table_name);
        }
        Ok(_) => println!("   Unexpected result type"),
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test 5: List all tables
    println!("\n5. Listing all tables:");
    let tables = db.catalog.list_tables()?;
    for table in tables {
        println!("   - {}", table.table_name);
    }
    
    // Test 6: Try to create duplicate table
    println!("\n6. Attempting to create duplicate table:");
    match session.execute_sql("CREATE TABLE employees (id INT)") {
        Ok(_) => println!("   ERROR: Should have failed!"),
        Err(e) => println!("   Expected error: {}", e),
    }
    
    // Cleanup
    std::fs::remove_file(db_path).ok();
    
    println!("\nCREATE TABLE SQL support is now fully functional!");
    
    Ok(())
}