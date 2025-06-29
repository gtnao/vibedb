//! Example demonstrating system table queries
//!
//! This example shows how to query vibedb's system tables
//! to inspect database metadata.

use anyhow::Result;
use std::path::Path;
use vibedb::access::{DataType, Value};
use vibedb::database::Database;
use vibedb::executor::{ExecutionContext, Executor, SeqScanExecutor};

fn main() -> Result<()> {
    println!("=== vibedb System Tables Demo ===\n");

    // Create a new database
    let db_path = Path::new("metadata_demo.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut db = Database::create(db_path)?;

    // Create some tables with different schemas
    db.create_table_with_columns(
        "users",
        vec![
            ("user_id", DataType::Int32),
            ("username", DataType::Varchar),
            ("email", DataType::Varchar),
            ("created_at", DataType::Int32),
        ],
    )?;

    db.create_table_with_columns(
        "products",
        vec![
            ("product_id", DataType::Int32),
            ("name", DataType::Varchar),
            ("price", DataType::Int32),
            ("in_stock", DataType::Boolean),
        ],
    )?;

    db.create_table_with_columns(
        "orders",
        vec![
            ("order_id", DataType::Int32),
            ("user_id", DataType::Int32),
            ("product_id", DataType::Int32),
            ("quantity", DataType::Int32),
            ("total", DataType::Int32),
        ],
    )?;

    println!("Created 3 tables: users, products, orders\n");

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Query pg_tables to list all tables
    println!("=== All Tables (pg_tables) ===");
    let mut tables_scan = SeqScanExecutor::new("pg_tables".to_string(), context.clone());
    tables_scan.init()?;

    let table_schema = vec![DataType::Int32, DataType::Varchar, DataType::Int32];
    let mut table_map = std::collections::HashMap::new();

    while let Some(tuple) = tables_scan.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &table_schema)?;

        let table_id = match &values[0] {
            Value::Int32(v) => *v,
            _ => continue,
        };

        let table_name = match &values[1] {
            Value::String(v) => v.clone(),
            _ => continue,
        };

        let first_page = match &values[2] {
            Value::Int32(v) => *v,
            _ => continue,
        };

        table_map.insert(table_id, table_name.clone());

        println!(
            "Table: {:20} ID: {:3} First Page: {}",
            table_name, table_id, first_page
        );
    }

    // Query pg_attribute to show all columns
    println!("\n=== All Columns (pg_attribute) ===");
    let mut attr_scan = SeqScanExecutor::new("pg_attribute".to_string(), context.clone());
    attr_scan.init()?;

    let attr_schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
    ];

    let mut current_table_id = -1;
    while let Some(tuple) = attr_scan.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &attr_schema)?;

        let table_id = match &values[0] {
            Value::Int32(v) => *v,
            _ => continue,
        };

        let column_name = match &values[1] {
            Value::String(v) => v.clone(),
            _ => continue,
        };

        let column_type = match &values[2] {
            Value::Int32(v) => match *v {
                1 => "Boolean",
                2 => "Int32",
                4 => "Varchar",
                _ => "Unknown",
            },
            _ => continue,
        };

        let column_order = match &values[3] {
            Value::Int32(v) => *v,
            _ => continue,
        };

        // Print table header when we encounter a new table
        if table_id != current_table_id {
            current_table_id = table_id;
            if let Some(table_name) = table_map.get(&table_id) {
                println!("\nTable: {}", table_name);
                println!("  Columns:");
            }
        }

        println!("    {:2}. {:20} {}", column_order, column_name, column_type);
    }

    // Show statistics
    println!("\n=== Database Statistics ===");
    println!("Total tables: {}", table_map.len());

    let user_tables: Vec<_> = table_map
        .values()
        .filter(|name| !name.starts_with("pg_"))
        .collect();

    let system_tables: Vec<_> = table_map
        .values()
        .filter(|name| name.starts_with("pg_"))
        .collect();

    println!("User tables: {} {:?}", user_tables.len(), user_tables);
    println!("System tables: {} {:?}", system_tables.len(), system_tables);

    // Clean up
    drop(db);
    std::fs::remove_file(db_path)?;
    println!("\n✓ Demo completed successfully!");

    Ok(())
}
