//! Example demonstrating basic executor usage
//!
//! This example shows how to:
//! 1. Create a database with tables
//! 2. Insert data using InsertExecutor
//! 3. Scan data using SeqScanExecutor
//! 4. Query system tables

use anyhow::Result;
use std::path::Path;
use vibedb::access::{DataType, Value};
use vibedb::database::Database;
use vibedb::executor::{
    ExecutionContext, Executor, InsertExecutor, SeqScanExecutor, SystemSeqScanExecutor,
};

fn main() -> Result<()> {
    println!("=== vibedb Executor Demo ===\n");

    // Create a new database
    let db_path = Path::new("demo.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut db = Database::create(db_path)?;
    println!("Created database: demo.db");

    // Create a table
    db.create_table_with_columns(
        "employees",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("department", DataType::Varchar),
            ("salary", DataType::Int32),
            ("active", DataType::Boolean),
        ],
    )?;
    println!("Created table: employees");

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Insert data using InsertExecutor
    println!("\n--- Inserting Data ---");
    let values = vec![
        vec![
            Value::Int32(1),
            Value::String("Alice Johnson".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(75000),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(2),
            Value::String("Bob Smith".to_string()),
            Value::String("Sales".to_string()),
            Value::Int32(65000),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(3),
            Value::String("Charlie Brown".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(80000),
            Value::Boolean(false),
        ],
        vec![
            Value::Int32(4),
            Value::String("Diana Prince".to_string()),
            Value::String("HR".to_string()),
            Value::Int32(70000),
            Value::Boolean(true),
        ],
    ];

    let mut insert_executor = InsertExecutor::new("employees".to_string(), values, context.clone());
    insert_executor.init()?;

    if let Some(result) = insert_executor.next()? {
        let count_values = vibedb::access::deserialize_values(&result.data, &[DataType::Int32])?;
        if let Value::Int32(count) = count_values[0] {
            println!("Inserted {} rows", count);
        }
    }

    // Scan data using SeqScanExecutor
    println!("\n--- Scanning Employee Table ---");
    let mut scan_executor = SeqScanExecutor::new("employees".to_string(), context.clone());
    scan_executor.init()?;

    println!("Schema: {:?}", scan_executor.output_schema());
    println!("\nData:");

    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Boolean,
    ];
    while let Some(tuple) = scan_executor.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "ID: {}, Name: {}, Dept: {}, Salary: ${}, Active: {}",
            match &values[0] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
            match &values[1] {
                Value::String(v) => v.clone(),
                _ => "?".to_string(),
            },
            match &values[2] {
                Value::String(v) => v.clone(),
                _ => "?".to_string(),
            },
            match &values[3] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
            match &values[4] {
                Value::Boolean(v) => v.to_string(),
                _ => "?".to_string(),
            },
        );
    }

    // Query system tables
    println!("\n--- System Tables ---");

    // List all tables
    println!("\npg_tables:");
    let mut system_scan = SystemSeqScanExecutor::new("pg_tables".to_string(), context.clone());
    system_scan.init()?;

    let system_schema = vec![DataType::Int32, DataType::Varchar, DataType::Int32];
    while let Some(tuple) = system_scan.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &system_schema)?;
        println!(
            "  Table ID: {}, Name: {}, First Page: {}",
            match &values[0] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
            match &values[1] {
                Value::String(v) => v.clone(),
                _ => "?".to_string(),
            },
            match &values[2] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
        );
    }

    // List columns for employees table
    println!("\npg_attribute (employees table):");
    let mut attr_scan = SystemSeqScanExecutor::new("pg_attribute".to_string(), context.clone());
    attr_scan.init()?;

    // Get employees table ID
    let employees_table = db.catalog.get_table("employees")?.unwrap();

    let attr_schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
    ];
    while let Some(tuple) = attr_scan.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &attr_schema)?;
        if let Value::Int32(table_id) = &values[0] {
            if *table_id == employees_table.table_id as i32 {
                println!(
                    "  Column: {}, Type: {}, Order: {}",
                    match &values[1] {
                        Value::String(v) => v.clone(),
                        _ => "?".to_string(),
                    },
                    match &values[2] {
                        Value::Int32(v) => {
                            match *v {
                                1 => "Boolean",
                                2 => "Int32",
                                4 => "Varchar",
                                _ => "Unknown",
                            }
                        }
                        _ => "?",
                    },
                    match &values[3] {
                        Value::Int32(v) => v.to_string(),
                        _ => "?".to_string(),
                    },
                );
            }
        }
    }

    // Clean up
    drop(db);
    std::fs::remove_file(db_path)?;
    println!("\n✓ Demo completed successfully!");

    Ok(())
}
