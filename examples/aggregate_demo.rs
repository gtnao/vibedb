//! Example demonstrating aggregate functions in vibedb

use vibedb::database::Database;
use vibedb::session::{Session, QueryResult};
use vibedb::access::{DataType, Value};
use std::sync::Arc;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    // Create database
    let db_path = Path::new("aggregate_demo.db");
    let db = Arc::new(Database::create(db_path)?);
    
    // Create a test table
    let table_name = "sales";
    db.create_table_with_columns(
        table_name,
        vec![
            ("department", DataType::Varchar),
            ("employee", DataType::Varchar),
            ("amount", DataType::Int32),
        ],
    )?;
    
    // Insert test data
    let test_data = vec![
        (Value::String("Sales".to_string()), Value::String("Alice".to_string()), Value::Int32(1000)),
        (Value::String("Sales".to_string()), Value::String("Bob".to_string()), Value::Int32(1500)),
        (Value::String("IT".to_string()), Value::String("Charlie".to_string()), Value::Int32(2000)),
        (Value::String("IT".to_string()), Value::String("David".to_string()), Value::Int32(2500)),
        (Value::String("Sales".to_string()), Value::String("Eve".to_string()), Value::Int32(1200)),
        (Value::String("HR".to_string()), Value::String("Frank".to_string()), Value::Int32(1800)),
    ];
    
    for (dept, emp, amt) in test_data {
        db.insert_into(table_name, vec![dept, emp, amt])?;
    }
    
    // Create a session
    let session = Session::new(db.clone());
    
    println!("=== Aggregate Functions Demo ===");
    println!("\nTable contents:");
    
    // Show all data
    match session.execute_sql("SELECT * FROM sales")? {
        QueryResult::Select { schema, rows } => {
            println!("Columns: {:?}", schema.iter().map(|c| &c.name).collect::<Vec<_>>());
            for row in rows {
                let values = vibedb::access::deserialize_values(
                    &row.data,
                    &schema.iter().map(|c| c.data_type).collect::<Vec<_>>(),
                )?;
                println!("{:?}", values);
            }
        }
        _ => println!("Unexpected result"),
    }
    
    // Test 1: COUNT(*)
    println!("\n1. COUNT(*) - Total number of records:");
    match session.execute_sql("SELECT COUNT(*) FROM sales") {
        Ok(QueryResult::Select { schema, rows }) => {
            if let Some(row) = rows.first() {
                let values = vibedb::access::deserialize_values(
                    &row.data,
                    &schema.iter().map(|c| c.data_type).collect::<Vec<_>>(),
                )?;
                println!("   Total count: {:?}", values[0]);
            }
        }
        Ok(_) => println!("   Unexpected result type"),
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test 2: SUM with GROUP BY
    println!("\n2. SUM with GROUP BY department:");
    match session.execute_sql("SELECT department, SUM(amount) FROM sales GROUP BY department") {
        Ok(QueryResult::Select { schema, rows }) => {
            println!("   Department | Total");
            println!("   -----------|------");
            for row in rows {
                let values = vibedb::access::deserialize_values(
                    &row.data,
                    &schema.iter().map(|c| c.data_type).collect::<Vec<_>>(),
                )?;
                if let (Value::String(dept), Value::Int32(total)) = (&values[0], &values[1]) {
                    println!("   {:<10} | {}", dept, total);
                }
            }
        }
        Ok(_) => println!("   Unexpected result type"),
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test 3: Multiple aggregates
    println!("\n3. Multiple aggregates:");
    match session.execute_sql("SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales") {
        Ok(QueryResult::Select { schema, rows }) => {
            if let Some(row) = rows.first() {
                let values = vibedb::access::deserialize_values(
                    &row.data,
                    &schema.iter().map(|c| c.data_type).collect::<Vec<_>>(),
                )?;
                println!("   COUNT: {:?}", values[0]);
                println!("   SUM:   {:?}", values[1]);
                println!("   AVG:   {:?}", values[2]);
                println!("   MIN:   {:?}", values[3]);
                println!("   MAX:   {:?}", values[4]);
            }
        }
        Ok(_) => println!("   Unexpected result type"),
        Err(e) => println!("   Error: {}", e),
    }
    
    // Test 4: GROUP BY with multiple aggregates
    println!("\n4. GROUP BY with COUNT and AVG:");
    match session.execute_sql("SELECT department, COUNT(*), AVG(amount) FROM sales GROUP BY department") {
        Ok(QueryResult::Select { schema, rows }) => {
            println!("   Department | Count | Average");
            println!("   -----------|-------|--------");
            for row in rows {
                let values = vibedb::access::deserialize_values(
                    &row.data,
                    &schema.iter().map(|c| c.data_type).collect::<Vec<_>>(),
                )?;
                if let (Value::String(dept), Value::Int32(count), Value::Int32(avg)) = 
                    (&values[0], &values[1], &values[2]) {
                    println!("   {:<10} | {:<5} | {}", dept, count, avg);
                }
            }
        }
        Ok(_) => println!("   Unexpected result type"),
        Err(e) => println!("   Error: {}", e),
    }
    
    // Cleanup
    std::fs::remove_file(db_path).ok();
    
    println!("\nNote: Aggregate functions are working but need proper SQL parsing support.");
    println!("      The queries shown above would work once SQL parsing for aggregates is implemented.");
    
    Ok(())
}