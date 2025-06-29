use anyhow::Result;
use vibedb::access::{DataType, Value};
use vibedb::database::Database;

fn main() -> Result<()> {
    // Create a new database
    let db_path = std::path::Path::new("schema_example.db");

    // Remove existing database if it exists
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    // Create database
    let mut db = Database::create(db_path)?;
    println!("Created database at {:?}", db_path);

    // Create table with schema
    db.create_table_with_columns(
        "users",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("email", DataType::Varchar),
            ("age", DataType::Int32),
            ("active", DataType::Boolean),
        ],
    )?;
    println!("Created users table with schema");

    // List all tables
    let tables = db.list_tables()?;
    println!("Tables in database: {:?}", tables);

    // Insert typed data
    let mut users_table = db.open_table("users")?;

    // Insert users with proper types
    let tid1 = users_table.insert_values(&[
        Value::Int32(1),
        Value::String("Alice".to_string()),
        Value::String("alice@example.com".to_string()),
        Value::Int32(25),
        Value::Boolean(true),
    ])?;

    let tid2 = users_table.insert_values(&[
        Value::Int32(2),
        Value::String("Bob".to_string()),
        Value::String("bob@example.com".to_string()),
        Value::Int32(30),
        Value::Boolean(false),
    ])?;

    let tid3 = users_table.insert_values(&[
        Value::Int32(3),
        Value::String("Charlie".to_string()),
        Value::String("charlie@example.com".to_string()),
        Value::Int32(35),
        Value::Boolean(true),
    ])?;

    println!("Inserted 3 users with typed data");

    // Read data back
    let tuple1 = users_table.get(tid1)?.expect("User 1 should exist");
    let values1 = vibedb::access::value::deserialize_values(&tuple1.data)?;

    println!("\nUser 1:");
    println!("  ID: {:?}", values1[0]);
    println!("  Name: {:?}", values1[1]);
    println!("  Email: {:?}", values1[2]);
    println!("  Age: {:?}", values1[3]);
    println!("  Active: {:?}", values1[4]);

    // Insert with NULL value
    let tid4 = users_table.insert_values(&[
        Value::Int32(4),
        Value::String("David".to_string()),
        Value::Null, // No email
        Value::Int32(28),
        Value::Boolean(true),
    ])?;

    let tuple4 = users_table.get(tid4)?.expect("User 4 should exist");
    let values4 = vibedb::access::value::deserialize_values(&tuple4.data)?;

    println!("\nUser 4 (with NULL email):");
    println!("  ID: {:?}", values4[0]);
    println!("  Name: {:?}", values4[1]);
    println!("  Email: {:?}", values4[2]);
    println!("  Age: {:?}", values4[3]);
    println!("  Active: {:?}", values4[4]);

    // Clean up
    drop(users_table);
    db.flush()?;
    println!("\nDatabase saved to disk");

    Ok(())
}
