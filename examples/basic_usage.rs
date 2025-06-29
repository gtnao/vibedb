use anyhow::Result;
use vibedb::database::Database;

fn main() -> Result<()> {
    // Create a new database
    let db_path = std::path::Path::new("example.db");

    // Remove existing database if it exists
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    // Create database
    let mut db = Database::create(db_path)?;
    println!("Created database at {:?}", db_path);

    // Create tables
    db.create_table("users")?;
    db.create_table("products")?;
    println!("Created tables: users, products");

    // List all tables
    let tables = db.list_tables()?;
    println!("Tables in database: {:?}", tables);

    // Insert data into users table
    let mut users_table = db.open_table("users")?;

    let user1 = b"Alice,alice@example.com,25";
    let user2 = b"Bob,bob@example.com,30";
    let user3 = b"Charlie,charlie@example.com,35";

    let tid1 = users_table.insert(user1)?;
    let tid2 = users_table.insert(user2)?;
    let tid3 = users_table.insert(user3)?;

    println!("Inserted 3 users");

    // Read data back
    let tuple1 = users_table.get(tid1)?.expect("User 1 should exist");
    let tuple2 = users_table.get(tid2)?.expect("User 2 should exist");
    let tuple3 = users_table.get(tid3)?.expect("User 3 should exist");

    println!("User 1: {}", String::from_utf8_lossy(&tuple1.data));
    println!("User 2: {}", String::from_utf8_lossy(&tuple2.data));
    println!("User 3: {}", String::from_utf8_lossy(&tuple3.data));

    // Delete a user
    users_table.delete(tid2)?;
    println!("Deleted user 2");

    // Verify deletion
    assert!(users_table.get(tid2)?.is_none());
    println!("Confirmed user 2 is deleted");

    // Flush to disk
    drop(users_table);
    db.flush()?;
    println!("Flushed database to disk");

    Ok(())
}
