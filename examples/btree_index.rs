//! Example demonstrating B+Tree index creation and usage

use anyhow::Result;
use tempfile::tempdir;
use vibedb::access::btree::BTree;
use vibedb::access::{DataType, TableHeap, Value};
use vibedb::catalog::Catalog;
use vibedb::storage::buffer::BufferPoolManager;
use vibedb::storage::disk::PageManager;

fn main() -> Result<()> {
    // Create a temporary directory for the database
    let dir = tempdir()?;
    let file_path = dir.path().join("index_example.db");

    // Initialize the database
    let page_manager = PageManager::create(&file_path)?;
    let replacer = Box::new(vibedb::storage::buffer::lru::LruReplacer::new(100));
    let buffer_pool = BufferPoolManager::new(page_manager, replacer, 100);

    // Initialize catalog
    let mut catalog = Catalog::initialize(buffer_pool.clone())?;

    // Create a users table with columns
    let table_info = catalog.create_table_with_columns(
        "users",
        vec![
            ("id", DataType::Int32),
            ("email", DataType::Varchar),
            ("age", DataType::Int32),
        ],
    )?;

    println!("Created table: {}", table_info.table_name);

    // Create an index on the email column (this also creates the B+Tree)
    let index_info = catalog.create_index(
        "idx_users_email",
        "users",
        &["email"],
        true, // unique index
    )?;

    println!(
        "Created index: {} on column(s): email",
        index_info.index_name
    );
    println!(
        "B+Tree created with root page: {:?}",
        index_info.root_page_id
    );

    // Open the B+Tree for the index
    let mut btree = BTree::open(
        buffer_pool.clone(),
        index_info.root_page_id,
        index_info.key_columns.clone(),
    )?;

    // Get table heap for inserting data
    let mut table_heap = TableHeap::with_first_page(
        buffer_pool.clone(),
        table_info.table_id,
        table_info.first_page_id,
    );

    // Insert some sample data
    let users = vec![
        (1, "alice@example.com", 25),
        (2, "bob@example.com", 30),
        (3, "charlie@example.com", 35),
        (4, "david@example.com", 28),
        (5, "eve@example.com", 32),
    ];

    // Define schema for serialization
    let schema = vec![DataType::Int32, DataType::Varchar, DataType::Int32];

    println!("\nInserting users...");
    for (id, email, age) in &users {
        // Serialize the tuple
        let values = vec![
            Value::Int32(*id),
            Value::String(email.to_string()),
            Value::Int32(*age),
        ];
        let serialized = vibedb::access::value::serialize_values(&values, &schema)?;

        // Insert into table
        let tuple_id = table_heap.insert(&serialized)?;
        println!("Inserted user {} with tuple_id: {:?}", id, tuple_id);

        // Insert into index (email -> tuple_id)
        let index_key = vec![Value::String(email.to_string())];
        btree.insert(&index_key, tuple_id)?;
    }

    // Demonstrate index lookup
    println!("\n--- Index Lookups ---");

    // Look up user by email
    let search_email = "charlie@example.com";
    let search_key = vec![Value::String(search_email.to_string())];

    match btree.search(&search_key)? {
        results if !results.is_empty() => {
            println!(
                "Found {} result(s) for email '{}':",
                results.len(),
                search_email
            );
            for tuple_id in results {
                // Fetch the actual tuple from the table
                let page_guard = buffer_pool.fetch_page(tuple_id.page_id)?;
                let heap_page = vibedb::storage::page::utils::heap_page_from_guard(&page_guard);
                if let Ok(tuple_data) = heap_page.get_tuple(tuple_id.slot_id) {
                    let values = vibedb::access::value::deserialize_values(&tuple_data, &schema)?;
                    println!(
                        "  User: id={:?}, email={:?}, age={:?}",
                        values[0], values[1], values[2]
                    );
                }
            }
        }
        _ => println!("No user found with email '{}'", search_email),
    }

    // Range scan example
    println!("\n--- Range Scan ---");
    let start_key = vec![Value::String("b".to_string())];
    let end_key = vec![Value::String("d".to_string())];

    let range_results = btree.range_scan(
        Some(&start_key),
        Some(&end_key),
        true,  // include start
        false, // exclude end
    )?;

    println!("Users with emails starting with 'b' or 'c':");
    for (key_values, tuple_id) in range_results {
        if let Value::String(email) = &key_values[0] {
            println!("  Email: {} -> TupleId: {:?}", email, tuple_id);
        }
    }

    // Demonstrate B+Tree statistics
    let stats = btree.get_statistics();
    println!("\n--- B+Tree Statistics ---");
    println!("Height: {}", stats.height);
    println!("Root page: {:?}", stats.root_page_id);
    println!("Key columns: {} column(s)", stats.key_columns.len());

    // Verify unique constraint would work
    println!("\n--- Unique Constraint Test ---");
    let duplicate_email = vec![Value::String("alice@example.com".to_string())];
    match btree.search(&duplicate_email)? {
        results if !results.is_empty() => {
            println!(
                "Email 'alice@example.com' already exists (found {} occurrence(s))",
                results.len()
            );
            println!("A unique index would prevent duplicate inserts");
        }
        _ => println!("Email not found"),
    }

    // List all indexes for the users table
    let indexes = catalog.get_table_indexes(table_info.table_id)?;
    println!("\n--- Indexes on 'users' table ---");
    for idx in indexes {
        println!("Index: {} (unique: {})", idx.index_name, idx.is_unique);
        println!(
            "  Columns: {}",
            idx.key_columns
                .iter()
                .map(|c| c.column_name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!("  Root page: {:?}", idx.root_page_id);
    }

    Ok(())
}
