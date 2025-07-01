//! Comprehensive CRUD (Create, Read, Update, Delete) operations example.
//!
//! This example demonstrates how to perform all basic database operations
//! with VibeDB, including:
//! - Creating tables
//! - Inserting data
//! - Querying with filters
//! - Updating records with conditions
//! - Deleting records

use anyhow::Result;
use std::path::Path;
use vibedb::{
    access::{DataType, Value},
    database::Database,
    executor::{
        DeleteExecutor, ExecutionContext, Executor, FilterExecutor, InsertExecutor,
        ProjectionExecutor, SeqScanExecutor, UpdateExecutor, UpdateExpression,
    },
    expression::{BinaryOperator, Expression},
};

fn main() -> Result<()> {
    println!("=== VibeDB CRUD Operations Demo ===\n");

    // Initialize database
    let db_path = Path::new("crud_demo.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut database = Database::create(db_path)?;
    println!("Created database: crud_demo.db");

    // Create a users table
    create_users_table(&mut database)?;

    // Demonstrate INSERT operations
    demonstrate_insert(&database)?;

    // Demonstrate SELECT operations with filters
    demonstrate_select(&database)?;

    // Demonstrate UPDATE operations with conditions
    demonstrate_update(&database)?;

    // Demonstrate DELETE operations
    demonstrate_delete(&database)?;

    // Clean up
    std::fs::remove_file(db_path)?;

    println!("\n=== CRUD Operations Demo Complete ===");
    Ok(())
}

fn create_users_table(database: &mut Database) -> Result<()> {
    println!("Creating users table...");

    database.create_table_with_columns(
        "users",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("email", DataType::Varchar),
            ("age", DataType::Int32),
            ("active", DataType::Boolean),
        ],
    )?;

    println!("✓ Table 'users' created\n");
    Ok(())
}

fn demonstrate_insert(database: &Database) -> Result<()> {
    println!("=== INSERT Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    // Insert multiple users
    let users = vec![
        vec![
            Value::Int32(1),
            Value::String("Alice Johnson".to_string()),
            Value::String("alice@example.com".to_string()),
            Value::Int32(28),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(2),
            Value::String("Bob Smith".to_string()),
            Value::String("bob@example.com".to_string()),
            Value::Int32(35),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(3),
            Value::String("Charlie Davis".to_string()),
            Value::String("charlie@example.com".to_string()),
            Value::Int32(42),
            Value::Boolean(false),
        ],
        vec![
            Value::Int32(4),
            Value::String("Diana Wilson".to_string()),
            Value::String("diana@example.com".to_string()),
            Value::Int32(31),
            Value::Boolean(true),
        ],
    ];

    let mut insert_executor = InsertExecutor::new("users".to_string(), users, context);
    insert_executor.init()?;

    if let Some(tuple) = insert_executor.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &[DataType::Int32])?;
        if let Value::Int32(count) = values[0] {
            println!("✓ Inserted {} users", count);
        }
    }

    println!();
    Ok(())
}

fn demonstrate_select(database: &Database) -> Result<()> {
    println!("=== SELECT Operations ===");

    // Query 1: Select all active users
    println!("1. Active users:");
    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    let scan = Box::new(SeqScanExecutor::new("users".to_string(), context.clone()));

    // Filter: active = true
    let active_filter = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(4, "active"),
        Expression::literal(Value::Boolean(true)),
    );

    let mut filter = FilterExecutor::new(scan, active_filter);
    filter.init()?;

    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Boolean,
    ];
    while let Some(tuple) = filter.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({}) - Age: {}",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // Query 2: Select users older than 30
    println!("\n2. Users older than 30:");
    let scan2 = Box::new(SeqScanExecutor::new("users".to_string(), context.clone()));

    let age_filter = Expression::binary_op(
        BinaryOperator::Gt,
        Expression::column_with_name(3, "age"),
        Expression::literal(Value::Int32(30)),
    );

    let mut filter2 = FilterExecutor::new(scan2, age_filter);
    filter2.init()?;

    while let Some(tuple) = filter2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (Age: {})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // Query 3: Project specific columns (name and email only)
    println!("\n3. Name and email projection:");
    let scan3 = Box::new(SeqScanExecutor::new("users".to_string(), context.clone()));

    let projection_indices = vec![1, 2]; // name and email columns
    let mut projection = ProjectionExecutor::new(scan3, projection_indices);
    projection.init()?;

    // Projection output schema is different - only name and email
    let proj_schema = vec![DataType::Varchar, DataType::Varchar];
    while let Some(tuple) = projection.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &proj_schema)?;
        println!(
            "  - {} <{}>",
            match &values[0] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_update(database: &Database) -> Result<()> {
    println!("=== UPDATE Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    // Update 1: Deactivate users older than 40
    println!("1. Deactivating users older than 40...");

    let scan = Box::new(SeqScanExecutor::new("users".to_string(), context.clone()));

    // Filter: age > 40
    let age_filter = Expression::binary_op(
        BinaryOperator::Gt,
        Expression::column_with_name(3, "age"),
        Expression::literal(Value::Int32(40)),
    );

    let filter_exec = Box::new(FilterExecutor::new(scan, age_filter));

    let update_expr = vec![UpdateExpression::new("active", Value::Boolean(false))];

    let mut update = UpdateExecutor::new(
        "users".to_string(),
        update_expr,
        Some(filter_exec),
        context.clone(),
    );
    update.init()?;

    if let Some(tuple) = update.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &[DataType::Int32])?;
        if let Value::Int32(count) = values[0] {
            println!("✓ Updated {} users", count);
        }
    }

    // Update 2: Change email domain for a specific user
    println!("\n2. Updating Bob's email domain...");

    let scan2 = Box::new(SeqScanExecutor::new("users".to_string(), context.clone()));

    // Filter: name = 'Bob Smith'
    let name_filter = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(1, "name"),
        Expression::literal(Value::String("Bob Smith".to_string())),
    );

    let filter_exec2 = Box::new(FilterExecutor::new(scan2, name_filter));

    let update_expr2 = vec![UpdateExpression::new(
        "email",
        Value::String("bob@newdomain.com".to_string()),
    )];

    let mut update2 = UpdateExecutor::new(
        "users".to_string(),
        update_expr2,
        Some(filter_exec2),
        context.clone(),
    );
    update2.init()?;

    if let Some(tuple) = update2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &[DataType::Int32])?;
        if let Value::Int32(count) = values[0] {
            println!("✓ Updated {} user(s)", count);
        }
    }

    // Verify the updates
    println!("\n3. Verifying updates:");
    let context2 = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    let mut scan_all = SeqScanExecutor::new("users".to_string(), context2);
    scan_all.init()?;

    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Boolean,
    ];
    while let Some(tuple) = scan_all.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({}) - Age: {}, Active: {}",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
            match &values[4] {
                Value::Boolean(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_delete(database: &Database) -> Result<()> {
    println!("=== DELETE Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    // Delete inactive users
    println!("1. Deleting inactive users...");

    let scan = Box::new(SeqScanExecutor::new("users".to_string(), context.clone()));

    // Filter: active = false
    let inactive_filter = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(4, "active"),
        Expression::literal(Value::Boolean(false)),
    );

    let filter_exec = Box::new(FilterExecutor::new(scan, inactive_filter));

    let mut delete = DeleteExecutor::new("users".to_string(), Some(filter_exec), context.clone());
    delete.init()?;

    if let Some(tuple) = delete.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &[DataType::Int32])?;
        if let Value::Int32(count) = values[0] {
            println!("✓ Deleted {} inactive users", count);
        }
    }

    // Show remaining users
    println!("\n2. Remaining users:");
    let context2 = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Boolean,
    ];
    let mut scan_remaining = SeqScanExecutor::new("users".to_string(), context2);
    scan_remaining.init()?;

    while let Some(tuple) = scan_remaining.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    println!();
    Ok(())
}
