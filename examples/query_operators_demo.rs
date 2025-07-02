//! Query operators demonstration example.
//!
//! This example showcases how different query operators work together
//! to create complex queries:
//! - Sequential scans with filters
//! - Projections for column selection
//! - Sorting with multiple columns
//! - Limiting results
//! - Combining operators in execution pipelines

use anyhow::Result;
use std::path::Path;
use vibedb::{
    access::{DataType, Value},
    database::Database,
    executor::{
        ExecutionContext, Executor, FilterExecutor, InsertExecutor, LimitExecutor,
        ProjectionExecutor, SeqScanExecutor, SortCriteria, SortExecutor, SortOrder,
    },
    expression::{BinaryOperator, Expression},
};

fn main() -> Result<()> {
    println!("=== VibeDB Query Operators Demo ===\n");

    // Initialize database
    let db_path = Path::new("query_demo.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut database = Database::create(db_path)?;
    println!("Created database: query_demo.db");

    // Create and populate a products table
    create_products_table(&mut database)?;
    populate_products_table(&database)?;

    // Demonstrate various query operators
    demonstrate_filter_operations(&database)?;
    demonstrate_projection_operations(&database)?;
    demonstrate_sort_operations(&database)?;
    demonstrate_limit_operations(&database)?;
    demonstrate_complex_pipeline(&database)?;

    // Clean up
    std::fs::remove_file(db_path)?;

    println!("\n=== Query Operators Demo Complete ===");
    Ok(())
}

fn create_products_table(database: &mut Database) -> Result<()> {
    println!("Creating products table...");

    database.create_table_with_columns(
        "products",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("category", DataType::Varchar),
            ("price_cents", DataType::Int32), // Price in cents
            ("stock", DataType::Int32),
            ("rating_x10", DataType::Int32), // Rating x 10 (e.g., 45 = 4.5)
        ],
    )?;

    println!("✓ Table 'products' created\n");
    Ok(())
}

fn populate_products_table(database: &Database) -> Result<()> {
    println!("Populating products table...");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    let products = vec![
        vec![
            Value::Int32(1),
            Value::String("Laptop Pro X1".to_string()),
            Value::String("Electronics".to_string()),
            Value::Int32(129999), // $1299.99
            Value::Int32(15),
            Value::Int32(45), // 4.5 rating
        ],
        vec![
            Value::Int32(2),
            Value::String("Wireless Mouse".to_string()),
            Value::String("Electronics".to_string()),
            Value::Int32(2999), // $29.99
            Value::Int32(150),
            Value::Int32(42), // 4.2 rating
        ],
        vec![
            Value::Int32(3),
            Value::String("Office Chair Deluxe".to_string()),
            Value::String("Furniture".to_string()),
            Value::Int32(39999), // $399.99
            Value::Int32(25),
            Value::Int32(47), // 4.7 rating
        ],
        vec![
            Value::Int32(4),
            Value::String("Standing Desk".to_string()),
            Value::String("Furniture".to_string()),
            Value::Int32(59999), // $599.99
            Value::Int32(10),
            Value::Int32(48), // 4.8 rating
        ],
        vec![
            Value::Int32(5),
            Value::String("USB-C Hub".to_string()),
            Value::String("Electronics".to_string()),
            Value::Int32(4999), // $49.99
            Value::Int32(200),
            Value::Int32(39), // 3.9 rating
        ],
        vec![
            Value::Int32(6),
            Value::String("Desk Lamp LED".to_string()),
            Value::String("Furniture".to_string()),
            Value::Int32(7999), // $79.99
            Value::Int32(50),
            Value::Int32(44), // 4.4 rating
        ],
        vec![
            Value::Int32(7),
            Value::String("Mechanical Keyboard".to_string()),
            Value::String("Electronics".to_string()),
            Value::Int32(14999), // $149.99
            Value::Int32(75),
            Value::Int32(46), // 4.6 rating
        ],
        vec![
            Value::Int32(8),
            Value::String("Monitor Stand".to_string()),
            Value::String("Furniture".to_string()),
            Value::Int32(3999), // $39.99
            Value::Int32(100),
            Value::Int32(41), // 4.1 rating
        ],
    ];

    let mut insert = InsertExecutor::new("products".to_string(), products, context);
    insert.init()?;

    if let Some(tuple) = insert.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &[DataType::Int32])?;
        if let Value::Int32(count) = values[0] {
            println!("✓ Inserted {} products", count);
        }
    }

    println!();
    Ok(())
}

fn demonstrate_filter_operations(database: &Database) -> Result<()> {
    println!("=== Filter Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Int32,
    ];

    // Filter 1: Electronics products
    println!("1. Electronics products:");
    let scan1 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let category_filter = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(2, "category"),
        Expression::literal(Value::String("Electronics".to_string())),
    );

    let mut filter1 = FilterExecutor::new(scan1, category_filter);
    filter1.init()?;

    while let Some(tuple) = filter1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (${})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(cents) => format!("{:.2}", *cents as f64 / 100.0),
                _ => "?".to_string(),
            }
        );
    }

    // Filter 2: Products under $100
    println!("\n2. Products under $100:");
    let scan2 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let price_filter = Expression::binary_op(
        BinaryOperator::Lt,
        Expression::column_with_name(3, "price_cents"),
        Expression::literal(Value::Int32(10000)), // $100.00
    );

    let mut filter2 = FilterExecutor::new(scan2, price_filter);
    filter2.init()?;

    while let Some(tuple) = filter2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (${})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(cents) => format!("{:.2}", *cents as f64 / 100.0),
                _ => "?".to_string(),
            }
        );
    }

    // Filter 3: High-rated products (rating >= 4.5)
    println!("\n3. High-rated products (rating >= 4.5):");
    let scan3 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let rating_filter = Expression::binary_op(
        BinaryOperator::Ge,
        Expression::column_with_name(5, "rating_x10"),
        Expression::literal(Value::Int32(45)), // 4.5 rating
    );

    let mut filter3 = FilterExecutor::new(scan3, rating_filter);
    filter3.init()?;

    while let Some(tuple) = filter3.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (rating: {})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[5] {
                Value::Int32(r) => format!("{:.1}", *r as f64 / 10.0),
                _ => "?".to_string(),
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_projection_operations(database: &Database) -> Result<()> {
    println!("=== Projection Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    // Projection 1: Name and price only
    println!("1. Product names and prices:");
    let scan1 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let projection_indices1 = vec![1, 3]; // name, price
    let mut projection1 = ProjectionExecutor::new(scan1, projection_indices1);
    projection1.init()?;

    let proj_schema1 = vec![DataType::Varchar, DataType::Int32];
    while let Some(tuple) = projection1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &proj_schema1)?;
        println!(
            "  - {} | ${}",
            match &values[0] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[1] {
                Value::Int32(cents) => format!("{:.2}", *cents as f64 / 100.0),
                _ => "?".to_string(),
            }
        );
    }

    // Projection 2: Category summary (category, stock, rating)
    println!("\n2. Category summary:");
    let scan2 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let projection_indices2 = vec![2, 4, 5]; // category, stock, rating
    let mut projection2 = ProjectionExecutor::new(scan2, projection_indices2);
    projection2.init()?;

    let proj_schema2 = vec![DataType::Varchar, DataType::Int32, DataType::Int32];
    while let Some(tuple) = projection2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &proj_schema2)?;
        println!(
            "  - {} | Stock: {} | Rating: {}",
            match &values[0] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[1] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
            match &values[2] {
                Value::Int32(r) => format!("{:.1}", *r as f64 / 10.0),
                _ => "?".to_string(),
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_sort_operations(database: &Database) -> Result<()> {
    println!("=== Sort Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Int32,
    ];

    // Sort 1: By price ascending
    println!("1. Products sorted by price (ascending):");
    let scan1 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let sort_keys1 = vec![SortCriteria::new(3, SortOrder::Asc)]; // price column
    let mut sort1 = SortExecutor::new(scan1, sort_keys1);
    sort1.init()?;

    while let Some(tuple) = sort1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (${})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(cents) => format!("{:.2}", *cents as f64 / 100.0),
                _ => "?".to_string(),
            }
        );
    }

    // Sort 2: By rating descending
    println!("\n2. Products sorted by rating (descending):");
    let scan2 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let sort_keys2 = vec![SortCriteria::new(5, SortOrder::Desc)]; // rating column
    let mut sort2 = SortExecutor::new(scan2, sort_keys2);
    sort2.init()?;

    while let Some(tuple) = sort2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (rating: {})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[5] {
                Value::Int32(r) => format!("{:.1}", *r as f64 / 10.0),
                _ => "?".to_string(),
            }
        );
    }

    // Sort 3: Multi-column sort (category ASC, price DESC)
    println!("\n3. Products sorted by category, then price:");
    let scan3 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let sort_keys3 = vec![
        SortCriteria::new(2, SortOrder::Asc),  // category
        SortCriteria::new(3, SortOrder::Desc), // price
    ];
    let mut sort3 = SortExecutor::new(scan3, sort_keys3);
    sort3.init()?;

    while let Some(tuple) = sort3.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} | {} | ${}",
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(cents) => format!("{:.2}", *cents as f64 / 100.0),
                _ => "?".to_string(),
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_limit_operations(database: &Database) -> Result<()> {
    println!("=== Limit Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Int32,
    ];

    // Limit 1: Top 3 products
    println!("1. First 3 products:");
    let scan1 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let mut limit1 = LimitExecutor::new(scan1, 3);
    limit1.init()?;

    while let Some(tuple) = limit1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {}",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    // Limit 2: Top 5 highest rated products
    println!("\n2. Top 5 highest rated products:");
    let scan2 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    let sort_keys = vec![SortCriteria::new(5, SortOrder::Desc)];
    let sorted = Box::new(SortExecutor::new(scan2, sort_keys));

    let mut limit2 = LimitExecutor::new(sorted, 5);
    limit2.init()?;

    while let Some(tuple) = limit2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (rating: {})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[5] {
                Value::Int32(r) => format!("{:.1}", *r as f64 / 10.0),
                _ => "?".to_string(),
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_complex_pipeline(database: &Database) -> Result<()> {
    println!("=== Complex Query Pipeline ===");
    println!("Query: Top 3 Electronics products by rating, showing name and rating only");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    // Step 1: Sequential scan
    let scan = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    // Step 2: Filter for Electronics
    let category_filter = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(2, "category"),
        Expression::literal(Value::String("Electronics".to_string())),
    );
    let filtered = Box::new(FilterExecutor::new(scan, category_filter));

    // Step 3: Sort by rating descending
    let sort_keys = vec![SortCriteria::new(5, SortOrder::Desc)];
    let sorted = Box::new(SortExecutor::new(filtered, sort_keys));

    // Step 4: Limit to top 3
    let limited = Box::new(LimitExecutor::new(sorted, 3));

    // Step 5: Project name and rating only
    let projection_indices = vec![1, 5]; // name, rating
    let mut projected = ProjectionExecutor::new(limited, projection_indices);

    // Execute the pipeline
    projected.init()?;

    let proj_schema = vec![DataType::Varchar, DataType::Int32];
    println!("\nResults:");
    let mut rank = 1;
    while let Some(tuple) = projected.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &proj_schema)?;
        println!(
            "  {}. {} (rating: {})",
            rank,
            match &values[0] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[1] {
                Value::Int32(r) => format!("{:.1}", *r as f64 / 10.0),
                _ => "?".to_string(),
            }
        );
        rank += 1;
    }

    // Show another complex example
    println!("\n=== Another Complex Query ===");
    println!("Query: Affordable furniture (< $100) with good ratings (>= 4.0), sorted by price");

    let context2 = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    // Build the pipeline
    let scan2 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context2.clone(),
    ));

    // Filter 1: Furniture category
    let furniture_filter = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(2, "category"),
        Expression::literal(Value::String("Furniture".to_string())),
    );
    let furniture_only = Box::new(FilterExecutor::new(scan2, furniture_filter));

    // Filter 2: Price < 100
    let price_filter = Expression::binary_op(
        BinaryOperator::Lt,
        Expression::column_with_name(3, "price_cents"),
        Expression::literal(Value::Int32(10000)), // $100.00
    );
    let affordable = Box::new(FilterExecutor::new(furniture_only, price_filter));

    // Filter 3: Rating >= 4.0
    let rating_filter = Expression::binary_op(
        BinaryOperator::Ge,
        Expression::column_with_name(5, "rating_x10"),
        Expression::literal(Value::Int32(40)), // 4.0 rating
    );
    let good_rating = Box::new(FilterExecutor::new(affordable, rating_filter));

    // Sort by price ascending
    let price_sort = vec![SortCriteria::new(3, SortOrder::Asc)];
    let mut sorted2 = SortExecutor::new(good_rating, price_sort);

    // Execute
    sorted2.init()?;

    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Int32,
    ];
    println!("\nResults:");
    while let Some(tuple) = sorted2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} | ${} | Rating: {}",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(cents) => format!("{:.2}", *cents as f64 / 100.0),
                _ => "?".to_string(),
            },
            match &values[5] {
                Value::Int32(r) => format!("{:.1}", *r as f64 / 10.0),
                _ => "?".to_string(),
            }
        );
    }

    Ok(())
}
