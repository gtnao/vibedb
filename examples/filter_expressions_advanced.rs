//! Advanced example demonstrating filter expressions with complex predicates

use anyhow::Result;
use vibedb::access::{DataType, Value};
use vibedb::database::Database;
use vibedb::executor::{
    ExecutionContext, Executor, FilterBuilder, FilterExecutor, SeqScanExecutor,
};
use vibedb::expression::Expression;

fn main() -> Result<()> {
    // Create a temporary database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("filter_advanced.db");
    let mut db = Database::create(&db_path)?;

    // Create a products table
    db.create_table_with_columns(
        "products",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("category", DataType::Varchar),
            ("price", DataType::Int32),
            ("stock", DataType::Int32),
            ("discontinued", DataType::Boolean),
        ],
    )?;

    // Get table schema
    let table_info = db.catalog.get_table("products")?.unwrap();
    let columns = db.catalog.get_table_columns(table_info.table_id)?;
    let schema: Vec<DataType> = columns.iter().map(|c| c.column_type).collect();

    // Insert sample products
    let mut table = db.open_table("products")?;

    let products = vec![
        (1, "Laptop Pro", "Electronics", 1200, 15, false),
        (2, "Desk Chair", "Furniture", 350, 50, false),
        (3, "USB Cable", "Electronics", 15, 200, false),
        (4, "Standing Desk", "Furniture", 600, 0, false),
        (5, "Monitor 4K", "Electronics", 450, 25, false),
        (6, "Bookshelf", "Furniture", 150, 10, true),
        (7, "Keyboard", "Electronics", 80, 0, false),
        (8, "Office Lamp", "Furniture", 45, 30, false),
        (9, "Mouse Pad", "Electronics", 12, 100, false),
        (10, "Filing Cabinet", "Furniture", 200, 5, true),
    ];

    for (id, name, category, price, stock, discontinued) in products {
        table.insert_values(
            &[
                Value::Int32(id),
                Value::String(name.to_string()),
                Value::String(category.to_string()),
                Value::Int32(price),
                Value::Int32(stock),
                Value::Boolean(discontinued),
            ],
            &schema,
        )?;
    }

    drop(table);

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Example 1: Products with low stock (stock < 10 AND NOT discontinued)
    println!("Example 1: Active products with low stock (need reordering)");
    println!("----------------------------------------------------------");

    let low_stock_filter = FilterBuilder::and(
        FilterBuilder::column_lt_int32(4, 10),             // stock < 10
        FilterBuilder::not_expr(FilterBuilder::column(5)), // NOT discontinued
    );

    run_filter_query(&context, low_stock_filter)?;

    // Example 2: Price range query with category filter
    // (price BETWEEN 100 AND 500) AND category = 'Electronics'
    println!("\nExample 2: Mid-range electronics (price between $100-$500)");
    println!("----------------------------------------------------------");

    let price_range_filter = FilterBuilder::and(
        FilterBuilder::and(
            FilterBuilder::column_ge_int32(3, 100), // price >= 100
            FilterBuilder::column_le_int32(3, 500), // price <= 500
        ),
        FilterBuilder::column_equals_string(2, "Electronics"),
    );

    run_filter_query(&context, price_range_filter)?;

    // Example 3: Complex OR condition
    // (category = 'Furniture' AND price < 200) OR (stock = 0)
    println!("\nExample 3: Cheap furniture OR out-of-stock items");
    println!("------------------------------------------------");

    let complex_or = FilterBuilder::or(
        FilterBuilder::and(
            FilterBuilder::column_equals_string(2, "Furniture"),
            FilterBuilder::column_lt_int32(3, 200),
        ),
        FilterBuilder::column_equals_int32(4, 0), // stock = 0
    );

    run_filter_query(&context, complex_or)?;

    // Example 4: Multiple conditions with arithmetic
    // price > 100 AND (stock * price) > 5000 (high inventory value)
    println!("\nExample 4: High-value inventory items");
    println!("-------------------------------------");

    // For now, we'll approximate this with: price > 100 AND stock > 50
    // (since we don't have arithmetic expressions in filters yet)
    let high_value_filter = FilterBuilder::and(
        FilterBuilder::column_gt_int32(3, 100),
        FilterBuilder::column_gt_int32(4, 20),
    );

    run_filter_query(&context, high_value_filter)?;

    // Example 5: Negation and complex boolean logic
    // NOT (discontinued = true OR (category = 'Electronics' AND price > 1000))
    println!("\nExample 5: Available non-premium items");
    println!("--------------------------------------");

    let premium_electronics = FilterBuilder::and(
        FilterBuilder::column_equals_string(2, "Electronics"),
        FilterBuilder::column_gt_int32(3, 1000),
    );

    let excluded_items = FilterBuilder::or(
        FilterBuilder::column(5), // discontinued = true
        premium_electronics,
    );

    let available_non_premium = FilterBuilder::not_expr(excluded_items);

    run_filter_query(&context, available_non_premium)?;

    // Example 6: Building expressions programmatically
    println!("\nExample 6: Dynamic filter building");
    println!("----------------------------------");

    // Simulate building a filter from user input
    let mut conditions = Vec::new();

    // User wants electronics
    conditions.push(FilterBuilder::column_equals_string(2, "Electronics"));

    // User sets max price
    conditions.push(FilterBuilder::column_le_int32(3, 500));

    // User wants in-stock items only
    conditions.push(FilterBuilder::column_gt_int32(4, 0));

    // Combine all conditions with AND
    let dynamic_filter = conditions
        .into_iter()
        .reduce(|acc, cond| FilterBuilder::and(acc, cond))
        .unwrap();

    run_filter_query(&context, dynamic_filter)?;

    Ok(())
}

/// Helper function to run a filter query and print results
fn run_filter_query(context: &ExecutionContext, filter: Expression) -> Result<()> {
    let seq_scan = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));
    let mut filter_executor = FilterExecutor::new(seq_scan, filter);
    filter_executor.init()?;

    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Boolean,
    ];

    let mut count = 0;
    while let Some(tuple) = filter_executor.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;

        if let (
            Value::Int32(id),
            Value::String(name),
            Value::String(category),
            Value::Int32(price),
            Value::Int32(stock),
            Value::Boolean(discontinued),
        ) = (
            &values[0], &values[1], &values[2], &values[3], &values[4], &values[5],
        ) {
            println!(
                "ID: {}, Name: {}, Category: {}, Price: ${}, Stock: {}, Discontinued: {}",
                id, name, category, price, stock, discontinued
            );
            count += 1;
        }
    }

    println!("Total results: {}", count);
    Ok(())
}
