//! Example demonstrating the use of LimitExecutor for pagination

use anyhow::Result;
use tempfile::tempdir;
use vibedb::access::{deserialize_values, DataType, TableHeap, Value};
use vibedb::database::Database;
use vibedb::executor::{ExecutionContext, Executor, LimitExecutor, SeqScanExecutor};

fn main() -> Result<()> {
    // Create a temporary database
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    let mut db = Database::create(&db_path)?;

    // Create a products table
    db.create_table_with_columns(
        "products",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("price", DataType::Int32), // Using Int32 for price in cents
        ],
    )?;

    // Insert sample data
    let table_info = db.catalog.get_table("products")?.unwrap();
    let mut heap = TableHeap::with_first_page(
        (*db.buffer_pool).clone(),
        table_info.table_id,
        table_info.first_page_id,
    );

    let schema = vec![DataType::Int32, DataType::Varchar, DataType::Int32];

    // Insert 20 products
    for i in 1..=20 {
        heap.insert_values(
            &vec![
                Value::Int32(i),
                Value::String(format!("Product {}", i)),
                Value::Int32(1000 * i), // Price in cents (e.g., 1000 = $10.00)
            ],
            &schema,
        )?;
    }

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    println!("All Products (20 items):");
    println!("=======================");

    // Example 1: Simple LIMIT
    println!("\nExample 1: First 5 products (LIMIT 5)");
    println!("-------------------------------------");

    let seq_scan1 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));
    let mut limit1 = LimitExecutor::new(seq_scan1, 5);
    limit1.init()?;

    while let Some(tuple) = limit1.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let id = match &values[0] {
            Value::Int32(i) => *i,
            _ => unreachable!(),
        };
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        let price_cents = match &values[2] {
            Value::Int32(p) => *p,
            _ => unreachable!(),
        };
        let price = price_cents as f64 / 100.0;
        println!("ID: {:2}, Name: {:<15}, Price: ${:.2}", id, name, price);
    }

    // Example 2: Pagination - Page 1
    println!("\nExample 2: Pagination - Page 1 (LIMIT 5 OFFSET 0)");
    println!("------------------------------------------------");

    let seq_scan2 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));
    let mut limit2 = LimitExecutor::with_offset(seq_scan2, 5, 0);
    limit2.init()?;

    while let Some(tuple) = limit2.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let id = match &values[0] {
            Value::Int32(i) => *i,
            _ => unreachable!(),
        };
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        println!("  {} - {}", id, name);
    }

    // Example 3: Pagination - Page 2
    println!("\nExample 3: Pagination - Page 2 (LIMIT 5 OFFSET 5)");
    println!("------------------------------------------------");

    let seq_scan3 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));
    let mut limit3 = LimitExecutor::with_offset(seq_scan3, 5, 5);
    limit3.init()?;

    while let Some(tuple) = limit3.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let id = match &values[0] {
            Value::Int32(i) => *i,
            _ => unreachable!(),
        };
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        println!("  {} - {}", id, name);
    }

    // Example 4: Pagination - Page 3
    println!("\nExample 4: Pagination - Page 3 (LIMIT 5 OFFSET 10)");
    println!("-------------------------------------------------");

    let seq_scan4 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));
    let mut limit4 = LimitExecutor::with_offset(seq_scan4, 5, 10);
    limit4.init()?;

    while let Some(tuple) = limit4.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let id = match &values[0] {
            Value::Int32(i) => *i,
            _ => unreachable!(),
        };
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        println!("  {} - {}", id, name);
    }

    // Example 5: Large offset
    println!("\nExample 5: Skip to last few items (LIMIT 5 OFFSET 17)");
    println!("----------------------------------------------------");

    let seq_scan5 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));
    let mut limit5 = LimitExecutor::with_offset(seq_scan5, 5, 17);
    limit5.init()?;

    let mut count = 0;
    while let Some(tuple) = limit5.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let id = match &values[0] {
            Value::Int32(i) => *i,
            _ => unreachable!(),
        };
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        println!("  {} - {}", id, name);
        count += 1;
    }
    println!("  (Returned {} items - less than limit due to end of data)", count);

    // Example 6: Combined with filter and limit
    println!("\nExample 6: Expensive products with limit (price > 100, LIMIT 3)");
    println!("--------------------------------------------------------------");

    let seq_scan6 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    // Filter for products with price > 10000 cents ($100)
    let filter_predicate = Box::new(move |values: &[Value]| {
        if let Value::Int32(price_cents) = &values[2] {
            *price_cents > 10000
        } else {
            false
        }
    });
    let filter = Box::new(vibedb::executor::FilterExecutor::new(
        seq_scan6,
        filter_predicate,
    ));

    let mut limit6 = LimitExecutor::new(filter, 3);
    limit6.init()?;

    while let Some(tuple) = limit6.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let id = match &values[0] {
            Value::Int32(i) => *i,
            _ => unreachable!(),
        };
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        let price_cents = match &values[2] {
            Value::Int32(p) => *p,
            _ => unreachable!(),
        };
        let price = price_cents as f64 / 100.0;
        println!("  ID: {:2}, Name: {:<15}, Price: ${:.2}", id, name, price);
    }

    Ok(())
}