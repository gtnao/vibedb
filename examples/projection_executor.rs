//! Example demonstrating the Projection executor
//!
//! This example shows how to use the ProjectionExecutor to select specific columns
//! from a table and reorder them.

use anyhow::Result;
use tempfile::tempdir;
use vibedb::access::{DataType, Value};
use vibedb::database::Database;
use vibedb::executor::{ExecutionContext, Executor, ProjectionExecutor, SeqScanExecutor};

fn main() -> Result<()> {
    // Create a temporary database
    let dir = tempdir()?;
    let db_path = dir.path().join("projection_example.db");
    let mut db = Database::create(&db_path)?;

    // Create a table with multiple columns
    db.create_table_with_columns(
        "products",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("category", DataType::Varchar),
            ("price", DataType::Int32),
            ("in_stock", DataType::Boolean),
        ],
    )?;

    // Get table schema
    let table_info = db.catalog.get_table("products")?.unwrap();
    let columns = db.catalog.get_table_columns(table_info.table_id)?;
    let schema: Vec<DataType> = columns.iter().map(|c| c.column_type).collect();

    // Insert some sample data
    let mut table = db.open_table("products")?;

    table.insert_values(
        &[
            Value::Int32(1),
            Value::String("Laptop".to_string()),
            Value::String("Electronics".to_string()),
            Value::Int32(999),
            Value::Boolean(true),
        ],
        &schema,
    )?;

    table.insert_values(
        &[
            Value::Int32(2),
            Value::String("Mouse".to_string()),
            Value::String("Electronics".to_string()),
            Value::Int32(29),
            Value::Boolean(true),
        ],
        &schema,
    )?;

    table.insert_values(
        &[
            Value::Int32(3),
            Value::String("Desk".to_string()),
            Value::String("Furniture".to_string()),
            Value::Int32(299),
            Value::Boolean(false),
        ],
        &schema,
    )?;

    table.insert_values(
        &[
            Value::Int32(4),
            Value::String("Chair".to_string()),
            Value::String("Furniture".to_string()),
            Value::Int32(199),
            Value::Boolean(true),
        ],
        &schema,
    )?;

    // Drop table handle to ensure data is flushed
    drop(table);

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Example 1: Select specific columns (SELECT name, price FROM products)
    println!("Example 1: SELECT name, price FROM products");
    println!("------------------------------------------");

    let seq_scan = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    // Project columns 1 (name) and 3 (price)
    let mut projection = ProjectionExecutor::new(seq_scan, vec![1, 3]);
    projection.init()?;

    while let Some(tuple) = projection.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![DataType::Varchar, DataType::Int32],
        )?;

        if let (Value::String(name), Value::Int32(price)) = (&values[0], &values[1]) {
            println!("Name: {:<15} Price: ${}", name, price);
        }
    }

    // Example 2: Reorder columns (SELECT price, id, name FROM products)
    println!("\nExample 2: SELECT price, id, name FROM products");
    println!("-----------------------------------------------");

    let seq_scan2 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    // Project columns 3 (price), 0 (id), 1 (name) - reordering
    let mut projection2 = ProjectionExecutor::new(seq_scan2, vec![3, 0, 1]);
    projection2.init()?;

    while let Some(tuple) = projection2.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![DataType::Int32, DataType::Int32, DataType::Varchar],
        )?;

        if let (Value::Int32(price), Value::Int32(id), Value::String(name)) =
            (&values[0], &values[1], &values[2])
        {
            println!("Price: ${:<6} ID: {} Name: {}", price, id, name);
        }
    }

    // Example 3: Select only in-stock products with name and availability
    println!("\nExample 3: SELECT name, in_stock FROM products WHERE in_stock = true");
    println!("--------------------------------------------------------------------");

    let seq_scan3 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    // First filter for in_stock = true
    use vibedb::expression::{BinaryOperator, Expression};
    let filter_expr = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column(4),
        Expression::literal(Value::Boolean(true)),
    );
    let filter = Box::new(vibedb::executor::FilterExecutor::new(
        seq_scan3,
        filter_expr,
    ));

    // Then project columns 1 (name) and 4 (in_stock)
    let mut projection3 = ProjectionExecutor::new(filter, vec![1, 4]);
    projection3.init()?;

    while let Some(tuple) = projection3.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![DataType::Varchar, DataType::Boolean],
        )?;

        if let (Value::String(name), Value::Boolean(in_stock)) = (&values[0], &values[1]) {
            println!(
                "Name: {:<15} In Stock: {}",
                name,
                if *in_stock { "Yes" } else { "No" }
            );
        }
    }

    // Example 4: Demonstrate duplicate column projection
    println!("\nExample 4: SELECT id, name, id FROM products (duplicate column)");
    println!("---------------------------------------------------------------");

    let seq_scan4 = Box::new(SeqScanExecutor::new(
        "products".to_string(),
        context.clone(),
    ));

    // Project columns 0 (id), 1 (name), 0 (id again)
    let mut projection4 = ProjectionExecutor::new(seq_scan4, vec![0, 1, 0]);
    projection4.init()?;

    let mut count = 0;
    while let Some(tuple) = projection4.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![DataType::Int32, DataType::Varchar, DataType::Int32],
        )?;

        if let (Value::Int32(id1), Value::String(name), Value::Int32(id2)) =
            (&values[0], &values[1], &values[2])
        {
            println!("ID: {} Name: {:<15} ID (again): {}", id1, name, id2);
        }

        count += 1;
        if count >= 2 {
            break; // Just show first 2 rows for brevity
        }
    }

    Ok(())
}
