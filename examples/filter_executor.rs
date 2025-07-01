//! Example demonstrating the FilterExecutor functionality

use anyhow::Result;
use vibedb::access::{DataType, Value};
use vibedb::database::Database;
use vibedb::executor::{ExecutionContext, Executor, FilterExecutor, SeqScanExecutor};

fn main() -> Result<()> {
    // Create a temporary database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("filter_example.db");
    let mut db = Database::create(&db_path)?;

    // Create a table
    db.create_table_with_columns(
        "employees",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("department", DataType::Varchar),
            ("salary", DataType::Int32),
        ],
    )?;

    // Get table schema
    let table_info = db.catalog.get_table("employees")?.unwrap();
    let columns = db.catalog.get_table_columns(table_info.table_id)?;
    let schema: Vec<DataType> = columns.iter().map(|c| c.column_type).collect();

    // Insert some sample data
    let mut table = db.open_table("employees")?;

    table.insert_values(
        &[
            Value::Int32(1),
            Value::String("Alice".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(80000),
        ],
        &schema,
    )?;

    table.insert_values(
        &[
            Value::Int32(2),
            Value::String("Bob".to_string()),
            Value::String("Sales".to_string()),
            Value::Int32(60000),
        ],
        &schema,
    )?;

    table.insert_values(
        &[
            Value::Int32(3),
            Value::String("Charlie".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(90000),
        ],
        &schema,
    )?;

    table.insert_values(
        &[
            Value::Int32(4),
            Value::String("David".to_string()),
            Value::String("HR".to_string()),
            Value::Int32(55000),
        ],
        &schema,
    )?;

    // Drop table handle to ensure data is flushed
    drop(table);

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Example 1: Filter by department (WHERE department = 'Engineering')
    println!("Example 1: Employees in Engineering department");
    println!("--------------------------------------------");

    let seq_scan = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));
    let dept_filter = Box::new(
        move |values: &[Value]| matches!(&values[2], Value::String(dept) if dept == "Engineering"),
    );

    let mut filter_executor = FilterExecutor::new(seq_scan, dept_filter);
    filter_executor.init()?;

    while let Some(tuple) = filter_executor.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![
                DataType::Int32,
                DataType::Varchar,
                DataType::Varchar,
                DataType::Int32,
            ],
        )?;

        if let (Value::Int32(id), Value::String(name), Value::String(dept), Value::Int32(salary)) =
            (&values[0], &values[1], &values[2], &values[3])
        {
            println!(
                "ID: {}, Name: {}, Department: {}, Salary: ${}",
                id, name, dept, salary
            );
        }
    }

    // Example 2: Filter by salary range (WHERE salary >= 70000)
    println!("\nExample 2: Employees with salary >= $70,000");
    println!("-------------------------------------------");

    let seq_scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));
    let salary_filter = Box::new(
        move |values: &[Value]| matches!(&values[3], Value::Int32(salary) if *salary >= 70000),
    );

    let mut filter_executor2 = FilterExecutor::new(seq_scan2, salary_filter);
    filter_executor2.init()?;

    while let Some(tuple) = filter_executor2.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![
                DataType::Int32,
                DataType::Varchar,
                DataType::Varchar,
                DataType::Int32,
            ],
        )?;

        if let (Value::Int32(id), Value::String(name), Value::String(dept), Value::Int32(salary)) =
            (&values[0], &values[1], &values[2], &values[3])
        {
            println!(
                "ID: {}, Name: {}, Department: {}, Salary: ${}",
                id, name, dept, salary
            );
        }
    }

    // Example 3: Complex filter (WHERE department = 'Engineering' AND salary > 85000)
    println!("\nExample 3: Engineering employees with salary > $85,000");
    println!("------------------------------------------------------");

    let seq_scan3 = Box::new(SeqScanExecutor::new("employees".to_string(), context));
    let complex_filter = Box::new(move |values: &[Value]| {
        let dept_match = matches!(&values[2], Value::String(dept) if dept == "Engineering");
        let salary_match = matches!(&values[3], Value::Int32(salary) if *salary > 85000);
        dept_match && salary_match
    });

    let mut filter_executor3 = FilterExecutor::new(seq_scan3, complex_filter);
    filter_executor3.init()?;

    while let Some(tuple) = filter_executor3.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![
                DataType::Int32,
                DataType::Varchar,
                DataType::Varchar,
                DataType::Int32,
            ],
        )?;

        if let (Value::Int32(id), Value::String(name), Value::String(dept), Value::Int32(salary)) =
            (&values[0], &values[1], &values[2], &values[3])
        {
            println!(
                "ID: {}, Name: {}, Department: {}, Salary: ${}",
                id, name, dept, salary
            );
        }
    }

    Ok(())
}
