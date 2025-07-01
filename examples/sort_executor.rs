//! Example demonstrating the SortExecutor
//!
//! This example shows how to use the SortExecutor to sort query results
//! by one or more columns with different sort orders and NULL handling.

use anyhow::Result;
use tempfile::tempdir;
use vibedb::access::{deserialize_values, DataType, TableHeap, Value};
use vibedb::database::Database;
use vibedb::executor::{
    ExecutionContext, Executor, NullOrder, SeqScanExecutor, SortCriteria, SortExecutor, SortOrder,
};

fn main() -> Result<()> {
    // Create a temporary database
    let dir = tempdir()?;
    let db_path = dir.path().join("sort_example.db");
    let mut db = Database::create(&db_path)?;

    // Create a table with sample data
    db.create_table_with_columns(
        "employees",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("department", DataType::Varchar),
            ("salary", DataType::Int32),
            ("active", DataType::Boolean),
        ],
    )?;

    // Get the table info and create a table heap
    let table_info = db.catalog.get_table("employees")?.unwrap();
    let mut heap = TableHeap::with_first_page(
        (*db.buffer_pool).clone(),
        table_info.table_id,
        table_info.first_page_id,
    );

    // Insert sample data with some NULLs
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Boolean,
    ];

    let employees = vec![
        vec![
            Value::Int32(1),
            Value::String("Alice".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(95000),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(2),
            Value::String("Bob".to_string()),
            Value::String("Sales".to_string()),
            Value::Int32(75000),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(3),
            Value::String("Charlie".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(85000),
            Value::Boolean(false),
        ],
        vec![
            Value::Int32(4),
            Value::String("David".to_string()),
            Value::String("HR".to_string()),
            Value::Null, // NULL salary
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(5),
            Value::String("Eve".to_string()),
            Value::String("Sales".to_string()),
            Value::Int32(80000),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(6),
            Value::String("Frank".to_string()),
            Value::Null, // NULL department
            Value::Int32(70000),
            Value::Boolean(true),
        ],
    ];

    for employee in employees {
        heap.insert_values(&employee, &schema)?;
    }

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Example 1: Sort by salary ascending (NULLs first by default)
    println!("Example 1: ORDER BY salary ASC");
    println!("================================");

    let seq_scan1 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let sort_criteria1 = vec![SortCriteria::new(3, SortOrder::Asc)]; // salary column
    let mut sort1 = SortExecutor::new(seq_scan1, sort_criteria1);
    sort1.init()?;

    while let Some(tuple) = sort1.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        let salary = match &values[3] {
            Value::Int32(i) => format!("${}", i),
            Value::Null => "NULL".to_string(),
            _ => unreachable!(),
        };
        println!("{:<10} {}", name, salary);
    }

    // Example 2: Sort by department ASC, then salary DESC
    println!("\nExample 2: ORDER BY department ASC, salary DESC");
    println!("================================================");

    let seq_scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let sort_criteria2 = vec![
        SortCriteria::new(2, SortOrder::Asc),  // department ASC
        SortCriteria::new(3, SortOrder::Desc), // salary DESC
    ];
    let mut sort2 = SortExecutor::new(seq_scan2, sort_criteria2);
    sort2.init()?;

    while let Some(tuple) = sort2.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        let department = match &values[2] {
            Value::String(s) => s.clone(),
            Value::Null => "NULL".to_string(),
            _ => unreachable!(),
        };
        let salary = match &values[3] {
            Value::Int32(i) => format!("${}", i),
            Value::Null => "NULL".to_string(),
            _ => unreachable!(),
        };
        println!("{:<10} {:<15} {}", name, department, salary);
    }

    // Example 3: Sort by active DESC, then name ASC with custom NULL ordering
    println!("\nExample 3: ORDER BY active DESC, name ASC (NULLs last for name)");
    println!("================================================================");

    let seq_scan3 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let sort_criteria3 = vec![
        SortCriteria::new(4, SortOrder::Desc), // active DESC
        SortCriteria::with_null_order(1, SortOrder::Asc, NullOrder::Last), // name ASC, NULLs last
    ];
    let mut sort3 = SortExecutor::new(seq_scan3, sort_criteria3);
    sort3.init()?;

    while let Some(tuple) = sort3.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        let active = match &values[4] {
            Value::Boolean(b) => if *b { "Active" } else { "Inactive" },
            _ => unreachable!(),
        };
        println!("{:<10} {}", name, active);
    }

    // Example 4: Complex sort with filtering
    println!("\nExample 4: Active employees sorted by department and salary");
    println!("===========================================================");

    let seq_scan4 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    // Filter for active employees only
    let filter_predicate = Box::new(move |values: &[Value]| matches!(&values[4], Value::Boolean(true)));
    let filter = Box::new(vibedb::executor::FilterExecutor::new(
        seq_scan4,
        filter_predicate,
    ));

    // Sort filtered results
    let sort_criteria4 = vec![
        SortCriteria::new(2, SortOrder::Asc),  // department ASC
        SortCriteria::new(3, SortOrder::Desc), // salary DESC
    ];
    let mut sort4 = SortExecutor::new(filter, sort_criteria4);
    sort4.init()?;

    while let Some(tuple) = sort4.next()? {
        let values = deserialize_values(&tuple.data, &schema)?;
        let name = match &values[1] {
            Value::String(s) => s,
            _ => unreachable!(),
        };
        let department = match &values[2] {
            Value::String(s) => s.clone(),
            Value::Null => "NULL".to_string(),
            _ => unreachable!(),
        };
        let salary = match &values[3] {
            Value::Int32(i) => format!("${}", i),
            Value::Null => "NULL".to_string(),
            _ => unreachable!(),
        };
        println!("{:<10} {:<15} {}", name, department, salary);
    }

    Ok(())
}