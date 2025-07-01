//! Example demonstrating the Nested Loop Join executor

use anyhow::Result;
use vibedb::access::{DataType, TableHeap, Value};
use vibedb::database::Database;
use vibedb::executor::{
    ExecutionContext, Executor, FilterBuilder, NestedLoopJoinExecutor, SeqScanExecutor,
};

fn main() -> Result<()> {
    // Create a new database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("join_demo.db");
    let mut db = Database::create(&db_path)?;

    // Create employees table
    println!("Creating employees table...");
    db.create_table_with_columns(
        "employees",
        vec![
            ("emp_id", DataType::Int32),
            ("emp_name", DataType::Varchar),
            ("dept_id", DataType::Int32),
            ("salary", DataType::Int32),
        ],
    )?;

    // Create departments table
    println!("Creating departments table...");
    db.create_table_with_columns(
        "departments",
        vec![
            ("dept_id", DataType::Int32),
            ("dept_name", DataType::Varchar),
            ("location", DataType::Varchar),
        ],
    )?;

    // Insert sample employees
    println!("\nInserting employees...");
    let emp_table = db.catalog.get_table("employees")?.unwrap();
    let mut emp_heap = TableHeap::with_first_page(
        (*db.buffer_pool).clone(),
        emp_table.table_id,
        emp_table.first_page_id,
    );
    let emp_schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
    ];

    let employees = vec![
        (1, "Alice Johnson", 10, 75000),
        (2, "Bob Smith", 20, 65000),
        (3, "Charlie Brown", 10, 80000),
        (4, "Diana Prince", 30, 90000),
        (5, "Eve Wilson", 20, 70000),
    ];

    for (id, name, dept_id, salary) in employees {
        emp_heap.insert_values(
            &vec![
                Value::Int32(id),
                Value::String(name.to_string()),
                Value::Int32(dept_id),
                Value::Int32(salary),
            ],
            &emp_schema,
        )?;
        println!("  Inserted employee: {} - {}", id, name);
    }

    // Insert sample departments
    println!("\nInserting departments...");
    let dept_table = db.catalog.get_table("departments")?.unwrap();
    let mut dept_heap = TableHeap::with_first_page(
        (*db.buffer_pool).clone(),
        dept_table.table_id,
        dept_table.first_page_id,
    );
    let dept_schema = vec![DataType::Int32, DataType::Varchar, DataType::Varchar];

    let departments = vec![
        (10, "Engineering", "Building A"),
        (20, "Sales", "Building B"),
        (30, "Executive", "Building C"),
    ];

    for (id, name, location) in departments {
        dept_heap.insert_values(
            &vec![
                Value::Int32(id),
                Value::String(name.to_string()),
                Value::String(location.to_string()),
            ],
            &dept_schema,
        )?;
        println!("  Inserted department: {} - {}", id, name);
    }

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Example 1: Simple join on dept_id
    println!("\n=== Example 1: Simple Join (employees.dept_id = departments.dept_id) ===");
    {
        let emp_scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));
        let dept_scan = Box::new(SeqScanExecutor::new(
            "departments".to_string(),
            context.clone(),
        ));

        // Join condition: employees.dept_id = departments.dept_id
        // Column indices: employees has 4 columns (0-3), departments starts at index 4
        let join_condition = FilterBuilder::eq(
            FilterBuilder::column(2), // employees.dept_id
            FilterBuilder::column(4), // departments.dept_id
        );

        let mut join = NestedLoopJoinExecutor::new(emp_scan, dept_scan, join_condition);
        join.init()?;

        println!("\nEmployees with their departments:");
        println!(
            "{:<10} {:<20} {:<15} {:<10}",
            "Emp ID", "Name", "Department", "Location"
        );
        println!("{}", "-".repeat(55));

        while let Some(tuple) = join.next()? {
            let values = vibedb::access::deserialize_values(
                &tuple.data,
                &vec![
                    DataType::Int32,   // emp_id
                    DataType::Varchar, // emp_name
                    DataType::Int32,   // dept_id
                    DataType::Int32,   // salary
                    DataType::Int32,   // dept_id (from departments)
                    DataType::Varchar, // dept_name
                    DataType::Varchar, // location
                ],
            )?;

            if let (
                Value::Int32(emp_id),
                Value::String(emp_name),
                _,
                _,
                _,
                Value::String(dept_name),
                Value::String(location),
            ) = (
                &values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6],
            ) {
                println!(
                    "{:<10} {:<20} {:<15} {:<10}",
                    emp_id, emp_name, dept_name, location
                );
            }
        }
    }

    // Example 2: Join with additional filter
    println!("\n=== Example 2: Join with salary filter (salary > 70000) ===");
    {
        // First, create a filtered employee scan
        let emp_scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));

        // Apply salary filter
        let salary_filter = FilterBuilder::gt(
            FilterBuilder::column(3), // salary
            FilterBuilder::int32(70000),
        );

        let filtered_emp_scan = Box::new(vibedb::executor::FilterExecutor::new(
            emp_scan,
            salary_filter,
        ));

        let dept_scan = Box::new(SeqScanExecutor::new(
            "departments".to_string(),
            context.clone(),
        ));

        // Join condition
        let join_condition = FilterBuilder::eq(
            FilterBuilder::column(2), // employees.dept_id
            FilterBuilder::column(4), // departments.dept_id
        );

        let mut join = NestedLoopJoinExecutor::new(filtered_emp_scan, dept_scan, join_condition);
        join.init()?;

        println!("\nHigh-earning employees with their departments:");
        println!(
            "{:<10} {:<20} {:<10} {:<15}",
            "Emp ID", "Name", "Salary", "Department"
        );
        println!("{}", "-".repeat(55));

        while let Some(tuple) = join.next()? {
            let values = vibedb::access::deserialize_values(
                &tuple.data,
                &vec![
                    DataType::Int32,   // emp_id
                    DataType::Varchar, // emp_name
                    DataType::Int32,   // dept_id
                    DataType::Int32,   // salary
                    DataType::Int32,   // dept_id (from departments)
                    DataType::Varchar, // dept_name
                    DataType::Varchar, // location
                ],
            )?;

            if let (
                Value::Int32(emp_id),
                Value::String(emp_name),
                _,
                Value::Int32(salary),
                _,
                Value::String(dept_name),
                _,
            ) = (
                &values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6],
            ) {
                println!(
                    "{:<10} {:<20} {:<10} {:<15}",
                    emp_id, emp_name, salary, dept_name
                );
            }
        }
    }

    println!("\n=== Join execution complete ===");
    Ok(())
}
