//! Example demonstrating hash join execution

use anyhow::Result;
use vibedb::access::{DataType, TableHeap, Value};
use vibedb::database::Database;
use vibedb::executor::{
    ExecutionContext, Executor, FilterBuilder, HashJoinExecutor, ProjectionExecutor,
    SeqScanExecutor,
};

fn main() -> Result<()> {
    // Create a temporary database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("hash_join_example.db");
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
            ("budget", DataType::Int32),
        ],
    )?;

    // Insert employee data
    println!("\nInserting employee data...");
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
        vec![
            Value::Int32(1),
            Value::String("Alice Johnson".to_string()),
            Value::Int32(100),
            Value::Int32(85000),
        ],
        vec![
            Value::Int32(2),
            Value::String("Bob Smith".to_string()),
            Value::Int32(200),
            Value::Int32(75000),
        ],
        vec![
            Value::Int32(3),
            Value::String("Charlie Brown".to_string()),
            Value::Int32(100),
            Value::Int32(90000),
        ],
        vec![
            Value::Int32(4),
            Value::String("Diana Prince".to_string()),
            Value::Int32(300),
            Value::Int32(95000),
        ],
        vec![
            Value::Int32(5),
            Value::String("Eve Wilson".to_string()),
            Value::Int32(200),
            Value::Int32(80000),
        ],
    ];

    for emp in employees {
        emp_heap.insert_values(&emp, &emp_schema)?;
    }

    // Insert department data
    println!("Inserting department data...");
    let dept_table = db.catalog.get_table("departments")?.unwrap();
    let mut dept_heap = TableHeap::with_first_page(
        (*db.buffer_pool).clone(),
        dept_table.table_id,
        dept_table.first_page_id,
    );

    let dept_schema = vec![DataType::Int32, DataType::Varchar, DataType::Int32];

    let departments = vec![
        vec![
            Value::Int32(100),
            Value::String("Engineering".to_string()),
            Value::Int32(500000),
        ],
        vec![
            Value::Int32(200),
            Value::String("Sales".to_string()),
            Value::Int32(300000),
        ],
        vec![
            Value::Int32(300),
            Value::String("Marketing".to_string()),
            Value::Int32(200000),
        ],
    ];

    for dept in departments {
        dept_heap.insert_values(&dept, &dept_schema)?;
    }

    // Create execution context
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    // Create hash join: employees JOIN departments ON employees.dept_id = departments.dept_id
    println!("\nPerforming hash join...");
    println!("Query: SELECT emp_name, dept_name, salary FROM employees JOIN departments ON employees.dept_id = departments.dept_id");

    // Create seq scans for both tables
    let emp_scan = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));
    let dept_scan = Box::new(SeqScanExecutor::new(
        "departments".to_string(),
        context.clone(),
    ));

    // Define join keys
    let left_keys = vec![FilterBuilder::column(2)]; // employees.dept_id (index 2)
    let right_keys = vec![FilterBuilder::column(0)]; // departments.dept_id (index 0)

    // Create hash join
    let join = Box::new(HashJoinExecutor::new(
        emp_scan, dept_scan, left_keys, right_keys, None,
    )?);

    // Add projection to select only emp_name, dept_name, salary
    let projections = vec![
        1, // emp_name
        5, // dept_name (index 5 = 4 emp columns + 1)
        3, // salary
    ];
    let mut projection = ProjectionExecutor::new(join, projections);

    // Execute the query
    projection.init()?;

    println!("\nResults:");
    println!(
        "{:<20} {:<15} {:<10}",
        "Employee Name", "Department", "Salary"
    );
    println!("{}", "-".repeat(50));

    while let Some(tuple) = projection.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![DataType::Varchar, DataType::Varchar, DataType::Int32],
        )?;

        if let (Value::String(emp_name), Value::String(dept_name), Value::Int32(salary)) =
            (&values[0], &values[1], &values[2])
        {
            println!("{:<20} {:<15} ${:<10}", emp_name, dept_name, salary);
        }
    }

    // Now let's try a hash join with an additional condition
    println!("\n\nPerforming hash join with additional condition...");
    println!("Query: SELECT emp_name, dept_name FROM employees JOIN departments");
    println!("       ON employees.dept_id = departments.dept_id");
    println!("       WHERE employees.salary > departments.budget / 5");

    // Create new seq scans
    let emp_scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));
    let dept_scan2 = Box::new(SeqScanExecutor::new("departments".to_string(), context));

    // Same join keys
    let left_keys2 = vec![FilterBuilder::column(2)]; // employees.dept_id
    let right_keys2 = vec![FilterBuilder::column(0)]; // departments.dept_id

    // Additional condition: employees.salary > departments.budget / 5
    let additional_condition = Some(FilterBuilder::gt(
        FilterBuilder::column(3), // employees.salary
        vibedb::expression::Expression::div_expr(
            FilterBuilder::column(6), // departments.budget (4 + 2)
            FilterBuilder::int32(5),
        ),
    ));

    // Create hash join with additional condition
    let join2 = Box::new(HashJoinExecutor::new(
        emp_scan2,
        dept_scan2,
        left_keys2,
        right_keys2,
        additional_condition,
    )?);

    // Project only emp_name and dept_name
    let projections2 = vec![
        1, // emp_name
        5, // dept_name
    ];
    let mut projection2 = ProjectionExecutor::new(join2, projections2);

    projection2.init()?;

    println!("\nHigh earners (salary > department budget / 5):");
    println!("{:<20} {:<15}", "Employee Name", "Department");
    println!("{}", "-".repeat(35));

    while let Some(tuple) = projection2.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &vec![DataType::Varchar, DataType::Varchar],
        )?;

        if let (Value::String(emp_name), Value::String(dept_name)) = (&values[0], &values[1]) {
            println!("{:<20} {:<15}", emp_name, dept_name);
        }
    }

    println!("\nHash join example completed successfully!");

    Ok(())
}
