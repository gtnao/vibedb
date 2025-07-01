//! Example demonstrating the Nested Loop Join executor with mock data
//! This example uses mock executors to demonstrate the full join functionality
//! without the limitation of SeqScanExecutor re-initialization.

use anyhow::Result;
use vibedb::access::{serialize_values, DataType, Tuple, TupleId, Value};
use vibedb::executor::{ColumnInfo, Executor, FilterBuilder, NestedLoopJoinExecutor};
use vibedb::storage::page::PageId;

/// Mock executor for testing that produces a fixed set of tuples
struct MockExecutor {
    tuples: Vec<(TupleId, Vec<Value>)>,
    schema: Vec<ColumnInfo>,
    schema_types: Vec<DataType>,
    current: usize,
    initialized: bool,
}

impl MockExecutor {
    fn new(tuples: Vec<(TupleId, Vec<Value>)>, schema: Vec<ColumnInfo>) -> Self {
        let schema_types = schema.iter().map(|col| col.data_type).collect();
        Self {
            tuples,
            schema,
            schema_types,
            current: 0,
            initialized: false,
        }
    }
}

impl Executor for MockExecutor {
    fn init(&mut self) -> Result<()> {
        self.initialized = true;
        self.current = 0; // Reset to beginning
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if !self.initialized {
            anyhow::bail!("Not initialized");
        }

        if self.current >= self.tuples.len() {
            return Ok(None);
        }

        let (tuple_id, values) = &self.tuples[self.current];
        self.current += 1;

        let data = serialize_values(values, &self.schema_types)?;
        Ok(Some(Tuple::new(*tuple_id, data)))
    }

    fn output_schema(&self) -> &[ColumnInfo] {
        &self.schema
    }
}

fn main() -> Result<()> {
    println!("=== Nested Loop Join Demo with Mock Data ===\n");

    // Create employee data
    let emp_schema = vec![
        ColumnInfo::new("emp_id", DataType::Int32),
        ColumnInfo::new("emp_name", DataType::Varchar),
        ColumnInfo::new("dept_id", DataType::Int32),
        ColumnInfo::new("salary", DataType::Int32),
    ];

    let emp_tuples = vec![
        (
            TupleId::new(PageId(1), 0),
            vec![
                Value::Int32(1),
                Value::String("Alice Johnson".to_string()),
                Value::Int32(10),
                Value::Int32(75000),
            ],
        ),
        (
            TupleId::new(PageId(1), 1),
            vec![
                Value::Int32(2),
                Value::String("Bob Smith".to_string()),
                Value::Int32(20),
                Value::Int32(65000),
            ],
        ),
        (
            TupleId::new(PageId(1), 2),
            vec![
                Value::Int32(3),
                Value::String("Charlie Brown".to_string()),
                Value::Int32(10),
                Value::Int32(80000),
            ],
        ),
        (
            TupleId::new(PageId(1), 3),
            vec![
                Value::Int32(4),
                Value::String("Diana Prince".to_string()),
                Value::Int32(30),
                Value::Int32(90000),
            ],
        ),
        (
            TupleId::new(PageId(1), 4),
            vec![
                Value::Int32(5),
                Value::String("Eve Wilson".to_string()),
                Value::Int32(20),
                Value::Int32(70000),
            ],
        ),
    ];

    // Create department data
    let dept_schema = vec![
        ColumnInfo::new("dept_id", DataType::Int32),
        ColumnInfo::new("dept_name", DataType::Varchar),
        ColumnInfo::new("location", DataType::Varchar),
    ];

    let dept_tuples = vec![
        (
            TupleId::new(PageId(2), 0),
            vec![
                Value::Int32(10),
                Value::String("Engineering".to_string()),
                Value::String("Building A".to_string()),
            ],
        ),
        (
            TupleId::new(PageId(2), 1),
            vec![
                Value::Int32(20),
                Value::String("Sales".to_string()),
                Value::String("Building B".to_string()),
            ],
        ),
        (
            TupleId::new(PageId(2), 2),
            vec![
                Value::Int32(30),
                Value::String("Executive".to_string()),
                Value::String("Building C".to_string()),
            ],
        ),
    ];

    // Example 1: Simple join
    println!("Example 1: Simple Join (employees.dept_id = departments.dept_id)");
    println!(
        "{:<5} {:<20} {:<15} {:<15}",
        "ID", "Name", "Department", "Location"
    );
    println!("{}", "-".repeat(55));

    let emp_executor = Box::new(MockExecutor::new(emp_tuples.clone(), emp_schema.clone()));
    let dept_executor = Box::new(MockExecutor::new(dept_tuples.clone(), dept_schema.clone()));

    // Join condition: employees.dept_id = departments.dept_id
    let join_condition = FilterBuilder::eq(
        FilterBuilder::column(2), // employees.dept_id
        FilterBuilder::column(4), // departments.dept_id (offset by 4 employee columns)
    );

    let mut join = NestedLoopJoinExecutor::new(emp_executor, dept_executor, join_condition);
    join.init()?;

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
                "{:<5} {:<20} {:<15} {:<15}",
                emp_id, emp_name, dept_name, location
            );
        }
    }

    // Example 2: Join with complex condition
    println!("\n\nExample 2: High-salary employees (salary > 70000) with departments");
    println!(
        "{:<5} {:<20} {:<10} {:<15}",
        "ID", "Name", "Salary", "Department"
    );
    println!("{}", "-".repeat(50));

    let emp_executor = Box::new(MockExecutor::new(emp_tuples.clone(), emp_schema.clone()));
    let dept_executor = Box::new(MockExecutor::new(dept_tuples.clone(), dept_schema.clone()));

    // Complex join condition: employees.dept_id = departments.dept_id AND employees.salary > 70000
    let join_condition = FilterBuilder::and(
        FilterBuilder::eq(
            FilterBuilder::column(2), // employees.dept_id
            FilterBuilder::column(4), // departments.dept_id
        ),
        FilterBuilder::gt(
            FilterBuilder::column(3), // employees.salary
            FilterBuilder::int32(70000),
        ),
    );

    let mut join = NestedLoopJoinExecutor::new(emp_executor, dept_executor, join_condition);
    join.init()?;

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
                "{:<5} {:<20} {:<10} {:<15}",
                emp_id, emp_name, salary, dept_name
            );
        }
    }

    // Example 3: Cross product (no join condition)
    println!("\n\nExample 3: Count of all employee-department combinations");

    let emp_executor = Box::new(MockExecutor::new(emp_tuples, emp_schema));
    let dept_executor = Box::new(MockExecutor::new(dept_tuples, dept_schema));

    // Always-true condition for cross product
    let join_condition = FilterBuilder::boolean(true);

    let mut join = NestedLoopJoinExecutor::new(emp_executor, dept_executor, join_condition);
    join.init()?;

    let mut count = 0;
    while let Some(_) = join.next()? {
        count += 1;
    }

    println!(
        "Total combinations: {} (5 employees × 3 departments)",
        count
    );

    println!("\n=== Demo Complete ===");
    Ok(())
}
