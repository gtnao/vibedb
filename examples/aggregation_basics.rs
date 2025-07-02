//! Basic aggregation functions without GROUP BY
//!
//! This example demonstrates the use of aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//! on entire tables without grouping.

use anyhow::Result;
use std::path::Path;
use vibedb::{
    access::{deserialize_values, DataType, Value},
    database::Database,
    executor::{
        AggregateFunction, AggregateSpec, ExecutionContext, Executor, FilterExecutor,
        GroupByClause, HashAggregateExecutor, InsertExecutor, SeqScanExecutor,
    },
    expression::{BinaryOperator, Expression},
};

// Helper function to display values
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::String(s) => s.clone(),
    }
}

fn main() -> Result<()> {
    println!("=== Basic Aggregation Functions Demo ===\n");

    // Initialize database
    let db_path = Path::new("aggregation_basics.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut database = Database::create(db_path)?;
    println!("Created database: aggregation_basics.db");

    // Create a test table: employees (id INT, name VARCHAR, department VARCHAR, salary INT)
    database.create_table_with_columns(
        "employees",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("department", DataType::Varchar),
            ("salary", DataType::Int32),
        ],
    )?;
    println!("✓ Table 'employees' created\n");

    // Insert sample employee data
    let employees = vec![
        (1, "Alice", "Engineering", 80000),
        (2, "Bob", "Engineering", 75000),
        (3, "Charlie", "Sales", 65000),
        (4, "Diana", "Sales", 70000),
        (5, "Eve", "Engineering", 85000),
        (6, "Frank", "HR", 60000),
        (7, "Grace", "Sales", 72000),
        (8, "Henry", "HR", 58000),
    ];

    println!("=== Inserting Employee Data ===");
    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    for (id, name, dept, salary) in employees {
        let values = vec![vec![
            Value::Int32(id),
            Value::String(name.to_string()),
            Value::String(dept.to_string()),
            Value::Int32(salary),
        ]];

        let mut insert_executor =
            InsertExecutor::new("employees".to_string(), values, context.clone());
        insert_executor.init()?;
        insert_executor.next()?;

        println!("Inserted: {} - {} - {} - ${}", id, name, dept, salary);
    }
    println!();

    // Example 1: COUNT(*) - Total number of employees
    println!("=== Example 1: COUNT(*) - Total Employees ===");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));

        // No GROUP BY - empty clause
        let group_by = GroupByClause::new(vec![]);

        // COUNT(*) - count all rows
        let aggregates = vec![AggregateSpec::with_alias(
            AggregateFunction::Count,
            None, // None means COUNT(*)
            "count".to_string(),
        )];

        let mut agg_executor = HashAggregateExecutor::new(scan, group_by, aggregates)?;

        agg_executor.init()?;
        if let Some(tuple) = agg_executor.next()? {
            let output_schema = agg_executor.output_schema();
            let schema_types: Vec<DataType> = output_schema.iter().map(|c| c.data_type).collect();
            let values = deserialize_values(&tuple.data, &schema_types)?;
            println!("Total number of employees: {}", value_to_string(&values[0]));
        }
    }
    println!();

    // Example 2: SUM(salary) - Total salary expense
    println!("=== Example 2: SUM(salary) - Total Salary Expense ===");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));

        let group_by = GroupByClause::new(vec![]);

        let aggregates = vec![AggregateSpec::with_alias(
            AggregateFunction::Sum,
            Some(3), // salary is column index 3
            "total_salary".to_string(),
        )];

        let mut agg_executor = HashAggregateExecutor::new(scan, group_by, aggregates)?;

        agg_executor.init()?;
        if let Some(tuple) = agg_executor.next()? {
            let output_schema = agg_executor.output_schema();
            let schema_types: Vec<DataType> = output_schema.iter().map(|c| c.data_type).collect();
            let values = deserialize_values(&tuple.data, &schema_types)?;
            println!("Total salary expense: ${}", value_to_string(&values[0]));
        }
    }
    println!();

    // Example 3: AVG(salary) - Average salary
    println!("=== Example 3: AVG(salary) - Average Salary ===");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));

        let group_by = GroupByClause::new(vec![]);

        let aggregates = vec![AggregateSpec::with_alias(
            AggregateFunction::Avg,
            Some(3), // salary column
            "avg_salary".to_string(),
        )];

        let mut agg_executor = HashAggregateExecutor::new(scan, group_by, aggregates)?;

        agg_executor.init()?;
        if let Some(tuple) = agg_executor.next()? {
            let output_schema = agg_executor.output_schema();
            let schema_types: Vec<DataType> = output_schema.iter().map(|c| c.data_type).collect();
            let values = deserialize_values(&tuple.data, &schema_types)?;
            println!("Average salary: ${}", value_to_string(&values[0]));
        }
    }
    println!();

    // Example 4: MIN(salary) and MAX(salary) - Salary range
    println!("=== Example 4: MIN/MAX(salary) - Salary Range ===");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));

        let group_by = GroupByClause::new(vec![]);

        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Min,
                Some(3), // salary column
                "min_salary".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Max,
                Some(3), // salary column
                "max_salary".to_string(),
            ),
        ];

        let mut agg_executor = HashAggregateExecutor::new(scan, group_by, aggregates)?;

        agg_executor.init()?;
        if let Some(tuple) = agg_executor.next()? {
            let output_schema = agg_executor.output_schema();
            let schema_types: Vec<DataType> = output_schema.iter().map(|c| c.data_type).collect();
            let values = deserialize_values(&tuple.data, &schema_types)?;
            println!("Minimum salary: ${}", value_to_string(&values[0]));
            println!("Maximum salary: ${}", value_to_string(&values[1]));
        }
    }
    println!();

    // Example 5: Multiple aggregations in one query
    println!("=== Example 5: Multiple Aggregations ===");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));

        let group_by = GroupByClause::new(vec![]);

        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None, // COUNT(*)
                "count".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(3), // salary column
                "total_salary".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Avg,
                Some(3), // salary column
                "avg_salary".to_string(),
            ),
        ];

        let mut agg_executor = HashAggregateExecutor::new(scan, group_by, aggregates)?;

        agg_executor.init()?;
        if let Some(tuple) = agg_executor.next()? {
            let output_schema = agg_executor.output_schema();
            let schema_types: Vec<DataType> = output_schema.iter().map(|c| c.data_type).collect();
            let values = deserialize_values(&tuple.data, &schema_types)?;
            println!("Employee Statistics:");
            println!("  Count: {}", value_to_string(&values[0]));
            println!("  Total Salary: ${}", value_to_string(&values[1]));
            println!("  Average Salary: ${}", value_to_string(&values[2]));
        }
    }
    println!();

    // Example 6: Aggregation with WHERE clause - Engineering department stats
    println!("=== Example 6: Aggregation with WHERE clause ===");
    println!("Engineering Department Statistics:");
    {
        // Create filter for Engineering department
        let filter_expr = Expression::binary_op(
            BinaryOperator::Eq,
            Expression::column(2), // department column
            Expression::literal(Value::String("Engineering".to_string())),
        );

        let scan = Box::new(SeqScanExecutor::new(
            "employees".to_string(),
            context.clone(),
        ));

        let filter = Box::new(FilterExecutor::new(scan, filter_expr));

        let group_by = GroupByClause::new(vec![]);

        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None, // COUNT(*)
                "count".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Avg,
                Some(3), // salary column
                "avg_salary".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Max,
                Some(3), // salary column
                "max_salary".to_string(),
            ),
        ];

        let mut agg_executor = HashAggregateExecutor::new(filter, group_by, aggregates)?;

        agg_executor.init()?;
        if let Some(tuple) = agg_executor.next()? {
            let output_schema = agg_executor.output_schema();
            let schema_types: Vec<DataType> = output_schema.iter().map(|c| c.data_type).collect();
            let values = deserialize_values(&tuple.data, &schema_types)?;
            println!("  Engineers: {}", value_to_string(&values[0]));
            println!("  Average Salary: ${}", value_to_string(&values[1]));
            println!("  Highest Salary: ${}", value_to_string(&values[2]));
        }
    }

    // Clean up
    std::fs::remove_file(db_path)?;

    println!("\n=== All aggregation operations completed successfully! ===");

    Ok(())
}
