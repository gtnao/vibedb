//! Expression system demonstration example.
//!
//! This example showcases the full power of VibeDB's expression system:
//! - Complex boolean expressions with AND/OR/NOT
//! - Arithmetic operations in queries (using integers)
//! - String operations (LIKE patterns)
//! - NULL handling
//! - Type coercion
//! - Nested expressions
//! - Expression evaluation in different contexts

use anyhow::Result;
use std::path::Path;
use vibedb::{
    access::{DataType, Value},
    database::Database,
    executor::{ExecutionContext, Executor, FilterExecutor, InsertExecutor, SeqScanExecutor},
    expression::{BinaryOperator, Expression, UnaryOperator},
};

fn main() -> Result<()> {
    println!("=== VibeDB Expression System Demo ===\n");

    // Initialize database
    let db_path = Path::new("expression_demo.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut database = Database::create(db_path)?;
    println!("Created database: expression_demo.db");

    // Create and populate tables
    create_employee_table(&mut database)?;
    populate_employee_table(&database)?;

    // Demonstrate various expression features
    demonstrate_comparison_operators(&database)?;
    demonstrate_logical_operators(&database)?;
    demonstrate_arithmetic_operations(&database)?;
    demonstrate_string_operations(&database)?;
    demonstrate_null_handling(&database)?;
    demonstrate_complex_expressions(&database)?;

    // Clean up
    std::fs::remove_file(db_path)?;

    println!("\n=== Expression System Demo Complete ===");
    Ok(())
}

fn create_employee_table(database: &mut Database) -> Result<()> {
    println!("Creating employee table...");

    database.create_table_with_columns(
        "employees",
        vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("department", DataType::Varchar),
            ("salary", DataType::Int32), // Salary in dollars
            ("years_experience", DataType::Int32),
            ("is_manager", DataType::Boolean),
            ("bonus_percentage", DataType::Int32), // Bonus percentage (e.g., 10 = 10%)
        ],
    )?;

    println!("✓ Table 'employees' created\n");
    Ok(())
}

fn populate_employee_table(database: &Database) -> Result<()> {
    println!("Populating employee table...");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    let employees = vec![
        vec![
            Value::Int32(1),
            Value::String("Alice Smith".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(95000),
            Value::Int32(5),
            Value::Boolean(false),
            Value::Int32(10),
        ],
        vec![
            Value::Int32(2),
            Value::String("Bob Johnson".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(120000),
            Value::Int32(8),
            Value::Boolean(true),
            Value::Int32(15),
        ],
        vec![
            Value::Int32(3),
            Value::String("Carol Davis".to_string()),
            Value::String("Sales".to_string()),
            Value::Int32(80000),
            Value::Int32(3),
            Value::Boolean(false),
            Value::Int32(20),
        ],
        vec![
            Value::Int32(4),
            Value::String("David Wilson".to_string()),
            Value::String("Sales".to_string()),
            Value::Int32(110000),
            Value::Int32(10),
            Value::Boolean(true),
            Value::Int32(25),
        ],
        vec![
            Value::Int32(5),
            Value::String("Eve Brown".to_string()),
            Value::String("HR".to_string()),
            Value::Int32(75000),
            Value::Int32(2),
            Value::Boolean(false),
            Value::Int32(5),
        ],
        vec![
            Value::Int32(6),
            Value::String("Frank Lee".to_string()),
            Value::String("Engineering".to_string()),
            Value::Int32(105000),
            Value::Int32(6),
            Value::Boolean(false),
            Value::Null, // No bonus
        ],
        vec![
            Value::Int32(7),
            Value::String("Grace Kim".to_string()),
            Value::String("HR".to_string()),
            Value::Int32(90000),
            Value::Int32(7),
            Value::Boolean(true),
            Value::Int32(12),
        ],
    ];

    let mut insert = InsertExecutor::new("employees".to_string(), employees, context);
    insert.init()?;

    if let Some(tuple) = insert.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &[DataType::Int32])?;
        if let Value::Int32(count) = values[0] {
            println!("✓ Inserted {} employees", count);
        }
    }

    println!();
    Ok(())
}

fn demonstrate_comparison_operators(database: &Database) -> Result<()> {
    println!("=== Comparison Operators ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Boolean,
        DataType::Int32,
    ];

    // 1. Greater than
    println!("1. Employees with salary > $100,000:");
    let scan1 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let salary_filter = Expression::binary_op(
        BinaryOperator::Gt,
        Expression::column_with_name(3, "salary"),
        Expression::literal(Value::Int32(100000)),
    );

    let mut filter1 = FilterExecutor::new(scan1, salary_filter);
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
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // 2. Less than or equal
    println!("\n2. Junior employees (≤ 3 years experience):");
    let scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let experience_filter = Expression::binary_op(
        BinaryOperator::Le,
        Expression::column_with_name(4, "years_experience"),
        Expression::literal(Value::Int32(3)),
    );

    let mut filter2 = FilterExecutor::new(scan2, experience_filter);
    filter2.init()?;

    while let Some(tuple) = filter2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({} years)",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[4] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // 3. Not equal
    println!("\n3. Non-Engineering departments:");
    let scan3 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let dept_filter = Expression::binary_op(
        BinaryOperator::Ne,
        Expression::column_with_name(2, "department"),
        Expression::literal(Value::String("Engineering".to_string())),
    );

    let mut filter3 = FilterExecutor::new(scan3, dept_filter);
    filter3.init()?;

    while let Some(tuple) = filter3.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_logical_operators(database: &Database) -> Result<()> {
    println!("=== Logical Operators ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Boolean,
        DataType::Int32,
    ];

    // 1. AND operator
    println!("1. Senior managers (manager AND experience > 5):");
    let scan1 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let is_manager = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(5, "is_manager"),
        Expression::literal(Value::Boolean(true)),
    );

    let is_senior = Expression::binary_op(
        BinaryOperator::Gt,
        Expression::column_with_name(4, "years_experience"),
        Expression::literal(Value::Int32(5)),
    );

    let and_filter = Expression::binary_op(BinaryOperator::And, is_manager, is_senior);

    let mut filter1 = FilterExecutor::new(scan1, and_filter);
    filter1.init()?;

    while let Some(tuple) = filter1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({} years, Manager)",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[4] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // 2. OR operator
    println!("\n2. High earners OR Sales department:");
    let scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let high_salary = Expression::binary_op(
        BinaryOperator::Gt,
        Expression::column_with_name(3, "salary"),
        Expression::literal(Value::Int32(110000)),
    );

    let is_sales = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(2, "department"),
        Expression::literal(Value::String("Sales".to_string())),
    );

    let or_filter = Expression::binary_op(BinaryOperator::Or, high_salary, is_sales);

    let mut filter2 = FilterExecutor::new(scan2, or_filter);
    filter2.init()?;

    while let Some(tuple) = filter2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({}, ${})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // 3. NOT operator
    println!("\n3. Non-managers:");
    let scan3 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let is_manager_expr = Expression::column_with_name(5, "is_manager");
    let not_manager = Expression::unary_op(UnaryOperator::Not, is_manager_expr);

    let mut filter3 = FilterExecutor::new(scan3, not_manager);
    filter3.init()?;

    while let Some(tuple) = filter3.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {}",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_arithmetic_operations(database: &Database) -> Result<()> {
    println!("=== Arithmetic Operations ===");
    println!("Note: Complex arithmetic in filters is limited in the current implementation.");
    println!("Showing conceptual examples:\n");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Boolean,
        DataType::Int32,
    ];

    // Show employees with high salaries
    println!("1. Employees with salary > $100,000:");
    let scan1 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let high_salary_filter = Expression::binary_op(
        BinaryOperator::Gt,
        Expression::column_with_name(3, "salary"),
        Expression::literal(Value::Int32(100000)),
    );

    let mut filter1 = FilterExecutor::new(scan1, high_salary_filter);
    filter1.init()?;

    while let Some(tuple) = filter1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        let name = match &values[1] {
            Value::String(v) => v.as_str(),
            _ => "?",
        };
        let salary = match &values[3] {
            Value::Int32(v) => v,
            _ => &0,
        };
        let bonus_pct = match &values[6] {
            Value::Int32(v) => Some(v),
            Value::Null => None,
            _ => Some(&0),
        };

        // Calculate total compensation manually for display
        let total = match bonus_pct {
            Some(pct) => salary + (salary * pct / 100),
            None => *salary,
        };
        println!("  - {} (Salary: ${}, Total: ${})", name, salary, total);
    }

    println!();
    Ok(())
}

fn demonstrate_string_operations(database: &Database) -> Result<()> {
    println!("=== String Operations ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Boolean,
        DataType::Int32,
    ];

    // 1. LIKE pattern matching - names ending with specific pattern
    println!("1. Employees with names ending in 'son':");
    let scan1 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let name_pattern = Expression::binary_op(
        BinaryOperator::Like,
        Expression::column_with_name(1, "name"),
        Expression::literal(Value::String("%son".to_string())),
    );

    let mut filter1 = FilterExecutor::new(scan1, name_pattern);
    filter1.init()?;

    while let Some(tuple) = filter1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {}",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    // 2. LIKE with wildcard in middle
    println!("\n2. Employees with 'a' in their name:");
    let scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let contains_a = Expression::binary_op(
        BinaryOperator::Like,
        Expression::column_with_name(1, "name"),
        Expression::literal(Value::String("%a%".to_string())),
    );

    let mut filter2 = FilterExecutor::new(scan2, contains_a);
    filter2.init()?;

    while let Some(tuple) = filter2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {}",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    // 3. Exact prefix matching
    println!("\n3. Departments starting with 'E':");
    let scan3 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let dept_prefix = Expression::binary_op(
        BinaryOperator::Like,
        Expression::column_with_name(2, "department"),
        Expression::literal(Value::String("E%".to_string())),
    );

    let mut filter3 = FilterExecutor::new(scan3, dept_prefix);
    filter3.init()?;

    while let Some(tuple) = filter3.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    println!();
    Ok(())
}

fn demonstrate_null_handling(database: &Database) -> Result<()> {
    println!("=== NULL Handling ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Boolean,
        DataType::Int32,
    ];

    // 1. IS NULL check
    println!("1. Employees with NULL bonus percentage:");
    let scan1 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let bonus_col = Expression::column_with_name(6, "bonus_percentage");
    let is_null = Expression::unary_op(UnaryOperator::IsNull, bonus_col);

    let mut filter1 = FilterExecutor::new(scan1, is_null);
    filter1.init()?;

    while let Some(tuple) = filter1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (no bonus)",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            }
        );
    }

    // 2. IS NOT NULL check
    println!("\n2. Employees with bonus percentage set:");
    let scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let bonus_col2 = Expression::column_with_name(6, "bonus_percentage");
    let is_not_null = Expression::unary_op(UnaryOperator::IsNotNull, bonus_col2);

    let mut filter2 = FilterExecutor::new(scan2, is_not_null);
    filter2.init()?;

    while let Some(tuple) = filter2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} (bonus: {}%)",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[6] {
                Value::Int32(v) => v.to_string(),
                Value::Null => "NULL".to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // 3. NULL in arithmetic (NULL propagation)
    println!("\n3. Total compensation with NULL handling:");
    let mut scan3_exec = SeqScanExecutor::new("employees".to_string(), context.clone());
    scan3_exec.init()?;

    while let Some(tuple) = scan3_exec.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        let name = match &values[1] {
            Value::String(v) => v.as_str(),
            _ => "?",
        };
        let salary = match &values[3] {
            Value::Int32(v) => v,
            _ => &0,
        };
        let bonus_pct = match &values[6] {
            Value::Int32(v) => Some(v),
            Value::Null => None,
            _ => Some(&0),
        };

        let total = match bonus_pct {
            None => *salary,
            Some(bonus) => salary + (salary * bonus / 100),
        };
        println!("  - {} (Total: ${})", name, total);
    }

    println!();
    Ok(())
}

fn demonstrate_complex_expressions(database: &Database) -> Result<()> {
    println!("=== Complex Nested Expressions ===");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    let schema = vec![
        DataType::Int32,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Int32,
        DataType::Int32,
        DataType::Boolean,
        DataType::Int32,
    ];

    // 1. Complex filter: (Engineering OR Sales) AND (salary > 90000 OR manager)
    println!("1. (Engineering OR Sales) AND (high salary OR manager):");
    let scan1 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    // Department condition: Engineering OR Sales
    let is_engineering = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(2, "department"),
        Expression::literal(Value::String("Engineering".to_string())),
    );

    let is_sales = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(2, "department"),
        Expression::literal(Value::String("Sales".to_string())),
    );

    let dept_condition = Expression::binary_op(BinaryOperator::Or, is_engineering, is_sales);

    // Compensation condition: salary > 90000 OR is_manager
    let high_salary = Expression::binary_op(
        BinaryOperator::Gt,
        Expression::column_with_name(3, "salary"),
        Expression::literal(Value::Int32(90000)),
    );

    let is_manager = Expression::binary_op(
        BinaryOperator::Eq,
        Expression::column_with_name(5, "is_manager"),
        Expression::literal(Value::Boolean(true)),
    );

    let comp_condition = Expression::binary_op(BinaryOperator::Or, high_salary, is_manager);

    // Combined condition
    let complex_filter = Expression::binary_op(BinaryOperator::And, dept_condition, comp_condition);

    let mut filter1 = FilterExecutor::new(scan1, complex_filter);
    filter1.init()?;

    while let Some(tuple) = filter1.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({}, ${}, Manager: {})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[3] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
            match &values[5] {
                Value::Boolean(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    // 2. Experience-based categorization
    println!("\n2. Senior employees (>= 7 years OR (manager AND >= 5 years)):");
    let scan2 = Box::new(SeqScanExecutor::new(
        "employees".to_string(),
        context.clone(),
    ));

    let years = Expression::column_with_name(4, "years_experience");
    let is_mgr_col = Expression::column_with_name(5, "is_manager");

    // Senior condition: >= 7 years
    let very_experienced = Expression::binary_op(
        BinaryOperator::Ge,
        years.clone(),
        Expression::literal(Value::Int32(7)),
    );

    // Manager with 5+ years
    let mgr_check = Expression::binary_op(
        BinaryOperator::Eq,
        is_mgr_col,
        Expression::literal(Value::Boolean(true)),
    );

    let experienced_check = Expression::binary_op(
        BinaryOperator::Ge,
        years,
        Expression::literal(Value::Int32(5)),
    );

    let mgr_and_experienced =
        Expression::binary_op(BinaryOperator::And, mgr_check, experienced_check);

    let senior_filter =
        Expression::binary_op(BinaryOperator::Or, very_experienced, mgr_and_experienced);

    let mut filter2 = FilterExecutor::new(scan2, senior_filter);
    filter2.init()?;

    while let Some(tuple) = filter2.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &schema)?;
        println!(
            "  - {} ({}, {} years, Manager: {})",
            match &values[1] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[2] {
                Value::String(v) => v.as_str(),
                _ => "?",
            },
            match &values[4] {
                Value::Int32(v) => v.to_string(),
                _ => "?".to_string(),
            },
            match &values[5] {
                Value::Boolean(v) => v.to_string(),
                _ => "?".to_string(),
            }
        );
    }

    println!();
    Ok(())
}
