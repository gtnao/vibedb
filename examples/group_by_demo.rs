//! GROUP BY demonstrations with various scenarios
//! 
//! This example shows how to use GROUP BY with aggregate functions
//! to analyze data by different grouping criteria.

use anyhow::Result;
use vibedb::{
    access::{DataType, Value},
    database::Database,
    executor::{
        AggregateFunction, AggregateSpec, GroupByClause, HashAggregateExecutor, 
        ExecutionContext, Executor, SeqScanExecutor, InsertExecutor, 
        SortExecutor, SortCriteria, SortOrder, NullOrder, FilterExecutor,
    },
    expression::{BinaryOperator, Expression},
};
use std::path::Path;

fn main() -> Result<()> {
    println!("=== GROUP BY Demonstrations ===\n");

    // Initialize database
    let db_path = Path::new("group_by_demo.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut database = Database::create(db_path)?;
    println!("Created database: group_by_demo.db");

    // Create a test table: sales (id INT, product VARCHAR, category VARCHAR, region VARCHAR, quantity INT, price INT)
    database.create_table_with_columns(
        "sales",
        vec![
            ("id", DataType::Int32),
            ("product", DataType::Varchar),
            ("category", DataType::Varchar),
            ("region", DataType::Varchar),
            ("quantity", DataType::Int32),
            ("price", DataType::Int32),
        ],
    )?;
    println!("✓ Table 'sales' created\n");

    // Insert sample sales data
    let sales_data = vec![
        (1, "Laptop", "Electronics", "North", 5, 1200),
        (2, "Phone", "Electronics", "North", 8, 800),
        (3, "Desk", "Furniture", "North", 3, 300),
        (4, "Chair", "Furniture", "North", 10, 150),
        (5, "Laptop", "Electronics", "South", 3, 1200),
        (6, "Phone", "Electronics", "South", 12, 800),
        (7, "Desk", "Furniture", "South", 5, 300),
        (8, "Monitor", "Electronics", "South", 7, 400),
        (9, "Laptop", "Electronics", "East", 4, 1200),
        (10, "Chair", "Furniture", "East", 15, 150),
        (11, "Monitor", "Electronics", "East", 6, 400),
        (12, "Phone", "Electronics", "West", 10, 800),
        (13, "Desk", "Furniture", "West", 2, 300),
        (14, "Chair", "Furniture", "West", 8, 150),
        (15, "Monitor", "Electronics", "West", 4, 400),
    ];

    println!("=== Inserting Sales Data ===");
    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());
    
    for (id, product, category, region, quantity, price) in &sales_data {
        let values = vec![
            vec![
                Value::Int32(*id),
                Value::String(product.to_string()),
                Value::String(category.to_string()),
                Value::String(region.to_string()),
                Value::Int32(*quantity),
                Value::Int32(*price),
            ],
        ];
        
        let mut insert_executor = InsertExecutor::new(
            "sales".to_string(),
            values,
            context.clone(),
        );
        insert_executor.init()?;
        insert_executor.next()?;
    }
    println!("Inserted {} sales records", sales_data.len());
    println!();

    // Example 1: GROUP BY category - Sales summary by category
    println!("=== Example 1: GROUP BY category ===");
    println!("Sales Summary by Category:");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "sales".to_string(),
            context.clone(),
        ));

        // Group by category (column 2)
        let group_by = GroupByClause::new(vec![2]);
        
        // Aggregate expressions: COUNT(*), SUM(quantity), SUM(quantity * price)
        // Note: We'll calculate revenue by summing quantity * price for each row
        // Since we can't do arithmetic in aggregates directly, we'll sum the total per row
        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None, // COUNT(*)
                "count".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(4), // quantity column
                "total_quantity".to_string(),
            ),
            // For revenue, we would need a computed column or do it differently
            // For now, let's just use price as a proxy
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(5), // price column (as proxy for revenue)
                "total_price".to_string(),
            ),
        ];

        let mut agg_executor = HashAggregateExecutor::new(
            scan,
            group_by,
            aggregates,
        )?;

        agg_executor.init()?;
        println!("{:<15} {:<10} {:<15} {:<15}", "Category", "Count", "Total Quantity", "Total Price");
        println!("{:-<55}", "");
        while let Some(tuple) = agg_executor.next()? {
            let values = tuple.get_values();
            println!("{:<15} {:<10} {:<15} ${:<14}", 
                values[0],  // category
                values[1],  // count
                values[2],  // total quantity
                values[3]   // total price
            );
        }
    }
    println!();

    // Example 2: GROUP BY region - Regional performance
    println!("=== Example 2: GROUP BY region ===");
    println!("Regional Performance:");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "sales".to_string(),
            context.clone(),
        ));

        // Group by region (column 3)
        let group_by = GroupByClause::new(vec![3]);
        
        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None, // COUNT(*)
                "order_count".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(4), // quantity
                "total_quantity".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Avg,
                Some(5), // price
                "avg_price".to_string(),
            ),
        ];

        let agg_executor = HashAggregateExecutor::new(
            scan,
            group_by,
            aggregates,
        )?;

        // Sort by total quantity descending (column 2 in output)
        let sort_criteria = vec![SortCriteria {
            column_idx: 2,
            order: SortOrder::Desc,
            null_order: NullOrder::NullsLast,
        }];

        let mut sort_executor = SortExecutor::new(
            Box::new(agg_executor),
            sort_criteria,
        );

        sort_executor.init()?;
        println!("{:<10} {:<10} {:<15} {:<15}", "Region", "Orders", "Total Quantity", "Avg Price");
        println!("{:-<50}", "");
        
        while let Some(tuple) = sort_executor.next()? {
            let values = tuple.get_values();
            println!("{:<10} {:<10} {:<15} ${:<14}", 
                values[0],  // region
                values[1],  // order count
                values[2],  // total quantity
                values[3]   // avg price
            );
        }
    }
    println!();

    // Example 3: GROUP BY multiple columns - Product sales by region
    println!("=== Example 3: GROUP BY product, region ===");
    println!("Product Sales by Region:");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "sales".to_string(),
            context.clone(),
        ));

        // Group by product (column 1) and region (column 3)
        let group_by = GroupByClause::new(vec![1, 3]);
        
        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(4), // quantity
                "units_sold".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Max,
                Some(5), // price
                "max_price".to_string(),
            ),
        ];

        let mut agg_executor = HashAggregateExecutor::new(
            scan,
            group_by,
            aggregates,
        )?;

        agg_executor.init()?;
        println!("{:<15} {:<10} {:<15} {:<15}", "Product", "Region", "Units Sold", "Max Price");
        println!("{:-<55}", "");
        while let Some(tuple) = agg_executor.next()? {
            let values = tuple.get_values();
            println!("{:<15} {:<10} {:<15} ${:<14}", 
                values[0],  // product
                values[1],  // region
                values[2],  // units sold
                values[3]   // max price
            );
        }
    }
    println!();

    // Example 4: GROUP BY with HAVING-like functionality (using FilterExecutor after aggregation)
    println!("=== Example 4: GROUP BY with post-aggregation filtering ===");
    println!("Categories with more than 5 items sold:");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "sales".to_string(),
            context.clone(),
        ));

        // Group by category
        let group_by = GroupByClause::new(vec![2]);
        
        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(4), // quantity
                "total_quantity".to_string(),
            ),
        ];

        let agg_executor = HashAggregateExecutor::new(
            scan,
            group_by,
            aggregates,
        )?;

        // Filter for total quantity > 5
        let filter_expr = Expression::binary(
            BinaryOperator::GreaterThan,
            Expression::column(1), // total_quantity is at index 1
            Expression::literal(Value::Int32(5)),
        );

        let mut filter_executor = FilterExecutor::new(
            Box::new(agg_executor),
            filter_expr,
        );

        filter_executor.init()?;
        println!("{:<15} {:<15}", "Category", "Total Quantity");
        println!("{:-<30}", "");
        while let Some(tuple) = filter_executor.next()? {
            let values = tuple.get_values();
            println!("{:<15} {:<14}", 
                values[0],  // category
                values[1]   // total quantity
            );
        }
    }
    println!();

    // Example 5: Complex aggregation - Top products
    println!("=== Example 5: Complex aggregation ===");
    println!("Product Performance Metrics:");
    {
        let scan = Box::new(SeqScanExecutor::new(
            "sales".to_string(),
            context.clone(),
        ));

        // Group by product
        let group_by = GroupByClause::new(vec![1]);
        
        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None, // COUNT(*)
                "order_count".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(4), // quantity
                "units_sold".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Min,
                Some(5), // price
                "min_price".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Max,
                Some(5), // price
                "max_price".to_string(),
            ),
        ];

        let agg_executor = HashAggregateExecutor::new(
            scan,
            group_by,
            aggregates,
        )?;

        // Sort by units sold descending
        let sort_criteria = vec![SortCriteria {
            column_idx: 2, // units_sold column
            order: SortOrder::Desc,
            null_order: NullOrder::NullsLast,
        }];

        let mut sort_executor = SortExecutor::new(
            Box::new(agg_executor),
            sort_criteria,
        );

        sort_executor.init()?;
        println!("{:<15} {:<10} {:<12} {:<12} {:<12}", "Product", "Orders", "Units Sold", "Min Price", "Max Price");
        println!("{:-<61}", "");
        while let Some(tuple) = sort_executor.next()? {
            let values = tuple.get_values();
            println!("{:<15} {:<10} {:<12} ${:<11} ${:<11}", 
                values[0],  // product
                values[1],  // order count
                values[2],  // units sold
                values[3],  // min price
                values[4]   // max price
            );
        }
    }

    // Clean up
    std::fs::remove_file(db_path)?;

    println!("\n=== All GROUP BY operations completed successfully! ===");

    Ok(())
}