//! Real-world sales analytics example using aggregations
//! 
//! This example demonstrates a comprehensive sales analytics scenario
//! combining various aggregation techniques with joins, filters, and sorting.

use anyhow::Result;
use vibedb::{
    access::{DataType, Value},
    database::Database,
    executor::{
        AggregateFunction, AggregateSpec, GroupByClause, HashAggregateExecutor, 
        ExecutionContext, Executor, SeqScanExecutor, InsertExecutor,
        SortExecutor, SortCriteria, SortOrder, NullOrder,
        FilterExecutor, NestedLoopJoinExecutor, LimitExecutor,
    },
    expression::{BinaryOperator, Expression},
};
use std::path::Path;

fn main() -> Result<()> {
    println!("=== Sales Analytics Demo ===\n");

    // Initialize database
    let db_path = Path::new("sales_analytics.db");
    if db_path.exists() {
        std::fs::remove_file(db_path)?;
    }

    let mut database = Database::create(db_path)?;
    println!("Created database: sales_analytics.db");

    // Create tables for a sales analytics scenario
    
    // 1. orders table (order_id, customer_id, order_date, total_amount, status)
    database.create_table_with_columns(
        "orders",
        vec![
            ("order_id", DataType::Int32),
            ("customer_id", DataType::Int32),
            ("order_date", DataType::Varchar), // Using VARCHAR for simplicity
            ("total_amount", DataType::Int32),
            ("status", DataType::Varchar),
        ],
    )?;

    // 2. order_items table (item_id, order_id, product_id, quantity, unit_price)
    database.create_table_with_columns(
        "order_items",
        vec![
            ("item_id", DataType::Int32),
            ("order_id", DataType::Int32),
            ("product_id", DataType::Int32),
            ("quantity", DataType::Int32),
            ("unit_price", DataType::Int32),
        ],
    )?;

    // 3. products table (product_id, product_name, category, supplier_id)
    database.create_table_with_columns(
        "products",
        vec![
            ("product_id", DataType::Int32),
            ("product_name", DataType::Varchar),
            ("category", DataType::Varchar),
            ("supplier_id", DataType::Int32),
        ],
    )?;

    // 4. customers table (customer_id, customer_name, region, segment)
    database.create_table_with_columns(
        "customers",
        vec![
            ("customer_id", DataType::Int32),
            ("customer_name", DataType::Varchar),
            ("region", DataType::Varchar),
            ("segment", DataType::Varchar),
        ],
    )?;

    println!("✓ All tables created\n");

    let context = ExecutionContext::new(database.catalog.clone(), database.buffer_pool.clone());

    // Insert sample data
    println!("=== Loading Sample Data ===");
    
    // Insert customers
    let customers = vec![
        (1, "TechCorp", "North", "Enterprise"),
        (2, "StartupXYZ", "West", "SMB"),
        (3, "MegaRetail", "East", "Enterprise"),
        (4, "LocalShop", "South", "SMB"),
        (5, "GlobalTrade", "North", "Enterprise"),
    ];
    
    for (id, name, region, segment) in customers {
        let values = vec![vec![
            Value::Int32(id),
            Value::String(name.to_string()),
            Value::String(region.to_string()),
            Value::String(segment.to_string()),
        ]];
        let mut insert = InsertExecutor::new("customers".to_string(), values, context.clone());
        insert.init()?;
        insert.next()?;
    }

    // Insert products
    let products = vec![
        (1, "Laptop Pro", "Electronics", 1),
        (2, "Office Chair", "Furniture", 2),
        (3, "Wireless Mouse", "Electronics", 1),
        (4, "Standing Desk", "Furniture", 2),
        (5, "Monitor 4K", "Electronics", 1),
        (6, "Desk Lamp", "Furniture", 3),
        (7, "Keyboard Mechanical", "Electronics", 1),
        (8, "Webcam HD", "Electronics", 3),
    ];
    
    for (id, name, category, supplier) in products {
        let values = vec![vec![
            Value::Int32(id),
            Value::String(name.to_string()),
            Value::String(category.to_string()),
            Value::Int32(supplier),
        ]];
        let mut insert = InsertExecutor::new("products".to_string(), values, context.clone());
        insert.init()?;
        insert.next()?;
    }

    // Insert orders
    let orders = vec![
        (1, 1, "2024-01-15", 4500, "Completed"),
        (2, 2, "2024-01-20", 1200, "Completed"),
        (3, 3, "2024-02-01", 8900, "Completed"),
        (4, 1, "2024-02-15", 2300, "Completed"),
        (5, 4, "2024-02-28", 650, "Completed"),
        (6, 5, "2024-03-10", 12000, "Completed"),
        (7, 2, "2024-03-15", 1800, "Pending"),
        (8, 3, "2024-03-20", 5600, "Completed"),
        (9, 1, "2024-03-25", 3200, "Completed"),
        (10, 5, "2024-03-30", 7800, "Completed"),
    ];
    
    for (id, customer, date, amount, status) in orders {
        let values = vec![vec![
            Value::Int32(id),
            Value::Int32(customer),
            Value::String(date.to_string()),
            Value::Int32(amount),
            Value::String(status.to_string()),
        ]];
        let mut insert = InsertExecutor::new("orders".to_string(), values, context.clone());
        insert.init()?;
        insert.next()?;
    }

    // Insert order items (simplified)
    let order_items = vec![
        // Order 1
        (1, 1, 1, 2, 1500),
        (2, 1, 3, 3, 50),
        // Order 2
        (3, 2, 2, 2, 300),
        (4, 2, 6, 4, 75),
        // Order 3
        (5, 3, 1, 3, 1500),
        (6, 3, 5, 2, 800),
        // Add more items as needed...
    ];
    
    for (id, order, product, qty, price) in order_items {
        let values = vec![vec![
            Value::Int32(id),
            Value::Int32(order),
            Value::Int32(product),
            Value::Int32(qty),
            Value::Int32(price),
        ]];
        let mut insert = InsertExecutor::new("order_items".to_string(), values, context.clone());
        insert.init()?;
        insert.next()?;
    }
    
    println!("Sample data loaded successfully!");
    println!();

    // Analysis 1: Top selling products by quantity
    println!("=== Analysis 1: Top Products by Quantity Sold ===");
    {
        // Join order_items with products
        let item_scan = Box::new(SeqScanExecutor::new(
            "order_items".to_string(),
            context.clone(),
        ));

        let product_scan = Box::new(SeqScanExecutor::new(
            "products".to_string(),
            context.clone(),
        ));

        // Join condition: order_items.product_id = products.product_id
        let join_condition = Expression::binary(
            BinaryOperator::Equal,
            Expression::column(2), // order_items.product_id
            Expression::column(5), // products.product_id (offset by 5 in joined tuple)
        );

        let join_executor = NestedLoopJoinExecutor::new(
            item_scan,
            product_scan,
            join_condition,
        );

        // Group by product_name (column 6), category (column 7)
        let group_by = GroupByClause::new(vec![6, 7]);

        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(3), // quantity
                "units_sold".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None, // COUNT(*)
                "order_count".to_string(),
            ),
        ];

        let agg_executor = HashAggregateExecutor::new(
            Box::new(join_executor),
            group_by,
            aggregates,
        )?;

        // Sort by units sold descending
        let sort_criteria = vec![SortCriteria {
            column_idx: 2, // units_sold column
            order: SortOrder::Desc,
            null_order: NullOrder::NullsLast,
        }];

        let sort_executor = SortExecutor::new(
            Box::new(agg_executor),
            sort_criteria,
        );

        // Limit to top 5
        let mut limit_executor = LimitExecutor::new(
            Box::new(sort_executor),
            5,
        );

        limit_executor.init()?;
        println!("{:<25} {:<15} {:<15} {:<15}", "Product", "Category", "Units Sold", "Orders");
        println!("{:-<70}", "");
        while let Some(tuple) = limit_executor.next()? {
            let values = tuple.get_values();
            println!("{:<25} {:<15} {:<15} {:<15}", 
                values[0],  // product_name
                values[1],  // category
                values[2],  // units sold
                values[3]   // order count
            );
        }
    }
    println!();

    // Analysis 2: Customer segment analysis
    println!("=== Analysis 2: Revenue by Customer Segment ===");
    {
        // Join orders with customers
        let order_scan = Box::new(SeqScanExecutor::new(
            "orders".to_string(),
            context.clone(),
        ));

        let customer_scan = Box::new(SeqScanExecutor::new(
            "customers".to_string(),
            context.clone(),
        ));

        // Join condition: orders.customer_id = customers.customer_id
        let join_condition = Expression::binary(
            BinaryOperator::Equal,
            Expression::column(1), // orders.customer_id
            Expression::column(5), // customers.customer_id (offset by 5)
        );

        let join_executor = NestedLoopJoinExecutor::new(
            order_scan,
            customer_scan,
            join_condition,
        );

        // Filter for completed orders only
        let status_filter = Expression::binary(
            BinaryOperator::Equal,
            Expression::column(4), // status
            Expression::literal(Value::String("Completed".to_string())),
        );

        let filter_executor = FilterExecutor::new(
            Box::new(join_executor),
            status_filter,
        );

        // Group by segment (column 8), region (column 7)
        let group_by = GroupByClause::new(vec![8, 7]);

        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None,
                "order_count".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Sum,
                Some(3), // total_amount
                "total_revenue".to_string(),
            ),
            AggregateSpec::with_alias(
                AggregateFunction::Avg,
                Some(3), // total_amount
                "avg_order_value".to_string(),
            ),
        ];

        let mut agg_executor = HashAggregateExecutor::new(
            Box::new(filter_executor),
            group_by,
            aggregates,
        )?;

        agg_executor.init()?;
        println!("{:<15} {:<10} {:<12} {:<15} {:<20}", "Segment", "Region", "Orders", "Total Revenue", "Avg Order Value");
        println!("{:-<72}", "");
        while let Some(tuple) = agg_executor.next()? {
            let values = tuple.get_values();
            println!("{:<15} {:<10} {:<12} ${:<14} ${:<19}", 
                values[0],  // segment
                values[1],  // region
                values[2],  // order count
                values[3],  // total revenue
                values[4]   // avg order value
            );
        }
    }
    println!();

    // Analysis 3: Category performance ranking
    println!("=== Analysis 3: Product Category Performance ===");
    {
        let product_scan = Box::new(SeqScanExecutor::new(
            "products".to_string(),
            context.clone(),
        ));

        // Group by category
        let group_by = GroupByClause::new(vec![2]); // category column

        let aggregates = vec![
            AggregateSpec::with_alias(
                AggregateFunction::Count,
                None,
                "product_count".to_string(),
            ),
        ];

        let mut agg_executor = HashAggregateExecutor::new(
            product_scan,
            group_by,
            aggregates,
        )?;

        agg_executor.init()?;
        println!("{:<15} {:<15}", "Category", "Product Count");
        println!("{:-<30}", "");
        
        while let Some(tuple) = agg_executor.next()? {
            let values = tuple.get_values();
            println!("{:<15} {:<15}", 
                values[0],  // category
                values[1]   // product count
            );
        }
    }

    // Clean up
    std::fs::remove_file(db_path)?;

    println!("\n=== Sales analytics completed successfully! ===");
    println!("\nThis example demonstrated:");
    println!("- Complex joins between multiple tables");
    println!("- Aggregations with GROUP BY on joined data");
    println!("- Filtering before and after aggregation");
    println!("- Sorting and limiting results");
    println!("- Real-world analytics patterns");

    Ok(())
}