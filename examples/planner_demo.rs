//! Demo showing how the planner converts SQL AST to physical plans

use anyhow::Result;
use std::sync::Arc;
use tempfile::tempdir;
use vibedb::access::Value;
use vibedb::catalog::Catalog;
use vibedb::planner::Planner;
use vibedb::sql::ast::{
    ColumnDefinition, CreateTableStatement, DataType, Expression, InsertStatement, OrderByItem,
    OrderDirection, SelectItem, SelectStatement, Statement, TableReference,
};
use vibedb::storage::buffer::{lru::LruReplacer, BufferPoolManager};
use vibedb::storage::disk::PageManager;

fn main() -> Result<()> {
    // Create database components
    let dir = tempdir()?;
    let file_path = dir.path().join("demo.db");
    let page_manager = PageManager::create(&file_path)?;
    let replacer = Box::new(LruReplacer::new(100));
    let buffer_pool = Arc::new(BufferPoolManager::new(page_manager, replacer, 100));
    let catalog = Arc::new(Catalog::initialize((*buffer_pool).clone())?);

    // Create planner
    let planner = Planner::new(catalog);

    println!("=== VibeDB Planner Demo ===\n");

    // Demo 1: Simple SELECT query
    demo_simple_select(&planner)?;

    // Demo 2: Complex SELECT with JOIN
    demo_complex_select(&planner)?;

    // Demo 3: INSERT statement
    demo_insert(&planner)?;

    // Demo 4: CREATE TABLE
    demo_create_table(&planner)?;

    Ok(())
}

fn demo_simple_select(planner: &Planner) -> Result<()> {
    println!("1. Simple SELECT Query");
    println!("   SQL: SELECT id, name FROM users WHERE active = true ORDER BY id DESC LIMIT 10");

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        projections: vec![
            SelectItem::Expression(Expression::column("id"), None),
            SelectItem::Expression(Expression::column("name"), None),
        ],
        from: Some(TableReference {
            name: "users".to_string(),
            alias: None,
        }),
        joins: vec![],
        where_clause: Some(
            Expression::column("active").eq(Expression::literal(Value::Boolean(true))),
        ),
        group_by: vec![],
        having: None,
        order_by: vec![OrderByItem {
            expression: Expression::column("id"),
            direction: OrderDirection::Desc,
        }],
        limit: Some(Expression::literal(Value::Int32(10))),
        offset: None,
    });

    let logical = planner.statement_to_logical(stmt.clone())?;
    println!("\n   Logical Plan:");
    println!("   {:?}", logical);

    let physical = planner.plan(stmt)?;
    println!("\n   Physical Plan:");
    println!("{}", indent_string(&physical.explain(), 3));

    println!();
    Ok(())
}

fn demo_complex_select(planner: &Planner) -> Result<()> {
    use vibedb::sql::ast::{Join, JoinType};

    println!("2. Complex SELECT with JOIN");
    println!("   SQL: SELECT u.name, COUNT(o.id) as order_count");
    println!("        FROM users u");
    println!("        INNER JOIN orders o ON u.id = o.user_id");
    println!("        WHERE u.active = true");
    println!("        GROUP BY u.name");
    println!("        HAVING COUNT(o.id) > 5");
    println!("        ORDER BY order_count DESC");

    let stmt = Statement::Select(SelectStatement {
        distinct: false,
        projections: vec![
            SelectItem::Expression(
                Expression::QualifiedColumn("u".to_string(), "name".to_string()),
                None,
            ),
            SelectItem::Expression(
                Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expression::QualifiedColumn(
                        "o".to_string(),
                        "id".to_string(),
                    )],
                    distinct: false,
                },
                Some("order_count".to_string()),
            ),
        ],
        from: Some(TableReference {
            name: "users".to_string(),
            alias: Some("u".to_string()),
        }),
        joins: vec![Join {
            join_type: JoinType::Inner,
            table: TableReference {
                name: "orders".to_string(),
                alias: Some("o".to_string()),
            },
            on: Some(
                Expression::QualifiedColumn("u".to_string(), "id".to_string()).eq(
                    Expression::QualifiedColumn("o".to_string(), "user_id".to_string()),
                ),
            ),
            using: vec![],
        }],
        where_clause: Some(
            Expression::QualifiedColumn("u".to_string(), "active".to_string())
                .eq(Expression::literal(Value::Boolean(true))),
        ),
        group_by: vec![Expression::QualifiedColumn(
            "u".to_string(),
            "name".to_string(),
        )],
        having: Some(
            Expression::Function {
                name: "COUNT".to_string(),
                args: vec![Expression::QualifiedColumn(
                    "o".to_string(),
                    "id".to_string(),
                )],
                distinct: false,
            }
            .gt(Expression::literal(Value::Int32(5))),
        ),
        order_by: vec![OrderByItem {
            expression: Expression::column("order_count"),
            direction: OrderDirection::Desc,
        }],
        limit: None,
        offset: None,
    });

    let physical = planner.plan(stmt)?;
    println!("\n   Physical Plan:");
    println!("{}", indent_string(&physical.explain(), 3));

    println!();
    Ok(())
}

fn demo_insert(planner: &Planner) -> Result<()> {
    println!("3. INSERT Statement");
    println!("   SQL: INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)");

    let stmt = Statement::Insert(InsertStatement {
        table_name: "users".to_string(),
        columns: Some(vec![
            "id".to_string(),
            "name".to_string(),
            "active".to_string(),
        ]),
        values: vec![vec![
            Expression::literal(Value::Int32(1)),
            Expression::literal(Value::String("Alice".to_string())),
            Expression::literal(Value::Boolean(true)),
        ]],
    });

    let physical = planner.plan(stmt)?;
    println!("\n   Physical Plan:");
    println!("{}", indent_string(&physical.explain(), 3));

    println!();
    Ok(())
}

fn demo_create_table(planner: &Planner) -> Result<()> {
    use vibedb::sql::ast::{ColumnConstraint, TableConstraint};

    println!("4. CREATE TABLE Statement");
    println!("   SQL: CREATE TABLE products (");
    println!("          id INT NOT NULL,");
    println!("          name VARCHAR(255) NOT NULL,");
    println!("          price DECIMAL(10,2),");
    println!("          PRIMARY KEY (id)");
    println!("        )");

    let stmt = Statement::CreateTable(CreateTableStatement {
        table_name: "products".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                default: None,
                constraints: vec![ColumnConstraint::NotNull],
            },
            ColumnDefinition {
                name: "name".to_string(),
                data_type: DataType::Varchar(Some(255)),
                nullable: false,
                default: None,
                constraints: vec![ColumnConstraint::NotNull],
            },
            ColumnDefinition {
                name: "price".to_string(),
                data_type: DataType::Decimal(Some(10), Some(2)),
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        constraints: vec![TableConstraint::PrimaryKey(vec!["id".to_string()])],
    });

    let physical = planner.plan(stmt)?;
    println!("\n   Physical Plan:");
    println!("{}", indent_string(&physical.explain(), 3));

    println!();
    Ok(())
}

fn indent_string(s: &str, spaces: usize) -> String {
    let indent = " ".repeat(spaces);
    s.lines()
        .map(|line| format!("{}{}", indent, line))
        .collect::<Vec<_>>()
        .join("\n")
}
