//! VibeDB Server - PostgreSQL-compatible database server

use anyhow::{Context, Result};
use clap::Parser as ClapParser;
use std::path::PathBuf;
use std::sync::Arc;
use vibedb::database::Database;
use vibedb::network::server::Server;
use vibedb::transaction::TransactionManager;

/// VibeDB Server - A PostgreSQL-compatible database server
#[derive(ClapParser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "5432")]
    port: u16,

    /// Host to bind to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Data directory
    #[arg(short = 'D', long, default_value = "./vibedb_data")]
    data_dir: PathBuf,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,

    /// Maximum concurrent connections
    #[arg(short = 'c', long, default_value = "100")]
    max_connections: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Set up logging
    let log_level = if args.debug { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    println!(
        r#"
    __     ___ _          ____  ____  
    \ \   / (_) |__   ___|  _ \| __ ) 
     \ \ / /| | '_ \ / _ \ | | |  _ \ 
      \ V / | | |_) |  __/ |_| | |_) |
       \_/  |_|_.__/ \___|____/|____/ 
    
    VibeDB Server v0.1.0
    PostgreSQL-compatible database server
    "#
    );

    // Create data directory if it doesn't exist
    std::fs::create_dir_all(&args.data_dir).context("Failed to create data directory")?;

    // Initialize or open the database
    let db_path = args.data_dir.join("vibedb.db");
    println!("📁 Data directory: {}", args.data_dir.display());

    let database = if db_path.exists() {
        println!("📂 Opening existing database at: {}", db_path.display());
        let db = Arc::new(Database::open(&db_path).context("Failed to open database")?);
        println!("✅ Database opened successfully");
        db
    } else {
        println!("🆕 Creating new database at: {}", db_path.display());
        let mut db = Database::create(&db_path).context("Failed to create database")?;
        create_default_schema(&mut db)?;
        Arc::new(db)
    };

    // Create transaction manager
    let transaction_manager = Arc::new(TransactionManager::new());
    println!("🔀 Created transaction manager");

    // Create and start the server
    let server = Server::new(database, transaction_manager, args.max_connections);
    println!("🔧 Created server instance");

    let addr = Some(std::net::SocketAddr::from((
        args.host
            .parse::<std::net::IpAddr>()
            .context("Invalid host address")?,
        args.port,
    )));
    println!("📍 Parsed address: {:?}", addr);

    println!("🚀 Server configuration:");
    println!("   - Host: {}", args.host);
    println!("   - Port: {}", args.port);
    println!("   - Max connections: {}", args.max_connections);
    println!();
    println!("📡 Ready to accept connections");
    println!();
    println!(
        "Connect with: psql -h {} -p {} -U vibedb -d vibedb",
        args.host, args.port
    );
    println!("(Any username/password will be accepted)");
    println!();

    // Handle shutdown gracefully
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run(addr).await {
            eprintln!("❌ Server error: {}", e);
        }
    });

    // Wait for Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for Ctrl+C")?;

    println!("\n🛑 Shutting down server...");

    // Note: In a real implementation, we would properly shut down the server
    // For now, we just exit
    server_handle.abort();

    println!("👋 Goodbye!");

    Ok(())
}

/// Creates default schema for demonstration
fn create_default_schema(database: &mut Database) -> Result<()> {
    use vibedb::access::DataType;

    println!("📊 Creating default schema...");

    // Create a sample users table
    let users_columns = vec![
        ("id", DataType::Int32),
        ("name", DataType::Varchar),
        ("email", DataType::Varchar),
        ("active", DataType::Boolean),
    ];

    database
        .create_table_with_columns("users", users_columns)
        .context("Failed to create users table")?;

    // Create a sample products table
    let products_columns = vec![
        ("id", DataType::Int32),
        ("name", DataType::Varchar),
        ("price", DataType::Int32),
        ("in_stock", DataType::Boolean),
    ];

    database
        .create_table_with_columns("products", products_columns)
        .context("Failed to create products table")?;

    println!("✅ Default schema created successfully");
    println!("   - Table 'users' (id, name, email, active)");
    println!("   - Table 'products' (id, name, price, in_stock)");

    Ok(())
}
