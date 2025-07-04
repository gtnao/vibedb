#[cfg(test)]
use crate::access::Value;
use crate::access::{DataType, TableHeap};
use crate::catalog::Catalog;
use crate::storage::buffer::lru::LruReplacer;
use crate::storage::buffer::BufferPoolManager;
use crate::storage::disk::PageManager;
use crate::storage::wal::manager::WalManager;
use crate::transaction::manager::TransactionManager;
use crate::concurrency::mvcc::MVCCManager;
use anyhow::{bail, Result};
use std::path::Path;
use std::sync::Arc;

/// High-level database interface that integrates all layers
pub struct Database {
    pub buffer_pool: Arc<BufferPoolManager>,
    pub catalog: Arc<Catalog>,
    pub wal_manager: Arc<WalManager>,
    pub transaction_manager: Arc<TransactionManager>,
    pub mvcc_manager: Arc<MVCCManager>,
}

impl Database {
    /// Create a new database at the specified path
    pub fn create(path: &Path) -> Result<Self> {
        // Check if database already exists
        if path.exists() {
            bail!("Database file already exists at {:?}", path);
        }

        // Create new database file
        let page_manager = PageManager::create(path)?;
        let replacer = Box::new(LruReplacer::new(64)); // Default 64 page buffer pool
        let buffer_pool = Arc::new(BufferPoolManager::new(page_manager, replacer, 64));

        // Initialize catalog
        let catalog = Arc::new(Catalog::initialize((*buffer_pool).clone())?);
        
        // Initialize WAL manager
        let wal_path = path.with_extension("wal");
        let wal_manager = Arc::new(WalManager::create(&wal_path)?);
        
        // Initialize transaction manager
        let transaction_manager = Arc::new(TransactionManager::new());
        
        // Initialize MVCC manager
        let mvcc_manager = Arc::new(MVCCManager::new());

        Ok(Self {
            buffer_pool,
            catalog,
            wal_manager,
            transaction_manager,
            mvcc_manager,
        })
    }

    /// Open an existing database
    pub fn open(path: &Path) -> Result<Self> {
        // Check if database exists
        if !path.exists() {
            bail!("Database file does not exist at {:?}", path);
        }

        // Open existing database file
        let page_manager = PageManager::open(path)?;
        let replacer = Box::new(LruReplacer::new(64));
        let buffer_pool = Arc::new(BufferPoolManager::new(page_manager, replacer, 64));

        // Open catalog
        let catalog = Arc::new(Catalog::open((*buffer_pool).clone())?);
        
        // Open WAL manager
        let wal_path = path.with_extension("wal");
        let wal_manager = Arc::new(WalManager::open(&wal_path)?);
        
        // Initialize transaction manager
        let transaction_manager = Arc::new(TransactionManager::new());
        
        // Initialize MVCC manager
        let mvcc_manager = Arc::new(MVCCManager::new());
        
        // TODO: Perform recovery using WAL
        // recovery::recover(&wal_manager, &buffer_pool)?;

        Ok(Self {
            buffer_pool,
            catalog,
            wal_manager,
            transaction_manager,
            mvcc_manager,
        })
    }

    /// Create a new table
    pub fn create_table(&mut self, table_name: &str) -> Result<()> {
        Arc::get_mut(&mut self.catalog)
            .ok_or_else(|| anyhow::anyhow!("Cannot mutate catalog"))?
            .create_table(table_name)?;
        Ok(())
    }

    /// Create a new table with column definitions
    pub fn create_table_with_columns(
        &mut self,
        table_name: &str,
        columns: Vec<(&str, DataType)>,
    ) -> Result<()> {
        Arc::get_mut(&mut self.catalog)
            .ok_or_else(|| anyhow::anyhow!("Cannot mutate catalog"))?
            .create_table_with_columns(table_name, columns)?;
        Ok(())
    }

    /// Open a table for reading/writing
    pub fn open_table(&self, table_name: &str) -> Result<TableHeap> {
        let table_info = self
            .catalog
            .get_table(table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

        Ok(TableHeap::with_first_page(
            (*self.buffer_pool).clone(),
            table_info.table_id,
            table_info.first_page_id,
        ))
    }

    /// List all tables in the database
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let tables = self.catalog.list_tables()?;
        Ok(tables.into_iter().map(|t| t.table_name).collect())
    }

    /// Flush all dirty pages to disk
    pub fn flush(&self) -> Result<()> {
        self.buffer_pool.flush_all()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Best effort flush on drop
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_database() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");

        // Create new database
        let db = Database::create(&db_path)?;
        assert!(db_path.exists());

        // Should contain system catalogs
        let tables = db.list_tables()?;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&"pg_tables".to_string()));
        assert!(tables.contains(&"pg_attribute".to_string()));
        assert!(tables.contains(&"pg_index".to_string()));

        Ok(())
    }

    #[test]
    fn test_create_duplicate_database() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");

        // Create first database
        let _db = Database::create(&db_path)?;

        // Try to create again - should fail
        let result = Database::create(&db_path);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_open_nonexistent_database() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("nonexistent.db");

        let result = Database::open(&db_path);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("does not exist"));

        Ok(())
    }

    #[test]
    fn test_create_and_open_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");

        // Create database and table
        {
            let mut db = Database::create(&db_path)?;
            db.create_table("users")?;
            db.create_table("products")?;
        }

        // Reopen and verify tables
        {
            let db = Database::open(&db_path)?;
            let tables = db.list_tables()?;
            assert_eq!(tables.len(), 5); // pg_tables + pg_attribute + pg_index + users + products
            assert!(tables.contains(&"users".to_string()));
            assert!(tables.contains(&"products".to_string()));

            // Open table
            let _users_table = db.open_table("users")?;
        }

        Ok(())
    }

    #[test]
    fn test_open_nonexistent_table() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");

        let db = Database::create(&db_path)?;
        let result = db.open_table("nonexistent");
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("does not exist"));

        Ok(())
    }

    #[test]
    fn test_table_operations() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");

        let mut db = Database::create(&db_path)?;
        db.create_table("test_table")?;

        // Insert data
        let mut table = db.open_table("test_table")?;
        let data1 = b"Hello, World!";
        let data2 = b"Another tuple";

        let tid1 = table.insert(data1)?;
        let tid2 = table.insert(data2)?;

        // Read data back
        let tuple1 = table.get(tid1)?.expect("Tuple should exist");
        let tuple2 = table.get(tid2)?.expect("Tuple should exist");

        assert_eq!(tuple1.data, data1);
        assert_eq!(tuple2.data, data2);

        Ok(())
    }

    #[test]
    fn test_persistence() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");

        let tid = {
            // Create database and insert data
            let mut db = Database::create(&db_path)?;
            db.create_table("persistent_table")?;

            let mut table = db.open_table("persistent_table")?;
            let tid = table.insert(b"Persistent data")?;

            // Explicit flush
            drop(table);
            db.flush()?;

            tid
        };

        // Reopen and verify data
        {
            let db = Database::open(&db_path)?;
            let table = db.open_table("persistent_table")?;

            let tuple = table.get(tid)?.expect("Data should persist");
            assert_eq!(tuple.data, b"Persistent data");
        }

        Ok(())
    }

    #[test]
    fn test_schema_integration() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");

        let mut db = Database::create(&db_path)?;

        // Create table with schema
        db.create_table_with_columns(
            "users",
            vec![
                ("id", DataType::Int32),
                ("name", DataType::Varchar),
                ("age", DataType::Int32),
                ("active", DataType::Boolean),
            ],
        )?;

        // Get the schema for the table
        let table_info = db.catalog.get_table("users")?.unwrap();
        let columns = db.catalog.get_table_columns(table_info.table_id)?;
        let schema: Vec<DataType> = columns.iter().map(|c| c.column_type).collect();

        // Insert typed data
        let mut table = db.open_table("users")?;
        let tid1 = table.insert_values(
            &[
                Value::Int32(1),
                Value::String("Alice".to_string()),
                Value::Int32(25),
                Value::Boolean(true),
            ],
            &schema,
        )?;

        let tid2 = table.insert_values(
            &[
                Value::Int32(2),
                Value::String("Bob".to_string()),
                Value::Int32(30),
                Value::Boolean(false),
            ],
            &schema,
        )?;

        // Read data back
        let tuple1 = table.get(tid1)?.expect("Tuple should exist");
        let values1 = crate::access::value::deserialize_values(&tuple1.data, &schema)?;
        assert_eq!(values1.len(), 4);
        assert_eq!(values1[0], Value::Int32(1));
        assert_eq!(values1[1], Value::String("Alice".to_string()));
        assert_eq!(values1[2], Value::Int32(25));
        assert_eq!(values1[3], Value::Boolean(true));

        let tuple2 = table.get(tid2)?.expect("Tuple should exist");
        let values2 = crate::access::value::deserialize_values(&tuple2.data, &schema)?;
        assert_eq!(values2[0], Value::Int32(2));
        assert_eq!(values2[1], Value::String("Bob".to_string()));

        Ok(())
    }
}
