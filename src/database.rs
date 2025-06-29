use crate::access::TableHeap;
use crate::catalog::Catalog;
use crate::storage::buffer::BufferPoolManager;
use crate::storage::buffer::lru::LruReplacer;
use crate::storage::disk::PageManager;
use anyhow::{Result, bail};
use std::path::Path;

/// High-level database interface that integrates all layers
pub struct Database {
    buffer_pool: BufferPoolManager,
    catalog: Catalog,
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
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 64);

        // Initialize catalog
        let catalog = Catalog::initialize(buffer_pool.clone())?;

        Ok(Self {
            buffer_pool,
            catalog,
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
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 64);

        // Open catalog
        let catalog = Catalog::open(buffer_pool.clone())?;

        Ok(Self {
            buffer_pool,
            catalog,
        })
    }

    /// Create a new table
    pub fn create_table(&mut self, table_name: &str) -> Result<()> {
        self.catalog.create_table(table_name)?;
        Ok(())
    }

    /// Open a table for reading/writing
    pub fn open_table(&self, table_name: &str) -> Result<TableHeap> {
        let table_info = self
            .catalog
            .get_table(table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

        Ok(TableHeap::with_first_page(
            self.buffer_pool.clone(),
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
    pub fn flush(&mut self) -> Result<()> {
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

        // Should contain system catalog
        let tables = db.list_tables()?;
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&"pg_tables".to_string()));

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
            assert_eq!(tables.len(), 3); // pg_tables + users + products
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
}
