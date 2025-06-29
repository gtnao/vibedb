use crate::access::{DataType, TableHeap};
use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageId;
use anyhow::{Result, bail};
use std::collections::HashMap;
use std::sync::RwLock;

pub type TableId = u32;

pub const CATALOG_TABLE_ID: TableId = 1;
pub const CATALOG_FIRST_PAGE: PageId = PageId(0);
pub const CATALOG_TABLE_NAME: &str = "pg_tables";

pub const CATALOG_ATTR_TABLE_ID: TableId = 2;
pub const CATALOG_ATTR_TABLE_NAME: &str = "pg_attribute";

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_id: TableId,
    pub table_name: String,
    pub first_page_id: PageId,
}

impl TableInfo {
    fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.table_id.to_le_bytes());
        data.extend_from_slice(&(self.table_name.len() as u32).to_le_bytes());
        data.extend_from_slice(self.table_name.as_bytes());
        data.extend_from_slice(&self.first_page_id.0.to_le_bytes());
        data
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            bail!("Invalid table info data: too short");
        }

        let mut offset = 0;

        // Read table_id
        let table_id = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        // Read table_name length
        let name_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        // Read table_name
        if data.len() < offset + name_len + 4 {
            bail!("Invalid table info data: name too long");
        }
        let table_name = String::from_utf8(data[offset..offset + name_len].to_vec())?;
        offset += name_len;

        // Read first_page_id
        let first_page_id = PageId(u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]));

        Ok(TableInfo {
            table_id,
            table_name,
            first_page_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub column_name: String,
    pub column_type: DataType,
    pub column_order: u32,
}

impl ColumnInfo {
    fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&(self.column_name.len() as u32).to_le_bytes());
        data.extend_from_slice(self.column_name.as_bytes());
        data.push(self.column_type as u8);
        data.extend_from_slice(&self.column_order.to_le_bytes());
        data
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 9 {
            bail!("Invalid column info data: too short");
        }

        let mut offset = 0;

        // Read column_name length
        let name_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        // Read column_name
        if data.len() < offset + name_len + 5 {
            bail!("Invalid column info data: name too long");
        }
        let column_name = String::from_utf8(data[offset..offset + name_len].to_vec())?;
        offset += name_len;

        // Read column_type
        let column_type = DataType::from_u8(data[offset])?;
        offset += 1;

        // Read column_order
        let column_order = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        Ok(ColumnInfo {
            column_name,
            column_type,
            column_order,
        })
    }
}

/// Represents a pg_attribute row
struct AttributeRow {
    table_id: TableId,
    column_info: ColumnInfo,
}

impl AttributeRow {
    fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.table_id.to_le_bytes());
        data.extend_from_slice(&self.column_info.serialize());
        data
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            bail!("Invalid attribute row data: too short");
        }

        let table_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

        let column_info = ColumnInfo::deserialize(&data[4..])?;

        Ok(AttributeRow {
            table_id,
            column_info,
        })
    }
}

pub struct Catalog {
    buffer_pool: BufferPoolManager,
    catalog_heap: TableHeap,
    attribute_heap: Option<TableHeap>,
    table_cache: RwLock<HashMap<String, TableInfo>>,
    column_cache: RwLock<HashMap<TableId, Vec<ColumnInfo>>>,
    next_table_id: RwLock<TableId>,
}

impl Catalog {
    /// Initialize a new database with system catalog
    pub fn initialize(buffer_pool: BufferPoolManager) -> Result<Self> {
        // Create the first page for catalog table
        let (page_id, mut guard) = buffer_pool.new_page()?;
        if page_id != CATALOG_FIRST_PAGE {
            bail!("Expected first page to be PageId(0), got {:?}", page_id);
        }

        // Initialize the page as a heap page
        let _heap_page = crate::storage::page::HeapPage::new(&mut guard, page_id);

        // Create catalog heap with the first page
        let mut catalog_heap =
            TableHeap::with_first_page(buffer_pool.clone(), CATALOG_TABLE_ID, CATALOG_FIRST_PAGE);

        // Insert catalog table's own entry
        let catalog_info = TableInfo {
            table_id: CATALOG_TABLE_ID,
            table_name: CATALOG_TABLE_NAME.to_string(),
            first_page_id: CATALOG_FIRST_PAGE,
        };

        drop(guard); // Release the page guard before inserting
        catalog_heap.insert(&catalog_info.serialize())?;

        // Create pg_attribute table
        let (attr_page_id, mut attr_guard) = buffer_pool.new_page()?;
        let _attr_heap_page = crate::storage::page::HeapPage::new(&mut attr_guard, attr_page_id);
        drop(attr_guard);

        let attr_table_info = TableInfo {
            table_id: CATALOG_ATTR_TABLE_ID,
            table_name: CATALOG_ATTR_TABLE_NAME.to_string(),
            first_page_id: attr_page_id,
        };
        catalog_heap.insert(&attr_table_info.serialize())?;

        let mut attribute_heap =
            TableHeap::with_first_page(buffer_pool.clone(), CATALOG_ATTR_TABLE_ID, attr_page_id);

        // Insert system table schemas
        // pg_tables columns
        let pg_tables_columns = vec![
            AttributeRow {
                table_id: CATALOG_TABLE_ID,
                column_info: ColumnInfo {
                    column_name: "table_id".to_string(),
                    column_type: DataType::Int32,
                    column_order: 1,
                },
            },
            AttributeRow {
                table_id: CATALOG_TABLE_ID,
                column_info: ColumnInfo {
                    column_name: "table_name".to_string(),
                    column_type: DataType::Varchar,
                    column_order: 2,
                },
            },
            AttributeRow {
                table_id: CATALOG_TABLE_ID,
                column_info: ColumnInfo {
                    column_name: "first_page_id".to_string(),
                    column_type: DataType::Int32,
                    column_order: 3,
                },
            },
        ];

        for attr_row in pg_tables_columns {
            attribute_heap.insert(&attr_row.serialize())?;
        }

        // pg_attribute columns
        let pg_attribute_columns = vec![
            AttributeRow {
                table_id: CATALOG_ATTR_TABLE_ID,
                column_info: ColumnInfo {
                    column_name: "table_id".to_string(),
                    column_type: DataType::Int32,
                    column_order: 1,
                },
            },
            AttributeRow {
                table_id: CATALOG_ATTR_TABLE_ID,
                column_info: ColumnInfo {
                    column_name: "column_name".to_string(),
                    column_type: DataType::Varchar,
                    column_order: 2,
                },
            },
            AttributeRow {
                table_id: CATALOG_ATTR_TABLE_ID,
                column_info: ColumnInfo {
                    column_name: "column_type".to_string(),
                    column_type: DataType::Int32,
                    column_order: 3,
                },
            },
            AttributeRow {
                table_id: CATALOG_ATTR_TABLE_ID,
                column_info: ColumnInfo {
                    column_name: "column_order".to_string(),
                    column_type: DataType::Int32,
                    column_order: 4,
                },
            },
        ];

        for attr_row in pg_attribute_columns {
            attribute_heap.insert(&attr_row.serialize())?;
        }

        // Initialize caches
        let mut initial_table_cache = HashMap::new();
        initial_table_cache.insert(CATALOG_TABLE_NAME.to_string(), catalog_info);
        initial_table_cache.insert(CATALOG_ATTR_TABLE_NAME.to_string(), attr_table_info);

        Ok(Self {
            buffer_pool,
            catalog_heap,
            attribute_heap: Some(attribute_heap),
            table_cache: RwLock::new(initial_table_cache),
            column_cache: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(CATALOG_ATTR_TABLE_ID + 1),
        })
    }

    /// Open an existing database
    pub fn open(buffer_pool: BufferPoolManager) -> Result<Self> {
        let catalog_heap =
            TableHeap::with_first_page(buffer_pool.clone(), CATALOG_TABLE_ID, CATALOG_FIRST_PAGE);

        let mut table_cache = HashMap::new();
        let mut max_table_id = CATALOG_TABLE_ID;

        // Scan catalog table to load all table info
        let mut current_page_id = Some(CATALOG_FIRST_PAGE);

        while let Some(page_id) = current_page_id {
            match buffer_pool.fetch_page(page_id) {
                Ok(guard) => {
                    // Create a temporary HeapPage view
                    // SAFETY: Same safety guarantees as in TableHeap::get()
                    // The guard ensures the page stays in memory during this operation
                    let page_data = unsafe {
                        std::slice::from_raw_parts_mut(
                            guard.as_ptr() as *mut u8,
                            crate::storage::PAGE_SIZE,
                        )
                    };
                    let page_array = unsafe {
                        &mut *(page_data.as_mut_ptr() as *mut [u8; crate::storage::PAGE_SIZE])
                    };
                    let heap_page = crate::storage::page::HeapPage::from_data(page_array);

                    // Scan all tuples in this page
                    let tuple_count = heap_page.get_tuple_count();
                    for slot_id in 0..tuple_count {
                        match heap_page.get_tuple(slot_id) {
                            Ok(data) => {
                                if let Ok(table_info) = TableInfo::deserialize(data) {
                                    max_table_id = max_table_id.max(table_info.table_id);
                                    table_cache.insert(table_info.table_name.clone(), table_info);
                                }
                            }
                            Err(_) => {
                                // Either invalid slot or deleted tuple
                                continue;
                            }
                        }
                    }

                    // Get next page
                    current_page_id = heap_page.get_next_page_id();
                }
                Err(_) => break,
            }
        }

        // Find pg_attribute table
        let attr_table_info = table_cache.get(CATALOG_ATTR_TABLE_NAME).cloned();
        let attribute_heap = attr_table_info.map(|info| {
            TableHeap::with_first_page(
                buffer_pool.clone(),
                CATALOG_ATTR_TABLE_ID,
                info.first_page_id,
            )
        });

        Ok(Self {
            buffer_pool,
            catalog_heap,
            attribute_heap,
            table_cache: RwLock::new(table_cache),
            column_cache: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(max_table_id + 1),
        })
    }

    /// Get table information by name
    pub fn get_table(&self, name: &str) -> Result<Option<TableInfo>> {
        // Check cache first
        {
            let cache = self.table_cache.read().unwrap();
            if let Some(info) = cache.get(name) {
                return Ok(Some(info.clone()));
            }
        }

        // Not in cache, scan catalog table
        let mut current_page_id = Some(CATALOG_FIRST_PAGE);

        while let Some(page_id) = current_page_id {
            match self.buffer_pool.fetch_page(page_id) {
                Ok(guard) => {
                    // Create a temporary HeapPage view
                    // SAFETY: Same safety guarantees as in TableHeap::get()
                    // The guard ensures the page stays in memory during this operation
                    let page_data = unsafe {
                        std::slice::from_raw_parts_mut(
                            guard.as_ptr() as *mut u8,
                            crate::storage::PAGE_SIZE,
                        )
                    };
                    let page_array = unsafe {
                        &mut *(page_data.as_mut_ptr() as *mut [u8; crate::storage::PAGE_SIZE])
                    };
                    let heap_page = crate::storage::page::HeapPage::from_data(page_array);

                    // Scan all tuples in this page
                    let tuple_count = heap_page.get_tuple_count();
                    for slot_id in 0..tuple_count {
                        match heap_page.get_tuple(slot_id) {
                            Ok(data) => {
                                if let Ok(table_info) = TableInfo::deserialize(data) {
                                    if table_info.table_name == name {
                                        // Update cache
                                        let mut cache = self.table_cache.write().unwrap();
                                        cache.insert(name.to_string(), table_info.clone());
                                        return Ok(Some(table_info));
                                    }
                                }
                            }
                            Err(_) => continue,
                        }
                    }

                    current_page_id = heap_page.get_next_page_id();
                }
                Err(_) => break,
            }
        }

        Ok(None)
    }

    /// Create a new table
    pub fn create_table(&mut self, name: &str) -> Result<TableInfo> {
        // Check if table already exists
        if self.get_table(name)?.is_some() {
            bail!("Table '{}' already exists", name);
        }

        // Allocate new table ID
        let table_id = {
            let mut next_id = self.next_table_id.write().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Create first page for the new table
        let (first_page_id, mut guard) = self.buffer_pool.new_page()?;
        let _heap_page = crate::storage::page::HeapPage::new(&mut guard, first_page_id);
        drop(guard);

        // Create table info
        let table_info = TableInfo {
            table_id,
            table_name: name.to_string(),
            first_page_id,
        };

        // Insert into catalog
        self.catalog_heap.insert(&table_info.serialize())?;

        // Update cache
        {
            let mut cache = self.table_cache.write().unwrap();
            cache.insert(name.to_string(), table_info.clone());
        }

        Ok(table_info)
    }

    /// Create a table with columns
    pub fn create_table_with_columns(
        &mut self,
        name: &str,
        columns: Vec<(&str, DataType)>,
    ) -> Result<TableInfo> {
        // Create the table first
        let table_info = self.create_table(name)?;

        // Insert column definitions if we have pg_attribute
        if let Some(ref mut attr_heap) = self.attribute_heap {
            for (order, (col_name, col_type)) in columns.into_iter().enumerate() {
                let attr_row = AttributeRow {
                    table_id: table_info.table_id,
                    column_info: ColumnInfo {
                        column_name: col_name.to_string(),
                        column_type: col_type,
                        column_order: (order + 1) as u32,
                    },
                };
                attr_heap.insert(&attr_row.serialize())?;
            }
        }

        Ok(table_info)
    }

    /// Get columns for a table
    pub fn get_table_columns(&self, table_id: TableId) -> Result<Vec<ColumnInfo>> {
        // Check cache first
        {
            let cache = self.column_cache.read().unwrap();
            if let Some(columns) = cache.get(&table_id) {
                return Ok(columns.clone());
            }
        }

        // If no pg_attribute table, return empty
        let _attr_heap = match &self.attribute_heap {
            Some(heap) => heap,
            None => return Ok(vec![]),
        };

        let mut columns = Vec::new();

        // Scan pg_attribute for this table's columns
        let attr_table_info = self
            .get_table(CATALOG_ATTR_TABLE_NAME)?
            .ok_or_else(|| anyhow::anyhow!("pg_attribute table not found"))?;

        let mut current_page_id = Some(attr_table_info.first_page_id);

        while let Some(page_id) = current_page_id {
            match self.buffer_pool.fetch_page(page_id) {
                Ok(guard) => {
                    let page_data = unsafe {
                        std::slice::from_raw_parts_mut(
                            guard.as_ptr() as *mut u8,
                            crate::storage::PAGE_SIZE,
                        )
                    };
                    let page_array = unsafe {
                        &mut *(page_data.as_mut_ptr() as *mut [u8; crate::storage::PAGE_SIZE])
                    };
                    let heap_page = crate::storage::page::HeapPage::from_data(page_array);

                    let tuple_count = heap_page.get_tuple_count();
                    for slot_id in 0..tuple_count {
                        match heap_page.get_tuple(slot_id) {
                            Ok(data) => {
                                if let Ok(attr_row) = AttributeRow::deserialize(data) {
                                    if attr_row.table_id == table_id {
                                        columns.push(attr_row.column_info);
                                    }
                                }
                            }
                            Err(_) => continue,
                        }
                    }

                    current_page_id = heap_page.get_next_page_id();
                }
                Err(_) => break,
            }
        }

        // Sort by column order
        columns.sort_by_key(|c| c.column_order);

        // Update cache
        {
            let mut cache = self.column_cache.write().unwrap();
            cache.insert(table_id, columns.clone());
        }

        Ok(columns)
    }

    /// List all tables
    pub fn list_tables(&self) -> Result<Vec<TableInfo>> {
        let cache = self.table_cache.read().unwrap();
        let mut tables: Vec<TableInfo> = cache.values().cloned().collect();
        tables.sort_by_key(|t| t.table_id);
        Ok(tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::PageManager;
    use tempfile::tempdir;

    fn create_test_catalog() -> Result<Catalog> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let page_manager = PageManager::create(&file_path)?;
        let replacer = Box::new(crate::storage::buffer::lru::LruReplacer::new(10));
        let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);
        Catalog::initialize(buffer_pool)
    }

    #[test]
    fn test_table_info_serialization() -> Result<()> {
        let info = TableInfo {
            table_id: 42,
            table_name: "test_table".to_string(),
            first_page_id: PageId(123),
        };

        let serialized = info.serialize();
        let deserialized = TableInfo::deserialize(&serialized)?;

        assert_eq!(info.table_id, deserialized.table_id);
        assert_eq!(info.table_name, deserialized.table_name);
        assert_eq!(info.first_page_id, deserialized.first_page_id);

        Ok(())
    }

    #[test]
    fn test_catalog_initialization() -> Result<()> {
        let catalog = create_test_catalog()?;

        // Catalog table should exist
        let catalog_table = catalog.get_table(CATALOG_TABLE_NAME)?;
        assert!(catalog_table.is_some());

        let info = catalog_table.unwrap();
        assert_eq!(info.table_id, CATALOG_TABLE_ID);
        assert_eq!(info.table_name, CATALOG_TABLE_NAME);
        assert_eq!(info.first_page_id, CATALOG_FIRST_PAGE);

        Ok(())
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let mut catalog = create_test_catalog()?;

        // Create a new table
        let table_info = catalog.create_table("users")?;
        assert_eq!(table_info.table_name, "users");
        assert!(table_info.table_id > CATALOG_TABLE_ID);

        // Verify it exists
        let retrieved = catalog.get_table("users")?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().table_id, table_info.table_id);

        Ok(())
    }

    #[test]
    fn test_duplicate_table_name() -> Result<()> {
        let mut catalog = create_test_catalog()?;

        // Create first table
        catalog.create_table("users")?;

        // Try to create duplicate
        let result = catalog.create_table("users");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_list_tables() -> Result<()> {
        let mut catalog = create_test_catalog()?;

        // Initially catalog tables exist (pg_tables and pg_attribute)
        let tables = catalog.list_tables()?;
        assert_eq!(tables.len(), 2);
        assert!(tables.iter().any(|t| t.table_name == CATALOG_TABLE_NAME));
        assert!(
            tables
                .iter()
                .any(|t| t.table_name == CATALOG_ATTR_TABLE_NAME)
        );

        // Create more tables
        catalog.create_table("users")?;
        catalog.create_table("products")?;
        catalog.create_table("orders")?;

        // List should include all tables (2 system + 3 user)
        let tables = catalog.list_tables()?;
        assert_eq!(tables.len(), 5);

        let names: Vec<String> = tables.iter().map(|t| t.table_name.clone()).collect();
        assert!(names.contains(&CATALOG_TABLE_NAME.to_string()));
        assert!(names.contains(&"users".to_string()));
        assert!(names.contains(&"products".to_string()));
        assert!(names.contains(&"orders".to_string()));

        Ok(())
    }

    #[test]
    fn test_catalog_persistence() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test_persist.db");
        
        // Create and populate catalog
        {
            let page_manager = PageManager::create(&file_path)?;
            let replacer = Box::new(crate::storage::buffer::lru::LruReplacer::new(10));
            let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);
            let mut catalog = Catalog::initialize(buffer_pool)?;
            
            // Create some tables
            catalog.create_table("users")?;
            catalog.create_table("products")?;
            catalog.create_table_with_columns(
                "orders",
                vec![
                    ("order_id", DataType::Int32),
                    ("user_id", DataType::Int32),
                    ("total", DataType::Int32),
                ],
            )?;
            
            // Flush to ensure persistence
            catalog.buffer_pool.flush_all()?;
        }
        
        // Reopen and verify
        {
            let page_manager = PageManager::open(&file_path)?;
            let replacer = Box::new(crate::storage::buffer::lru::LruReplacer::new(10));
            let buffer_pool = BufferPoolManager::new(page_manager, replacer, 10);
            let catalog = Catalog::open(buffer_pool)?;
            
            // Verify all tables exist
            let tables = catalog.list_tables()?;
            assert_eq!(tables.len(), 5); // pg_tables + pg_attribute + 3 user tables
            
            // Verify table names
            let table_names: Vec<String> = tables.iter().map(|t| t.table_name.clone()).collect();
            assert!(table_names.contains(&"pg_tables".to_string()));
            assert!(table_names.contains(&"pg_attribute".to_string()));
            assert!(table_names.contains(&"users".to_string()));
            assert!(table_names.contains(&"products".to_string()));
            assert!(table_names.contains(&"orders".to_string()));
            
            // Verify orders table columns
            let orders_table = catalog.get_table("orders")?.unwrap();
            let columns = catalog.get_table_columns(orders_table.table_id)?;
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0].column_name, "order_id");
            assert_eq!(columns[0].column_type, DataType::Int32);
            assert_eq!(columns[1].column_name, "user_id");
            assert_eq!(columns[1].column_type, DataType::Int32);
            assert_eq!(columns[2].column_name, "total");
            assert_eq!(columns[2].column_type, DataType::Int32);
        }
        
        Ok(())
    }

    #[test]
    fn test_create_table_with_columns() -> Result<()> {
        let mut catalog = create_test_catalog()?;

        // Create a table with columns
        let columns = vec![
            ("id", DataType::Int32),
            ("name", DataType::Varchar),
            ("active", DataType::Boolean),
        ];

        let table_info = catalog.create_table_with_columns("test_table", columns)?;
        assert_eq!(table_info.table_name, "test_table");

        // Verify columns were created
        let retrieved_columns = catalog.get_table_columns(table_info.table_id)?;
        assert_eq!(retrieved_columns.len(), 3);

        assert_eq!(retrieved_columns[0].column_name, "id");
        assert_eq!(retrieved_columns[0].column_type, DataType::Int32);
        assert_eq!(retrieved_columns[0].column_order, 1);

        assert_eq!(retrieved_columns[1].column_name, "name");
        assert_eq!(retrieved_columns[1].column_type, DataType::Varchar);
        assert_eq!(retrieved_columns[1].column_order, 2);

        assert_eq!(retrieved_columns[2].column_name, "active");
        assert_eq!(retrieved_columns[2].column_type, DataType::Boolean);
        assert_eq!(retrieved_columns[2].column_order, 3);

        Ok(())
    }

    #[test]
    fn test_system_table_columns() -> Result<()> {
        let catalog = create_test_catalog()?;

        // Check pg_tables columns
        let pg_tables_columns = catalog.get_table_columns(CATALOG_TABLE_ID)?;
        assert_eq!(pg_tables_columns.len(), 3);
        assert_eq!(pg_tables_columns[0].column_name, "table_id");
        assert_eq!(pg_tables_columns[1].column_name, "table_name");
        assert_eq!(pg_tables_columns[2].column_name, "first_page_id");

        // Check pg_attribute columns
        let pg_attr_columns = catalog.get_table_columns(CATALOG_ATTR_TABLE_ID)?;
        assert_eq!(pg_attr_columns.len(), 4);
        assert_eq!(pg_attr_columns[0].column_name, "table_id");
        assert_eq!(pg_attr_columns[1].column_name, "column_name");
        assert_eq!(pg_attr_columns[2].column_name, "column_type");
        assert_eq!(pg_attr_columns[3].column_name, "column_order");

        Ok(())
    }
}
