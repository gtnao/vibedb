// Submodule declarations
pub mod column_info;
pub mod index_info;
pub mod system_tables;
pub mod table_info;

use crate::access::{DataType, TableHeap};
use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageId;
use anyhow::{Result, bail};
use std::collections::HashMap;
use std::sync::RwLock;

// Re-export commonly used types
pub use column_info::{AttributeRow, ColumnInfo};
pub use index_info::{IndexId, IndexInfo};
pub use system_tables::*;
pub use table_info::{CustomDeserializer, TableId, TableInfo};

pub struct Catalog {
    buffer_pool: BufferPoolManager,
    catalog_heap: TableHeap,
    attribute_heap: Option<TableHeap>,
    index_heap: Option<TableHeap>,
    table_cache: RwLock<HashMap<String, TableInfo>>,
    column_cache: RwLock<HashMap<TableId, Vec<ColumnInfo>>>,
    index_cache: RwLock<HashMap<TableId, Vec<IndexInfo>>>,
    next_table_id: RwLock<TableId>,
    next_index_id: RwLock<IndexId>,
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
            schema: None,
            column_names: None,
            custom_deserializer: None,
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
            schema: None,
            column_names: None,
            custom_deserializer: None,
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

        // Insert pg_index table entry
        let index_table_info = TableInfo {
            table_id: CATALOG_INDEX_TABLE_ID,
            table_name: CATALOG_INDEX_TABLE_NAME.to_string(),
            first_page_id: PageId(3), // Will be allocated when first index is created
            schema: None,
            column_names: None,
            custom_deserializer: None,
        };
        catalog_heap.insert(&index_table_info.serialize())?;

        // Initialize caches with proper metadata
        let mut initial_table_cache = HashMap::new();

        // Set up pg_tables metadata
        let mut catalog_info_with_meta = catalog_info;
        catalog_info_with_meta.schema = Some(pg_tables_schema());
        catalog_info_with_meta.column_names = Some(pg_tables_column_names());
        catalog_info_with_meta.custom_deserializer = Some(deserialize_pg_tables);
        initial_table_cache.insert(CATALOG_TABLE_NAME.to_string(), catalog_info_with_meta);

        // Set up pg_attribute metadata
        let mut attr_info_with_meta = attr_table_info;
        attr_info_with_meta.schema = Some(pg_attribute_schema());
        attr_info_with_meta.column_names = Some(pg_attribute_column_names());
        attr_info_with_meta.custom_deserializer = Some(deserialize_pg_attribute);
        initial_table_cache.insert(CATALOG_ATTR_TABLE_NAME.to_string(), attr_info_with_meta);

        // Set up pg_index metadata
        let mut index_info_with_meta = index_table_info;
        index_info_with_meta.schema = Some(pg_index_schema());
        index_info_with_meta.column_names = Some(pg_index_column_names());
        initial_table_cache.insert(CATALOG_INDEX_TABLE_NAME.to_string(), index_info_with_meta);

        Ok(Self {
            buffer_pool,
            catalog_heap,
            attribute_heap: Some(attribute_heap),
            index_heap: None, // Will be created on first index creation
            table_cache: RwLock::new(initial_table_cache),
            column_cache: RwLock::new(HashMap::new()),
            index_cache: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(TableId(CATALOG_INDEX_TABLE_ID.0 + 1)),
            next_index_id: RwLock::new(IndexId(1)),
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
                    let heap_page = crate::storage::page::utils::heap_page_from_guard(&guard);

                    // Scan all tuples in this page
                    let tuple_count = heap_page.get_tuple_count();
                    for slot_id in 0..tuple_count {
                        match heap_page.get_tuple(slot_id) {
                            Ok(data) => {
                                if let Ok(table_info) = TableInfo::deserialize(data) {
                                    if table_info.table_id > max_table_id {
                                        max_table_id = table_info.table_id;
                                    }
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

        // Set metadata for system tables
        if let Some(pg_tables_info) = table_cache.get_mut(CATALOG_TABLE_NAME) {
            pg_tables_info.schema = Some(pg_tables_schema());
            pg_tables_info.column_names = Some(pg_tables_column_names());
            pg_tables_info.custom_deserializer = Some(deserialize_pg_tables);
        }

        if let Some(pg_attribute_info) = table_cache.get_mut(CATALOG_ATTR_TABLE_NAME) {
            pg_attribute_info.schema = Some(pg_attribute_schema());
            pg_attribute_info.column_names = Some(pg_attribute_column_names());
            pg_attribute_info.custom_deserializer = Some(deserialize_pg_attribute);
        }

        if let Some(pg_index_info) = table_cache.get_mut(CATALOG_INDEX_TABLE_NAME) {
            pg_index_info.schema = Some(pg_index_schema());
            pg_index_info.column_names = Some(pg_index_column_names());
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

        // Find pg_index table
        let index_table_info = table_cache.get(CATALOG_INDEX_TABLE_NAME).cloned();
        let index_heap = index_table_info.map(|info| {
            TableHeap::with_first_page(
                buffer_pool.clone(),
                CATALOG_INDEX_TABLE_ID,
                info.first_page_id,
            )
        });

        Ok(Self {
            buffer_pool,
            catalog_heap,
            attribute_heap,
            index_heap,
            table_cache: RwLock::new(table_cache),
            column_cache: RwLock::new(HashMap::new()),
            index_cache: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(TableId(max_table_id.0 + 1)),
            next_index_id: RwLock::new(IndexId(1)), // TODO: load max index id
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
                    let heap_page = crate::storage::page::utils::heap_page_from_guard(&guard);

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
            next_id.0 += 1;
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
            schema: None,
            column_names: None,
            custom_deserializer: None,
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
                    let heap_page = crate::storage::page::utils::heap_page_from_guard(&guard);

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

    /// Create an index on a table
    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        column_names: &[&str],
        is_unique: bool,
    ) -> Result<IndexInfo> {
        // Get the table
        let table_info = self
            .get_table(table_name)?
            .ok_or_else(|| anyhow::anyhow!("Table {} not found", table_name))?;

        // Get the columns for the table
        let table_columns = self.get_table_columns(table_info.table_id)?;

        // Validate and collect key columns
        let mut key_columns = Vec::new();
        for col_name in column_names {
            let col_info = table_columns
                .iter()
                .find(|c| c.column_name == *col_name)
                .ok_or_else(|| {
                    anyhow::anyhow!("Column {} not found in table {}", col_name, table_name)
                })?;
            key_columns.push(col_info.clone());
        }

        // Generate new index ID
        let index_id = {
            let mut next_id = self.next_index_id.write().unwrap();
            let id = *next_id;
            next_id.0 += 1;
            id
        };

        // Create index info (root_page_id will be set when B+Tree is created)
        let index_info = IndexInfo {
            index_id,
            index_name: index_name.to_string(),
            table_id: table_info.table_id,
            key_columns,
            root_page_id: None,
            is_unique,
        };

        // Create index heap if not exists
        if self.index_heap.is_none() {
            // Allocate first page for index table
            let (page_id, mut guard) = self.buffer_pool.new_page()?;
            let _heap_page = crate::storage::page::HeapPage::new(&mut guard, page_id);
            drop(guard);

            self.index_heap = Some(TableHeap::with_first_page(
                self.buffer_pool.clone(),
                CATALOG_INDEX_TABLE_ID,
                page_id,
            ));

            // Update pg_index table's first_page_id in catalog
            let index_table_info = TableInfo {
                table_id: CATALOG_INDEX_TABLE_ID,
                table_name: CATALOG_INDEX_TABLE_NAME.to_string(),
                first_page_id: page_id,
                schema: Some(pg_index_schema()),
                column_names: Some(pg_index_column_names()),
                custom_deserializer: None,
            };
            self.catalog_heap.insert(&index_table_info.serialize())?;

            // Update cache
            self.table_cache
                .write()
                .unwrap()
                .insert(CATALOG_INDEX_TABLE_NAME.to_string(), index_table_info);
        }

        // Insert index info into pg_index
        if let Some(ref mut index_heap) = self.index_heap {
            let tuple = index_info.to_tuple();
            let serialized = crate::access::value::serialize_values(&tuple, &pg_index_schema())?;
            index_heap.insert(&serialized)?;
        }

        // Update index cache
        {
            let mut cache = self.index_cache.write().unwrap();
            let indexes = cache.entry(table_info.table_id).or_default();
            indexes.push(index_info.clone());
        }

        Ok(index_info)
    }

    /// Get indexes for a table
    pub fn get_table_indexes(&self, table_id: TableId) -> Result<Vec<IndexInfo>> {
        // Check cache first
        {
            let cache = self.index_cache.read().unwrap();
            if let Some(indexes) = cache.get(&table_id) {
                return Ok(indexes.clone());
            }
        }

        // Load from pg_index if not in cache
        if self.index_heap.is_none() {
            return Ok(Vec::new());
        }

        let mut indexes = Vec::new();
        let table_columns = self.get_table_columns(table_id)?;

        // Scan pg_index table
        let index_table_info = self
            .get_table(CATALOG_INDEX_TABLE_NAME)?
            .ok_or_else(|| anyhow::anyhow!("pg_index table not found"))?;

        let mut current_page_id = Some(index_table_info.first_page_id);

        while let Some(page_id) = current_page_id {
            match self.buffer_pool.fetch_page(page_id) {
                Ok(guard) => {
                    let heap_page = crate::storage::page::utils::heap_page_from_guard(&guard);

                    let tuple_count = heap_page.get_tuple_count();
                    for slot_id in 0..tuple_count {
                        match heap_page.get_tuple(slot_id) {
                            Ok(data) => {
                                // Deserialize the tuple data
                                if let Ok(values) = crate::access::value::deserialize_values(
                                    data,
                                    &pg_index_schema(),
                                ) {
                                    if let Ok(index_info) =
                                        IndexInfo::from_tuple_values(&values, &table_columns)
                                    {
                                        if index_info.table_id == table_id {
                                            indexes.push(index_info);
                                        }
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

        // Update cache
        {
            let mut cache = self.index_cache.write().unwrap();
            cache.insert(table_id, indexes.clone());
        }

        Ok(indexes)
    }

    /// Update index root page after B+Tree creation
    pub fn update_index_root_page(
        &mut self,
        index_id: IndexId,
        root_page_id: PageId,
    ) -> Result<()> {
        // TODO: Update pg_index table with new root_page_id
        // For now, just update the cache
        let mut cache = self.index_cache.write().unwrap();
        for indexes in cache.values_mut() {
            for index in indexes.iter_mut() {
                if index.index_id == index_id {
                    index.root_page_id = Some(root_page_id);
                    return Ok(());
                }
            }
        }

        anyhow::bail!("Index with id {:?} not found", index_id)
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
        assert_eq!(tables.len(), 3); // pg_tables, pg_attribute, and pg_index
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

        // List should include all tables (3 system + 3 user)
        let tables = catalog.list_tables()?;
        assert_eq!(tables.len(), 6);

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
            assert_eq!(tables.len(), 6); // pg_tables + pg_attribute + pg_index + 3 user tables

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
