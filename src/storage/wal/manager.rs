//! WAL manager implementation.
//!
//! The WAL manager handles writing log records to disk, managing the WAL buffer,
//! and ensuring durability through proper flushing.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use super::record::{WalRecord, LSN};
use crate::storage::error::StorageError;

/// Size of the WAL buffer in bytes (1MB).
const WAL_BUFFER_SIZE: usize = 1024 * 1024;

/// WAL file prefix.
const WAL_FILE_PREFIX: &str = "wal_";

/// WAL file extension.
const WAL_FILE_EXTENSION: &str = ".log";

/// WAL manager configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory where WAL files are stored.
    pub wal_dir: PathBuf,
    /// Maximum size of a single WAL file in bytes.
    pub max_file_size: u64,
    /// Whether to sync WAL to disk on every commit.
    pub sync_on_commit: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            wal_dir: PathBuf::from("wal"),
            max_file_size: 10 * 1024 * 1024 * 1024, // 10GB
            sync_on_commit: true,
        }
    }
}

/// WAL buffer for batching writes.
struct WalBuffer {
    /// Buffer data.
    data: Vec<u8>,
    /// Current position in the buffer.
    position: usize,
}

impl WalBuffer {
    /// Create a new WAL buffer.
    fn new() -> Self {
        WalBuffer {
            data: Vec::with_capacity(WAL_BUFFER_SIZE),
            position: 0,
        }
    }

    /// Write data to the buffer.
    fn write(&mut self, data: &[u8]) -> Result<(), StorageError> {
        if self.position + data.len() > WAL_BUFFER_SIZE {
            return Err(StorageError::IoError(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "WAL buffer full",
            )));
        }

        if self.data.len() < self.position + data.len() {
            self.data.resize(self.position + data.len(), 0);
        }

        self.data[self.position..self.position + data.len()].copy_from_slice(data);
        self.position += data.len();
        Ok(())
    }

    /// Get the current buffer contents.
    fn contents(&self) -> &[u8] {
        &self.data[..self.position]
    }

    /// Clear the buffer.
    fn clear(&mut self) {
        self.position = 0;
    }

    /// Check if the buffer is empty.
    fn is_empty(&self) -> bool {
        self.position == 0
    }

    /// Get the current buffer size.
    fn len(&self) -> usize {
        self.position
    }
}

/// WAL file handle.
struct WalFile {
    /// File handle.
    file: BufWriter<File>,
    /// Current file size.
    size: u64,
}

impl WalFile {
    /// Create a new WAL file.
    fn create(path: PathBuf) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)?;

        Ok(WalFile {
            file: BufWriter::new(file),
            size: 0,
        })
    }

    /// Write data to the file.
    fn write(&mut self, data: &[u8]) -> Result<(), StorageError> {
        self.file.write_all(data)?;
        self.size += data.len() as u64;
        Ok(())
    }

    /// Flush the file buffer.
    fn flush(&mut self) -> Result<(), StorageError> {
        self.file.flush()?;
        Ok(())
    }

    /// Sync the file to disk.
    fn sync(&mut self) -> Result<(), StorageError> {
        self.file.get_mut().sync_all()?;
        Ok(())
    }
}

/// WAL manager for handling write-ahead logging.
pub struct WalManager {
    /// Configuration.
    config: WalConfig,
    /// Current LSN.
    current_lsn: Arc<RwLock<LSN>>,
    /// WAL buffer.
    buffer: Arc<Mutex<WalBuffer>>,
    /// Current WAL file.
    current_file: Arc<Mutex<Option<WalFile>>>,
    /// Flush LSN - all records up to this LSN have been flushed to disk.
    flush_lsn: Arc<RwLock<LSN>>,
}

impl WalManager {
    /// Create a new WAL manager.
    pub fn new(config: WalConfig) -> Result<Self, StorageError> {
        // Create WAL directory if it doesn't exist
        std::fs::create_dir_all(&config.wal_dir)?;

        Ok(WalManager {
            config,
            current_lsn: Arc::new(RwLock::new(LSN::new())),
            buffer: Arc::new(Mutex::new(WalBuffer::new())),
            current_file: Arc::new(Mutex::new(None)),
            flush_lsn: Arc::new(RwLock::new(LSN::new())),
        })
    }
    
    /// Create a new WAL manager with a simple path
    pub fn create(path: &Path) -> Result<Self, StorageError> {
        let config = WalConfig {
            wal_dir: path.to_path_buf(),
            ..Default::default()
        };
        
        let manager = Self::new(config)?;
        manager.initialize()?;
        Ok(manager)
    }
    
    /// Open an existing WAL manager
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let config = WalConfig {
            wal_dir: path.to_path_buf(),
            ..Default::default()
        };
        
        let manager = Self::new(config)?;
        // TODO: Scan existing WAL files and set correct LSN
        manager.initialize()?;
        Ok(manager)
    }

    /// Initialize the WAL manager by opening or creating the first WAL file.
    pub fn initialize(&self) -> Result<(), StorageError> {
        let mut current_file = self.current_file.lock().unwrap();
        if current_file.is_none() {
            let file_path = self.get_new_wal_file_path()?;
            *current_file = Some(WalFile::create(file_path)?);
        }
        Ok(())
    }

    /// Get the next LSN.
    pub fn get_next_lsn(&self) -> LSN {
        let mut lsn = self.current_lsn.write().unwrap();
        let next_lsn = lsn.next();
        *lsn = next_lsn;
        next_lsn
    }

    /// Get the current LSN.
    pub fn get_current_lsn(&self) -> LSN {
        *self.current_lsn.read().unwrap()
    }

    /// Get the flush LSN.
    pub fn get_flush_lsn(&self) -> LSN {
        *self.flush_lsn.read().unwrap()
    }

    /// Write a WAL record.
    pub fn write_record(&self, record: &WalRecord) -> Result<LSN, StorageError> {
        // Serialize the record
        let serialized = record
            .serialize()
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;

        // Write record size (4 bytes) followed by the record data
        let mut record_data = Vec::with_capacity(4 + serialized.len());
        record_data.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
        record_data.extend_from_slice(&serialized);

        // Write to buffer
        let mut buffer = self.buffer.lock().unwrap();

        // If adding this record would exceed buffer size, flush first
        if buffer.len() + record_data.len() > WAL_BUFFER_SIZE {
            drop(buffer);
            self.flush_buffer()?;
            buffer = self.buffer.lock().unwrap();
        }

        buffer.write(&record_data)?;

        Ok(record.header.lsn)
    }

    /// Flush the WAL buffer to disk.
    pub fn flush(&self) -> Result<(), StorageError> {
        self.flush_buffer()?;

        // Sync to disk if configured
        if self.config.sync_on_commit {
            self.sync()?;
        }

        Ok(())
    }

    /// Flush the buffer to the current WAL file.
    fn flush_buffer(&self) -> Result<(), StorageError> {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.is_empty() {
            return Ok(());
        }

        let mut current_file_guard = self.current_file.lock().unwrap();
        let current_file = current_file_guard.as_mut().ok_or_else(|| {
            StorageError::IoError(io::Error::new(io::ErrorKind::NotFound, "No WAL file open"))
        })?;

        // Check if we need to rotate the file
        if current_file.size + buffer.len() as u64 > self.config.max_file_size {
            // Close current file and create a new one
            current_file.flush()?;
            current_file.sync()?;

            let new_file_path = self.get_new_wal_file_path()?;
            *current_file = WalFile::create(new_file_path)?;
        }

        // Write buffer contents to file
        current_file.write(buffer.contents())?;
        current_file.flush()?;

        // Update flush LSN
        let current_lsn = self.get_current_lsn();
        *self.flush_lsn.write().unwrap() = current_lsn;

        // Clear the buffer
        buffer.clear();

        Ok(())
    }

    /// Sync the WAL file to disk.
    fn sync(&self) -> Result<(), StorageError> {
        let mut current_file = self.current_file.lock().unwrap();
        if let Some(file) = current_file.as_mut() {
            file.sync()?;
        }
        Ok(())
    }

    /// Get a new WAL file path.
    fn get_new_wal_file_path(&self) -> Result<PathBuf, StorageError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                StorageError::IoError(io::Error::other(format!("Failed to get timestamp: {}", e)))
            })?
            .as_micros();

        let filename = format!("{}{}{}", WAL_FILE_PREFIX, timestamp, WAL_FILE_EXTENSION);
        Ok(self.config.wal_dir.join(filename))
    }

    /// Read WAL records from a file.
    pub fn read_records_from_file(path: &Path) -> Result<Vec<WalRecord>, StorageError> {
        let mut file = File::open(path)?;
        let mut records = Vec::new();

        loop {
            // Read record size
            let mut size_bytes = [0u8; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::IoError(e)),
            }

            let size = u32::from_le_bytes(size_bytes) as usize;

            // Read record data
            let mut record_data = vec![0u8; size];
            file.read_exact(&mut record_data)?;

            // Deserialize record
            let record = WalRecord::deserialize(&record_data)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;

            records.push(record);
        }

        Ok(records)
    }

    /// Get all WAL files in the WAL directory.
    pub fn get_wal_files(&self) -> Result<Vec<PathBuf>, StorageError> {
        let mut files = Vec::new();

        for entry in std::fs::read_dir(&self.config.wal_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(filename) = path.file_name() {
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with(WAL_FILE_PREFIX)
                    && filename_str.ends_with(WAL_FILE_EXTENSION)
                {
                    files.push(path);
                }
            }
        }

        // Sort files by name (which includes timestamp)
        files.sort();

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_wal_manager() -> (WalManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_file_size: 1024 * 1024, // 1MB for testing
            sync_on_commit: false,      // Disable sync for faster tests
        };
        let manager = WalManager::new(config).unwrap();
        manager.initialize().unwrap();
        (manager, temp_dir)
    }

    #[test]
    fn test_wal_buffer() {
        let mut buffer = WalBuffer::new();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);

        let data = b"Hello, WAL!";
        buffer.write(data).unwrap();
        assert!(!buffer.is_empty());
        assert_eq!(buffer.len(), data.len());
        assert_eq!(buffer.contents(), data);

        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_lsn_generation() {
        let (manager, _temp_dir) = create_test_wal_manager();

        let lsn1 = manager.get_next_lsn();
        let lsn2 = manager.get_next_lsn();
        let lsn3 = manager.get_next_lsn();

        assert_eq!(lsn1.0, 1);
        assert_eq!(lsn2.0, 2);
        assert_eq!(lsn3.0, 3);
        assert_eq!(manager.get_current_lsn().0, 3);
    }

    #[test]
    fn test_write_and_read_records() {
        let (manager, _temp_dir) = create_test_wal_manager();

        // Write some records
        let lsn1 = manager.get_next_lsn();
        let record1 = WalRecord::begin(lsn1, 1);
        manager.write_record(&record1).unwrap();

        let lsn2 = manager.get_next_lsn();
        let record2 = WalRecord::insert(
            lsn2,
            lsn1,
            1,
            crate::storage::PageId(10),
            20,
            vec![crate::storage::wal::record::Value::Integer(42)],
        );
        manager.write_record(&record2).unwrap();

        let lsn3 = manager.get_next_lsn();
        let record3 = WalRecord::commit(lsn3, lsn2, 1, 1234567890);
        manager.write_record(&record3).unwrap();

        // Flush to disk
        manager.flush().unwrap();

        // Read records back
        let wal_files = manager.get_wal_files().unwrap();
        assert_eq!(wal_files.len(), 1);

        let records = WalManager::read_records_from_file(&wal_files[0]).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], record1);
        assert_eq!(records[1], record2);
        assert_eq!(records[2], record3);
    }

    #[test]
    fn test_buffer_flushing() {
        let (manager, _temp_dir) = create_test_wal_manager();

        // Write a large record to trigger buffer flush
        let mut large_values = Vec::new();
        for i in 0..10000 {
            large_values.push(crate::storage::wal::record::Value::Integer(i));
        }

        let lsn = manager.get_next_lsn();
        let large_record = WalRecord::insert(
            lsn,
            LSN::new(),
            1,
            crate::storage::PageId(1),
            1,
            large_values,
        );

        // This should trigger automatic buffer flush
        manager.write_record(&large_record).unwrap();

        // Verify the record was written
        let wal_files = manager.get_wal_files().unwrap();
        assert!(!wal_files.is_empty());
    }

    #[test]
    fn test_file_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_file_size: 1024, // Very small size to trigger rotation
            sync_on_commit: false,
        };
        let manager = WalManager::new(config).unwrap();
        manager.initialize().unwrap();

        // Write multiple records to trigger file rotation
        for i in 0..100 {
            let lsn = manager.get_next_lsn();
            let record = WalRecord::insert(
                lsn,
                LSN::new(),
                1,
                crate::storage::PageId(i),
                i as u16,
                vec![crate::storage::wal::record::Value::Integer(i as i64)],
            );
            manager.write_record(&record).unwrap();

            if i % 10 == 0 {
                manager.flush().unwrap();
            }
        }

        manager.flush().unwrap();

        // Should have multiple WAL files
        let wal_files = manager.get_wal_files().unwrap();
        assert!(wal_files.len() > 1);
    }

    #[test]
    fn test_flush_lsn() {
        let (manager, _temp_dir) = create_test_wal_manager();

        // Initially, flush LSN should be 0
        assert_eq!(manager.get_flush_lsn().0, 0);

        // Write a record
        let lsn = manager.get_next_lsn();
        let record = WalRecord::begin(lsn, 1);
        manager.write_record(&record).unwrap();

        // Flush LSN should still be 0 (not flushed yet)
        assert_eq!(manager.get_flush_lsn().0, 0);

        // Flush
        manager.flush().unwrap();

        // Flush LSN should now be updated
        assert_eq!(manager.get_flush_lsn(), manager.get_current_lsn());
    }
}
