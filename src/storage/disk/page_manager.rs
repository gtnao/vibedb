use crate::storage::page::PageId;
use anyhow::{Context, Result, bail};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 8192;

pub struct PageManager {
    file: File,
}

impl PageManager {
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("Failed to create file: {:?}", path))?;

        Ok(Self { file })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("Failed to open file: {:?}", path))?;

        Ok(Self { file })
    }

    pub fn read_page(&mut self, page_id: PageId, buf: &mut [u8]) -> Result<()> {
        if buf.len() != PAGE_SIZE {
            bail!(
                "Buffer size must be PAGE_SIZE ({}), got {}",
                PAGE_SIZE,
                buf.len()
            );
        }

        let offset = Self::page_offset(page_id);
        let file_size = self.file.metadata()?.len();

        if offset >= file_size {
            bail!("Page {} does not exist", page_id.0);
        }

        self.file
            .seek(SeekFrom::Start(offset))
            .context("Failed to seek")?;
        self.file.read_exact(buf).context("Failed to read page")?;

        Ok(())
    }

    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() != PAGE_SIZE {
            bail!(
                "Data size must be PAGE_SIZE ({}), got {}",
                PAGE_SIZE,
                data.len()
            );
        }

        let offset = Self::page_offset(page_id);
        let file_size = self.file.metadata()?.len();

        // Extend file if necessary
        if offset >= file_size {
            let new_size = offset + PAGE_SIZE as u64;
            self.file
                .set_len(new_size)
                .context("Failed to extend file")?;
        }

        self.file
            .seek(SeekFrom::Start(offset))
            .context("Failed to seek")?;
        self.file.write_all(data).context("Failed to write page")?;
        self.file.sync_all().context("Failed to sync")?;

        Ok(())
    }

    pub fn num_pages(&self) -> Result<u32> {
        let file_size = self.file.metadata()?.len();
        Ok((file_size / PAGE_SIZE as u64) as u32)
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        let current_pages = self.num_pages()?;
        let new_page_id = PageId(current_pages);

        // Extend file to include new page
        let new_size = (current_pages as u64 + 1) * PAGE_SIZE as u64;
        self.file
            .set_len(new_size)
            .context("Failed to extend file")?;

        Ok(new_page_id)
    }

    fn page_offset(page_id: PageId) -> u64 {
        page_id.0 as u64 * PAGE_SIZE as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_open() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");

        // Create new file
        {
            let pm = PageManager::create(&file_path)?;
            assert_eq!(pm.num_pages()?, 0);
        }

        // Open existing file
        {
            let pm = PageManager::open(&file_path)?;
            assert_eq!(pm.num_pages()?, 0);
        }

        Ok(())
    }

    #[test]
    fn test_write_and_read_page() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        // Write page
        let mut write_buf = vec![0u8; PAGE_SIZE];
        write_buf[0] = 42;
        write_buf[PAGE_SIZE - 1] = 24;
        pm.write_page(PageId(0), &write_buf)?;

        // Read page
        let mut read_buf = vec![0u8; PAGE_SIZE];
        pm.read_page(PageId(0), &mut read_buf)?;

        assert_eq!(read_buf[0], 42);
        assert_eq!(read_buf[PAGE_SIZE - 1], 24);

        Ok(())
    }

    #[test]
    fn test_multiple_pages() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        // Write multiple pages
        for i in 0..5 {
            let mut buf = vec![0u8; PAGE_SIZE];
            buf[0] = i as u8;
            pm.write_page(PageId(i), &buf)?;
        }

        assert_eq!(pm.num_pages()?, 5);

        // Read pages back
        for i in 0..5 {
            let mut buf = vec![0u8; PAGE_SIZE];
            pm.read_page(PageId(i), &mut buf)?;
            assert_eq!(buf[0], i as u8);
        }

        Ok(())
    }

    #[test]
    fn test_overwrite_page() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        // Write initial data
        let buf1 = vec![1u8; PAGE_SIZE];
        pm.write_page(PageId(0), &buf1)?;

        // Overwrite with new data
        let buf2 = vec![2u8; PAGE_SIZE];
        pm.write_page(PageId(0), &buf2)?;

        // Read and verify
        let mut read_buf = vec![0u8; PAGE_SIZE];
        pm.read_page(PageId(0), &mut read_buf)?;
        assert_eq!(read_buf[0], 2);

        Ok(())
    }

    #[test]
    fn test_read_nonexistent_page() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        let mut buf = vec![0u8; PAGE_SIZE];
        let result = pm.read_page(PageId(10), &mut buf);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_invalid_buffer_size() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        // Test read with wrong buffer size
        let mut small_buf = vec![0u8; 100];
        let result = pm.read_page(PageId(0), &mut small_buf);
        assert!(result.is_err());

        // Test write with wrong buffer size
        let small_data = vec![0u8; 100];
        let result = pm.write_page(PageId(0), &small_data);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_open_nonexistent_file() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("nonexistent.db");

        let result = PageManager::open(&file_path);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_first_page() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        let buf = vec![42u8; PAGE_SIZE];
        pm.write_page(PageId(0), &buf)?;

        let mut read_buf = vec![0u8; PAGE_SIZE];
        pm.read_page(PageId(0), &mut read_buf)?;
        assert_eq!(read_buf[0], 42);

        Ok(())
    }

    #[test]
    fn test_page_boundary() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        // Write different patterns to adjacent pages
        let buf1 = vec![1u8; PAGE_SIZE];
        let buf2 = vec![2u8; PAGE_SIZE];
        pm.write_page(PageId(0), &buf1)?;
        pm.write_page(PageId(1), &buf2)?;

        // Read back and verify no overlap
        let mut read_buf = vec![0u8; PAGE_SIZE];
        pm.read_page(PageId(0), &mut read_buf)?;
        assert!(read_buf.iter().all(|&b| b == 1));

        pm.read_page(PageId(1), &mut read_buf)?;
        assert!(read_buf.iter().all(|&b| b == 2));

        Ok(())
    }

    #[test]
    fn test_file_growth() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        assert_eq!(pm.num_pages()?, 0);

        // Write to page 5 (skipping 0-4)
        let buf = vec![5u8; PAGE_SIZE];
        pm.write_page(PageId(5), &buf)?;

        // File should have grown to accommodate 6 pages
        assert_eq!(pm.num_pages()?, 6);

        Ok(())
    }

    #[test]
    fn test_persistence() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");

        // Write data
        {
            let mut pm = PageManager::create(&file_path)?;
            let buf = vec![99u8; PAGE_SIZE];
            pm.write_page(PageId(0), &buf)?;
        }

        // Read data after reopening
        {
            let mut pm = PageManager::open(&file_path)?;
            let mut buf = vec![0u8; PAGE_SIZE];
            pm.read_page(PageId(0), &mut buf)?;
            assert_eq!(buf[0], 99);
        }

        Ok(())
    }

    #[test]
    fn test_allocate_page() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.db");
        let mut pm = PageManager::create(&file_path)?;

        assert_eq!(pm.num_pages()?, 0);

        let page_id = pm.allocate_page()?;
        assert_eq!(page_id, PageId(0));
        assert_eq!(pm.num_pages()?, 1);

        let page_id = pm.allocate_page()?;
        assert_eq!(page_id, PageId(1));
        assert_eq!(pm.num_pages()?, 2);

        Ok(())
    }
}
