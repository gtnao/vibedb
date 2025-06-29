use crate::storage::PAGE_SIZE;
use crate::storage::page::{Page, PageId};
use anyhow::{Result, bail};

// Header structure (16 bytes)
const HEADER_SIZE: usize = 16;
const PAGE_ID_OFFSET: usize = 0;
const FREE_SPACE_POINTER_OFFSET: usize = 12;
const TUPLE_COUNT_OFFSET: usize = 14;

// Slot size (4 bytes: 2 for offset, 2 for length)
const SLOT_SIZE: usize = 4;

pub struct HeapPage<'a> {
    data: &'a mut [u8; PAGE_SIZE],
}

impl<'a> HeapPage<'a> {
    pub fn new(data: &'a mut [u8; PAGE_SIZE], page_id: PageId) -> Self {
        // Initialize header
        let page_id_bytes = page_id.0.to_le_bytes();
        data[PAGE_ID_OFFSET..PAGE_ID_OFFSET + 4].copy_from_slice(&page_id_bytes);

        // Set initial free space pointer (right after header)
        let free_space_pointer = HEADER_SIZE as u16;
        data[FREE_SPACE_POINTER_OFFSET..FREE_SPACE_POINTER_OFFSET + 2]
            .copy_from_slice(&free_space_pointer.to_le_bytes());

        // Initialize tuple count to 0
        data[TUPLE_COUNT_OFFSET..TUPLE_COUNT_OFFSET + 2].copy_from_slice(&0u16.to_le_bytes());

        Self { data }
    }

    pub fn from_data(data: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { data }
    }

    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> Result<u16> {
        let tuple_size = tuple_data.len();
        if tuple_size > u16::MAX as usize {
            bail!("Tuple size too large");
        }

        let tuple_count = self.get_tuple_count();
        let free_space_pointer = self.get_free_space_pointer();
        let slot_array_end = PAGE_SIZE - (tuple_count as usize * SLOT_SIZE);

        // Check if we have enough space
        let required_space = tuple_size + SLOT_SIZE;
        let available_space = slot_array_end - free_space_pointer as usize;
        if available_space < required_space {
            bail!("Not enough space in page");
        }

        // Write tuple data
        let tuple_offset = free_space_pointer;
        self.data[tuple_offset as usize..tuple_offset as usize + tuple_size]
            .copy_from_slice(tuple_data);

        // Update free space pointer
        let new_free_space_pointer = tuple_offset + tuple_size as u16;
        self.set_free_space_pointer(new_free_space_pointer);

        // Add slot entry
        let slot_offset = PAGE_SIZE - ((tuple_count + 1) as usize * SLOT_SIZE);
        self.data[slot_offset..slot_offset + 2].copy_from_slice(&tuple_offset.to_le_bytes());
        self.data[slot_offset + 2..slot_offset + 4]
            .copy_from_slice(&(tuple_size as u16).to_le_bytes());

        // Update tuple count
        self.set_tuple_count(tuple_count + 1);

        Ok(tuple_count)
    }

    pub fn get_tuple(&self, slot_id: u16) -> Result<&[u8]> {
        let tuple_count = self.get_tuple_count();
        if slot_id >= tuple_count {
            bail!("Invalid slot id");
        }

        let slot_offset = PAGE_SIZE - ((slot_id + 1) as usize * SLOT_SIZE);
        let tuple_offset = u16::from_le_bytes([self.data[slot_offset], self.data[slot_offset + 1]]);
        let tuple_length =
            u16::from_le_bytes([self.data[slot_offset + 2], self.data[slot_offset + 3]]);

        if tuple_offset == 0 && tuple_length == 0 {
            bail!("Tuple has been deleted");
        }

        Ok(&self.data[tuple_offset as usize..(tuple_offset + tuple_length) as usize])
    }

    pub fn delete_tuple(&mut self, slot_id: u16) -> Result<()> {
        let tuple_count = self.get_tuple_count();
        if slot_id >= tuple_count {
            bail!("Invalid slot id");
        }

        // Mark slot as deleted (offset = 0, length = 0)
        let slot_offset = PAGE_SIZE - ((slot_id + 1) as usize * SLOT_SIZE);
        self.data[slot_offset..slot_offset + 4].fill(0);

        Ok(())
    }

    fn get_page_id(&self) -> PageId {
        let bytes = [
            self.data[PAGE_ID_OFFSET],
            self.data[PAGE_ID_OFFSET + 1],
            self.data[PAGE_ID_OFFSET + 2],
            self.data[PAGE_ID_OFFSET + 3],
        ];
        PageId(u32::from_le_bytes(bytes))
    }

    fn get_free_space_pointer(&self) -> u16 {
        u16::from_le_bytes([
            self.data[FREE_SPACE_POINTER_OFFSET],
            self.data[FREE_SPACE_POINTER_OFFSET + 1],
        ])
    }

    fn set_free_space_pointer(&mut self, pointer: u16) {
        self.data[FREE_SPACE_POINTER_OFFSET..FREE_SPACE_POINTER_OFFSET + 2]
            .copy_from_slice(&pointer.to_le_bytes());
    }

    fn get_tuple_count(&self) -> u16 {
        u16::from_le_bytes([
            self.data[TUPLE_COUNT_OFFSET],
            self.data[TUPLE_COUNT_OFFSET + 1],
        ])
    }

    fn set_tuple_count(&mut self, count: u16) {
        self.data[TUPLE_COUNT_OFFSET..TUPLE_COUNT_OFFSET + 2].copy_from_slice(&count.to_le_bytes());
    }

    pub fn get_free_space(&self) -> usize {
        let free_space_pointer = self.get_free_space_pointer();
        let tuple_count = self.get_tuple_count();
        let slot_array_end = PAGE_SIZE - (tuple_count as usize * SLOT_SIZE);

        slot_array_end.saturating_sub(free_space_pointer as usize)
    }
}

impl<'a> Page for HeapPage<'a> {
    fn page_id(&self) -> PageId {
        self.get_page_id()
    }

    fn data(&self) -> &[u8; PAGE_SIZE] {
        self.data
    }

    fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_page_initialization() {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let page_id = PageId(42);
        let page = HeapPage::new(&mut data, page_id);

        assert_eq!(page.page_id(), page_id);
        assert_eq!(page.get_tuple_count(), 0);
        assert_eq!(page.get_free_space_pointer(), HEADER_SIZE as u16);
        assert_eq!(page.get_free_space(), PAGE_SIZE - HEADER_SIZE);
    }

    #[test]
    fn test_insert_and_get_tuple() -> Result<()> {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let mut page = HeapPage::new(&mut data, PageId(1));

        let tuple1 = b"Hello, World!";
        let slot1 = page.insert_tuple(tuple1)?;
        assert_eq!(slot1, 0);

        let tuple2 = b"Second tuple";
        let slot2 = page.insert_tuple(tuple2)?;
        assert_eq!(slot2, 1);

        assert_eq!(page.get_tuple(slot1)?, tuple1);
        assert_eq!(page.get_tuple(slot2)?, tuple2);
        assert_eq!(page.get_tuple_count(), 2);

        Ok(())
    }

    #[test]
    fn test_delete_tuple() -> Result<()> {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let mut page = HeapPage::new(&mut data, PageId(1));

        let tuple = b"Test tuple";
        let slot = page.insert_tuple(tuple)?;

        page.delete_tuple(slot)?;

        // Trying to get deleted tuple should fail
        assert!(page.get_tuple(slot).is_err());

        Ok(())
    }

    #[test]
    fn test_page_full() -> Result<()> {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let mut page = HeapPage::new(&mut data, PageId(1));

        // Fill page with large tuples
        let large_tuple = vec![0xAA; 1000];
        let mut count = 0;

        while page.get_free_space() >= large_tuple.len() + SLOT_SIZE {
            page.insert_tuple(&large_tuple)?;
            count += 1;
        }

        // Try to insert one more tuple - should fail
        let result = page.insert_tuple(&large_tuple);
        assert!(result.is_err());

        assert!(count > 0);
        Ok(())
    }

    #[test]
    fn test_invalid_slot_id() -> Result<()> {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let page = HeapPage::new(&mut data, PageId(1));

        // Try to get tuple with invalid slot id
        assert!(page.get_tuple(0).is_err());
        assert!(page.get_tuple(100).is_err());

        Ok(())
    }

    #[test]
    fn test_from_existing_data() -> Result<()> {
        let mut data = Box::new([0u8; PAGE_SIZE]);

        // Create and populate page
        {
            let mut page = HeapPage::new(&mut data, PageId(123));
            page.insert_tuple(b"Persistent data")?;
        }

        // Load from existing data
        {
            let page = HeapPage::from_data(&mut data);
            assert_eq!(page.page_id(), PageId(123));
            assert_eq!(page.get_tuple_count(), 1);
            assert_eq!(page.get_tuple(0)?, b"Persistent data");
        }

        Ok(())
    }
}
