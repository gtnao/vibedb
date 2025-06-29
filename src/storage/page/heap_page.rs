use crate::storage::PAGE_SIZE;
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::page::{Page, PageId};

// Header structure (20 bytes) - PostgreSQL style
const HEADER_SIZE: usize = 20;
const PAGE_ID_OFFSET: usize = 0;
const LOWER_OFFSET: usize = 12; // End of slot array (grows down)
const UPPER_OFFSET: usize = 14; // Start of tuple data (grows up)
const SPECIAL_OFFSET: usize = 16; // For future extensions

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

        // Set initial lower (slot array end - starts right after header)
        let lower = HEADER_SIZE as u16;
        data[LOWER_OFFSET..LOWER_OFFSET + 2].copy_from_slice(&lower.to_le_bytes());

        // Set initial upper (tuple data start - starts at page end)
        let upper = PAGE_SIZE as u16;
        data[UPPER_OFFSET..UPPER_OFFSET + 2].copy_from_slice(&upper.to_le_bytes());

        // Initialize special area to 0xFFFFFFFF (no next page)
        data[SPECIAL_OFFSET..SPECIAL_OFFSET + 4].copy_from_slice(&0xFFFFFFFF_u32.to_le_bytes());

        Self { data }
    }

    pub fn from_data(data: &'a mut [u8; PAGE_SIZE]) -> Self {
        Self { data }
    }

    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> StorageResult<u16> {
        let tuple_size = tuple_data.len();
        if tuple_size > u16::MAX as usize {
            return Err(StorageError::Other(format!(
                "Tuple size {} exceeds maximum {}",
                tuple_size,
                u16::MAX
            )));
        }

        let lower = self.get_lower();
        let upper = self.get_upper();
        let tuple_count = self.get_tuple_count();

        // Check if we have enough space
        let required_space = tuple_size + SLOT_SIZE;
        let available_space = (upper - lower) as usize;
        if available_space < required_space {
            return Err(StorageError::PageFull {
                required: required_space,
                available: available_space,
            });
        }

        // Write tuple data (from upper, growing up)
        let new_upper = upper - tuple_size as u16;
        self.data[new_upper as usize..upper as usize].copy_from_slice(tuple_data);

        // Add slot entry (at lower, growing down)
        let slot_offset = lower as usize;
        self.data[slot_offset..slot_offset + 2].copy_from_slice(&new_upper.to_le_bytes());
        self.data[slot_offset + 2..slot_offset + 4]
            .copy_from_slice(&(tuple_size as u16).to_le_bytes());

        // Update lower and upper
        self.set_lower(lower + SLOT_SIZE as u16);
        self.set_upper(new_upper);

        Ok(tuple_count)
    }

    pub fn get_tuple(&self, slot_id: u16) -> StorageResult<&[u8]> {
        let tuple_count = self.get_tuple_count();
        if slot_id >= tuple_count {
            return Err(StorageError::InvalidSlotId {
                slot_id,
                max_slot: tuple_count.saturating_sub(1),
            });
        }

        // Slots are now at the beginning, after header
        let slot_offset = HEADER_SIZE + (slot_id as usize * SLOT_SIZE);
        let tuple_offset = u16::from_le_bytes([self.data[slot_offset], self.data[slot_offset + 1]]);
        let tuple_length =
            u16::from_le_bytes([self.data[slot_offset + 2], self.data[slot_offset + 3]]);

        if tuple_offset == 0 && tuple_length == 0 {
            return Err(StorageError::TupleNotFound { slot_id });
        }

        Ok(&self.data[tuple_offset as usize..(tuple_offset + tuple_length) as usize])
    }

    pub fn delete_tuple(&mut self, slot_id: u16) -> StorageResult<()> {
        let tuple_count = self.get_tuple_count();
        if slot_id >= tuple_count {
            return Err(StorageError::InvalidSlotId {
                slot_id,
                max_slot: tuple_count.saturating_sub(1),
            });
        }

        // Mark slot as deleted (offset = 0, length = 0)
        let slot_offset = HEADER_SIZE + (slot_id as usize * SLOT_SIZE);
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

    fn get_lower(&self) -> u16 {
        u16::from_le_bytes([self.data[LOWER_OFFSET], self.data[LOWER_OFFSET + 1]])
    }

    fn set_lower(&mut self, lower: u16) {
        self.data[LOWER_OFFSET..LOWER_OFFSET + 2].copy_from_slice(&lower.to_le_bytes());
    }

    fn get_upper(&self) -> u16 {
        u16::from_le_bytes([self.data[UPPER_OFFSET], self.data[UPPER_OFFSET + 1]])
    }

    fn set_upper(&mut self, upper: u16) {
        self.data[UPPER_OFFSET..UPPER_OFFSET + 2].copy_from_slice(&upper.to_le_bytes());
    }

    pub fn get_tuple_count(&self) -> u16 {
        let lower = self.get_lower();
        if lower < HEADER_SIZE as u16 {
            return 0;
        }
        (lower - HEADER_SIZE as u16) / SLOT_SIZE as u16
    }

    pub fn get_free_space(&self) -> usize {
        let lower = self.get_lower();
        let upper = self.get_upper();

        upper.saturating_sub(lower) as usize
    }

    pub fn get_next_page_id(&self) -> Option<PageId> {
        let bytes = [
            self.data[SPECIAL_OFFSET],
            self.data[SPECIAL_OFFSET + 1],
            self.data[SPECIAL_OFFSET + 2],
            self.data[SPECIAL_OFFSET + 3],
        ];
        let next_page_id = u32::from_le_bytes(bytes);

        // 0xFFFFFFFF means no next page
        if next_page_id == 0xFFFFFFFF {
            None
        } else {
            Some(PageId(next_page_id))
        }
    }

    pub fn set_next_page_id(&mut self, next_page_id: Option<PageId>) {
        let value = match next_page_id {
            Some(PageId(id)) => id,
            None => 0xFFFFFFFF,
        };
        self.data[SPECIAL_OFFSET..SPECIAL_OFFSET + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn required_space_for(data_len: usize) -> usize {
        data_len + SLOT_SIZE
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
    use anyhow::Result;

    #[test]
    fn test_heap_page_initialization() {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let page_id = PageId(42);
        let page = HeapPage::new(&mut data, page_id);

        assert_eq!(page.page_id(), page_id);
        assert_eq!(page.get_tuple_count(), 0);
        assert_eq!(page.get_lower(), HEADER_SIZE as u16);
        assert_eq!(page.get_upper(), PAGE_SIZE as u16);
        assert_eq!(page.get_free_space(), PAGE_SIZE - HEADER_SIZE);
    }

    #[test]
    fn test_insert_and_get_tuple() -> Result<()> {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let mut page = HeapPage::new(&mut data, PageId(1));

        let tuple1 = b"Hello, World!";
        let slot1 = page
            .insert_tuple(tuple1)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        assert_eq!(slot1, 0);

        let tuple2 = b"Second tuple";
        let slot2 = page
            .insert_tuple(tuple2)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        assert_eq!(slot2, 1);

        assert_eq!(
            page.get_tuple(slot1)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?,
            tuple1
        );
        assert_eq!(
            page.get_tuple(slot2)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?,
            tuple2
        );
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

    #[test]
    fn test_next_page_id() -> Result<()> {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let mut page = HeapPage::new(&mut data, PageId(1));

        // Initially no next page
        assert_eq!(page.get_next_page_id(), None);

        // Set next page
        page.set_next_page_id(Some(PageId(5)));
        assert_eq!(page.get_next_page_id(), Some(PageId(5)));

        // Set to None again
        page.set_next_page_id(None);
        assert_eq!(page.get_next_page_id(), None);

        Ok(())
    }

    #[test]
    fn test_required_space_for() {
        assert_eq!(HeapPage::required_space_for(10), 14); // 10 + 4 (SLOT_SIZE)
        assert_eq!(HeapPage::required_space_for(100), 104);
        assert_eq!(HeapPage::required_space_for(0), 4);
    }
}
