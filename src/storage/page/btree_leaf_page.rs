use crate::access::TupleId;
use crate::storage::{PAGE_SIZE, Page, PageId};

const BTREE_LEAF_HEADER_SIZE: usize = 32;
pub const MIN_KEYS_PER_LEAF_PAGE: usize = 2;

// Slot entry for leaf page (key_offset, key_length, tuple_id)
#[derive(Debug, Clone, Copy)]
pub struct LeafSlot {
    key_offset: u16,   // 2 bytes
    key_length: u16,   // 2 bytes
    tuple_id: TupleId, // 6 bytes (4 bytes page_id + 2 bytes slot_id)
}

impl LeafSlot {
    const SIZE: usize = 10;

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= Self::SIZE);
        Self {
            key_offset: u16::from_le_bytes([bytes[0], bytes[1]]),
            key_length: u16::from_le_bytes([bytes[2], bytes[3]]),
            tuple_id: TupleId::from_bytes(&bytes[4..10]),
        }
    }

    fn to_bytes(self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..2].copy_from_slice(&self.key_offset.to_le_bytes());
        bytes[2..4].copy_from_slice(&self.key_length.to_le_bytes());
        bytes[4..10].copy_from_slice(&self.tuple_id.to_bytes());
        bytes
    }
}

// Manual layout to ensure exactly 32 bytes
pub struct BTreeLeafPageHeader {
    data: [u8; BTREE_LEAF_HEADER_SIZE],
}

impl BTreeLeafPageHeader {
    fn new() -> Self {
        Self {
            data: [0; BTREE_LEAF_HEADER_SIZE],
        }
    }

    #[allow(dead_code)]
    fn page_type(&self) -> u8 {
        self.data[0]
    }

    fn set_page_type(&mut self, val: u8) {
        self.data[0] = val;
    }

    #[allow(dead_code)]
    fn lower(&self) -> u16 {
        u16::from_le_bytes([self.data[2], self.data[3]])
    }

    fn set_lower(&mut self, val: u16) {
        let bytes = val.to_le_bytes();
        self.data[2] = bytes[0];
        self.data[3] = bytes[1];
    }

    #[allow(dead_code)]
    fn upper(&self) -> u16 {
        u16::from_le_bytes([self.data[4], self.data[5]])
    }

    fn set_upper(&mut self, val: u16) {
        let bytes = val.to_le_bytes();
        self.data[4] = bytes[0];
        self.data[5] = bytes[1];
    }

    fn slot_count(&self) -> u16 {
        u16::from_le_bytes([self.data[6], self.data[7]])
    }

    fn set_slot_count(&mut self, val: u16) {
        let bytes = val.to_le_bytes();
        self.data[6] = bytes[0];
        self.data[7] = bytes[1];
    }

    fn free_space(&self) -> u16 {
        u16::from_le_bytes([self.data[8], self.data[9]])
    }

    fn set_free_space(&mut self, val: u16) {
        let bytes = val.to_le_bytes();
        self.data[8] = bytes[0];
        self.data[9] = bytes[1];
    }

    #[allow(dead_code)]
    fn page_lsn(&self) -> u64 {
        u64::from_le_bytes([
            self.data[10],
            self.data[11],
            self.data[12],
            self.data[13],
            self.data[14],
            self.data[15],
            self.data[16],
            self.data[17],
        ])
    }

    fn set_page_lsn(&mut self, val: u64) {
        let bytes = val.to_le_bytes();
        self.data[10..18].copy_from_slice(&bytes);
    }

    fn next_page_id(&self) -> u32 {
        u32::from_le_bytes([self.data[18], self.data[19], self.data[20], self.data[21]])
    }

    fn set_next_page_id(&mut self, val: u32) {
        let bytes = val.to_le_bytes();
        self.data[18..22].copy_from_slice(&bytes);
    }

    fn prev_page_id(&self) -> u32 {
        u32::from_le_bytes([self.data[22], self.data[23], self.data[24], self.data[25]])
    }

    fn set_prev_page_id(&mut self, val: u32) {
        let bytes = val.to_le_bytes();
        self.data[22..26].copy_from_slice(&bytes);
    }

    // Reserved bytes: self.data[26..32]
}

pub struct BTreeLeafPage {
    pub page_id: PageId,
    data: [u8; PAGE_SIZE],
}

impl BTreeLeafPage {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self {
            page_id,
            data: [0; PAGE_SIZE],
        };

        // Initialize header
        let mut header = BTreeLeafPageHeader::new();
        header.set_page_type(0x02); // Leaf page type
        header.set_lower(BTREE_LEAF_HEADER_SIZE as u16);
        header.set_upper(PAGE_SIZE as u16);
        header.set_slot_count(0);
        header.set_free_space((PAGE_SIZE - BTREE_LEAF_HEADER_SIZE) as u16);
        header.set_page_lsn(0);
        header.set_next_page_id(0); // No next page initially
        header.set_prev_page_id(0); // No prev page initially
        page.write_header(&header);

        page
    }

    fn header(&self) -> BTreeLeafPageHeader {
        let mut header = BTreeLeafPageHeader::new();
        header
            .data
            .copy_from_slice(&self.data[..BTREE_LEAF_HEADER_SIZE]);
        header
    }

    fn write_header(&mut self, header: &BTreeLeafPageHeader) {
        self.data[..BTREE_LEAF_HEADER_SIZE].copy_from_slice(&header.data);
    }

    pub fn slot_count(&self) -> u16 {
        self.header().slot_count()
    }

    pub fn free_space(&self) -> u16 {
        self.header().free_space()
    }

    pub fn next_page_id(&self) -> Option<PageId> {
        let id = self.header().next_page_id();
        if id == 0 { None } else { Some(PageId(id)) }
    }

    pub fn set_next_page_id(&mut self, page_id: Option<PageId>) {
        let mut header = self.header();
        header.set_next_page_id(page_id.map(|p| p.0).unwrap_or(0));
        self.write_header(&header);
    }

    pub fn prev_page_id(&self) -> Option<PageId> {
        let id = self.header().prev_page_id();
        if id == 0 { None } else { Some(PageId(id)) }
    }

    pub fn set_prev_page_id(&mut self, page_id: Option<PageId>) {
        let mut header = self.header();
        header.set_prev_page_id(page_id.map(|p| p.0).unwrap_or(0));
        self.write_header(&header);
    }

    // Slot array management
    fn slot_offset(&self, index: usize) -> usize {
        BTREE_LEAF_HEADER_SIZE + index * LeafSlot::SIZE
    }

    pub fn get_slot(&self, index: usize) -> Option<LeafSlot> {
        if index >= self.slot_count() as usize {
            return None;
        }

        let offset = self.slot_offset(index);
        let slot_data = &self.data[offset..offset + LeafSlot::SIZE];
        Some(LeafSlot::from_bytes(slot_data))
    }

    pub fn insert_slot(&mut self, index: usize, slot: LeafSlot) -> Result<(), &'static str> {
        let slot_count = self.slot_count() as usize;
        if index > slot_count {
            return Err("Invalid slot index");
        }

        // Check space
        let required_space = LeafSlot::SIZE;
        if self.free_space() < required_space as u16 {
            return Err("Insufficient space for new slot");
        }

        // Shift existing slots
        if index < slot_count {
            let src = self.slot_offset(index);
            let dst = self.slot_offset(index + 1);
            let len = (slot_count - index) * LeafSlot::SIZE;
            self.data.copy_within(src..src + len, dst);
        }

        // Write new slot
        let offset = self.slot_offset(index);
        let slot_bytes = slot.to_bytes();
        self.data[offset..offset + LeafSlot::SIZE].copy_from_slice(&slot_bytes);

        // Update header
        let mut header = self.header();
        header.set_slot_count((slot_count + 1) as u16);
        header.set_lower((BTREE_LEAF_HEADER_SIZE + (slot_count + 1) * LeafSlot::SIZE) as u16);
        header.set_free_space(self.free_space() - required_space as u16);
        self.write_header(&header);

        Ok(())
    }

    pub fn delete_slot(&mut self, index: usize) -> Result<(), &'static str> {
        let slot_count = self.slot_count() as usize;
        if index >= slot_count {
            return Err("Invalid slot index");
        }

        // Shift remaining slots
        if index < slot_count - 1 {
            let src = self.slot_offset(index + 1);
            let dst = self.slot_offset(index);
            let len = (slot_count - index - 1) * LeafSlot::SIZE;
            self.data.copy_within(src..src + len, dst);
        }

        // Update header
        let mut header = self.header();
        header.set_slot_count((slot_count - 1) as u16);
        header.set_lower((BTREE_LEAF_HEADER_SIZE + (slot_count - 1) * LeafSlot::SIZE) as u16);
        header.set_free_space(self.free_space() + LeafSlot::SIZE as u16);
        self.write_header(&header);

        Ok(())
    }

    pub fn get_key(&self, slot_index: usize) -> Option<Vec<u8>> {
        let slot = self.get_slot(slot_index)?;

        if slot.key_length == 0 {
            return Some(Vec::new());
        }

        let key_start = slot.key_offset as usize;
        let key_end = key_start + slot.key_length as usize;

        if key_end > PAGE_SIZE {
            return None;
        }

        Some(self.data[key_start..key_end].to_vec())
    }

    pub fn get_tuple_id(&self, slot_index: usize) -> Option<TupleId> {
        self.get_slot(slot_index).map(|slot| slot.tuple_id)
    }

    pub fn available_space(&self) -> usize {
        let header = self.header();
        (header.upper() - header.lower()) as usize
    }

    // Key insertion with proper ordering
    pub fn insert_key(&mut self, slot_index: usize, key: &[u8]) -> Result<(), &'static str> {
        let slot_count = self.slot_count() as usize;
        if slot_index >= slot_count {
            return Err("Invalid slot index");
        }

        // Check available space in key area
        let key_len = key.len();
        if key_len > self.free_space() as usize {
            return Err("Insufficient space for key");
        }

        let header = self.header();
        let new_upper = header.upper() - key_len as u16;

        // Write key data at new upper position
        self.data[new_upper as usize..new_upper as usize + key_len].copy_from_slice(key);

        // Update slot with key location
        let mut slot = self.get_slot(slot_index).ok_or("Slot not found")?;
        slot.key_offset = new_upper;
        slot.key_length = key_len as u16;

        // Rewrite slot
        let offset = self.slot_offset(slot_index);
        let slot_bytes = slot.to_bytes();
        self.data[offset..offset + LeafSlot::SIZE].copy_from_slice(&slot_bytes);

        // Update header
        let mut header = self.header();
        header.set_upper(new_upper);
        header.set_free_space(header.free_space() - key_len as u16);
        self.write_header(&header);

        Ok(())
    }

    pub fn compact(&mut self) {
        // Collect all valid keys
        let mut keys = Vec::new();
        for i in 0..self.slot_count() as usize {
            if let Some(key) = self.get_key(i) {
                keys.push((i, key));
            }
        }

        // Reset upper to page size
        let mut new_upper = PAGE_SIZE as u16;

        // Rewrite keys from the end of the page
        for (slot_index, key) in keys.iter() {
            new_upper -= key.len() as u16;
            self.data[new_upper as usize..new_upper as usize + key.len()].copy_from_slice(key);

            // Update slot
            if let Some(mut slot) = self.get_slot(*slot_index) {
                slot.key_offset = new_upper;
                slot.key_length = key.len() as u16;

                let offset = self.slot_offset(*slot_index);
                let slot_bytes = slot.to_bytes();
                self.data[offset..offset + LeafSlot::SIZE].copy_from_slice(&slot_bytes);
            }
        }

        // Update header
        let mut header = self.header();
        let lower = header.lower();
        header.set_upper(new_upper);
        header.set_free_space(new_upper - lower);
        self.write_header(&header);
    }

    // Insert key-value pair with duplicate key support
    pub fn insert_key_value(
        &mut self,
        key: &[u8],
        tuple_id: TupleId,
    ) -> Result<usize, &'static str> {
        // Find insertion position - keys are sorted, duplicates ordered by TupleId
        let slot_count = self.slot_count() as usize;
        let mut insert_pos = slot_count;

        for i in 0..slot_count {
            if let Some(existing_key) = self.get_key(i) {
                match key.cmp(existing_key.as_slice()) {
                    std::cmp::Ordering::Less => {
                        insert_pos = i;
                        break;
                    }
                    std::cmp::Ordering::Equal => {
                        // For duplicate keys, order by TupleId
                        if let Some(existing_tuple_id) = self.get_tuple_id(i) {
                            if tuple_id < existing_tuple_id {
                                insert_pos = i;
                                break;
                            }
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        // Continue searching
                    }
                }
            }
        }

        // Check space for new slot and key
        let required_space = LeafSlot::SIZE + key.len();
        if required_space > self.available_space() {
            return Err("Insufficient space for new key and slot");
        }

        // Store key first
        let header = self.header();
        let key_offset = header.upper() - key.len() as u16;
        self.data[key_offset as usize..key_offset as usize + key.len()].copy_from_slice(key);

        // Create and insert slot
        let slot = LeafSlot {
            key_offset,
            key_length: key.len() as u16,
            tuple_id,
        };

        self.insert_slot(insert_pos, slot)?;

        // Update upper pointer (insert_slot already updated lower and free_space)
        let mut header = self.header();
        header.set_upper(key_offset);
        header.set_free_space(header.free_space() - key.len() as u16);
        self.write_header(&header);

        Ok(insert_pos)
    }

    // Find all tuple IDs for a given key (handles duplicates)
    pub fn find_tuples_for_key(&self, search_key: &[u8]) -> Vec<TupleId> {
        let mut results = Vec::new();
        let slot_count = self.slot_count() as usize;

        for i in 0..slot_count {
            if let Some(key) = self.get_key(i) {
                match key.as_slice().cmp(search_key) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => {
                        if let Some(tuple_id) = self.get_tuple_id(i) {
                            results.push(tuple_id);
                        }
                    }
                    std::cmp::Ordering::Greater => break, // Keys are sorted, no more matches
                }
            }
        }

        results
    }

    // Check if a key exists in the page
    pub fn contains_key(&self, search_key: &[u8]) -> bool {
        let slot_count = self.slot_count() as usize;

        for i in 0..slot_count {
            if let Some(key) = self.get_key(i) {
                match key.as_slice().cmp(search_key) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => return true,
                    std::cmp::Ordering::Greater => return false,
                }
            }
        }

        false
    }

    // Delete a specific key-value pair
    pub fn delete_key_value(
        &mut self,
        key_to_delete: &[u8],
        tuple_id: TupleId,
    ) -> Result<(), &'static str> {
        let slot_count = self.slot_count() as usize;
        let mut delete_index = None;

        // Find the exact key-value pair to delete
        for i in 0..slot_count {
            if let Some(key) = self.get_key(i) {
                match key.as_slice().cmp(key_to_delete) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => {
                        if let Some(tid) = self.get_tuple_id(i) {
                            if tid == tuple_id {
                                delete_index = Some(i);
                                break;
                            }
                        }
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
        }

        match delete_index {
            Some(index) => {
                self.delete_slot(index)?;
                // Compact after deletion to reclaim key space
                self.compact();
                Ok(())
            }
            None => Err("Key-value pair not found"),
        }
    }

    // Delete all entries for a given key
    pub fn delete_all_for_key(&mut self, key_to_delete: &[u8]) -> Result<usize, &'static str> {
        let mut deleted_count = 0;
        let mut i = 0;

        while i < self.slot_count() as usize {
            if let Some(key) = self.get_key(i) {
                match key.as_slice().cmp(key_to_delete) {
                    std::cmp::Ordering::Less => {
                        i += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        self.delete_slot(i)?;
                        deleted_count += 1;
                        // Don't increment i because slots have shifted
                    }
                    std::cmp::Ordering::Greater => {
                        break;
                    }
                }
            } else {
                i += 1;
            }
        }

        if deleted_count > 0 {
            self.compact();
            Ok(deleted_count)
        } else {
            Err("Key not found")
        }
    }

    // Get the minimum key in the page
    pub fn get_min_key(&self) -> Option<Vec<u8>> {
        if self.slot_count() == 0 {
            None
        } else {
            self.get_key(0)
        }
    }

    // Get the maximum key in the page
    pub fn get_max_key(&self) -> Option<Vec<u8>> {
        let slot_count = self.slot_count();
        if slot_count == 0 {
            None
        } else {
            self.get_key((slot_count - 1) as usize)
        }
    }

    // Check if the page needs split (based on available space)
    pub fn needs_split(&self) -> bool {
        // Split when less than 20% space remaining
        self.available_space() < (PAGE_SIZE / 5)
    }

    // Check if the page needs merge (based on number of entries)
    pub fn needs_merge(&self) -> bool {
        self.slot_count() < MIN_KEYS_PER_LEAF_PAGE as u16
    }

    // Binary search for a key within the page
    pub fn search(&self, search_key: &[u8]) -> Option<usize> {
        let slot_count = self.slot_count() as usize;
        if slot_count == 0 {
            return None;
        }

        let mut left = 0;
        let mut right = slot_count;

        while left < right {
            let mid = left + (right - left) / 2;

            if let Some(key) = self.get_key(mid) {
                match key.as_slice().cmp(search_key) {
                    std::cmp::Ordering::Less => left = mid + 1,
                    std::cmp::Ordering::Equal => {
                        // Found the key, but need to find first occurrence for duplicates
                        let mut first = mid;
                        while first > 0 {
                            if let Some(prev_key) = self.get_key(first - 1) {
                                if prev_key.as_slice() != search_key {
                                    break;
                                }
                            }
                            first -= 1;
                        }
                        return Some(first);
                    }
                    std::cmp::Ordering::Greater => right = mid,
                }
            } else {
                // Key not found at this position, shouldn't happen
                return None;
            }
        }

        None
    }

    // Find the position where a key should be inserted (for navigation)
    pub fn find_insert_position(&self, search_key: &[u8]) -> usize {
        let slot_count = self.slot_count() as usize;
        if slot_count == 0 {
            return 0;
        }

        let mut left = 0;
        let mut right = slot_count;

        while left < right {
            let mid = left + (right - left) / 2;

            if let Some(key) = self.get_key(mid) {
                if key.as_slice() < search_key {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                return left;
            }
        }

        left
    }

    // Range scan within the page
    pub fn range_scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        include_start: bool,
        include_end: bool,
    ) -> Vec<(Vec<u8>, TupleId)> {
        let mut results = Vec::new();
        let slot_count = self.slot_count() as usize;

        // Find start position
        let start_pos = if let Some(start) = start_key {
            let pos = self.find_insert_position(start);
            if !include_start {
                // If we found an exact match and don't want to include it, skip
                if pos < slot_count {
                    if let Some(key) = self.get_key(pos) {
                        if key.as_slice() == start {
                            pos + 1
                        } else {
                            pos
                        }
                    } else {
                        pos
                    }
                } else {
                    pos
                }
            } else {
                pos
            }
        } else {
            0
        };

        // Scan from start position
        for i in start_pos..slot_count {
            if let Some(key) = self.get_key(i) {
                // Check if we've passed the end key
                if let Some(end) = end_key {
                    match key.as_slice().cmp(end) {
                        std::cmp::Ordering::Greater => break,
                        std::cmp::Ordering::Equal => {
                            if !include_end {
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                if let Some(tuple_id) = self.get_tuple_id(i) {
                    results.push((key, tuple_id));
                }
            }
        }

        results
    }

    // Count keys in a range (useful for selectivity estimation)
    pub fn count_range(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        include_start: bool,
        include_end: bool,
    ) -> usize {
        let slot_count = self.slot_count() as usize;

        // Find start position
        let start_pos = if let Some(start) = start_key {
            let pos = self.find_insert_position(start);
            if !include_start && pos < slot_count {
                if let Some(key) = self.get_key(pos) {
                    if key.as_slice() == start {
                        pos + 1
                    } else {
                        pos
                    }
                } else {
                    pos
                }
            } else {
                pos
            }
        } else {
            0
        };

        let mut count = 0;
        for i in start_pos..slot_count {
            if let Some(key) = self.get_key(i) {
                if let Some(end) = end_key {
                    match key.as_slice().cmp(end) {
                        std::cmp::Ordering::Greater => break,
                        std::cmp::Ordering::Equal => {
                            if include_end {
                                count += 1;
                            } else {
                                break;
                            }
                        }
                        _ => count += 1,
                    }
                } else {
                    count += 1;
                }
            }
        }

        count
    }

    // Page split logic for leaf nodes
    pub fn split(&mut self) -> Result<(BTreeLeafPage, Vec<u8>), &'static str> {
        let slot_count = self.slot_count() as usize;
        if slot_count < MIN_KEYS_PER_LEAF_PAGE * 2 {
            return Err("Not enough keys to split");
        }

        // Calculate split point (middle)
        let split_index = slot_count / 2;

        // Create new page
        let mut new_page = BTreeLeafPage::new(PageId(0)); // PageId will be assigned by caller

        // Store original next pointer
        let original_next = self.next_page_id();

        // Collect keys and slots to move
        let mut keys_to_move = Vec::new();
        let mut slots_to_move = Vec::new();

        for i in split_index..slot_count {
            if let Some(key) = self.get_key(i) {
                keys_to_move.push(key);
            }
            if let Some(slot) = self.get_slot(i) {
                slots_to_move.push(slot);
            }
        }

        // Move keys to new page
        for (key, slot) in keys_to_move.iter().zip(slots_to_move.iter()) {
            new_page.insert_key_value(key, slot.tuple_id)?;
        }

        // Delete moved keys from original page
        for _ in split_index..slot_count {
            self.delete_slot(split_index)?;
        }

        // Compact original page
        self.compact();

        // Update sibling pointers - new page goes after self
        new_page.set_next_page_id(original_next);
        new_page.set_prev_page_id(Some(self.page_id));
        // Note: self.set_next_page_id() should be called by the caller after assigning page_id to new_page

        // The first key in the new page is the separator key for parent
        let separator_key = new_page.get_min_key().ok_or("New page has no keys")?;

        Ok((new_page, separator_key))
    }

    pub fn can_split(&self) -> bool {
        self.slot_count() as usize >= MIN_KEYS_PER_LEAF_PAGE * 2
    }

    // Page merge logic for leaf nodes
    pub fn can_merge_with(&self, other: &BTreeLeafPage) -> bool {
        // Check if both pages can fit in one page
        let total_slots = self.slot_count() + other.slot_count();
        let total_keys_size = self.calculate_keys_size() + other.calculate_keys_size();
        let required_space = (total_slots as usize * LeafSlot::SIZE) + total_keys_size;

        required_space <= (PAGE_SIZE - BTREE_LEAF_HEADER_SIZE)
    }

    fn calculate_keys_size(&self) -> usize {
        let mut total = 0;
        for i in 0..self.slot_count() {
            if let Some(key) = self.get_key(i as usize) {
                total += key.len();
            }
        }
        total
    }

    pub fn merge_with(&mut self, other: &BTreeLeafPage) -> Result<(), &'static str> {
        if !self.can_merge_with(other) {
            return Err("Pages cannot be merged - insufficient space");
        }

        // Copy all slots and keys from other page
        for i in 0..other.slot_count() {
            if let Some(slot) = other.get_slot(i as usize) {
                if let Some(key) = other.get_key(i as usize) {
                    self.insert_key_value(&key, slot.tuple_id)?;
                }
            }
        }

        // Update sibling pointers (assuming other is to the right of self)
        if let Some(next_id) = other.next_page_id() {
            self.set_next_page_id(Some(next_id));
        } else {
            self.set_next_page_id(None);
        }

        Ok(())
    }

    // Redistribute keys between two sibling pages
    pub fn redistribute_with(
        &mut self,
        sibling: &mut BTreeLeafPage,
        is_left_sibling: bool,
    ) -> Result<Vec<u8>, &'static str> {
        let self_count = self.slot_count() as usize;
        let sibling_count = sibling.slot_count() as usize;
        let total_count = self_count + sibling_count;

        if total_count < MIN_KEYS_PER_LEAF_PAGE * 2 {
            return Err("Not enough keys to redistribute");
        }

        let target_count = total_count / 2;

        if is_left_sibling {
            // Move keys from left sibling to self
            if sibling_count <= target_count {
                return Err("Sibling doesn't have enough keys to redistribute");
            }

            let mut moved_keys = Vec::new();
            let mut moved_slots = Vec::new();

            // Collect keys from the end of left sibling
            for i in (target_count..sibling_count).rev() {
                if let Some(key) = sibling.get_key(i) {
                    moved_keys.push(key);
                }
                if let Some(slot) = sibling.get_slot(i) {
                    moved_slots.push(slot);
                }
            }

            // Delete moved keys from sibling
            for _ in target_count..sibling_count {
                sibling.delete_slot(target_count)?;
            }
            sibling.compact();

            // Insert moved keys at the beginning of self
            // Need to reverse because we collected in reverse order
            for (key, slot) in moved_keys.iter().rev().zip(moved_slots.iter().rev()) {
                self.insert_key_value(key, slot.tuple_id)?;
            }

            // The new separator is the minimum key in self
            self.get_min_key()
                .ok_or("Self has no keys after redistribution")
        } else {
            // Move keys from self to right sibling
            if self_count <= target_count {
                return Err("Not enough keys to redistribute to right sibling");
            }

            let mut moved_keys = Vec::new();
            let mut moved_slots = Vec::new();

            // Collect keys from the end of self
            for i in target_count..self_count {
                if let Some(key) = self.get_key(i) {
                    moved_keys.push(key);
                }
                if let Some(slot) = self.get_slot(i) {
                    moved_slots.push(slot);
                }
            }

            // Delete moved keys from self
            for _ in target_count..self_count {
                self.delete_slot(target_count)?;
            }
            self.compact();

            // Insert moved keys at the beginning of sibling
            for (key, slot) in moved_keys.iter().zip(moved_slots.iter()) {
                sibling.insert_key_value(key, slot.tuple_id)?;
            }

            // The new separator is the minimum key in sibling
            sibling
                .get_min_key()
                .ok_or("Sibling has no keys after redistribution")
        }
    }

    // Serialize the page to bytes (for persistence)
    pub fn serialize(&self) -> Vec<u8> {
        self.data.to_vec()
    }

    // Deserialize from bytes
    pub fn deserialize(page_id: PageId, data: &[u8]) -> Result<Self, &'static str> {
        if data.len() != PAGE_SIZE {
            return Err("Invalid page size for deserialization");
        }

        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(data);

        let page = Self {
            page_id,
            data: page_data,
        };

        // Validate header
        let header = page.header();
        if header.page_type() != 0x02 {
            return Err("Invalid page type for leaf page");
        }

        // Basic sanity checks
        if header.lower() < BTREE_LEAF_HEADER_SIZE as u16 {
            return Err("Invalid lower pointer");
        }
        if header.upper() > PAGE_SIZE as u16 {
            return Err("Invalid upper pointer");
        }
        if header.lower() > header.upper() {
            return Err("Lower pointer exceeds upper pointer");
        }

        // Validate slot count
        let expected_lower =
            BTREE_LEAF_HEADER_SIZE + (header.slot_count() as usize * LeafSlot::SIZE);
        if expected_lower as u16 != header.lower() {
            return Err("Inconsistent slot count and lower pointer");
        }

        Ok(page)
    }

    // Create a page from raw data (useful for buffer pool integration)
    pub fn from_data(page_id: PageId, data: &mut [u8; PAGE_SIZE]) -> Self {
        Self {
            page_id,
            data: *data,
        }
    }

    // Check page integrity
    pub fn validate(&self) -> Result<(), &'static str> {
        let header = self.header();

        // Check page type
        if header.page_type() != 0x02 {
            return Err("Invalid page type");
        }

        // Check pointer consistency
        if header.lower() < BTREE_LEAF_HEADER_SIZE as u16 {
            return Err("Lower pointer too small");
        }
        if header.upper() > PAGE_SIZE as u16 {
            return Err("Upper pointer too large");
        }
        if header.lower() > header.upper() {
            return Err("Lower exceeds upper");
        }

        // Check slot array bounds
        let slot_end = BTREE_LEAF_HEADER_SIZE + (header.slot_count() as usize * LeafSlot::SIZE);
        if slot_end > header.lower() as usize {
            return Err("Slot array exceeds lower bound");
        }

        // Validate each slot
        for i in 0..header.slot_count() {
            if let Some(slot) = self.get_slot(i as usize) {
                // Check key bounds
                if slot.key_offset < header.upper() {
                    return Err("Key offset below upper bound");
                }
                if slot.key_offset as usize + slot.key_length as usize > PAGE_SIZE {
                    return Err("Key extends beyond page");
                }
            }
        }

        // Check keys are in sorted order
        let mut prev_key: Option<Vec<u8>> = None;
        for i in 0..header.slot_count() {
            if let Some(key) = self.get_key(i as usize) {
                if let Some(prev) = prev_key {
                    if prev > key {
                        return Err("Keys not in sorted order");
                    }
                }
                prev_key = Some(key);
            }
        }

        Ok(())
    }
}

impl Page for BTreeLeafPage {
    fn page_id(&self) -> PageId {
        self.page_id
    }

    fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(
            std::mem::size_of::<BTreeLeafPageHeader>(),
            BTREE_LEAF_HEADER_SIZE
        );
    }

    #[test]
    fn test_new_leaf_page() {
        let page = BTreeLeafPage::new(PageId(42));

        assert_eq!(page.page_id(), PageId(42));
        let header = page.header();
        assert_eq!(header.page_type(), 0x02);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(header.lower(), BTREE_LEAF_HEADER_SIZE as u16);
        assert_eq!(header.upper(), PAGE_SIZE as u16);
        assert_eq!(
            page.free_space(),
            (PAGE_SIZE - BTREE_LEAF_HEADER_SIZE) as u16
        );
        assert_eq!(page.next_page_id(), None);
        assert_eq!(page.prev_page_id(), None);
    }

    #[test]
    fn test_sibling_pointers() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Initially no siblings
        assert_eq!(page.next_page_id(), None);
        assert_eq!(page.prev_page_id(), None);

        // Set next page
        page.set_next_page_id(Some(PageId(2)));
        assert_eq!(page.next_page_id(), Some(PageId(2)));
        assert_eq!(page.prev_page_id(), None);

        // Set prev page
        page.set_prev_page_id(Some(PageId(3)));
        assert_eq!(page.next_page_id(), Some(PageId(2)));
        assert_eq!(page.prev_page_id(), Some(PageId(3)));

        // Clear next page
        page.set_next_page_id(None);
        assert_eq!(page.next_page_id(), None);
        assert_eq!(page.prev_page_id(), Some(PageId(3)));

        // Clear prev page
        page.set_prev_page_id(None);
        assert_eq!(page.next_page_id(), None);
        assert_eq!(page.prev_page_id(), None);
    }

    #[test]
    fn test_page_trait_implementation() {
        let mut page = BTreeLeafPage::new(PageId(100));

        // Test Page trait methods
        assert_eq!(page.page_id(), PageId(100));

        // Test data access
        let data = page.data();
        assert_eq!(data.len(), PAGE_SIZE);

        // Test mutable data access
        let data_mut = page.data_mut();
        data_mut[1000] = 42;
        assert_eq!(page.data()[1000], 42);
    }

    #[test]
    fn test_leaf_slot() {
        let slot = LeafSlot {
            key_offset: 1000,
            key_length: 25,
            tuple_id: TupleId::new(PageId(10), 5),
        };

        let bytes = slot.to_bytes();
        assert_eq!(bytes.len(), LeafSlot::SIZE);

        let decoded = LeafSlot::from_bytes(&bytes);
        assert_eq!(decoded.key_offset, 1000);
        assert_eq!(decoded.key_length, 25);
        assert_eq!(decoded.tuple_id, TupleId::new(PageId(10), 5));
    }

    #[test]
    fn test_slot_insert() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert first slot
        let slot1 = LeafSlot {
            key_offset: 1000,
            key_length: 5,
            tuple_id: TupleId::new(PageId(10), 1),
        };
        assert!(page.insert_slot(0, slot1).is_ok());
        assert_eq!(page.slot_count(), 1);

        // Insert second slot at beginning
        let slot2 = LeafSlot {
            key_offset: 2000,
            key_length: 10,
            tuple_id: TupleId::new(PageId(20), 2),
        };
        assert!(page.insert_slot(0, slot2).is_ok());
        assert_eq!(page.slot_count(), 2);

        // Verify slots
        let retrieved1 = page.get_slot(0).unwrap();
        assert_eq!(retrieved1.key_offset, 2000);
        assert_eq!(retrieved1.tuple_id, TupleId::new(PageId(20), 2));

        let retrieved2 = page.get_slot(1).unwrap();
        assert_eq!(retrieved2.key_offset, 1000);
        assert_eq!(retrieved2.tuple_id, TupleId::new(PageId(10), 1));
    }

    #[test]
    fn test_slot_delete() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert multiple slots
        for i in 0..5 {
            let slot = LeafSlot {
                key_offset: 1000 + i as u16 * 100,
                key_length: 5,
                tuple_id: TupleId::new(PageId(i * 10), i as u16),
            };
            page.insert_slot(i as usize, slot).unwrap();
        }

        assert_eq!(page.slot_count(), 5);

        // Delete middle slot
        assert!(page.delete_slot(2).is_ok());
        assert_eq!(page.slot_count(), 4);

        // Verify remaining slots
        assert_eq!(
            page.get_slot(0).unwrap().tuple_id,
            TupleId::new(PageId(0), 0)
        );
        assert_eq!(
            page.get_slot(1).unwrap().tuple_id,
            TupleId::new(PageId(10), 1)
        );
        assert_eq!(
            page.get_slot(2).unwrap().tuple_id,
            TupleId::new(PageId(30), 3)
        );
        assert_eq!(
            page.get_slot(3).unwrap().tuple_id,
            TupleId::new(PageId(40), 4)
        );
    }

    #[test]
    fn test_get_tuple_id() {
        let mut page = BTreeLeafPage::new(PageId(1));

        let slot = LeafSlot {
            key_offset: 0,
            key_length: 0,
            tuple_id: TupleId::new(PageId(999), 42),
        };
        page.insert_slot(0, slot).unwrap();

        assert_eq!(page.get_tuple_id(0), Some(TupleId::new(PageId(999), 42)));
        assert_eq!(page.get_tuple_id(1), None);
    }

    #[test]
    fn test_get_key() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Write key data
        let key_data = b"test_key";
        let key_offset = 1000;
        page.data[key_offset..key_offset + key_data.len()].copy_from_slice(key_data);

        // Insert slot pointing to key
        let slot = LeafSlot {
            key_offset: key_offset as u16,
            key_length: key_data.len() as u16,
            tuple_id: TupleId::new(PageId(100), 5),
        };
        page.insert_slot(0, slot).unwrap();

        // Retrieve key
        let retrieved_key = page.get_key(0).unwrap();
        assert_eq!(retrieved_key, key_data);
    }

    #[test]
    fn test_available_space() {
        let mut page = BTreeLeafPage::new(PageId(1));

        let initial_space = page.available_space();
        assert_eq!(initial_space, PAGE_SIZE - BTREE_LEAF_HEADER_SIZE);

        // Insert a slot
        let slot = LeafSlot {
            key_offset: 0,
            key_length: 0,
            tuple_id: TupleId::new(PageId(1), 1),
        };
        page.insert_slot(0, slot).unwrap();

        // Available space should decrease by slot size
        assert_eq!(page.available_space(), initial_space - LeafSlot::SIZE);
    }

    #[test]
    fn test_insert_key() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert a slot first
        let slot = LeafSlot {
            key_offset: 0,
            key_length: 0,
            tuple_id: TupleId::new(PageId(100), 1),
        };
        page.insert_slot(0, slot).unwrap();

        // Insert key for the slot
        let key = b"test_key";
        assert!(page.insert_key(0, key).is_ok());

        // Verify key was stored
        let retrieved_key = page.get_key(0).unwrap();
        assert_eq!(retrieved_key, key);

        // Verify upper pointer was updated
        let header = page.header();
        assert_eq!(header.upper(), PAGE_SIZE as u16 - key.len() as u16);
    }

    #[test]
    fn test_compact() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert multiple slots with keys
        let keys = vec![b"key1", b"key2", b"key3"];
        for (i, key) in keys.iter().enumerate() {
            let slot = LeafSlot {
                key_offset: 0,
                key_length: 0,
                tuple_id: TupleId::new(PageId(i as u32 * 10), i as u16),
            };
            page.insert_slot(i, slot).unwrap();
            page.insert_key(i, *key).unwrap();
        }

        // Manually fragment by deleting middle slot
        page.delete_slot(1).unwrap();

        // Insert new slot and key
        let new_slot = LeafSlot {
            key_offset: 0,
            key_length: 0,
            tuple_id: TupleId::new(PageId(999), 99),
        };
        page.insert_slot(1, new_slot).unwrap();
        page.insert_key(1, b"new_key").unwrap();

        let old_upper = page.header().upper();

        // Compact the page
        page.compact();

        // Verify all keys are still accessible
        assert_eq!(page.get_key(0).unwrap(), b"key1");
        assert_eq!(page.get_key(1).unwrap(), b"new_key");
        assert_eq!(page.get_key(2).unwrap(), b"key3");

        // Verify space was reclaimed
        let new_upper = page.header().upper();
        assert!(new_upper > old_upper);
    }

    #[test]
    fn test_insert_key_value_ordering() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert keys in random order
        page.insert_key_value(b"key3", TupleId::new(PageId(30), 3))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key5", TupleId::new(PageId(50), 5))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 2))
            .unwrap();
        page.insert_key_value(b"key4", TupleId::new(PageId(40), 4))
            .unwrap();

        // Verify keys are in order
        assert_eq!(page.get_key(0).unwrap(), b"key1");
        assert_eq!(page.get_key(1).unwrap(), b"key2");
        assert_eq!(page.get_key(2).unwrap(), b"key3");
        assert_eq!(page.get_key(3).unwrap(), b"key4");
        assert_eq!(page.get_key(4).unwrap(), b"key5");

        // Verify tuple IDs match
        assert_eq!(page.get_tuple_id(0).unwrap(), TupleId::new(PageId(10), 1));
        assert_eq!(page.get_tuple_id(1).unwrap(), TupleId::new(PageId(20), 2));
        assert_eq!(page.get_tuple_id(2).unwrap(), TupleId::new(PageId(30), 3));
        assert_eq!(page.get_tuple_id(3).unwrap(), TupleId::new(PageId(40), 4));
        assert_eq!(page.get_tuple_id(4).unwrap(), TupleId::new(PageId(50), 5));
    }

    #[test]
    fn test_duplicate_key_support() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert same key with different tuple IDs
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 3))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 2))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 1))
            .unwrap();

        assert_eq!(page.slot_count(), 4);

        // Verify all entries for key1 are ordered by TupleId
        assert_eq!(page.get_key(0).unwrap(), b"key1");
        assert_eq!(page.get_tuple_id(0).unwrap(), TupleId::new(PageId(10), 1));

        assert_eq!(page.get_key(1).unwrap(), b"key1");
        assert_eq!(page.get_tuple_id(1).unwrap(), TupleId::new(PageId(10), 2));

        assert_eq!(page.get_key(2).unwrap(), b"key1");
        assert_eq!(page.get_tuple_id(2).unwrap(), TupleId::new(PageId(10), 3));

        assert_eq!(page.get_key(3).unwrap(), b"key2");
        assert_eq!(page.get_tuple_id(3).unwrap(), TupleId::new(PageId(20), 1));
    }

    #[test]
    fn test_find_tuples_for_key() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert duplicate keys
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 2))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 1))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 3))
            .unwrap();

        // Find all tuples for key1
        let tuples = page.find_tuples_for_key(b"key1");
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0], TupleId::new(PageId(10), 1));
        assert_eq!(tuples[1], TupleId::new(PageId(10), 2));
        assert_eq!(tuples[2], TupleId::new(PageId(10), 3));

        // Find tuples for key2
        let tuples = page.find_tuples_for_key(b"key2");
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0], TupleId::new(PageId(20), 1));

        // Find tuples for non-existent key
        let tuples = page.find_tuples_for_key(b"key3");
        assert_eq!(tuples.len(), 0);
    }

    #[test]
    fn test_contains_key() {
        let mut page = BTreeLeafPage::new(PageId(1));

        page.insert_key_value(b"apple", TupleId::new(PageId(1), 1))
            .unwrap();
        page.insert_key_value(b"banana", TupleId::new(PageId(2), 1))
            .unwrap();
        page.insert_key_value(b"cherry", TupleId::new(PageId(3), 1))
            .unwrap();

        assert!(page.contains_key(b"apple"));
        assert!(page.contains_key(b"banana"));
        assert!(page.contains_key(b"cherry"));
        assert!(!page.contains_key(b"orange"));
        assert!(!page.contains_key(b"apricot"));
    }

    #[test]
    fn test_insert_key_value_space_check() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Fill page with keys until no space left
        let mut i = 0;
        loop {
            let key = format!("key{:04}", i).into_bytes();
            if page
                .insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .is_err()
            {
                break;
            }
            i += 1;
        }

        // Verify we filled some keys
        assert!(i > 0);
        assert_eq!(page.slot_count() as usize, i);

        // Try to insert one more - should fail
        let key = b"extra_key";
        assert!(
            page.insert_key_value(key, TupleId::new(PageId(999), 999))
                .is_err()
        );
    }

    #[test]
    fn test_delete_key_value() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert some key-value pairs
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 2))
            .unwrap();
        page.insert_key_value(b"key3", TupleId::new(PageId(30), 3))
            .unwrap();

        assert_eq!(page.slot_count(), 3);

        // Delete specific key-value pair
        assert!(
            page.delete_key_value(b"key2", TupleId::new(PageId(20), 2))
                .is_ok()
        );
        assert_eq!(page.slot_count(), 2);

        // Verify remaining keys
        assert_eq!(page.get_key(0).unwrap(), b"key1");
        assert_eq!(page.get_key(1).unwrap(), b"key3");

        // Try to delete non-existent key-value pair
        assert!(
            page.delete_key_value(b"key2", TupleId::new(PageId(20), 2))
                .is_err()
        );
        assert!(
            page.delete_key_value(b"key4", TupleId::new(PageId(40), 4))
                .is_err()
        );
    }

    #[test]
    fn test_delete_key_value_with_duplicates() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert duplicate keys with different tuple IDs
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 2))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 3))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 1))
            .unwrap();

        assert_eq!(page.slot_count(), 4);

        // Delete specific duplicate
        assert!(
            page.delete_key_value(b"key1", TupleId::new(PageId(10), 2))
                .is_ok()
        );
        assert_eq!(page.slot_count(), 3);

        // Verify remaining duplicates
        let tuples = page.find_tuples_for_key(b"key1");
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0], TupleId::new(PageId(10), 1));
        assert_eq!(tuples[1], TupleId::new(PageId(10), 3));
    }

    #[test]
    fn test_delete_all_for_key() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert duplicate keys
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 2))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 3))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 1))
            .unwrap();
        page.insert_key_value(b"key3", TupleId::new(PageId(30), 1))
            .unwrap();

        assert_eq!(page.slot_count(), 5);

        // Delete all entries for key1
        let deleted = page.delete_all_for_key(b"key1").unwrap();
        assert_eq!(deleted, 3);
        assert_eq!(page.slot_count(), 2);

        // Verify key1 is gone
        assert!(!page.contains_key(b"key1"));
        assert!(page.contains_key(b"key2"));
        assert!(page.contains_key(b"key3"));

        // Try to delete non-existent key
        assert!(page.delete_all_for_key(b"key4").is_err());
    }

    #[test]
    fn test_get_min_max_key() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Empty page
        assert_eq!(page.get_min_key(), None);
        assert_eq!(page.get_max_key(), None);

        // Add keys
        page.insert_key_value(b"banana", TupleId::new(PageId(2), 1))
            .unwrap();
        page.insert_key_value(b"apple", TupleId::new(PageId(1), 1))
            .unwrap();
        page.insert_key_value(b"cherry", TupleId::new(PageId(3), 1))
            .unwrap();

        assert_eq!(page.get_min_key().unwrap(), b"apple");
        assert_eq!(page.get_max_key().unwrap(), b"cherry");
    }

    #[test]
    fn test_needs_split() {
        let mut page = BTreeLeafPage::new(PageId(1));

        assert!(!page.needs_split()); // Empty page doesn't need split

        // Fill page until it needs split
        let mut i = 0;
        while !page.needs_split() {
            let key = format!("key{:04}", i).into_bytes();
            if page
                .insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .is_err()
            {
                break;
            }
            i += 1;
        }

        assert!(page.needs_split());
        assert!(page.available_space() < PAGE_SIZE / 5);
    }

    #[test]
    fn test_needs_merge() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Empty page needs merge
        assert!(page.needs_merge());

        // Add minimum keys
        for i in 0..MIN_KEYS_PER_LEAF_PAGE {
            page.insert_key_value(
                &format!("key{}", i).into_bytes(),
                TupleId::new(PageId(i as u32), i as u16),
            )
            .unwrap();
        }

        // Should not need merge anymore
        assert!(!page.needs_merge());

        // Delete one key to go below minimum
        page.delete_key_value(b"key0", TupleId::new(PageId(0), 0))
            .unwrap();
        assert!(page.needs_merge());
    }

    #[test]
    fn test_delete_with_compaction() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert several keys to fragment the page
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        let initial_free_space = page.free_space();

        // Delete alternating keys to create fragmentation
        for i in (0..10).step_by(2) {
            let key = format!("key{:02}", i).into_bytes();
            page.delete_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        // Free space should increase after deletion and compaction
        assert!(page.free_space() > initial_free_space);

        // Verify remaining keys
        assert_eq!(page.slot_count(), 5);
        for (i, expected) in [1, 3, 5, 7, 9].iter().enumerate() {
            let expected_key = format!("key{:02}", expected).into_bytes();
            assert_eq!(page.get_key(i).unwrap(), expected_key);
        }
    }

    #[test]
    fn test_page_split() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert enough keys to allow split
        for i in 0..8 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32 + 100), i as u16))
                .unwrap();
        }

        assert!(page.can_split());
        let original_count = page.slot_count();
        let original_next = page.next_page_id();

        // Perform split
        let (mut new_page, separator_key) = page.split().unwrap();
        new_page.page_id = PageId(2); // Assign page ID for test
        page.set_next_page_id(Some(PageId(2))); // Update next pointer after assigning ID

        // Verify split occurred
        assert!(page.slot_count() < original_count);
        assert!(new_page.slot_count() > 0);
        assert_eq!(page.slot_count() + new_page.slot_count(), original_count);

        // Verify separator key is the first key in new page
        assert_eq!(separator_key, new_page.get_min_key().unwrap());

        // Verify sibling pointers
        assert_eq!(page.next_page_id(), Some(PageId(2)));
        assert_eq!(new_page.prev_page_id(), Some(PageId(1)));
        assert_eq!(new_page.next_page_id(), original_next);

        // Verify all keys in left page are less than or equal to separator
        let max_left = page.get_max_key().unwrap();
        assert!(max_left.as_slice() <= separator_key.as_slice());

        // Verify keys are still in order
        for i in 0..page.slot_count() {
            let key = page.get_key(i as usize).unwrap();
            let expected = format!("key{:02}", i).into_bytes();
            assert_eq!(key, expected);
        }

        for i in 0..new_page.slot_count() {
            let key = new_page.get_key(i as usize).unwrap();
            let expected = format!("key{:02}", page.slot_count() + i).into_bytes();
            assert_eq!(key, expected);
        }
    }

    #[test]
    fn test_split_minimum_keys() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert less than minimum required for split
        page.insert_key_value(b"key1", TupleId::new(PageId(1), 1))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(2), 2))
            .unwrap();
        page.insert_key_value(b"key3", TupleId::new(PageId(3), 3))
            .unwrap();

        assert!(!page.can_split());
        assert!(page.split().is_err());
    }

    #[test]
    fn test_split_with_duplicates() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert keys including duplicates
        for i in 0..4 {
            page.insert_key_value(b"keyA", TupleId::new(PageId(10), i))
                .unwrap();
            page.insert_key_value(b"keyB", TupleId::new(PageId(20), i))
                .unwrap();
        }

        assert!(page.can_split());
        let (mut new_page, _separator_key) = page.split().unwrap();
        new_page.page_id = PageId(2);

        // Verify both pages have keys
        assert!(page.slot_count() > 0);
        assert!(new_page.slot_count() > 0);

        // Verify duplicates are preserved in order
        let left_tuples = page.find_tuples_for_key(b"keyA");
        let right_tuples = new_page.find_tuples_for_key(b"keyB");

        // Check tuple IDs are still in order
        for i in 1..left_tuples.len() {
            assert!(left_tuples[i - 1] < left_tuples[i]);
        }
        for i in 1..right_tuples.len() {
            assert!(right_tuples[i - 1] < right_tuples[i]);
        }
    }

    #[test]
    fn test_page_merge() {
        let mut page1 = BTreeLeafPage::new(PageId(1));
        let mut page2 = BTreeLeafPage::new(PageId(2));

        // Set up sibling pointers
        page1.set_next_page_id(Some(PageId(2)));
        page2.set_prev_page_id(Some(PageId(1)));
        page2.set_next_page_id(Some(PageId(3)));

        // Add keys to first page
        page1
            .insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page1
            .insert_key_value(b"key2", TupleId::new(PageId(20), 2))
            .unwrap();

        // Add keys to second page
        page2
            .insert_key_value(b"key3", TupleId::new(PageId(30), 3))
            .unwrap();
        page2
            .insert_key_value(b"key4", TupleId::new(PageId(40), 4))
            .unwrap();

        let original_count1 = page1.slot_count();
        let original_count2 = page2.slot_count();

        // Check merge is possible
        assert!(page1.can_merge_with(&page2));

        // Perform merge
        assert!(page1.merge_with(&page2).is_ok());

        // Verify merge occurred
        assert_eq!(page1.slot_count(), original_count1 + original_count2);

        // Verify all keys are present and in order
        assert_eq!(page1.get_key(0).unwrap(), b"key1");
        assert_eq!(page1.get_key(1).unwrap(), b"key2");
        assert_eq!(page1.get_key(2).unwrap(), b"key3");
        assert_eq!(page1.get_key(3).unwrap(), b"key4");

        // Verify tuple IDs
        assert_eq!(page1.get_tuple_id(0).unwrap(), TupleId::new(PageId(10), 1));
        assert_eq!(page1.get_tuple_id(1).unwrap(), TupleId::new(PageId(20), 2));
        assert_eq!(page1.get_tuple_id(2).unwrap(), TupleId::new(PageId(30), 3));
        assert_eq!(page1.get_tuple_id(3).unwrap(), TupleId::new(PageId(40), 4));

        // Verify sibling pointer update
        assert_eq!(page1.next_page_id(), Some(PageId(3)));
    }

    #[test]
    fn test_merge_insufficient_space() {
        let mut page1 = BTreeLeafPage::new(PageId(1));
        let mut page2 = BTreeLeafPage::new(PageId(2));

        // Fill both pages until they're almost full
        let mut i = 0;
        while page1.available_space() > 100 {
            let key = format!("key{:04}", i).into_bytes();
            if page1
                .insert_key_value(&key, TupleId::new(PageId(i), i as u16))
                .is_err()
            {
                break;
            }
            i += 1;
        }

        while page2.available_space() > 100 {
            let key = format!("key{:04}", i).into_bytes();
            if page2
                .insert_key_value(&key, TupleId::new(PageId(i), i as u16))
                .is_err()
            {
                break;
            }
            i += 1;
        }

        // Ensure both pages have content
        assert!(page1.slot_count() > 0);
        assert!(page2.slot_count() > 0);

        // Merge should not be possible if both pages are nearly full
        assert!(!page1.can_merge_with(&page2));
        assert!(page1.merge_with(&page2).is_err());
    }

    #[test]
    fn test_merge_preserves_order() {
        let mut page1 = BTreeLeafPage::new(PageId(1));
        let mut page2 = BTreeLeafPage::new(PageId(2));

        // Add alternating keys
        page1
            .insert_key_value(b"10", TupleId::new(PageId(10), 1))
            .unwrap();
        page1
            .insert_key_value(b"30", TupleId::new(PageId(30), 3))
            .unwrap();
        page1
            .insert_key_value(b"50", TupleId::new(PageId(50), 5))
            .unwrap();

        page2
            .insert_key_value(b"20", TupleId::new(PageId(20), 2))
            .unwrap();
        page2
            .insert_key_value(b"40", TupleId::new(PageId(40), 4))
            .unwrap();
        page2
            .insert_key_value(b"60", TupleId::new(PageId(60), 6))
            .unwrap();

        // Merge
        page1.merge_with(&page2).unwrap();

        // Verify keys are in sorted order
        assert_eq!(page1.slot_count(), 6);
        let keys: Vec<_> = (0..6).filter_map(|i| page1.get_key(i)).collect();
        for i in 1..keys.len() {
            assert!(keys[i - 1] < keys[i]);
        }
    }

    #[test]
    fn test_redistribute_from_left_sibling() {
        let mut left = BTreeLeafPage::new(PageId(1));
        let mut right = BTreeLeafPage::new(PageId(2));

        // Left sibling has more keys
        for i in 0..6 {
            left.insert_key_value(
                &format!("key{:02}", i).into_bytes(),
                TupleId::new(PageId(i as u32), i as u16),
            )
            .unwrap();
        }

        // Right sibling has fewer keys
        right
            .insert_key_value(b"key10", TupleId::new(PageId(10), 10))
            .unwrap();
        right
            .insert_key_value(b"key11", TupleId::new(PageId(11), 11))
            .unwrap();

        let left_count_before = left.slot_count();
        let right_count_before = right.slot_count();

        // Redistribute
        let separator = right.redistribute_with(&mut left, true).unwrap();

        // Verify counts are balanced
        let left_count_after = left.slot_count();
        let right_count_after = right.slot_count();
        assert!(left_count_after >= 3 && left_count_after <= 5);
        assert!(right_count_after >= 3 && right_count_after <= 5);
        assert_eq!(
            left_count_after + right_count_after,
            left_count_before + right_count_before
        );

        // Verify separator key
        assert!(!separator.is_empty());

        // Verify all keys in left are less than separator
        for i in 0..left_count_after {
            if let Some(key) = left.get_key(i as usize) {
                assert!(key < separator);
            }
        }

        // Verify all keys in right are greater than or equal to separator
        for i in 0..right_count_after {
            if let Some(key) = right.get_key(i as usize) {
                assert!(key >= separator);
            }
        }
    }

    #[test]
    fn test_redistribute_to_right_sibling() {
        let mut left = BTreeLeafPage::new(PageId(1));
        let mut right = BTreeLeafPage::new(PageId(2));

        // Left sibling has more keys
        for i in 0..6 {
            left.insert_key_value(
                &format!("key{:02}", i).into_bytes(),
                TupleId::new(PageId(i as u32), i as u16),
            )
            .unwrap();
        }

        // Right sibling has fewer keys
        right
            .insert_key_value(b"key10", TupleId::new(PageId(10), 10))
            .unwrap();
        right
            .insert_key_value(b"key11", TupleId::new(PageId(11), 11))
            .unwrap();

        let left_count_before = left.slot_count();
        let right_count_before = right.slot_count();

        // Redistribute
        let separator = left.redistribute_with(&mut right, false).unwrap();

        // Verify counts are balanced
        let left_count_after = left.slot_count();
        let right_count_after = right.slot_count();
        assert!(left_count_after >= 3 && left_count_after <= 5);
        assert!(right_count_after >= 3 && right_count_after <= 5);
        assert_eq!(
            left_count_after + right_count_after,
            left_count_before + right_count_before
        );

        // Verify separator key
        assert!(!separator.is_empty());

        // Verify all keys in left are less than separator
        for i in 0..left_count_after {
            if let Some(key) = left.get_key(i as usize) {
                assert!(key < separator);
            }
        }

        // Verify all keys in right are greater than or equal to separator
        for i in 0..right_count_after {
            if let Some(key) = right.get_key(i as usize) {
                assert!(key >= separator);
            }
        }
    }

    #[test]
    fn test_redistribute_insufficient_keys() {
        let mut left = BTreeLeafPage::new(PageId(1));
        let mut right = BTreeLeafPage::new(PageId(2));

        // Both have minimum keys
        left.insert_key_value(b"key1", TupleId::new(PageId(1), 1))
            .unwrap();
        left.insert_key_value(b"key2", TupleId::new(PageId(2), 2))
            .unwrap();

        right
            .insert_key_value(b"key3", TupleId::new(PageId(3), 3))
            .unwrap();

        // Redistribute should fail
        assert!(left.redistribute_with(&mut right, false).is_err());
        assert!(right.redistribute_with(&mut left, true).is_err());
    }

    #[test]
    fn test_search_empty_page() {
        let page = BTreeLeafPage::new(PageId(1));
        assert_eq!(page.search(b"key1"), None);
        assert_eq!(page.find_insert_position(b"key1"), 0);
    }

    #[test]
    fn test_search_single_key() {
        let mut page = BTreeLeafPage::new(PageId(1));
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();

        assert_eq!(page.search(b"key1"), Some(0));
        assert_eq!(page.search(b"key0"), None);
        assert_eq!(page.search(b"key2"), None);

        assert_eq!(page.find_insert_position(b"key0"), 0);
        assert_eq!(page.find_insert_position(b"key1"), 0);
        assert_eq!(page.find_insert_position(b"key2"), 1);
    }

    #[test]
    fn test_search_multiple_keys() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert keys in order
        for i in 0..10 {
            let key = format!("key{:02}", i * 2).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        // Search for existing keys
        assert_eq!(page.search(b"key00"), Some(0));
        assert_eq!(page.search(b"key06"), Some(3));
        assert_eq!(page.search(b"key18"), Some(9));

        // Search for non-existing keys
        assert_eq!(page.search(b"key01"), None);
        assert_eq!(page.search(b"key19"), None);
        assert_eq!(page.search(b"key99"), None);

        // Test insert positions
        assert_eq!(page.find_insert_position(b"key01"), 1);
        assert_eq!(page.find_insert_position(b"key05"), 3);
        assert_eq!(page.find_insert_position(b"key99"), 10);
    }

    #[test]
    fn test_search_with_duplicates() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert duplicate keys
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 1))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 2))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 3))
            .unwrap();
        page.insert_key_value(b"key3", TupleId::new(PageId(30), 1))
            .unwrap();

        // Search should return the first occurrence
        assert_eq!(page.search(b"key1"), Some(0));
        assert_eq!(page.search(b"key2"), Some(1));
        assert_eq!(page.search(b"key3"), Some(4));

        // Verify it returns the first duplicate
        let found_pos = page.search(b"key2").unwrap();
        assert_eq!(
            page.get_tuple_id(found_pos).unwrap(),
            TupleId::new(PageId(20), 1)
        );
    }

    #[test]
    fn test_binary_search_efficiency() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert many keys to test binary search
        for i in 0..100 {
            let key = format!("key{:04}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        // Search for keys at different positions
        assert_eq!(page.search(b"key0000"), Some(0));
        assert_eq!(page.search(b"key0050"), Some(50));
        assert_eq!(page.search(b"key0099"), Some(99));

        // Search for boundary cases
        assert_eq!(page.search(b"key"), None);
        assert_eq!(page.search(b"key9999"), None);
    }

    #[test]
    fn test_range_scan_all() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert some keys
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32 * 10), i as u16))
                .unwrap();
        }

        // Scan all entries
        let results = page.range_scan(None, None, true, true);
        assert_eq!(results.len(), 5);

        for (i, (key, tuple_id)) in results.iter().enumerate() {
            assert_eq!(key, &format!("key{}", i).into_bytes());
            assert_eq!(*tuple_id, TupleId::new(PageId(i as u32 * 10), i as u16));
        }
    }

    #[test]
    fn test_range_scan_with_bounds() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert keys
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        // Test inclusive range [key03, key07]
        let results = page.range_scan(Some(b"key03"), Some(b"key07"), true, true);
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, b"key03");
        assert_eq!(results[4].0, b"key07");

        // Test exclusive range (key03, key07)
        let results = page.range_scan(Some(b"key03"), Some(b"key07"), false, false);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"key04");
        assert_eq!(results[2].0, b"key06");

        // Test half-open range [key03, key07)
        let results = page.range_scan(Some(b"key03"), Some(b"key07"), true, false);
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, b"key03");
        assert_eq!(results[3].0, b"key06");
    }

    #[test]
    fn test_range_scan_unbounded() {
        let mut page = BTreeLeafPage::new(PageId(1));

        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        // Test unbounded start
        let results = page.range_scan(None, Some(b"key05"), true, true);
        assert_eq!(results.len(), 6);
        assert_eq!(results[0].0, b"key00");
        assert_eq!(results[5].0, b"key05");

        // Test unbounded end
        let results = page.range_scan(Some(b"key05"), None, true, true);
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, b"key05");
        assert_eq!(results[4].0, b"key09");
    }

    #[test]
    fn test_range_scan_with_duplicates() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert duplicate keys
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 1))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 2))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 3))
            .unwrap();
        page.insert_key_value(b"key3", TupleId::new(PageId(30), 1))
            .unwrap();

        // Range scan should include all duplicates
        let results = page.range_scan(Some(b"key2"), Some(b"key2"), true, true);
        assert_eq!(results.len(), 3);
        for (i, (key, tuple_id)) in results.iter().enumerate() {
            assert_eq!(key, b"key2");
            assert_eq!(*tuple_id, TupleId::new(PageId(20), (i + 1) as u16));
        }
    }

    #[test]
    fn test_range_scan_empty_range() {
        let mut page = BTreeLeafPage::new(PageId(1));

        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        // Range where start > end
        let results = page.range_scan(Some(b"key4"), Some(b"key1"), true, true);
        assert_eq!(results.len(), 0);

        // Non-existent range
        let results = page.range_scan(Some(b"key5"), Some(b"key9"), true, true);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_count_range() {
        let mut page = BTreeLeafPage::new(PageId(1));

        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32), i as u16))
                .unwrap();
        }

        // Count all
        assert_eq!(page.count_range(None, None, true, true), 10);

        // Count inclusive range
        assert_eq!(
            page.count_range(Some(b"key03"), Some(b"key07"), true, true),
            5
        );

        // Count exclusive range
        assert_eq!(
            page.count_range(Some(b"key03"), Some(b"key07"), false, false),
            3
        );

        // Count with duplicates - first check current count
        let initial_count = page.count_range(Some(b"key05"), Some(b"key05"), true, true);
        assert_eq!(initial_count, 1); // Only one key05 initially

        // Add duplicates (they will be inserted in TupleId order)
        page.insert_key_value(b"key05", TupleId::new(PageId(5), 10))
            .unwrap();
        page.insert_key_value(b"key05", TupleId::new(PageId(5), 11))
            .unwrap();

        // Debug: let's check all keys in page
        let all_tuples = page.find_tuples_for_key(b"key05");
        assert_eq!(all_tuples.len(), 3);

        // Now we should have 3 entries for key05
        let count = page.count_range(Some(b"key05"), Some(b"key05"), true, true);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_range_scan_non_existent_bounds() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Insert keys with gaps
        page.insert_key_value(b"key10", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key20", TupleId::new(PageId(20), 2))
            .unwrap();
        page.insert_key_value(b"key30", TupleId::new(PageId(30), 3))
            .unwrap();

        // Range with non-existent bounds
        let results = page.range_scan(Some(b"key15"), Some(b"key25"), true, true);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"key20");

        // Range that includes gaps
        let results = page.range_scan(Some(b"key05"), Some(b"key35"), true, true);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_serialize_deserialize_empty_page() {
        let page = BTreeLeafPage::new(PageId(42));

        // Serialize
        let serialized = page.serialize();
        assert_eq!(serialized.len(), PAGE_SIZE);

        // Deserialize
        let deserialized = BTreeLeafPage::deserialize(PageId(42), &serialized).unwrap();

        // Verify
        assert_eq!(deserialized.page_id, PageId(42));
        assert_eq!(deserialized.slot_count(), 0);
        assert_eq!(deserialized.next_page_id(), None);
        assert_eq!(deserialized.prev_page_id(), None);
    }

    #[test]
    fn test_serialize_deserialize_with_data() {
        let mut page = BTreeLeafPage::new(PageId(100));

        // Add some data
        page.set_next_page_id(Some(PageId(101)));
        page.set_prev_page_id(Some(PageId(99)));

        for i in 0..5 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_value(&key, TupleId::new(PageId(i as u32 * 10), i as u16))
                .unwrap();
        }

        // Serialize
        let serialized = page.serialize();

        // Deserialize
        let deserialized = BTreeLeafPage::deserialize(PageId(100), &serialized).unwrap();

        // Verify header
        assert_eq!(deserialized.page_id, PageId(100));
        assert_eq!(deserialized.slot_count(), 5);
        assert_eq!(deserialized.next_page_id(), Some(PageId(101)));
        assert_eq!(deserialized.prev_page_id(), Some(PageId(99)));

        // Verify data
        for i in 0..5 {
            let expected_key = format!("key{:02}", i).into_bytes();
            assert_eq!(deserialized.get_key(i).unwrap(), expected_key);
            assert_eq!(
                deserialized.get_tuple_id(i).unwrap(),
                TupleId::new(PageId(i as u32 * 10), i as u16)
            );
        }
    }

    #[test]
    fn test_deserialize_invalid_data() {
        // Wrong size
        let small_data = vec![0u8; 100];
        assert!(BTreeLeafPage::deserialize(PageId(1), &small_data).is_err());

        // Invalid page type
        let mut bad_type = vec![0u8; PAGE_SIZE];
        bad_type[0] = 0xFF; // Invalid page type
        assert!(BTreeLeafPage::deserialize(PageId(1), &bad_type).is_err());

        // Invalid pointers
        let page = BTreeLeafPage::new(PageId(1));
        let mut data = page.serialize();
        // Corrupt lower pointer to be less than header size
        data[2] = 0;
        data[3] = 0;
        assert!(BTreeLeafPage::deserialize(PageId(1), &data).is_err());
    }

    #[test]
    fn test_from_data() {
        let mut data = [0u8; PAGE_SIZE];

        // Set up a valid header
        data[0] = 0x02; // Leaf page type
        let lower = BTREE_LEAF_HEADER_SIZE as u16;
        data[2..4].copy_from_slice(&lower.to_le_bytes());
        let upper = PAGE_SIZE as u16;
        data[4..6].copy_from_slice(&upper.to_le_bytes());

        let page = BTreeLeafPage::from_data(PageId(50), &mut data);
        assert_eq!(page.page_id, PageId(50));
        assert_eq!(page.slot_count(), 0);
    }

    #[test]
    fn test_validate_valid_page() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Empty page should be valid
        assert!(page.validate().is_ok());

        // Add some data
        for i in 0..3 {
            page.insert_key_value(
                &format!("key{}", i).into_bytes(),
                TupleId::new(PageId(i as u32), i as u16),
            )
            .unwrap();
        }

        // Should still be valid
        assert!(page.validate().is_ok());
    }

    #[test]
    fn test_validate_corrupted_page() {
        let mut page = BTreeLeafPage::new(PageId(1));

        // Add some valid data first
        page.insert_key_value(b"key1", TupleId::new(PageId(1), 1))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(2), 2))
            .unwrap();

        // Corrupt the page type
        page.data[0] = 0xFF;
        assert!(page.validate().is_err());

        // Fix page type but corrupt lower pointer
        page.data[0] = 0x02;
        page.data[2] = 0;
        page.data[3] = 0;
        assert!(page.validate().is_err());
    }

    #[test]
    fn test_serialize_deserialize_with_duplicates() {
        let mut page = BTreeLeafPage::new(PageId(200));

        // Add duplicate keys
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 1))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 2))
            .unwrap();
        page.insert_key_value(b"key1", TupleId::new(PageId(10), 3))
            .unwrap();
        page.insert_key_value(b"key2", TupleId::new(PageId(20), 1))
            .unwrap();

        // Serialize and deserialize
        let serialized = page.serialize();
        let deserialized = BTreeLeafPage::deserialize(PageId(200), &serialized).unwrap();

        // Verify duplicates are preserved
        let tuples = deserialized.find_tuples_for_key(b"key1");
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0], TupleId::new(PageId(10), 1));
        assert_eq!(tuples[1], TupleId::new(PageId(10), 2));
        assert_eq!(tuples[2], TupleId::new(PageId(10), 3));
    }

    #[test]
    fn test_round_trip_with_splits_and_merges() {
        let mut page1 = BTreeLeafPage::new(PageId(1));

        // Fill the page
        for i in 0..8 {
            page1
                .insert_key_value(
                    &format!("key{:02}", i).into_bytes(),
                    TupleId::new(PageId(i as u32), i as u16),
                )
                .unwrap();
        }

        // Split
        let (mut page2, _) = page1.split().unwrap();
        page2.page_id = PageId(2);
        page1.set_next_page_id(Some(PageId(2)));

        // Serialize both
        let serialized1 = page1.serialize();
        let serialized2 = page2.serialize();

        // Deserialize
        let restored1 = BTreeLeafPage::deserialize(PageId(1), &serialized1).unwrap();
        let restored2 = BTreeLeafPage::deserialize(PageId(2), &serialized2).unwrap();

        // Verify data is intact
        assert_eq!(restored1.slot_count() + restored2.slot_count(), 8);
        assert_eq!(restored1.next_page_id(), Some(PageId(2)));
        assert_eq!(restored2.prev_page_id(), Some(PageId(1)));

        // Verify all keys are present
        let mut all_keys = Vec::new();
        for i in 0..restored1.slot_count() {
            if let Some(key) = restored1.get_key(i as usize) {
                all_keys.push(key);
            }
        }
        for i in 0..restored2.slot_count() {
            if let Some(key) = restored2.get_key(i as usize) {
                all_keys.push(key);
            }
        }
        assert_eq!(all_keys.len(), 8);
    }
}
