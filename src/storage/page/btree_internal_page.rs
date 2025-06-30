use crate::storage::{PAGE_SIZE, Page, PageId};

const BTREE_INTERNAL_HEADER_SIZE: usize = 24;
pub const MIN_KEYS_PER_INTERNAL_PAGE: usize = 2;

// Manual layout to ensure exactly 24 bytes
pub struct BTreeInternalPageHeader {
    data: [u8; BTREE_INTERNAL_HEADER_SIZE],
}

impl BTreeInternalPageHeader {
    fn new() -> Self {
        Self {
            data: [0; BTREE_INTERNAL_HEADER_SIZE],
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

    fn level(&self) -> u16 {
        u16::from_le_bytes([self.data[8], self.data[9]])
    }

    fn set_level(&mut self, val: u16) {
        let bytes = val.to_le_bytes();
        self.data[8] = bytes[0];
        self.data[9] = bytes[1];
    }

    fn free_space(&self) -> u16 {
        u16::from_le_bytes([self.data[10], self.data[11]])
    }

    fn set_free_space(&mut self, val: u16) {
        let bytes = val.to_le_bytes();
        self.data[10] = bytes[0];
        self.data[11] = bytes[1];
    }

    #[allow(dead_code)]
    fn page_lsn(&self) -> u64 {
        u64::from_le_bytes([
            self.data[12],
            self.data[13],
            self.data[14],
            self.data[15],
            self.data[16],
            self.data[17],
            self.data[18],
            self.data[19],
        ])
    }

    fn set_page_lsn(&mut self, val: u64) {
        let bytes = val.to_le_bytes();
        self.data[12..20].copy_from_slice(&bytes);
    }
}

// Slot entry for internal page (child_page_id, key_offset, key_length)
#[derive(Debug, Clone, Copy)]
pub struct InternalSlot {
    child_page_id: u32, // 4 bytes
    key_offset: u16,    // 2 bytes
    key_length: u16,    // 2 bytes
}

impl InternalSlot {
    const SIZE: usize = 8;

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= Self::SIZE);
        Self {
            child_page_id: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            key_offset: u16::from_le_bytes([bytes[4], bytes[5]]),
            key_length: u16::from_le_bytes([bytes[6], bytes[7]]),
        }
    }

    fn to_bytes(self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.child_page_id.to_le_bytes());
        bytes[4..6].copy_from_slice(&self.key_offset.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.key_length.to_le_bytes());
        bytes
    }
}

pub struct BTreeInternalPage {
    page_id: PageId,
    data: [u8; PAGE_SIZE],
}

impl BTreeInternalPage {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self {
            page_id,
            data: [0; PAGE_SIZE],
        };

        // Initialize header
        let mut header = BTreeInternalPageHeader::new();
        header.set_page_type(0x01); // Internal page type
        header.set_lower(BTREE_INTERNAL_HEADER_SIZE as u16);
        header.set_upper(PAGE_SIZE as u16);
        header.set_slot_count(0);
        header.set_level(0);
        header.set_free_space((PAGE_SIZE - BTREE_INTERNAL_HEADER_SIZE) as u16);
        header.set_page_lsn(0);
        page.write_header(&header);

        page
    }

    fn header(&self) -> BTreeInternalPageHeader {
        let mut header = BTreeInternalPageHeader::new();
        header
            .data
            .copy_from_slice(&self.data[..BTREE_INTERNAL_HEADER_SIZE]);
        header
    }

    fn write_header(&mut self, header: &BTreeInternalPageHeader) {
        self.data[..BTREE_INTERNAL_HEADER_SIZE].copy_from_slice(&header.data);
    }

    pub fn level(&self) -> u16 {
        self.header().level()
    }

    pub fn set_level(&mut self, level: u16) {
        let mut header = self.header();
        header.set_level(level);
        self.write_header(&header);
    }

    pub fn slot_count(&self) -> u16 {
        self.header().slot_count()
    }

    pub fn free_space(&self) -> u16 {
        self.header().free_space()
    }

    // Slot array management
    fn slot_offset(&self, index: usize) -> usize {
        BTREE_INTERNAL_HEADER_SIZE + index * InternalSlot::SIZE
    }

    pub fn get_slot(&self, index: usize) -> Option<InternalSlot> {
        if index >= self.slot_count() as usize {
            return None;
        }

        let offset = self.slot_offset(index);
        let slot_data = &self.data[offset..offset + InternalSlot::SIZE];
        Some(InternalSlot::from_bytes(slot_data))
    }

    pub fn insert_slot(&mut self, index: usize, slot: InternalSlot) -> Result<(), &'static str> {
        let slot_count = self.slot_count() as usize;
        if index > slot_count {
            return Err("Invalid slot index");
        }

        // Check space
        let required_space = InternalSlot::SIZE;
        if self.free_space() < required_space as u16 {
            return Err("Insufficient space for new slot");
        }

        // Shift existing slots
        if index < slot_count {
            let src = self.slot_offset(index);
            let dst = self.slot_offset(index + 1);
            let len = (slot_count - index) * InternalSlot::SIZE;
            self.data.copy_within(src..src + len, dst);
        }

        // Write new slot
        let offset = self.slot_offset(index);
        let slot_bytes = slot.to_bytes();
        self.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);

        // Update header
        let mut header = self.header();
        header.set_slot_count((slot_count + 1) as u16);
        header
            .set_lower((BTREE_INTERNAL_HEADER_SIZE + (slot_count + 1) * InternalSlot::SIZE) as u16);
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
            let len = (slot_count - index - 1) * InternalSlot::SIZE;
            self.data.copy_within(src..src + len, dst);
        }

        // Update header
        let mut header = self.header();
        header.set_slot_count((slot_count - 1) as u16);
        header
            .set_lower((BTREE_INTERNAL_HEADER_SIZE + (slot_count - 1) * InternalSlot::SIZE) as u16);
        header.set_free_space(self.free_space() + InternalSlot::SIZE as u16);
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

    pub fn child_page_id(&self, slot_index: usize) -> Option<PageId> {
        self.get_slot(slot_index)
            .map(|slot| PageId(slot.child_page_id))
    }

    // Lower/upper pointer management
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
        self.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);

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
                self.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);
            }
        }

        // Update header
        let mut header = self.header();
        let lower = header.lower();
        header.set_upper(new_upper);
        header.set_free_space(new_upper - lower);
        self.write_header(&header);
    }

    pub fn available_space(&self) -> usize {
        let header = self.header();
        (header.upper() - header.lower()) as usize
    }

    // Key insertion with proper ordering
    pub fn insert_key_and_child(
        &mut self,
        key: &[u8],
        child_page_id: PageId,
    ) -> Result<usize, &'static str> {
        // Find insertion position
        let slot_count = self.slot_count() as usize;
        let mut insert_pos = slot_count;

        for i in 0..slot_count {
            if let Some(existing_key) = self.get_key(i) {
                if key < existing_key.as_slice() {
                    insert_pos = i;
                    break;
                }
            }
        }

        // Check space for new slot and key
        let required_space = InternalSlot::SIZE + key.len();
        if required_space > self.available_space() {
            return Err("Insufficient space for new key and slot");
        }

        // Store key first
        let header = self.header();
        let key_offset = header.upper() - key.len() as u16;
        self.data[key_offset as usize..key_offset as usize + key.len()].copy_from_slice(key);

        // Create and insert slot
        let slot = InternalSlot {
            child_page_id: child_page_id.0,
            key_offset,
            key_length: key.len() as u16,
        };

        self.insert_slot(insert_pos, slot)?;

        // Update upper pointer (insert_slot already updated lower and free_space)
        let mut header = self.header();
        header.set_upper(key_offset);
        header.set_free_space(header.free_space() - key.len() as u16);
        self.write_header(&header);

        Ok(insert_pos)
    }

    pub fn find_child_index(&self, search_key: &[u8]) -> usize {
        let slot_count = self.slot_count() as usize;

        // Binary search for the child pointer
        let mut left = 0;
        let mut right = slot_count;

        while left < right {
            let mid = left + (right - left) / 2;
            if let Some(key) = self.get_key(mid) {
                if search_key < key.as_slice() {
                    right = mid;
                } else {
                    left = mid + 1;
                }
            } else {
                // If no key, it's the rightmost pointer
                return mid;
            }
        }

        left.saturating_sub(1)
    }

    // Key deletion with slot compaction
    pub fn delete_key_at(&mut self, index: usize) -> Result<(), &'static str> {
        let slot_count = self.slot_count() as usize;
        if index >= slot_count {
            return Err("Invalid key index");
        }

        // Delete the slot (this shifts remaining slots)
        self.delete_slot(index)?;

        // Compact to reclaim key space
        self.compact();

        Ok(())
    }

    pub fn delete_key(&mut self, key_to_delete: &[u8]) -> Result<(), &'static str> {
        let slot_count = self.slot_count() as usize;

        // Find the key to delete
        let mut delete_index = None;
        for i in 0..slot_count {
            if let Some(key) = self.get_key(i) {
                if key.as_slice() == key_to_delete {
                    delete_index = Some(i);
                    break;
                }
            }
        }

        match delete_index {
            Some(index) => self.delete_key_at(index),
            None => Err("Key not found"),
        }
    }

    // Page split logic
    pub fn split(&mut self) -> Result<(BTreeInternalPage, Vec<u8>), &'static str> {
        let slot_count = self.slot_count() as usize;
        if slot_count < MIN_KEYS_PER_INTERNAL_PAGE * 2 {
            return Err("Not enough keys to split");
        }

        // Calculate split point (middle)
        let split_index = slot_count / 2;

        // Create new page
        let mut new_page = BTreeInternalPage::new(PageId(0)); // PageId will be assigned by caller
        new_page.set_level(self.level());

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

        // The middle key will be promoted to parent
        let promoted_key = keys_to_move.remove(0).clone();
        let promoted_slot = slots_to_move.remove(0);

        // Move remaining keys to new page
        for (key, slot) in keys_to_move.iter().zip(slots_to_move.iter()) {
            new_page.insert_key_and_child(key, PageId(slot.child_page_id))?;
        }

        // Also add the promoted slot's child as the first child of new page
        // (it becomes the leftmost pointer of the new page)
        if new_page.slot_count() > 0 {
            // Shift existing slots right
            let slot_count = new_page.slot_count() as usize;
            for i in (0..slot_count).rev() {
                if let Some(slot) = new_page.get_slot(i) {
                    let offset = new_page.slot_offset(i + 1);
                    let slot_bytes = slot.to_bytes();
                    new_page.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);
                }
            }
        }

        // Insert promoted child as first slot
        let first_slot = InternalSlot {
            child_page_id: promoted_slot.child_page_id,
            key_offset: 0,
            key_length: 0,
        };
        let offset = new_page.slot_offset(0);
        let slot_bytes = first_slot.to_bytes();
        new_page.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);

        // Update new page header
        let mut header = new_page.header();
        header.set_slot_count(header.slot_count() + 1);
        header.set_lower(header.lower() + InternalSlot::SIZE as u16);
        header.set_free_space(header.free_space() - InternalSlot::SIZE as u16);
        new_page.write_header(&header);

        // Remove moved keys from original page
        for _ in split_index..slot_count {
            self.delete_slot(split_index)?;
        }

        // Compact original page
        self.compact();

        Ok((new_page, promoted_key))
    }

    pub fn can_split(&self) -> bool {
        self.slot_count() as usize >= MIN_KEYS_PER_INTERNAL_PAGE * 2
    }

    pub fn needs_split(&self) -> bool {
        // Split when less than 20% space remaining
        self.available_space() < (PAGE_SIZE / 5)
    }

    // Page merge logic
    pub fn can_merge_with(&self, other: &BTreeInternalPage) -> bool {
        // Check if both pages can fit in one page
        let total_slots = self.slot_count() + other.slot_count();
        let total_keys_size = self.calculate_keys_size() + other.calculate_keys_size();
        let required_space = (total_slots as usize * InternalSlot::SIZE) + total_keys_size;

        required_space <= (PAGE_SIZE - BTREE_INTERNAL_HEADER_SIZE)
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

    pub fn merge_with(&mut self, other: &BTreeInternalPage) -> Result<(), &'static str> {
        if !self.can_merge_with(other) {
            return Err("Pages cannot be merged - insufficient space");
        }

        // Copy all slots and keys from other page
        for i in 0..other.slot_count() {
            if let Some(slot) = other.get_slot(i as usize) {
                if let Some(key) = other.get_key(i as usize) {
                    self.insert_key_and_child(&key, PageId(slot.child_page_id))?;
                }
            }
        }

        Ok(())
    }

    pub fn needs_merge(&self) -> bool {
        // Merge when less than minimum keys
        self.slot_count() < MIN_KEYS_PER_INTERNAL_PAGE as u16
    }

    // Redistribute keys between two sibling pages
    pub fn redistribute_with(
        &mut self,
        sibling: &mut BTreeInternalPage,
        is_left_sibling: bool,
    ) -> Result<Vec<u8>, &'static str> {
        let self_count = self.slot_count() as usize;
        let sibling_count = sibling.slot_count() as usize;
        let total_count = self_count + sibling_count;

        if total_count < MIN_KEYS_PER_INTERNAL_PAGE * 2 {
            return Err("Not enough keys to redistribute");
        }

        let target_count = total_count / 2;

        if is_left_sibling {
            // Move keys from left sibling to self
            if sibling_count <= target_count {
                return Err("Sibling doesn't have enough keys to redistribute");
            }

            let _keys_to_move = sibling_count - target_count;
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

            // The last key from left sibling becomes the new separator
            let separator_key = moved_keys.pop().unwrap();
            let separator_slot = moved_slots.pop().unwrap();

            // Delete moved keys from sibling
            for _ in target_count..sibling_count {
                sibling.delete_slot(target_count)?;
            }
            sibling.compact();

            // Insert moved keys at the beginning of self
            // First, shift existing slots right
            let shift_amount = moved_keys.len() + 1; // +1 for separator's child
            for i in (0..self_count).rev() {
                if let Some(slot) = self.get_slot(i) {
                    let new_offset = self.slot_offset(i + shift_amount);
                    let slot_bytes = slot.to_bytes();
                    self.data[new_offset..new_offset + InternalSlot::SIZE]
                        .copy_from_slice(&slot_bytes);
                }
            }

            // Insert separator's child as first slot
            let first_slot = InternalSlot {
                child_page_id: separator_slot.child_page_id,
                key_offset: 0,
                key_length: 0,
            };
            let offset = self.slot_offset(0);
            let slot_bytes = first_slot.to_bytes();
            self.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);

            // Insert moved keys
            for (i, (_key, slot)) in moved_keys
                .iter()
                .rev()
                .zip(moved_slots.iter().rev())
                .enumerate()
            {
                let slot_index = i + 1;
                let new_slot = InternalSlot {
                    child_page_id: slot.child_page_id,
                    key_offset: 0,
                    key_length: 0,
                };
                let offset = self.slot_offset(slot_index);
                let slot_bytes = new_slot.to_bytes();
                self.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);
            }

            // Update header
            let mut header = self.header();
            header.set_slot_count((self_count + shift_amount) as u16);
            header.set_lower(
                (BTREE_INTERNAL_HEADER_SIZE + (self_count + shift_amount) * InternalSlot::SIZE)
                    as u16,
            );
            header.set_free_space(header.free_space() - (shift_amount * InternalSlot::SIZE) as u16);
            self.write_header(&header);

            // Now insert the keys properly
            for (i, key) in moved_keys.iter().rev().enumerate() {
                self.insert_key(i + 1, key)?;
            }

            Ok(separator_key)
        } else {
            // Move keys from self to right sibling
            if self_count <= target_count {
                return Err("Not enough keys to redistribute to right sibling");
            }

            let _keys_to_move = self_count - target_count;
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

            // The first key to move becomes the new separator
            let separator_key = moved_keys.remove(0);
            let separator_slot = moved_slots.remove(0);

            // Shift sibling's slots right to make room
            let shift_amount = moved_keys.len() + 1; // +1 for separator's child
            for i in (0..sibling_count).rev() {
                if let Some(slot) = sibling.get_slot(i) {
                    let new_offset = sibling.slot_offset(i + shift_amount);
                    let slot_bytes = slot.to_bytes();
                    sibling.data[new_offset..new_offset + InternalSlot::SIZE]
                        .copy_from_slice(&slot_bytes);
                }
            }

            // Insert separator's child as first slot in sibling
            let first_slot = InternalSlot {
                child_page_id: separator_slot.child_page_id,
                key_offset: 0,
                key_length: 0,
            };
            let offset = sibling.slot_offset(0);
            let slot_bytes = first_slot.to_bytes();
            sibling.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);

            // Insert moved slots
            for (i, slot) in moved_slots.iter().enumerate() {
                let slot_index = i + 1;
                let offset = sibling.slot_offset(slot_index);
                let slot_bytes = slot.to_bytes();
                sibling.data[offset..offset + InternalSlot::SIZE].copy_from_slice(&slot_bytes);
            }

            // Update sibling header
            let mut header = sibling.header();
            header.set_slot_count((sibling_count + shift_amount) as u16);
            header.set_lower(
                (BTREE_INTERNAL_HEADER_SIZE + (sibling_count + shift_amount) * InternalSlot::SIZE)
                    as u16,
            );
            header.set_free_space(header.free_space() - (shift_amount * InternalSlot::SIZE) as u16);
            sibling.write_header(&header);

            // Insert keys into sibling
            for (i, key) in moved_keys.iter().enumerate() {
                sibling.insert_key(i + 1, key)?;
            }

            // Delete moved keys from self
            for _ in target_count..self_count {
                self.delete_slot(target_count)?;
            }
            self.compact();

            Ok(separator_key)
        }
    }

    // Serialization/deserialization already handled by Page trait
    pub fn from_page_data(page_id: PageId, data: [u8; PAGE_SIZE]) -> Self {
        BTreeInternalPage { page_id, data }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        let header = self.header();

        // Validate page type
        if header.page_type() != 0x01 {
            return Err("Invalid page type for internal page");
        }

        // Validate lower/upper bounds
        if header.lower() < BTREE_INTERNAL_HEADER_SIZE as u16 {
            return Err("Lower bound is less than header size");
        }

        if header.upper() > PAGE_SIZE as u16 {
            return Err("Upper bound exceeds page size");
        }

        if header.lower() > header.upper() {
            return Err("Lower bound exceeds upper bound");
        }

        // Validate slot count
        let expected_lower =
            BTREE_INTERNAL_HEADER_SIZE + (header.slot_count() as usize * InternalSlot::SIZE);
        if header.lower() != expected_lower as u16 {
            return Err("Lower bound doesn't match slot count");
        }

        // Validate free space
        let expected_free = header.upper() - header.lower();
        if header.free_space() != expected_free {
            return Err("Free space calculation mismatch");
        }

        Ok(())
    }
}

impl Page for BTreeInternalPage {
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
            std::mem::size_of::<BTreeInternalPageHeader>(),
            BTREE_INTERNAL_HEADER_SIZE
        );
    }

    #[test]
    fn test_new_internal_page() {
        let page = BTreeInternalPage::new(PageId(42));

        assert_eq!(page.page_id(), PageId(42));
        let header = page.header();
        assert_eq!(header.page_type(), 0x01);
        assert_eq!(page.level(), 0);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(header.lower(), BTREE_INTERNAL_HEADER_SIZE as u16);
        assert_eq!(header.upper(), PAGE_SIZE as u16);
        assert_eq!(
            page.free_space(),
            (PAGE_SIZE - BTREE_INTERNAL_HEADER_SIZE) as u16
        );
    }

    #[test]
    fn test_set_level() {
        let mut page = BTreeInternalPage::new(PageId(1));

        page.set_level(3);
        assert_eq!(page.level(), 3);

        page.set_level(0);
        assert_eq!(page.level(), 0);
    }

    #[test]
    fn test_page_trait_implementation() {
        let mut page = BTreeInternalPage::new(PageId(100));

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
    fn test_internal_slot() {
        let slot = InternalSlot {
            child_page_id: 12345,
            key_offset: 100,
            key_length: 25,
        };

        let bytes = slot.to_bytes();
        assert_eq!(bytes.len(), InternalSlot::SIZE);

        let decoded = InternalSlot::from_bytes(&bytes);
        assert_eq!(decoded.child_page_id, 12345);
        assert_eq!(decoded.key_offset, 100);
        assert_eq!(decoded.key_length, 25);
    }

    #[test]
    fn test_slot_insert() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert first slot
        let slot1 = InternalSlot {
            child_page_id: 10,
            key_offset: 1000,
            key_length: 5,
        };
        assert!(page.insert_slot(0, slot1).is_ok());
        assert_eq!(page.slot_count(), 1);

        // Insert second slot at beginning
        let slot2 = InternalSlot {
            child_page_id: 20,
            key_offset: 2000,
            key_length: 10,
        };
        assert!(page.insert_slot(0, slot2).is_ok());
        assert_eq!(page.slot_count(), 2);

        // Verify slots
        let retrieved1 = page.get_slot(0).unwrap();
        assert_eq!(retrieved1.child_page_id, 20);

        let retrieved2 = page.get_slot(1).unwrap();
        assert_eq!(retrieved2.child_page_id, 10);
    }

    #[test]
    fn test_slot_delete() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert multiple slots
        for i in 0..5 {
            let slot = InternalSlot {
                child_page_id: i * 10,
                key_offset: 1000 + i as u16 * 100,
                key_length: 5,
            };
            page.insert_slot(i as usize, slot).unwrap();
        }

        assert_eq!(page.slot_count(), 5);

        // Delete middle slot
        assert!(page.delete_slot(2).is_ok());
        assert_eq!(page.slot_count(), 4);

        // Verify remaining slots
        assert_eq!(page.get_slot(0).unwrap().child_page_id, 0);
        assert_eq!(page.get_slot(1).unwrap().child_page_id, 10);
        assert_eq!(page.get_slot(2).unwrap().child_page_id, 30);
        assert_eq!(page.get_slot(3).unwrap().child_page_id, 40);
    }

    #[test]
    fn test_child_page_id() {
        let mut page = BTreeInternalPage::new(PageId(1));

        let slot = InternalSlot {
            child_page_id: 999,
            key_offset: 0,
            key_length: 0,
        };
        page.insert_slot(0, slot).unwrap();

        assert_eq!(page.child_page_id(0), Some(PageId(999)));
        assert_eq!(page.child_page_id(1), None);
    }

    #[test]
    fn test_get_key() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Write key data
        let key_data = b"test_key";
        let key_offset = 1000;
        page.data[key_offset..key_offset + key_data.len()].copy_from_slice(key_data);

        // Insert slot pointing to key
        let slot = InternalSlot {
            child_page_id: 100,
            key_offset: key_offset as u16,
            key_length: key_data.len() as u16,
        };
        page.insert_slot(0, slot).unwrap();

        // Retrieve key
        let retrieved_key = page.get_key(0).unwrap();
        assert_eq!(retrieved_key, key_data);
    }

    #[test]
    fn test_insert_key() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert a slot first
        let slot = InternalSlot {
            child_page_id: 100,
            key_offset: 0,
            key_length: 0,
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
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert multiple slots with keys
        let keys = vec![b"key1", b"key2", b"key3"];
        for (i, key) in keys.iter().enumerate() {
            let slot = InternalSlot {
                child_page_id: i as u32 * 10,
                key_offset: 0,
                key_length: 0,
            };
            page.insert_slot(i, slot).unwrap();
            page.insert_key(i, *key).unwrap();
        }

        // Manually fragment by deleting middle slot
        page.delete_slot(1).unwrap();

        // Insert new slot and key
        let new_slot = InternalSlot {
            child_page_id: 999,
            key_offset: 0,
            key_length: 0,
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
    fn test_available_space() {
        let mut page = BTreeInternalPage::new(PageId(1));

        let initial_space = page.available_space();
        assert_eq!(initial_space, PAGE_SIZE - BTREE_INTERNAL_HEADER_SIZE);

        // Insert a slot
        let slot = InternalSlot {
            child_page_id: 100,
            key_offset: 0,
            key_length: 0,
        };
        page.insert_slot(0, slot).unwrap();

        // Available space should decrease by slot size
        assert_eq!(page.available_space(), initial_space - InternalSlot::SIZE);

        // Insert a key
        let key = b"test_key";
        page.insert_key(0, key).unwrap();

        // Available space should decrease by key size
        assert_eq!(
            page.available_space(),
            initial_space - InternalSlot::SIZE - key.len()
        );
    }

    #[test]
    fn test_insert_key_and_child_ordering() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert keys in random order
        page.insert_key_and_child(b"key3", PageId(30)).unwrap();
        page.insert_key_and_child(b"key1", PageId(10)).unwrap();
        page.insert_key_and_child(b"key5", PageId(50)).unwrap();
        page.insert_key_and_child(b"key2", PageId(20)).unwrap();
        page.insert_key_and_child(b"key4", PageId(40)).unwrap();

        // Verify keys are in order
        assert_eq!(page.get_key(0).unwrap(), b"key1");
        assert_eq!(page.get_key(1).unwrap(), b"key2");
        assert_eq!(page.get_key(2).unwrap(), b"key3");
        assert_eq!(page.get_key(3).unwrap(), b"key4");
        assert_eq!(page.get_key(4).unwrap(), b"key5");

        // Verify child pointers match
        assert_eq!(page.child_page_id(0).unwrap(), PageId(10));
        assert_eq!(page.child_page_id(1).unwrap(), PageId(20));
        assert_eq!(page.child_page_id(2).unwrap(), PageId(30));
        assert_eq!(page.child_page_id(3).unwrap(), PageId(40));
        assert_eq!(page.child_page_id(4).unwrap(), PageId(50));
    }

    #[test]
    fn test_find_child_index() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Setup: keys 10, 20, 30, 40 with child pages 1, 2, 3, 4, 5
        page.insert_key_and_child(b"10", PageId(2)).unwrap();
        page.insert_key_and_child(b"20", PageId(3)).unwrap();
        page.insert_key_and_child(b"30", PageId(4)).unwrap();
        page.insert_key_and_child(b"40", PageId(5)).unwrap();

        // Test finding child indices
        assert_eq!(page.find_child_index(b"05"), 0); // Less than all keys
        assert_eq!(page.find_child_index(b"15"), 0); // Between first and second
        assert_eq!(page.find_child_index(b"25"), 1); // Between second and third
        assert_eq!(page.find_child_index(b"35"), 2); // Between third and fourth
        assert_eq!(page.find_child_index(b"45"), 3); // Greater than all keys

        // Test exact matches
        assert_eq!(page.find_child_index(b"10"), 0);
        assert_eq!(page.find_child_index(b"20"), 1);
        assert_eq!(page.find_child_index(b"30"), 2);
        assert_eq!(page.find_child_index(b"40"), 3);
    }

    #[test]
    fn test_insert_key_and_child_space_check() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Fill page with keys until no space left
        let mut i = 0;
        loop {
            let key = format!("key{:04}", i).into_bytes();
            if page.insert_key_and_child(&key, PageId(i as u32)).is_err() {
                break;
            }
            i += 1;
        }

        // Verify we filled some keys
        assert!(i > 0);
        assert_eq!(page.slot_count() as usize, i);

        // Try to insert one more - should fail
        let key = b"extra_key";
        assert!(page.insert_key_and_child(key, PageId(999)).is_err());
    }

    #[test]
    fn test_delete_key_at() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert multiple keys
        page.insert_key_and_child(b"key1", PageId(10)).unwrap();
        page.insert_key_and_child(b"key2", PageId(20)).unwrap();
        page.insert_key_and_child(b"key3", PageId(30)).unwrap();
        page.insert_key_and_child(b"key4", PageId(40)).unwrap();

        assert_eq!(page.slot_count(), 4);

        // Delete middle key
        assert!(page.delete_key_at(1).is_ok());
        assert_eq!(page.slot_count(), 3);

        // Verify remaining keys
        assert_eq!(page.get_key(0).unwrap(), b"key1");
        assert_eq!(page.get_key(1).unwrap(), b"key3");
        assert_eq!(page.get_key(2).unwrap(), b"key4");

        // Verify child pointers
        assert_eq!(page.child_page_id(0).unwrap(), PageId(10));
        assert_eq!(page.child_page_id(1).unwrap(), PageId(30));
        assert_eq!(page.child_page_id(2).unwrap(), PageId(40));
    }

    #[test]
    fn test_delete_key() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert multiple keys
        page.insert_key_and_child(b"alpha", PageId(1)).unwrap();
        page.insert_key_and_child(b"beta", PageId(2)).unwrap();
        page.insert_key_and_child(b"gamma", PageId(3)).unwrap();
        page.insert_key_and_child(b"delta", PageId(4)).unwrap();

        // Delete by key value
        assert!(page.delete_key(b"beta").is_ok());
        assert_eq!(page.slot_count(), 3);

        // Try to delete non-existent key
        assert!(page.delete_key(b"epsilon").is_err());

        // Verify remaining keys are in order
        assert_eq!(page.get_key(0).unwrap(), b"alpha");
        assert_eq!(page.get_key(1).unwrap(), b"delta");
        assert_eq!(page.get_key(2).unwrap(), b"gamma");
    }

    #[test]
    fn test_delete_with_compaction() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert several keys to fragment the page
        for i in 0..10 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_and_child(&key, PageId(i as u32)).unwrap();
        }

        let initial_free_space = page.free_space();

        // Delete alternating keys to create fragmentation
        for i in (0..10).step_by(2) {
            page.delete_key_at(i / 2).unwrap();
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
    fn test_delete_all_keys() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert some keys
        page.insert_key_and_child(b"key1", PageId(1)).unwrap();
        page.insert_key_and_child(b"key2", PageId(2)).unwrap();
        page.insert_key_and_child(b"key3", PageId(3)).unwrap();

        // Delete all keys
        while page.slot_count() > 0 {
            page.delete_key_at(0).unwrap();
        }

        assert_eq!(page.slot_count(), 0);
        assert_eq!(
            page.available_space(),
            PAGE_SIZE - BTREE_INTERNAL_HEADER_SIZE
        );
    }

    #[test]
    fn test_page_split() {
        let mut page = BTreeInternalPage::new(PageId(1));
        page.set_level(2);

        // Insert enough keys to allow split
        for i in 0..8 {
            let key = format!("key{:02}", i).into_bytes();
            page.insert_key_and_child(&key, PageId(i as u32 + 100))
                .unwrap();
        }

        assert!(page.can_split());
        let original_count = page.slot_count();

        // Perform split
        let (new_page, promoted_key) = page.split().unwrap();

        // Verify split occurred
        assert!(page.slot_count() < original_count);
        assert!(new_page.slot_count() > 0);
        // After split: left page has split_index keys, right page has (original_count - split_index) keys
        // No key is lost, just redistributed
        assert_eq!(page.slot_count() + new_page.slot_count(), original_count);

        // Verify promoted key
        assert_eq!(
            promoted_key,
            format!("key{:02}", original_count / 2).into_bytes()
        );

        // Verify levels match
        assert_eq!(new_page.level(), page.level());

        // Verify all keys in left page are less than promoted
        for i in 0..page.slot_count() {
            if let Some(key) = page.get_key(i as usize) {
                assert!(key < promoted_key);
            }
        }

        // Verify all keys in right page are greater than promoted
        for i in 0..new_page.slot_count() {
            if let Some(key) = new_page.get_key(i as usize) {
                if !key.is_empty() {
                    // Skip the leftmost pointer which has no key
                    assert!(key > promoted_key);
                }
            }
        }
    }

    #[test]
    fn test_split_minimum_keys() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert less than minimum required for split
        page.insert_key_and_child(b"key1", PageId(1)).unwrap();
        page.insert_key_and_child(b"key2", PageId(2)).unwrap();
        page.insert_key_and_child(b"key3", PageId(3)).unwrap();

        assert!(!page.can_split());
        assert!(page.split().is_err());
    }

    #[test]
    fn test_needs_split() {
        let mut page = BTreeInternalPage::new(PageId(1));

        assert!(!page.needs_split()); // Empty page doesn't need split

        // Fill page until it needs split
        let mut i = 0;
        while !page.needs_split() {
            let key = format!("key{:04}", i).into_bytes();
            if page.insert_key_and_child(&key, PageId(i as u32)).is_err() {
                break;
            }
            i += 1;
        }

        assert!(page.needs_split());
        assert!(page.available_space() < PAGE_SIZE / 5);
    }

    #[test]
    fn test_split_preserves_order() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Insert keys in specific order
        let keys: Vec<&[u8]> = vec![b"10", b"20", b"30", b"40", b"50", b"60", b"70", b"80"];
        for (i, key) in keys.iter().enumerate() {
            page.insert_key_and_child(key, PageId(i as u32)).unwrap();
        }

        let (new_page, promoted_key) = page.split().unwrap();

        // Collect all keys from both pages
        let mut all_keys = Vec::new();
        for i in 0..page.slot_count() {
            if let Some(key) = page.get_key(i as usize) {
                all_keys.push(key);
            }
        }
        all_keys.push(promoted_key.clone());
        for i in 0..new_page.slot_count() {
            if let Some(key) = new_page.get_key(i as usize) {
                if !key.is_empty() {
                    all_keys.push(key);
                }
            }
        }

        // Verify order is preserved
        for i in 1..all_keys.len() {
            assert!(all_keys[i - 1] <= all_keys[i]);
        }
    }

    #[test]
    fn test_page_merge() {
        let mut page1 = BTreeInternalPage::new(PageId(1));
        let mut page2 = BTreeInternalPage::new(PageId(2));

        // Add keys to first page
        page1.insert_key_and_child(b"key1", PageId(10)).unwrap();
        page1.insert_key_and_child(b"key2", PageId(20)).unwrap();

        // Add keys to second page
        page2.insert_key_and_child(b"key3", PageId(30)).unwrap();
        page2.insert_key_and_child(b"key4", PageId(40)).unwrap();

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

        // Verify child pointers
        assert_eq!(page1.child_page_id(0).unwrap(), PageId(10));
        assert_eq!(page1.child_page_id(1).unwrap(), PageId(20));
        assert_eq!(page1.child_page_id(2).unwrap(), PageId(30));
        assert_eq!(page1.child_page_id(3).unwrap(), PageId(40));
    }

    #[test]
    fn test_merge_insufficient_space() {
        let mut page1 = BTreeInternalPage::new(PageId(1));
        let mut page2 = BTreeInternalPage::new(PageId(2));

        // Fill both pages until they're almost full
        let mut i = 0;
        while page1.available_space() > 100 {
            let key = format!("key{:04}", i).into_bytes();
            if page1.insert_key_and_child(&key, PageId(i)).is_err() {
                break;
            }
            i += 1;
        }

        while page2.available_space() > 100 {
            let key = format!("key{:04}", i).into_bytes();
            if page2.insert_key_and_child(&key, PageId(i)).is_err() {
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
    fn test_needs_merge() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Empty page needs merge
        assert!(page.needs_merge());

        // Add minimum keys
        for i in 0..MIN_KEYS_PER_INTERNAL_PAGE {
            page.insert_key_and_child(&format!("key{}", i).into_bytes(), PageId(i as u32))
                .unwrap();
        }

        // Should not need merge anymore
        assert!(!page.needs_merge());

        // Delete one key to go below minimum
        page.delete_key_at(0).unwrap();
        assert!(page.needs_merge());
    }

    #[test]
    fn test_merge_preserves_order() {
        let mut page1 = BTreeInternalPage::new(PageId(1));
        let mut page2 = BTreeInternalPage::new(PageId(2));

        // Add alternating keys
        page1.insert_key_and_child(b"10", PageId(10)).unwrap();
        page1.insert_key_and_child(b"30", PageId(30)).unwrap();
        page1.insert_key_and_child(b"50", PageId(50)).unwrap();

        page2.insert_key_and_child(b"20", PageId(20)).unwrap();
        page2.insert_key_and_child(b"40", PageId(40)).unwrap();
        page2.insert_key_and_child(b"60", PageId(60)).unwrap();

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
        let mut left = BTreeInternalPage::new(PageId(1));
        let mut right = BTreeInternalPage::new(PageId(2));

        // Left sibling has more keys
        for i in 0..6 {
            left.insert_key_and_child(&format!("key{:02}", i).into_bytes(), PageId(i as u32))
                .unwrap();
        }

        // Right sibling has fewer keys
        right.insert_key_and_child(b"key10", PageId(10)).unwrap();
        right.insert_key_and_child(b"key11", PageId(11)).unwrap();

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

        // Verify all keys in right are greater than separator
        for i in 0..right_count_after {
            if let Some(key) = right.get_key(i as usize) {
                if !key.is_empty() {
                    assert!(key > separator);
                }
            }
        }
    }

    #[test]
    fn test_redistribute_to_right_sibling() {
        let mut left = BTreeInternalPage::new(PageId(1));
        let mut right = BTreeInternalPage::new(PageId(2));

        // Left sibling has more keys
        for i in 0..6 {
            left.insert_key_and_child(&format!("key{:02}", i).into_bytes(), PageId(i as u32))
                .unwrap();
        }

        // Right sibling has fewer keys
        right.insert_key_and_child(b"key10", PageId(10)).unwrap();
        right.insert_key_and_child(b"key11", PageId(11)).unwrap();

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

        // Verify all keys in right are greater than separator
        for i in 0..right_count_after {
            if let Some(key) = right.get_key(i as usize) {
                if !key.is_empty() {
                    assert!(key > separator);
                }
            }
        }
    }

    #[test]
    fn test_redistribute_insufficient_keys() {
        let mut left = BTreeInternalPage::new(PageId(1));
        let mut right = BTreeInternalPage::new(PageId(2));

        // Both have minimum keys
        left.insert_key_and_child(b"key1", PageId(1)).unwrap();
        left.insert_key_and_child(b"key2", PageId(2)).unwrap();

        right.insert_key_and_child(b"key3", PageId(3)).unwrap();

        // Redistribute should fail
        assert!(left.redistribute_with(&mut right, false).is_err());
        assert!(right.redistribute_with(&mut left, true).is_err());
    }

    #[test]
    fn test_redistribute_maintains_order() {
        let mut left = BTreeInternalPage::new(PageId(1));
        let mut right = BTreeInternalPage::new(PageId(2));

        // Create imbalanced pages with sequential keys
        for i in 0..8 {
            left.insert_key_and_child(&format!("{:02}", i).into_bytes(), PageId(i as u32))
                .unwrap();
        }

        right.insert_key_and_child(b"10", PageId(10)).unwrap();
        right.insert_key_and_child(b"11", PageId(11)).unwrap();

        // Redistribute
        let separator = left.redistribute_with(&mut right, false).unwrap();

        // Collect all keys
        let mut all_keys = Vec::new();
        for i in 0..left.slot_count() {
            if let Some(key) = left.get_key(i as usize) {
                all_keys.push(key);
            }
        }
        all_keys.push(separator);
        for i in 0..right.slot_count() {
            if let Some(key) = right.get_key(i as usize) {
                if !key.is_empty() {
                    all_keys.push(key);
                }
            }
        }

        // Verify order is maintained
        for i in 1..all_keys.len() {
            assert!(all_keys[i - 1] < all_keys[i]);
        }
    }

    #[test]
    fn test_serialization_deserialization() {
        let mut page = BTreeInternalPage::new(PageId(42));
        page.set_level(3);

        // Add some data
        page.insert_key_and_child(b"key1", PageId(10)).unwrap();
        page.insert_key_and_child(b"key2", PageId(20)).unwrap();
        page.insert_key_and_child(b"key3", PageId(30)).unwrap();

        // Get the raw data
        let data = *page.data();
        let page_id = page.page_id();

        // Create new page from raw data
        let restored = BTreeInternalPage::from_page_data(page_id, data);

        // Verify everything is the same
        assert_eq!(restored.page_id(), page_id);
        assert_eq!(restored.level(), 3);
        assert_eq!(restored.slot_count(), 3);
        assert_eq!(restored.get_key(0).unwrap(), b"key1");
        assert_eq!(restored.get_key(1).unwrap(), b"key2");
        assert_eq!(restored.get_key(2).unwrap(), b"key3");
        assert_eq!(restored.child_page_id(0).unwrap(), PageId(10));
        assert_eq!(restored.child_page_id(1).unwrap(), PageId(20));
        assert_eq!(restored.child_page_id(2).unwrap(), PageId(30));
    }

    #[test]
    fn test_validate_valid_page() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Empty page should be valid
        assert!(page.validate().is_ok());

        // Add some data
        page.insert_key_and_child(b"key1", PageId(10)).unwrap();
        page.insert_key_and_child(b"key2", PageId(20)).unwrap();

        // Should still be valid
        assert!(page.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_page_type() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Corrupt the page type
        let mut header = page.header();
        header.data[0] = 0xFF; // Invalid page type
        page.write_header(&header);

        assert!(page.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_bounds() {
        let mut page = BTreeInternalPage::new(PageId(1));

        // Test lower < header size
        let mut header = page.header();
        header.set_lower(10); // Less than BTREE_INTERNAL_HEADER_SIZE
        page.write_header(&header);
        assert!(page.validate().is_err());

        // Test upper > page size
        let mut page2 = BTreeInternalPage::new(PageId(2));
        let mut header2 = page2.header();
        header2.set_upper(PAGE_SIZE as u16 + 1);
        page2.write_header(&header2);
        assert!(page2.validate().is_err());

        // Test lower > upper
        let mut page3 = BTreeInternalPage::new(PageId(3));
        let mut header3 = page3.header();
        header3.set_lower(5000);
        header3.set_upper(4000);
        page3.write_header(&header3);
        assert!(page3.validate().is_err());
    }

    #[test]
    fn test_page_persistence_roundtrip() {
        let mut page = BTreeInternalPage::new(PageId(100));
        page.set_level(5);

        // Fill with various keys
        for i in 0..10 {
            let key = format!("test_key_{:03}", i).into_bytes();
            page.insert_key_and_child(&key, PageId(i * 10)).unwrap();
        }

        // Validate before serialization
        assert!(page.validate().is_ok());

        // Simulate persistence
        let page_data = *page.data();

        // Restore from data
        let restored = BTreeInternalPage::from_page_data(PageId(100), page_data);

        // Validate after restoration
        assert!(restored.validate().is_ok());

        // Verify all data
        assert_eq!(restored.level(), 5);
        assert_eq!(restored.slot_count(), 10);
        for i in 0..10 {
            let expected_key = format!("test_key_{:03}", i).into_bytes();
            assert_eq!(restored.get_key(i).unwrap(), expected_key);
            assert_eq!(restored.child_page_id(i).unwrap(), PageId((i * 10) as u32));
        }
    }
}
