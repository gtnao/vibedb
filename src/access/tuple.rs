use crate::storage::page::PageId;
use std::cmp::Ordering;

/// Unique identifier for a tuple within the database
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TupleId {
    pub page_id: PageId,
    pub slot_id: u16,
}

impl TupleId {
    pub fn new(page_id: PageId, slot_id: u16) -> Self {
        Self { page_id, slot_id }
    }
}

impl PartialOrd for TupleId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TupleId {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare by page_id, then by slot_id
        match self.page_id.0.cmp(&other.page_id.0) {
            Ordering::Equal => self.slot_id.cmp(&other.slot_id),
            other => other,
        }
    }
}

/// Represents a row in the database
#[derive(Debug, Clone)]
pub struct Tuple {
    pub tuple_id: TupleId,
    pub data: Vec<u8>,
}

impl Tuple {
    pub fn new(tuple_id: TupleId, data: Vec<u8>) -> Self {
        Self { tuple_id, data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_id_creation() {
        let tid = TupleId::new(PageId(42), 7);
        assert_eq!(tid.page_id, PageId(42));
        assert_eq!(tid.slot_id, 7);
    }

    #[test]
    fn test_tuple_id_equality() {
        let tid1 = TupleId::new(PageId(1), 2);
        let tid2 = TupleId::new(PageId(1), 2);
        let tid3 = TupleId::new(PageId(1), 3);
        let tid4 = TupleId::new(PageId(2), 2);

        assert_eq!(tid1, tid2);
        assert_ne!(tid1, tid3);
        assert_ne!(tid1, tid4);
    }

    #[test]
    fn test_tuple_id_ordering() {
        let tid1 = TupleId::new(PageId(1), 5);
        let tid2 = TupleId::new(PageId(1), 10);
        let tid3 = TupleId::new(PageId(2), 3);

        assert!(tid1 < tid2); // Same page, different slot
        assert!(tid2 < tid3); // Different page
        assert!(tid1 < tid3); // Transitivity
    }

    #[test]
    fn test_tuple_id_debug() {
        let tid = TupleId::new(PageId(100), 25);
        let debug_str = format!("{:?}", tid);
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("25"));
    }

    #[test]
    fn test_tuple_creation() {
        let tid = TupleId::new(PageId(1), 0);
        let data = vec![1, 2, 3, 4, 5];
        let tuple = Tuple::new(tid, data.clone());

        assert_eq!(tuple.tuple_id, tid);
        assert_eq!(tuple.data, data);
    }

    #[test]
    fn test_tuple_empty_data() {
        let tid = TupleId::new(PageId(0), 0);
        let tuple = Tuple::new(tid, vec![]);

        assert_eq!(tuple.data.len(), 0);
    }
}
