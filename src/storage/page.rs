pub mod btree_internal_page;
pub mod btree_leaf_page;
pub mod heap_page;
pub mod utils;

use crate::storage::PAGE_SIZE;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub u32);

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}

pub trait Page {
    fn page_id(&self) -> PageId;
    fn data(&self) -> &[u8; PAGE_SIZE];
    fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE];
}

pub use heap_page::HeapPage;
