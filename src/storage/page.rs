pub mod heap_page;

use crate::storage::PAGE_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub u32);

pub trait Page {
    fn page_id(&self) -> PageId;
    fn data(&self) -> &[u8; PAGE_SIZE];
    fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE];
}

pub use heap_page::HeapPage;
