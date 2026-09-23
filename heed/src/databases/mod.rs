pub use database::{Database, DatabaseOpenOptions};
#[cfg(master3)]
pub use encrypted_database::{EncryptedDatabase, EncryptedDatabaseOpenOptions};

mod database;
#[cfg(master3)]
mod encrypted_database;

/// Statistics for a database in the environment.
#[derive(Debug, Clone, Copy)]
pub struct DatabaseStat {
    /// Size of a database page.
    /// This is currently the same for all databases.
    pub page_size: u32,
    /// Depth (height) of the B-tree.
    pub depth: u32,
    /// Number of internal (non-leaf) pages
    pub branch_pages: usize,
    /// Number of leaf pages.
    pub leaf_pages: usize,
    /// Number of overflow pages.
    pub overflow_pages: usize,
    /// Number of data items.
    pub entries: usize,
}

impl DatabaseStat {
    /// Size used by this database without the free pages.
    pub fn non_free_page_size(&self) -> usize {
        (self.leaf_pages + self.branch_pages + self.overflow_pages) * self.page_size as usize
    }
}
