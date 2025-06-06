//! Error types for heed-core

use std::borrow::Cow;
use std::fmt;
use std::io;
use thiserror::Error;

/// The main error type for heed-core operations
#[derive(Error, Debug)]
pub enum Error {
    /// I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    
    /// Transaction conflict detected
    #[error("Transaction conflict: {0}")]
    Conflict(ConflictDetails),
    
    /// Database corruption detected
    #[error("Corruption detected: {details}")]
    Corruption { 
        /// Description of the corruption
        details: String, 
        /// Page where corruption was detected
        page_id: Option<PageId> 
    },
    
    /// Key not found in database
    #[error("Key not found")]
    KeyNotFound,
    
    /// Database is full
    #[error("Database full: current size is {current_size} bytes, max is {max_size} bytes")]
    DatabaseFull { 
        /// Current database size
        current_size: u64,
        /// Maximum allowed size
        max_size: u64
    },
    
    /// Invalid operation attempted
    #[error("Invalid operation: {0}")]
    InvalidOperation(&'static str),
    
    /// Version mismatch
    #[error("Version mismatch: expected {expected}, found {found}")]
    VersionMismatch {
        /// Expected version
        expected: u32,
        /// Found version
        found: u32,
    },
    
    /// Bad transaction
    #[error("Bad transaction")]
    BadTransaction,
    
    /// Invalid database
    #[error("Invalid database")]
    InvalidDatabase,
    
    /// Page not found
    #[error("Page {0} not found")]
    PageNotFound(PageId),
    
    /// Encoding error
    #[error("Encoding error: {0}")]
    Encoding(Cow<'static, str>),
    
    /// Decoding error
    #[error("Decoding error: {0}")]
    Decoding(Cow<'static, str>),
    
    /// Environment already open
    #[error("Environment already open")]
    EnvironmentAlreadyOpen,
    
    /// Invalid parameter
    #[error("Invalid parameter: {0}")]
    InvalidParameter(&'static str),
    
    /// Map full - too many databases open
    #[error("Map full: too many databases open")]
    MapFull,
    
    /// Reader table full
    #[error("Reader table full")]
    ReadersFull,
    
    /// Transaction too big
    #[error("Transaction too big: {size} bytes")]
    TxnFull { 
        /// Size that was attempted
        size: usize 
    },
    
    /// Cursor is not positioned
    #[error("Cursor is not positioned")]
    NotFound,
    
    /// Invalid page ID
    #[error("Invalid page ID: {0}")]
    InvalidPageId(PageId),
    
    /// Invalid page type
    #[error("Invalid page type: expected {expected:?}, found {found:?}")]
    InvalidPageType {
        /// Expected page type
        expected: PageType,
        /// Found page type
        found: PageType,
    },
    
    /// Database corrupted
    #[error("Database corrupted")]
    Corrupted,
    
    /// Custom error
    #[error("{0}")]
    Custom(Cow<'static, str>),
}

/// Details about a transaction conflict
#[derive(Debug, Clone)]
pub struct ConflictDetails {
    /// The transaction that had the conflict
    pub txn_id: TransactionId,
    /// The page that was in conflict
    pub conflicting_page: PageId,
    /// The operation that caused the conflict
    pub operation: Operation,
}

impl fmt::Display for ConflictDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f, 
            "transaction {} conflicted on page {} during {:?}",
            self.txn_id, self.conflicting_page, self.operation
        )
    }
}

/// Type of database operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Read operation
    Read,
    /// Write operation
    Write,
    /// Delete operation
    Delete,
    /// Cursor operation
    Cursor,
}

/// Page identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub u64);

impl PageId {
    /// Convert to byte offset in file
    pub fn to_offset(self, page_size: usize) -> u64 {
        self.0 * page_size as u64
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Transaction identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionId(pub u64);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Page type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Branch page (internal node)
    Branch,
    /// Leaf page (contains data)
    Leaf,
    /// Overflow page (for large data)
    Overflow,
    /// Meta page (database metadata)
    Meta,
    /// Free page
    Free,
}

/// Result type alias for heed-core operations
pub type Result<T> = std::result::Result<T, Error>;

/// Convert LMDB error codes to our error type (for compatibility)
impl Error {
    /// Convert from LMDB error code
    pub fn from_err_code(code: i32) -> Self {
        match code {
            libc::ENOENT => Error::NotFound,
            libc::EIO => Error::Io(io::Error::from_raw_os_error(code)),
            libc::ENOMEM => Error::Io(io::Error::new(io::ErrorKind::OutOfMemory, "out of memory")),
            libc::EACCES => Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, "permission denied")),
            libc::EBUSY => Error::Io(io::Error::new(io::ErrorKind::Other, "resource busy")),
            libc::EINVAL => Error::InvalidParameter("invalid parameter"),
            libc::ENOSPC => Error::MapFull,
            -30799 => Error::KeyNotFound, // MDB_NOTFOUND
            -30798 => Error::PageNotFound(PageId(0)), // MDB_PAGE_NOTFOUND
            -30797 => Error::Corruption { details: "corrupted database".into(), page_id: None }, // MDB_CORRUPTED
            -30796 => Error::Custom("panic in transaction".into()), // MDB_PANIC
            -30795 => Error::VersionMismatch { expected: 1, found: 0 }, // MDB_VERSION_MISMATCH
            -30794 => Error::InvalidDatabase, // MDB_INVALID
            -30793 => Error::MapFull, // MDB_MAP_FULL
            -30792 => Error::MapFull, // MDB_DBS_FULL
            -30791 => Error::ReadersFull, // MDB_READERS_FULL
            -30788 => Error::TxnFull { size: 0 }, // MDB_TXN_FULL
            -30787 => Error::Custom("cursor stack too deep".into()), // MDB_CURSOR_FULL
            -30786 => Error::Custom("page has no more space".into()), // MDB_PAGE_FULL
            -30785 => Error::DatabaseFull { current_size: 0, max_size: 0 }, // MDB_MAP_RESIZED
            -30784 => Error::InvalidOperation("incompatible operation"), // MDB_INCOMPATIBLE
            -30783 => Error::BadTransaction, // MDB_BAD_RSLOT
            -30782 => Error::BadTransaction, // MDB_BAD_TXN
            -30781 => Error::InvalidParameter("bad value size"), // MDB_BAD_VALSIZE
            -30780 => Error::InvalidDatabase, // MDB_BAD_DBI
            _ => Error::Custom(format!("unknown error code: {}", code).into()),
        }
    }
    
    /// Convert to LMDB error code (for compatibility)
    pub fn to_err_code(&self) -> i32 {
        match self {
            Error::Io(e) => e.raw_os_error().unwrap_or(libc::EIO),
            Error::KeyNotFound => -30799,
            Error::PageNotFound(_) => -30798,
            Error::Corruption { .. } => -30797,
            Error::VersionMismatch { .. } => -30795,
            Error::InvalidDatabase => -30794,
            Error::MapFull => -30793,
            Error::ReadersFull => -30791,
            Error::TxnFull { .. } => -30788,
            Error::DatabaseFull { .. } => -30785,
            Error::BadTransaction => -30782,
            Error::NotFound => -30799,
            _ => -1,
        }
    }
}