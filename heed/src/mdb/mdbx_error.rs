use std::error::Error as StdError;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::{fmt, str};

use libc::c_int;
use mdbx_sys as ffi;

/// An LMDB error kind.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Error {
    /// key/data pair already exists
    KeyExist,
    /// key/data pair not found (EOF)
    NotFound,
    /// Requested page not found - this usually indicates corruption
    PageNotfound,
    /// Database is corrupted (page was wrong type and so on)
    Corrupted,
    /// Environment had fatal error (i.e. update of meta page failed and so on)
    Panic,
    /// DB file version mismatch with libmdbx
    VersionMismatch,
    /// File is not a valid MDBX file
    Invalid,
    /// Environment mapsize reached
    MapFull,
    /// Environment maxdbs reached
    DbsFull,
    /// Environment maxreaders reached
    ReadersFull,
    /// Transaction has too many dirty pages, i.e transaction too big
    TxnFull,
    /// Cursor stack too deep - internal error
    CursorFull,
    /// Page has not enough space - internal error
    PageFull,
    /// Database engine was unable to extend mapping, e.g. since address space
    /// is unavailable or busy. This can mean:
    ///  - Database size extended by other process beyond to environment mapsize
    ///    and engine was unable to extend mapping while starting read transaction.
    ///    Environment should be reopened to continue.
    ///  - Engine was unable to extend mapping during write transaction
    ///    or explicit call of mdbx_env_set_geometry().
    UnableExtendMapSize,
    /// Environment or database is not compatible with the requested operation
    /// or the specified flags. This can mean:
    ///  - The operation expects an MDBX_DUPSORT / MDBX_DUPFIXED database.
    ///  - Opening a named DB when the unnamed DB has MDBX_DUPSORT/MDBX_INTEGERKEY.
    ///  - Accessing a data record as a database, or vice versa.
    ///  - The database was dropped and recreated with different flags.
    Incompatible,
    /// Invalid reuse of reader locktable slot,
    /// e.g. read-transaction already run for current thread
    BadRslot,
    /// Transaction is not valid for requested operation,
    /// e.g. had errored and be must aborted, has a child, or is invalid
    BadTxn,
    /// Invalid size or alignment of key or data for target database,
    /// either invalid subDB name
    BadValSize,
    /// The specified DBI-handle is invalid
    /// or changed by another thread/transaction
    BadDbi,
    /// Unexpected internal error, transaction should be aborted
    Problem,
    /// Another write transaction is running or environment is already used while
    /// opening with MDBX_EXCLUSIVE flag */
    Busy,
    /// The specified key has more than one associated value
    MultiVal,
    /// Bad signature of a runtime object(s), this can mean:
    ///  - memory corruption or double-free;
    ///  - ABI version mismatch (rare case);
    BadSign,
    /// Database should be recovered, but this could NOT be done for now
    /// since it opened in read-only mode
    WannaRecovery,
    /// The given key value is mismatched to the current cursor position
    KeyMismatch,
    /// Database is too large for current system,
    /// e.g. could NOT be mapped into RAM.
    TooLarge,
    /// A thread has attempted to use a not owned object,
    /// e.g. a transaction that started by another thread.
    ThreadMismatch,
    /// Overlapping read and write transactions for the current thread
    TxnOverlapping,
    /// Other error.
    Other(c_int),
}

impl Error {
    pub fn not_found(&self) -> bool {
        *self == Error::NotFound
    }

    /// Converts a raw error code to an `Error`.
    pub fn from_err_code(err_code: c_int) -> Error {
        match err_code {
            ffi::MDBX_KEYEXIST => Error::KeyExist,
            ffi::MDBX_NOTFOUND => Error::NotFound,
            ffi::MDBX_PAGE_NOTFOUND => Error::PageNotfound,
            ffi::MDBX_CORRUPTED => Error::Corrupted,
            ffi::MDBX_PANIC => Error::Panic,
            ffi::MDBX_VERSION_MISMATCH => Error::VersionMismatch,
            ffi::MDBX_INVALID => Error::Invalid,
            ffi::MDBX_MAP_FULL => Error::MapFull,
            ffi::MDBX_DBS_FULL => Error::DbsFull,
            ffi::MDBX_READERS_FULL => Error::ReadersFull,
            ffi::MDBX_TXN_FULL => Error::TxnFull,
            ffi::MDBX_CURSOR_FULL => Error::CursorFull,
            ffi::MDBX_PAGE_FULL => Error::PageFull,
            ffi::MDBX_UNABLE_EXTEND_MAPSIZE => Error::UnableExtendMapSize,
            ffi::MDBX_INCOMPATIBLE => Error::Incompatible,
            ffi::MDBX_BAD_RSLOT => Error::BadRslot,
            ffi::MDBX_BAD_TXN => Error::BadTxn,
            ffi::MDBX_BAD_VALSIZE => Error::BadValSize,
            ffi::MDBX_BAD_DBI => Error::BadDbi,
            ffi::MDBX_PROBLEM => Error::Problem,
            ffi::MDBX_BUSY => Error::Busy,
            ffi::MDBX_EMULTIVAL => Error::MultiVal,
            ffi::MDBX_EBADSIGN => Error::BadSign,
            ffi::MDBX_WANNA_RECOVERY => Error::WannaRecovery,
            ffi::MDBX_EKEYMISMATCH => Error::KeyMismatch,
            ffi::MDBX_TOO_LARGE => Error::TooLarge,
            ffi::MDBX_THREAD_MISMATCH => Error::ThreadMismatch,
            ffi::MDBX_TXN_OVERLAPPING => Error::TxnOverlapping,
            other => Error::Other(other),
        }
    }

    /// Converts an `Error` to the raw error code.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn to_err_code(&self) -> c_int {
        match *self {
            Error::KeyExist => ffi::MDBX_KEYEXIST,
            Error::NotFound => ffi::MDBX_NOTFOUND,
            Error::PageNotfound => ffi::MDBX_PAGE_NOTFOUND,
            Error::Corrupted => ffi::MDBX_CORRUPTED,
            Error::Panic => ffi::MDBX_PANIC,
            Error::VersionMismatch => ffi::MDBX_VERSION_MISMATCH,
            Error::Invalid => ffi::MDBX_INVALID,
            Error::MapFull => ffi::MDBX_MAP_FULL,
            Error::DbsFull => ffi::MDBX_DBS_FULL,
            Error::ReadersFull => ffi::MDBX_READERS_FULL,
            Error::TxnFull => ffi::MDBX_TXN_FULL,
            Error::CursorFull => ffi::MDBX_CURSOR_FULL,
            Error::PageFull => ffi::MDBX_PAGE_FULL,
            Error::UnableExtendMapSize => ffi::MDBX_UNABLE_EXTEND_MAPSIZE,
            Error::Incompatible => ffi::MDBX_INCOMPATIBLE,
            Error::BadRslot => ffi::MDBX_BAD_RSLOT,
            Error::BadTxn => ffi::MDBX_BAD_TXN,
            Error::BadValSize => ffi::MDBX_BAD_VALSIZE,
            Error::BadDbi => ffi::MDBX_BAD_DBI,
            Error::Problem => ffi::MDBX_PROBLEM,
            Error::Busy => ffi::MDBX_BUSY,
            Error::MultiVal => ffi::MDBX_EMULTIVAL,
            Error::BadSign => ffi::MDBX_EBADSIGN,
            Error::WannaRecovery => ffi::MDBX_WANNA_RECOVERY,
            Error::KeyMismatch => ffi::MDBX_EKEYMISMATCH,
            Error::TooLarge => ffi::MDBX_TOO_LARGE,
            Error::ThreadMismatch => ffi::MDBX_THREAD_MISMATCH,
            Error::TxnOverlapping => ffi::MDBX_TXN_OVERLAPPING,
            Error::Other(err_code) => err_code,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let description = unsafe {
            // This is safe since the error messages returned from mdb_strerror are static.
            let err: *const c_char = ffi::mdbx_strerror(self.to_err_code()) as *const c_char;
            str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes())
        };

        fmt.write_str(description)
    }
}

impl StdError for Error {}

pub fn mdb_result(err_code: c_int) -> Result<(), Error> {
    if err_code == ffi::MDBX_SUCCESS {
        Ok(())
    } else {
        Err(Error::from_err_code(err_code))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_description() {
        assert_eq!("Permission denied", Error::from_err_code(13).to_string());
        assert_eq!(
            "MDBX_NOTFOUND: No matching key/data pair found",
            Error::NotFound.to_string()
        );
    }
}
