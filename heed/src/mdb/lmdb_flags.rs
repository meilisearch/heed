use lmdb_master_sys as ffi;

/// LMDB flags (see <http://www.lmdb.tech/doc/group__mdb__env.html> for more details).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Flag {
    /// mmap at a fixed address (experimental)
    Fixedmap = ffi::MDB_FIXEDMAP,
    /// no environment directory
    NoSubDir = ffi::MDB_NOSUBDIR,
    /// don't fsync after commit
    NoSync = ffi::MDB_NOSYNC,
    /// read only
    RdOnly = ffi::MDB_RDONLY,
    /// don't fsync metapage after commit
    NoMetaSync = ffi::MDB_NOMETASYNC,
    /// use writable mmap
    WriteMap = ffi::MDB_WRITEMAP,
    /// use asynchronous msync when MDB_WRITEMAP is used
    MapAsync = ffi::MDB_MAPASYNC,
    /// tie reader locktable slots to MDB_txn objects instead of to threads
    NoTls = ffi::MDB_NOTLS,
    /// don't do any locking, caller must manage their own locks
    NoLock = ffi::MDB_NOLOCK,
    /// don't do readahead (no effect on Windows)
    NoRdAhead = ffi::MDB_NORDAHEAD,
    /// don't initialize malloc'd memory before writing to datafile
    NoMemInit = ffi::MDB_NOMEMINIT,
}
