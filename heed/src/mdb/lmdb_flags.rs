use bitflags::bitflags;
use lmdb_master_sys as ffi;

bitflags! {
    /// LMDB environment flags (see <http://www.lmdb.tech/doc/group__mdb__env.html> for more details).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[repr(transparent)]
    pub struct EnvFlags: u32 {
        /// mmap at a fixed address (experimental)
        const FIXEDMAP = ffi::MDB_FIXEDMAP;
        /// no environment directory
        const NO_SUB_DIR = ffi::MDB_NOSUBDIR;
        /// don't fsync after commit
        const NO_SYNC = ffi::MDB_NOSYNC;
        /// read only
        const READ_ONLY = ffi::MDB_RDONLY;
        /// don't fsync metapage after commit
        const NO_META_SYNC = ffi::MDB_NOMETASYNC;
        /// use writable mmap
        const WRITE_MAP = ffi::MDB_WRITEMAP;
        /// use asynchronous msync when MDB_WRITEMAP is used
        const MAP_ASYNC = ffi::MDB_MAPASYNC;
        /// tie reader locktable slots to MDB_txn objects instead of to threads
        const NO_TLS = ffi::MDB_NOTLS;
        /// don't do any locking, caller must manage their own locks
        const NO_LOCK = ffi::MDB_NOLOCK;
        /// don't do readahead (no effect on Windows)
        const NO_READ_AHEAD = ffi::MDB_NORDAHEAD;
        /// don't initialize malloc'd memory before writing to datafile
        const NO_MEM_INIT = ffi::MDB_NOMEMINIT;
    }
}

bitflags! {
    /// LMDB database flags (see <http://www.lmdb.tech/doc/group__mdb__dbi__open.html> for more details).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    #[repr(transparent)]
    pub struct DatabaseFlags: u32 {
        /// use sorted duplicates
        const DUP_SORT = ffi::MDB_DUPSORT;
        /// create DB if not already existing
        const CREATE = ffi::MDB_CREATE;
    }
}
