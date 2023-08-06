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

bitflags! {
    /// LMDB put flags (see <http://www.lmdb.tech/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0>
    /// or <http://www.lmdb.tech/doc/group__mdb.html#ga1f83ccb40011837ff37cc32be01ad91e> for more details).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    #[repr(transparent)]
    pub struct PutFlags: u32 {
        /// Enter the new key/data pair only if it does not already appear in the database.
        ///
        /// This flag may only be specified if the database was opened with MDB_DUPSORT.
        /// The function will return MDB_KEYEXIST if the key/data pair already appears in the database.
        const NO_DUP_DATA = ffi::MDB_NODUPDATA;
        /// Enter the new key/data pair only if the key does not already appear in the database.
        ///
        /// The function will return MDB_KEYEXIST if the key already appears in the database, even if the database supports duplicates (MDB_DUPSORT).
        /// The data parameter will be set to point to the existing item.
        const NO_OVERWRITE = ffi::MDB_NOOVERWRITE;
        /// Append the given key/data pair to the end of the database.
        ///
        /// This option allows fast bulk loading when keys are already known to be in the correct order.
        /// Loading unsorted keys with this flag will cause a MDB_KEYEXIST error.
        const APPEND = ffi::MDB_APPEND;
        /// Append the given key/data pair to the end of the database but for sorted dup data.
        ///
        /// This option allows fast bulk loading when keys and dup data are already known to be in the correct order.
        /// Loading unsorted key/values with this flag will cause a MDB_KEYEXIST error.
        const APPEND_DUP = ffi::MDB_APPENDDUP;
    }
}
