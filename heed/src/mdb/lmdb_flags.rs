use lmdb_master_sys as ffi;

/// LMDB flags (see <http://www.lmdb.tech/doc/group__mdb__env.html> for more details).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Flag {
    Fixedmap = ffi::MDB_FIXEDMAP,
    NoSubDir = ffi::MDB_NOSUBDIR,
    NoSync = ffi::MDB_NOSYNC,
    RdOnly = ffi::MDB_RDONLY,
    NoMetaSync = ffi::MDB_NOMETASYNC,
    WriteMap = ffi::MDB_WRITEMAP,
    MapAsync = ffi::MDB_MAPASYNC,
    NoTls = ffi::MDB_NOTLS,
    NoLock = ffi::MDB_NOLOCK,
    NoRdAhead = ffi::MDB_NORDAHEAD,
    NoMemInit = ffi::MDB_NOMEMINIT,
}
