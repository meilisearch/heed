use lmdb_master3_sys as ffi;

/// LMDB flags (see <http://www.lmdb.tech/doc/group__mdb__env.html> for more details).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Flags {
    MdbFixedmap = ffi::MDB_FIXEDMAP,
    MdbNoSubDir = ffi::MDB_NOSUBDIR,
    MdbNoSync = ffi::MDB_NOSYNC,
    MdbRdOnly = ffi::MDB_RDONLY,
    MdbNoMetaSync = ffi::MDB_NOMETASYNC,
    MdbWriteMap = ffi::MDB_WRITEMAP,
    MdbMapAsync = ffi::MDB_MAPASYNC,
    MdbNoTls = ffi::MDB_NOTLS,
    MdbNoLock = ffi::MDB_NOLOCK,
    MdbNoRdAhead = ffi::MDB_NORDAHEAD,
    MdbNoMemInit = ffi::MDB_NOMEMINIT,
}
