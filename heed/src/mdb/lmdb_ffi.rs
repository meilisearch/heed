use lmdb_sys as ffi;

pub use ffi::MDB_cursor as MDB_cursor;
pub use ffi::MDB_dbi as MDB_dbi;
pub use ffi::MDB_env as MDB_env;
pub use ffi::mdb_filehandle_t as mdb_filehandle_t;
pub use ffi::MDB_txn as MDB_txn;

pub use ffi::MDB_APPEND as MDB_APPEND;
pub use ffi::MDB_CP_COMPACT as MDB_CP_COMPACT;
pub use ffi::MDB_CREATE as MDB_CREATE;
pub use ffi::MDB_CURRENT as MDB_CURRENT;
pub use ffi::MDB_RDONLY as MDB_RDONLY;

pub use ffi::mdb_env_close as mdb_env_close;
pub use ffi::mdb_env_copyfd2 as mdb_env_copy2fd;
pub use ffi::mdb_env_create as mdb_env_create;
pub use ffi::mdb_env_open as mdb_env_open;
pub use ffi::mdb_env_set_mapsize as mdb_env_set_mapsize;
pub use ffi::mdb_env_set_maxdbs as mdb_env_set_maxdbs;
pub use ffi::mdb_env_set_maxreaders as mdb_env_set_maxreaders;
pub use ffi::mdb_env_sync as mdb_env_sync;

pub use ffi::mdb_dbi_open as mdb_dbi_open;
pub use ffi::mdb_del as mdb_del;
pub use ffi::mdb_drop as mdb_drop;
pub use ffi::mdb_get as mdb_get;
pub use ffi::mdb_put as mdb_put;

pub use ffi::mdb_txn_abort as mdb_txn_abort;
pub use ffi::mdb_txn_begin as mdb_txn_begin;
pub use ffi::mdb_txn_commit as mdb_txn_commit;

pub use ffi::mdb_cursor_close as mdb_cursor_close;
pub use ffi::mdb_cursor_del as mdb_cursor_del;
pub use ffi::mdb_cursor_get as mdb_cursor_get;
pub use ffi::mdb_cursor_open as mdb_cursor_open;
pub use ffi::mdb_cursor_put as mdb_cursor_put;

pub mod cursor_op {
    use super::ffi::{self, MDB_cursor_op};

    pub const MDB_FIRST: MDB_cursor_op = ffi::MDB_FIRST;
    pub const MDB_LAST: MDB_cursor_op = ffi::MDB_LAST;
    pub const MDB_SET_RANGE: MDB_cursor_op = ffi::MDB_SET_RANGE;
    pub const MDB_PREV: MDB_cursor_op = ffi::MDB_PREV;
    pub const MDB_NEXT: MDB_cursor_op = ffi::MDB_NEXT;
    pub const MDB_GET_CURRENT: MDB_cursor_op = ffi::MDB_GET_CURRENT;
}

pub unsafe fn into_val(value: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val {
        mv_data: value.as_ptr() as *mut libc::c_void,
        mv_size: value.len(),
    }
}

pub unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}
