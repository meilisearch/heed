use mdbx_sys as ffi;

pub use ffi::MDBX_cursor as MDB_cursor;
pub use ffi::MDBX_dbi as MDB_dbi;
pub use ffi::MDBX_env as MDB_env;
pub use ffi::mdbx_filehandle_t as mdb_filehandle_t;
pub use ffi::MDBX_txn as MDB_txn;

pub use ffi::MDBX_APPEND as MDB_APPEND;
pub use ffi::MDBX_CP_COMPACT as MDB_CP_COMPACT;
pub use ffi::MDBX_CREATE as MDB_CREATE;
pub use ffi::MDBX_CURRENT as MDB_CURRENT;
pub use ffi::MDBX_RDONLY as MDB_RDONLY;

pub use ffi::mdbx_env_close as mdb_env_close;
pub use ffi::mdbx_env_copy2fd as mdb_env_copy2fd;
pub use ffi::mdbx_env_create as mdb_env_create;
pub use ffi::mdbx_env_open as mdb_env_open;
pub use ffi::mdbx_env_set_mapsize as mdb_env_set_mapsize;
pub use ffi::mdbx_env_set_maxdbs as mdb_env_set_maxdbs;
pub use ffi::mdbx_env_set_maxreaders as mdb_env_set_maxreaders;
pub use ffi::mdbx_env_sync as mdb_env_sync;

pub use ffi::mdbx_dbi_open as mdb_dbi_open;
pub use ffi::mdbx_dbi_sequence;
pub use ffi::mdbx_del as mdb_del;
pub use ffi::mdbx_drop as mdb_drop;
pub use ffi::mdbx_get as mdb_get;
pub use ffi::mdbx_put as mdb_put;

pub use ffi::mdbx_txn_abort as mdb_txn_abort;
pub use ffi::mdbx_txn_begin as mdb_txn_begin;
pub use ffi::mdbx_txn_commit as mdb_txn_commit;

pub use ffi::mdbx_cursor_close as mdb_cursor_close;
pub use ffi::mdbx_cursor_del as mdb_cursor_del;
pub use ffi::mdbx_cursor_get as mdb_cursor_get;
pub use ffi::mdbx_cursor_open as mdb_cursor_open;
pub use ffi::mdbx_cursor_put as mdb_cursor_put;

pub mod cursor_op {
    use super::ffi::MDBX_cursor_op;

    pub const MDB_FIRST: MDBX_cursor_op = MDBX_cursor_op::MDBX_FIRST;
    pub const MDB_LAST: MDBX_cursor_op = MDBX_cursor_op::MDBX_LAST;
    pub const MDB_SET_RANGE: MDBX_cursor_op = MDBX_cursor_op::MDBX_SET_RANGE;
    pub const MDB_PREV: MDBX_cursor_op = MDBX_cursor_op::MDBX_PREV;
    pub const MDB_NEXT: MDBX_cursor_op = MDBX_cursor_op::MDBX_NEXT;
    pub const MDB_GET_CURRENT: MDBX_cursor_op = MDBX_cursor_op::MDBX_GET_CURRENT;
}

pub unsafe fn into_val(value: &[u8]) -> ffi::MDBX_val {
    ffi::MDBX_val {
        iov_base: value.as_ptr() as *mut libc::c_void,
        iov_len: value.len(),
    }
}

pub unsafe fn from_val<'a>(value: ffi::MDBX_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.iov_base as *const u8, value.iov_len)
}
