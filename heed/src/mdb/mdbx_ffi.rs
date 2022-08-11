use mdbx_sys as ffi;

#[rustfmt::skip]
pub use ffi::{
    MDBX_cursor as MDB_cursor,
    MDBX_dbi as MDB_dbi,
    MDBX_env as MDB_env,
    mdbx_filehandle_t as mdb_filehandle_t,
    MDBX_txn as MDB_txn,

    MDBX_APPEND as MDB_APPEND,
    MDBX_CP_COMPACT as MDB_CP_COMPACT,
    MDBX_CREATE as MDB_CREATE,
    MDBX_CURRENT as MDB_CURRENT,
    MDBX_RDONLY as MDB_RDONLY,

    mdbx_env_close as mdb_env_close,
    mdbx_env_copy2fd as mdb_env_copy2fd,
    mdbx_env_create as mdb_env_create,
    mdbx_env_open as mdb_env_open,
    mdbx_env_set_mapsize as mdb_env_set_mapsize,
    mdbx_env_set_maxdbs as mdb_env_set_maxdbs,
    mdbx_env_set_maxreaders as mdb_env_set_maxreaders,
    mdbx_env_sync as mdb_env_sync,

    mdbx_dbi_open as mdb_dbi_open,
    mdbx_dbi_sequence,
    mdbx_del as mdb_del,
    mdbx_drop as mdb_drop,
    mdbx_get as mdb_get,
    mdbx_put as mdb_put,

    mdbx_txn_abort as mdb_txn_abort,
    mdbx_txn_begin as mdb_txn_begin,
    mdbx_txn_commit as mdb_txn_commit,

    mdbx_cursor_close as mdb_cursor_close,
    mdbx_cursor_del as mdb_cursor_del,
    mdbx_cursor_get as mdb_cursor_get,
    mdbx_cursor_open as mdb_cursor_open,
    mdbx_cursor_put as mdb_cursor_put,
};

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
    ffi::MDBX_val { iov_base: value.as_ptr() as *mut libc::c_void, iov_len: value.len() }
}

pub unsafe fn from_val<'a>(value: ffi::MDBX_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.iov_base as *const u8, value.iov_len)
}
