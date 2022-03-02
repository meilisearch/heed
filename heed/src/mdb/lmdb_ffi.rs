pub use ffi::{
    mdb_cursor_close, mdb_cursor_del, mdb_cursor_get, mdb_cursor_open, mdb_cursor_put,
    mdb_dbi_open, mdb_del, mdb_drop, mdb_env_close, mdb_env_copyfd2 as mdb_env_copy2fd,
    mdb_env_create, mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_env_set_maxreaders,
    mdb_env_sync, mdb_filehandle_t, mdb_get, mdb_put, mdb_txn_abort, mdb_txn_begin, mdb_txn_commit,
    MDB_cursor, MDB_dbi, MDB_env, MDB_txn, MDB_APPEND, MDB_CP_COMPACT, MDB_CREATE, MDB_CURRENT,
    MDB_RDONLY,
};
use lmdb_sys as ffi;

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
    ffi::MDB_val { mv_data: value.as_ptr() as *mut libc::c_void, mv_size: value.len() }
}

pub unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}
