use lmdb_sys as ffi;

#[rustfmt::skip]
pub use ffi::{
    mdb_filehandle_t,
    MDB_cursor,
    MDB_dbi,
    MDB_env,
    MDB_stat,
    MDB_txn,

    MDB_APPEND,
    MDB_CP_COMPACT,
    MDB_CREATE,
    MDB_CURRENT,
    MDB_RDONLY,
};

pub unsafe fn mdb_env_close(env: *mut MDB_env) {
    ffi::mdb_env_close(env)
}

pub unsafe fn mdb_env_copy2fd(
    env: *mut MDB_env,
    fd: mdb_filehandle_t,
    flags: ::libc::c_uint,
) -> ::libc::c_int {
    ffi::mdb_env_copyfd2(env, fd, flags)
}

pub unsafe fn mdb_env_create(env: *mut *mut MDB_env) -> ::libc::c_int {
    ffi::mdb_env_create(env)
}

pub unsafe fn mdb_env_get_flags(env: *mut MDB_env, flags: *mut ::libc::c_uint) -> ::libc::c_int {
    ffi::mdb_env_get_flags(env, flags)
}

// FIXME: should we expose ffi::MDB_envinfo as this function cannot be called without it? 🤔
pub unsafe fn mdb_env_info(env: *mut MDB_env, stat: *mut ffi::MDB_envinfo) -> ::libc::c_int {
    ffi::mdb_env_info(env, stat)
}

pub unsafe fn mdb_env_open(
    env: *mut MDB_env,
    path: *const ::libc::c_char,
    flags: ::libc::c_uint,
    mode: ffi::mdb_mode_t,
) -> ::libc::c_int {
    ffi::mdb_env_open(env, path, flags, mode)
}

pub unsafe fn mdb_env_set_mapsize(env: *mut MDB_env, size: ffi::mdb_size_t) -> ::libc::c_int {
    ffi::mdb_env_set_mapsize(env, size)
}

pub unsafe fn mdb_env_set_maxdbs(env: *mut MDB_env, dbs: MDB_dbi) -> ::libc::c_int {
    ffi::mdb_env_set_maxdbs(env, dbs)
}

pub unsafe fn mdb_env_set_maxreaders(env: *mut MDB_env, readers: ::libc::c_uint) -> ::libc::c_int {
    ffi::mdb_env_set_maxreaders(env, readers)
}

pub unsafe fn mdb_env_stat(env: *mut MDB_env, stat: *mut MDB_stat) -> ::libc::c_int {
    ffi::mdb_env_stat(env, stat)
}

pub unsafe fn mdb_env_sync(env: *mut MDB_env, force: ::libc::c_int) -> ::libc::c_int {
    ffi::mdb_env_sync(env, force)
}

pub unsafe fn mdb_dbi_close(env: *mut MDB_env, dbi: MDB_dbi) {
    ffi::mdb_dbi_close(env, dbi)
}

pub unsafe fn mdb_dbi_open(
    txn: *mut MDB_txn,
    name: *const ::libc::c_char,
    flags: ::libc::c_uint,
    dbi: *mut MDB_dbi,
) -> ::libc::c_int {
    ffi::mdb_dbi_open(txn, name, flags, dbi)
}

pub unsafe fn mdb_del(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
) -> ::libc::c_int {
    ffi::mdb_del(txn, dbi, key, data)
}

pub unsafe fn mdb_drop(txn: *mut MDB_txn, dbi: MDB_dbi, del: ::libc::c_int) -> ::libc::c_int {
    ffi::mdb_drop(txn, dbi, del)
}

pub unsafe fn mdb_get(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
) -> ::libc::c_int {
    ffi::mdb_get(txn, dbi, key, data)
}

// FIXME: should we expose ffi::MDB_val as this function cannot be called without it? 🤔
pub unsafe fn mdb_put(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
    flags: ::libc::c_uint,
) -> ::libc::c_int {
    ffi::mdb_put(txn, dbi, key, data, flags)
}

pub unsafe fn mdb_stat(txn: *mut MDB_txn, dbi: MDB_dbi, stat: *mut MDB_stat) -> ::libc::c_int {
    ffi::mdb_stat(txn, dbi, stat)
}

pub unsafe fn mdb_txn_abort(txn: *mut MDB_txn) {
    ffi::mdb_txn_abort(txn)
}

pub unsafe fn mdb_txn_begin(
    env: *mut MDB_env,
    parent: *mut MDB_txn,
    flags: ::libc::c_uint,
    txn: *mut *mut MDB_txn,
) -> ::libc::c_int {
    ffi::mdb_txn_begin(env, parent, flags, txn)
}

pub unsafe fn mdb_txn_commit(txn: *mut MDB_txn) -> ::libc::c_int {
    ffi::mdb_txn_commit(txn)
}

pub unsafe fn mdb_cursor_close(cursor: *mut MDB_cursor) {
    ffi::mdb_cursor_close(cursor)
}

pub unsafe fn mdb_cursor_del(cursor: *mut MDB_cursor, flags: ::libc::c_uint) -> ::libc::c_int {
    ffi::mdb_cursor_del(cursor, flags)
}

pub unsafe fn mdb_cursor_get(
    cursor: *mut MDB_cursor,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
    op: ffi::MDB_cursor_op,
) -> ::libc::c_int {
    ffi::mdb_cursor_get(cursor, key, data, op)
}

pub unsafe fn mdb_cursor_open(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    cursor: *mut *mut MDB_cursor,
) -> ::libc::c_int {
    ffi::mdb_cursor_open(txn, dbi, cursor)
}

pub unsafe fn mdb_cursor_put(
    cursor: *mut MDB_cursor,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
    flags: ::libc::c_uint,
) -> ::libc::c_int {
    ffi::mdb_cursor_put(cursor, key, data, flags)
}

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
