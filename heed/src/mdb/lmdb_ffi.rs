use std::{
    collections::HashMap,
    fmt::{Display, Write},
};

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

#[derive(Debug, Default)]
struct TracingStateInner {
    envs: std::sync::Mutex<HashMap<usize, String>>,
    txns: std::sync::Mutex<HashMap<usize, String>>,
    cursors: std::sync::Mutex<HashMap<usize, String>>,
}

struct TracingState(once_cell::sync::OnceCell<TracingStateInner>);

impl TracingState {
    pub const fn new() -> Self {
        Self(once_cell::sync::OnceCell::new())
    }

    pub fn new_env(&self, env: *mut MDB_env) -> String {
        let this = self.0.get_or_init(Default::default);
        let mut envs = this.envs.lock().unwrap();
        let id = envs.len();
        envs.entry(env as _).or_insert_with(|| format!("env_{id:04}")).clone()
    }

    pub fn env(&self, env: *mut MDB_env) -> String {
        let this = self.0.get_or_init(Default::default);
        let envs = this.envs.lock().unwrap();
        envs.get(&(env as _)).map(Clone::clone).unwrap_or_else(|| format!("unkown {env:?}"))
    }

    pub fn new_txn(&self, txn: *mut MDB_txn) -> String {
        let this = self.0.get_or_init(Default::default);
        let mut txns = this.txns.lock().unwrap();
        let id = txns.len();
        txns.entry(txn as _).or_insert_with(|| format!("txn_{id:06}")).clone()
    }

    pub fn txn(&self, txn: *mut MDB_txn) -> String {
        let this = self.0.get_or_init(Default::default);
        let txns = this.txns.lock().unwrap();
        txns.get(&(txn as _)).map(Clone::clone).unwrap_or_else(|| format!("unkown {txn:?}"))
    }

    pub fn new_cursor(&self, cursor: *mut MDB_cursor) -> String {
        let this = self.0.get_or_init(Default::default);
        let mut cursors = this.cursors.lock().unwrap();
        let id = cursors.len();
        cursors.entry(cursor as _).or_insert_with(|| format!("cursor_{id:06}")).clone()
    }

    pub fn cursor(&self, cursor: *mut MDB_cursor) -> String {
        let this = self.0.get_or_init(Default::default);
        let cursors = this.cursors.lock().unwrap();
        cursors
            .get(&(cursor as _))
            .map(Clone::clone)
            .unwrap_or_else(|| format!("unkown {cursor:?}"))
    }
}

static TRACING_STATE: TracingState = TracingState::new();

macro_rules! trace_with_thread {
    ($($arg:tt)*) => {
        let current_thread = std::thread::current();
        log::trace!("[{} ({:?})]{}", current_thread.name().unwrap_or_default(), current_thread.id(), format!($($arg)*))
    };
}

pub unsafe fn mdb_env_close(env: *mut MDB_env) {
    ffi::mdb_env_close(env);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_close({env})");
}

pub unsafe fn mdb_env_copy2fd(
    env: *mut MDB_env,
    fd: mdb_filehandle_t,
    flags: ::libc::c_uint,
) -> ::libc::c_int {
    let rc = ffi::mdb_env_copyfd2(env, fd, flags);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_copyfd2({env}, {fd}, {flags}) -> {rc}");
    rc
}

pub unsafe fn mdb_env_create(env: *mut *mut MDB_env) -> ::libc::c_int {
    let rc = ffi::mdb_env_create(env);
    let env = if rc == 0 { TRACING_STATE.new_env(*env) } else { "???".into() };
    trace_with_thread!("mdb_env_create({env}) -> {rc}");
    rc
}

pub unsafe fn mdb_env_get_flags(env: *mut MDB_env, flags: *mut ::libc::c_uint) -> ::libc::c_int {
    let rc = ffi::mdb_env_get_flags(env, flags);
    let env = TRACING_STATE.env(env);
    let flags: String = if rc == 0 { (*flags).to_string() } else { "???".into() };
    trace_with_thread!("mdb_env_get_flags({env}, out flags={flags}) -> {rc}");
    rc
}

// FIXME: should we expose ffi::MDB_envinfo as this function cannot be called without it? 🤔
pub unsafe fn mdb_env_info(env: *mut MDB_env, stat: *mut ffi::MDB_envinfo) -> ::libc::c_int {
    let rc = ffi::mdb_env_info(env, stat);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_info({env}, out stat) -> {rc}");
    rc
}

pub unsafe fn mdb_env_open(
    env: *mut MDB_env,
    path: *const ::libc::c_char,
    flags: ::libc::c_uint,
    mode: ffi::mdb_mode_t,
) -> ::libc::c_int {
    let rc = ffi::mdb_env_open(env, path, flags, mode);
    let path = std::ffi::CStr::from_ptr(path);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_open({env}, {path:?}, {flags}, {mode}) -> {rc}");
    rc
}

pub unsafe fn mdb_env_set_mapsize(env: *mut MDB_env, size: ffi::mdb_size_t) -> ::libc::c_int {
    let rc = ffi::mdb_env_set_mapsize(env, size);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_set_mapsize({env}, {size}) -> {rc}");
    rc
}

pub unsafe fn mdb_env_set_maxdbs(env: *mut MDB_env, dbs: MDB_dbi) -> ::libc::c_int {
    let rc = ffi::mdb_env_set_maxdbs(env, dbs);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_set_maxdbs({env}, {dbs}) -> {rc}");
    rc
}

pub unsafe fn mdb_env_set_maxreaders(env: *mut MDB_env, readers: ::libc::c_uint) -> ::libc::c_int {
    let rc = ffi::mdb_env_set_maxreaders(env, readers);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_set_maxreaders({env}, {readers}) -> {rc}");
    rc
}

pub unsafe fn mdb_env_stat(env: *mut MDB_env, stat: *mut MDB_stat) -> ::libc::c_int {
    let rc = ffi::mdb_env_stat(env, stat);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_stat({env}, out stat) -> {rc}");
    rc
}

pub unsafe fn mdb_env_sync(env: *mut MDB_env, force: ::libc::c_int) -> ::libc::c_int {
    let rc = ffi::mdb_env_sync(env, force);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_env_sync({env}, {force}) -> {rc}");
    rc
}

pub unsafe fn mdb_dbi_close(env: *mut MDB_env, dbi: MDB_dbi) {
    ffi::mdb_dbi_close(env, dbi);
    let env = TRACING_STATE.env(env);
    trace_with_thread!("mdb_dbi_close({env}, {dbi})");
}

pub unsafe fn mdb_dbi_open(
    txn: *mut MDB_txn,
    name: *const ::libc::c_char,
    flags: ::libc::c_uint,
    dbi: *mut MDB_dbi,
) -> ::libc::c_int {
    let rc = ffi::mdb_dbi_open(txn, name, flags, dbi);
    let name = std::ffi::CStr::from_ptr(name);
    let dbi: String = if rc == 0 { (*dbi).to_string() } else { "???".into() };
    let txn = TRACING_STATE.txn(txn);
    trace_with_thread!("mdb_dbi_open({txn}, {name:?}, {flags}, out {dbi}) -> {rc}");
    rc
}

pub unsafe fn mdb_del(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
) -> ::libc::c_int {
    let rc = ffi::mdb_del(txn, dbi, key, data);

    let txn = TRACING_STATE.txn(txn);
    trace_with_thread!("mdb_del({txn}, key, data) -> {rc}");
    let key = CArray::from_val(key);
    let data: String =
        if data.is_null() { "NULL".into() } else { format!("{}", CArray::from_val(data)) };
    trace_with_thread!("mdb_del({txn}, {key}, {data}) -> {rc}");
    rc
}

pub unsafe fn mdb_drop(txn: *mut MDB_txn, dbi: MDB_dbi, del: ::libc::c_int) -> ::libc::c_int {
    let rc = ffi::mdb_drop(txn, dbi, del);
    let txn = TRACING_STATE.txn(txn);

    trace_with_thread!("mdb_drop({txn}, {dbi}, {del}) -> {rc}");
    rc
}

pub unsafe fn mdb_get(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
) -> ::libc::c_int {
    let rc = ffi::mdb_get(txn, dbi, key, data);
    let txn = TRACING_STATE.txn(txn);
    trace_with_thread!("mdb_get({txn}, key, out data) -> {rc}");

    let key = CArray::from_val(key);
    let data: String = if rc == 0 {
        let data = CArray::from_val(data);
        format!("{data}")
    } else {
        "???".into()
    };

    trace_with_thread!("mdb_get({txn}, {key}, out {data}) -> {rc}");
    rc
}

// FIXME: should we expose ffi::MDB_val as this function cannot be called without it? 🤔
pub unsafe fn mdb_put(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
    flags: ::libc::c_uint,
) -> ::libc::c_int {
    trace_with_thread!("mdb_put(txn, key, in data, {flags}) -> rc");

    // fetch the data before the insertion as it might get modified by the insertion.
    // We need to own the data as otherwise it might change from under our foot, invoking UB.
    let in_data = from_val(std::ptr::read(data)).to_vec();
    let in_data = CArray::from_slice(&in_data);
    let rc = ffi::mdb_put(txn, dbi, key, data, flags);
    let key = CArray::from_val(key);
    let txn = TRACING_STATE.txn(txn);

    // We could try retrieving the out data here, but it is unclear to what it will be set if the key doesn't exist in the base,
    // so it is safer to not get it.

    trace_with_thread!("mdb_put({txn}, {key}, in {in_data}, {flags}) -> {rc}");
    rc
}

pub unsafe fn mdb_stat(txn: *mut MDB_txn, dbi: MDB_dbi, stat: *mut MDB_stat) -> ::libc::c_int {
    let rc = ffi::mdb_stat(txn, dbi, stat);
    let txn = TRACING_STATE.txn(txn);

    trace_with_thread!("mdb_stat({txn}, {dbi}, out stat) -> {rc}");
    rc
}

pub unsafe fn mdb_txn_abort(txn: *mut MDB_txn) {
    ffi::mdb_txn_abort(txn);
    let txn = TRACING_STATE.txn(txn);

    trace_with_thread!("mdb_abort({txn})");
}

pub unsafe fn mdb_txn_begin(
    env: *mut MDB_env,
    parent: *mut MDB_txn,
    flags: ::libc::c_uint,
    txn: *mut *mut MDB_txn,
) -> ::libc::c_int {
    let rc = ffi::mdb_txn_begin(env, parent, flags, txn);
    let env = TRACING_STATE.env(env);
    let txn: String = if rc == 0 { TRACING_STATE.new_txn(*txn) } else { "error".into() };
    let parent: String = TRACING_STATE.txn(parent);
    trace_with_thread!("mdb_txn_begin({env}, {parent}, {flags}, out {txn}) -> {rc}");
    rc
}

pub unsafe fn mdb_txn_commit(txn: *mut MDB_txn) -> ::libc::c_int {
    let rc = ffi::mdb_txn_commit(txn);
    let txn = TRACING_STATE.txn(txn);

    trace_with_thread!("mdb_txn_commit({txn}) -> {rc}");
    rc
}

pub unsafe fn mdb_cursor_close(cursor: *mut MDB_cursor) {
    ffi::mdb_cursor_close(cursor);
    let cursor = TRACING_STATE.cursor(cursor);
    trace_with_thread!("mdb_cursor_close({cursor})");
}

pub unsafe fn mdb_cursor_del(cursor: *mut MDB_cursor, flags: ::libc::c_uint) -> ::libc::c_int {
    let rc = ffi::mdb_cursor_del(cursor, flags);
    let cursor = TRACING_STATE.cursor(cursor);
    trace_with_thread!("mdb_cursor_del({cursor}, {flags}) -> {rc}");
    rc
}

pub unsafe fn mdb_cursor_get(
    cursor: *mut MDB_cursor,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
    op: ffi::MDB_cursor_op,
) -> ::libc::c_int {
    let rc = ffi::mdb_cursor_get(cursor, key, data, op);
    let cursor = TRACING_STATE.cursor(cursor);

    if rc != 0 {
        trace_with_thread!("mdb_cursor_get({cursor}, out key error, out data error, {op}) -> {rc}");
        return rc;
    }
    let key = CArray::from_val(key);

    let data = if rc == 0 { format!("{}", CArray::from_val(data)) } else { "???".into() };

    trace_with_thread!("mdb_cursor_get({cursor}, out {key}, out {data}, {op}) -> {rc}");
    rc
}

pub unsafe fn mdb_cursor_open(
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    cursor: *mut *mut MDB_cursor,
) -> ::libc::c_int {
    let rc = ffi::mdb_cursor_open(txn, dbi, cursor);
    let txn = TRACING_STATE.txn(txn);

    let cursor: String = if rc == 0 { TRACING_STATE.new_cursor(*cursor) } else { "???".into() };
    trace_with_thread!("mdb_cursor_open({txn}, {dbi}, out {cursor}) -> {rc}");
    rc
}

pub unsafe fn mdb_cursor_put(
    cursor: *mut MDB_cursor,
    key: *mut ffi::MDB_val,
    data: *mut ffi::MDB_val,
    flags: ::libc::c_uint,
) -> ::libc::c_int {
    let rc = ffi::mdb_cursor_put(cursor, key, data, flags);
    let cursor = TRACING_STATE.cursor(cursor);
    trace_with_thread!("mdb_cursor_put({cursor}, key, data) -> {rc}");

    let key = CArray::from_val(key);
    let data = CArray::from_val(data);

    trace_with_thread!("mdb_cursor_put({cursor}, {key}, {data}) -> {rc}");
    rc
}

struct CArray<'a>(Option<&'a [u8]>);

impl<'a> CArray<'a> {
    /// Convenience constructor that accepts a pointer to a MDB_val and returns the corresponding CArray.
    ///
    /// If the pointer or its inner mv_data pointer is NULL, then the array will contain None.
    ///
    /// # SAFETY
    ///
    /// - value is dereferenceable or NULL
    /// - value's mv_data is dereferenceable or NULL
    /// - value's mv_data doesn't change while the resulting slice is active
    pub unsafe fn from_val(value: *mut ffi::MDB_val) -> Self {
        Self(if value.is_null() {
            None
        } else {
            // OK because MDB_val is a C struct without drop glue. Would be better to have that struct Copy and/or from_val accepting a reference
            let value = std::ptr::read(value);
            if value.mv_data.is_null() {
                None
            } else {
                Some(from_val(value))
            }
        })
    }

    pub fn from_slice(value: &'a [u8]) -> Self {
        Self(Some(value))
    }
}

impl Display for CArray<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(value) => {
                f.write_char('{')?;
                for (index, byte) in value.iter().enumerate() {
                    if index + 1 == value.len() {
                        write!(f, "{byte:#x}")?
                    } else {
                        write!(f, "{byte:#x}, ")?
                    }
                }
                f.write_char('}')
            }
            None => write!(f, "NULL"),
        }
    }
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
