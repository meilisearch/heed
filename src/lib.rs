use std::io;

mod cursor;
mod db;
mod env;
mod traits;
mod txn;
mod types;

pub use self::cursor::{RoCursor, RwCursor};
pub use self::db::Database;
pub use self::env::{EnvBuilder, Env};
pub use self::traits::{BytesEncode, BytesDecode};
pub use self::txn::{RoTxn, RwTxn};
pub use self::types::{Type, Slice, Str, Ignore};

use lmdb_sys as ffi;

#[cfg(feature = "serde")]
pub use self::types::Serde;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Encoding,
    Decoding,
    VersionMismatch,
    CouldNotCreateEnv,
    InvalidFile,
}

pub type ZResult<T> = Result<T, Error>;

unsafe fn into_val(value: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val { mv_size: value.len(), mv_data: value.as_ptr() as *mut libc::c_void }
}

unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}
