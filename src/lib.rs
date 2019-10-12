use std::io;

mod cursor;
mod db;
mod lmdb_error;
mod env;
mod traits;
mod txn;
pub mod types;

use self::cursor::{RoCursor, RwCursor};
pub use self::db::Database;
pub use self::lmdb_error::Error as LmdbError;
pub use self::env::{EnvBuilder, Env};
pub use self::traits::{BytesEncode, BytesDecode};
pub use self::txn::{RoTxn, RwTxn};

use std::fmt;
use std::error::Error as StdError;
use std::result::Result as StdResult;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Lmdb(LmdbError),
    Encoding,
    Decoding,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "{}", error),
            Error::Lmdb(error) => write!(f, "{}", error),
            Error::Encoding => write!(f, "error while encoding"),
            Error::Decoding => write!(f, "error while decoding"),
        }
    }
}

impl StdError for Error { }

impl From<LmdbError> for Error {
    fn from(error: LmdbError) -> Error {
        match error {
            LmdbError::Other(e) => Error::Io(io::Error::from_raw_os_error(e)),
            _ => Error::Lmdb(error),
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

pub type Result<T> = StdResult<T, Error>;

unsafe fn into_val(value: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val { mv_size: value.len(), mv_data: value.as_ptr() as *mut libc::c_void }
}

unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}
