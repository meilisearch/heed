//! Crate `heed` is a high-level wrapper of [LMDB], high-level doesn't mean heavy (think about Rust).
//!
//! It provides you a way to store types in LMDB without any limit and with a minimal overhead as possible,
//! relying on the [zerocopy] library to avoid copying bytes when that's unnecessary and the [serde] library
//! when this is unavoidable.
//!
//! The Lightning Memory-Mapped Database (LMDB) directly maps files parts into main memory, combined
//! with the zerocopy library allows us to safely zero-copy parse and serialize Rust types into LMDB.
//!
//! [LMDB]: https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database
//! [zerocopy]: https://docs.rs/zerocopy
//! [serde]: https://docs.rs/serde
//!
//! # Examples
//!
//! Discern let you open a database, that will support some typed key/data
//! and ensures, at compile time, that you'll write those types and not others.
//!
//! ```
//! use std::fs;
//! use heed::{EnvOpenOptions, Database};
//! use heed::types::*;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! fs::create_dir_all("target/zerocopy.mdb")?;
//! let env = EnvOpenOptions::new().open("target/zerocopy.mdb")?;
//!
//! // we will open the default unamed database
//! let db: Database<Str, OwnedType<i32>> = env.create_database(None)?;
//!
//! // opening a write transaction
//! let mut wtxn = env.write_txn()?;
//! db.put(&mut wtxn, "seven", &7)?;
//! db.put(&mut wtxn, "zero", &0)?;
//! db.put(&mut wtxn, "five", &5)?;
//! db.put(&mut wtxn, "three", &3)?;
//! wtxn.commit()?;
//!
//! // opening a read transaction
//! // to check if those values are now available
//! let mut rtxn = env.read_txn()?;
//!
//! let ret = db.get(&rtxn, "zero")?;
//! assert_eq!(ret, Some(0));
//!
//! let ret = db.get(&rtxn, "five")?;
//! assert_eq!(ret, Some(5));
//! # Ok(()) }
//! ```

mod cursor;
mod db;
mod env;
pub mod flags;
mod lmdb_error;
mod traits;
mod txn;
pub mod types;

pub use byteorder;
pub use zerocopy;

use self::cursor::{RoCursor, RwCursor};
pub use self::db::{Database, PolyDatabase, RoIter, RoRange, RwIter, RwRange};
pub use self::env::{CompactionOption, Env, EnvOpenOptions};
pub use self::lmdb_error::Error as LmdbError;
pub use self::traits::{BytesDecode, BytesEncode};
pub use self::txn::{NestedRwTxn, RoTxn, RwTxn};

use std::{error, fmt, io, result};

use lmdb_sys as ffi;

/// An error that encapsulates all possible errors in this crate.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Lmdb(LmdbError),
    Encoding,
    Decoding,
    InvalidDatabaseTyping,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "{}", error),
            Error::Lmdb(error) => write!(f, "{}", error),
            Error::Encoding => write!(f, "error while encoding"),
            Error::Decoding => write!(f, "error while decoding"),
            Error::InvalidDatabaseTyping => {
                write!(f, "database was previously opened with different types")
            }
        }
    }
}

impl error::Error for Error {}

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

pub type Result<T> = result::Result<T, Error>;

unsafe fn into_val(value: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val {
        mv_size: value.len(),
        mv_data: value.as_ptr() as *mut libc::c_void,
    }
}

unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}
