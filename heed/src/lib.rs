//! Crate `heed` is a high-level wrapper of [LMDB], high-level doesn't mean heavy (think about Rust).
//!
//! It provides you a way to store types in LMDB without any limit and with a minimal overhead as possible,
//! relying on the [bytemuck] library to avoid copying bytes when that's unnecessary and the serde library
//! when this is unavoidable.
//!
//! The Lightning Memory-Mapped Database (LMDB) directly maps files parts into main memory, combined
//! with the bytemuck library allows us to safely zero-copy parse and serialize Rust types into LMDB.
//!
//! [LMDB]: https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database
//!
//! # Examples
//!
//! Discern let you open a database, that will support some typed key/data
//! and ensures, at compile time, that you'll write those types and not others.
//!
//! ```
//! use std::fs;
//! use std::path::Path;
//! use heed::{EnvOpenOptions, Database};
//! use heed::types::*;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let env = EnvOpenOptions::new().open(dir.path())?;
//!
//! // we will open the default unamed database
//! let mut wtxn = env.write_txn()?;
//! let db: Database<Str, OwnedType<i32>> = env.create_database(&mut wtxn, None)?;
//!
//! // opening a write transaction
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
mod iter;
mod lazy_decode;
mod mdb;
mod txn;

use std::{error, fmt, io, result};

use heed_traits as traits;
pub use {bytemuck, byteorder, heed_types as types};

use self::cursor::{RoCursor, RwCursor};
pub use self::db::{Database, PolyDatabase};
pub use self::env::{env_closing_event, CompactionOption, Env, EnvClosingEvent, EnvOpenOptions};
pub use self::iter::{
    RoIter, RoPrefix, RoRange, RoRevIter, RoRevPrefix, RoRevRange, RwIter, RwPrefix, RwRange,
    RwRevIter, RwRevPrefix, RwRevRange,
};
pub use self::lazy_decode::{Lazy, LazyDecode};
pub use self::mdb::error::Error as MdbError;
use self::mdb::ffi::{from_val, into_val};
pub use self::mdb::flags;
pub use self::traits::{BoxedError, BytesDecode, BytesEncode};
pub use self::txn::{RoTxn, RwTxn};

/// An error that encapsulates all possible errors in this crate.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Mdb(MdbError),
    Encoding(BoxedError),
    Decoding(BoxedError),
    InvalidDatabaseTyping,
    DatabaseClosing,
    BadOpenOptions,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "{}", error),
            Error::Mdb(error) => write!(f, "{}", error),
            Error::Encoding(error) => write!(f, "error while encoding: {}", error),
            Error::Decoding(error) => write!(f, "error while decoding: {}", error),
            Error::InvalidDatabaseTyping => {
                f.write_str("database was previously opened with different types")
            }
            Error::DatabaseClosing => {
                f.write_str("database is in a closing phase, you can't open it at the same time")
            }
            Error::BadOpenOptions => {
                f.write_str("an environment is already opened with different options")
            }
        }
    }
}

impl error::Error for Error {}

impl From<MdbError> for Error {
    fn from(error: MdbError) -> Error {
        match error {
            MdbError::Other(e) => Error::Io(io::Error::from_raw_os_error(e)),
            _ => Error::Mdb(error),
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

pub type Result<T> = result::Result<T, Error>;
