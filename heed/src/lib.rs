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
//! use std::path::Path;
//! use heed::{EnvOpenOptions, Database};
//! use heed::types::*;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
//! let env = EnvOpenOptions::new().open(Path::new("target").join("zerocopy.mdb"))?;
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
mod mdb;
mod txn;

pub use byteorder;
pub use heed_types as types;
pub use zerocopy;
use heed_traits as traits;

pub use self::db::{Database, PolyDatabase, RoIter, RoIterDup, RoRange, RwIter, RwRange};
pub use self::env::{CompactionOption, Env, EnvOpenOptions};
pub use self::mdb::error::Error as MdbError;
pub use self::mdb::flags;
pub use self::traits::{BytesDecode, BytesEncode};
pub use self::txn::{RoTxn, RwTxn};
use self::cursor::{RoCursor, RwCursor};
use self::mdb::ffi::{into_val, from_val};

use std::{error, fmt, io, result};

/// An error that encapsulates all possible errors in this crate.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Mdb(MdbError),
    Encoding,
    Decoding,
    InvalidDatabaseTyping,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "{}", error),
            Error::Mdb(error) => write!(f, "{}", error),
            Error::Encoding => write!(f, "error while encoding"),
            Error::Decoding => write!(f, "error while decoding"),
            Error::InvalidDatabaseTyping => {
                write!(f, "database was previously opened with different types")
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
