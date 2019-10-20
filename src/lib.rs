//! Crate `discern` is a high-level wrapper of [LMDB], high-level doesn't mean heavy (think about Rust).
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
//! use discern::{EnvOpenOptions, Database};
//! use discern::types::*;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! fs::create_dir_all("target/zerocopy.mdb")?;
//!
//! let env = EnvOpenOptions::new()
//!     .map_size(10 * 1024 * 1024 * 1024) // 10GB
//!     .max_dbs(3000)
//!     .open("target/zerocopy.mdb")?;
//!
//! // here we specify that the key is an i32 array and the data an str
//! let db: Database<OwnedType<[i32; 2]>, Str> = env.create_database(Some("str"))?;
//!
//! let mut wtxn = env.write_txn()?;
//! db.put(&mut wtxn, &[2, 3], "what's up?")?;
//!
//! let ret = db.get(&wtxn, &[2, 3])?;
//! assert_eq!(ret, Some("what's up?"));
//!
//! wtxn.commit()?;
//!
//! // Be careful, you cannot open a database while in a transaction!
//! // So don't forget to commit/abort it before.
//! let db: Database<Str, ByteSlice> = env.create_database(Some("bytes"))?;
//!
//! let mut wtxn = env.write_txn()?;
//! db.put(&mut wtxn, "hello", &[2, 3][..])?;
//!
//! let ret = db.get(&wtxn, "hello")?;
//! assert_eq!(ret, Some(&[2, 3][..]));
//!
//! wtxn.commit()?;
//! # Ok(()) }
//! ```

mod cursor;
mod db;
mod lmdb_error;
mod env;
mod traits;
mod txn;
pub mod types;

pub use byteorder;
pub use zerocopy;

use self::cursor::{RoCursor, RwCursor};
pub use self::db::{Database, PolyDatabase, RoIter, RwIter, RoRange, RwRange};
pub use self::lmdb_error::Error as LmdbError;
pub use self::env::{EnvOpenOptions, Env, CompactionOption};
pub use self::traits::{BytesEncode, BytesDecode};
pub use self::txn::{RoTxn, RwTxn};

use std::{fmt, io, error, result};

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
            },
        }
    }
}

impl error::Error for Error { }

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
    ffi::MDB_val { mv_size: value.len(), mv_data: value.as_ptr() as *mut libc::c_void }
}

unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    std::slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}
