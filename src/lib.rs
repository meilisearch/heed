//! Crate `zerocopy-lmdb` is a high-level wrapper of [LMDB], high-level doesn't mean heavy (think about Rust).
//!
//! It provides you a way to store types in [LMDB] without any limit and with a minimal overhead as possible,
//! relying on the [zerocopy] library to avoid copying bytes when that's unnecessary and the [serde] library
//! when this is unavoidable.
//!
//! [LMDB]: https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database
//! [zerocopy]: https://docs.rs/zerocopy
//! [serde]: https://docs.rs/serde
//!
//! # Example: opening a database and writing into it
//!
//! ```
//! use std::fs;
//! use zerocopy_lmdb::{EnvOpenOptions, Database};
//! use zerocopy_lmdb::types::*;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! fs::create_dir_all("target/zerocopy.mdb")?;
//!
//! let env = EnvOpenOptions::new()
//!     .map_size(10 * 1024 * 1024 * 1024) // 10GB
//!     .max_dbs(3000)
//!     .open("target/zerocopy.mdb")?;
//!
//! // you can specify that a database will support some typed key/data
//! //
//! // like here we specify that the key will be an array of two i32
//! // and the data will be an str
//! let db: Database<OwnedType<[i32; 2]>, Str> = env.create_database(Some("str"))?;
//!
//! let mut wtxn = env.write_txn()?;
//! let _ret = db.put(&mut wtxn, &[2, 3], "what's up?")?;
//!
//! let ret  = db.get(&wtxn, &[2, 3])?;
//!
//! assert_eq!(ret, Some("what's up?"));
//! wtxn.commit()?;
//!
//! // Be careful, you cannot open a database while in a transaction!
//! // here the key will be an str and the data will be a slice of u8
//! let db: Database<Str, ByteSlice> = env.create_database(Some("bytes"))?;
//!
//! let mut wtxn = env.write_txn()?;
//! let _ret = db.put(&mut wtxn, "hello", &[2, 3][..])?;
//!
//! let ret  = db.get(&wtxn, "hello")?;
//!
//! assert_eq!(ret, Some(&[2, 3][..]));
//!
//! wtxn.commit()?;
//! # Ok(()) }
//! ```
//!
//! # Example: writing serde types without much overhead
//!
//! ```
//! # use std::fs;
//! # use zerocopy_lmdb::EnvOpenOptions;
//! use zerocopy_lmdb::Database;
//! use zerocopy_lmdb::types::*;
//! use serde::{Serialize, Deserialize};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # fs::create_dir_all("target/zerocopy.mdb")?;
//! # let env = EnvOpenOptions::new()
//! #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
//! #     .max_dbs(3000)
//! #     .open("target/zerocopy.mdb")?;
//! // serde types are also supported!!!
//! #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
//! struct Hello<'a> {
//!     string: &'a str,
//! }
//!
//! let db: Database<Str, Serde<Hello>> = env.create_database(Some("serde"))?;
//!
//! let mut wtxn = env.write_txn()?;
//!
//! let hello = Hello { string: "hi" };
//! let _ret  = db.put(&mut wtxn, "hello", &hello)?;
//!
//! let ret = db.get(&wtxn, "hello")?;
//!
//! assert_eq!(ret, Some(hello));
//! wtxn.commit()?;
//!
//! # Ok(()) }
//! ```
//!
//! # Example: opening a database with the wrong type
//!
//! ```
//! # use std::fs;
//! # use zerocopy_lmdb::EnvOpenOptions;
//! use zerocopy_lmdb::Database;
//! use zerocopy_lmdb::types::*;
//! use serde::{Serialize, Deserialize};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # fs::create_dir_all("target/zerocopy.mdb")?;
//! # let env = EnvOpenOptions::new()
//! #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
//! #     .max_dbs(3000)
//! #     .open("target/zerocopy.mdb")?;
//! // database opening and types are checked
//! // here we try to open a database twice with the same types
//! let _db: Database<Str, Unit> = env.create_database(Some("str-unit"))?;
//!
//! // and here we try to open it with other types
//! // asserting that it correctly returns an error
//! //
//! // NOTE that those types are not saved upon runs and
//! // therefore types cannot be checked upon different runs,
//! // the first database opening fix the types for this run.
//! let result = env.create_database::<Str, OwnedSlice<i32>>(Some("str-unit"));
//! assert!(result.is_err());
//! # Ok(()) }
//! ```

use std::io;

mod cursor;
mod db;
mod lmdb_error;
mod env;
mod traits;
mod txn;
pub mod types;

use self::cursor::{RoCursor, RwCursor};
pub use self::db::{Database, DynDatabase, RoIter, RwIter, RoRange, RwRange};
pub use self::lmdb_error::Error as LmdbError;
pub use self::env::{EnvOpenOptions, Env};
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
