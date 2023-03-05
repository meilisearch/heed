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
//! Open a database, that will support some typed key/data and ensures, at compile time,
//! that you'll write those types and not others.
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
mod reserved_space;
mod txn;

use std::convert::Infallible;
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
pub use self::mdb::flags::Flags;
pub use self::reserved_space::ReservedSpace;
pub use self::traits::{BytesDecode, BytesEncode};
pub use self::txn::{RoTxn, RwTxn};

/// An error that encapsulates all possible errors in this crate.
#[derive(Debug)]
pub enum Error<KE, KD, DE, DD> {
    Io(io::Error),
    Mdb(MdbError),
    KeyEncoding(KE),
    KeyDecoding(KD),
    DataEncoding(DE),
    DataDecoding(DD),
    InvalidDatabaseTyping,
    DatabaseClosing,
    BadOpenOptions {
        /// The options that were used to originaly open this env.
        options: EnvOpenOptions,
        /// The env opened with the original options.
        env: Env,
    },
}

impl<KE: fmt::Display, KD: fmt::Display, DE: fmt::Display, DD: fmt::Display> fmt::Display
    for Error<KE, KD, DE, DD>
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "{}", error),
            Error::Mdb(error) => write!(f, "{}", error),
            Error::KeyEncoding(error) => write!(f, "error while encoding a key: {}", error),
            Error::KeyDecoding(error) => write!(f, "error while decoding a key: {}", error),
            Error::DataEncoding(error) => write!(f, "error while encoding a data: {}", error),
            Error::DataDecoding(error) => write!(f, "error while decoding a data: {}", error),
            Error::InvalidDatabaseTyping => {
                f.write_str("database was previously opened with different types")
            }
            Error::DatabaseClosing => {
                f.write_str("database is in a closing phase, you can't open it at the same time")
            }
            Error::BadOpenOptions { .. } => {
                f.write_str("an environment is already opened with different options")
            }
        }
    }
}

impl<KE: error::Error, KD: error::Error, DE: error::Error, DD: error::Error> error::Error
    for Error<KE, KD, DE, DD>
{
}

impl<KE, KD, DE, DD> From<MdbError> for Error<KE, KD, DE, DD> {
    fn from(error: MdbError) -> Error<KE, KD, DE, DD> {
        match error {
            MdbError::Other(e) => Error::Io(io::Error::from_raw_os_error(e)),
            _ => Error::Mdb(error),
        }
    }
}

impl<KE, KD, DE, DD> From<io::Error> for Error<KE, KD, DE, DD> {
    fn from(error: io::Error) -> Error<KE, KD, DE, DD> {
        Error::Io(error)
    }
}

/// Either a success or an [`Error`].
pub type Result<T, KE = Infallible, KD = Infallible, DE = Infallible, DD = Infallible> =
    result::Result<T, Error<KE, KD, DE, DD>>;

macro_rules! assert_eq_env_db_txn {
    ($database:ident, $txn:ident) => {
        assert!(
            $database.env_ident == $txn.env_mut_ptr() as usize,
            "The database environment doesn't match the transaction's environment"
        );
    };
}

macro_rules! assert_eq_env_txn {
    ($env:ident, $txn:ident) => {
        assert!(
            $env.env_mut_ptr() == $txn.env_mut_ptr(),
            "The environment doesn't match the transaction's environment"
        );
    };
}

pub(crate) use {assert_eq_env_db_txn, assert_eq_env_txn};
