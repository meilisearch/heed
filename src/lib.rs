use std::io;

mod db;
mod env;
mod traits;
mod txn;
mod types;

pub use self::db::Database;
pub use self::env::{EnvBuilder, Env};
pub use self::traits::{BytesEncode, BytesDecode};
pub use self::txn::{TxnRead, TxnWrite};
pub use self::types::{Type, Slice, Str, Ignore};

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
