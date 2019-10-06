use std::{io, marker, mem, ptr, slice, str};
use std::borrow::Cow;
use zerocopy::{LayoutVerified, AsBytes, FromBytes};
use lmdb_sys as ffi;

mod db;
mod env;
mod traits;
mod txn;
mod types;

pub use self::db::Database;
pub use self::env::{EnvBuilder, Env};
pub use self::traits::{BytesEncode, BytesDecode};
pub use self::txn::{TxnRead, TxnWrite};
pub use self::types::{Type, Slice, Str, Ignore, Serde};

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
