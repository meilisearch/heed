// MDBX

#[cfg(feature = "mdbx")]
pub mod mdbx_error;
#[cfg(feature = "mdbx")]
pub use self::mdbx_error as error;

#[cfg(feature = "mdbx")]
pub mod mdbx_ffi;
#[cfg(feature = "mdbx")]
pub use self::mdbx_ffi as ffi;

#[cfg(feature = "mdbx")]
pub mod mdbx_flags;
#[cfg(feature = "mdbx")]
pub use self::mdbx_flags as flags;

// LMDB

#[cfg(feature = "lmdb")]
pub mod lmdb_error;
#[cfg(feature = "lmdb")]
pub use self::lmdb_error as error;

#[cfg(feature = "lmdb")]
pub mod lmdb_ffi;
#[cfg(feature = "lmdb")]
pub use self::lmdb_ffi as ffi;

#[cfg(feature = "lmdb")]
pub mod lmdb_flags;
#[cfg(feature = "lmdb")]
pub use self::lmdb_flags as flags;
