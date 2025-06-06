//! Pure Rust implementation of LMDB with modern performance optimizations
//!
//! This crate provides a high-performance, type-safe embedded database
//! that is compatible with LMDB while leveraging modern Rust features
//! and performance technologies like io_uring and SIMD.

#![warn(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
// #![cfg_attr(feature = "simd", feature(portable_simd))]

pub mod error;
pub mod page;
pub mod node;
pub mod meta;
pub mod env;
pub mod txn;
pub mod db;
pub mod btree;
pub mod cursor;
pub mod overflow;
pub mod freelist;
pub mod reader;
pub mod dupsort;
pub mod checksum;
pub mod io;
pub mod catalog;
pub mod copy;
pub mod branch;
pub mod branch_v2;
pub mod tree_utils;

#[cfg(test)]
mod io_test;
#[cfg(test)]
mod btree_tests;

// Re-exports
pub use error::{Error, Result};
pub use env::{Environment, EnvBuilder};
pub use txn::{Transaction, ReadTransaction, WriteTransaction};
pub use db::{Database, DatabaseFlags, Key, Value};

// Type aliases for common use cases
/// A read-only transaction
pub type RoTxn<'env> = Transaction<'env, txn::Read>;
/// A read-write transaction
pub type RwTxn<'env> = Transaction<'env, txn::Write>;

/// The default page size (4KB)
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Maximum key size (when not using longer-keys feature)
pub const DEFAULT_MAX_KEY_SIZE: usize = 511;

/// Library version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");