use std::cmp::Ordering;
use std::collections::HashSet;
use std::ffi::c_void;
use std::fs::{File, Metadata};
use std::io;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, BorrowedFd, RawFd};
use std::panic::catch_unwind;
use std::path::{Path, PathBuf};
use std::process::abort;
use std::ptr::NonNull;
use std::sync::{LazyLock, RwLock};
use std::time::Duration;
#[cfg(windows)]
use std::{
    ffi::OsStr,
    os::windows::io::{AsRawHandle as _, BorrowedHandle, RawHandle},
};

use heed_traits::{Comparator, LexicographicComparator};

use crate::mdb::ffi;

#[cfg(master3)]
mod encrypted_env;
mod env;
mod env_open_options;

#[cfg(master3)]
pub use encrypted_env::EncryptedEnv;
pub use env::Env;
pub use env_open_options::EnvOpenOptions;

/// Records the current list of opened environments for tracking purposes. The canonical
/// path of an environment is removed when either an `Env` or `EncryptedEnv` is closed.
static OPENED_ENV: LazyLock<RwLock<HashSet<PathBuf>>> = LazyLock::new(RwLock::default);

/// Contains information about the environment.
#[derive(Debug, Clone, Copy)]
pub struct EnvInfo {
    /// Address of the map, if fixed.
    pub map_addr: *mut c_void,
    /// Size of the data memory map.
    pub map_size: usize,
    /// ID of the last used page.
    pub last_page_number: usize,
    /// ID of the last committed transaction.
    pub last_txn_id: usize,
    /// Maximum number of reader slots in the environment.
    pub maximum_number_of_readers: u32,
    /// Number of reader slots used in the environment.
    pub number_of_readers: u32,
}

// Thanks to the mozilla/rkv project
// Workaround the UNC path on Windows, see https://github.com/rust-lang/rust/issues/42869.
// Otherwise, `Env::from_env()` will panic with error_no(123).
#[cfg(not(windows))]
fn canonicalize_path(path: &Path) -> io::Result<PathBuf> {
    path.canonicalize()
}

#[cfg(windows)]
fn canonicalize_path(path: &Path) -> io::Result<PathBuf> {
    let canonical = path.canonicalize()?;
    let url = url::Url::from_file_path(&canonical)
        .map_err(|_e| io::Error::new(io::ErrorKind::Other, "URL passing error"))?;
    url.to_file_path()
        .map_err(|_e| io::Error::new(io::ErrorKind::Other, "path canonicalization error"))
}

#[cfg(windows)]
/// Adding a 'missing' trait from windows OsStrExt
trait OsStrExtLmdb {
    fn as_bytes(&self) -> &[u8];
}
#[cfg(windows)]
impl OsStrExtLmdb for OsStr {
    fn as_bytes(&self) -> &[u8] {
        &self.to_str().unwrap().as_bytes()
    }
}

#[cfg(unix)]
fn get_file_fd(file: &File) -> RawFd {
    file.as_raw_fd()
}

#[cfg(windows)]
fn get_file_fd(file: &File) -> RawHandle {
    file.as_raw_handle()
}

#[cfg(unix)]
/// Get metadata from a file descriptor.
unsafe fn metadata_from_fd(raw_fd: RawFd) -> io::Result<Metadata> {
    let fd = BorrowedFd::borrow_raw(raw_fd);
    let owned = fd.try_clone_to_owned()?;
    File::from(owned).metadata()
}

#[cfg(windows)]
/// Get metadata from a file descriptor.
unsafe fn metadata_from_fd(raw_fd: RawHandle) -> io::Result<Metadata> {
    let fd = BorrowedHandle::borrow_raw(raw_fd);
    let owned = fd.try_clone_to_owned()?;
    File::from(owned).metadata()
}

/// A helper function that transforms the LMDB types into Rust types (`MDB_val` into slices)
/// and vice versa, the Rust types into C types (`Ordering` into an integer).
///
/// # Safety
///
/// `a` and `b` should both properly aligned, valid for reads and should point to a valid
/// [`MDB_val`][ffi::MDB_val]. An [`MDB_val`][ffi::MDB_val] (consists of a pointer and size) is
/// valid when its pointer (`mv_data`) is valid for reads of `mv_size` bytes and is not null.
unsafe extern "C" fn custom_key_cmp_wrapper<C: Comparator>(
    a: *const ffi::MDB_val,
    b: *const ffi::MDB_val,
) -> i32 {
    let a = unsafe { ffi::from_val(*a) };
    let b = unsafe { ffi::from_val(*b) };
    match catch_unwind(|| C::compare(a, b)) {
        Ok(Ordering::Less) => -1,
        Ok(Ordering::Equal) => 0,
        Ok(Ordering::Greater) => 1,
        Err(_) => abort(),
    }
}

/// A representation of LMDB's default comparator behavior.
///
/// This enum is used to indicate the absence of a custom comparator for an LMDB
/// database instance. When a [`Database`] is created or opened with
/// [`DefaultComparator`], it signifies that the comparator should not be explicitly
/// set via [`ffi::mdb_set_compare`]. Consequently, the database
/// instance utilizes LMDB's built-in default comparator, which inherently performs
/// lexicographic comparison of keys.
///
/// This comparator's lexicographic implementation is employed in scenarios involving
/// prefix iterators. Specifically, methods other than [`Comparator::compare`] are utilized
/// to determine the lexicographic successors and predecessors of byte sequences, which
/// is essential for these iterators' operation.
///
/// When a custom comparator is provided, the wrapper is responsible for setting
/// it with the [`ffi::mdb_set_compare`] function, which overrides the default comparison
/// behavior of LMDB with the user-defined logic.
pub enum DefaultComparator {}

impl LexicographicComparator for DefaultComparator {
    #[inline]
    fn compare_elem(a: u8, b: u8) -> Ordering {
        a.cmp(&b)
    }

    #[inline]
    fn successor(elem: u8) -> Option<u8> {
        match elem {
            u8::MAX => None,
            elem => Some(elem + 1),
        }
    }

    #[inline]
    fn predecessor(elem: u8) -> Option<u8> {
        match elem {
            u8::MIN => None,
            elem => Some(elem - 1),
        }
    }

    #[inline]
    fn max_elem() -> u8 {
        u8::MAX
    }

    #[inline]
    fn min_elem() -> u8 {
        u8::MIN
    }
}

/// Whether to perform compaction while copying an environment.
#[derive(Debug, Copy, Clone)]
pub enum CompactionOption {
    /// Omit free pages and sequentially renumber all pages in output.
    ///
    /// This option consumes more CPU and runs more slowly than the default.
    /// Currently it fails if the environment has suffered a page leak.
    Enabled,

    /// Copy everything without taking any special action about free pages.
    Disabled,
}

/// Whether to enable or disable flags in [`Env::set_flags`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FlagSetMode {
    /// Enable the flags.
    Enable,
    /// Disable the flags.
    Disable,
}

impl FlagSetMode {
    /// Convert the enum into the `i32` required by LMDB.
    /// "A non-zero value sets the flags, zero clears them."
    /// <http://www.lmdb.tech/doc/group__mdb.html#ga83f66cf02bfd42119451e9468dc58445>
    fn as_mdb_env_set_flags_input(self) -> i32 {
        match self {
            Self::Enable => 1,
            Self::Disable => 0,
        }
    }
}