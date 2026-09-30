use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::c_void;
use std::fs::File;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
use std::panic::catch_unwind;
use std::path::{Path, PathBuf};
use std::process::abort;
use std::sync::{Arc, LazyLock, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;
#[cfg(windows)]
use std::{
    ffi::OsStr,
    os::windows::io::{AsRawHandle as _, RawHandle},
};
use std::{fmt, io};

use heed_traits::{Comparator, LexicographicComparator};
use synchronoise::event::SignalEvent;

use crate::mdb::ffi;
#[allow(unused)] // for cargo auto doc links
use crate::{Database, DatabaseFlags};

#[cfg(master3)]
mod encrypted_env;
mod env;
mod env_open_options;

#[cfg(master3)]
pub use encrypted_env::EncryptedEnv;
pub use env::Env;
pub(crate) use env::EnvInner;
pub use env_open_options::EnvOpenOptions;

/// Records the environments currently open in this process, keyed by canonical path. Each
/// entry coordinates one path only, so opening or closing an environment never waits on work
/// happening at a different path.
static OPENED_ENV: LazyLock<RwLock<HashMap<PathBuf, Weak<PathEntry>>>> =
    LazyLock::new(RwLock::default);

/// The registry's entry for one canonical path.
///
/// The state carries the event signaled once the environment at this path has been closed, and
/// is `None` while no environment is open there. Its lock is held for the whole of
/// `mdb_env_open` and `mdb_env_close`, so a thread opening a path that is being closed waits
/// for the close to finish and then opens a fresh environment, while a thread opening a path
/// that is already open gets [`Error::EnvAlreadyOpened`](crate::Error::EnvAlreadyOpened).
pub(crate) struct PathEntry {
    state: Mutex<Option<Arc<SignalEvent>>>,
    path: PathBuf,
}

impl Drop for PathEntry {
    fn drop(&mut self) {
        // A poisoned registry is taken anyway, since dropping an entry must not panic and a stale
        // registration is harmless: reopening the path replaces it.
        let mut opened = OPENED_ENV.write().unwrap_or_else(PoisonError::into_inner);
        // An entry registered at this path while this one was being dropped keeps its place, so
        // only a registration whose entry is gone is removed.
        if opened.get(&self.path).is_some_and(|registration| registration.strong_count() == 0) {
            opened.remove(&self.path);
        }
    }
}

impl PathEntry {
    /// The entry for `path`, registering one when no environment is open there.
    ///
    /// The registry lock is released before the caller locks the entry's state, so the registry is
    /// never held across LMDB work. The registration lives as long as any handle to the entry.
    fn at(path: &Path) -> Arc<Self> {
        // Panic rather than open against a registry not trusted to hold one entry per path.
        let mut opened = OPENED_ENV.write().unwrap();
        if let Some(entry) = opened.get(path).and_then(Weak::upgrade) {
            return entry;
        }
        let entry = Arc::new(Self { state: Mutex::default(), path: path.to_path_buf() });
        opened.insert(path.to_path_buf(), Arc::downgrade(&entry));
        entry
    }
}

/// Returns a struct that allows to wait for the effective closing of an environment.
pub fn env_closing_event<P: AsRef<Path>>(path: P) -> Option<EnvClosingEvent> {
    let entry = OPENED_ENV.read().unwrap().get(path.as_ref()).and_then(Weak::upgrade)?;
    let signal_event = entry.state.lock().unwrap().clone()?;
    Some(EnvClosingEvent(signal_event))
}

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

/// Statistics for an environment.
#[derive(Debug, Clone, Copy)]
pub struct EnvStat {
    /// Size of a database page.
    /// This is currently the same for all databases.
    pub page_size: u32,
    /// Depth (height) of the B-tree.
    pub depth: u32,
    /// Number of internal (non-leaf) pages
    pub branch_pages: usize,
    /// Number of leaf pages.
    pub leaf_pages: usize,
    /// Number of overflow pages.
    pub overflow_pages: usize,
    /// Number of data items.
    pub entries: usize,
}

/// A structure that can be used to wait for the closing event.
/// Multiple threads can wait on this event.
#[derive(Clone)]
pub struct EnvClosingEvent(Arc<SignalEvent>);

impl EnvClosingEvent {
    /// Blocks this thread until the environment is effectively closed.
    ///
    /// # Safety
    ///
    /// Make sure that you don't have any copy of the environment in the thread
    /// that is waiting for a close event. If you do, you will have a deadlock.
    pub fn wait(&self) {
        self.0.wait()
    }

    /// Blocks this thread until either the environment has been closed
    /// or until the timeout elapses. Returns `true` if the environment
    /// has been effectively closed.
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.0.wait_timeout(timeout)
    }
}

impl fmt::Debug for EnvClosingEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EnvClosingEvent").finish()
    }
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
#[derive(Debug)]
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

/// A representation of LMDB's `MDB_INTEGERKEY` and `MDB_INTEGERDUP` comparator behavior.
///
/// This enum is used to indicate a table should be sorted by the keys numeric
/// value in native byte order. When a [`Database`] is created or opened with
/// [`IntegerComparator`], it signifies that the comparator should not be explicitly
/// set via [`ffi::mdb_set_compare`], instead the flag [`DatabaseFlags::INTEGER_KEY`]
/// or [`DatabaseFlags::INTEGER_DUP`] is set on the table.
///
/// This can only be used on certain types: either `u32` or `usize`.
/// The keys must all be of the same size.
#[derive(Debug)]
pub enum IntegerComparator {}

impl Comparator for IntegerComparator {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        #[cfg(target_endian = "big")]
        return a.cmp(b);

        #[cfg(target_endian = "little")]
        {
            let len = a.len();

            for i in (0..len).rev() {
                match a[i].cmp(&b[i]) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }

            Ordering::Equal
        }
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::thread::{scope, yield_now};

    use super::*;
    use crate::{EnvOpenOptions, Error};

    const MAP: usize = 16 * 1024;

    /// Whether the registry holds an entry for `path`, live or not.
    fn has_entry(path: &Path) -> bool {
        OPENED_ENV.read().unwrap().contains_key(path)
    }

    /// An open environment holds its path against a second open and gives it back on close, and
    /// a failed open gives its path back too, so a process opening many paths over its lifetime
    /// keeps no entry per path ever seen.
    #[test]
    fn an_env_holds_its_path_until_it_closes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().canonicalize().unwrap();

        // A regular file canonicalizes, so this open registers an entry before LMDB rejects it.
        let file = path.join("not-a-dir");
        fs::write(&file, b"").unwrap();
        assert!(unsafe { EnvOpenOptions::new().map_size(MAP).open(&file) }.is_err());
        assert!(!has_entry(&file), "a failed open leaves no registry entry");
        fs::remove_file(&file).unwrap();
        fs::create_dir(&file).unwrap();
        unsafe { EnvOpenOptions::new().map_size(MAP).open(&file) }
            .expect("a failed open does not reserve the path");

        let env = unsafe { EnvOpenOptions::new().map_size(MAP).open(&path).unwrap() };
        assert!(has_entry(&path), "an open env is registered");
        assert!(matches!(
            unsafe { EnvOpenOptions::new().map_size(MAP).open(&path) },
            Err(Error::EnvAlreadyOpened)
        ));

        drop(env);
        assert!(!has_entry(&path), "a closed env leaves no registry entry");
        unsafe { EnvOpenOptions::new().map_size(MAP).open(&path) }
            .expect("the path opens again once its env is closed");
    }

    /// Racing opens of one path never yield two `Env`s at once. The count drops before each
    /// close begins, so an open overlapping a close is outside what this can observe.
    #[test]
    fn racing_opens_of_one_path_never_overlap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let (live, peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let (opened, already) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let barrier = Barrier::new(16);

        scope(|s| {
            for _ in 0..16 {
                s.spawn(|| {
                    barrier.wait();
                    for _ in 0..20 {
                        match unsafe { EnvOpenOptions::new().map_size(MAP).open(path) } {
                            Ok(env) => {
                                let n = live.fetch_add(1, Ordering::SeqCst) + 1;
                                peak.fetch_max(n, Ordering::SeqCst);
                                opened.fetch_add(1, Ordering::SeqCst);
                                // Widen the window in which an overlap would be visible.
                                yield_now();
                                live.fetch_sub(1, Ordering::SeqCst);
                                drop(env);
                            }
                            Err(Error::EnvAlreadyOpened) => {
                                already.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                });
            }
        });

        assert!(opened.into_inner() > 0, "no thread ever won the race");
        assert!(already.into_inner() > 0, "the opens never contended, so nothing was tested");
        assert_eq!(peak.into_inner(), 1, "two envs were live for the same path");
    }
}
