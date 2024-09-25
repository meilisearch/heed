use std::any::TypeId;
use std::cmp::Ordering;
use std::collections::hash_map::{Entry, HashMap};
use std::ffi::{c_void, CString};
use std::fs::{File, Metadata};
use std::io::ErrorKind::NotFound;
#[cfg(unix)]
use std::os::unix::{
    ffi::OsStrExt,
    io::{AsRawFd, BorrowedFd, RawFd},
};
use std::panic::catch_unwind;
use std::path::{Path, PathBuf};
use std::process::abort;
use std::ptr::NonNull;
use std::sync::{Arc, RwLock};
use std::time::Duration;
#[cfg(windows)]
use std::{
    ffi::OsStr,
    os::windows::io::{AsRawHandle, BorrowedHandle, RawHandle},
};
use std::{fmt, io, mem, ptr};

use heed_traits::{Comparator, LexicographicComparator};
use once_cell::sync::Lazy;
use synchronoise::event::SignalEvent;

use crate::cursor::MoveOperation;
use crate::database::DatabaseOpenOptions;
use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::mdb::lmdb_flags::AllDatabaseFlags;
use crate::{Database, EnvFlags, Error, Result, RoCursor, RoTxn, RwTxn, Unspecified};

/// The list of opened environments, the value is an optional environment, it is None
/// when someone asks to close the environment, closing is a two-phase step, to make sure
/// noone tries to open the same environment between these two phases.
///
/// Trying to open a None marked environment returns an error to the user trying to open it.
static OPENED_ENV: Lazy<RwLock<HashMap<PathBuf, EnvEntry>>> = Lazy::new(RwLock::default);

struct EnvEntry {
    env: Option<Env>,
    signal_event: Arc<SignalEvent>,
    options: EnvOpenOptions,
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

/// Options and flags which can be used to configure how an environment is opened.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EnvOpenOptions {
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: Option<u32>,
    flags: EnvFlags,
}

impl Default for EnvOpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvOpenOptions {
    /// Creates a blank new set of options ready for configuration.
    pub fn new() -> EnvOpenOptions {
        EnvOpenOptions {
            map_size: None,
            max_readers: None,
            max_dbs: None,
            flags: EnvFlags::empty(),
        }
    }

    /// Set the size of the memory map to use for this environment.
    pub fn map_size(&mut self, size: usize) -> &mut Self {
        self.map_size = Some(size);
        self
    }

    /// Set the maximum number of threads/reader slots for the environment.
    pub fn max_readers(&mut self, readers: u32) -> &mut Self {
        self.max_readers = Some(readers);
        self
    }

    /// Set the maximum number of named databases for the environment.
    pub fn max_dbs(&mut self, dbs: u32) -> &mut Self {
        self.max_dbs = Some(dbs);
        self
    }

    /// Set one or more [LMDB flags](http://www.lmdb.tech/doc/group__mdb__env.html).
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use heed::{EnvOpenOptions, Database, EnvFlags};
    /// use heed::types::*;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// let mut env_builder = EnvOpenOptions::new();
    /// unsafe { env_builder.flags(EnvFlags::NO_TLS | EnvFlags::NO_META_SYNC); }
    /// let dir = tempfile::tempdir().unwrap();
    /// let env = unsafe { env_builder.open(dir.path())? };
    ///
    /// // we will open the default unnamed database
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, U32<byteorder::NativeEndian>> = env.create_database(&mut wtxn, None)?;
    ///
    /// // opening a write transaction
    /// db.put(&mut wtxn, "seven", &7)?;
    /// db.put(&mut wtxn, "zero", &0)?;
    /// db.put(&mut wtxn, "five", &5)?;
    /// db.put(&mut wtxn, "three", &3)?;
    /// wtxn.commit()?;
    ///
    /// // force the OS to flush the buffers (see Flag::NoSync and Flag::NoMetaSync).
    /// env.force_sync();
    ///
    /// // opening a read transaction
    /// // to check if those values are now available
    /// let mut rtxn = env.read_txn()?;
    ///
    /// let ret = db.get(&rtxn, "zero")?;
    /// assert_eq!(ret, Some(0));
    ///
    /// let ret = db.get(&rtxn, "five")?;
    /// assert_eq!(ret, Some(5));
    /// # Ok(()) }
    /// ```
    ///
    /// # Safety
    ///
    /// It is unsafe to use unsafe LMDB flags such as `NO_SYNC`, `NO_META_SYNC`, or `NO_LOCK`.
    pub unsafe fn flags(&mut self, flags: EnvFlags) -> &mut Self {
        self.flags |= flags;
        self
    }

    /// Open an environment that will be located at the specified path.
    ///
    /// # Safety
    /// LMDB is backed by a memory map [^1] which comes with some safety precautions.
    ///
    /// Memory map constructors are marked `unsafe` because of the potential
    /// for Undefined Behavior (UB) using the map if the underlying file is
    /// subsequently modified, in or out of process.
    ///
    /// LMDB itself has a locking system that solves this problem,
    /// but it will not save you from making mistakes yourself.
    ///
    /// These are some things to take note of:
    ///
    /// - Avoid long-lived transactions, they will cause the database to grow quickly [^2]
    /// - Avoid aborting your process with an active transaction [^3]
    /// - Do not use LMDB on remote filesystems, even between processes on the same host [^4]
    /// - You must manage concurrent accesses yourself if using [`EnvFlags::NO_LOCK`] [^5]
    /// - Anything that causes LMDB's lock file to be broken will cause synchronization issues and may introduce UB [^6]
    ///
    /// `heed` itself upholds some safety invariants, including but not limited to:
    /// - Calling [`EnvOpenOptions::open`] twice in the same process, at the same time is OK [^7]
    ///
    /// For more details, it is highly recommended to read LMDB's official documentation. [^8]
    ///
    /// [^1]: <https://en.wikipedia.org/wiki/Memory_map>
    ///
    /// [^2]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L107-L114>
    ///
    /// [^3]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L118-L121>
    ///
    /// [^4]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    ///
    /// [^5]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    ///
    /// [^6]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L49-L52>
    ///
    /// [^7]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L102-L105>
    ///
    /// [^8]: <http://www.lmdb.tech/doc/index.html>
    pub unsafe fn open<P: AsRef<Path>>(&self, path: P) -> Result<Env> {
        let mut lock = OPENED_ENV.write().unwrap();

        let path = match canonicalize_path(path.as_ref()) {
            Err(err) => {
                if err.kind() == NotFound && self.flags.contains(EnvFlags::NO_SUB_DIR) {
                    let path = path.as_ref();
                    match path.parent().zip(path.file_name()) {
                        Some((dir, file_name)) => canonicalize_path(dir)?.join(file_name),
                        None => return Err(err.into()),
                    }
                } else {
                    return Err(err.into());
                }
            }
            Ok(path) => path,
        };

        match lock.entry(path) {
            Entry::Occupied(entry) => {
                let env = entry.get().env.clone().ok_or(Error::DatabaseClosing)?;
                let options = entry.get().options.clone();
                if &options == self {
                    Ok(env)
                } else {
                    Err(Error::BadOpenOptions { env, options })
                }
            }
            Entry::Vacant(entry) => {
                let path = entry.key();
                let path_str = CString::new(path.as_os_str().as_bytes()).unwrap();

                unsafe {
                    let mut env: *mut ffi::MDB_env = ptr::null_mut();
                    mdb_result(ffi::mdb_env_create(&mut env))?;

                    if let Some(size) = self.map_size {
                        if size % page_size::get() != 0 {
                            let msg = format!(
                                "map size ({}) must be a multiple of the system page size ({})",
                                size,
                                page_size::get()
                            );
                            return Err(Error::Io(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                msg,
                            )));
                        }
                        mdb_result(ffi::mdb_env_set_mapsize(env, size))?;
                    }

                    if let Some(readers) = self.max_readers {
                        mdb_result(ffi::mdb_env_set_maxreaders(env, readers))?;
                    }

                    if let Some(dbs) = self.max_dbs {
                        mdb_result(ffi::mdb_env_set_maxdbs(env, dbs))?;
                    }

                    // When the `read-txn-no-tls` feature is enabled, we must force LMDB
                    // to avoid using the thread local storage, this way we allow users
                    // to use references of RoTxn between threads safely.
                    let flags = if cfg!(feature = "read-txn-no-tls") {
                        self.flags | EnvFlags::NO_TLS
                    } else {
                        self.flags
                    };

                    let result =
                        mdb_result(ffi::mdb_env_open(env, path_str.as_ptr(), flags.bits(), 0o600));

                    match result {
                        Ok(()) => {
                            let signal_event = Arc::new(SignalEvent::manual(false));
                            let inner = EnvInner { env, path: path.clone() };
                            let env = Env(Arc::new(inner));
                            let cache_entry = EnvEntry {
                                env: Some(env.clone()),
                                options: self.clone(),
                                signal_event,
                            };
                            entry.insert(cache_entry);
                            Ok(env)
                        }
                        Err(e) => {
                            ffi::mdb_env_close(env);
                            Err(e.into())
                        }
                    }
                }
            }
        }
    }
}

/// Returns a struct that allows to wait for the effective closing of an environment.
pub fn env_closing_event<P: AsRef<Path>>(path: P) -> Option<EnvClosingEvent> {
    let lock = OPENED_ENV.read().unwrap();
    lock.get(path.as_ref()).map(|e| EnvClosingEvent(e.signal_event.clone()))
}

/// An environment handle constructed by using [`EnvOpenOptions`].
#[derive(Clone)]
pub struct Env(Arc<EnvInner>);

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let EnvInner { env: _, path } = self.0.as_ref();
        f.debug_struct("Env").field("path", &path.display()).finish_non_exhaustive()
    }
}

struct EnvInner {
    env: *mut ffi::MDB_env,
    path: PathBuf,
}

unsafe impl Send for EnvInner {}

unsafe impl Sync for EnvInner {}

impl Drop for EnvInner {
    fn drop(&mut self) {
        let mut lock = OPENED_ENV.write().unwrap();

        match lock.remove(&self.path) {
            None => panic!("It seems another env closed this env before"),
            Some(EnvEntry { signal_event, .. }) => {
                unsafe {
                    ffi::mdb_env_close(self.env);
                }
                // We signal to all the waiters that the env is closed now.
                signal_event.signal();
            }
        }
    }
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

impl Env {
    pub(crate) fn env_mut_ptr(&self) -> *mut ffi::MDB_env {
        self.0.env
    }

    /// The size of the data file on disk.
    ///
    /// # Example
    ///
    /// ```
    /// use heed::EnvOpenOptions;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let dir = tempfile::tempdir()?;
    /// let size_in_bytes = 1024 * 1024;
    /// let env = unsafe { EnvOpenOptions::new().map_size(size_in_bytes).open(dir.path())? };
    ///
    /// let actual_size = env.real_disk_size()? as usize;
    /// assert!(actual_size < size_in_bytes);
    /// # Ok(()) }
    /// ```
    pub fn real_disk_size(&self) -> Result<u64> {
        let mut fd = mem::MaybeUninit::uninit();
        unsafe { mdb_result(ffi::mdb_env_get_fd(self.env_mut_ptr(), fd.as_mut_ptr()))? };
        let fd = unsafe { fd.assume_init() };
        let metadata = unsafe { metadata_from_fd(fd)? };
        Ok(metadata.len())
    }

    /// Return the raw flags the environment was opened with.
    ///
    /// Returns `None` if the environment flags are different from the [`EnvFlags`] set.
    pub fn flags(&self) -> Result<Option<EnvFlags>> {
        self.get_flags().map(EnvFlags::from_bits)
    }

    /// Enable or disable the environment's currently active [`EnvFlags`].
    ///
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use heed::{EnvOpenOptions, Database, EnvFlags, FlagSetMode};
    /// use heed::types::*;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// let mut env_builder = EnvOpenOptions::new();
    /// let dir = tempfile::tempdir().unwrap();
    /// let env = unsafe { env_builder.open(dir.path())? };
    ///
    /// // Env was opened without flags.
    /// assert_eq!(env.get_flags().unwrap(), EnvFlags::empty().bits());
    ///
    /// // Enable a flag after opening.
    /// unsafe { env.set_flags(EnvFlags::NO_SYNC, FlagSetMode::Enable).unwrap(); }
    /// assert_eq!(env.get_flags().unwrap(), EnvFlags::NO_SYNC.bits());
    ///
    /// // Disable a flag after opening.
    /// unsafe { env.set_flags(EnvFlags::NO_SYNC, FlagSetMode::Disable).unwrap(); }
    /// assert_eq!(env.get_flags().unwrap(), EnvFlags::empty().bits());
    /// # Ok(()) }
    /// ```
    ///
    /// # Safety
    ///
    /// It is unsafe to use unsafe LMDB flags such as `NO_SYNC`, `NO_META_SYNC`, or `NO_LOCK`.
    ///
    /// LMDB also requires that only 1 thread calls this function at any given moment.
    /// Neither `heed` or LMDB check for this condition, so the caller must ensure it explicitly.
    pub unsafe fn set_flags(&self, flags: EnvFlags, mode: FlagSetMode) -> Result<()> {
        // safety: caller must ensure no other thread is calling this function.
        // <http://www.lmdb.tech/doc/group__mdb.html#ga83f66cf02bfd42119451e9468dc58445>
        mdb_result(unsafe {
            ffi::mdb_env_set_flags(
                self.env_mut_ptr(),
                flags.bits(),
                mode.as_mdb_env_set_flags_input(),
            )
        })
        .map_err(Into::into)
    }

    /// Return the raw flags the environment is currently set with.
    pub fn get_flags(&self) -> Result<u32> {
        let mut flags = mem::MaybeUninit::uninit();
        unsafe { mdb_result(ffi::mdb_env_get_flags(self.env_mut_ptr(), flags.as_mut_ptr()))? };
        let flags = unsafe { flags.assume_init() };
        Ok(flags)
    }

    /// Returns some basic informations about this environment.
    pub fn info(&self) -> EnvInfo {
        let mut raw_info = mem::MaybeUninit::uninit();
        unsafe { ffi::mdb_env_info(self.0.env, raw_info.as_mut_ptr()) };
        let raw_info = unsafe { raw_info.assume_init() };

        EnvInfo {
            map_addr: raw_info.me_mapaddr,
            map_size: raw_info.me_mapsize,
            last_page_number: raw_info.me_last_pgno,
            last_txn_id: raw_info.me_last_txnid,
            maximum_number_of_readers: raw_info.me_maxreaders,
            number_of_readers: raw_info.me_numreaders,
        }
    }

    /// Returns the size used by all the databases in the environment without the free pages.
    ///
    /// It is crucial to configure [`EnvOpenOptions::max_dbs`] with a sufficiently large value
    /// before invoking this function. All databases within the environment will be opened
    /// and remain so.
    pub fn non_free_pages_size(&self) -> Result<u64> {
        let compute_size = |stat: ffi::MDB_stat| {
            (stat.ms_leaf_pages + stat.ms_branch_pages + stat.ms_overflow_pages) as u64
                * stat.ms_psize as u64
        };

        let mut size = 0;

        let mut stat = mem::MaybeUninit::uninit();
        unsafe { mdb_result(ffi::mdb_env_stat(self.env_mut_ptr(), stat.as_mut_ptr()))? };
        let stat = unsafe { stat.assume_init() };
        size += compute_size(stat);

        let rtxn = self.read_txn()?;
        // Open the main database
        let dbi =
            self.raw_open_dbi::<DefaultComparator, DefaultComparator>(rtxn.txn.unwrap(), None, 0)?;

        // We're going to iterate on the unnamed database
        let mut cursor = RoCursor::new(&rtxn, dbi)?;

        while let Some((key, _value)) = cursor.move_on_next(MoveOperation::NoDup)? {
            if key.contains(&0) {
                continue;
            }

            let key = String::from_utf8(key.to_vec()).unwrap();
            // Calling `ffi::db_stat` on a database instance does not involve key comparison
            // in LMDB, so it's safe to specify a noop key compare function for it.
            if let Ok(dbi) = self.raw_open_dbi::<DefaultComparator, DefaultComparator>(
                rtxn.txn.unwrap(),
                Some(&key),
                0,
            ) {
                let mut stat = mem::MaybeUninit::uninit();
                let mut txn = rtxn.txn.unwrap();
                unsafe { mdb_result(ffi::mdb_stat(txn.as_mut(), dbi, stat.as_mut_ptr()))? };
                let stat = unsafe { stat.assume_init() };
                size += compute_size(stat);
            }
        }

        Ok(size)
    }

    /// Options and flags which can be used to configure how a [`Database`] is opened.
    pub fn database_options(&self) -> DatabaseOpenOptions<Unspecified, Unspecified> {
        DatabaseOpenOptions::new(self)
    }

    /// Opens a typed database that already exists in this environment.
    ///
    /// If the database was previously opened in this program run, types will be checked.
    ///
    /// ## Important Information
    ///
    /// LMDB has an important restriction on the unnamed database when named ones are opened.
    /// The names of the named databases are stored as keys in the unnamed one and are immutable,
    /// and these keys can only be read and not written.
    ///
    /// ## LMDB read-only access of existing database
    ///
    /// In the case of accessing a database in a read-only manner from another process
    /// where you wrote, you might need to manually call [`RoTxn::commit`] to get metadata
    /// and the database handles opened and shared with the global [`Env`] handle.
    ///
    /// If not done, you might raise `Io(Os { code: 22, kind: InvalidInput, message: "Invalid argument" })`
    /// known as `EINVAL`.
    pub fn open_database<KC, DC>(
        &self,
        rtxn: &RoTxn,
        name: Option<&str>,
    ) -> Result<Option<Database<KC, DC>>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut options = self.database_options().types::<KC, DC>();
        if let Some(name) = name {
            options.name(name);
        }
        options.open(rtxn)
    }

    /// Creates a typed database that can already exist in this environment.
    ///
    /// If the database was previously opened during this program run, types will be checked.
    ///
    /// ## Important Information
    ///
    /// LMDB has an important restriction on the unnamed database when named ones are opened.
    /// The names of the named databases are stored as keys in the unnamed one and are immutable,
    /// and these keys can only be read and not written.
    pub fn create_database<KC, DC>(
        &self,
        wtxn: &mut RwTxn,
        name: Option<&str>,
    ) -> Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut options = self.database_options().types::<KC, DC>();
        if let Some(name) = name {
            options.name(name);
        }
        options.create(wtxn)
    }

    pub(crate) fn raw_init_database<C: Comparator + 'static, CDUP: Comparator + 'static>(
        &self,
        raw_txn: NonNull<ffi::MDB_txn>,
        name: Option<&str>,
        flags: AllDatabaseFlags,
    ) -> Result<u32> {
        match self.raw_open_dbi::<C, CDUP>(raw_txn, name, flags.bits()) {
            Ok(dbi) => Ok(dbi),
            Err(e) => Err(e.into()),
        }
    }

    fn raw_open_dbi<C: Comparator + 'static, CDUP: Comparator + 'static>(
        &self,
        mut raw_txn: NonNull<ffi::MDB_txn>,
        name: Option<&str>,
        flags: u32,
    ) -> std::result::Result<u32, crate::mdb::lmdb_error::Error> {
        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        // safety: The name cstring is cloned by LMDB, we can drop it after.
        //         If a read-only is used with the MDB_CREATE flag, LMDB will throw an error.
        unsafe {
            mdb_result(ffi::mdb_dbi_open(raw_txn.as_mut(), name_ptr, flags, &mut dbi))?;
            if TypeId::of::<C>() != TypeId::of::<DefaultComparator>() {
                mdb_result(ffi::mdb_set_compare(
                    raw_txn.as_mut(),
                    dbi,
                    Some(custom_key_cmp_wrapper::<C>),
                ))?;
            }

            if TypeId::of::<CDUP>() != TypeId::of::<DefaultComparator>() {
                mdb_result(ffi::mdb_set_dupsort(
                    raw_txn,
                    dbi,
                    Some(custom_key_cmp_wrapper::<CDUP>),
                ))?;
            }
        };

        Ok(dbi)
    }

    /// Create a transaction with read and write access for use with the environment.
    ///
    /// ## LMDB Limitations
    ///
    /// Only one [`RwTxn`] may exist simultaneously in the current environment.
    /// If another write transaction is initiated, while another write transaction exists
    /// the thread initiating the new one will wait on a mutex upon completion of the previous
    /// transaction.
    pub fn write_txn(&self) -> Result<RwTxn> {
        RwTxn::new(self)
    }

    /// Create a nested transaction with read and write access for use with the environment.
    ///
    /// The new transaction will be a nested transaction, with the transaction indicated by parent
    /// as its parent. Transactions may be nested to any level.
    ///
    /// A parent transaction and its cursors may not issue any other operations than _commit_ and
    /// _abort_ while it has active child transactions.
    pub fn nested_write_txn<'p>(&'p self, parent: &'p mut RwTxn) -> Result<RwTxn<'p>> {
        RwTxn::nested(self, parent)
    }

    /// Create a transaction with read-only access for use with the environment.
    ///
    /// You can make this transaction `Send`able between threads by
    /// using the `read-txn-no-tls` crate feature.
    /// See [`Self::static_read_txn`] if you want the txn to own the environment.
    ///
    /// ## LMDB Limitations
    ///
    /// It's possible to have multiple read transactions in the same environment
    /// while there is a write transaction ongoing.
    ///
    /// But read transactions prevent reuse of pages freed by newer write transactions,
    /// thus the database can grow quickly. Write transactions prevent other write transactions,
    /// since writes are serialized.
    ///
    /// So avoid long-lived read transactions.
    ///
    /// ## Errors
    ///
    /// * [`crate::MdbError::Panic`]: A fatal error occurred earlier, and the environment must be shut down
    /// * [`crate::MdbError::MapResized`]: Another process wrote data beyond this [`Env`] mapsize and this env
    ///   map must be resized
    /// * [`crate::MdbError::ReadersFull`]: a read-only transaction was requested, and the reader lock table is
    ///   full
    pub fn read_txn(&self) -> Result<RoTxn> {
        RoTxn::new(self)
    }

    /// Create a transaction with read-only access for use with the environment.
    /// Contrary to [`Self::read_txn`], this version **owns** the environment, which
    /// means you won't be able to close the environment while this transaction is alive.
    ///
    /// You can make this transaction `Send`able between threads by
    /// using the `read-txn-no-tls` crate feature.
    ///
    /// ## LMDB Limitations
    ///
    /// It's possible to have multiple read transactions in the same environment
    /// while there is a write transaction ongoing.
    ///
    /// But read transactions prevent reuse of pages freed by newer write transactions,
    /// thus the database can grow quickly. Write transactions prevent other write transactions,
    /// since writes are serialized.
    ///
    /// So avoid long-lived read transactions.
    ///
    /// ## Errors
    ///
    /// * [`crate::MdbError::Panic`]: A fatal error occurred earlier, and the environment must be shut down
    /// * [`crate::MdbError::MapResized`]: Another process wrote data beyond this [`Env`] mapsize and this env
    ///   map must be resized
    /// * [`crate::MdbError::ReadersFull`]: a read-only transaction was requested, and the reader lock table is
    ///   full
    pub fn static_read_txn(self) -> Result<RoTxn<'static>> {
        RoTxn::static_read_txn(self)
    }

    /// Copy an LMDB environment to the specified path, with options.
    ///
    /// This function may be used to make a backup of an existing environment.
    /// No lockfile is created, since it gets recreated at need.
    pub fn copy_to_file<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        let file = File::options().create_new(true).write(true).open(&path)?;
        let fd = get_file_fd(&file);

        unsafe { self.copy_to_fd(fd, option)? };

        // We reopen the file to make sure the cursor is at the start,
        // even a seek to start doesn't work properly.
        let file = File::open(path)?;

        Ok(file)
    }

    /// Copy an LMDB environment to the specified file descriptor, with compaction option.
    ///
    /// This function may be used to make a backup of an existing environment.
    /// No lockfile is created, since it gets recreated at need.
    ///
    /// # Safety
    ///
    /// The [`ffi::mdb_filehandle_t`] must have already been opened for Write access.
    pub unsafe fn copy_to_fd(
        &self,
        fd: ffi::mdb_filehandle_t,
        option: CompactionOption,
    ) -> Result<()> {
        let flags = if let CompactionOption::Enabled = option { ffi::MDB_CP_COMPACT } else { 0 };
        mdb_result(ffi::mdb_env_copyfd2(self.0.env, fd, flags))?;
        Ok(())
    }

    /// Flush the data buffers to disk.
    pub fn force_sync(&self) -> Result<()> {
        unsafe { mdb_result(ffi::mdb_env_sync(self.0.env, 1))? }
        Ok(())
    }

    /// Returns the canonicalized path where this env lives.
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// Returns an `EnvClosingEvent` that can be used to wait for the closing event,
    /// multiple threads can wait on this event.
    ///
    /// Make sure that you drop all the copies of `Env`s you have, env closing are triggered
    /// when all references are dropped, the last one will eventually close the environment.
    pub fn prepare_for_closing(self) -> EnvClosingEvent {
        let mut lock = OPENED_ENV.write().unwrap();
        match lock.get_mut(self.path()) {
            None => panic!("cannot find the env that we are trying to close"),
            Some(EnvEntry { env, signal_event, .. }) => {
                // We remove the env from the global list and replace it with a None.
                let _env = env.take();
                let signal_event = signal_event.clone();

                // we must make sure we release the lock before we drop the env
                // as the drop of the EnvInner also tries to lock the OPENED_ENV
                // global and we don't want to trigger a dead-lock.
                drop(lock);

                EnvClosingEvent(signal_event)
            }
        }
    }

    /// Check for stale entries in the reader lock table and clear them.
    ///
    /// Returns the number of stale readers cleared.
    pub fn clear_stale_readers(&self) -> Result<usize> {
        let mut dead: i32 = 0;
        unsafe { mdb_result(ffi::mdb_reader_check(self.0.env, &mut dead))? }
        // safety: The reader_check function asks for an i32, initialize it to zero
        //         and never decrements it. It is safe to use either an u32 or u64 (usize).
        Ok(dead as usize)
    }

    /// Resize the memory map to a new size.
    ///
    /// # Safety
    ///
    /// According to the [LMDB documentation](http://www.lmdb.tech/doc/group__mdb.html#gaa2506ec8dab3d969b0e609cd82e619e5),
    /// it is okay to call `mdb_env_set_mapsize` for an open environment as long as no transactions are active,
    /// but the library does not check for this condition, so the caller must ensure it explicitly.
    pub unsafe fn resize(&self, new_size: usize) -> Result<()> {
        if new_size % page_size::get() != 0 {
            let msg = format!(
                "map size ({}) must be a multiple of the system page size ({})",
                new_size,
                page_size::get()
            );
            return Err(Error::Io(io::Error::new(io::ErrorKind::InvalidInput, msg)));
        }
        mdb_result(unsafe { ffi::mdb_env_set_mapsize(self.env_mut_ptr(), new_size) })
            .map_err(Into::into)
    }

    /// Get the maximum size of keys and MDB_DUPSORT data we can write.
    ///
    /// Depends on the compile-time constant MDB_MAXKEYSIZE. Default 511
    pub fn max_key_size(&self) -> usize {
        let maxsize: i32 = unsafe { ffi::mdb_env_get_maxkeysize(self.env_mut_ptr()) };
        maxsize as usize
    }
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

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::time::Duration;
    use std::{fs, thread};

    use crate::types::*;
    use crate::{env_closing_event, EnvOpenOptions, Error};

    #[test]
    fn close_env() {
        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(30)
                .open(dir.path())
                .unwrap()
        };

        // Force a thread to keep the env for 1 second.
        let env_cloned = env.clone();
        thread::spawn(move || {
            let _env = env_cloned;
            thread::sleep(Duration::from_secs(1));
        });

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Str, Str>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, "hello", "hello").unwrap();
        db.put(&mut wtxn, "world", "world").unwrap();

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some(("hello", "hello")));
        assert_eq!(iter.next().transpose().unwrap(), Some(("world", "world")));
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.commit().unwrap();

        let signal_event = env.prepare_for_closing();

        eprintln!("waiting for the env to be closed");
        signal_event.wait();
        eprintln!("env closed successfully");

        // Make sure we don't have a reference to the env
        assert!(env_closing_event(dir.path()).is_none());
    }

    #[test]
    fn reopen_env_with_different_options_is_err() {
        let dir = tempfile::tempdir().unwrap();
        let _env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .open(dir.path())
                .unwrap()
        };

        let result = unsafe {
            EnvOpenOptions::new()
                .map_size(12 * 1024 * 1024) // 12MB
                .open(dir.path())
        };

        assert!(matches!(result, Err(Error::BadOpenOptions { .. })));
    }

    #[test]
    fn open_env_with_named_path() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("babar.mdb")).unwrap();
        let _env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .open(dir.path().join("babar.mdb"))
                .unwrap()
        };

        let _env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .open(dir.path().join("babar.mdb"))
                .unwrap()
        };
    }

    #[test]
    #[cfg(not(windows))]
    fn open_database_with_writemap_flag() {
        let dir = tempfile::tempdir().unwrap();
        let mut envbuilder = EnvOpenOptions::new();
        envbuilder.map_size(10 * 1024 * 1024); // 10MB
        envbuilder.max_dbs(10);
        unsafe { envbuilder.flags(crate::EnvFlags::WRITE_MAP) };
        let env = unsafe { envbuilder.open(dir.path()).unwrap() };

        let mut wtxn = env.write_txn().unwrap();
        let _db = env.create_database::<Str, Str>(&mut wtxn, Some("my-super-db")).unwrap();
        wtxn.commit().unwrap();
    }

    #[test]
    fn open_database_with_nosubdir() {
        let dir = tempfile::tempdir().unwrap();
        let mut envbuilder = EnvOpenOptions::new();
        unsafe { envbuilder.flags(crate::EnvFlags::NO_SUB_DIR) };
        let _env = unsafe { envbuilder.open(dir.path().join("data.mdb")).unwrap() };
    }

    #[test]
    fn create_database_without_commit() {
        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(10)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let _db = env.create_database::<Str, Str>(&mut wtxn, Some("my-super-db")).unwrap();
        wtxn.abort();

        let rtxn = env.read_txn().unwrap();
        let option = env.open_database::<Str, Str>(&rtxn, Some("my-super-db")).unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn open_already_existing_database() {
        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(10)
                .open(dir.path())
                .unwrap()
        };

        // we first create a database
        let mut wtxn = env.write_txn().unwrap();
        let _db = env.create_database::<Str, Str>(&mut wtxn, Some("my-super-db")).unwrap();
        wtxn.commit().unwrap();

        // Close the environement and reopen it, databases must not be loaded in memory.
        env.prepare_for_closing().wait();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(10)
                .open(dir.path())
                .unwrap()
        };

        let rtxn = env.read_txn().unwrap();
        let option = env.open_database::<Str, Str>(&rtxn, Some("my-super-db")).unwrap();
        assert!(option.is_some());
    }

    #[test]
    fn resize_database() {
        let dir = tempfile::tempdir().unwrap();
        let page_size = page_size::get();
        let env = unsafe {
            EnvOpenOptions::new().map_size(9 * page_size).max_dbs(1).open(dir.path()).unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Str, Str>(&mut wtxn, Some("my-super-db")).unwrap();
        wtxn.commit().unwrap();

        let mut wtxn = env.write_txn().unwrap();
        for i in 0..64 {
            db.put(&mut wtxn, &i.to_string(), "world").unwrap();
        }
        wtxn.commit().unwrap();

        let mut wtxn = env.write_txn().unwrap();
        for i in 64..128 {
            db.put(&mut wtxn, &i.to_string(), "world").unwrap();
        }
        wtxn.commit().expect_err("cannot commit a transaction that would reach the map size limit");

        unsafe {
            env.resize(10 * page_size).unwrap();
        }
        let mut wtxn = env.write_txn().unwrap();
        for i in 64..128 {
            db.put(&mut wtxn, &i.to_string(), "world").unwrap();
        }
        wtxn.commit().expect("transaction should commit after resizing the map size");

        assert_eq!(10 * page_size, env.info().map_size);
    }

    /// Non-regression test for
    /// <https://github.com/meilisearch/heed/issues/183>
    ///
    /// We should be able to open database Read-Only Env with
    /// no prior Read-Write Env opening. And query data.
    #[test]
    fn open_read_only_without_no_env_opened_before() {
        let expected_data0 = "Data Expected db0";
        let dir = tempfile::tempdir().unwrap();

        {
            // We really need this env to be dropped before the read-only access.
            let env = unsafe {
                EnvOpenOptions::new()
                    .map_size(16 * 1024 * 1024 * 1024) // 10MB
                    .max_dbs(32)
                    .open(dir.path())
                    .unwrap()
            };
            let mut wtxn = env.write_txn().unwrap();
            let database0 = env.create_database::<Str, Str>(&mut wtxn, Some("shared0")).unwrap();

            wtxn.commit().unwrap();
            let mut wtxn = env.write_txn().unwrap();
            database0.put(&mut wtxn, "shared0", expected_data0).unwrap();
            wtxn.commit().unwrap();
            // We also really need that no other env reside in memory in other thread doing tests.
            env.prepare_for_closing().wait();
        }

        {
            // Open now we do a read-only opening
            let env = unsafe {
                EnvOpenOptions::new()
                    .map_size(16 * 1024 * 1024 * 1024) // 10MB
                    .max_dbs(32)
                    .open(dir.path())
                    .unwrap()
            };
            let database0 = {
                let rtxn = env.read_txn().unwrap();
                let database0 =
                    env.open_database::<Str, Str>(&rtxn, Some("shared0")).unwrap().unwrap();
                // This commit is mandatory if not committed you might get
                // Io(Os { code: 22, kind: InvalidInput, message: "Invalid argument" })
                rtxn.commit().unwrap();
                database0
            };

            {
                // If we didn't committed the opening it might fail with EINVAL.
                let rtxn = env.read_txn().unwrap();
                let value = database0.get(&rtxn, "shared0").unwrap().unwrap();
                assert_eq!(value, expected_data0);
            }

            env.prepare_for_closing().wait();
        }

        // To avoid reintroducing the bug let's try to open again but without the commit
        {
            // Open now we do a read-only opening
            let env = unsafe {
                EnvOpenOptions::new()
                    .map_size(16 * 1024 * 1024 * 1024) // 10MB
                    .max_dbs(32)
                    .open(dir.path())
                    .unwrap()
            };
            let database0 = {
                let rtxn = env.read_txn().unwrap();
                let database0 =
                    env.open_database::<Str, Str>(&rtxn, Some("shared0")).unwrap().unwrap();
                // No commit it's important, dropping explicitly
                drop(rtxn);
                database0
            };

            {
                // We didn't committed the opening we will get EINVAL.
                let rtxn = env.read_txn().unwrap();
                // The dbg!() is intentional in case of a change in rust-std or in lmdb related
                // to the windows error.
                let err = dbg!(database0.get(&rtxn, "shared0"));

                // The error kind is still ErrorKind Uncategorized on windows.
                // Behind it's a ERROR_BAD_COMMAND code 22 like EINVAL.
                if cfg!(windows) {
                    assert!(err.is_err());
                } else {
                    assert!(
                        matches!(err, Err(Error::Io(ref e)) if e.kind() == ErrorKind::InvalidInput)
                    );
                }
            }

            env.prepare_for_closing().wait();
        }
    }

    #[test]
    fn max_key_size() {
        let dir = tempfile::tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path().join(dir.path())).unwrap() };
        let maxkeysize = env.max_key_size();

        eprintln!("maxkeysize: {}", maxkeysize);

        if cfg!(feature = "longer-keys") {
            // Should be larger than the default of 511
            assert!(maxkeysize > 511);
        } else {
            // Should be the default of 511
            assert_eq!(maxkeysize, 511);
        }
    }
}
