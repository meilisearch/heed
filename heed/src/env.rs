use std::any::TypeId;
use std::collections::hash_map::{Entry, HashMap};
use std::ffi::CString;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{io, ptr, sync};
#[cfg(windows)]
use std::ffi::OsStr;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

use crate::flags::{Flags, DBFlags};
use crate::mdb::error::mdb_result;
use crate::{Database, Error, PolyDatabase, Result, RoTxn, RwTxn};
use crate::mdb::ffi;
use once_cell::sync::OnceCell;

static OPENED_ENV: OnceCell<Mutex<HashMap<PathBuf, Env>>> = OnceCell::new();

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
    let url = url::Url::from_file_path(&canonical).map_err(|_e| io::Error::new(io::ErrorKind::Other, "URL passing error"))?;
    url.to_file_path().map_err(|_e| io::Error::new(io::ErrorKind::Other, "path canonicalization error"))
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

#[cfg(windows)]
fn get_file_fd(file: &File) -> std::os::windows::io::RawHandle {
    use std::os::windows::io::AsRawHandle;
    file.as_raw_handle()
}

#[cfg(unix)]
fn get_file_fd(file: &File) -> std::os::unix::io::RawFd {
    use std::os::unix::io::AsRawFd;
    file.as_raw_fd()
}

#[derive(Clone, Debug)]
pub struct EnvOpenOptions {
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: Option<u32>,
    flags: u32, // LMDB flags
}

impl EnvOpenOptions {
    pub fn new() -> EnvOpenOptions {
        EnvOpenOptions {
            map_size: None,
            max_readers: None,
            max_dbs: None,
            flags: 0,
        }
    }

    pub fn map_size(&mut self, size: usize) -> &mut Self {
        if size % page_size::get() != 0 {
            panic!(
                "map size ({}) must be a multiple of the system page size ({})",
                size,
                page_size::get()
            );
        }

        self.map_size = Some(size);

        self
    }

    pub fn max_readers(&mut self, readers: u32) -> &mut Self {
        self.max_readers = Some(readers);
        self
    }

    pub fn max_dbs(&mut self, dbs: u32) -> &mut Self {
        self.max_dbs = Some(dbs);
        self
    }

    /// Set one or more LMDB flags (see http://www.lmdb.tech/doc/group__mdb__env.html).
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use heed::{EnvOpenOptions, Database};
    /// use heed::types::*;
    /// use heed::flags::Flags;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// let mut env_builder = EnvOpenOptions::new();
    /// unsafe {
    ///     env_builder.flag(Flags::MdbNoTls);
    ///     env_builder.flag(Flags::MdbNoMetaSync);
    /// }
    /// let env = env_builder.open(Path::new("target").join("zerocopy.mdb"))?;
    ///
    /// // we will open the default unamed database
    /// let db: Database<Str, OwnedType<i32>> = env.create_database(None)?;
    ///
    /// // opening a write transaction
    /// let mut wtxn = env.write_txn()?;
    /// db.put(&mut wtxn, "seven", &7)?;
    /// db.put(&mut wtxn, "zero", &0)?;
    /// db.put(&mut wtxn, "five", &5)?;
    /// db.put(&mut wtxn, "three", &3)?;
    /// wtxn.commit()?;
    ///
    /// // Force the OS to flush the buffers (see Flags::MdbNoSync and Flags::MdbNoMetaSync).
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
    pub unsafe fn flag(&mut self, flag: Flags) -> &mut Self {
        self.flags = self.flags | flag as u32;
        self
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<Env> {
        let path = path.as_ref();
        let path = canonicalize_path(path)?;

        let mutex = OPENED_ENV.get_or_init(Mutex::default);
        let mut lock = mutex.lock().unwrap();

        match lock.entry(path) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let path = entry.key();
                let path = CString::new(path.as_os_str().as_bytes()).unwrap();

                unsafe {
                    let mut env: *mut ffi::MDB_env = ptr::null_mut();
                    mdb_result(ffi::mdb_env_create(&mut env))?;

                    if let Some(size) = self.map_size {
                        mdb_result(ffi::mdb_env_set_mapsize(env, size))?;
                    }

                    if let Some(readers) = self.max_readers {
                        mdb_result(ffi::mdb_env_set_maxreaders(env, readers))?;
                    }

                    if let Some(dbs) = self.max_dbs {
                        mdb_result(ffi::mdb_env_set_maxdbs(env, dbs))?;
                    }

                    let result =
                        mdb_result(ffi::mdb_env_open(env, path.as_ptr(), self.flags, 0o600));

                    match result {
                        Ok(()) => {
                            let inner = EnvInner {
                                env,
                                dbi_open_mutex: sync::Mutex::default(),
                            };
                            let env = Env(Arc::new(inner));
                            Ok(entry.insert(env).clone())
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

#[derive(Clone)]
pub struct Env(Arc<EnvInner>);

struct EnvInner {
    env: *mut ffi::MDB_env,
    dbi_open_mutex: sync::Mutex<HashMap<u32, Option<(TypeId, TypeId)>>>,
}

unsafe impl Send for EnvInner {}

unsafe impl Sync for EnvInner {}

#[derive(Debug, Copy, Clone)]
pub enum CompactionOption {
    Enabled,
    Disabled,
}

impl Env {
    pub fn open_database<KC, DC>(&self, name: Option<&str>, db_flags: Option<u32>) -> Result<Option<Database<KC, DC>>>
    where
        KC: 'static,
        DC: 'static,
    {
        let types = (TypeId::of::<KC>(), TypeId::of::<DC>());
        Ok(self
            .raw_open_database(name, Some(types), db_flags)?
            .map(Database::new))
    }

    pub fn open_poly_database(&self, name: Option<&str>, db_flags: Option<u32>) -> Result<Option<PolyDatabase>> {
        Ok(self.raw_open_database(name, None, db_flags)?.map(PolyDatabase::new))
    }

    fn raw_open_database(
        &self,
        name: Option<&str>,
        types: Option<(TypeId, TypeId)>,
        db_flags: Option<u32>
    ) -> Result<Option<u32>> {
        let rtxn = self.read_txn()?;

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let mut lock = self.0.dbi_open_mutex.lock().unwrap();

        let result = unsafe { mdb_result(ffi::mdb_dbi_open(rtxn.txn, name_ptr, db_flags.unwrap_or(0) as u32, &mut dbi)) };

        drop(name);

        match result {
            Ok(()) => {
                rtxn.commit()?;

                let old_types = lock.entry(dbi).or_insert(types);

                if *old_types == types {
                    Ok(Some(dbi))
                } else {
                    Err(Error::InvalidDatabaseTyping)
                }
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn create_database<KC, DC>(&self, name: Option<&str>,
        db_flags: Option<u32>) -> Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut parent_wtxn = self.write_txn()?;
        let db = self.create_database_with_txn(name, &mut parent_wtxn, db_flags)?;
        parent_wtxn.commit()?;
        Ok(db)
    }

    pub fn create_database_with_txn<KC, DC>(
        &self,
        name: Option<&str>,
        parent_wtxn: &mut RwTxn,
        db_flags: Option<u32>,
    ) -> Result<Database<KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let types = (TypeId::of::<KC>(), TypeId::of::<DC>());
        self.raw_create_database(name, Some(types), parent_wtxn, db_flags)
            .map(Database::new)
    }

    pub fn create_poly_database(&self, name: Option<&str>, db_flags: Option<u32>) -> Result<PolyDatabase> {
        let mut parent_wtxn = self.write_txn()?;
        let db = self.create_poly_database_with_txn(name, &mut parent_wtxn, db_flags)?;
        parent_wtxn.commit()?;
        Ok(db)
    }

    pub fn create_poly_database_with_txn(
        &self,
        name: Option<&str>,
        parent_wtxn: &mut RwTxn,
        db_flags: Option<u32>,
    ) -> Result<PolyDatabase> {
        self.raw_create_database(name, None, parent_wtxn, db_flags)
            .map(PolyDatabase::new)
    }

    fn raw_create_database(
        &self,
        name: Option<&str>,
        types: Option<(TypeId, TypeId)>,
        parent_wtxn: &mut RwTxn,
        db_flags: Option<u32>,
    ) -> Result<u32> {
        let wtxn = self.nested_write_txn(parent_wtxn)?;

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let mut lock = self.0.dbi_open_mutex.lock().unwrap();

        let result = unsafe {
            mdb_result(ffi::mdb_dbi_open(
                wtxn.txn.txn,
                name_ptr,
                DBFlags::MdbCreate as u32 | db_flags.unwrap_or(0),
                &mut dbi,
            ))
        };

        drop(name);

        match result {
            Ok(()) => {
                wtxn.commit()?;

                let old_types = lock.entry(dbi).or_insert(types);

                if *old_types == types {
                    Ok(dbi)
                } else {
                    Err(Error::InvalidDatabaseTyping)
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn write_txn(&self) -> Result<RwTxn> {
        RwTxn::new(self.0.env)
    }

    pub fn typed_write_txn<T>(&self) -> Result<RwTxn<T>> {
        RwTxn::<T>::new(self.0.env)
    }

    pub fn nested_write_txn<'p, T>(&self, parent: &'p mut RwTxn<T>) -> Result<RwTxn<'p, T>> {
        RwTxn::nested(self.0.env, parent)
    }

    pub fn read_txn(&self) -> Result<RoTxn> {
        RoTxn::new(self.0.env)
    }

    pub fn typed_read_txn<T>(&self) -> Result<RoTxn<T>> {
        RoTxn::new(self.0.env)
    }

    // TODO rename into `copy_to_file` for more clarity
    pub fn copy_to_path<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        let file = File::create(&path)?;
        let fd = get_file_fd(&file);

        unsafe { self.copy_to_fd(fd, option)?; }

        // We reopen the file to make sure the cursor is at the start,
        // even a seek to start doesn't work properly.
        let file = File::open(path)?;

        Ok(file)
    }

    pub unsafe fn copy_to_fd(&self, fd: ffi::mdb_filehandle_t, option: CompactionOption) -> Result<()> {
        let flags = if let CompactionOption::Enabled = option { ffi::MDB_CP_COMPACT } else { 0 };

        mdb_result(ffi::mdb_env_copy2fd(self.0.env, fd, flags))?;

        Ok(())
    }

    #[cfg(all(feature = "lmdb", not(feature = "mdbx")))]
    pub fn force_sync(&self) -> Result<()> {
        unsafe { mdb_result(ffi::mdb_env_sync(self.0.env, 1))? }

        Ok(())
    }

    #[cfg(all(feature = "mdbx", not(feature = "lmdb")))]
    pub fn force_sync(&self) -> Result<()> {
        unsafe { mdb_result(ffi::mdb_env_sync(self.0.env))? }

        Ok(())
    }
}
