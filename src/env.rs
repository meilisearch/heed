use std::any::TypeId;
use std::collections::hash_map::{Entry, HashMap};
use std::ffi::CString;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{ptr, sync};

use crate::lmdb_error::lmdb_result;
use crate::{Database, Error, PolyDatabase, Result, RoTxn, RwTxn};
use lmdb_sys as ffi;
use once_cell::sync::OnceCell;
use enumflags2::BitFlags;
use crate::flags::Flags;

static OPENED_ENV: OnceCell<Mutex<HashMap<PathBuf, Env>>> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct EnvOpenOptions {
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: Option<u32>,
    flags: BitFlags<Flags>,     // LMDB flags
}

impl EnvOpenOptions {
    pub fn new() -> EnvOpenOptions {
        EnvOpenOptions {
            map_size: None,
            max_readers: None,
            max_dbs: None,
            flags: BitFlags::empty(),
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
    /// use heed::{EnvOpenOptions, Database};
    /// use heed::types::*;
    /// use heed::flags::Flags;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// fs::create_dir_all("target/zerocopy.mdb")?;
    /// let mut envBuilder = EnvOpenOptions::new();
    /// unsafe {
    ///     envBuilder.flag(Flags::MdbNoSync);
    ///     envBuilder.flag(Flags::MdbNoMetaSync);
    /// }
    ///  let env = envBuilder.open("target/zerocopy.mdb")?;
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
        self.flags.insert(flag);
        self
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<Env> {
        let path = path.as_ref();
        let path = path.canonicalize()?;

        let mutex = OPENED_ENV.get_or_init(Mutex::default);
        let mut lock = mutex.lock().unwrap();

        match lock.entry(path) {
            Entry::Occupied(entry) => Ok(entry.get().clone()),
            Entry::Vacant(entry) => {
                let path = entry.key();
                let path = path.to_string_lossy();
                let path = CString::new(path.as_bytes()).unwrap();

                unsafe {
                    let mut env: *mut ffi::MDB_env = ptr::null_mut();
                    lmdb_result(ffi::mdb_env_create(&mut env))?;

                    if let Some(size) = self.map_size {
                        lmdb_result(ffi::mdb_env_set_mapsize(env, size))?;
                    }

                    if let Some(readers) = self.max_readers {
                        lmdb_result(ffi::mdb_env_set_maxreaders(env, readers))?;
                    }

                    if let Some(dbs) = self.max_dbs {
                        lmdb_result(ffi::mdb_env_set_maxdbs(env, dbs))?;
                    }

                    let result = lmdb_result(ffi::mdb_env_open(env, path.as_ptr(), self.flags.bits(), 0o600));

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
    pub fn open_database<KC, DC>(&self, name: Option<&str>) -> Result<Option<Database<KC, DC>>>
        where
            KC: 'static,
            DC: 'static,
    {
        let types = (TypeId::of::<KC>(), TypeId::of::<DC>());
        Ok(self
            .raw_open_database(name, Some(types))?
            .map(Database::new))
    }

    pub fn open_dyn_database(&self, name: Option<&str>) -> Result<Option<PolyDatabase>> {
        Ok(self.raw_open_database(name, None)?.map(PolyDatabase::new))
    }

    fn raw_open_database(
        &self,
        name: Option<&str>,
        types: Option<(TypeId, TypeId)>,
    ) -> Result<Option<u32>> {
        let rtxn = self.read_txn()?;

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let mut lock = self.0.dbi_open_mutex.lock().unwrap();

        let result = unsafe { lmdb_result(ffi::mdb_dbi_open(rtxn.txn, name_ptr, 0, &mut dbi)) };

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

    pub fn create_database<KC, DC>(&self, name: Option<&str>) -> Result<Database<KC, DC>>
        where
            KC: 'static,
            DC: 'static,
    {
        let types = (TypeId::of::<KC>(), TypeId::of::<DC>());
        self.raw_create_database(name, Some(types))
            .map(Database::new)
    }

    pub fn create_poly_database(&self, name: Option<&str>) -> Result<PolyDatabase> {
        self.raw_create_database(name, None).map(PolyDatabase::new)
    }

    fn raw_create_database(
        &self,
        name: Option<&str>,
        types: Option<(TypeId, TypeId)>,
    ) -> Result<u32> {
        let wtxn = self.write_txn()?;

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let mut lock = self.0.dbi_open_mutex.lock().unwrap();

        let result = unsafe {
            lmdb_result(ffi::mdb_dbi_open(
                wtxn.txn.txn,
                name_ptr,
                ffi::MDB_CREATE,
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

    pub fn read_txn(&self) -> Result<RoTxn> {
        RoTxn::new(self.0.env)
    }

    pub fn copy_to_path<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        let flags = if let CompactionOption::Enabled = option {
            ffi::MDB_CP_COMPACT
        } else {
            0
        };

        let file = File::create(path)?;
        let fd = file.as_raw_fd();

        unsafe { lmdb_result(ffi::mdb_env_copyfd2(self.0.env, fd, flags))? }

        Ok(file)
    }
}
