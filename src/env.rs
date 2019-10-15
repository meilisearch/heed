use std::any::TypeId;
use std::collections::hash_map::{HashMap, Entry};
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{ptr, sync};

use once_cell::sync::OnceCell;
use crate::lmdb_error::lmdb_result;
use crate::{RoTxn, RwTxn, Database, DynDatabase, Result, Error};

static OPENED_ENV: OnceCell<Mutex<HashMap<PathBuf, Env>>> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct EnvOpenOptions {
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: Option<u32>,
}

impl EnvOpenOptions {
    pub fn new() -> EnvOpenOptions {
        EnvOpenOptions { map_size: None, max_readers: None, max_dbs: None }
    }

    pub fn map_size(&mut self, size: usize) -> &mut Self {
        if size % page_size::get() != 0 {
            panic!("map size ({}) must be a multiple of the system page size ({})",
                    size, page_size::get());
        }

        self.map_size = Some(size);

        self
    }

    pub fn max_readers(&mut self, readers: u32) -> &mut Self {
        self.max_readers = Some(readers); self
    }

    pub fn max_dbs(&mut self, dbs: u32) -> &mut Self {
        self.max_dbs = Some(dbs); self
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

                    let result = lmdb_result(ffi::mdb_env_open(env, path.as_ptr(), 0, 0o600));

                    match result {
                        Ok(()) => {
                            let inner = EnvInner { env, dbi_open_mutex: sync::Mutex::default() };
                            let env = Env(Arc::new(inner));
                            Ok(entry.insert(env).clone())
                        },
                        Err(e) => { ffi::mdb_env_close(env); Err(e.into()) },
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

impl Env {
    pub fn open_database<KC, DC>(&self, name: Option<&str>) -> Result<Option<Database<KC, DC>>>
    where KC: 'static,
          DC: 'static,
    {
        let types = (TypeId::of::<KC>(), TypeId::of::<DC>());
        Ok(self.raw_open_database(name, Some(types))?.map(Database::new))
    }

    pub fn open_dyn_database(&self, name: Option<&str>) -> Result<Option<DynDatabase>> {
        Ok(self.raw_open_database(name, None)?.map(DynDatabase::new))
    }

    fn raw_open_database(&self, name: Option<&str>, types: Option<(TypeId, TypeId)>) -> Result<Option<u32>> {
        let rtxn = self.read_txn()?;

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let mut lock = self.0.dbi_open_mutex.lock().unwrap();

        let result = unsafe {
            lmdb_result(ffi::mdb_dbi_open(
                rtxn.txn,
                name_ptr,
                0,
                &mut dbi,
            ))
        };

        drop(name);

        match result {
            Ok(()) => {
                let old_types = lock.entry(dbi).or_insert(types);

                if *old_types == types {
                    Ok(Some(dbi))
                } else {
                    Err(Error::InvalidDatabaseTyping)
                }
            },
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn create_database<KC, DC>(&self, name: Option<&str>) -> Result<Database<KC, DC>>
    where KC: 'static,
          DC: 'static,
    {
        let types = (TypeId::of::<KC>(), TypeId::of::<DC>());
        self.raw_create_database(name, Some(types)).map(Database::new)
    }

    pub fn create_dyn_database(&self, name: Option<&str>) -> Result<DynDatabase> {
        self.raw_create_database(name, None).map(DynDatabase::new)
    }

    fn raw_create_database(&self, name: Option<&str>, types: Option<(TypeId, TypeId)>) -> Result<u32> {
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
            },
            Err(e) => Err(e.into()),
        }
    }

    pub fn write_txn(&self) -> Result<RwTxn> {
        RwTxn::new(self.0.env)
    }

    pub fn read_txn(&self) -> Result<RoTxn> {
        RoTxn::new(self.0.env)
    }
}
