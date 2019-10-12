use std::path::Path;
use std::ffi::CString;
use std::{ptr, sync};

use crate::lmdb_error::lmdb_result;
use crate::{RoTxn, RwTxn, Database, Result};

#[derive(Clone, Debug)]
pub struct EnvBuilder {
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: Option<u32>,
}

impl EnvBuilder {
    pub fn new() -> EnvBuilder {
        EnvBuilder { map_size: None, max_readers: None, max_dbs: None }
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
        unsafe {
            let mut env: *mut ffi::MDB_env = ptr::null_mut();
            lmdb_result(ffi::mdb_env_create(&mut env))?;

            let path = path.as_ref();
            let path = path.to_string_lossy();
            let path = CString::new(path.as_bytes()).unwrap();

            if let Some(size) = self.map_size {
                lmdb_result(ffi::mdb_env_set_mapsize(env, size))?;
            }

            if let Some(readers) = self.max_readers {
                lmdb_result(ffi::mdb_env_set_maxreaders(env, readers))?;
            }

            if let Some(dbs) = self.max_dbs {
                lmdb_result(ffi::mdb_env_set_maxdbs(env, dbs))?;
            }

            match lmdb_result(ffi::mdb_env_open(env, path.as_ptr(), 0, 0o600)) {
                Ok(()) => return Ok(Env { env, dbi_open_mutex: sync::Mutex::new(()) }),
                Err(e) => { ffi::mdb_env_close(env); Err(e.into()) },
            }
        }
    }
}

pub struct Env {
    env: *mut ffi::MDB_env,
    dbi_open_mutex: sync::Mutex<()>,
}

impl Env {
    pub fn open_database<KC, DC>(&self, name: Option<&str>) -> Result<Option<Database<KC, DC>>> {
        let rtxn = self.read_txn()?;

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let lock = self.dbi_open_mutex.lock().unwrap();

        let result = unsafe {
            lmdb_result(ffi::mdb_dbi_open(
                rtxn.txn,
                name_ptr,
                0,
                &mut dbi,
            ))
        };

        drop(lock);
        drop(name);

        match result {
            Ok(()) => Ok(Some(Database::new(dbi))),
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn create_database<KC, DC>(&self, name: Option<&str>) -> Result<Database<KC, DC>> {
        let wtxn = self.write_txn()?;

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let lock = self.dbi_open_mutex.lock().unwrap();

        let result = unsafe {
            lmdb_result(ffi::mdb_dbi_open(
                wtxn.txn.txn,
                name_ptr,
                ffi::MDB_CREATE,
                &mut dbi,
            ))
        };

        drop(lock);
        drop(name);

        match result {
            Ok(()) => {
                wtxn.commit()?;
                Ok(Database::new(dbi))
            },
            Err(e) => Err(e.into()),
        }
    }

    pub fn write_txn(&self) -> Result<RwTxn> {
        RwTxn::new(self.env)
    }

    pub fn read_txn(&self) -> Result<RoTxn> {
        RoTxn::new(self.env)
    }
}
