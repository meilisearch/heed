use std::path::Path;
use std::ffi::CString;
use std::{io, ptr};
use lmdb_sys as ffi;

use crate::{RoTxn, RwTxn, Database, ZResult, Error};

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

    pub fn open<P: AsRef<Path>>(&self, path: P) -> ZResult<Env> {
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        let ret = unsafe { ffi::mdb_env_create(&mut env) };

        if ret != 0 { return Err(Error::CouldNotCreateEnv) }

        let path = path.as_ref();
        let path = path.to_string_lossy();
        let path = CString::new(path.as_bytes()).unwrap();

        if let Some(size) = self.map_size {
            let ret = unsafe { ffi::mdb_env_set_mapsize(env, size) };
            if ret != 0 { panic!("BUG: failed to set map size ({})", ret) }
        }

        if let Some(readers) = self.max_readers {
            let ret = unsafe { ffi::mdb_env_set_maxreaders(env, readers) };
            if ret != 0 { panic!("BUG: failed to set max readers ({})", ret) }
        }

        if let Some(dbs) = self.max_dbs {
            let ret = unsafe { ffi::mdb_env_set_maxdbs(env, dbs) };
            if ret != 0 { panic!("BUG: failed to set max dbs ({})", ret) }
        }

        let ret = unsafe { ffi::mdb_env_open(env, path.as_ptr(), 0, 0o600) };

        let error = match ret {
            ffi::MDB_VERSION_MISMATCH => Error::VersionMismatch,
            ffi::MDB_INVALID => Error::InvalidFile,
            0 => return Ok(Env::new(env)),
            os_error         => Error::Io(io::Error::from_raw_os_error(os_error)),
        };

        unsafe { ffi::mdb_env_close(env) }
        Err(error)
    }
}

pub struct Env {
    env: *mut ffi::MDB_env,
}

impl Env {
    fn new(env: *mut ffi::MDB_env) -> Env {
        Env { env }
    }

    pub fn create_database<KC, DC>(&self, name: Option<&str>) -> Database<KC, DC> {
        let wtxn = self.write_txn();

        let mut dbi = 0;
        let name = name.map(|n| CString::new(n).unwrap());
        let name_ptr = match name {
            Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
            None => ptr::null(),
        };

        let ret = unsafe {
            ffi::mdb_dbi_open(
                wtxn.txn.txn,
                name_ptr,
                ffi::MDB_CREATE,
                &mut dbi,
            )
        };

        drop(name);
        assert_eq!(ret, 0);

        wtxn.commit();

        Database::new(dbi)
    }

    pub fn write_txn(&self) -> RwTxn {
        RwTxn::new(self.env)
    }

    pub fn read_txn(&self) -> RoTxn {
        RoTxn::new(self.env)
    }
}
