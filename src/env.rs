use std::path::Path;
use std::ffi::CString;
use std::ptr;
use lmdb_sys as ffi;

use crate::{TxnRead, TxnWrite, Database};

pub struct Env {
    env: *mut ffi::MDB_env,
}

impl Env {
    pub fn open<P: AsRef<Path>>(path: P) -> Env {
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        let ret = unsafe { ffi::mdb_env_create(&mut env) };

        assert_eq!(ret, 0);

        let path = path.as_ref();
        let path = path.to_string_lossy();
        let path = CString::new(path.as_bytes()).unwrap();
        let path_bytes = path.as_bytes_with_nul().as_ptr() as *const i8;

        let flags = 0;
        let mode = 0o600;
        let ret = unsafe { ffi::mdb_env_open(env, path_bytes, flags, mode) };

        assert_eq!(ret, 0);

        Env { env }
    }

    pub fn open_database<KC, DC>(&self, name: Option<&str>) -> Database<KC, DC> {
        let wtxn = self.write_txn();

        let mut dbi = 0;
        let name = ptr::null();

        let ret = unsafe {
            ffi::mdb_dbi_open(
                wtxn.txn.txn,
                name,
                0,
                &mut dbi,
            )
        };

        assert_eq!(ret, 0);

        Database::new(dbi)
    }

    pub fn write_txn(&self) -> TxnWrite {
        TxnWrite::new(self.env)
    }

    pub fn read_txn(&self) -> TxnRead {
        TxnRead::new(self.env)
    }
}
