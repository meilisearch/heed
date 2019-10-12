use std::ops::Deref;
use std::ptr;

use crate::lmdb_error::lmdb_result;
use crate::Result;

pub struct RoTxn {
    pub(crate) txn: *mut ffi::MDB_txn,
}

impl RoTxn {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> Result<RoTxn> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            lmdb_result(ffi::mdb_txn_begin(
                env,
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn })
    }

    pub fn abort(self) {
        drop(self)
    }
}

impl Drop for RoTxn {
    fn drop(&mut self) {
        unsafe { ffi::mdb_txn_abort(self.txn) }
    }
}

pub struct RwTxn {
    pub(crate) txn: RoTxn,
}

impl RwTxn {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> Result<RwTxn> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            lmdb_result(ffi::mdb_txn_begin(
                env,
                ptr::null_mut(),
                0,
                &mut txn,
            ))?
        };

        Ok(RwTxn { txn: RoTxn { txn } })
    }

    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { lmdb_result(ffi::mdb_txn_commit(self.txn.txn)) };
        self.txn.txn = ptr::null_mut();
        result.map_err(Into::into)
    }

    pub fn abort(self) {
        drop(self.txn)
    }
}

impl Deref for RwTxn {
    type Target = RoTxn;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
