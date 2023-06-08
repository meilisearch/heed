use std::ops::Deref;
use std::ptr;

use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::{Env, Result};

/// A read-only transaction.
pub struct RoTxn<'e> {
    pub(crate) txn: *mut ffi::MDB_txn,
    env: &'e Env,
}

impl<'e> RoTxn<'e> {
    pub(crate) fn new(env: &'e Env) -> Result<RoTxn<'e>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn, env })
    }

    pub(crate) fn env_mut_ptr(&self) -> *mut ffi::MDB_env {
        self.env.env_mut_ptr()
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            abort_txn(self.txn);
        }
    }
}

#[cfg(feature = "sync-read-txn")]
unsafe impl Sync for RoTxn<'_> {}

fn abort_txn(txn: *mut ffi::MDB_txn) {
    // Asserts that the transaction hasn't been already committed.
    assert!(!txn.is_null());
    unsafe { ffi::mdb_txn_abort(txn) }
}

/// A read-write transaction.
pub struct RwTxn<'p> {
    pub(crate) txn: RoTxn<'p>,
}

impl<'p> RwTxn<'p> {
    pub(crate) fn new(env: &'p Env) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe { mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr(), ptr::null_mut(), 0, &mut txn))? };

        Ok(RwTxn { txn: RoTxn { txn, env } })
    }

    pub(crate) fn nested(env: &'p Env, parent: &'p mut RwTxn) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_ptr: *mut ffi::MDB_txn = parent.txn.txn;

        unsafe { mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr(), parent_ptr, 0, &mut txn))? };

        Ok(RwTxn { txn: RoTxn { txn, env } })
    }

    pub(crate) fn env_mut_ptr(&self) -> *mut ffi::MDB_env {
        self.txn.env.env_mut_ptr()
    }

    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(self.txn.txn)) };
        self.txn.txn = ptr::null_mut();
        result.map_err(Into::into)
    }

    pub fn abort(mut self) {
        abort_txn(self.txn.txn);
        self.txn.txn = ptr::null_mut();
    }
}

impl<'p> Deref for RwTxn<'p> {
    type Target = RoTxn<'p>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
