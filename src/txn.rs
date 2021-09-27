use std::marker;
use std::ops::Deref;
use std::ptr;

use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::{Env, Result};

pub struct RoTxn<'e> {
    pub(crate) txn: *mut ffi::MDB_txn,
    pub(crate) env: &'e Env,
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

    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(self.txn)) };
        self.txn = ptr::null_mut();
        result.map_err(Into::into)
    }

    pub fn abort(mut self) -> Result<()> {
        let result = abort_txn(self.txn);
        self.txn = ptr::null_mut();
        result
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            let _ = abort_txn(self.txn);
        }
    }
}

#[cfg(feature = "sync-read-txn")]
unsafe impl Sync for RoTxn<'_> {}

fn abort_txn(txn: *mut ffi::MDB_txn) -> Result<()> {
    // Asserts that the transaction hasn't been already committed.
    assert!(!txn.is_null());
    Ok(unsafe { ffi::mdb_txn_abort(txn) })
}

pub struct RwTxn<'e, 'p> {
    pub(crate) txn: RoTxn<'e>,
    _phantom: marker::PhantomData<&'p ()>,
}

impl<'e> RwTxn<'e, 'e> {
    pub(crate) fn new(env: &'e Env) -> Result<RwTxn<'e, 'e>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr(),
                ptr::null_mut(),
                0,
                &mut txn,
            ))?
        };

        Ok(RwTxn {
            txn: RoTxn { txn, env },
            _phantom: marker::PhantomData,
        })
    }

    pub(crate) fn nested<'p: 'e>(env: &'e Env, parent: &'p mut RwTxn) -> Result<RwTxn<'e, 'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_ptr: *mut ffi::MDB_txn = parent.txn.txn;

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr(),
                parent_ptr,
                0,
                &mut txn,
            ))?
        };

        Ok(RwTxn {
            txn: RoTxn { txn, env },
            _phantom: marker::PhantomData,
        })
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    pub fn abort(self) -> Result<()> {
        self.txn.abort()
    }
}

impl<'e, 'p> Deref for RwTxn<'e, 'p> {
    type Target = RoTxn<'e>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
