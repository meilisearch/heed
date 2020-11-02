use std::marker;
use std::ops::Deref;
use std::ptr;

use crate::mdb::ffi;
use crate::mdb::error::mdb_result;
use crate::{Env, Result};

pub struct RoTxn<'e, T=()> {
    pub(crate) txn: *mut ffi::MDB_txn,
    pub(crate) env: &'e Env,
    _phantom: marker::PhantomData<T>,
}

impl<'e, T> RoTxn<'e, T> {
    pub(crate) fn new(env: &'e Env) -> Result<RoTxn<'e, T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn, env, _phantom: marker::PhantomData })
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

impl<T> Drop for RoTxn<'_, T> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            let _ = abort_txn(self.txn);
        }
    }
}

#[cfg(feature = "sync-read-txn")]
unsafe impl<T> Sync for RoTxn<'_, T> { }

#[cfg(all(feature = "lmdb", not(feature = "mdbx")))]
fn abort_txn(txn: *mut ffi::MDB_txn) -> Result<()> {
    // Asserts that the transaction hasn't been already committed.
    assert!(!txn.is_null());
    Ok(unsafe { ffi::mdb_txn_abort(txn) })
}

#[cfg(all(feature = "mdbx", not(feature = "lmdb")))]
fn abort_txn(txn: *mut ffi::MDB_txn) -> Result<()> {
    // Asserts that the transaction hasn't been already committed.
    assert!(!txn.is_null());

    let ret = unsafe { ffi::mdb_txn_abort(txn) };
    mdb_result(ret).map_err(Into::into)
}

pub struct RwTxn<'e, 'p, T=()> {
    pub(crate) txn: RoTxn<'e, T>,
    _parent: marker::PhantomData<&'p mut ()>,
}

impl<'e, T> RwTxn<'e, 'e, T> {
    pub(crate) fn new(env: &'e Env) -> Result<RwTxn<'e, 'e, T>> {
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
            txn: RoTxn { txn, env, _phantom: marker::PhantomData },
            _parent: marker::PhantomData,
        })
    }

    pub(crate) fn nested<'p: 'e>(env: &'e Env, parent: &'p mut RwTxn<T>) -> Result<RwTxn<'e, 'p, T>> {
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
            txn: RoTxn { txn, env, _phantom: marker::PhantomData },
            _parent: marker::PhantomData,
        })
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    pub fn abort(self) -> Result<()> {
        self.txn.abort()
    }
}

impl<'e, 'p, T> Deref for RwTxn<'e, 'p, T> {
    type Target = RoTxn<'e, T>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
