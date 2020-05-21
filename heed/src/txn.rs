use std::marker;
use std::ops::Deref;
use std::ptr;

use crate::mdb::ffi;
use crate::mdb::error::mdb_result;
use crate::Result;

pub struct RoTxn<T=()> {
    pub(crate) txn: *mut ffi::MDB_txn,
    _phantom: marker::PhantomData<T>,
}

impl<T> RoTxn<T> {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> Result<RoTxn<T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env,
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn, _phantom: marker::PhantomData })
    }

    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(self.txn)) };
        self.txn = ptr::null_mut();
        result.map_err(Into::into)
    }

    pub fn abort(self) -> Result<()> {
        abort_txn(self.txn)
    }
}

impl<T> Drop for RoTxn<T> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            let _ = abort_txn(self.txn);
        }
    }
}

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

pub struct RwTxn<'p, T=()> {
    pub(crate) txn: RoTxn<T>,
    _parent: marker::PhantomData<&'p mut ()>,
}

impl<T> RwTxn<'_, T> {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> Result<RwTxn<'static, T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe { mdb_result(ffi::mdb_txn_begin(env, ptr::null_mut(), 0, &mut txn))? };

        Ok(RwTxn {
            txn: RoTxn { txn, _phantom: marker::PhantomData },
            _parent: marker::PhantomData,
        })
    }

    pub(crate) fn nested<'p>(env: *mut ffi::MDB_env, parent: &'p mut RwTxn<T>) -> Result<RwTxn<'p, T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_ptr: *mut ffi::MDB_txn = parent.txn.txn;

        unsafe { mdb_result(ffi::mdb_txn_begin(env, parent_ptr, 0, &mut txn))? };

        Ok(RwTxn {
            txn: RoTxn { txn, _phantom: marker::PhantomData },
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

impl<T> Deref for RwTxn<'_, T> {
    type Target = RoTxn<T>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
