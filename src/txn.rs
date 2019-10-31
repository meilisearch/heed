use std::marker;
use std::ops::Deref;
use std::ptr;

use lmdb_sys as ffi;

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

    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { lmdb_result(ffi::mdb_txn_commit(self.txn)) };
        self.txn = ptr::null_mut();
        result.map_err(Into::into)
    }

    pub fn abort(self) {}
}

impl Drop for RoTxn {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            unsafe { ffi::mdb_txn_abort(self.txn) }
        }
    }
}

pub struct RwTxn<'p> {
    pub(crate) txn: RoTxn,
    _parent: marker::PhantomData<&'p mut ()>,
}

impl RwTxn<'_> {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> Result<RwTxn<'static>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe { lmdb_result(ffi::mdb_txn_begin(env, ptr::null_mut(), 0, &mut txn))? };

        Ok(RwTxn {
            txn: RoTxn { txn },
            _parent: marker::PhantomData,
        })
    }

    pub(crate) fn nested<'p>(env: *mut ffi::MDB_env, parent: &'p mut RwTxn) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_ptr: *mut ffi::MDB_txn = parent.txn.txn;

        unsafe { lmdb_result(ffi::mdb_txn_begin(env, parent_ptr, 0, &mut txn))? };

        Ok(RwTxn {
            txn: RoTxn { txn },
            _parent: marker::PhantomData,
        })
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    pub fn abort(self) {}
}

impl Deref for RwTxn<'_> {
    type Target = RoTxn;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
