use std::ops::Deref;
use std::ptr;
use lmdb_sys as ffi;

pub struct TxnRead {
    pub txn: *mut ffi::MDB_txn,
}

impl TxnRead {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> TxnRead {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        let ret = unsafe {
            ffi::mdb_txn_begin(
                env,
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            )
        };

        assert_eq!(ret, 0);

        TxnRead { txn }
    }

    pub fn abort(self) {
        drop(self)
    }
}

impl Drop for TxnRead {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            unsafe { ffi::mdb_txn_abort(self.txn) }
            self.txn = ptr::null_mut();
        }
    }
}

pub struct TxnWrite {
    pub txn: TxnRead,
}

impl TxnWrite {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> TxnWrite {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        let ret = unsafe {
            ffi::mdb_txn_begin(
                env,
                ptr::null_mut(),
                0,
                &mut txn,
            )
        };

        assert_eq!(ret, 0);

        TxnWrite { txn: TxnRead { txn } }
    }

    pub fn commit(mut self) {
        let ret = unsafe { ffi::mdb_txn_commit(self.txn.txn) };
        assert_eq!(ret, 0);
        self.txn.txn = ptr::null_mut();
    }

    pub fn abort(self) {
        drop(self)
    }
}

impl Deref for TxnWrite {
    type Target = TxnRead;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
