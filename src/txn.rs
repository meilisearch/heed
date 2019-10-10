use std::ops::Deref;
use std::ptr;

pub struct RoTxn {
    pub txn: *mut ffi::MDB_txn,
}

impl RoTxn {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> RoTxn {
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

        RoTxn { txn }
    }

    pub fn abort(self) {
        drop(self)
    }
}

impl Drop for RoTxn {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            unsafe { ffi::mdb_txn_abort(self.txn) }
            self.txn = ptr::null_mut();
        }
    }
}

pub struct RwTxn {
    pub txn: RoTxn,
}

impl RwTxn {
    pub(crate) fn new(env: *mut ffi::MDB_env) -> RwTxn {
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

        RwTxn { txn: RoTxn { txn } }
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

impl Deref for RwTxn {
    type Target = RoTxn;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}
