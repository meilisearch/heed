use std::ops::Deref;
use std::ptr;

use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::{Env, Result};

/// A read-only transaction.
///
/// ## LMDB Limitations
///
/// It's a must to keep read transactions short-lived.
///
/// Active Read transactions prevent the reuse of pages freed
/// by newer write transactions, thus the database can grow quickly.
///
/// ## OSX/Darwin Limitation
///
/// At least 10 transactions can be active at the same time in the same process, since only 10 POSIX semaphores can
/// be active at the same time for a process. Threads are in the same process space.
///
/// If the process crashes in the POSIX semaphore locking section of the transaction, the semaphore will be kept locked.
///
/// Note: if your program already use POSIX semaphores, you will have less available for heed/LMDB!
///
/// You may increase the limit by editing it **at your own risk**: `/Library/LaunchDaemons/sysctl.plist`
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

    /// Create a nested transaction with read only access for use with the environment.
    ///
    /// The new transaction will be a nested transaction, with the current transaction
    /// as its parent. Transactions may be nested to any level.
    ///
    /// A parent transaction and its cursors may not issue any other operations than _commit_ and
    /// _abort_ while it has active child transactions.
    pub fn nested_read_txn(&self) -> Result<RoTxn> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let env = self.env;
        let parent_ptr: *mut ffi::MDB_txn = self.txn;

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr(),
                parent_ptr,
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn, env })
    }

    pub(crate) fn env_mut_ptr(&self) -> *mut ffi::MDB_env {
        self.env.env_mut_ptr()
    }

    /// Commit a read transaction.
    ///
    /// Synchronizing some [`Env`] metadata with the global handle.
    ///
    /// ## LMDB
    ///
    /// It's mandatory in a multi-process setup to call [`RoTxn::commit`] upon read-only database opening.
    /// After the transaction opening, the database is dropped. The next transaction might return
    /// `Io(Os { code: 22, kind: InvalidInput, message: "Invalid argument" })` known as `EINVAL`.
    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(self.txn)) };
        self.txn = ptr::null_mut();
        result.map_err(Into::into)
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            abort_txn(self.txn);
        }
    }
}

#[cfg(feature = "read-txn-no-tls")]
unsafe impl Send for RoTxn<'_> {}

fn abort_txn(txn: *mut ffi::MDB_txn) {
    // Asserts that the transaction hasn't been already committed.
    assert!(!txn.is_null());
    unsafe { ffi::mdb_txn_abort(txn) }
}

/// A read-write transaction.
///
/// ## LMDB Limitations
///
/// Only one [`RwTxn`] may exist in the same environment at the same time.
/// If two exist, the new one may wait on a mutex for [`RwTxn::commit`] or [`RwTxn::abort`] to
/// be called for the first one.
///
/// ## OSX/Darwin Limitation
///
/// At least 10 transactions can be active at the same time in the same process, since only 10 POSIX semaphores can
/// be active at the same time for a process. Threads are in the same process space.
///
/// If the process crashes in the POSIX semaphore locking section of the transaction, the semaphore will be kept locked.
///
/// Note: if your program already use POSIX semaphores, you will have less available for heed/LMDB!
///
/// You may increase the limit by editing it **at your own risk**: `/Library/LaunchDaemons/sysctl.plist`
pub struct RwTxn<'p> {
    pub(crate) txn: RoTxn<'p>,
}

impl<'p> RwTxn<'p> {
    pub(crate) fn new(env: &'p Env) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe { mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr(), ptr::null_mut(), 0, &mut txn))? };

        Ok(RwTxn { txn: RoTxn { txn, env } })
    }

    /// Create a nested transaction with read and write access for use with the environment.
    ///
    /// The new transaction will be a nested transaction, with the transaction indicated by parent
    /// as its parent. Transactions may be nested to any level.
    ///
    /// A parent transaction and its cursors may not issue any other operations than _commit_ and
    /// _abort_ while it has active child transactions.
    pub fn nested_write_txn(&mut self) -> Result<RwTxn> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let env = self.txn.env;
        let parent_ptr: *mut ffi::MDB_txn = self.txn.txn;

        unsafe { mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr(), parent_ptr, 0, &mut txn))? };

        Ok(RwTxn { txn: RoTxn { txn, env } })
    }

    pub(crate) fn env_mut_ptr(&self) -> *mut ffi::MDB_env {
        self.txn.env.env_mut_ptr()
    }

    /// Commit all the operations of a transaction into the database.
    /// The transaction is reset.
    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(self.txn.txn)) };
        self.txn.txn = ptr::null_mut();
        result.map_err(Into::into)
    }

    /// Abandon all the operations of the transaction instead of saving them.
    /// The transaction is reset.
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
