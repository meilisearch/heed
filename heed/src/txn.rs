use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::{self, NonNull};

use crate::envs::Env;
use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::Result;

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
///
/// ## This struct is covariant
///
/// ```rust
/// #[allow(dead_code)]
/// trait CovariantMarker<'a>: 'static {
///     type R: 'a;
///
///     fn is_covariant(&'a self) -> &'a Self::R;
/// }
///
/// impl<'a, T> CovariantMarker<'a> for heed::RoTxn<'static, T> {
///     type R = heed::RoTxn<'a, T>;
///
///     fn is_covariant(&'a self) -> &'a heed::RoTxn<'a, T> {
///         self
///     }
/// }
/// ```
pub struct RoTxn<'e, T = WithTls> {
    /// Makes the struct covariant and !Sync
    pub(crate) txn: Option<NonNull<ffi::MDB_txn>>,
    env: Cow<'e, Env<T>>,
    _tls_marker: PhantomData<T>,
}

impl<'e, T> RoTxn<'e, T> {
    pub(crate) fn new(env: &'e Env<T>) -> Result<RoTxn<'e, T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr().as_mut(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn: NonNull::new(txn), env: Cow::Borrowed(env), _tls_marker: PhantomData })
    }

    pub(crate) fn static_read_txn(env: Env<T>) -> Result<RoTxn<'static, T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr().as_mut(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn: NonNull::new(txn), env: Cow::Owned(env), _tls_marker: PhantomData })
    }

    pub(crate) fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env> {
        self.env.env_mut_ptr()
    }

    /// Return the transaction's ID.
    ///
    /// This returns the identifier associated with this transaction. For a
    /// [`RoTxn`], this corresponds to the snapshot being read;
    /// concurrent readers will frequently have the same transaction ID.
    pub fn id(&self) -> usize {
        unsafe { ffi::mdb_txn_id(self.txn.unwrap().as_ptr()) }
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
        // Asserts that the transaction hasn't been already
        // committed/aborter and ensure we cannot use it twice.
        let mut txn = self.txn.take().unwrap();
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(txn.as_mut())) };
        result.map_err(Into::into)
    }
}

impl<T> Drop for RoTxn<'_, T> {
    fn drop(&mut self) {
        if let Some(mut txn) = self.txn.take() {
            // Asserts that the transaction hasn't been already
            // committed/aborter and ensure we cannot use it twice.
            unsafe { ffi::mdb_txn_abort(txn.as_mut()) }
        }
    }
}

/// Parameter defining that read transactions are opened with
/// Thread Local Storage (TLS) and cannot be sent between threads
/// `!Send`. It is often faster to open TLS-backed transactions.
///
/// When used to open transactions: A thread can only use one transaction
/// at a time, plus any child (nested) transactions. Each transaction belongs
/// to one thread. A `BadRslot` error will be thrown when multiple read
/// transactions exists on the same thread.
#[derive(Debug, PartialEq, Eq)]
pub enum WithTls {}

/// Parameter defining that read transactions are opened without
/// Thread Local Storage (TLS) and are therefore `Send`.
///
/// When used to open transactions: A thread can use any number
/// of read transactions at a time on the same thread. Read transactions
/// can be moved in between threads (`Send`).
#[derive(Debug, PartialEq, Eq)]
pub enum WithoutTls {}

/// Specificies if Thread Local Storage (TLS) must be used when
/// opening transactions. It is often faster to open TLS-backed
/// transactions but makes them `!Send`.
///
/// The `#MDB_NOTLS` flag is set on `Env` opening, `RoTxn`s and
/// iterators implements the `Send` trait. This allows the user to
/// move `RoTxn`s and iterators between threads as read transactions
/// will no more use thread local storage and will tie reader
/// locktable slots to transaction objects instead of to threads.
pub trait TlsUsage {
    /// True if TLS must be used, false otherwise.
    const ENABLED: bool;
}

impl TlsUsage for WithTls {
    const ENABLED: bool = true;
}

impl TlsUsage for WithoutTls {
    const ENABLED: bool = false;
}

/// Is sendable only if `MDB_NOTLS` has been used to open this transaction.
unsafe impl Send for RoTxn<'_, WithoutTls> {}

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
///
/// ## This struct is covariant
///
/// ```rust
/// #[allow(dead_code)]
/// trait CovariantMarker<'a>: 'static {
///     type T: 'a;
///
///     fn is_covariant(&'a self) -> &'a Self::T;
/// }
///
/// impl<'a> CovariantMarker<'a> for heed::RwTxn<'static> {
///     type T = heed::RwTxn<'a>;
///
///     fn is_covariant(&'a self) -> &'a heed::RwTxn<'a> {
///         self
///     }
/// }
/// ```
pub struct RwTxn<'p> {
    pub(crate) txn: RoTxn<'p, WithoutTls>,
}

impl<'p> RwTxn<'p> {
    pub(crate) fn new<T>(env: &'p Env<T>) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr().as_mut(),
                ptr::null_mut(),
                0,
                &mut txn,
            ))?
        };

        let env_without_tls = unsafe { env.as_without_tls() };

        Ok(RwTxn {
            txn: RoTxn {
                txn: NonNull::new(txn),
                env: Cow::Borrowed(env_without_tls),
                _tls_marker: PhantomData,
            },
        })
    }

    pub(crate) fn nested<T>(env: &'p Env<T>, parent: &'p mut RwTxn) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_ptr: *mut ffi::MDB_txn = unsafe { parent.txn.txn.unwrap().as_mut() };

        unsafe {
            mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr().as_mut(), parent_ptr, 0, &mut txn))?
        };

        let env_without_tls = unsafe { env.as_without_tls() };

        Ok(RwTxn {
            txn: RoTxn {
                txn: NonNull::new(txn),
                env: Cow::Borrowed(env_without_tls),
                _tls_marker: PhantomData,
            },
        })
    }

    pub(crate) fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env> {
        self.txn.env.env_mut_ptr()
    }

    /// Commit all the operations of a transaction into the database.
    /// The transaction is reset.
    pub fn commit(mut self) -> Result<()> {
        // Asserts that the transaction hasn't been already
        // committed/aborter and ensure we cannot use it two times.
        let mut txn = self.txn.txn.take().unwrap();
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(txn.as_mut())) };
        result.map_err(Into::into)
    }

    /// Abandon all the operations of the transaction instead of saving them.
    /// The transaction is reset.
    pub fn abort(mut self) {
        // Asserts that the transaction hasn't been already
        // committed/aborter and ensure we cannot use it twice.
        let mut txn = self.txn.txn.take().unwrap();
        unsafe { ffi::mdb_txn_abort(txn.as_mut()) }
    }
}

impl<'p> Deref for RwTxn<'p> {
    type Target = RoTxn<'p, WithoutTls>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

// TODO can't we just always implement it?
#[cfg(master3)]
impl std::ops::DerefMut for RwTxn<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}
