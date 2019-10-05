use std::{io, marker, mem, ptr, slice, str};
use std::borrow::Cow;
use zerocopy::{LayoutVerified, AsBytes, FromBytes};
use lmdb_sys as ffi;

mod traits;
mod types;

pub use self::traits::{BytesEncode, BytesDecode};
pub use self::types::{Type, Slice, Str, Ignore, Serde};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Encoding,
    Decoding,
}

pub type ZResult<T> = Result<T, Error>;

unsafe fn into_val(value: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val { mv_size: value.len(), mv_data: value.as_ptr() as *mut libc::c_void }
}

unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}

pub struct TxnRead {
    pub txn: *mut ffi::MDB_txn,
}

impl TxnRead {
    pub fn new(env: *mut ffi::MDB_env) -> TxnRead {
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
    pub fn new(env: *mut ffi::MDB_env) -> TxnWrite {
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

impl std::ops::Deref for TxnWrite {
    type Target = TxnRead;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

pub struct Database<KC, DC> {
    dbi: ffi::MDB_dbi,
    marker: std::marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub fn new(dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database { dbi, marker: std::marker::PhantomData }
    }
}

impl<KC, DC> Database<KC, DC> {
    pub fn get<'txn>(
        &self,
        txn: &'txn TxnRead,
        key: &KC::Item,
    ) -> ZResult<Option<Cow<'txn, DC::Item>>>
    where
        KC: BytesEncode,
        DC: BytesDecode,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { into_val(&key_bytes) };
        let mut data_val = mem::MaybeUninit::uninit();

        let ret = unsafe {
            ffi::mdb_get(
                txn.txn,
                self.dbi,
                &mut key_val,
                data_val.as_mut_ptr(),
            )
        };

        if ret == ffi::MDB_NOTFOUND { return Ok(None) }

        assert_eq!(ret, 0);

        let data = unsafe { from_val(data_val.assume_init()) };
        let data = DC::bytes_decode(data).ok_or(Error::Decoding).unwrap();

        Ok(Some(data))
    }

    pub fn put(
        &self,
        txn: &mut TxnWrite,
        key: &KC::Item,
        data: &DC::Item,
    ) -> ZResult<()>
    where
        KC: BytesEncode,
        DC: BytesEncode,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { into_val(&key_bytes) };
        let mut data_val = unsafe { into_val(&data_bytes) };
        let flags = 0;

        let ret = unsafe {
            ffi::mdb_put(
                txn.txn.txn,
                self.dbi,
                &mut key_val,
                &mut data_val,
                flags,
            )
        };

        assert_eq!(ret, 0);

        Ok(())
    }
}
