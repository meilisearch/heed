use std::{marker, mem, slice};
use std::borrow::Cow;

use crate::{ZResult, Error, BytesDecode, BytesEncode, TxnRead, TxnWrite};
use lmdb_sys as ffi;

unsafe fn into_val(value: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val { mv_size: value.len(), mv_data: value.as_ptr() as *mut libc::c_void }
}

unsafe fn from_val<'a>(value: ffi::MDB_val) -> &'a [u8] {
    slice::from_raw_parts(value.mv_data as *const u8, value.mv_size)
}

#[derive(Copy, Clone)]
pub struct Database<KC, DC> {
    dbi: ffi::MDB_dbi,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(dbi: ffi::MDB_dbi) -> Database<KC, DC> {
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
