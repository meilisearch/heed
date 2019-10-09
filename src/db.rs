use std::{marker, mem};
use std::borrow::Cow;

use crate::{ZResult, Error, BytesDecode, BytesEncode, RoTxn, RwTxn};
use lmdb_sys as ffi;

#[derive(Copy, Clone)]
pub struct Database<KC, DC> {
    dbi: ffi::MDB_dbi,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database { dbi, marker: std::marker::PhantomData }
    }

    pub fn get<'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &KC::EItem,
    ) -> ZResult<Option<Cow<'txn, DC::DItem>>>
    where
        KC: BytesEncode,
        DC: BytesDecode<'txn>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
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

        let data = unsafe { crate::from_val(data_val.assume_init()) };
        let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

        Ok(Some(data))
    }

    pub fn put(
        &self,
        txn: &mut RwTxn,
        key: &KC::EItem,
        data: &DC::EItem,
    ) -> ZResult<()>
    where
        KC: BytesEncode,
        DC: BytesEncode,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = unsafe { crate::into_val(&data_bytes) };
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
