use std::{marker, mem};
use std::borrow::Cow;

use crate::*;

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
    ) -> Result<Option<Cow<'txn, DC::DItem>>>
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

    pub fn iter<'txn>(&self, txn: &'txn RoTxn) -> RoIter<'txn, KC, DC> {
        RoIter {
            cursor: RoCursor::new(txn, *self),
            init_op: Some(InitOp::MoveOnFirst),
            _phantom: marker::PhantomData,
        }
    }

    pub fn put(
        &self,
        txn: &mut RwTxn,
        key: &KC::EItem,
        data: &DC::EItem,
    ) -> Result<()>
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

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Database<KC, DC> {
        Database::new(self.dbi)
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}

enum InitOp {
    MoveOnFirst,
    GetCurrent,
}

pub struct RoIter<'txn, KC, DC> {
    cursor: RoCursor<'txn, KC, DC>,
    init_op: Option<InitOp>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> Iterator for RoIter<'txn, KC, DC>
where KC: BytesDecode<'txn>,
      DC: BytesDecode<'txn>,
{
    type Item = Result<(Cow<'txn, KC::DItem>, Cow<'txn, DC::DItem>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.init_op.take() {
            Some(InitOp::MoveOnFirst) => self.cursor.move_on_first(),
            Some(InitOp::GetCurrent) => self.cursor.get_current(),
            None => self.cursor.move_on_next(),
        };

        result.transpose()
    }
}
