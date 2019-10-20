use std::{marker, mem, ptr};
use std::borrow::Cow;
use std::ops::{RangeBounds, Bound};

use crate::*;
use crate::lmdb_error::lmdb_result;
use super::advance_key;

/// A dynamically typed database that accepts types at call (e.g. `get`, `put`).
#[derive(Copy, Clone)]
pub struct PolyDatabase {
    pub(crate) dbi: ffi::MDB_dbi
}

impl PolyDatabase {
    pub(crate) fn new(dbi: ffi::MDB_dbi) -> PolyDatabase {
        PolyDatabase { dbi }
    }

    pub fn get<'txn, KC, DC>(&self, txn: &'txn RoTxn, key: &KC::EItem) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode,
        DC: BytesDecode<'txn>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = mem::MaybeUninit::uninit();

        let result = unsafe {
            lmdb_result(ffi::mdb_get(
                txn.txn,
                self.dbi,
                &mut key_val,
                data_val.as_mut_ptr(),
            ))
        };

        match result {
            Ok(()) => {
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;
                Ok(Some(data))
            },
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn first<'txn, KC, DC>(&self, txn: &'txn RoTxn) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let mut cursor = RoCursor::new(txn, self.dbi)?;
        match cursor.move_on_first() {
            Ok(Some((key, data))) => {
                match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                    (Some(key), Some(data)) => Ok(Some((key, data))),
                    (_, _) => Err(Error::Decoding),
                }
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn last<'txn, KC, DC>(&self, txn: &'txn RoTxn) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let mut cursor = RoCursor::new(txn, self.dbi)?;
        match cursor.move_on_last() {
            Ok(Some((key, data))) => {
                match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                    (Some(key), Some(data)) => Ok(Some((key, data))),
                    (_, _) => Err(Error::Decoding),
                }
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn iter<'txn, KC, DC>(&self, txn: &'txn RoTxn) -> Result<RoIter<'txn, KC, DC>> {
        Ok(RoIter {
            cursor: RoCursor::new(txn, self.dbi)?,
            move_on_first: true,
            _phantom: marker::PhantomData,
        })
    }

    pub fn iter_mut<'txn, KC, DC>(&self, txn: &'txn mut RwTxn) -> Result<RwIter<'txn, KC, DC>> {
        Ok(RwIter {
            cursor: RwCursor::new(txn, self.dbi)?,
            move_on_first: true,
            _phantom: marker::PhantomData,
        })
    }

    pub fn range<'txn, KC, DC, R>(&self, txn: &'txn RoTxn, range: R) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            },
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            },
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            },
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            },
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RoRange {
            cursor: RoCursor::new(txn, self.dbi)?,
            start_bound: Some(start_bound),
            end_bound,
            _phantom: marker::PhantomData,
        })
    }

    pub fn range_mut<'txn, KC, DC, R>(&self, txn: &'txn mut RwTxn, range: R) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            },
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            },
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            },
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            },
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RwRange {
            cursor: RwCursor::new(txn, self.dbi)?,
            start_bound: Some(start_bound),
            end_bound,
            _phantom: marker::PhantomData,
        })
    }

    pub fn prefix_iter<'txn, KC, DC>(
        &self,
        txn: &'txn RoTxn,
        prefix: &KC::EItem,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        let prefix_bytes = KC::bytes_encode(prefix).ok_or(Error::Encoding)?;

        let start_bytes = prefix_bytes.into_owned();

        let mut end_bytes = start_bytes.clone();
        advance_key(&mut end_bytes);

        let end_bound = Bound::Excluded(end_bytes);
        let start_bound = Bound::Included(start_bytes);

        Ok(RoRange {
            cursor: RoCursor::new(txn, self.dbi)?,
            start_bound: Some(start_bound),
            end_bound,
            _phantom: marker::PhantomData,
        })
    }

    pub fn prefix_iter_mut<'txn, KC, DC>(
        &self,
        txn: &'txn RwTxn,
        prefix: &KC::EItem,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        let prefix_bytes = KC::bytes_encode(prefix).ok_or(Error::Encoding)?;

        let start_bytes = prefix_bytes.into_owned();

        let mut end_bytes = start_bytes.clone();
        advance_key(&mut end_bytes);

        let end_bound = Bound::Excluded(end_bytes);
        let start_bound = Bound::Included(start_bytes);

        Ok(RwRange {
            cursor: RwCursor::new(txn, self.dbi)?,
            start_bound: Some(start_bound),
            end_bound,
            _phantom: marker::PhantomData,
        })
    }

    pub fn put<KC, DC>(&self, txn: &mut RwTxn, key: &KC::EItem, data: &DC::EItem) -> Result<()>
    where
        KC: BytesEncode,
        DC: BytesEncode,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = unsafe { crate::into_val(&data_bytes) };
        let flags = 0;

        unsafe {
            lmdb_result(ffi::mdb_put(
                txn.txn.txn,
                self.dbi,
                &mut key_val,
                &mut data_val,
                flags,
            ))?
        }

        Ok(())
    }

    pub fn delete<KC>(&self, txn: &mut RwTxn, key: &KC::EItem) -> Result<bool>
    where
        KC: BytesEncode,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let mut key_val = unsafe { crate::into_val(&key_bytes) };

        let result = unsafe {
            lmdb_result(ffi::mdb_del(
                txn.txn.txn,
                self.dbi,
                &mut key_val,
                ptr::null_mut(),
            ))
        };

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn delete_range<'txn, KC, DC, R>(&self, txn: &'txn mut RwTxn, range: R) -> Result<usize>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        let mut count = 0;
        let mut iter = self.range_mut::<KC, DC, _>(txn, range)?;

        while let Some(_) = iter.next() {
            iter.del_current()?;
            count += 1;
        }

        Ok(count)
    }

    pub fn clear(&self, txn: &mut RwTxn) -> Result<()> {
        unsafe {
            lmdb_result(ffi::mdb_drop(
                txn.txn.txn,
                self.dbi,
                0,
            ))
            .map_err(Into::into)
        }
    }
}
