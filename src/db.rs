use std::{marker, mem};
use std::borrow::Cow;
use std::ops::{RangeBounds, Bound};

use crate::lmdb_error::lmdb_result;
use crate::*;

pub struct Database<KC, DC> {
    pub(crate) dbi: ffi::MDB_dbi,
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
    ) -> Result<Option<DC::DItem>>
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

        if let Err(error) = result {
            if error.not_found() {
                return Ok(None)
            } else {
                return Err(error.into())
            }
        }

        let data = unsafe { crate::from_val(data_val.assume_init()) };
        let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

        Ok(Some(data))
    }

    pub fn iter<'txn>(&self, txn: &'txn RoTxn) -> Result<RoIter<'txn, KC, DC>> {
        Ok(RoIter {
            cursor: RoCursor::new(txn, *self)?,
            move_on_first: true,
            _phantom: marker::PhantomData,
        })
    }

    pub fn range<'txn, R>(&self, txn: &'txn RoTxn, range: R) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        let start_bound = match range.start_bound() {
            Bound::Included(bound) => Bound::Included(KC::bytes_encode(bound).unwrap().into_owned()),
            Bound::Excluded(bound) => Bound::Excluded(KC::bytes_encode(bound).unwrap().into_owned()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => Bound::Included(KC::bytes_encode(bound).unwrap().into_owned()),
            Bound::Excluded(bound) => Bound::Excluded(KC::bytes_encode(bound).unwrap().into_owned()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RoRange {
            cursor: RoCursor::new(txn, *self)?,
            start_bound: Some(start_bound),
            end_bound,
            _phantom: marker::PhantomData,
        })
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
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Database<KC, DC> {
        Database::new(self.dbi)
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}

pub struct RoIter<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> Iterator for RoIter<'txn, KC, DC>
where KC: BytesDecode<'txn>,
      DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_first()
        } else {
            self.cursor.move_on_next()
        };

        match result {
            Ok(Some((key, data))) => {
                match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                    (Some(key), Some(data)) => Some(Ok((key, data))),
                    (_, _) => Some(Err(Error::Decoding)),
                }
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

fn advance_key(bytes: &mut Vec<u8>) {
    match bytes.last_mut() {
        Some(&mut 255) | None => bytes.push(0),
        Some(last) => *last += 1,
    }
}

pub struct RoRange<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    start_bound: Option<Bound<Vec<u8>>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> Iterator for RoRange<'txn, KC, DC>
where KC: BytesDecode<'txn>,
      DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.start_bound.take() {
            Some(Bound::Included(start)) => self.cursor.move_on_key_greater_than_or_equal_to(&start),
            Some(Bound::Excluded(mut start)) => {
                advance_key(&mut start);
                self.cursor.move_on_key_greater_than_or_equal_to(&start)
            },
            Some(Bound::Unbounded) => self.cursor.move_on_first(),
            None => self.cursor.move_on_next(),
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match self.end_bound {
                    Bound::Included(ref end) => key <= end,
                    Bound::Excluded(ref end) => key < end,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
                    }
                } else {
                    None
                }
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
