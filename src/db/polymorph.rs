use std::borrow::Cow;
use std::ops::{Bound, RangeBounds};
use std::{marker, mem, ptr};

use lmdb_sys as ffi;

use super::advance_key;
use crate::lmdb_error::lmdb_result;
use crate::*;

/// A polymorphic database that accepts types on call methods and not at creation.
///
/// # Example: Iterate over ranges of databases entries
///
/// In this example we store numbers in big endian this way those are ordered.
/// Thanks to their bytes representation, heed is able to iterate over them
/// from the lowest to the highest.
///
/// ```
/// # use std::fs;
/// # use heed::EnvOpenOptions;
/// use heed::PolyDatabase;
/// use heed::types::*;
/// use heed::{zerocopy::I64, byteorder::BigEndian};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all("target/zerocopy.mdb")?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
/// #     .max_dbs(3000)
/// #     .open("target/zerocopy.mdb")?;
/// type BEI64 = I64<BigEndian>;
///
/// let db: PolyDatabase = env.create_poly_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// # db.clear(&mut wtxn)?;
/// db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(0), &())?;
/// db.put::<OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(35), "thirty five")?;
/// db.put::<OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(42), "forty two")?;
/// db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(68), &())?;
///
/// // you can iterate over database entries in order
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let mut range = db.range::<OwnedType<BEI64>, Str, _>(&wtxn, range)?;
/// assert_eq!(range.next().transpose()?, Some((BEI64::new(35), "thirty five")));
/// assert_eq!(range.next().transpose()?, Some((BEI64::new(42), "forty two")));
/// assert_eq!(range.next().transpose()?, None);
///
/// drop(range);
/// wtxn.commit()?;
/// # Ok(()) }
/// ```
///
/// # Example: Selete ranges of entries
///
/// Discern also support ranges deletions.
/// Same configuration as above, numbers are ordered, therefore it is safe to specify
/// a range and be able to iterate over and/or delete it.
///
/// ```
/// # use std::fs;
/// # use heed::EnvOpenOptions;
/// use heed::PolyDatabase;
/// use heed::types::*;
/// use heed::{zerocopy::I64, byteorder::BigEndian};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all("target/zerocopy.mdb")?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
/// #     .max_dbs(3000)
/// #     .open("target/zerocopy.mdb")?;
/// type BEI64 = I64<BigEndian>;
///
/// let db: PolyDatabase = env.create_poly_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// # db.clear(&mut wtxn)?;
/// db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(0), &())?;
/// db.put::<OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(35), "thirty five")?;
/// db.put::<OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(42), "forty two")?;
/// db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(68), &())?;
///
/// // even delete a range of keys
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let deleted = db.delete_range::<OwnedType<BEI64>, Str,_ >(&mut wtxn, range)?;
/// assert_eq!(deleted, 2);
///
/// let rets: Result<_, _> = db.iter::<OwnedType<BEI64>, Unit>(&wtxn)?.collect();
/// let rets: Vec<(BEI64, _)> = rets?;
///
/// let expected = vec![
///     (BEI64::new(0), ()),
///     (BEI64::new(68), ()),
/// ];
///
/// assert_eq!(deleted, 2);
/// assert_eq!(rets, expected);
///
/// wtxn.commit()?;
/// # Ok(()) }
/// ```
#[derive(Copy, Clone)]
pub struct PolyDatabase {
    pub(crate) dbi: ffi::MDB_dbi,
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
            }
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
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Ok(Some((key, data))),
                (_, _) => Err(Error::Decoding),
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
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Ok(Some((key, data))),
                (_, _) => Err(Error::Decoding),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn len<'txn>(&self, txn: &'txn RoTxn) -> Result<usize> {
        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let mut count = 0;

        match cursor.move_on_first()? {
            Some(_) => count += 1,
            None => return Ok(0),
        }

        while let Some(_) = cursor.move_on_next()? {
            count += 1;
        }

        Ok(count)
    }

    pub fn is_empty<'txn>(&self, txn: &'txn RoTxn) -> Result<bool> {
        let mut cursor = RoCursor::new(txn, self.dbi)?;
        match cursor.move_on_first()? {
            Some(_) => Ok(false),
            None => Ok(true),
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

    pub fn range<'txn, KC, DC, R>(
        &self,
        txn: &'txn RoTxn,
        range: R,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RoRange {
            cursor: RoCursor::new(txn, self.dbi)?,
            start_bound: Some(start_bound),
            end_bound,
            _phantom: marker::PhantomData,
        })
    }

    pub fn range_mut<'txn, KC, DC, R>(
        &self,
        txn: &'txn mut RwTxn,
        range: R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).ok_or(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
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
        unsafe { lmdb_result(ffi::mdb_drop(txn.txn.txn, self.dbi, 0)).map_err(Into::into) }
    }
}
