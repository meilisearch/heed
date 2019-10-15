use std::{marker, mem, ptr};
use std::borrow::Cow;
use std::ops::{RangeBounds, Bound};

use crate::lmdb_error::lmdb_result;
use crate::*;

/// A typed database that accepts only the types it was created with.
///
/// # Example: iterating over entries
///
/// ```
/// # use std::fs;
/// # use zerocopy_lmdb::EnvOpenOptions;
/// use zerocopy_lmdb::Database;
/// use zerocopy_lmdb::types::*;
/// use serde::{Serialize, Deserialize};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all("target/zerocopy.mdb")?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
/// #     .max_dbs(3000)
/// #     .open("target/zerocopy.mdb")?;
/// type BEI64 = zerocopy::I64<byteorder::BigEndian>;
///
/// let db: Database<OwnedType<BEI64>, Unit> = env.create_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// db.put(&mut wtxn, &BEI64::new(0), &())?;
/// db.put(&mut wtxn, &BEI64::new(68), &())?;
/// db.put(&mut wtxn, &BEI64::new(35), &())?;
/// db.put(&mut wtxn, &BEI64::new(42), &())?;
///
/// // you can iterate over database entries in order
/// let rets: Result<Vec<(BEI64, _)>, _> = db.iter(&wtxn)?.collect();
/// let rets: Vec<(BEI64, _)> = rets?;
///
/// let expected = vec![
///     (BEI64::new(0), ()),
///     (BEI64::new(35), ()),
///     (BEI64::new(42), ()),
///     (BEI64::new(68), ()),
/// ];
///
/// assert_eq!(rets, expected);
/// wtxn.abort();
/// # Ok(()) }
/// ```
///
/// # Example: iterating over and delete ranges of entries
///
/// ```
/// # use std::fs;
/// # use zerocopy_lmdb::EnvOpenOptions;
/// use zerocopy_lmdb::Database;
/// use zerocopy_lmdb::types::*;
/// use serde::{Serialize, Deserialize};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all("target/zerocopy.mdb")?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
/// #     .max_dbs(3000)
/// #     .open("target/zerocopy.mdb")?;
/// type BEI64 = zerocopy::I64<byteorder::BigEndian>;
///
/// let db: Database<OwnedType<BEI64>, Unit> = env.create_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// db.put(&mut wtxn, &BEI64::new(0), &())?;
/// db.put(&mut wtxn, &BEI64::new(68), &())?;
/// db.put(&mut wtxn, &BEI64::new(35), &())?;
/// db.put(&mut wtxn, &BEI64::new(42), &())?;
///
/// // you can iterate over ranges too!!!
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let rets: Result<Vec<(BEI64, _)>, _> = db.range(&wtxn, range)?.collect();
/// let rets: Vec<(BEI64, _)> = rets?;
///
/// let expected = vec![
///     (BEI64::new(35), ()),
///     (BEI64::new(42), ()),
/// ];
///
/// assert_eq!(rets, expected);
///
///
/// // even delete a range of keys
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let deleted: usize = db.delete_range(&mut wtxn, range)?;
///
/// let rets: Result<Vec<(BEI64, _)>, _> = db.iter(&wtxn)?.collect();
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
/// wtxn.abort();
/// # Ok(()) }
/// ```
pub struct Database<KC, DC> {
    pub(crate) dbi: ffi::MDB_dbi,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database { dbi, marker: std::marker::PhantomData }
    }

    pub fn get<'txn>(&self, txn: &'txn RoTxn, key: &KC::EItem) -> Result<Option<DC::DItem>>
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

    pub fn iter<'txn>(&self, txn: &'txn RoTxn) -> Result<RoIter<'txn, KC, DC>> {
        Ok(RoIter {
            cursor: RoCursor::new(txn, self.dbi)?,
            move_on_first: true,
            _phantom: marker::PhantomData,
        })
    }

    pub fn iter_mut<'txn>(&self, txn: &'txn mut RwTxn) -> Result<RwIter<'txn, KC, DC>> {
        Ok(RwIter {
            cursor: RwCursor::new(txn, self.dbi)?,
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

    pub fn range_mut<'txn, R>(&self, txn: &'txn mut RwTxn, range: R) -> Result<RwRange<'txn, KC, DC>>
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

    pub fn put(&self, txn: &mut RwTxn, key: &KC::EItem, data: &DC::EItem) -> Result<()>
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

    pub fn delete(&self, txn: &mut RwTxn, key: &KC::EItem) -> Result<bool>
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

    pub fn delete_range<'txn, R>(&self, txn: &'txn mut RwTxn, range: R) -> Result<usize>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        let mut count = 0;
        let mut iter = self.range_mut(txn, range)?;

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

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Database<KC, DC> {
        Database::new(self.dbi)
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}
