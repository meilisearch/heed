use std::borrow::Cow;
use std::ops::{Bound, RangeBounds};
use std::{marker, mem, ptr};

use lmdb_sys as ffi;

use super::advance_key;
use crate::lmdb_error::lmdb_result;
use crate::types::DecodeIgnore;
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
/// # use std::path::Path;
/// # use heed::EnvOpenOptions;
/// use heed::PolyDatabase;
/// use heed::types::*;
/// use heed::{zerocopy::I64, byteorder::BigEndian};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
/// #     .max_dbs(3000)
/// #     .open(Path::new("target").join("zerocopy.mdb"))?;
/// type BEI64 = I64<BigEndian>;
///
/// let db: PolyDatabase = env.create_poly_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// # db.clear(&mut wtxn)?;
/// db.put::<_, OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(0), &())?;
/// db.put::<_, OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(35), "thirty five")?;
/// db.put::<_, OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(42), "forty two")?;
/// db.put::<_, OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(68), &())?;
///
/// // you can iterate over database entries in order
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let mut range = db.range::<_, OwnedType<BEI64>, Str, _>(&wtxn, &range)?;
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
/// # use std::path::Path;
/// # use heed::EnvOpenOptions;
/// use heed::PolyDatabase;
/// use heed::types::*;
/// use heed::{zerocopy::I64, byteorder::BigEndian};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
/// #     .max_dbs(3000)
/// #     .open(Path::new("target").join("zerocopy.mdb"))?;
/// type BEI64 = I64<BigEndian>;
///
/// let db: PolyDatabase = env.create_poly_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// # db.clear(&mut wtxn)?;
/// db.put::<_, OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(0), &())?;
/// db.put::<_, OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(35), "thirty five")?;
/// db.put::<_, OwnedType<BEI64>, Str>(&mut wtxn, &BEI64::new(42), "forty two")?;
/// db.put::<_, OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(68), &())?;
///
/// // even delete a range of keys
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let deleted = db.delete_range::<_, OwnedType<BEI64>, _>(&mut wtxn, &range)?;
/// assert_eq!(deleted, 2);
///
/// let rets: Result<_, _> = db.iter::<_, OwnedType<BEI64>, Unit>(&wtxn)?.collect();
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

    /// Retrieves the value associated with a key.
    ///
    /// If the key does not exist, then `None` is returned.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// let db = env.create_poly_database(Some("get-poly-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, Str, OwnedType<i32>>(&mut wtxn, "i-am-forty-two", &42)?;
    /// db.put::<_, Str, OwnedType<i32>>(&mut wtxn, "i-am-twenty-seven", &27)?;
    ///
    /// let ret = db.get::<_, Str, OwnedType<i32>>(&wtxn, "i-am-forty-two")?;
    /// assert_eq!(ret, Some(42));
    ///
    /// let ret = db.get::<_, Str, OwnedType<i32>>(&wtxn, "i-am-twenty-one")?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get<'a, 'txn, T, KC, DC>(
        self,
        txn: &'txn RoTxn<T>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
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

    /// Retrieves the first key/value pair of this database.
    ///
    /// If the database if empty, then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("first-poly-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    ///
    /// let ret = db.first::<_, OwnedType<BEI32>, Str>(&wtxn)?;
    /// assert_eq!(ret, Some((BEI32::new(27), "i-am-twenty-seven")));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn first<'txn, T, KC, DC>(
        self,
        txn: &'txn RoTxn<T>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
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

    /// Retrieves the last key/value pair of this database.
    ///
    /// If the database if empty, then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("last-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    ///
    /// let ret = db.last::<_, OwnedType<BEI32>, Str>(&wtxn)?;
    /// assert_eq!(ret, Some((BEI32::new(42), "i-am-forty-two")));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn last<'txn, T, KC, DC>(
        self,
        txn: &'txn RoTxn<T>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
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

    /// Returns the number of elements in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 4);
    ///
    /// db.delete::<_, OwnedType<BEI32>>(&mut wtxn, &BEI32::new(27))?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 3);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn len<'txn, T>(self, txn: &'txn RoTxn<T>) -> Result<usize> {
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

    /// Returns `true` if and only if this database is empty.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.is_empty(&wtxn)?;
    /// assert_eq!(ret, false);
    ///
    /// db.clear(&mut wtxn)?;
    ///
    /// let ret = db.is_empty(&wtxn)?;
    /// assert_eq!(ret, true);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn is_empty<'txn, T>(self, txn: &'txn RoTxn<T>) -> Result<bool> {
        let mut cursor = RoCursor::new(txn, self.dbi)?;
        match cursor.move_on_first()? {
            Some(_) => Ok(false),
            None => Ok(true),
        }
    }

    /// Return a lexicographically ordered iterator of all key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    ///
    /// let mut iter = db.iter::<_, OwnedType<BEI32>, Str>(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn iter<'txn, T, KC, DC>(self, txn: &'txn RoTxn<T>) -> Result<RoIter<'txn, KC, DC>> {
        Ok(RoIter {
            cursor: RoCursor::new(txn, self.dbi)?,
            move_on_first: true,
            _phantom: marker::PhantomData,
        })
    }

    /// Return a mutable lexicographically ordered iterator of all key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    ///
    /// let mut iter = db.iter_mut::<_, OwnedType<BEI32>, Str>(&mut wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// let ret = iter.del_current()?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = iter.put_current(&BEI32::new(42), "i-am-the-new-forty-two")?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get::<_, OwnedType<BEI32>, Str>(&wtxn, &BEI32::new(13))?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get::<_, OwnedType<BEI32>, Str>(&wtxn, &BEI32::new(42))?;
    /// assert_eq!(ret, Some("i-am-the-new-forty-two"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn iter_mut<'txn, T, KC, DC>(
        self,
        txn: &'txn mut RwTxn<T>,
    ) -> Result<RwIter<'txn, KC, DC>> {
        Ok(RwIter {
            cursor: RwCursor::new(txn, self.dbi)?,
            move_on_first: true,
            _phantom: marker::PhantomData,
        })
    }

    /// Return a lexicographically ordered iterator of a range of key-value pairs in this database.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let mut iter = db.range::<_, OwnedType<BEI32>, Str, _>(&wtxn, &range)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn range<'a, 'txn, T, KC, DC, R>(
        self,
        txn: &'txn RoTxn<T>,
        range: &'a R,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
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

    /// Return a mutable lexicographically ordered iterator of a range of
    /// key-value pairs in this database.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let mut range = db.range_mut::<_, OwnedType<BEI32>, Str, _>(&mut wtxn, &range)?;
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// let ret = range.del_current()?;
    /// assert!(ret);
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = range.put_current(&BEI32::new(42), "i-am-the-new-forty-two")?;
    /// assert!(ret);
    ///
    /// assert_eq!(range.next().transpose()?, None);
    /// drop(range);
    ///
    ///
    /// let mut iter = db.iter::<_, OwnedType<BEI32>, Str>(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-the-new-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(521), "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn range_mut<'a, 'txn, T, KC, DC, R>(
        self,
        txn: &'txn mut RwTxn<T>,
        range: &'a R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
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

    /// Return a lexicographically ordered iterator of all key-value pairs
    /// in this database that starts with the given prefix.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-forty-one",    &BEI32::new(41))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.prefix_iter::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter<'a, 'txn, T, KC, DC>(
        self,
        txn: &'txn RoTxn<T>,
        prefix: &'a KC::EItem,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
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

    /// Return a mutable lexicographically ordered iterator of all key-value pairs
    /// in this database that starts with the given prefix.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-forty-one",    &BEI32::new(41))?;
    /// db.put::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.prefix_iter_mut::<_, Str, OwnedType<BEI32>>(&mut wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// let ret = iter.del_current()?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// let ret = iter.put_current("i-am-twenty-seven", &BEI32::new(27000))?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get::<_, Str, OwnedType<BEI32>>(&wtxn, "i-am-twenty-eight")?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get::<_, Str, OwnedType<BEI32>>(&wtxn, "i-am-twenty-seven")?;
    /// assert_eq!(ret, Some(BEI32::new(27000)));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter_mut<'a, 'txn, T, KC, DC>(
        self,
        txn: &'txn RwTxn<T>,
        prefix: &'a KC::EItem,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
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

    /// Insert a key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn put<'a, T, KC, DC>(
        self,
        txn: &mut RwTxn<T>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
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

    /// Append the given key/data pair to the end of the database.
    ///
    /// This option allows fast bulk loading when keys are already known to be in the correct order.
    /// Loading unsorted keys will cause a MDB_KEYEXIST error.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("append-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn append<'a, T, KC, DC>(
        self,
        txn: &mut RwTxn<T>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = unsafe { crate::into_val(&data_bytes) };
        let flags = lmdb_sys::MDB_APPEND;

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

    /// Deletes a key-value pairs in this database.
    ///
    /// If the key does not exist, then `false` is returned.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.delete::<_, OwnedType<BEI32>>(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, true);
    ///
    /// let ret = db.get::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.delete::<_, OwnedType<BEI32>>(&mut wtxn, &BEI32::new(467))?;
    /// assert_eq!(ret, false);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn delete<'a, T, KC>(self, txn: &mut RwTxn<T>, key: &'a KC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
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

    /// Deletes a range of key-value pairs in this database.
    ///
    /// Perfer using [`clear`] instead of a call to this method with a full range ([`..`]).
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// [`clear`]: crate::Database::clear
    /// [`..`]: std::ops::RangeFull
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let ret = db.delete_range::<_, OwnedType<BEI32>, _>(&mut wtxn, &range)?;
    /// assert_eq!(ret, 2);
    ///
    ///
    /// let mut iter = db.iter::<_, OwnedType<BEI32>, Str>(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(521), "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn delete_range<'a, 'txn, T, KC, R>(
        self,
        txn: &'txn mut RwTxn<T>,
        range: &'a R,
    ) -> Result<usize>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        let mut count = 0;
        let mut iter = self.range_mut::<T, KC, DecodeIgnore, _>(txn, range)?;

        while let Some(_) = iter.next() {
            iter.del_current()?;
            count += 1;
        }

        Ok(count)
    }

    /// Deletes all key/value pairs in this database.
    ///
    /// Perfer using this method instead of a call to [`delete_range`] with a full range ([`..`]).
    ///
    /// [`delete_range`]: crate::Database::delete_range
    /// [`..`]: std::ops::RangeFull
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db = env.create_poly_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put::<_, OwnedType<BEI32>, Str>(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// db.clear(&mut wtxn)?;
    ///
    /// let ret = db.is_empty(&wtxn)?;
    /// assert!(ret);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn clear<T>(self, txn: &mut RwTxn<T>) -> Result<()> {
        unsafe { lmdb_result(ffi::mdb_drop(txn.txn.txn, self.dbi, 0)).map_err(Into::into) }
    }
}
