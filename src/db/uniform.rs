use crate::*;
use std::marker;
use std::ops::RangeBounds;

/// A typed database that accepts only the types it was created with.
///
/// # Example: Iterate over databases entries
///
/// In this example we store numbers in big endian this way those are ordered.
/// Thanks to their bytes representation, heed is able to iterate over them
/// from the lowest to the highest.
///
/// ```
/// # use std::fs;
/// # use heed::EnvOpenOptions;
/// use heed::Database;
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
/// let db: Database<OwnedType<BEI64>, Unit> = env.create_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// # db.clear(&mut wtxn)?;
/// db.put(&mut wtxn, &BEI64::new(68), &())?;
/// db.put(&mut wtxn, &BEI64::new(35), &())?;
/// db.put(&mut wtxn, &BEI64::new(0), &())?;
/// db.put(&mut wtxn, &BEI64::new(42), &())?;
///
/// // you can iterate over database entries in order
/// let rets: Result<_, _> = db.iter(&wtxn)?.collect();
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
/// wtxn.commit()?;
/// # Ok(()) }
/// ```
///
/// # Example: Iterate over and delete ranges of entries
///
/// Discern also support ranges and ranges deletions.
/// Same configuration as above, numbers are ordered, therefore it is safe to specify
/// a range and be able to iterate over and/or delete it.
///
/// ```
/// # use std::fs;
/// # use heed::EnvOpenOptions;
/// use heed::Database;
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
/// let db: Database<OwnedType<BEI64>, Unit> = env.create_database(Some("big-endian-iter"))?;
///
/// let mut wtxn = env.write_txn()?;
/// # db.clear(&mut wtxn)?;
/// db.put(&mut wtxn, &BEI64::new(0), &())?;
/// db.put(&mut wtxn, &BEI64::new(68), &())?;
/// db.put(&mut wtxn, &BEI64::new(35), &())?;
/// db.put(&mut wtxn, &BEI64::new(42), &())?;
///
/// // you can iterate over ranges too!!!
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let rets: Result<_, _> = db.range(&wtxn, range)?.collect();
/// let rets: Vec<(BEI64, _)> = rets?;
///
/// let expected = vec![
///     (BEI64::new(35), ()),
///     (BEI64::new(42), ()),
/// ];
///
/// assert_eq!(rets, expected);
///
/// // even delete a range of keys
/// let range = BEI64::new(35)..=BEI64::new(42);
/// let deleted: usize = db.delete_range(&mut wtxn, range)?;
///
/// let rets: Result<_, _> = db.iter(&wtxn)?.collect();
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
pub struct Database<KC, DC> {
    pub(crate) dyndb: PolyDatabase,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database {
            dyndb: PolyDatabase::new(dbi),
            marker: std::marker::PhantomData,
        }
    }

    /// Retrieves the value associated with a key.
    ///
    /// If the key does not exist, then `None` is returned.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// let db: Database<Str, OwnedType<i32>> = env.create_database(Some("get-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, "i-am-forty-two", &42)?;
    /// db.put(&mut wtxn, "i-am-twenty-seven", &27)?;
    ///
    /// let ret = db.get(&wtxn, "i-am-forty-two")?;
    /// assert_eq!(ret, Some(42));
    ///
    /// let ret = db.get(&wtxn, "i-am-twenty-one")?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get<'txn>(&self, txn: &'txn RoTxn, key: &KC::EItem) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get::<KC, DC>(txn, key)
    }

    /// Retrieves the first key/value pair of this database.
    ///
    /// If the database if empty, then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("first-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    ///
    /// let ret = db.first(&wtxn)?;
    /// assert_eq!(ret, Some((BEI32::new(27), "i-am-twenty-seven")));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn first<'txn>(&self, txn: &'txn RoTxn) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.first::<KC, DC>(txn)
    }

    /// Retrieves the last key/value pair of this database.
    ///
    /// If the database if empty, then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("last-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    ///
    /// let ret = db.last(&wtxn)?;
    /// assert_eq!(ret, Some((BEI32::new(42), "i-am-forty-two")));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn last<'txn>(&self, txn: &'txn RoTxn) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.last::<KC, DC>(txn)
    }

    /// Returns the number of elements in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 4);
    ///
    /// db.delete(&mut wtxn, &BEI32::new(27))?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 3);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn len<'txn>(&self, txn: &'txn RoTxn) -> Result<usize> {
        self.dyndb.len(txn)
    }

    /// Returns `true` if and only if this database is empty.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
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
    pub fn is_empty<'txn>(&self, txn: &'txn RoTxn) -> Result<bool> {
        self.dyndb.is_empty(txn)
    }

    /// Return a lexicographically ordered iterator of all key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    ///
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn iter<'txn>(&self, txn: &'txn RoTxn) -> Result<RoIter<'txn, KC, DC>> {
        self.dyndb.iter::<KC, DC>(txn)
    }

    /// Return a mutable lexicographically ordered iterator of all key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    ///
    /// let mut iter = db.iter_mut(&mut wtxn)?;
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
    /// let ret = db.get(&wtxn, &BEI32::new(13))?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, &BEI32::new(42))?;
    /// assert_eq!(ret, Some("i-am-the-new-forty-two"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn iter_mut<'txn>(&self, txn: &'txn mut RwTxn) -> Result<RwIter<'txn, KC, DC>> {
        self.dyndb.iter_mut::<KC, DC>(txn)
    }

    /// Return a lexicographically ordered iterator of a range of key-value pairs in this database.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let mut iter = db.range(&wtxn, range)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn range<'txn, R>(&self, txn: &'txn RoTxn, range: R) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.range::<KC, DC, R>(txn, range)
    }

    /// Return a mutable lexicographically ordered iterator of a range of
    /// key-value pairs in this database.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let mut range = db.range_mut(&mut wtxn, range)?;
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
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-the-new-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(521), "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn range_mut<'txn, R>(
        &self,
        txn: &'txn mut RwTxn,
        range: R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.range_mut::<KC, DC, R>(txn, range)
    }

    /// Return a lexicographically ordered iterator of all key-value pairs
    /// in this database that starts with the given prefix.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Str, OwnedType<BEI32>> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, "i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put(&mut wtxn, "i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put(&mut wtxn, "i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put(&mut wtxn, "i-am-forty-one",    &BEI32::new(41))?;
    /// db.put(&mut wtxn, "i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.prefix_iter(&mut wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter<'txn>(
        &self,
        txn: &'txn RoTxn,
        prefix: &KC::EItem,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        self.dyndb.prefix_iter::<KC, DC>(txn, prefix)
    }

    /// Return a mutable lexicographically ordered iterator of all key-value pairs
    /// in this database that starts with the given prefix.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Str, OwnedType<BEI32>> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, "i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put(&mut wtxn, "i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put(&mut wtxn, "i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put(&mut wtxn, "i-am-forty-one",    &BEI32::new(41))?;
    /// db.put(&mut wtxn, "i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.prefix_iter_mut(&mut wtxn, "i-am-twenty")?;
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
    /// let ret = db.get(&wtxn, "i-am-twenty-eight")?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, "i-am-twenty-seven")?;
    /// assert_eq!(ret, Some(BEI32::new(27000)));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter_mut<'txn>(
        &self,
        txn: &'txn RwTxn,
        prefix: &KC::EItem,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        self.dyndb.prefix_iter_mut::<KC, DC>(txn, prefix)
    }

    /// Insert a key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn put(&self, txn: &mut RwTxn, key: &KC::EItem, data: &DC::EItem) -> Result<()>
    where
        KC: BytesEncode,
        DC: BytesEncode,
    {
        self.dyndb.put::<KC, DC>(txn, key, data)
    }

    /// Deletes a key-value pairs in this database.
    ///
    /// If the key does not exist, then `false` is returned.
    ///
    /// ```
    /// # use std::fs;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.delete(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, true);
    ///
    /// let ret = db.get(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.delete(&mut wtxn, &BEI32::new(467))?;
    /// assert_eq!(ret, false);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn delete(&self, txn: &mut RwTxn, key: &KC::EItem) -> Result<bool>
    where
        KC: BytesEncode,
    {
        self.dyndb.delete::<KC>(txn, key)
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
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let ret = db.delete_range(&mut wtxn, range)?;
    /// assert_eq!(ret, 2);
    ///
    ///
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(521), "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn delete_range<'txn, R>(&self, txn: &'txn mut RwTxn, range: R) -> Result<usize>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.delete_range::<KC, DC, R>(txn, range)
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
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all("target/zerocopy.mdb")?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024 * 1024) // 10GB
    /// #     .max_dbs(3000)
    /// #     .open("target/zerocopy.mdb")?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// db.clear(&mut wtxn)?;
    ///
    /// let ret = db.is_empty(&wtxn)?;
    /// assert!(ret);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn clear(&self, txn: &mut RwTxn) -> Result<()> {
        self.dyndb.clear(txn)
    }
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Database<KC, DC> {
        Database {
            dyndb: self.dyndb,
            marker: marker::PhantomData,
        }
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}
