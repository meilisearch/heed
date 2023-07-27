use std::ops::RangeBounds;
use std::{any, fmt, marker};

use crate::mdb::ffi;
use crate::*;

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
/// # use std::path::Path;
/// # use heed::EnvOpenOptions;
/// use heed::Database;
/// use heed::types::*;
/// use heed::byteorder::BigEndian;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let dir = tempfile::tempdir()?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024) // 10MB
/// #     .max_dbs(3000)
/// #     .open(dir.path())?;
/// type BEI64 = I64<BigEndian>;
///
/// let mut wtxn = env.write_txn()?;
/// let db: Database<BEI64, Unit> = env.create_database(&mut wtxn, Some("big-endian-iter"))?;
///
/// # db.clear(&wtxn)?;
/// db.put(&wtxn, &68, &())?;
/// db.put(&wtxn, &35, &())?;
/// db.put(&wtxn, &0, &())?;
/// db.put(&wtxn, &42, &())?;
///
/// // you can iterate over database entries in order
/// let rets: Result<_, _> = db.iter(&wtxn)?.collect();
/// let rets: Vec<(i64, _)> = rets?;
///
/// let expected = vec![
///     (0, ()),
///     (35, ()),
///     (42, ()),
///     (68, ()),
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
/// # use std::path::Path;
/// # use heed::EnvOpenOptions;
/// use heed::Database;
/// use heed::types::*;
/// use heed::byteorder::BigEndian;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let dir = tempfile::tempdir()?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024) // 10MB
/// #     .max_dbs(3000)
/// #     .open(dir.path())?;
/// type BEI64 = I64<BigEndian>;
///
/// let mut wtxn = env.write_txn()?;
/// let db: Database<BEI64, Unit> = env.create_database(&mut wtxn, Some("big-endian-iter"))?;
///
/// # db.clear(&wtxn)?;
/// db.put(&wtxn, &0, &())?;
/// db.put(&wtxn, &68, &())?;
/// db.put(&wtxn, &35, &())?;
/// db.put(&wtxn, &42, &())?;
///
/// // you can iterate over ranges too!!!
/// let range = 35..=42;
/// let rets: Result<_, _> = db.range(&wtxn, &range)?.collect();
/// let rets: Vec<(i64, _)> = rets?;
///
/// let expected = vec![
///     (35, ()),
///     (42, ()),
/// ];
///
/// assert_eq!(rets, expected);
///
/// // even delete a range of keys
/// let range = 35..=42;
/// let deleted: usize = db.delete_range(&wtxn, &range)?;
///
/// let rets: Result<_, _> = db.iter(&wtxn)?.collect();
/// let rets: Vec<(i64, _)> = rets?;
///
/// let expected = vec![
///     (0, ()),
///     (68, ()),
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
    pub(crate) fn new(env_ident: usize, dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database { dyndb: PolyDatabase::new(env_ident, dbi), marker: std::marker::PhantomData }
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32= U32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, BEI32> = env.create_database(&mut wtxn, Some("get-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, "i-am-forty-two", &42)?;
    /// db.put(&wtxn, "i-am-twenty-seven", &27)?;
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
    pub fn get<'a, 'txn>(&self, txn: &'txn RoTxn, key: &'a KC::EItem) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get::<KC, DC>(txn, key)
    }

    /// Retrieves the key/value pair lower than the given one in this database.
    ///
    /// If the database if empty or there is no key lower than the given one,
    /// then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.create_database::<BEU32, Unit>(&mut wtxn, Some("get-lt-u32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &27, &())?;
    /// db.put(&wtxn, &42, &())?;
    /// db.put(&wtxn, &43, &())?;
    ///
    /// let ret = db.get_lower_than(&wtxn, &4404)?;
    /// assert_eq!(ret, Some((43, ())));
    ///
    /// let ret = db.get_lower_than(&wtxn, &43)?;
    /// assert_eq!(ret, Some((42, ())));
    ///
    /// let ret = db.get_lower_than(&wtxn, &27)?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_lower_than<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get_lower_than::<KC, DC>(txn, key)
    }

    /// Retrieves the key/value pair lower than or equal to the given one in this database.
    ///
    /// If the database if empty or there is no key lower than or equal to the given one,
    /// then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.create_database::<BEU32, Unit>(&mut wtxn, Some("get-lt-u32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &27, &())?;
    /// db.put(&wtxn, &42, &())?;
    /// db.put(&wtxn, &43, &())?;
    ///
    /// let ret = db.get_lower_than_or_equal_to(&wtxn, &4404)?;
    /// assert_eq!(ret, Some((43, ())));
    ///
    /// let ret = db.get_lower_than_or_equal_to(&wtxn, &43)?;
    /// assert_eq!(ret, Some((43, ())));
    ///
    /// let ret = db.get_lower_than_or_equal_to(&wtxn, &26)?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_lower_than_or_equal_to<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get_lower_than_or_equal_to::<KC, DC>(txn, key)
    }

    /// Retrieves the key/value pair greater than the given one in this database.
    ///
    /// If the database if empty or there is no key greater than the given one,
    /// then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.create_database::<BEU32, Unit>(&mut wtxn, Some("get-lt-u32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &27, &())?;
    /// db.put(&wtxn, &42, &())?;
    /// db.put(&wtxn, &43, &())?;
    ///
    /// let ret = db.get_greater_than(&wtxn, &0)?;
    /// assert_eq!(ret, Some((27, ())));
    ///
    /// let ret = db.get_greater_than(&wtxn, &42)?;
    /// assert_eq!(ret, Some((43, ())));
    ///
    /// let ret = db.get_greater_than(&wtxn, &43)?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_greater_than<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get_greater_than::<KC, DC>(txn, key)
    }

    /// Retrieves the key/value pair greater than or equal to the given one in this database.
    ///
    /// If the database if empty or there is no key greater than or equal to the given one,
    /// then `None` is returned.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.create_database::<BEU32, Unit>(&mut wtxn, Some("get-lt-u32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &27, &())?;
    /// db.put(&wtxn, &42, &())?;
    /// db.put(&wtxn, &43, &())?;
    ///
    /// let ret = db.get_greater_than_or_equal_to(&wtxn, &0)?;
    /// assert_eq!(ret, Some((27, ())));
    ///
    /// let ret = db.get_greater_than_or_equal_to(&wtxn, &42)?;
    /// assert_eq!(ret, Some((42, ())));
    ///
    /// let ret = db.get_greater_than_or_equal_to(&wtxn, &44)?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_greater_than_or_equal_to<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get_greater_than_or_equal_to::<KC, DC>(txn, key)
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("first-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    ///
    /// let ret = db.first(&wtxn)?;
    /// assert_eq!(ret, Some((27, "i-am-twenty-seven")));
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
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("last-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    ///
    /// let ret = db.last(&wtxn)?;
    /// assert_eq!(ret, Some((42, "i-am-forty-two")));
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
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 4);
    ///
    /// db.delete(&wtxn, &27)?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 3);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn len(&self, txn: &RoTxn) -> Result<u64> {
        self.dyndb.len(txn)
    }

    /// Returns `true` if and only if this database is empty.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.is_empty(&wtxn)?;
    /// assert_eq!(ret, false);
    ///
    /// db.clear(&wtxn)?;
    ///
    /// let ret = db.is_empty(&wtxn)?;
    /// assert_eq!(ret, true);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn is_empty(&self, txn: &RoTxn) -> Result<bool> {
        self.dyndb.is_empty(txn)
    }

    /// Return a lexicographically ordered iterator of all key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    ///
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((13, "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((42, "i-am-forty-two")));
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
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    ///
    /// let mut iter = db.iter_mut(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((13, "i-am-thirteen")));
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((42, "i-am-forty-two")));
    /// let ret = unsafe { iter.put_current(&42, "i-am-the-new-forty-two")? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get(&wtxn, &13)?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, &42)?;
    /// assert_eq!(ret, Some("i-am-the-new-forty-two"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn iter_mut<'txn>(&self, txn: &'txn RwTxn) -> Result<RwIter<'txn, KC, DC>> {
        self.dyndb.iter_mut::<KC, DC>(txn)
    }

    /// Return a reversed lexicographically ordered iterator of all key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    ///
    /// let mut iter = db.rev_iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((42, "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((13, "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_iter<'txn>(&self, txn: &'txn RoTxn) -> Result<RoRevIter<'txn, KC, DC>> {
        self.dyndb.rev_iter::<KC, DC>(txn)
    }

    /// Return a mutable reversed lexicographically ordered iterator of all key-value\
    /// pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    ///
    /// let mut iter = db.rev_iter_mut(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((42, "i-am-forty-two")));
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((13, "i-am-thirteen")));
    /// let ret = unsafe { iter.put_current(&13, "i-am-the-new-thirteen")? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get(&wtxn, &42)?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, &13)?;
    /// assert_eq!(ret, Some("i-am-the-new-thirteen"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_iter_mut<'txn>(&self, txn: &'txn RwTxn) -> Result<RwRevIter<'txn, KC, DC>> {
        self.dyndb.rev_iter_mut::<KC, DC>(txn)
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=42;
    /// let mut iter = db.range(&wtxn, &range)?;
    /// assert_eq!(iter.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((42, "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn range<'a, 'txn, R>(
        &self,
        txn: &'txn RoTxn,
        range: &'a R,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
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
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=42;
    /// let mut range = db.range_mut(&wtxn, &range)?;
    /// assert_eq!(range.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// let ret = unsafe { range.del_current()? };
    /// assert!(ret);
    /// assert_eq!(range.next().transpose()?, Some((42, "i-am-forty-two")));
    /// let ret = unsafe { range.put_current(&42, "i-am-the-new-forty-two")? };
    /// assert!(ret);
    ///
    /// assert_eq!(range.next().transpose()?, None);
    /// drop(range);
    ///
    ///
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((13, "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((42, "i-am-the-new-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((521, "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn range_mut<'a, 'txn, R>(
        &self,
        txn: &'txn RwTxn,
        range: &'a R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.range_mut::<KC, DC, R>(txn, range)
    }

    /// Return a reversed lexicographically ordered iterator of a range of key-value
    /// pairs in this database.
    ///
    /// Comparisons are made by using the bytes representation of the key.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=43;
    /// let mut iter = db.rev_range(&wtxn, &range)?;
    /// assert_eq!(iter.next().transpose()?, Some((42, "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_range<'a, 'txn, R>(
        &self,
        txn: &'txn RoTxn,
        range: &'a R,
    ) -> Result<RoRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.rev_range::<KC, DC, R>(txn, range)
    }

    /// Return a mutable reversed lexicographically ordered iterator of a range of
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=42;
    /// let mut range = db.rev_range_mut(&wtxn, &range)?;
    /// assert_eq!(range.next().transpose()?, Some((42, "i-am-forty-two")));
    /// let ret = unsafe { range.del_current()? };
    /// assert!(ret);
    /// assert_eq!(range.next().transpose()?, Some((27, "i-am-twenty-seven")));
    /// let ret = unsafe { range.put_current(&27, "i-am-the-new-twenty-seven")? };
    /// assert!(ret);
    ///
    /// assert_eq!(range.next().transpose()?, None);
    /// drop(range);
    ///
    ///
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((13, "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((27, "i-am-the-new-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((521, "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_range_mut<'a, 'txn, R>(
        &self,
        txn: &'txn RwTxn,
        range: &'a R,
    ) -> Result<RwRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.rev_range_mut::<KC, DC, R>(txn, range)
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, BEI32> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.prefix_iter(&wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", 28)));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", 29)));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", 27)));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RoPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
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
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, BEI32> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.prefix_iter_mut(&wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", 28)));
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", 29)));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", 27)));
    /// let ret = unsafe { iter.put_current("i-am-twenty-seven", &27000)? };
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
    /// assert_eq!(ret, Some(27000));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter_mut<'a, 'txn>(
        &self,
        txn: &'txn RwTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RwPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.prefix_iter_mut::<KC, DC>(txn, prefix)
    }

    /// Return a reversed lexicographically ordered iterator of all key-value pairs
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, BEI32> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.rev_prefix_iter(&wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", 27)));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", 29)));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", 28)));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_prefix_iter<'a, 'txn>(
        &self,
        txn: &'txn RoTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RoRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.rev_prefix_iter::<KC, DC>(txn, prefix)
    }

    /// Return a mutable reversed lexicographically ordered iterator of all key-value pairs
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, BEI32> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.rev_prefix_iter_mut(&wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", 27)));
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", 29)));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", 28)));
    /// let ret = unsafe { iter.put_current("i-am-twenty-eight", &28000)? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get(&wtxn, "i-am-twenty-seven")?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, "i-am-twenty-eight")?;
    /// assert_eq!(ret, Some(28000));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_prefix_iter_mut<'a, 'txn>(
        &self,
        txn: &'txn RwTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RwRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.rev_prefix_iter_mut::<KC, DC>(txn, prefix)
    }

    /// Insert a key-value pairs in this database.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&wtxn, &27)?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn put<'a>(&self, txn: &RwTxn, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.dyndb.put::<KC, DC>(txn, key, data)
    }

    /// Insert a key-value pair where the value can directly be written to disk.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use std::io::Write;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.create_database::<BEI32, Str>(&mut wtxn, Some("number-string"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// let value = "I am a long long long value";
    /// db.put_reserved(&wtxn, &42, value.len(), |reserved| {
    ///     reserved.write_all(value.as_bytes())
    /// })?;
    ///
    /// let ret = db.get(&wtxn, &42)?;
    /// assert_eq!(ret, Some(value));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn put_reserved<'a, F>(
        &self,
        txn: &RwTxn,
        key: &'a KC::EItem,
        data_size: usize,
        write_func: F,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        F: FnMut(&mut ReservedSpace) -> io::Result<()>,
    {
        self.dyndb.put_reserved::<KC, F>(txn, key, data_size, write_func)
    }

    /// Append the given key/data pair to the end of the database.
    ///
    /// This option allows fast bulk loading when keys are already known to be in the correct order.
    /// Loading unsorted keys will cause a `KeyExist`/`MDB_KEYEXIST` error.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.append(&wtxn, &13, "i-am-thirteen")?;
    /// db.append(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.append(&wtxn, &42, "i-am-forty-two")?;
    /// db.append(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&wtxn, &27)?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// // Be wary if you insert at the end unsorted you get the KEYEXIST error.
    /// assert!(db.append(&wtxn, &1, "Oh No").is_err());
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn append<'a>(&self, txn: &RwTxn, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.dyndb.append::<KC, DC>(txn, key, data)
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.delete(&wtxn, &27)?;
    /// assert_eq!(ret, true);
    ///
    /// let ret = db.get(&wtxn, &27)?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.delete(&wtxn, &467)?;
    /// assert_eq!(ret, false);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn delete<'a>(&self, txn: &RwTxn, key: &'a KC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.delete::<KC>(txn, key)
    }

    /// Deletes a range of key-value pairs in this database.
    ///
    /// Prefer using [`clear`] instead of a call to this method with a full range ([`..`]).
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=42;
    /// let ret = db.delete_range(&wtxn, &range)?;
    /// assert_eq!(ret, 2);
    ///
    ///
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((13, "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((521, "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn delete_range<'a, 'txn, R>(&self, txn: &'txn RwTxn, range: &'a R) -> Result<usize>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.delete_range::<KC, R>(txn, range)
    }

    /// Deletes all key/value pairs in this database.
    ///
    /// Prefer using this method instead of a call to [`delete_range`] with a full range ([`..`]).
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// db.clear(&wtxn)?;
    ///
    /// let ret = db.is_empty(&wtxn)?;
    /// assert!(ret);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn clear(&self, txn: &RwTxn) -> Result<()> {
        self.dyndb.clear(txn)
    }

    /// Change the codec types of this uniform database, specifying the codecs.
    ///
    /// # Safety
    ///
    /// It is up to you to ensure that the data read and written using the polymorphic
    /// handle correspond to the the typed, uniform one. If an invalid write is made,
    /// it can corrupt the database from the eyes of heed.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::{Database, PolyDatabase};
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Unit, Unit> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// // We remap the types for ease of use.
    /// let db = db.remap_types::<BEI32, Str>();
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn remap_types<KC2, DC2>(&self) -> Database<KC2, DC2> {
        Database::new(self.dyndb.env_ident, self.dyndb.dbi)
    }

    /// Change the key codec type of this uniform database, specifying the new codec.
    pub fn remap_key_type<KC2>(&self) -> Database<KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this uniform database, specifying the new codec.
    pub fn remap_data_type<DC2>(&self) -> Database<KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(&self) -> Database<KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }

    /// Get an handle on the internal polymorphic database.
    ///
    /// Using this method is useful when you want to skip deserializing of the value,
    /// by specifying that the value is of type `DecodeIgnore` for example.
    ///
    /// [`DecodeIgnore`]: crate::types::DecodeIgnore
    ///
    /// # Safety
    ///
    /// It is up to you to ensure that the data read and written using the polymorphic
    /// handle correspond to the the typed, uniform one. If an invalid write is made,
    /// it can corrupt the database from the eyes of heed.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::EnvOpenOptions;
    /// use heed::Database;
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<BEI32, Str> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&wtxn)?;
    /// db.put(&wtxn, &42, "i-am-forty-two")?;
    /// db.put(&wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&wtxn, &13, "i-am-thirteen")?;
    /// db.put(&wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// // Check if a key exists and skip potentially expensive deserializing
    /// let ret = db.as_polymorph().get::<BEI32, DecodeIgnore>(&wtxn, &42)?;
    /// assert!(ret.is_some());
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn as_polymorph(&self) -> &PolyDatabase {
        &self.dyndb
    }
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Database<KC, DC> {
        Database { dyndb: self.dyndb, marker: marker::PhantomData }
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}

impl<KC, DC> fmt::Debug for Database<KC, DC> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Database")
            .field("key_codec", &any::type_name::<KC>())
            .field("data_codec", &any::type_name::<DC>())
            .finish()
    }
}
