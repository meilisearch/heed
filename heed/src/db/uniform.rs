use std::marker;
use std::ops::RangeBounds;

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
/// use heed::{zerocopy::I64, byteorder::BigEndian};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024) // 10MB
/// #     .max_dbs(3000)
/// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
/// # use std::path::Path;
/// # use heed::EnvOpenOptions;
/// use heed::Database;
/// use heed::types::*;
/// use heed::{zerocopy::I64, byteorder::BigEndian};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024) // 10MB
/// #     .max_dbs(3000)
/// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
/// let rets: Result<_, _> = db.range(&wtxn, &range)?.collect();
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
/// let deleted: usize = db.delete_range(&mut wtxn, &range)?;
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
    pub(crate) fn new(env_ident: usize, dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database {
            dyndb: PolyDatabase::new(env_ident, dbi),
            marker: std::marker::PhantomData,
        }
    }

    /// Retrieve the sequence of a database.
    ///
    /// This function allows to retrieve the unique positive integer of this database.
    /// You can see an example usage on the `PolyDatabase::sequence` method documentation.
    #[cfg(all(feature = "mdbx", not(feature = "lmdb")))]
    pub fn sequence<T>(&self, txn: &RoTxn<T>) -> Result<u64> {
        self.dyndb.sequence(txn)
    }

    /// Increment the sequence of a database.
    ///
    /// This function allows to create a linear sequence of a unique positive integer
    /// for this database. Sequence changes become visible outside the current write
    /// transaction after it is committed, and discarded on abort.
    /// You can see an example usage on the `PolyDatabase::increase_sequence` method documentation.
    ///
    /// Returns `Some` with the previous value and `None` if increasing the value
    /// resulted in an overflow an therefore cannot be executed.
    #[cfg(all(feature = "mdbx", not(feature = "lmdb")))]
    pub fn increase_sequence<T>(&self, txn: &mut RwTxn<T>, increment: u64) -> Result<Option<u64>> {
        self.dyndb.increase_sequence(txn, increment)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn get<'a, 'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get::<T, KC, DC>(txn, key)
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
    /// use heed::{zerocopy::U32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let db = env.create_database::<OwnedType<BEU32>, Unit>(Some("get-lt-u32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEU32::new(27), &())?;
    /// db.put(&mut wtxn, &BEU32::new(42), &())?;
    /// db.put(&mut wtxn, &BEU32::new(43), &())?;
    ///
    /// let ret = db.get_lower_than(&wtxn, &BEU32::new(4404))?;
    /// assert_eq!(ret, Some((BEU32::new(43), ())));
    ///
    /// let ret = db.get_lower_than(&wtxn, &BEU32::new(43))?;
    /// assert_eq!(ret, Some((BEU32::new(42), ())));
    ///
    /// let ret = db.get_lower_than(&wtxn, &BEU32::new(27))?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_lower_than<'a, 'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get_lower_than::<T, KC, DC>(txn, key)
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
    /// use heed::{zerocopy::U32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let db = env.create_database::<OwnedType<BEU32>, Unit>(Some("get-lt-u32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEU32::new(27), &())?;
    /// db.put(&mut wtxn, &BEU32::new(42), &())?;
    /// db.put(&mut wtxn, &BEU32::new(43), &())?;
    ///
    /// let ret = db.get_lower_than_or_equal_to(&wtxn, &BEU32::new(4404))?;
    /// assert_eq!(ret, Some((BEU32::new(43), ())));
    ///
    /// let ret = db.get_lower_than_or_equal_to(&wtxn, &BEU32::new(43))?;
    /// assert_eq!(ret, Some((BEU32::new(43), ())));
    ///
    /// let ret = db.get_lower_than_or_equal_to(&wtxn, &BEU32::new(26))?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_lower_than_or_equal_to<'a, 'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get_lower_than_or_equal_to::<T, KC, DC>(txn, key)
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
    /// use heed::{zerocopy::U32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let db = env.create_database::<OwnedType<BEU32>, Unit>(Some("get-lt-u32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEU32::new(27), &())?;
    /// db.put(&mut wtxn, &BEU32::new(42), &())?;
    /// db.put(&mut wtxn, &BEU32::new(43), &())?;
    ///
    /// let ret = db.get_greater_than(&wtxn, &BEU32::new(0))?;
    /// assert_eq!(ret, Some((BEU32::new(27), ())));
    ///
    /// let ret = db.get_greater_than(&wtxn, &BEU32::new(42))?;
    /// assert_eq!(ret, Some((BEU32::new(43), ())));
    ///
    /// let ret = db.get_greater_than(&wtxn, &BEU32::new(43))?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_greater_than<'a, 'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get_greater_than::<T, KC, DC>(txn, key)
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
    /// use heed::{zerocopy::U32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEU32 = U32<BigEndian>;
    ///
    /// let db = env.create_database::<OwnedType<BEU32>, Unit>(Some("get-lt-u32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEU32::new(27), &())?;
    /// db.put(&mut wtxn, &BEU32::new(42), &())?;
    /// db.put(&mut wtxn, &BEU32::new(43), &())?;
    ///
    /// let ret = db.get_greater_than_or_equal_to(&wtxn, &BEU32::new(0))?;
    /// assert_eq!(ret, Some((BEU32::new(27), ())));
    ///
    /// let ret = db.get_greater_than_or_equal_to(&wtxn, &BEU32::new(42))?;
    /// assert_eq!(ret, Some((BEU32::new(42), ())));
    ///
    /// let ret = db.get_greater_than_or_equal_to(&wtxn, &BEU32::new(44))?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get_greater_than_or_equal_to<'a, 'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &'a KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb
            .get_greater_than_or_equal_to::<T, KC, DC>(txn, key)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn first<'txn, T>(&self, txn: &'txn RoTxn<T>) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.first::<T, KC, DC>(txn)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn last<'txn, T>(&self, txn: &'txn RoTxn<T>) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.last::<T, KC, DC>(txn)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn len<'txn, T>(&self, txn: &'txn RoTxn<T>) -> Result<usize> {
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn is_empty<'txn, T>(&self, txn: &'txn RoTxn<T>) -> Result<bool> {
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn iter<'txn, T>(&self, txn: &'txn RoTxn<T>) -> Result<RoIter<'txn, KC, DC>> {
        self.dyndb.iter::<T, KC, DC>(txn)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = unsafe { iter.put_current(&BEI32::new(42), "i-am-the-new-forty-two")? };
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
    pub fn iter_mut<'txn, T>(&self, txn: &'txn mut RwTxn<T>) -> Result<RwIter<'txn, KC, DC>> {
        self.dyndb.iter_mut::<T, KC, DC>(txn)
    }

    /// Return a reversed lexicographically ordered iterator of all key-value pairs in this database.
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let mut iter = db.rev_iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_iter<'txn, T>(&self, txn: &'txn RoTxn<T>) -> Result<RoRevIter<'txn, KC, DC>> {
        self.dyndb.rev_iter::<T, KC, DC>(txn)
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let mut iter = db.rev_iter_mut(&mut wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// let ret = unsafe { iter.put_current(&BEI32::new(13), "i-am-the-new-thirteen")? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get(&wtxn, &BEI32::new(42))?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, &BEI32::new(13))?;
    /// assert_eq!(ret, Some("i-am-the-new-thirteen"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_iter_mut<'txn, T>(
        &self,
        txn: &'txn mut RwTxn<T>,
    ) -> Result<RwRevIter<'txn, KC, DC>> {
        self.dyndb.rev_iter_mut::<T, KC, DC>(txn)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let mut iter = db.range(&wtxn, &range)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn range<'a, 'txn, T, R>(
        &self,
        txn: &'txn RoTxn<T>,
        range: &'a R,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.range::<T, KC, DC, R>(txn, range)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let mut range = db.range_mut(&mut wtxn, &range)?;
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// let ret = unsafe { range.del_current()? };
    /// assert!(ret);
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = unsafe { range.put_current(&BEI32::new(42), "i-am-the-new-forty-two")? };
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
    pub fn range_mut<'a, 'txn, T, R>(
        &self,
        txn: &'txn mut RwTxn<T>,
        range: &'a R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.range_mut::<T, KC, DC, R>(txn, range)
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let range = BEI32::new(27)..=BEI32::new(43);
    /// let mut iter = db.rev_range(&wtxn, &range)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_range<'a, 'txn, T, R>(
        &self,
        txn: &'txn RoTxn<T>,
        range: &'a R,
    ) -> Result<RoRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.rev_range::<T, KC, DC, R>(txn, range)
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let mut range = db.rev_range_mut(&mut wtxn, &range)?;
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = unsafe { range.del_current()? };
    /// assert!(ret);
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// let ret = unsafe { range.put_current(&BEI32::new(27), "i-am-the-new-twenty-seven")? };
    /// assert!(ret);
    ///
    /// assert_eq!(range.next().transpose()?, None);
    /// drop(range);
    ///
    ///
    /// let mut iter = db.iter(&wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-the-new-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(521), "i-am-five-hundred-and-twenty-one")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_range_mut<'a, 'txn, T, R>(
        &self,
        txn: &'txn mut RwTxn<T>,
        range: &'a R,
    ) -> Result<RwRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.rev_range_mut::<T, KC, DC, R>(txn, range)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn prefix_iter<'a, 'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        prefix: &'a KC::EItem,
    ) -> Result<RoPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.prefix_iter::<T, KC, DC>(txn, prefix)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// let ret = unsafe { iter.put_current("i-am-twenty-seven", &BEI32::new(27000))? };
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
    pub fn prefix_iter_mut<'a, 'txn, T>(
        &self,
        txn: &'txn mut RwTxn<T>,
        prefix: &'a KC::EItem,
    ) -> Result<RwPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.prefix_iter_mut::<T, KC, DC>(txn, prefix)
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let mut iter = db.rev_prefix_iter(&mut wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_prefix_iter<'a, 'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        prefix: &'a KC::EItem,
    ) -> Result<RoRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.rev_prefix_iter::<T, KC, DC>(txn, prefix)
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let mut iter = db.rev_prefix_iter_mut(&mut wtxn, "i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// let ret = unsafe { iter.del_current()? };
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// let ret = unsafe { iter.put_current("i-am-twenty-eight", &BEI32::new(28000))? };
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
    /// assert_eq!(ret, Some(BEI32::new(28000)));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_prefix_iter_mut<'a, 'txn, T>(
        &self,
        txn: &'txn mut RwTxn<T>,
        prefix: &'a KC::EItem,
    ) -> Result<RwRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.rev_prefix_iter_mut::<T, KC, DC>(txn, prefix)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn put<'a, T>(
        &self,
        txn: &mut RwTxn<T>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.dyndb.put::<T, KC, DC>(txn, key, data)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn append<'a, T>(
        &self,
        txn: &mut RwTxn<T>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.dyndb.append::<T, KC, DC>(txn, key, data)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn delete<'a, T>(&self, txn: &mut RwTxn<T>, key: &'a KC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
    {
        self.dyndb.delete::<T, KC>(txn, key)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// let ret = db.delete_range(&mut wtxn, &range)?;
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
    pub fn delete_range<'a, 'txn, T, R>(
        &self,
        txn: &'txn mut RwTxn<T>,
        range: &'a R,
    ) -> Result<usize>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.delete_range::<T, KC, R>(txn, range)
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
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    pub fn clear<T>(&self, txn: &mut RwTxn<T>) -> Result<()> {
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Unit, Unit> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// // We remap the types for ease of use.
    /// let db = db.remap_types::<OwnedType<BEI32>, Str>();
    /// db.put(&mut wtxn, &BEI32::new(42), "i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), "i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), "i-am-five-hundred-and-twenty-one")?;
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
    /// use heed::{zerocopy::I32, byteorder::BigEndian};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("zerocopy.mdb"))?;
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
    /// // Check if a key exists and skip potentially expensive deserializing
    /// let ret = db.as_polymorph().get::<_, OwnedType<BEI32>, DecodeIgnore>(&wtxn, &BEI32::new(42))?;
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
        Database {
            dyndb: self.dyndb,
            marker: marker::PhantomData,
        }
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}
