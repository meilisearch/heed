use std::borrow::Cow;
use std::ops::{Bound, RangeBounds};
use std::{any, fmt, marker, mem, ptr};

use types::DecodeIgnore;

use crate::mdb::error::mdb_result;
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
/// # db.clear(&mut wtxn)?;
/// db.put(&mut wtxn, &68, &())?;
/// db.put(&mut wtxn, &35, &())?;
/// db.put(&mut wtxn, &0, &())?;
/// db.put(&mut wtxn, &42, &())?;
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
/// # db.clear(&mut wtxn)?;
/// db.put(&mut wtxn, &0, &())?;
/// db.put(&mut wtxn, &68, &())?;
/// db.put(&mut wtxn, &35, &())?;
/// db.put(&mut wtxn, &42, &())?;
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
/// let deleted: usize = db.delete_range(&mut wtxn, &range)?;
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
    pub(crate) env_ident: usize,
    pub(crate) dbi: ffi::MDB_dbi,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(env_ident: usize, dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database { env_ident, dbi, marker: std::marker::PhantomData }
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
    pub fn get<'a, 'txn>(&self, txn: &'txn RoTxn, key: &'a KC::EItem) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        assert_eq_env_db_txn!(self, txn);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = mem::MaybeUninit::uninit();

        let result = unsafe {
            mdb_result(ffi::mdb_get(txn.txn, self.dbi, &mut key_val, data_val.as_mut_ptr()))
        };

        match result {
            Ok(()) => {
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                let data = DC::bytes_decode(data).map_err(Error::Decoding)?;
                Ok(Some(data))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &27, &())?;
    /// db.put(&mut wtxn, &42, &())?;
    /// db.put(&mut wtxn, &43, &())?;
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
        assert_eq_env_db_txn!(self, txn);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        cursor.move_on_key_greater_than_or_equal_to(&key_bytes)?;

        match cursor.move_on_prev() {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Ok(Some((key, data))),
                (Err(e), _) | (_, Err(e)) => Err(Error::Decoding(e)),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &27, &())?;
    /// db.put(&mut wtxn, &42, &())?;
    /// db.put(&mut wtxn, &43, &())?;
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
        assert_eq_env_db_txn!(self, txn);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let result = match cursor.move_on_key_greater_than_or_equal_to(&key_bytes) {
            Ok(Some((key, data))) if key == &key_bytes[..] => Ok(Some((key, data))),
            Ok(_) => cursor.move_on_prev(),
            Err(e) => Err(e),
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Ok(Some((key, data))),
                (Err(e), _) | (_, Err(e)) => Err(Error::Decoding(e)),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &27, &())?;
    /// db.put(&mut wtxn, &42, &())?;
    /// db.put(&mut wtxn, &43, &())?;
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
        assert_eq_env_db_txn!(self, txn);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let entry = match cursor.move_on_key_greater_than_or_equal_to(&key_bytes)? {
            Some((key, data)) if key > &key_bytes[..] => Some((key, data)),
            Some((_key, _data)) => cursor.move_on_next()?,
            None => None,
        };

        match entry {
            Some((key, data)) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Ok(Some((key, data))),
                (Err(e), _) | (_, Err(e)) => Err(Error::Decoding(e)),
            },
            None => Ok(None),
        }
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &27, &())?;
    /// db.put(&mut wtxn, &42, &())?;
    /// db.put(&mut wtxn, &43, &())?;
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
        assert_eq_env_db_txn!(self, txn);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        match cursor.move_on_key_greater_than_or_equal_to(&key_bytes) {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Ok(Some((key, data))),
                (Err(e), _) | (_, Err(e)) => Err(Error::Decoding(e)),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
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
        assert_eq_env_db_txn!(self, txn);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        match cursor.move_on_first() {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Ok(Some((key, data))),
                (Err(e), _) | (_, Err(e)) => Err(Error::Decoding(e)),
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
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
        assert_eq_env_db_txn!(self, txn);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        match cursor.move_on_last() {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Ok(Some((key, data))),
                (Err(e), _) | (_, Err(e)) => Err(Error::Decoding(e)),
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 4);
    ///
    /// db.delete(&mut wtxn, &27)?;
    ///
    /// let ret = db.len(&wtxn)?;
    /// assert_eq!(ret, 3);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn len(&self, txn: &RoTxn) -> Result<u64> {
        assert_eq_env_db_txn!(self, txn);

        let mut db_stat = mem::MaybeUninit::uninit();
        let result = unsafe { mdb_result(ffi::mdb_stat(txn.txn, self.dbi, db_stat.as_mut_ptr())) };

        match result {
            Ok(()) => {
                let stats = unsafe { db_stat.assume_init() };
                Ok(stats.ms_entries as u64)
            }
            Err(e) => Err(e.into()),
        }
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
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
    pub fn is_empty(&self, txn: &RoTxn) -> Result<bool> {
        self.len(txn).map(|l| l == 0)
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
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
        assert_eq_env_db_txn!(self, txn);

        RoCursor::new(txn, self.dbi).map(|cursor| RoIter::new(cursor))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    ///
    /// let mut iter = db.iter_mut(&mut wtxn)?;
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
    pub fn iter_mut<'txn>(&self, txn: &'txn mut RwTxn) -> Result<RwIter<'txn, KC, DC>> {
        assert_eq_env_db_txn!(self, txn);

        RwCursor::new(txn, self.dbi).map(|cursor| RwIter::new(cursor))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
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
        assert_eq_env_db_txn!(self, txn);

        RoCursor::new(txn, self.dbi).map(|cursor| RoRevIter::new(cursor))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    ///
    /// let mut iter = db.rev_iter_mut(&mut wtxn)?;
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
    pub fn rev_iter_mut<'txn>(&self, txn: &'txn mut RwTxn) -> Result<RwRevIter<'txn, KC, DC>> {
        assert_eq_env_db_txn!(self, txn);

        RwCursor::new(txn, self.dbi).map(|cursor| RwRevIter::new(cursor))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
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
        assert_eq_env_db_txn!(self, txn);

        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        RoCursor::new(txn, self.dbi).map(|cursor| RoRange::new(cursor, start_bound, end_bound))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=42;
    /// let mut range = db.range_mut(&mut wtxn, &range)?;
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
        txn: &'txn mut RwTxn,
        range: &'a R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq_env_db_txn!(self, txn);

        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        RwCursor::new(txn, self.dbi).map(|cursor| RwRange::new(cursor, start_bound, end_bound))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
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
        assert_eq_env_db_txn!(self, txn);

        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        RoCursor::new(txn, self.dbi).map(|cursor| RoRevRange::new(cursor, start_bound, end_bound))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=42;
    /// let mut range = db.rev_range_mut(&mut wtxn, &range)?;
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
        txn: &'txn mut RwTxn,
        range: &'a R,
    ) -> Result<RwRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq_env_db_txn!(self, txn);

        let start_bound = match range.start_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Included(bytes.into_owned())
            }
            Bound::Excluded(bound) => {
                let bytes = KC::bytes_encode(bound).map_err(Error::Encoding)?;
                Bound::Excluded(bytes.into_owned())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        RwCursor::new(txn, self.dbi).map(|cursor| RwRevRange::new(cursor, start_bound, end_bound))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&mut wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&mut wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&mut wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&mut wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.prefix_iter(&mut wtxn, "i-am-twenty")?;
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
        assert_eq_env_db_txn!(self, txn);

        let prefix_bytes = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        let prefix_bytes = prefix_bytes.into_owned();
        RoCursor::new(txn, self.dbi).map(|cursor| RoPrefix::new(cursor, prefix_bytes))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&mut wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&mut wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&mut wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&mut wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.prefix_iter_mut(&mut wtxn, "i-am-twenty")?;
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
        txn: &'txn mut RwTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RwPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        assert_eq_env_db_txn!(self, txn);

        let prefix_bytes = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        let prefix_bytes = prefix_bytes.into_owned();
        RwCursor::new(txn, self.dbi).map(|cursor| RwPrefix::new(cursor, prefix_bytes))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&mut wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&mut wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&mut wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&mut wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.rev_prefix_iter(&mut wtxn, "i-am-twenty")?;
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
        assert_eq_env_db_txn!(self, txn);

        let prefix_bytes = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        let prefix_bytes = prefix_bytes.into_owned();
        RoCursor::new(txn, self.dbi).map(|cursor| RoRevPrefix::new(cursor, prefix_bytes))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, "i-am-twenty-eight", &28)?;
    /// db.put(&mut wtxn, "i-am-twenty-seven", &27)?;
    /// db.put(&mut wtxn, "i-am-twenty-nine",  &29)?;
    /// db.put(&mut wtxn, "i-am-forty-one",    &41)?;
    /// db.put(&mut wtxn, "i-am-forty-two",    &42)?;
    ///
    /// let mut iter = db.rev_prefix_iter_mut(&mut wtxn, "i-am-twenty")?;
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
        txn: &'txn mut RwTxn,
        prefix: &'a KC::EItem,
    ) -> Result<RwRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode<'a>,
    {
        assert_eq_env_db_txn!(self, txn);

        let prefix_bytes = KC::bytes_encode(prefix).map_err(Error::Encoding)?;
        let prefix_bytes = prefix_bytes.into_owned();
        RwCursor::new(txn, self.dbi).map(|cursor| RwRevPrefix::new(cursor, prefix_bytes))
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&mut wtxn, &27)?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn put<'a>(&self, txn: &mut RwTxn, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        assert_eq_env_db_txn!(self, txn);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(data).map_err(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = unsafe { crate::into_val(&data_bytes) };
        let flags = 0;

        unsafe {
            mdb_result(ffi::mdb_put(txn.txn.txn, self.dbi, &mut key_val, &mut data_val, flags))?
        }

        Ok(())
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
    /// # db.clear(&mut wtxn)?;
    /// let value = "I am a long long long value";
    /// db.put_reserved(&mut wtxn, &42, value.len(), |reserved| {
    ///     reserved.write_all(value.as_bytes())
    /// })?;
    ///
    /// let ret = db.get(&mut wtxn, &42)?;
    /// assert_eq!(ret, Some(value));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn put_reserved<'a, F>(
        &self,
        txn: &mut RwTxn,
        key: &'a KC::EItem,
        data_size: usize,
        mut write_func: F,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        F: FnMut(&mut ReservedSpace) -> io::Result<()>,
    {
        assert_eq_env_db_txn!(self, txn);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut reserved = ffi::reserve_size_val(data_size);
        let flags = ffi::MDB_RESERVE;

        unsafe {
            mdb_result(ffi::mdb_put(txn.txn.txn, self.dbi, &mut key_val, &mut reserved, flags))?
        }

        let mut reserved = unsafe { ReservedSpace::from_val(reserved) };
        (write_func)(&mut reserved)?;
        if reserved.remaining() == 0 {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::UnexpectedEof).into())
        }
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
    /// # db.clear(&mut wtxn)?;
    /// db.append(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.append(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.append(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.append(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&mut wtxn, &27)?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// // Be wary if you insert at the end unsorted you get the KEYEXIST error.
    /// assert!(db.append(&mut wtxn, &1, "Oh No").is_err());
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn append<'a>(&self, txn: &mut RwTxn, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        assert_eq_env_db_txn!(self, txn);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(data).map_err(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = unsafe { crate::into_val(&data_bytes) };
        let flags = ffi::MDB_APPEND;

        unsafe {
            mdb_result(ffi::mdb_put(txn.txn.txn, self.dbi, &mut key_val, &mut data_val, flags))?
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.delete(&mut wtxn, &27)?;
    /// assert_eq!(ret, true);
    ///
    /// let ret = db.get(&mut wtxn, &27)?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.delete(&mut wtxn, &467)?;
    /// assert_eq!(ret, false);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn delete<'a>(&self, txn: &mut RwTxn, key: &'a KC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
    {
        assert_eq_env_db_txn!(self, txn);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let mut key_val = unsafe { crate::into_val(&key_bytes) };

        let result = unsafe {
            mdb_result(ffi::mdb_del(txn.txn.txn, self.dbi, &mut key_val, ptr::null_mut()))
        };

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(e.into()),
        }
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = 27..=42;
    /// let ret = db.delete_range(&mut wtxn, &range)?;
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
    pub fn delete_range<'a, 'txn, R>(&self, txn: &'txn mut RwTxn, range: &'a R) -> Result<usize>
    where
        KC: BytesEncode<'a> + BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq_env_db_txn!(self, txn);

        let mut count = 0;
        let mut iter = self.remap_data_type::<DecodeIgnore>().range_mut(txn, range)?;

        while iter.next().is_some() {
            // safety: We do not keep any reference from the database while using `del_current`.
            //         The user can't keep any reference inside of the database as we ask for a
            //         mutable reference to the `txn`.
            unsafe { iter.del_current()? };
            count += 1;
        }

        Ok(count)
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
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
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
        assert_eq_env_db_txn!(self, txn);

        unsafe { mdb_result(ffi::mdb_drop(txn.txn.txn, self.dbi, 0)).map_err(Into::into) }
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
    /// let db: Database<Unit, Unit> = env.create_database(&mut wtxn, Some("iter-i32"))?;
    ///
    /// # db.clear(&mut wtxn)?;
    /// // We remap the types for ease of use.
    /// let db = db.remap_types::<BEI32, Str>();
    /// db.put(&mut wtxn, &42, "i-am-forty-two")?;
    /// db.put(&mut wtxn, &27, "i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &13, "i-am-thirteen")?;
    /// db.put(&mut wtxn, &521, "i-am-five-hundred-and-twenty-one")?;
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn remap_types<KC2, DC2>(&self) -> Database<KC2, DC2> {
        Database::new(self.env_ident, self.dbi)
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
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Database<KC, DC> {
        Database { env_ident: self.env_ident, dbi: self.dbi, marker: marker::PhantomData }
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
