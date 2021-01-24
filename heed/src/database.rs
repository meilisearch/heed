use std::ops::Bound;
use std::borrow::Cow;
use std::{marker, mem, ptr};
use std::ops::RangeBounds;

use crate::*;
use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::types::DecodeIgnore;

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
/// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024) // 10MB
/// #     .max_dbs(3000)
/// #     .open(Path::new("target").join("database.mdb"))?;
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
/// use heed::byteorder::BigEndian;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
/// # let env = EnvOpenOptions::new()
/// #     .map_size(10 * 1024 * 1024) // 10MB
/// #     .max_dbs(3000)
/// #     .open(Path::new("target").join("database.mdb"))?;
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
    env_ident: usize,
    dbi: ffi::MDB_dbi,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(env_ident: usize, dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database { env_ident, dbi, marker: std::marker::PhantomData }
    }

    /// Retrieve the sequence of a database.
    ///
    /// This function allows to retrieve the unique positive integer of this database.
    /// You can see an example usage on the `PolyDatabase::sequence` method documentation.
    #[cfg(all(feature = "mdbx", not(feature = "lmdb")))]
    pub fn sequence<T>(&self, txn: &RoTxn<T>) -> Result<u64> {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

        let mut value = mem::MaybeUninit::uninit();

        let result = unsafe {
            mdb_result(ffi::mdbx_dbi_sequence(
                txn.txn,
                self.dbi,
                value.as_mut_ptr(),
                0, // increment must be 0 for read-only transactions
            ))
        };

        match result {
            Ok(()) => unsafe { Ok(value.assume_init()) },
            Err(e) => Err(e.into()),
        }
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
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);

        use crate::mdb::error::Error;

        let mut value = mem::MaybeUninit::uninit();

        let result = unsafe {
            mdb_result(ffi::mdbx_dbi_sequence(
                txn.txn.txn,
                self.dbi,
                value.as_mut_ptr(),
                increment,
            ))
        };

        match result {
            Ok(()) => unsafe { Ok(Some(value.assume_init())) },
            Err(Error::Other(c)) if c == i32::max_value() => Ok(None), // MDBX_RESULT_TRUE
            Err(e) => Err(e.into()),
        }
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// let db: Database<Str, OwnedType<i32>> = env.create_database(Some("get-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &"i-am-forty-two", &42)?;
    /// db.put(&mut wtxn, &"i-am-twenty-seven", &27)?;
    ///
    /// let ret = db.get(&wtxn, &"i-am-forty-two")?;
    /// assert_eq!(ret, Some(42));
    ///
    /// let ret = db.get(&wtxn, &"i-am-twenty-one")?;
    /// assert_eq!(ret, None);
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn get<'txn, T>(&self, txn: &'txn RoTxn<T>, key: &KC::EItem) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode,
        DC: BytesDecode<'txn>,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = mem::MaybeUninit::uninit();

        let result = unsafe {
            mdb_result(ffi::mdb_get(
                txn.txn,
                self.dbi,
                &mut key_val,
                data_val.as_mut_ptr(),
            ))
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
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
    pub fn get_lower_than<'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
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
    pub fn get_lower_than_or_equal_to<'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
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
    pub fn get_greater_than<'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
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
    pub fn get_greater_than_or_equal_to<'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        key: &KC::EItem,
    ) -> Result<Option<(KC::DItem, DC::DItem)>>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

        let mut cursor = RoCursor::new(txn, self.dbi)?;
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("first-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
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
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("last-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
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
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
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
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
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
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
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
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    ///
    /// let mut iter = db.iter_mut(&mut wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// let ret = iter.del_current()?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = iter.put_current(&BEI32::new(42), &"i-am-the-new-forty-two")?;
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
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
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
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    ///
    /// let mut iter = db.rev_iter_mut(&mut wtxn)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = iter.del_current()?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(13), "i-am-thirteen")));
    /// let ret = iter.put_current(&BEI32::new(13), &"i-am-the-new-thirteen")?;
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
    pub fn rev_iter_mut<'txn, T>(&self, txn: &'txn mut RwTxn<T>) -> Result<RwRevIter<'txn, KC, DC>> {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
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
    pub fn range<'txn, T, R>(
        &self,
        txn: &'txn RoTxn<T>,
        range: R,
    ) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let mut range = db.range_mut(&mut wtxn, range)?;
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// let ret = range.del_current()?;
    /// assert!(ret);
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = range.put_current(&BEI32::new(42), &"i-am-the-new-forty-two")?;
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
    pub fn range_mut<'txn, T, R>(
        &self,
        txn: &'txn mut RwTxn<T>,
        range: R,
    ) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);

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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(43);
    /// let mut iter = db.rev_range(&wtxn, range)?;
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// assert_eq!(iter.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_range<'txn, T, R>(
        &self,
        txn: &'txn RoTxn<T>,
        range: R,
    ) -> Result<RoRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);

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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
    ///
    /// let range = BEI32::new(27)..=BEI32::new(42);
    /// let mut range = db.rev_range_mut(&mut wtxn, range)?;
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(42), "i-am-forty-two")));
    /// let ret = range.del_current()?;
    /// assert!(ret);
    /// assert_eq!(range.next().transpose()?, Some((BEI32::new(27), "i-am-twenty-seven")));
    /// let ret = range.put_current(&BEI32::new(27), &"i-am-the-new-twenty-seven")?;
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
    pub fn rev_range_mut<'txn, T, R>(
        &self,
        txn: &'txn mut RwTxn<T>,
        range: R,
    ) -> Result<RwRevRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);

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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Str, OwnedType<BEI32>> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &"i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put(&mut wtxn, &"i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put(&mut wtxn, &"i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put(&mut wtxn, &"i-am-forty-one",    &BEI32::new(41))?;
    /// db.put(&mut wtxn, &"i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.prefix_iter(&mut wtxn, &"i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter<'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        prefix: &KC::EItem,
    ) -> Result<RoPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Str, OwnedType<BEI32>> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &"i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put(&mut wtxn, &"i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put(&mut wtxn, &"i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put(&mut wtxn, &"i-am-forty-one",    &BEI32::new(41))?;
    /// db.put(&mut wtxn, &"i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.prefix_iter_mut(&mut wtxn, &"i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// let ret = iter.del_current()?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// let ret = iter.put_current(&"i-am-twenty-seven", &BEI32::new(27000))?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get(&wtxn, &"i-am-twenty-eight")?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, &"i-am-twenty-seven")?;
    /// assert_eq!(ret, Some(BEI32::new(27000)));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn prefix_iter_mut<'txn, T>(
        &self,
        txn: &'txn mut RwTxn<T>,
        prefix: &KC::EItem,
    ) -> Result<RwPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Str, OwnedType<BEI32>> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &"i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put(&mut wtxn, &"i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put(&mut wtxn, &"i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put(&mut wtxn, &"i-am-forty-one",    &BEI32::new(41))?;
    /// db.put(&mut wtxn, &"i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.rev_prefix_iter(&mut wtxn, &"i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_prefix_iter<'txn, T>(
        &self,
        txn: &'txn RoTxn<T>,
        prefix: &KC::EItem,
    ) -> Result<RoRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        assert_eq!(self.env_ident, txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Str, OwnedType<BEI32>> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &"i-am-twenty-eight", &BEI32::new(28))?;
    /// db.put(&mut wtxn, &"i-am-twenty-seven", &BEI32::new(27))?;
    /// db.put(&mut wtxn, &"i-am-twenty-nine",  &BEI32::new(29))?;
    /// db.put(&mut wtxn, &"i-am-forty-one",    &BEI32::new(41))?;
    /// db.put(&mut wtxn, &"i-am-forty-two",    &BEI32::new(42))?;
    ///
    /// let mut iter = db.rev_prefix_iter_mut(&mut wtxn, &"i-am-twenty")?;
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-seven", BEI32::new(27))));
    /// let ret = iter.del_current()?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-nine", BEI32::new(29))));
    /// assert_eq!(iter.next().transpose()?, Some(("i-am-twenty-eight", BEI32::new(28))));
    /// let ret = iter.put_current(&"i-am-twenty-eight", &BEI32::new(28000))?;
    /// assert!(ret);
    ///
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    ///
    /// let ret = db.get(&wtxn, &"i-am-twenty-seven")?;
    /// assert_eq!(ret, None);
    ///
    /// let ret = db.get(&wtxn, &"i-am-twenty-eight")?;
    /// assert_eq!(ret, Some(BEI32::new(28000)));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn rev_prefix_iter_mut<'txn, T>(
        &self,
        txn: &'txn mut RwTxn<T>,
        prefix: &KC::EItem,
    ) -> Result<RwRevPrefix<'txn, KC, DC>>
    where
        KC: BytesEncode,
    {
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn put<T>(&self, txn: &mut RwTxn<T>, key: &KC::EItem, data: &DC::EItem) -> Result<()>
    where
        KC: BytesEncode,
        DC: BytesEncode,
    {
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).map_err(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = unsafe { crate::into_val(&data_bytes) };
        let flags = 0;

        unsafe {
            mdb_result(ffi::mdb_put(
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
    ///
    /// let ret = db.get(&mut wtxn, &BEI32::new(27))?;
    /// assert_eq!(ret, Some("i-am-twenty-seven"));
    ///
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn append<T>(&self, txn: &mut RwTxn<T>, key: &KC::EItem, data: &DC::EItem) -> Result<()>
    where
        KC: BytesEncode,
        DC: BytesEncode,
    {
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).map_err(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
        let mut data_val = unsafe { crate::into_val(&data_bytes) };
        let flags = ffi::MDB_APPEND;

        unsafe {
            mdb_result(ffi::mdb_put(
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
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
    pub fn delete<T>(&self, txn: &mut RwTxn<T>, key: &KC::EItem) -> Result<bool>
    where
        KC: BytesEncode,
    {
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);

        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).map_err(Error::Encoding)?;
        let mut key_val = unsafe { crate::into_val(&key_bytes) };

        let result = unsafe {
            mdb_result(ffi::mdb_del(
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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
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
    pub fn delete_range<'txn, T, R>(&self, txn: &'txn mut RwTxn<T>, range: R) -> Result<usize>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);

        let mut count = 0;
        let mut iter = self.remap_data_type::<DecodeIgnore>().range_mut(txn, range)?;

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
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<OwnedType<BEI32>, Str> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
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
        assert_eq!(self.env_ident, txn.txn.env.env_mut_ptr() as usize);
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
    /// # fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// # let env = EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(Path::new("target").join("database.mdb"))?;
    /// type BEI32 = I32<BigEndian>;
    ///
    /// let db: Database<Unit, Unit> = env.create_database(Some("iter-i32"))?;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// # db.clear(&mut wtxn)?;
    /// // We remap the types for ease of use.
    /// let db = db.remap_types::<OwnedType<BEI32>, Str>();
    /// db.put(&mut wtxn, &BEI32::new(42), &"i-am-forty-two")?;
    /// db.put(&mut wtxn, &BEI32::new(27), &"i-am-twenty-seven")?;
    /// db.put(&mut wtxn, &BEI32::new(13), &"i-am-thirteen")?;
    /// db.put(&mut wtxn, &BEI32::new(521), &"i-am-five-hundred-and-twenty-one")?;
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
        Database {
            env_ident: self.env_ident,
            dbi: self.dbi,
            marker: marker::PhantomData,
        }
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}
