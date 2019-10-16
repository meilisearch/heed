use std::marker;
use std::ops::RangeBounds;
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
    pub(crate) dyndb: DynDatabase,
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub(crate) fn new(dbi: ffi::MDB_dbi) -> Database<KC, DC> {
        Database { dyndb: DynDatabase::new(dbi), marker: std::marker::PhantomData }
    }

    pub fn get<'txn>(&self, txn: &'txn RoTxn, key: &KC::EItem) -> Result<Option<DC::DItem>>
    where
        KC: BytesEncode,
        DC: BytesDecode<'txn>,
    {
        self.dyndb.get::<KC, DC>(txn, key)
    }

    pub fn iter<'txn>(&self, txn: &'txn RoTxn) -> Result<RoIter<'txn, KC, DC>> {
        self.dyndb.iter::<KC, DC>(txn)
    }

    pub fn iter_mut<'txn>(&self, txn: &'txn mut RwTxn) -> Result<RwIter<'txn, KC, DC>> {
        self.dyndb.iter_mut::<KC, DC>(txn)
    }

    pub fn range<'txn, R>(&self, txn: &'txn RoTxn, range: R) -> Result<RoRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.range::<KC, DC, R>(txn, range)
    }

    pub fn range_mut<'txn, R>(&self, txn: &'txn mut RwTxn, range: R) -> Result<RwRange<'txn, KC, DC>>
    where
        KC: BytesEncode,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.range_mut::<KC, DC, R>(txn, range)
    }

    pub fn put(&self, txn: &mut RwTxn, key: &KC::EItem, data: &DC::EItem) -> Result<()>
    where
        KC: BytesEncode,
        DC: BytesEncode,
    {
        self.dyndb.put::<KC, DC>(txn, key, data)
    }

    pub fn delete(&self, txn: &mut RwTxn, key: &KC::EItem) -> Result<bool>
    where
        KC: BytesEncode,
    {
        self.dyndb.delete::<KC>(txn, key)
    }

    pub fn delete_range<'txn, R>(&self, txn: &'txn mut RwTxn, range: R) -> Result<usize>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
        R: RangeBounds<KC::EItem>,
    {
        self.dyndb.delete_range::<KC, DC, R>(txn, range)
    }

    pub fn clear(&self, txn: &mut RwTxn) -> Result<()> {
        self.dyndb.clear(txn)
    }
}

impl<KC, DC> Clone for Database<KC, DC> {
    fn clone(&self) -> Database<KC, DC> {
        Database { dyndb: self.dyndb, marker: marker::PhantomData }
    }
}

impl<KC, DC> Copy for Database<KC, DC> {}
