//! A cookbook of examples on how to use heed. Here is the list of the different topics you can learn about:
//!
//! - [Decode Values on Demand](#decode-values-on-demand)
//! - [Listing and Opening the Named Databases](#listing-and-opening-the-named-databases)
//! - [Create Custom and Prefix Codecs](#create-custom-and-prefix-codecs)
//! - [Change the Environment Size Dynamically](#change-the-environment-size-dynamically)
//! - [Advanced Multithreaded Access of Entries](#advanced-multithreaded-access-of-entries)
//! - [Custom Key Comparator](#custom-key-comparator)
//! - [Custom Dupsort Comparator](#custom-dupsort-comparator)
//!
//! # Decode Values on Demand
//!
//! Sometimes, you need to iterate on the content of a database and
//! conditionnaly decode the value depending on the key. You can use the
//! [`Database::lazily_decode_data`] method to indicate this to heed.
//!
//! ```
//! use std::collections::HashMap;
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//!
//! use heed::types::*;
//! use heed::{Database, EnvOpenOptions};
//!
//! pub type StringMap = HashMap<String, String>;
//!
//! fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
//!     let path = Path::new("target").join("heed.mdb");
//!
//!     fs::create_dir_all(&path)?;
//!
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(1024 * 1024 * 100) // 100 MiB
//!             .open(&path)?
//!     };
//!
//!     let mut wtxn = env.write_txn()?;
//!     let db: Database<Str, SerdeJson<StringMap>> = env.create_database(&mut wtxn, None)?;
//!
//!     fill_with_data(&mut wtxn, db)?;
//!
//!     // We make sure that iterating over this database will
//!     // not deserialize the values. We just want to decode
//!     // the value corresponding to 43th key.
//!     for (i, result) in db.lazily_decode_data().iter(&wtxn)?.enumerate() {
//!         let (_key, lazy_value) = result?;
//!         if i == 43 {
//!             // This is where the magic happens. We receive a Lazy type
//!             // that wraps a slice of bytes. We can decode on purpose.
//!             let value = lazy_value.decode()?;
//!             assert_eq!(value.get("secret"), Some(&String::from("434343")));
//!             break;
//!         }
//!     }
//!
//!     Ok(())
//! }
//!
//! fn fill_with_data(
//!     wtxn: &mut heed::RwTxn,
//!     db: Database<Str, SerdeJson<StringMap>>,
//! ) -> heed::Result<()> {
//!     // This represents a very big value that we only want to decode when necessary.
//!     let mut big_string_map = HashMap::new();
//!     big_string_map.insert("key1".into(), "I am a very long string".into());
//!     big_string_map.insert("key2".into(), "I am a also very long string".into());
//!
//!     for i in 0..100 {
//!         let key = format!("{i:5}");
//!         big_string_map.insert("secret".into(), format!("{i}{i}{i}"));
//!         db.put(wtxn, &key, &big_string_map)?;
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Listing and Opening the Named Databases
//!
//! Sometimes it is useful to list the databases available in an environment.
//! LMDB automatically stores their names in the unnamed database, a database that doesn't
//! need to be created in which you can write.
//!
//! Once you create new databases, after defining the [`EnvOpenOptions::max_dbs`]
//! parameter, the names of those databases are automatically stored in the unnamed one.
//!
//! ```
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//!
//! use heed::types::*;
//! use heed::{Database, EnvOpenOptions};
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let env_path = Path::new("target").join("heed.mdb");
//!
//!     fs::create_dir_all(&env_path)?;
//!
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(10 * 1024 * 1024) // 10MB
//!             .max_dbs(3) // Number of opened databases
//!             .open(env_path)?
//!     };
//!
//!     let rtxn = env.read_txn()?;
//!     // The database names are mixed with the user entries therefore we prefer
//!     // ignoring the values and try to open the databases one by one using the keys.
//!     let unnamed: Database<Str, DecodeIgnore> =
//!         env.open_database(&rtxn, None)?.expect("the unnamed database always exists");
//!
//!     // The unnamed (or main) database contains the other
//!     // database names associated to empty values.
//!     for result in unnamed.iter(&rtxn)? {
//!         let (name, ()) = result?;
//!
//!         if let Ok(Some(_db)) = env.open_database::<Str, Bytes>(&rtxn, Some(name)) {
//!             // We succeeded into opening a new database that
//!             // contains strings associated to raw bytes.
//!         }
//!     }
//!
//!     // When opening databases in a read-only transaction
//!     // you must commit your read transaction to make your
//!     // freshly opened databases globally available.
//!     rtxn.commit()?;
//!
//!     // If you abort (or drop) your read-only transaction
//!     // the database handle will be invalid outside
//!     // the transaction scope.
//!
//!     Ok(())
//! }
//! ```
//!
//! # Create Custom and Prefix Codecs
//!
//! With heed you can store any kind of data and serialize it the way you want.
//! To do so you'll need to create a codec by using the [`BytesEncode`] and [`BytesDecode`] traits.
//!
//! Now imagine that your data is lexicographically well ordered. You can now leverage
//! the use of prefix codecs. Those are classic codecs but are only used to encode key prefixes.
//!
//! In this example we will store logs associated to a timestamp. By encoding the timestamp
//! in big endian we can create a prefix codec that restricts a subset of the data. It is recommended
//! to create codecs to encode prefixes when possible instead of using a slice of bytes.
//!
//! ```
//! use std::borrow::Cow;
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//!
//! use heed::types::*;
//! use heed::{BoxedError, BytesDecode, BytesEncode, Database, EnvOpenOptions};
//!
//! #[derive(Debug, PartialEq, Eq)]
//! pub enum Level {
//!     Debug,
//!     Warn,
//!     Error,
//! }
//!
//! #[derive(Debug, PartialEq, Eq)]
//! pub struct LogKey {
//!     timestamp: u32,
//!     level: Level,
//! }
//!
//! pub struct LogKeyCodec;
//!
//! impl<'a> BytesEncode<'a> for LogKeyCodec {
//!     type EItem = LogKey;
//!
//!     /// Encodes the u32 timestamp in big endian followed by the log level with a single byte.
//!     fn bytes_encode(log: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
//!         let (timestamp_bytes, level_byte) = match log {
//!             LogKey { timestamp, level: Level::Debug } => (timestamp.to_be_bytes(), 0),
//!             LogKey { timestamp, level: Level::Warn } => (timestamp.to_be_bytes(), 1),
//!             LogKey { timestamp, level: Level::Error } => (timestamp.to_be_bytes(), 2),
//!         };
//!
//!         let mut output = Vec::new();
//!         output.extend_from_slice(&timestamp_bytes);
//!         output.push(level_byte);
//!         Ok(Cow::Owned(output))
//!     }
//! }
//!
//! impl<'a> BytesDecode<'a> for LogKeyCodec {
//!     type DItem = LogKey;
//!
//!     fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
//!         use std::mem::size_of;
//!
//!         let timestamp = match bytes.get(..size_of::<u32>()) {
//!             Some(bytes) => bytes.try_into().map(u32::from_be_bytes).unwrap(),
//!             None => return Err("invalid log key: cannot extract timestamp".into()),
//!         };
//!
//!         let level = match bytes.get(size_of::<u32>()) {
//!             Some(&0) => Level::Debug,
//!             Some(&1) => Level::Warn,
//!             Some(&2) => Level::Error,
//!             Some(_) => return Err("invalid log key: invalid log level".into()),
//!             None => return Err("invalid log key: cannot extract log level".into()),
//!         };
//!
//!         Ok(LogKey { timestamp, level })
//!     }
//! }
//!
//! /// Encodes the high part of a timestamp. As it is located
//! /// at the start of the key it can be used to only return
//! /// the logs that appeared during a, rather long, period.
//! pub struct LogAtHalfTimestampCodec;
//!
//! impl<'a> BytesEncode<'a> for LogAtHalfTimestampCodec {
//!     type EItem = u32;
//!
//!     /// This method encodes only the prefix of the keys in this particular case, the timestamp.
//!     fn bytes_encode(half_timestamp: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
//!         Ok(Cow::Owned(half_timestamp.to_be_bytes()[..2].to_vec()))
//!     }
//! }
//!
//! impl<'a> BytesDecode<'a> for LogAtHalfTimestampCodec {
//!     type DItem = LogKey;
//!
//!     fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
//!         LogKeyCodec::bytes_decode(bytes)
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let path = Path::new("target").join("heed.mdb");
//!
//!     fs::create_dir_all(&path)?;
//!
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(10 * 1024 * 1024) // 10MB
//!             .max_dbs(3000)
//!             .open(path)?
//!     };
//!
//!     let mut wtxn = env.write_txn()?;
//!     let db: Database<LogKeyCodec, Str> = env.create_database(&mut wtxn, None)?;
//!
//!     db.put(
//!         &mut wtxn,
//!         &LogKey { timestamp: 1608326232, level: Level::Debug },
//!         "this is a very old log",
//!     )?;
//!     db.put(
//!         &mut wtxn,
//!         &LogKey { timestamp: 1708326232, level: Level::Debug },
//!         "fibonacci was executed in 21ms",
//!     )?;
//!     db.put(&mut wtxn, &LogKey { timestamp: 1708326242, level: Level::Error }, "fibonacci crashed")?;
//!     db.put(
//!         &mut wtxn,
//!         &LogKey { timestamp: 1708326272, level: Level::Warn },
//!         "fibonacci is running since 12s",
//!     )?;
//!
//!     // We change the way we want to read our database by changing the key codec.
//!     // In this example we can prefix search only for the logs between a period of time
//!     // (the two high bytes of the u32 timestamp).
//!     let iter = db.remap_key_type::<LogAtHalfTimestampCodec>().prefix_iter(&wtxn, &1708326232)?;
//!
//!     // As we filtered the log for a specific
//!     // period of time we must not see the very old log.
//!     for result in iter {
//!         let (LogKey { timestamp: _, level: _ }, content) = result?;
//!         assert_ne!(content, "this is a very old log");
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Change the Environment Size Dynamically
//!
//! You must specify the maximum size of an LMDB environment when you open it.
//! Environment do not dynamically increase there size for performance reasons and also to
//! have more control on it.
//!
//! Here is a simple example on the way to go to dynamically increase the size
//! of an environment when you detect that it is going out of space.
//!
//! ```
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//!
//! use heed::types::*;
//! use heed::{Database, EnvOpenOptions};
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let path = Path::new("target").join("small-space.mdb");
//!
//!     fs::create_dir_all(&path)?;
//!
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(16384) // one page
//!             .open(&path)?
//!     };
//!
//!     let mut wtxn = env.write_txn()?;
//!     let db: Database<Str, Str> = env.create_database(&mut wtxn, None)?;
//!
//!     // Ho! Crap! We don't have enough space in this environment...
//!     assert!(matches!(
//!         fill_with_data(&mut wtxn, db),
//!         Err(heed::Error::Mdb(heed::MdbError::MapFull))
//!     ));
//!
//!     drop(wtxn);
//!
//!     // We need to increase the page size and we can only do that
//!     // when no transaction are running so closing the env is easier.
//!     env.prepare_for_closing().wait();
//!
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(10 * 16384) // 10 pages
//!             .open(&path)?
//!     };
//!
//!     let mut wtxn = env.write_txn()?;
//!     let db: Database<Str, Str> = env.create_database(&mut wtxn, None)?;
//!
//!     // We now have enough space in the env to store all of our entries.
//!     assert!(matches!(fill_with_data(&mut wtxn, db), Ok(())));
//!
//!     Ok(())
//! }
//!
//! fn fill_with_data(wtxn: &mut heed::RwTxn, db: Database<Str, Str>) -> heed::Result<()> {
//!     for i in 0..1000 {
//!         let key = i.to_string();
//!         db.put(wtxn, &key, "I am a very long string")?;
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # Advanced Multithreaded Access of Entries
//!
//! LMDB disallow sharing cursors amongs threads. It is only possible to send
//! them between threads when the heed `read-txn-no-tls` feature is enabled.
//!
//! This limits some usecases that require a parallel access to the content of the databases
//! to process stuff faster. This is the case of arroy, a multithreads fast approximate
//! neighbors search library. I wrote [an article explaining how
//! to read entries in parallel][arroy article].
//!
//! It is forbidden to write in an environement while reading in it. However, it is possible
//! to keep pointers to the values of the entries returned by LMDB. Those pointers are valid
//! until the end of the transaction.
//!
//! Here is a small example on how to declare a datastructure to be used in parallel across thread,
//! safely. The unsafe part declare that the datastructure can be shared between thread despite
//! the write transaction not being `Send` nor `Sync`.
//!
//! [arroy article]: https://blog.kerollmops.com/multithreading-and-memory-mapping-refining-ann-performance-with-arroy
//!
//! ```
//! use std::collections::HashMap;
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//!
//! use heed::types::*;
//! use heed::{Database, EnvOpenOptions, RoTxn};
//!
//! fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
//!     let path = Path::new("target").join("heed.mdb");
//!
//!     fs::create_dir_all(&path)?;
//!
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(1024 * 1024 * 100) // 100 MiB
//!             .open(&path)?
//!     };
//!
//!     let mut wtxn = env.write_txn()?;
//!     let db: Database<Str, Str> = env.create_database(&mut wtxn, None)?;
//!
//!     fill_with_data(&mut wtxn, db)?;
//!
//!     let immutable_map = ImmutableMap::from_db(&wtxn, db)?;
//!
//!     // We can share the immutable map over multiple threads because it is Sync.
//!     // It is safe because we keep the write transaction lifetime in this type.
//!     std::thread::scope(|s| {
//!         s.spawn(|| {
//!             let value = immutable_map.get("10");
//!             assert_eq!(value, Some("I am a very long string"));
//!         });
//!         s.spawn(|| {
//!             let value = immutable_map.get("20");
//!             assert_eq!(value, Some("I am a very long string"));
//!         });
//!     });
//!
//!     // You can see that we always have it on the main thread.
//!     // We didn't sent it over threads.
//!     let value = immutable_map.get("50");
//!     assert_eq!(value, Some("I am a very long string"));
//!
//!     Ok(())
//! }
//!
//! fn fill_with_data(wtxn: &mut heed::RwTxn, db: Database<Str, Str>) -> heed::Result<()> {
//!     for i in 0..100 {
//!         let key = i.to_string();
//!         db.put(wtxn, &key, "I am a very long string")?;
//!     }
//!     Ok(())
//! }
//!
//! struct ImmutableMap<'a> {
//!     map: HashMap<&'a str, &'a str>,
//! }
//!
//! impl<'t> ImmutableMap<'t> {
//!     fn from_db(rtxn: &'t RoTxn, db: Database<Str, Str>) -> heed::Result<Self> {
//!         let mut map = HashMap::new();
//!         for result in db.iter(rtxn)? {
//!             let (k, v) = result?;
//!             map.insert(k, v);
//!         }
//!         Ok(ImmutableMap { map })
//!     }
//!
//!     fn get(&self, key: &str) -> Option<&'t str> {
//!         self.map.get(key).copied()
//!     }
//! }
//!
//! unsafe impl Sync for ImmutableMap<'_> {}
//! ```
//!
//! # Custom Key Comparator
//!
//! LMDB keys are sorted in lexicographic order by default. To change this behavior you can implement a custom [`Comparator`]
//! and provide it when creating the database.
//!
//! Under the hood this translates into a [`mdb_set_compare`] call.
//!
//! ```
//! use std::cmp::Ordering;
//! use std::error::Error;
//! use std::path::Path;
//! use std::{fs, str};
//!
//! use heed::EnvOpenOptions;
//! use heed_traits::Comparator;
//! use heed_types::{Str, Unit};
//!
//! enum StringAsIntCmp {}
//!
//! // This function takes two strings which represent positive numbers,
//! // parses them into i32s and compare the parsed value.
//! // Therefore "-1000" < "-100" must be true even without '0' padding.
//! impl Comparator for StringAsIntCmp {
//!     fn compare(a: &[u8], b: &[u8]) -> Ordering {
//!         let a: i32 = str::from_utf8(a).unwrap().parse().unwrap();
//!         let b: i32 = str::from_utf8(b).unwrap().parse().unwrap();
//!         a.cmp(&b)
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let env_path = Path::new("target").join("custom-key-cmp.mdb");
//!
//!     let _ = fs::remove_dir_all(&env_path);
//!
//!     fs::create_dir_all(&env_path)?;
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(10 * 1024 * 1024) // 10MB
//!             .max_dbs(3)
//!             .open(env_path)?
//!     };
//!
//!     let mut wtxn = env.write_txn()?;
//!     let db = env
//!         .database_options()
//!         .types::<Str, Unit>()
//!         .key_comparator::<StringAsIntCmp>()
//!         .create(&mut wtxn)?;
//!     wtxn.commit()?;
//!
//!     let mut wtxn = env.write_txn()?;
//!
//!     // We fill our database with entries.
//!     db.put(&mut wtxn, "-100000", &())?;
//!     db.put(&mut wtxn, "-10000", &())?;
//!     db.put(&mut wtxn, "-1000", &())?;
//!     db.put(&mut wtxn, "-100", &())?;
//!     db.put(&mut wtxn, "100", &())?;
//!
//!     // We check that the key are in the right order ("-100" < "-1000" < "-10000"...)
//!     let mut iter = db.iter(&wtxn)?;
//!     assert_eq!(iter.next().transpose()?, Some(("-100000", ())));
//!     assert_eq!(iter.next().transpose()?, Some(("-10000", ())));
//!     assert_eq!(iter.next().transpose()?, Some(("-1000", ())));
//!     assert_eq!(iter.next().transpose()?, Some(("-100", ())));
//!     assert_eq!(iter.next().transpose()?, Some(("100", ())));
//!     drop(iter);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Custom Dupsort Comparator
//!
//! When using DUPSORT LMDB sorts values of the same key in lexicographic order by default. To change this behavior you can implement a custom [`Comparator`]
//! and provide it when creating the database.
//!
//! Under the hood this translates into a [`mdb_set_dupsort`] call.
//!
//! ```
//! use std::cmp::Ordering;
//! use std::error::Error;
//! use std::fs;
//! use std::path::Path;
//!
//! use byteorder::BigEndian;
//! use heed::{DatabaseFlags, EnvOpenOptions};
//! use heed_traits::Comparator;
//! use heed_types::{Str, U128};
//!
//! enum DescendingIntCmp {}
//!
//! impl Comparator for DescendingIntCmp {
//!     fn compare(a: &[u8], b: &[u8]) -> Ordering {
//!         b.cmp(&a)
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let env_path = Path::new("target").join("custom-dupsort-cmp.mdb");
//!
//!     let _ = fs::remove_dir_all(&env_path);
//!
//!     fs::create_dir_all(&env_path)?;
//!     let env = unsafe {
//!         EnvOpenOptions::new()
//!             .map_size(10 * 1024 * 1024) // 10MB
//!             .max_dbs(3)
//!            .open(env_path)?
//!     };
//!
//!     let mut wtxn = env.write_txn()?;
//!     let db = env
//!         .database_options()
//!         .types::<Str, U128<BigEndian>>()
//!         .flags(DatabaseFlags::DUP_SORT)
//!         .dup_sort_comparator::<DescendingIntCmp>()
//!         .create(&mut wtxn)?;
//!     wtxn.commit()?;
//!
//!     let mut wtxn = env.write_txn()?;
//!
//!     // We fill our database with entries.
//!     db.put(&mut wtxn, "1", &1)?;
//!     db.put(&mut wtxn, "1", &2)?;
//!     db.put(&mut wtxn, "1", &3)?;
//!     db.put(&mut wtxn, "2", &4)?;
//!     db.put(&mut wtxn, "1", &5)?;
//!     db.put(&mut wtxn, "0", &0)?;
//!
//!     // We check that the keys are in lexicographic and values in descending order.
//!     let mut iter = db.iter(&wtxn)?;
//!     assert_eq!(iter.next().transpose()?, Some(("0", 0)));
//!     assert_eq!(iter.next().transpose()?, Some(("1", 5)));
//!     assert_eq!(iter.next().transpose()?, Some(("1", 3)));
//!     assert_eq!(iter.next().transpose()?, Some(("1", 2)));
//!     assert_eq!(iter.next().transpose()?, Some(("1", 1)));
//!     assert_eq!(iter.next().transpose()?, Some(("2", 4)));
//!     drop(iter);
//!
//!     Ok(())
//! }
//! ```
//!

// To let cargo generate doc links
#![allow(unused_imports)]

use crate::{
    mdb::ffi::mdb_set_compare, mdb::ffi::mdb_set_dupsort, BytesDecode, BytesEncode, Comparator,
    Database, EnvOpenOptions,
};
