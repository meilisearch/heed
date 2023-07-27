use std::error::Error;
use std::fs;
use std::path::Path;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

/// This example exposes some of the possibilities using
/// a non-mutable RwTxn to iterate on a database permits.
fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new("target").join("heed.mdb");

    fs::create_dir_all(&path)?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3000)
        .open(path)?;

    // here the key will be an str and the data will be a slice of u8
    let mut wtxn = env.write_txn()?;
    let one: Database<Str, ByteSlice> = env.create_database(&mut wtxn, Some("one"))?;
    let two: Database<Str, ByteSlice> = env.create_database(&mut wtxn, Some("two"))?;
    let numbers: Database<U8, Unit> = env.create_database(&mut wtxn, Some("numbers"))?;

    // clear db
    one.clear(&wtxn)?;
    two.clear(&wtxn)?;
    wtxn.commit()?;

    // -----

    let wtxn = env.write_txn()?;
    for number in 0u32..1000 {
        let k = number.to_string();
        let v = number.to_be_bytes();
        one.put(&wtxn, &k, &v)?;
    }

    for result in one.iter(&wtxn)? {
        let (k, v) = result?;
        two.put(&wtxn, k, v)?;
    }

    for (res1, res2) in one.iter(&wtxn)?.zip(two.iter(&wtxn)?) {
        let (k1, v1) = res1?;
        let (k2, v2) = res2?;
        assert_eq!(k1, k2);
        assert_eq!(v1, v2);
    }

    wtxn.commit()?;

    // -----

    let wtxn = env.write_txn()?;
    for result in one.iter(&wtxn)? {
        let (k, v) = result?;
        if k == "10" {
            let n: u32 = 11;
            one.put(&wtxn, &n.to_string(), &[])?;
        }
        if k == "11" {
            assert!(v.is_empty());
        }
    }

    wtxn.commit()?;

    // -----

    let wtxn = env.write_txn()?;
    let n: u32 = 100_000;
    one.put(&wtxn, &n.to_string(), &n.to_be_bytes())?;
    let v = one.get(&wtxn, &n.to_string())?.unwrap();
    one.put(&wtxn, &n.to_string(), v)?;

    let v = one.get(&wtxn, &n.to_string())?.unwrap();
    assert_eq!(v, n.to_be_bytes());

    wtxn.commit()?;

    // -----

    // What happen when we clear the database while iterating on it?
    let wtxn = env.write_txn()?;
    for result in one.remap_data_type::<DecodeIgnore>().iter(&wtxn)? {
        let (k, _) = result?;
        if k == "10" {
            one.clear(&wtxn)?;
        }
    }

    assert!(one.is_empty(&wtxn)?);
    wtxn.commit()?;

    // -----

    // We can also insert values along the way, while we are iterating
    let wtxn = env.write_txn()?;
    numbers.put(&wtxn, &0, &())?;
    for (result, expected) in numbers.iter(&wtxn)?.zip(0..=u8::MAX) {
        let (i, ()) = result?;
        assert_eq!(i, expected);
        if let Some(next) = i.checked_add(1) {
            numbers.put(&wtxn, &next, &())?;
        }
    }

    wtxn.commit()?;

    Ok(())
}
