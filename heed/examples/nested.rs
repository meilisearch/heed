use std::error::Error;
use std::fs;
use std::iter::repeat_with;
use std::path::Path;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new("target").join("heed.mdb");

    fs::create_dir_all(&path)?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3000)
        .open(path)?;

    // here the key will be an str and the data will be a slice of u8
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Bytes> = env.create_database(&mut wtxn, None)?;

    // clear db
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    // -----

    let mut wtxn = env.write_txn()?;
    let mut nwtxn = wtxn.nested_write_txn()?;

    db.put(&mut nwtxn, "what", &[4, 5][..])?;
    let ret = db.get(&nwtxn, "what")?;
    println!("nested(1) \"what\": {:?}", ret);

    println!("nested(1) abort");
    nwtxn.abort();

    let ret = db.get(&wtxn, "what")?;
    println!("parent \"what\": {:?}", ret);

    // ------
    println!();

    // also try with multiple levels of nesting
    let mut nwtxn = wtxn.nested_write_txn()?;
    let mut nnwtxn = nwtxn.nested_write_txn()?;

    db.put(&mut nnwtxn, "humm...", &[6, 7][..])?;
    let ret = db.get(&nnwtxn, "humm...")?;
    println!("nested(2) \"humm...\": {:?}", ret);

    println!("nested(2) commit");
    nnwtxn.commit()?;
    nwtxn.commit()?;

    let ret = db.get(&wtxn, "humm...")?;
    println!("parent \"humm...\": {:?}", ret);

    db.put(&mut wtxn, "hello", &[2, 3][..])?;

    let ret = db.get(&wtxn, "hello")?;
    println!("parent \"hello\": {:?}", ret);

    println!("parent commit");
    wtxn.commit()?;

    // ------
    println!();

    let rtxn = env.read_txn()?;

    let ret = db.get(&rtxn, "hello")?;
    println!("parent (reader) \"hello\": {:?}", ret);

    let ret = db.get(&rtxn, "humm...")?;
    println!("parent (reader) \"humm...\": {:?}", ret);

    drop(rtxn);

    // ------

    println!("We generates 100 nested transactions");
    let wtxn = env.write_txn()?;
    let rtxns: Result<Vec<_>, _> = repeat_with(|| wtxn.env().read_txn()).take(100).collect();
    let rtxns = rtxns?;

    // Always use a different transaction to fetch
    // the stored numbers in the database.
    let mut big_sum = 0usize;
    for rtxn in &rtxns {
        big_sum +=
            db.get(rtxn, "hello").unwrap().unwrap_or_default().iter().copied().sum::<u8>() as usize;
    }
    rtxns.into_iter().for_each(is_send);

    assert_eq!(big_sum, 100 * (2 + 3));
    println!("We computed that the big sum of numbers is {big_sum}");

    Ok(())
}

fn is_send<T: Send>(_: T) {}
