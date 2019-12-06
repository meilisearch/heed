use std::error::Error;
use std::fs;
use std::path::Path;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(Path::new("target").join("zerocopy.mdb"))?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .max_dbs(3000)
        .open(Path::new("target").join("zerocopy.mdb"))?;

    // here the key will be an str and the data will be a slice of u8
    let db: Database<Str, ByteSlice> = env.create_database(None)?;

    // clear db
    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    // -----

    let grtxn = env.read_txn()?;
    let mut wtxn = env.write_txn()?;

    let mut nwtxn = env.nested_write_txn(&mut wtxn)?;

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
    let mut nwtxn = env.nested_write_txn(&mut wtxn)?;
    let mut nnwtxn = env.nested_write_txn(&mut nwtxn)?;

    db.put(&mut nnwtxn, "humm...", &[6, 7][..])?;
    let ret = db.get(&nnwtxn, "humm...")?;
    println!("nested(2) \"humm...\": {:?}", ret);

    println!("nested(2) commit");
    nnwtxn.commit()?;
    nwtxn.commit()?;

    let ret = db.get(&grtxn, "humm...")?;
    println!("grand parent (reader) \"humm...\": {:?}", ret);
    grtxn.abort();

    let ret = db.get(&wtxn, "humm...")?;
    println!("parent \"humm...\": {:?}", ret);

    db.put(&mut wtxn, "hello", &[2, 3][..])?;

    let ret = db.get(&wtxn, "hello")?;
    println!("parent \"hello\": {:?}", ret);

    println!("parent commit");
    wtxn.commit()?;
    // wtxn.abort();

    // ------
    println!();

    let rtxn = env.read_txn()?;

    let ret = db.get(&rtxn, "hello")?;
    println!("parent (reader) \"hello\": {:?}", ret);

    let ret = db.get(&rtxn, "humm...")?;
    println!("parent (reader) \"humm...\": {:?}", ret);

    Ok(())
}
