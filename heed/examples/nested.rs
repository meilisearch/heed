use std::error::Error;
use std::fs;
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
    let db: Database<Str, ByteSlice> = env.create_database(&mut wtxn, None)?;

    // clear db
    db.clear(&wtxn)?;
    wtxn.commit()?;

    // -----

    let mut wtxn = env.write_txn()?;
    let nwtxn = env.nested_write_txn(&mut wtxn)?;

    db.put(&nwtxn, "what", &[4, 5][..])?;
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
    let nnwtxn = env.nested_write_txn(&mut nwtxn)?;

    db.put(&nnwtxn, "humm...", &[6, 7][..])?;
    let ret = db.get(&nnwtxn, "humm...")?;
    println!("nested(2) \"humm...\": {:?}", ret);

    println!("nested(2) commit");
    nnwtxn.commit()?;
    nwtxn.commit()?;

    let ret = db.get(&wtxn, "humm...")?;
    println!("parent \"humm...\": {:?}", ret);

    db.put(&wtxn, "hello", &[2, 3][..])?;

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

    Ok(())
}
