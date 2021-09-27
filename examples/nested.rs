use std::error::Error;
use std::fs;
use std::path::Path;

use heed::EnvOpenOptions;

fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new("target").join("heed.mdb");

    fs::create_dir_all(&path)?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3000)
        .open(path)?;

    // here the key will be an str and the data will be a slice of u8
    let db = env.create_database(None)?;

    // clear db
    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    // -----

    let mut wtxn = env.write_txn()?;
    let mut nwtxn = env.nested_write_txn(&mut wtxn)?;

    db.put(&mut nwtxn, b"what", &[4, 5][..])?;
    let ret = db.get(&nwtxn, b"what")?;
    println!("nested(1) \"what\": {:?}", ret);

    println!("nested(1) abort");
    nwtxn.abort()?;

    let ret = db.get(&wtxn, b"what")?;
    println!("parent \"what\": {:?}", ret);

    // ------
    println!();

    // also try with multiple levels of nesting
    let mut nwtxn = env.nested_write_txn(&mut wtxn)?;
    let mut nnwtxn = env.nested_write_txn(&mut nwtxn)?;

    db.put(&mut nnwtxn, b"humm...", &[6, 7][..])?;
    let ret = db.get(&nnwtxn, b"humm...")?;
    println!("nested(2) \"humm...\": {:?}", ret);

    println!("nested(2) commit");
    nnwtxn.commit()?;
    nwtxn.commit()?;

    let ret = db.get(&wtxn, b"humm...")?;
    println!("parent \"humm...\": {:?}", ret);

    db.put(&mut wtxn, b"hello", &[2, 3][..])?;

    let ret = db.get(&wtxn, b"hello")?;
    println!("parent \"hello\": {:?}", ret);

    println!("parent commit");
    wtxn.commit()?;

    // ------
    println!();

    let rtxn = env.read_txn()?;

    let ret = db.get(&rtxn, b"hello")?;
    println!("parent (reader) \"hello\": {:?}", ret);

    let ret = db.get(&rtxn, b"humm...")?;
    println!("parent (reader) \"humm...\": {:?}", ret);

    Ok(())
}
