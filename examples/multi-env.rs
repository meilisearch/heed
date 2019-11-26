use std::error::Error;
use std::fs;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    fs::create_dir_all("target/env1.mdb")?;
    let env1 = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .max_dbs(3000)
        .open("target/env1.mdb")?;

    fs::create_dir_all("target/env2.mdb")?;
    let env2 = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .max_dbs(3000)
        .open("target/env2.mdb")?;

    let db1: Database<Str, ByteSlice> = env1.create_database(Some("hello"))?;
    let db2: Database<OwnedType<u32>, OwnedType<u32>> = env2.create_database(Some("hello"))?;

    // clear db
    let mut wtxn = env1.write_txn()?;
    db1.clear(&mut wtxn)?;
    wtxn.commit()?;

    // clear db
    let mut wtxn = env2.write_txn()?;
    db2.clear(&mut wtxn)?;
    wtxn.commit()?;

    // -----

    let mut wtxn1 = env1.write_txn()?;

    db1.put(&mut wtxn1, "what", &[4, 5][..])?;
    db1.get(&wtxn1, "what")?;
    wtxn1.commit()?;

    let rtxn2 = env2.read_txn()?;
    let ret = db2.last(&rtxn2)?;
    assert_eq!(ret, None);

    Ok(())
}
