use std::error::Error;
use std::fs;
use std::path::Path;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let env1_path = Path::new("target").join("env1.mdb");
    let env2_path = Path::new("target").join("env2.mdb");

    fs::create_dir_all(&env1_path)?;
    let env1 = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3000)
        .open(env1_path)?;

    fs::create_dir_all(&env2_path)?;
    let env2 = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3000)
        .open(env2_path)?;

    let mut wtxn1 = env1.write_txn()?;
    let mut wtxn2 = env2.write_txn()?;
    let db1: Database<Str, ByteSlice> = env1.create_database(&mut wtxn1, Some("hello"))?;
    let db2: Database<OwnedType<u32>, OwnedType<u32>> =
        env2.create_database(&mut wtxn2, Some("hello"))?;

    // clear db
    db1.clear(&wtxn1)?;
    wtxn1.commit()?;

    // clear db
    db2.clear(&wtxn2)?;
    wtxn2.commit()?;

    // -----

    let wtxn1 = env1.write_txn()?;

    db1.put(&wtxn1, "what", &[4, 5][..])?;
    db1.get(&wtxn1, "what")?;
    wtxn1.commit()?;

    let rtxn2 = env2.read_txn()?;
    let ret = db2.last(&rtxn2)?;
    assert_eq!(ret, None);

    Ok(())
}
