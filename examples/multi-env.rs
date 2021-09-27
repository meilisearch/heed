use std::error::Error;
use std::fs;
use std::path::Path;

use heed::EnvOpenOptions;

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

    let db1 = env1.create_database(Some("hello"))?;
    let db2 = env2.create_database(Some("hello"))?;

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

    db1.put(&mut wtxn1, b"what", &[4, 5][..])?;
    db1.get(&wtxn1, b"what")?;
    wtxn1.commit()?;

    let rtxn2 = env2.read_txn()?;
    let ret = db2.last(&rtxn2)?;
    assert_eq!(ret, None);

    Ok(())
}
