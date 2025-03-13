use std::error::Error;

use byteorder::BE;
use heed::types::*;
use heed::{Database, EnvOpenOptions};

type BEU32 = U32<BE>;

fn main() -> Result<(), Box<dyn Error>> {
    let env1_path = tempfile::tempdir()?;
    let env2_path = tempfile::tempdir()?;
    let env1 = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(env1_path)?
    };

    let env2 = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(env2_path)?
    };

    let mut wtxn1 = env1.write_txn()?;
    let mut wtxn2 = env2.write_txn()?;
    let db1: Database<Str, Bytes> = env1.create_database(&mut wtxn1, Some("hello"))?;
    let db2: Database<BEU32, BEU32> = env2.create_database(&mut wtxn2, Some("hello"))?;

    // clear db
    db1.clear(&mut wtxn1)?;
    wtxn1.commit()?;

    // clear db
    db2.clear(&mut wtxn2)?;
    wtxn2.commit()?;

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
