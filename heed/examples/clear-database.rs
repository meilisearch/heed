use std::error::Error;
use std::fs;
use std::path::Path;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

// In this test we are checking that we can clear database entries and
// write just after in the same transaction without loosing the writes.
fn main() -> Result<(), Box<dyn Error>> {
    let env_path = Path::new("target").join("clear-database.mdb");

    let _ = fs::remove_dir_all(&env_path);

    fs::create_dir_all(&env_path)?;
    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3)
        .open(env_path)?;

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;

    // We fill the db database with entries.
    db.put(&wtxn, "I am here", "to test things")?;
    db.put(&wtxn, "I am here too", "for the same purpose")?;

    wtxn.commit()?;

    let wtxn = env.write_txn()?;
    db.clear(&wtxn)?;
    db.put(&wtxn, "And I come back", "to test things")?;

    let mut iter = db.iter(&wtxn)?;
    assert_eq!(iter.next().transpose()?, Some(("And I come back", "to test things")));
    assert_eq!(iter.next().transpose()?, None);

    drop(iter);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut iter = db.iter(&rtxn)?;
    assert_eq!(iter.next().transpose()?, Some(("And I come back", "to test things")));
    assert_eq!(iter.next().transpose()?, None);

    Ok(())
}
