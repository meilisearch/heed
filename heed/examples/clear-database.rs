use std::error::Error;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

// In this test we are checking that we can clear database entries and
// write just after in the same transaction without loosing the writes.
fn main() -> Result<(), Box<dyn Error>> {
    let env_path = tempfile::tempdir()?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open(env_path)?
    };

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;

    // We fill the db database with entries.
    db.put(&mut wtxn, "I am here", "to test things")?;
    db.put(&mut wtxn, "I am here too", "for the same purpose")?;

    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    db.put(&mut wtxn, "And I come back", "to test things")?;

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
