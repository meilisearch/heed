use std::error::Error;

use heed3::types::*;
use heed3::{AbortOrCommit, Database, EnvFlags, EnvOpenOptions};

// In this test we are checking that we can move to a previous environement snapshot.
fn main() -> Result<(), Box<dyn Error>> {
    let env_path = tempfile::tempdir()?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open(&env_path)?
    };

    let rtxn = env.read_txn()?;
    let db: Database<Str, Str> = env
        .open_databases_and_commit(rtxn, |dbo, rtxn| {
            dbo.open_database(rtxn, None).map(AbortOrCommit::Commit)
        })?
        .unwrap_commit()
        .expect("the unnamed database to exists");

    let mut wtxn = env.write_txn()?;
    // We fill the db database with entries.
    db.put(&mut wtxn, "I am here", "to test things")?;
    db.put(&mut wtxn, "I am here too", "for the same purpose")?;

    wtxn.commit()?;

    env.prepare_for_closing().wait();

    // We can get the env state from before the last commit
    // and therefore see an empty env.
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .flags(EnvFlags::PREV_SNAPSHOT)
            .open(&env_path)?
    };

    let rtxn = env.read_txn()?;
    let db: Database<Str, Str> = env
        .open_databases_and_commit(rtxn, |dbo, rtxn| {
            dbo.open_database(rtxn, None).map(AbortOrCommit::Commit)
        })?
        .unwrap_commit()
        .expect("the unnamed database to exists");

    let rtxn = env.read_txn()?;
    assert!(db.is_empty(&rtxn)?);

    drop(rtxn);
    env.prepare_for_closing().wait();

    // However, if we don't commit we can still get
    // back the latest version of the env.
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open(&env_path)?
    };

    let rtxn = env.read_txn()?;
    let db: Database<Str, Str> = env
        .open_databases_and_commit(rtxn, |dbo, rtxn| {
            dbo.open_database(rtxn, None).map(AbortOrCommit::Commit)
        })?
        .unwrap_commit()
        .expect("the unnamed database to exists");

    let mut wtxn = env.write_txn()?;
    assert_eq!(db.get(&wtxn, "I am here")?, Some("to test things"));
    assert_eq!(db.get(&wtxn, "I am here too")?, Some("for the same purpose"));

    // And write new stuff in the env.
    db.put(&mut wtxn, "I will fade away", "I am so sad")?;

    wtxn.commit()?;
    env.prepare_for_closing().wait();

    // Once again we can get back the previous version
    // of the env and see some entries disappear.
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .flags(EnvFlags::PREV_SNAPSHOT)
            .open(&env_path)?
    };

    let rtxn = env.read_txn()?;
    let db: Database<Str, Str> = env
        .open_databases_and_commit(rtxn, |dbo, rtxn| {
            dbo.open_database(rtxn, None).map(AbortOrCommit::Commit)
        })?
        .unwrap_commit()
        .expect("the unnamed database to exists");

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "I am here")?, Some("to test things"));
    assert_eq!(db.get(&rtxn, "I am here too")?, Some("for the same purpose"));
    assert_eq!(db.get(&rtxn, "I will fade away")?, None);

    Ok(())
}
