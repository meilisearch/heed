use std::error::Error;
use std::fs;
use std::path::Path;

use heed::types::*;
use heed::{Database, EnvOpenOptions};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

const NUMBER_NESTED_TXNS: i32 = 20;

// This program can be used with a modified LMDB where the following line is commented out:
//
//   parent->mt_flags |= MDB_TXN_HAS_CHILD;
//
// You can find this line at the following URL:
// https://github.com/LMDB/lmdb/blob/da9aeda08c3ff710a0d47d61a079f5a905b0a10a/libraries/liblmdb/mdb.c#L3275

fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new("target").join("heed.mdb");

    fs::create_dir_all(&path)?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(path)?
    };

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, None)?;
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    // -----

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "hello", "world")?;

    println!(
        "This program will use {} thread to run stuff in parallel",
        rayon::current_num_threads()
    );

    let multiple_nrtxns: Vec<_> =
        (0..NUMBER_NESTED_TXNS).map(|_| env.nested_read_txn(&wtxn)).collect::<heed::Result<_>>()?;

    multiple_nrtxns.into_par_iter().try_for_each(|nrtxn| {
        let ret = db.get(&nrtxn, "hello")?;
        assert_eq!(ret, Some("world"));
        println!(
            "We successfully found an uncommitted \"world\" associated to \"hello\" in parallel!"
        );
        heed::Result::Ok(())
    })?;

    for n in 0..1000 {
        let n = n.to_string();
        db.put(&mut wtxn, &n, &n)?;
    }

    let multiple_nrtxns: Vec<_> =
        (0..NUMBER_NESTED_TXNS).map(|_| env.nested_read_txn(&wtxn)).collect::<heed::Result<_>>()?;

    multiple_nrtxns.into_par_iter().try_for_each(|nrtxn| {
        for n in 0..1000 {
            let n = n.to_string();
            let ret = db.get(&nrtxn, &n)?;
            assert_eq!(ret, Some(n.as_str()));
        }
        println!("We successfully found 1000 uncommitted entries in parallel!");
        heed::Result::Ok(())
    })?;

    Ok(())
}
