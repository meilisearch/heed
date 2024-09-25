use std::cmp::Ordering;
use std::error::Error;
use std::fs;
use std::path::Path;

use byteorder::BigEndian;
use heed::{DatabaseFlags, EnvOpenOptions};
use heed_traits::Comparator;
use heed_types::{Str, U128};

enum DescendingIntCmp {}

impl Comparator for DescendingIntCmp {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(&a)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let env_path = Path::new("target").join("custom-dupsort-cmp.mdb");

    let _ = fs::remove_dir_all(&env_path);

    fs::create_dir_all(&env_path)?;
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open(env_path)?
    };

    let mut wtxn = env.write_txn()?;
    let db = env
        .database_options()
        .types::<Str, U128<BigEndian>>()
        .flags(DatabaseFlags::DUP_SORT)
        .dup_sort_comparator::<DescendingIntCmp>()
        .create(&mut wtxn)?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;

    // We fill our database with entries.
    db.put(&mut wtxn, "1", &1)?;
    db.put(&mut wtxn, "1", &2)?;
    db.put(&mut wtxn, "1", &3)?;
    db.put(&mut wtxn, "2", &4)?;
    db.put(&mut wtxn, "1", &5)?;
    db.put(&mut wtxn, "0", &0)?;

    // We check that the keys are in lexicographic and values in descending order.
    let mut iter = db.iter(&wtxn)?;
    assert_eq!(iter.next().transpose()?, Some(("0", 0)));
    assert_eq!(iter.next().transpose()?, Some(("1", 5)));
    assert_eq!(iter.next().transpose()?, Some(("1", 3)));
    assert_eq!(iter.next().transpose()?, Some(("1", 2)));
    assert_eq!(iter.next().transpose()?, Some(("1", 1)));
    assert_eq!(iter.next().transpose()?, Some(("2", 4)));
    drop(iter);

    Ok(())
}
