use std::error::Error;
use std::{fs, str};
use std::cmp::Ordering;
use std::path::Path;

use heed::types::*;
use heed::{Database, EnvOpenOptions, CustomKeyCmp};

enum StringAsIntCmp {}

// This function takes two strings which represent positive numbers,
// parses them into i32s and compare the parsed value.
// Therefore "-1000" < "-100" must be true even without '0' padding.
impl CustomKeyCmp for StringAsIntCmp {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        let a: i32 = str::from_utf8(a).unwrap().parse().unwrap();
        let b: i32 = str::from_utf8(b).unwrap().parse().unwrap();
        a.cmp(&b)
    }
}

// In this test we are checking that we can use
// a custom key comparison function at database creation.
fn main() -> Result<(), Box<dyn Error>> {
    let env_path = Path::new("target").join("custom-key-cmp.mdb");

    let _ = fs::remove_dir_all(&env_path);

    fs::create_dir_all(&env_path)?;
    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3)
        .open(env_path)?;

    let db: Database<Str, Unit> = env.create_database_with_custom_key_cmp::<_, _, StringAsIntCmp>(None)?;

    let mut wtxn = env.write_txn()?;

    // We fill our database with entries.
    db.put(&mut wtxn, "-100000", &())?;
    db.put(&mut wtxn, "-10000", &())?;
    db.put(&mut wtxn, "-1000", &())?;
    db.put(&mut wtxn, "-100", &())?;
    db.put(&mut wtxn, "100", &())?;

    // We check that the key are in the right order ("-100" < "-1000" < "-10000"...)
    let mut iter = db.iter(&wtxn)?;
    assert_eq!(iter.next().transpose()?, Some(("-100000", ())));
    assert_eq!(iter.next().transpose()?, Some(("-10000", ())));
    assert_eq!(iter.next().transpose()?, Some(("-1000", ())));
    assert_eq!(iter.next().transpose()?, Some(("-100", ())));
    assert_eq!(iter.next().transpose()?, Some(("100", ())));
    drop(iter);

    Ok(())
}
