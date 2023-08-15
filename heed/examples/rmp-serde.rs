use std::error::Error;
use std::fs;
use std::path::Path;

use heed::bytemuck::{Pod, Zeroable};
use heed::byteorder::BE;
use heed::types::*;
use heed::{Database, EnvOpenOptions};
use serde::{Deserialize, Serialize};


fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new("target").join("heed.mdb");

    fs::create_dir_all(&path)?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3000)
        .open(path)?;

    // you can specify that a database will support some typed key/data
    // serde types are also supported!!!
    #[derive(Debug, Serialize, Deserialize)]
    struct Hello<'a> {
        string: &'a str,
    }

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, SerdeRmp<Hello>> = env.create_database(&mut wtxn, Some("serde-rmp"))?;

    let hello = Hello { string: "hi" };
    db.put(&mut wtxn, "hello", &hello)?;

    let ret: Option<Hello> = db.get(&wtxn, "hello")?;
    println!("serde-rmp:\t{:?}", ret);

    wtxn.commit()?;

    Ok(())
}
