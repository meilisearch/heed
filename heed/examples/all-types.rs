use std::error::Error;
use std::fs;
use std::path::Path;

use heed::byteorder::BE;
use heed::types::*;
use heed::zerocopy::{AsBytes, FromBytes, Unaligned, I64};
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
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an str
    let db: Database<OwnedType<[i32; 2]>, Str> = env.create_database(Some("kikou"))?;

    let mut wtxn = env.write_txn()?;
    let _ret = db.put(&mut wtxn, &[2, 3], "what's up?")?;
    let ret: Option<&str> = db.get(&wtxn, &[2, 3])?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // here the key will be an str and the data will be a slice of u8
    let db: Database<Str, ByteSlice> = env.create_database(Some("kiki"))?;

    let mut wtxn = env.write_txn()?;
    let _ret = db.put(&mut wtxn, "hello", &[2, 3][..])?;
    let ret: Option<&[u8]> = db.get(&wtxn, "hello")?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // serde types are also supported!!!
    #[derive(Debug, Serialize, Deserialize)]
    struct Hello<'a> {
        string: &'a str,
    }

    let db: Database<Str, SerdeBincode<Hello>> = env.create_database(Some("serde-bincode"))?;

    let mut wtxn = env.write_txn()?;

    let hello = Hello { string: "hi" };
    db.put(&mut wtxn, "hello", &hello)?;

    let ret: Option<Hello> = db.get(&wtxn, "hello")?;
    println!("serde-bincode:\t{:?}", ret);

    wtxn.commit()?;

    let db: Database<Str, SerdeJson<Hello>> = env.create_database(Some("serde-json"))?;

    let mut wtxn = env.write_txn()?;

    let hello = Hello { string: "hi" };
    db.put(&mut wtxn, "hello", &hello)?;

    let ret: Option<Hello> = db.get(&wtxn, "hello")?;
    println!("serde-json:\t{:?}", ret);

    wtxn.commit()?;

    // it is prefered to use zerocopy when possible
    #[derive(Debug, PartialEq, Eq, AsBytes, FromBytes, Unaligned)]
    #[repr(C)]
    struct ZeroBytes {
        bytes: [u8; 12],
    }

    let db: Database<Str, UnalignedType<ZeroBytes>> =
        env.create_database(Some("zerocopy-struct"))?;

    let mut wtxn = env.write_txn()?;

    let zerobytes = ZeroBytes { bytes: [24; 12] };
    db.put(&mut wtxn, "zero", &zerobytes)?;

    let ret = db.get(&wtxn, "zero")?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // you can ignore the data
    let db: Database<Str, Unit> = env.create_database(Some("ignored-data"))?;

    let mut wtxn = env.write_txn()?;
    let _ret = db.put(&mut wtxn, "hello", &())?;
    let ret: Option<()> = db.get(&wtxn, "hello")?;

    println!("{:?}", ret);

    let ret: Option<()> = db.get(&wtxn, "non-existant")?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // database opening and types are tested in a way
    //
    // we try to open a database twice with the same types
    let _db: Database<Str, Unit> = env.create_database(Some("ignored-data"))?;

    // and here we try to open it with other types
    // asserting that it correctly returns an error
    //
    // NOTE that those types are not saved upon runs and
    // therefore types cannot be checked upon different runs,
    // the first database opening fix the types for this run.
    let result = env.create_database::<Str, OwnedSlice<i32>>(Some("ignored-data"));
    assert!(result.is_err());

    // you can iterate over keys in order
    type BEI64 = I64<BE>;

    let db: Database<OwnedType<BEI64>, Unit> = env.create_database(Some("big-endian-iter"))?;

    let mut wtxn = env.write_txn()?;
    let _ret = db.put(&mut wtxn, &BEI64::new(0), &())?;
    let _ret = db.put(&mut wtxn, &BEI64::new(68), &())?;
    let _ret = db.put(&mut wtxn, &BEI64::new(35), &())?;
    let _ret = db.put(&mut wtxn, &BEI64::new(42), &())?;

    let rets: Result<Vec<(BEI64, _)>, _> = db.iter(&wtxn)?.collect();

    println!("{:?}", rets);

    // or iterate over ranges too!!!
    let range = BEI64::new(35)..=BEI64::new(42);
    let rets: Result<Vec<(BEI64, _)>, _> = db.range(&wtxn, &range)?.collect();

    println!("{:?}", rets);

    // delete a range of key
    let range = BEI64::new(35)..=BEI64::new(42);
    let deleted: usize = db.delete_range(&mut wtxn, &range)?;

    let rets: Result<Vec<(BEI64, _)>, _> = db.iter(&wtxn)?.collect();

    println!("deleted: {:?}, {:?}", deleted, rets);
    wtxn.commit()?;

    Ok(())
}
