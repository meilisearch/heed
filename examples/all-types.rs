use std::error::Error;
use std::fs;

use zerocopy_lmdb::{EnvOpenOptions, CompactionOption, Database};
use zerocopy_lmdb::types::*;
use serde::{Serialize, Deserialize};
use zerocopy::{AsBytes, FromBytes, Unaligned};

fn main() -> Result<(), Box<dyn Error>> {

    fs::create_dir_all("target/zerocopy.mdb")?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .max_dbs(3000)
        .open("target/zerocopy.mdb")?;

    // you can specify that a database will support some typed key/data
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an str
    let db: Database<OwnedType<[i32; 2]>, Str> = env.create_database(Some("kikou"))?;

    let mut wtxn = env.write_txn()?;
    let _ret              = db.put(&mut wtxn, &[2, 3], "what's up?")?;
    let ret: Option<&str> = db.get(&wtxn,     &[2, 3])?;

    println!("{:?}", ret);
    wtxn.commit()?;



    // here the key will be an str and the data will be a slice of u8
    let db: Database<Str, ByteSlice> = env.create_database(Some("kiki"))?;

    let mut wtxn = env.write_txn()?;
    let _ret               = db.put(&mut wtxn, "hello", &[2, 3][..])?;
    let ret: Option<&[u8]> = db.get(&wtxn,     "hello")?;

    println!("{:?}", ret);
    wtxn.commit()?;



    // serde types are also supported!!!
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Hello<'a> { string: &'a str }

    let db: Database<Str, Serde<Hello>> = env.create_database(Some("serde"))?;

    let mut wtxn = env.write_txn()?;

    let hello = Hello { string: "hi" };
    db.put(&mut wtxn, "hello", &hello)?;

    let ret: Option<Hello> = db.get(&wtxn,     "hello")?;

    println!("{:?}", ret);
    wtxn.commit()?;




    // it is prefered to use zerocopy when possible
    #[derive(Debug, PartialEq, Eq)]
    #[derive(AsBytes, FromBytes, Unaligned)]
    #[repr(C)]
    struct ZeroBytes {
        bytes: [u8; 12],
    }

    let db: Database<Str, UnalignedType<ZeroBytes>> = env.create_database(Some("zerocopy-struct"))?;

    let mut wtxn = env.write_txn()?;

    let zerobytes = ZeroBytes { bytes: [24; 12] };
    db.put(&mut wtxn, "zero", &zerobytes)?;

    let ret = db.get(&wtxn, "zero")?;

    println!("{:?}", ret);
    wtxn.commit()?;



    // you can ignore the data
    let db: Database<Str, Unit> = env.create_database(Some("ignored-data"))?;


    let mut wtxn = env.write_txn()?;
    let _ret            = db.put(&mut wtxn, "hello", &())?;
    let ret: Option<()> = db.get(&wtxn,     "hello")?;

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
    type BEI64 = zerocopy::I64<byteorder::BigEndian>;

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
    let rets: Result<Vec<(BEI64, _)>, _> = db.range(&wtxn, range)?.collect();

    println!("{:?}", rets);


    // delete a range of key
    let range = BEI64::new(35)..=BEI64::new(42);
    let deleted: usize = db.delete_range(&mut wtxn, range)?;

    let rets: Result<Vec<(BEI64, _)>, _> = db.iter(&wtxn)?.collect();

    println!("deleted: {:?}, {:?}", deleted, rets);
    wtxn.commit()?;

    let file = env.copy_to_path("target/zerocopy-copied.mdb", CompactionOption::Enabled)?;
    println!("copied to {:?}", file);

    Ok(())
}
