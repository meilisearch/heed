use std::error::Error;
use std::fs;

use heed::EnvOpenOptions;
use heed::types::*;
use serde::{Serialize, Deserialize};
use heed::zerocopy::{AsBytes, FromBytes, Unaligned, I64};
use heed::byteorder::BE;

fn main() -> Result<(), Box<dyn Error>> {

    fs::create_dir_all("target/zerocopy-dyn.mdb")?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .max_dbs(3000)
        .open("target/zerocopy-dyn.mdb")?;

    // you can specify that a database will support some typed key/data
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an str
    let db = env.create_dyn_database(Some("kikou"))?;

    let mut wtxn = env.write_txn()?;
    db.put::<OwnedType<[i32; 2]>, Str>(&mut wtxn, &[2, 3], "what's up?")?;
    let ret = db.get::<OwnedType<[i32; 2]>, Str>(&wtxn, &[2, 3])?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // here the key will be an str and the data will be a slice of u8
    let db = env.create_dyn_database(Some("kiki"))?;

    let mut wtxn = env.write_txn()?;
    db.put::<Str, ByteSlice>(&mut wtxn, "hello", &[2, 3][..])?;
    let ret = db.get::<Str, ByteSlice>(&wtxn, "hello")?;

    println!("{:?}", ret);
    wtxn.commit()?;



    // serde types are also supported!!!
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Hello<'a> { string: &'a str }

    let db = env.create_dyn_database(Some("serde"))?;

    let mut wtxn = env.write_txn()?;

    let hello = Hello { string: "hi" };
    db.put::<Str, SerdeBincode<Hello>>(&mut wtxn, "hello", &hello)?;

    let ret = db.get::<Str, SerdeBincode<Hello>>(&wtxn, "hello")?;
    println!("serde-bincode:\t{:?}", ret);

    let hello = Hello { string: "hi" };
    db.put::<Str, SerdeJson<Hello>>(&mut wtxn, "hello", &hello)?;

    let ret = db.get::<Str, SerdeJson<Hello>>(&wtxn, "hello")?;
    println!("serde-json:\t{:?}", ret);

    wtxn.commit()?;




    // it is prefered to use zerocopy when possible
    #[derive(Debug, PartialEq, Eq)]
    #[derive(AsBytes, FromBytes, Unaligned)]
    #[repr(C)]
    struct ZeroBytes {
        bytes: [u8; 12],
    }

    let db = env.create_dyn_database(Some("zerocopy-struct"))?;

    let mut wtxn = env.write_txn()?;

    let zerobytes = ZeroBytes { bytes: [24; 12] };
    db.put::<Str, UnalignedType<ZeroBytes>>(&mut wtxn, "zero", &zerobytes)?;

    let ret = db.get::<Str, UnalignedType<ZeroBytes>>(&wtxn, "zero")?;

    println!("{:?}", ret);
    wtxn.commit()?;



    // you can ignore the data
    let db = env.create_dyn_database(Some("ignored-data"))?;


    let mut wtxn = env.write_txn()?;
    db.put::<Str, Unit>(&mut wtxn, "hello", &())?;
    let ret = db.get::<Str, Unit>(&wtxn, "hello")?;

    println!("{:?}", ret);

    let ret = db.get::<Str, Unit>(&wtxn, "non-existant")?;

    println!("{:?}", ret);
    wtxn.commit()?;


    // database opening and types are tested in a way
    //
    // we try to open a database twice with the same types
    let _db = env.create_dyn_database(Some("ignored-data"))?;

    // and here we try to open it with other types
    // asserting that it correctly returns an error
    //
    // NOTE that those types are not saved upon runs and
    // therefore types cannot be checked upon different runs,
    // the first database opening fix the types for this run.
    let result = env.create_database::<OwnedType<BEI64>, Unit>(Some("ignored-data"));
    assert!(result.is_err());



    // you can iterate over keys in order
    type BEI64 = I64<BE>;

    let db = env.create_dyn_database(Some("big-endian-iter"))?;

    let mut wtxn = env.write_txn()?;
    db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(0), &())?;
    db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(68), &())?;
    db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(35), &())?;
    db.put::<OwnedType<BEI64>, Unit>(&mut wtxn, &BEI64::new(42), &())?;

    let rets: Result<Vec<(BEI64, _)>, _> = db.iter::<OwnedType<BEI64>, Unit>(&wtxn)?.collect();

    println!("{:?}", rets);


    // or iterate over ranges too!!!
    let range = BEI64::new(35)..=BEI64::new(42);
    let rets: Result<Vec<(BEI64, _)>, _> = db.range::<OwnedType<BEI64>, Unit, _>(&wtxn, range)?.collect();

    println!("{:?}", rets);


    // delete a range of key
    let range = BEI64::new(35)..=BEI64::new(42);
    let deleted: usize = db.delete_range::<OwnedType<BEI64>, Unit, _>(&mut wtxn, range)?;

    let rets: Result<Vec<(BEI64, _)>, _> = db.iter::<OwnedType<BEI64>, Unit>(&wtxn)?.collect();

    println!("deleted: {:?}, {:?}", deleted, rets);
    wtxn.commit()?;

    Ok(())
}
