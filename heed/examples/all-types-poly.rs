use std::error::Error;
use std::fs;
use std::path::Path;

use bytemuck::{Pod, Zeroable};
use heed::byteorder::BE;
use heed::types::*;
use heed::EnvOpenOptions;
use serde::{Deserialize, Serialize};

fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new("target").join("heed-poly.mdb");

    fs::create_dir_all(&path)?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3000)
        .open(path)?;

    // you can specify that a database will support some typed key/data
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an str
    let mut wtxn = env.write_txn()?;
    let db = env.create_poly_database(&mut wtxn, Some("kikou"))?;

    db.put::<OwnedType<[i32; 2]>, Str>(&mut wtxn, &[2, 3], "what's up?")?;
    let ret = db.get::<OwnedType<[i32; 2]>, Str>(&wtxn, &[2, 3])?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // here the key will be an str and the data will be a slice of u8
    let mut wtxn = env.write_txn()?;
    let db = env.create_poly_database(&mut wtxn, Some("kiki"))?;

    db.put::<Str, ByteSlice>(&mut wtxn, "hello", &[2, 3][..])?;
    let ret = db.get::<Str, ByteSlice>(&wtxn, "hello")?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // serde types are also supported!!!
    #[derive(Debug, Serialize, Deserialize)]
    struct Hello<'a> {
        string: &'a str,
    }

    let mut wtxn = env.write_txn()?;
    let db = env.create_poly_database(&mut wtxn, Some("serde"))?;

    let hello = Hello { string: "hi" };
    db.put::<Str, SerdeBincode<Hello>>(&mut wtxn, "hello", &hello)?;

    let ret = db.get::<Str, SerdeBincode<Hello>>(&wtxn, "hello")?;
    println!("serde-bincode:\t{:?}", ret);

    let hello = Hello { string: "hi" };
    db.put::<Str, SerdeJson<Hello>>(&mut wtxn, "hello", &hello)?;

    let ret = db.get::<Str, SerdeJson<Hello>>(&wtxn, "hello")?;
    println!("serde-json:\t{:?}", ret);

    wtxn.commit()?;

    #[derive(Debug, PartialEq, Eq, Clone, Copy, Pod, Zeroable)]
    #[repr(C)]
    struct ZeroBytes {
        bytes: [u8; 12],
    }

    let mut wtxn = env.write_txn()?;
    let db = env.create_poly_database(&mut wtxn, Some("nocopy-struct"))?;

    let zerobytes = ZeroBytes { bytes: [24; 12] };
    db.put::<Str, UnalignedType<ZeroBytes>>(&mut wtxn, "zero", &zerobytes)?;

    let ret = db.get::<Str, UnalignedType<ZeroBytes>>(&wtxn, "zero")?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // you can ignore the data
    let mut wtxn = env.write_txn()?;
    let db = env.create_poly_database(&mut wtxn, Some("ignored-data"))?;

    db.put::<Str, Unit>(&mut wtxn, "hello", &())?;
    let ret = db.get::<Str, Unit>(&wtxn, "hello")?;

    println!("{:?}", ret);

    let ret = db.get::<Str, Unit>(&wtxn, "non-existant")?;

    println!("{:?}", ret);
    wtxn.commit()?;

    // database opening and types are tested in a safe way
    //
    // we try to open a database twice with the same types
    let mut wtxn = env.write_txn()?;
    let _db = env.create_poly_database(&mut wtxn, Some("ignored-data"))?;

    // and here we try to open it with other types
    // asserting that it correctly returns an error
    //
    // NOTE that those types are not saved upon runs and
    // therefore types cannot be checked upon different runs,
    // the first database opening fix the types for this run.
    let result = env.create_database::<BEI64, Unit>(&mut wtxn, Some("ignored-data"));
    assert!(result.is_err());

    // you can iterate over keys in order
    type BEI64 = I64<BE>;

    let db = env.create_poly_database(&mut wtxn, Some("big-endian-iter"))?;

    db.put::<BEI64, Unit>(&mut wtxn, &0, &())?;
    db.put::<BEI64, Unit>(&mut wtxn, &68, &())?;
    db.put::<BEI64, Unit>(&mut wtxn, &35, &())?;
    db.put::<BEI64, Unit>(&mut wtxn, &42, &())?;

    let rets: Result<Vec<(i64, _)>, _> = db.iter::<BEI64, Unit>(&wtxn)?.collect();

    println!("{:?}", rets);

    // or iterate over ranges too!!!
    let range = 35..=42;
    let rets: Result<Vec<(i64, _)>, _> = db.range::<BEI64, Unit, _>(&wtxn, &range)?.collect();

    println!("{:?}", rets);

    // delete a range of key
    let range = 35..=42;
    let deleted: usize = db.delete_range::<BEI64, _>(&mut wtxn, &range)?;

    let rets: Result<Vec<(i64, _)>, _> = db.iter::<BEI64, Unit>(&wtxn)?.collect();

    println!("deleted: {:?}, {:?}", deleted, rets);
    wtxn.commit()?;

    Ok(())
}
