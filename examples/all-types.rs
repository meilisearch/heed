use std::borrow::Cow;
use zerocopy_lmdb::{EnvBuilder, Database};
use zerocopy_lmdb::types::*;
use serde::{Serialize, Deserialize};

fn main() {
    let env = EnvBuilder::new()
        .map_size(10 * 1024 * 1024 * 1024) // 10GB
        .max_dbs(3000)
        .open("zerocopy.mdb")
        .unwrap();

    // you can specify that a database will support some typed key/data
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an str
    let db: Database<OwnedType<[i32; 2]>, Str> = env.create_database(Some("kikou")).unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let _ret              = db.put(&mut wtxn, &[2, 3], "what's up?").unwrap();
    let ret: Option<&str> = db.get(&wtxn,     &[2, 3]).unwrap();

    println!("{:?}", ret);
    wtxn.commit().unwrap();



    // here the key will be an str and the data will be a slice of u8
    let db: Database<Str, ByteSlice> = env.create_database(Some("kiki")).unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let _ret               = db.put(&mut wtxn, "hello", &[2, 3][..]).unwrap();
    let ret: Option<&[u8]> = db.get(&wtxn,     "hello").unwrap();

    println!("{:?}", ret);
    wtxn.commit().unwrap();



    // serde types are also supported!!!
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Hello<'a> { string: &'a str }

    let db: Database<Str, Serde<Hello>> = env.create_database(None).unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let hello = Hello { string: "hi" };
    let _ret                    = db.put(&mut wtxn, "hello", &hello).unwrap();
    let ret: Option<Cow<Hello>> = db.get(&wtxn,     "hello").unwrap();

    println!("{:?}", ret);
    wtxn.commit().unwrap();



    // you can ignore the data
    let db: Database<Str, Ignore> = env.create_database(None).unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let _ret            = db.put(&mut wtxn, "hello", &()).unwrap();
    let ret: Option<()> = db.get(&wtxn,     "hello").unwrap();

    println!("{:?}", ret);



    let ret: Option<()> = db.get(&wtxn, "non-existant").unwrap();

    println!("{:?}", ret);
    wtxn.commit().unwrap();


    // you can iterate over keys in order
    type BEI64 = zerocopy::I64<byteorder::BigEndian>;

    let db: Database<OwnedType<BEI64>, Ignore> = env.create_database(Some("big-endian-iter")).unwrap();

    let mut wtxn = env.write_txn().unwrap();
    let _ret = db.put(&mut wtxn, &BEI64::new(0), &()).unwrap();
    let _ret = db.put(&mut wtxn, &BEI64::new(68), &()).unwrap();
    let _ret = db.put(&mut wtxn, &BEI64::new(35), &()).unwrap();
    let _ret = db.put(&mut wtxn, &BEI64::new(42), &()).unwrap();

    let rets: Result<Vec<(BEI64, _)>, _> = db.iter(&wtxn).unwrap().collect();

    println!("{:?}", rets);


    // or iterate over ranges too!!!
    let range = BEI64::new(35)..=BEI64::new(42);
    let rets: Result<Vec<(BEI64, _)>, _> = db.range(&wtxn, range).unwrap().collect();

    println!("{:?}", rets);


    // delete a range of key
    let range = BEI64::new(35)..=BEI64::new(42);
    let deleted: usize = db.delete_range(&mut wtxn, range).unwrap();

    let rets: Result<Vec<(BEI64, _)>, _> = db.iter(&wtxn).unwrap().collect();

    println!("deleted: {:?}, {:?}", deleted, rets);
    wtxn.commit().unwrap();
}
