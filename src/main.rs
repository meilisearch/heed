use std::borrow::Cow;
use std::ptr;
use zerocopy_lmdb::{Env, Database, TxnRead, TxnWrite, Type, Slice, Str, Ignore, Serde};
use serde::{Serialize, Deserialize};
use lmdb_sys as ffi;

fn main() {
    let env = Env::open("zerocopy.mdb");

    // you can specify that a database will support some typed key/data
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an unsized array of u64
    let db: Database<Type<[i32; 2]>, Str> = env.open_database(None);

    let mut wtxn = env.write_txn();
    let ret                   = db.put(&mut wtxn, &[2, 3], "what's up?").unwrap();
    let ret: Option<Cow<str>> = db.get(&wtxn, &[2, 3]).unwrap();

    println!("{:?}", ret);
    wtxn.commit();



    // even str are supported,
    // here the key will be an str and the data will be an array of two i32
    let db: Database<Str, Slice<i32>> = env.open_database(None);

    let mut wtxn = env.write_txn();
    let ret                     = db.put(&mut wtxn, "hello", &[2, 3][..]).unwrap();
    let ret: Option<Cow<[i32]>> = db.get(&wtxn, "hello").unwrap();

    println!("{:?}", ret);
    wtxn.commit();



    // // serde types are also supported but this could be improved a little bit...
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Hello { string: String }

    let db: Database<Str, Serde<Hello>> = env.open_database(None);

    let mut wtxn = env.write_txn();
    let hello = Hello { string: String::from("hi") };
    let ret                     = db.put(&mut wtxn, "hello", &hello).unwrap();
    let ret: Option<Cow<Hello>> = db.get(&wtxn, "hello").unwrap();

    println!("{:?}", ret);
    wtxn.commit();



    // you can also ignore the key or the data
    let db: Database<Str, Ignore> = env.open_database(None);

    let mut wtxn = env.write_txn();
    let ret                  = db.put(&mut wtxn, "hello", &()).unwrap();
    let ret: Option<Cow<()>> = db.get(&wtxn, "hello").unwrap();

    println!("{:?}", ret);



    let ret: Option<Cow<()>> = db.get(&wtxn, "non-existant").unwrap();

    println!("{:?}", ret);
    wtxn.commit();
}
