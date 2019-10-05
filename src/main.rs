use std::borrow::Cow;
use std::ptr;
use zerocopy_lmdb::{Database, TxnRead, TxnWrite, Type, Slice, Str, Ignore, Serde};
use serde::{Serialize, Deserialize};
use lmdb_sys as ffi;

fn main() {

    let mut env: *mut ffi::MDB_env = ptr::null_mut();
    let ret = unsafe { ffi::mdb_env_create(&mut env) };

    assert_eq!(ret, 0);

    let path = std::ffi::CString::new("zerocopy.mdb").unwrap();
    let path_bytes = path.as_bytes_with_nul().as_ptr() as *const i8;

    let flags = 0;
    let mode = 0o600;
    let ret = unsafe { ffi::mdb_env_open(env, path_bytes, flags, mode) };

    assert_eq!(ret, 0);

    let mut wtxn = TxnWrite::new(env);

    let mut dbi = 0;
    let name = ptr::null();

    let ret = unsafe {
        ffi::mdb_dbi_open(
            wtxn.txn.txn,
            name,
            0,
            &mut dbi,
        )
    };

    assert_eq!(ret, 0);

    // you can specify that a database will support some typed key/data
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an unsized array of u64
    let db: Database<Type<[i32; 2]>, Str> = Database::new(dbi);

    let ret                   = db.put(&mut wtxn, &[2, 3], "what's up?").unwrap();
    let ret: Option<Cow<str>> = db.get(&wtxn, &[2, 3]).unwrap();

    println!("{:?}", ret);



    // even str are supported,
    // here the key will be an str and the data will be an array of two i32
    let db: Database<Str, Slice<i32>> = Database::new(dbi);

    let ret                     = db.put(&mut wtxn, "hello", &[2, 3][..]).unwrap();
    let ret: Option<Cow<[i32]>> = db.get(&wtxn, "hello").unwrap();

    println!("{:?}", ret);



    // // serde types are also supported but this could be improved a little bit...
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Hello { string: String }

    let db: Database<Str, Serde<Hello>> = Database::new(dbi);

    let hello = Hello { string: String::from("hi") };
    let ret                     = db.put(&mut wtxn, "hello", &hello).unwrap();
    let ret: Option<Cow<Hello>> = db.get(&wtxn, "hello").unwrap();

    println!("{:?}", ret);



    // you can also ignore the key or the data
    let db: Database<Str, Ignore> = Database::new(dbi);

    let ret                  = db.put(&mut wtxn, "hello", &()).unwrap();
    let ret: Option<Cow<()>> = db.get(&wtxn, "hello").unwrap();

    println!("{:?}", ret);




    let ret: Option<Cow<()>> = db.get(&wtxn, "non-existant").unwrap();

    println!("{:?}", ret);



    wtxn.commit();
}
