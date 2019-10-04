use std::borrow::Cow;
use zerocopy_lmdb::{Database, Type, Slice, Str, Ignore};

fn main() {
    // you can specify that a database will support some typed key/data
    //
    // like here we specify that the key will be an array of two i32
    // and the data will be an unsized array of u64
    let db: Database<Type<[i32; 2]>, Slice<u64>> = Database::new();

    let ret: Cow<[u64]> = db.put(&[2, 3], &[21, 22, 33][..]).unwrap().unwrap();
    let ret: Cow<[u64]> = db.get(&[2, 3]).unwrap().unwrap();



    // even str are supported,
    // here the key will be an str and the data will be an array of two i32
    let db: Database<Str, Type<[i32; 2]>> = Database::new();

    let ret: Cow<[i32; 2]> = db.put("hello", &[2, 3]).unwrap().unwrap();
    let ret: Cow<[i32; 2]> = db.get("hello").unwrap().unwrap();



    // serde types are also supported but this could be improved a little bit...
    #[derive(Clone, Serialize, Deserialize)]
    struct Hello { string: String }

    let db: Database<Str, Serde<Hello>> = Database::new();

    let hello = Hello { string: String::from("hi") };
    let ret: Cow<Serde<Hello>> = db.put("hello", &Serde(hello)).unwrap().unwrap();
    let ret: Cow<Serde<Hello>> = db.get("hello").unwrap().unwrap();



    // you can also ignore the key or the data
    let db: Database<Str, Ignore> = Database::new();

    let ret: Cow<()> = db.put("hello", &()).unwrap().unwrap();
    let ret: Cow<()> = db.get("hello").unwrap().unwrap();
}
