# zerocopy-lmdb
An LMDB wrapper with the minimum overhead possible that uses the zerocopy library.

This library is able to serialize all kind of types, not just bytes slices, even Serde types are supported.

```rust
fs::create_dir_all("target/zerocopy.mdb")?;

let env = EnvOpenOptions::new()
    .map_size(10 * 1024 * 1024 * 1024) // 10GB
    .max_dbs(3000)
    .open("target/zerocopy.mdb")?;

#[derive(Debug, PartialEq, Eq, AsBytes, FromBytes, Unaligned)]
#[repr(C)]
struct ZeroBytes {
    bytes: [u8; 12],
}

let db: Database<Str, UnalignedType<ZeroBytes>> = env.create_database(Some("zerocopy-struct"))?;

let mut wtxn = env.write_txn()?;

let zerobytes = ZeroBytes { bytes: [24; 12] };
db.put(&mut wtxn, "zero", &zerobytes)?;

let ret = db.get(&wtxn, "zero")?;

assert_eq!(ret, Some(zerobytes));
wtxn.commit()?;
```

Yo want to see more about all the possibilities? Go check out [the example](examples/all-types.rs).

## Where is the 0.1 version?

I am currently not sure about the library name so I did not published a version on crates.io.
So to be able to see the documentation you will need to have a nighlty rust version, clone the repository
and generate the documentation by yourself.

About the name, I think that `zerocopy-lmdb` is way to long and could be reduced to `zlmdb` or something like that.

```bash
git clone https://github.com/Kerollmops/zerocopy-lmdb.git
cd zerocopy-lmdb
# rustup override set nighlty
cargo doc --open
```
