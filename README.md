# heed
A fully typed LMDB wrapper with minimum overhead, uses zerocopy internally.

[![Build Status](https://dev.azure.com/renaultcle/heed/_apis/build/status/Kerollmops.heed?branchName=master)](https://dev.azure.com/renaultcle/heed/_build/latest?definitionId=1&branchName=master)
[![Dependency Status](https://deps.rs/repo/github/Kerollmops/heed/status.svg)](https://deps.rs/repo/github/Kerollmops/heed)
[![Heed Doc](https://docs.rs/heed/badge.svg)](https://docs.rs/heed)
[![Crates.io](https://img.shields.io/crates/v/$CRATE.svg)](https://crates.io/crates/heed)

![the opposite of heed](https://thesaurus.plus/img/antonyms/153/heed.png)

This library is able to serialize all kind of types, not just bytes slices, even Serde types are supported.

```rust
fs::create_dir_all("target/zerocopy.mdb")?;
let env = EnvOpenOptions::new().open("target/zerocopy.mdb")?;

// we will open the default unamed database
let db: Database<Str, OwnedType<i32>> = env.create_database(None)?;

// opening a write transaction
let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "seven", &7)?;
db.put(&mut wtxn, "zero", &0)?;
db.put(&mut wtxn, "five", &5)?;
db.put(&mut wtxn, "three", &3)?;
wtxn.commit()?;

// opening a read transaction
// to check if those values are now available
let mut rtxn = env.read_txn()?;

let ret = db.get(&rtxn, "zero")?;
assert_eq!(ret, Some(0));

let ret = db.get(&rtxn, "five")?;
assert_eq!(ret, Some(5));
```

You want to see more about all the possibilities? Go check out [the example](examples/all-types.rs).
