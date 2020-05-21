# heed
A fully typed [LMDB]/[MDBX] wrapper with minimum overhead, uses zerocopy internally.

[![Build Status](https://dev.azure.com/renaultcle/heed/_apis/build/status/Kerollmops.heed?branchName=master)](https://dev.azure.com/renaultcle/heed/_build/latest?definitionId=1&branchName=master)
[![Dependency Status](https://deps.rs/repo/github/Kerollmops/heed/status.svg)](https://deps.rs/repo/github/Kerollmops/heed)
[![Heed Doc](https://docs.rs/heed/badge.svg)](https://docs.rs/heed)
[![Crates.io](https://img.shields.io/crates/v/heed.svg)](https://crates.io/crates/heed)

![the opposite of heed](https://thesaurus.plus/img/antonyms/153/heed.png)

This library is able to serialize all kind of types, not just bytes slices, even Serde types are supported.

## Using MDBX instead of LMDB

Heed can also [be used with MDBX][MDBX], this is a compatible backend, you can activate it by using the `mdbx` feature. When MDBX is used the generated databases cannot be read by using the `lmdb` feature, those two kv-stores are not compatible and vice-versa.

Notice that some specific features will only be accessible by using the `mdbx` cargo feature.
Environment creation flags depends on the backend you chose.

To test that heed works with MDBX you can run this command:

```bash
cargo test --features 'mdbx serde-json' --no-default-features
```

## Example Usage

```rust
fs::create_dir_all("target/heed.mdb")?;
let env = EnvOpenOptions::new().open("target/heed.mdb")?;

// We open the default unamed database.
// Specifying the type of the newly created database.
// Here we specify that the key is an str and the value a simple integer.
let db: Database<Str, OwnedType<i32>> = env.create_database(None)?;

// We then open a write transaction and start writing into the database.
// All of those puts are type checked at compile time,
// therefore you cannot write an integer instead of a string.
let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "seven", &7)?;
db.put(&mut wtxn, "zero", &0)?;
db.put(&mut wtxn, "five", &5)?;
db.put(&mut wtxn, "three", &3)?;
wtxn.commit()?;

// We open a read transaction to check if those values are available.
// When we read we also type check at compile time.
let rtxn = env.read_txn()?;

let ret = db.get(&rtxn, "zero")?;
assert_eq!(ret, Some(0));

let ret = db.get(&rtxn, "five")?;
assert_eq!(ret, Some(5));
```

You want to see more about all the possibilities? Go check out [the examples](heed/examples/).

[LMDB]: https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database
[MDBX]: https://github.com/erthink/libmdbx
