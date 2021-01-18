# heed
A fully typed [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) wrapper with minimum overhead, uses bytemuck internally.

[![License](https://img.shields.io/badge/license-MIT-green)](#LICENSE)
[![Crates.io](https://img.shields.io/crates/v/heed)](https://crates.io/crates/heed)
[![Docs](https://docs.rs/heed/badge.svg)](https://docs.rs/heed)
[![dependency status](https://deps.rs/repo/github/meilisearch/heed/status.svg)](https://deps.rs/repo/github/meilisearch/heed)
[![Build](https://github.com/meilisearch/heed/actions/workflows/test.yml/badge.svg)](https://github.com/meilisearch/heed/actions/workflows/test.yml)

![the opposite of heed](https://thesaurus.plus/img/antonyms/153/heed.png)

This library is able to serialize all kind of types, not just bytes slices, even _Serde_ types are supported.

## Example Usage

```rust
fs::create_dir_all("my-env.mdb")?;
let env = EnvOpenOptions::new().max_dbs(10).open("my-env.mdb")?;

// We open the default unamed database.
// Specifying the type of the newly created database.
// Here we specify that the key is an str and the value a simple integer.
let mut wtxn = env.write_txn()?;
let db: Database<Str, OwnedType<i32>> = env.create_database(&mut wtxn, None)?;

// We then open a write transaction and start writing into the database.
// All of those puts are type checked at compile time,
// therefore you cannot write an integer instead of a string.
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

## Building from Source

If you don't already have clone the repository you can use this command:

```bash
git clone --recursive https://github.com/meilisearch/heed.git
cd heed
cargo build
```

However, if you already cloned it and forgot about the initialising the submodules:

```bash
git submodule update --init
```
