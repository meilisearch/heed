<p align="center"><img width="280px" src="https://raw.githubusercontent.com/meilisearch/heed/main/assets/heed-pigeon-logo.png"></a></p>
<h1 align="center" >heed & heed3</h1>

[![License](https://img.shields.io/badge/license-MIT-green)](#LICENSE)
[![Crates.io](https://img.shields.io/crates/v/heed)](https://crates.io/crates/heed)
[![Docs](https://docs.rs/heed/badge.svg)](https://docs.rs/heed)
[![dependency status](https://deps.rs/repo/github/meilisearch/heed/status.svg)](https://deps.rs/repo/github/meilisearch/heed)
[![Build](https://github.com/meilisearch/heed/actions/workflows/rust.yml/badge.svg)](https://github.com/meilisearch/heed/actions/workflows/rust.yml)
[![Discord](https://img.shields.io/discord/1006923006964154428?style=flat&logo=discord&logoColor=ffffff&label=&labelColor=6A7EC2&color=7389D8)](https://discord.com/channels/1006923006964154428/1347203493106024528)

Rust-centric [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) abstractions with minimal overhead. These libraries enable the storage of various Rust types within LMDB, extending support to include Serde-compatible types. It supports not only the LMDB `mdb.master` branch but also the `mdb.master3` branch, which features encryption-at-rest.

## Simple Example Usage

Here is an example on how to store and read entries into LMDB in a safe and ACID way. For usage examples, see [heed/examples/](heed/examples/). To see more advanced usage techniques go check our [Cookbook](https://docs.rs/heed/latest/heed/cookbook/index.html).

```rust
use std::fs;
use std::path::Path;
use heed::{EnvOpenOptions, Database};
use heed::types::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = unsafe { EnvOpenOptions::new().open("my-first-db")? };

    // We open the default unnamed database
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, U32<byteorder::NativeEndian>> = env.create_database(&mut wtxn, None)?;

    // We open a write transaction
    db.put(&mut wtxn, "seven", &7)?;
    db.put(&mut wtxn, "zero", &0)?;
    db.put(&mut wtxn, "five", &5)?;
    db.put(&mut wtxn, "three", &3)?;
    wtxn.commit()?;

    // We open a read transaction to check if those values are now available
    let mut rtxn = env.read_txn()?;

    let ret = db.get(&rtxn, "zero")?;
    assert_eq!(ret, Some(0));

    let ret = db.get(&rtxn, "five")?;
    assert_eq!(ret, Some(5));

    Ok(())
}
```

## Working with two Crates: heed and heed3

The heed and heed3 crates manage a shared codebase. Within the heed3 folder, you can find the Cargo.toml specific to the heed3 crate.
To facilitate work on heed3, utilize the `convert-to-heed3.sh` script.

This script conveniently moves the `heed3/Cargo.toml` file to the `heed/` folder, updates the `heed::` references to `heed3::`, and generates a commit for easy rollback if needed.

## Building from Source

You can use this command to clone the repository:

```bash
git clone --recursive https://github.com/meilisearch/heed.git
cd heed
cargo build
```

However, if you already cloned it and forgot to initialize the submodules, execute the following command:

```bash
git submodule update --init
```
