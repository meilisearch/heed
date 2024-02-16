<p align="center"><img width="280px" src="https://raw.githubusercontent.com/meilisearch/heed/main/assets/heed-pigeon-logo.png"></a>
<h1 align="center" >heed</h1>

[![License](https://img.shields.io/badge/license-MIT-green)](#LICENSE)
[![Crates.io](https://img.shields.io/crates/v/heed)](https://crates.io/crates/heed)
[![Docs](https://docs.rs/heed/badge.svg)](https://docs.rs/heed)
[![dependency status](https://deps.rs/repo/github/meilisearch/heed/status.svg)](https://deps.rs/repo/github/meilisearch/heed)
[![Build](https://github.com/meilisearch/heed/actions/workflows/rust.yml/badge.svg)](https://github.com/meilisearch/heed/actions/workflows/rust.yml)

A Rust-centric [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) abstraction with minimal overhead.

`heed` enables the storage of various Rust types within LMDB, extending support to include Serde-compatible types.

For usage examples, see [heed/examples/](heed/examples/).

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
