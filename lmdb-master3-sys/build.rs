extern crate cc;
extern crate pkg_config;

#[cfg(feature = "bindgen")]
extern crate bindgen;

#[cfg(feature = "bindgen")]
#[path = "bindgen.rs"]
mod generate;

use std::env;
use std::path::PathBuf;

macro_rules! warn {
    ($message:expr) => {
        println!("cargo:warning={}", $message);
    };
}

fn main() {
    #[cfg(feature = "bindgen")]
    generate::generate();

    let mut lmdb = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    lmdb.push("lmdb");
    lmdb.push("libraries");
    lmdb.push("liblmdb");

    if cfg!(feature = "with-fuzzer") && cfg!(feature = "with-fuzzer-no-link") {
        warn!("Features `with-fuzzer` and `with-fuzzer-no-link` are mutually exclusive.");
        warn!("Building with `-fsanitize=fuzzer`.");
    }

    if cfg!(feature = "vendored") || pkg_config::find_library("lmdb").is_err() {
        let mut builder = cc::Build::new();

        builder
            .file(lmdb.join("mdb.c"))
            .file(lmdb.join("midl.c"))
            // https://github.com/mozilla/lmdb/blob/b7df2cac50fb41e8bd16aab4cc5fd167be9e032a/libraries/liblmdb/Makefile#L23
            .flag_if_supported("-Wno-unused-parameter")
            .flag_if_supported("-Wbad-function-cast")
            .flag_if_supported("-Wuninitialized");

        if env::var("CARGO_FEATURE_WITH_ASAN").is_ok() {
            builder.flag("-fsanitize=address");
        }

        if env::var("CARGO_FEATURE_WITH_FUZZER").is_ok() {
            builder.flag("-fsanitize=fuzzer");
        } else if env::var("CARGO_FEATURE_WITH_FUZZER_NO_LINK").is_ok() {
            builder.flag("-fsanitize=fuzzer-no-link");
        }

        builder.compile("liblmdb.a")
    }
}
