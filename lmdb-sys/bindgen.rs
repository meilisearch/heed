use std::env;
use std::path::PathBuf;

use bindgen::callbacks::{IntKind, ParseCallbacks};

#[derive(Debug)]
struct Callbacks;

impl ParseCallbacks for Callbacks {
    fn process_comment(&self, comment: &str) -> Option<String> {
        Some(doxygen_rs::transform(comment))
    }

    fn int_macro(&self, name: &str, _value: i64) -> Option<IntKind> {
        match name {
            "MDB_ADDR_BUSY"
            | "MDB_BAD_CHECKSUM"
            | "MDB_BAD_DBI"
            | "MDB_BAD_RSLOT"
            | "MDB_BAD_TXN"
            | "MDB_BAD_VALSIZE"
            | "MDB_CANT_ROLLBACK"
            | "MDB_CORRUPTED"
            | "MDB_CRYPTO_FAIL"
            | "MDB_CURSOR_FULL"
            | "MDB_DBIS_BUSY"
            | "MDB_DBS_FULL"
            | "MDB_ENV_BUSY"
            | "MDB_ENV_ENCRYPTION"
            | "MDB_INCOMPATIBLE"
            | "MDB_INVALID"
            | "MDB_IS_READONLY"
            | "MDB_KEYEXIST"
            | "MDB_LAST_ERRCODE"
            | "MDB_MAP_FULL"
            | "MDB_MAP_RESIZED"
            | "MDB_NOTFOUND"
            | "MDB_PAGE_FULL"
            | "MDB_PAGE_NOTFOUND"
            | "MDB_PANIC"
            | "MDB_PROBLEM"
            | "MDB_READERS_FULL"
            | "MDB_SHORT_WRITE"
            | "MDB_SUCCESS"
            | "MDB_TLS_FULL"
            | "MDB_TXN_FULL"
            | "MDB_TXN_PENDING"
            | "MDB_VERSION_MISMATCH" => Some(IntKind::Int),
            "MDB_SIZE_MAX" => Some(IntKind::U64),
            _ => Some(IntKind::UInt),
        }
    }
}

pub fn generate() {
    let mut lmdb = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    lmdb.push("lmdb");
    lmdb.push("libraries");
    lmdb.push("liblmdb");

    let mut out_path = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    out_path.push("src");

    let bindings = bindgen::Builder::default()
        .header(lmdb.join("lmdb.h").to_string_lossy())
        .allowlist_var("^(MDB|mdb)_.*")
        .allowlist_type("^(MDB|mdb)_.*")
        .allowlist_function("^(MDB|mdb)_.*")
        .size_t_is_usize(true)
        .ctypes_prefix("::libc")
        .use_core()
        .blocklist_item("mode_t")
        .blocklist_item("mdb_mode_t")
        .blocklist_item("mdb_filehandle_t")
        .blocklist_item("^__.*")
        .parse_callbacks(Box::new(Callbacks {}))
        .layout_tests(false)
        .prepend_enum_name(false)
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
