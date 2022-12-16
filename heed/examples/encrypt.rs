use std::error::Error;
use std::fs;
use std::path::Path;

use argon2::Argon2;
use chacha20poly1305::{ChaCha20Poly1305, Key};
use heed::types::*;
use heed::{Database, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let env_path = Path::new("target").join("encrypt.mdb");
    let password = "This is the password that will be hashed by the argon2 algorithm";
    let salt = "The salt added to the password hashes to add more security when stored";

    let _ = fs::remove_dir_all(&env_path);
    fs::create_dir_all(&env_path)?;

    // We choose to use argon2 as our Key Derivation Function, but you can choose whatever you want.
    // <https://github.com/RustCrypto/traits/tree/master/password-hash#supported-crates>
    let mut key = Key::default();
    Argon2::default().hash_password_into(password.as_bytes(), salt.as_bytes(), &mut key)?;

    // We open the environment
    let mut options = EnvOpenOptions::new().encrypt_with::<ChaCha20Poly1305>(key);
    let env = options
        .map_size(10 * 1024 * 1024) // 10MB
        .max_dbs(3)
        .open(&env_path)?;

    let key1 = "first-key";
    let val1 = "this is a secret info";
    let key2 = "second-key";
    let val2 = "this is another secret info";

    // We create database and write secret values in it
    let mut wtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;
    db.put(&mut wtxn, key1, val1)?;
    db.put(&mut wtxn, key2, val2)?;
    wtxn.commit()?;
    env.prepare_for_closing().wait();

    // We reopen the environment now
    let env = options.open(&env_path)?;

    // We check that the secret entries are correctly decrypted
    let mut rtxn = env.write_txn()?;
    let db: Database<Str, Str> = env.open_database(&mut rtxn, Some("first"))?.unwrap();
    let mut iter = db.iter(&rtxn)?;
    assert_eq!(iter.next().transpose()?, Some((key1, val1)));
    assert_eq!(iter.next().transpose()?, Some((key2, val2)));
    assert_eq!(iter.next().transpose()?, None);

    Ok(())
}
