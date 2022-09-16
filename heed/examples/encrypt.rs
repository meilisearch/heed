use std::error::Error;
use std::fs;
use std::path::Path;

use chacha20::cipher::{KeyIvInit, StreamCipher};
use chacha20::ChaCha20;
use heed::types::*;
use heed::{Checksum, Database, Encrypt, EncryptDecrypt, EnvOpenOptions};

enum Crc32Checksum {}

impl Checksum for Crc32Checksum {
    const SIZE: u32 = 32 / 8;

    fn checksum(input: &[u8], output: &mut [u8], _key: Option<&[u8]>) {
        let checksum = crc32fast::hash(input);
        output.copy_from_slice(&checksum.to_le_bytes());
    }
}

enum Chacha20Encrypt {}

impl Encrypt for Chacha20Encrypt {
    fn encrypt_decrypt(
        _action: EncryptDecrypt,
        input: &[u8],
        output: &mut [u8],
        key: &[u8],
        iv: &[u8],
        _auth: &[u8],
    ) -> Result<(), ()> {
        Ok(ChaCha20::new_from_slices(key, &iv[..12])
            .map_err(drop)?
            .apply_keystream_b2b(input, output)
            .map_err(drop)?)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let env_path = Path::new("target").join("encrypt.mdb");
    let password: &[_; 32] = b"I told you this is my password!!";
    let mac_size = 0;

    let _ = fs::remove_dir_all(&env_path);
    fs::create_dir_all(&env_path)?;

    // We open the environment
    let mut options = EnvOpenOptions::new()
        .encrypt_with::<Chacha20Encrypt>(password.to_vec(), mac_size)
        // By setting the checksum function we will have checksum errors if the decryption
        // fail instead of random LMDB errors due to invalid data in the decrypted pages
        .checksum_with::<Crc32Checksum>();
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
