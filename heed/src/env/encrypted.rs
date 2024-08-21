use std::collections::hash_map::Entry;
use std::ffi::CString;
#[cfg(windows)]
use std::ffi::OsStr;
use std::io::ErrorKind::NotFound;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::panic::catch_unwind;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io, ptr};

use aead::consts::U0;
use aead::generic_array::typenum::Unsigned;
use aead::generic_array::GenericArray;
use aead::{AeadCore, AeadMutInPlace, Key, KeyInit, KeySizeUser, Nonce, Tag};
use synchronoise::SignalEvent;

#[cfg(windows)]
use crate::env::OsStrExtLmdb as _;
use crate::env::{canonicalize_path, Env, EnvFlags, EnvInner, OPENED_ENV};
use crate::mdb::ffi;
use crate::mdb::lmdb_error::mdb_result;
use crate::{Error, Result};

pub struct EnvEntry {
    pub(super) env: Option<Env>,
    pub(super) signal_event: Arc<SignalEvent>,
    pub(super) options: SimplifiedOpenOptions,
}

/// A simplified version of the options that were used to open a given [`Env`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimplifiedOpenOptions {
    /// Weither this [`Env`] has been opened with an encryption/decryption algorithm.
    pub use_encryption: bool,
    /// The maximum size this [`Env`] with take in bytes or [`None`] if it was not specified.
    pub map_size: Option<usize>,
    /// The maximum number of concurrent readers or [`None`] if it was not specified.
    pub max_readers: Option<u32>,
    /// The maximum number of opened database or [`None`] if it was not specified.
    pub max_dbs: Option<u32>,
    /// The raw flags enabled for this [`Env`] or [`None`] if it was not specified.
    pub flags: u32,
}

impl<E: AeadMutInPlace + KeyInit> From<&EnvOpenOptions<E>> for SimplifiedOpenOptions {
    fn from(eoo: &EnvOpenOptions<E>) -> SimplifiedOpenOptions {
        let EnvOpenOptions { encrypt, map_size, max_readers, max_dbs, flags } = eoo;
        SimplifiedOpenOptions {
            use_encryption: encrypt.is_some(),
            map_size: *map_size,
            max_readers: *max_readers,
            max_dbs: *max_dbs,
            flags: flags.bits(),
        }
    }
}

/// Options and flags which can be used to configure how an environment is opened.
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EnvOpenOptions<E: AeadMutInPlace + KeyInit = DummyEncrypt> {
    encrypt: Option<Key<E>>,
    map_size: Option<usize>,
    max_readers: Option<u32>,
    max_dbs: Option<u32>,
    flags: EnvFlags,
}

impl Default for EnvOpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvOpenOptions {
    /// Creates a blank new set of options ready for configuration.
    pub fn new() -> EnvOpenOptions {
        EnvOpenOptions {
            encrypt: None,
            map_size: None,
            max_readers: None,
            max_dbs: None,
            flags: EnvFlags::empty(),
        }
    }
}

impl<E: AeadMutInPlace + KeyInit> EnvOpenOptions<E> {
    /// Set the size of the memory map to use for this environment.
    pub fn map_size(&mut self, size: usize) -> &mut Self {
        self.map_size = Some(size);
        self
    }

    /// Set the maximum number of threads/reader slots for the environment.
    pub fn max_readers(&mut self, readers: u32) -> &mut Self {
        self.max_readers = Some(readers);
        self
    }

    /// Set the maximum number of named databases for the environment.
    pub fn max_dbs(&mut self, dbs: u32) -> &mut Self {
        self.max_dbs = Some(dbs);
        self
    }

    /// Specifies that the [`Env`] will be encrypted using the `A` algorithm with the given `key`.
    ///
    /// You can find more compatible algorithms on [the RustCrypto/AEADs page](https://github.com/RustCrypto/AEADs#crates).
    ///
    /// Note that you cannot use any type of encryption algorithm as LMDB exposes a nonce of 16 bytes.
    /// Heed makes sure to truncate it if necessary.
    ///
    /// As an example, XChaCha20 requires a 20 bytes long nonce. However, XChaCha20 is used to protect
    /// against nonce misuse in systems that use randomly generated nonces i.e., to protect against
    /// weak RNGs. There is no need to use this kind of algorithm in LMDB since LMDB nonces aren't
    /// random and are guaranteed to be unique.
    ///
    /// ## Basic Example
    ///
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use argon2::Argon2;
    /// use chacha20poly1305::{ChaCha20Poly1305, Key};
    /// use heed3::types::*;
    /// use heed3::{EnvOpenOptions, Database};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let env_path = Path::new("target").join("encrypt.mdb");
    /// let password = "This is the password that will be hashed by the argon2 algorithm";
    /// let salt = "The salt added to the password hashes to add more security when stored";
    ///
    /// let _ = fs::remove_dir_all(&env_path);
    /// fs::create_dir_all(&env_path)?;
    ///
    /// let mut key = Key::default();
    /// Argon2::default().hash_password_into(password.as_bytes(), salt.as_bytes(), &mut key)?;
    ///
    /// // We open the environment
    /// let mut options = EnvOpenOptions::new().encrypt_with::<ChaCha20Poly1305>(key);
    /// let env = unsafe {
    ///     options
    ///         .map_size(10 * 1024 * 1024) // 10MB
    ///         .max_dbs(3)
    ///         .open(&env_path)?
    /// };
    ///
    /// let key1 = "first-key";
    /// let val1 = "this is a secret info";
    /// let key2 = "second-key";
    /// let val2 = "this is another secret info";
    ///
    /// // We create database and write secret values in it
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;
    /// db.put(&mut wtxn, key1, val1)?;
    /// db.put(&mut wtxn, key2, val2)?;
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    ///
    /// ## Example Showing limitations
    ///
    /// ```compile_fail
    /// use std::fs;
    /// use std::path::Path;
    /// use argon2::Argon2;
    /// use chacha20poly1305::{ChaCha20Poly1305, Key};
    /// use heed3::types::*;
    /// use heed3::{EnvOpenOptions, Database};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let env_path = Path::new("target").join("encrypt.mdb");
    /// let password = "This is the password that will be hashed by the argon2 algorithm";
    /// let salt = "The salt added to the password hashes to add more security when stored";
    ///
    /// let _ = fs::remove_dir_all(&env_path);
    /// fs::create_dir_all(&env_path)?;
    ///
    /// let mut key = Key::default();
    /// Argon2::default().hash_password_into(password.as_bytes(), salt.as_bytes(), &mut key)?;
    ///
    /// // We open the environment
    /// let mut options = EnvOpenOptions::new().encrypt_with::<ChaCha20Poly1305>(key);
    /// let env = unsafe {
    ///     options
    ///         .map_size(10 * 1024 * 1024) // 10MB
    ///         .max_dbs(3)
    ///         .open(&env_path)?
    /// };
    ///
    /// let key1 = "first-key";
    /// let key2 = "second-key";
    ///
    /// // Declare the read transaction as mutable because LMDB, when using encryption,
    /// // does not allow keeping keys between reads due to the use of an internal cache.
    /// let mut rtxn = env.read_txn()?;
    /// let val1 = db.get(&mut rtxn, key1)?;
    /// let val2 = db.get(&mut rtxn, key2)?;
    ///
    /// // This example won't compile because val1 cannot be used for too long.
    /// let _force_keep = val1;
    /// # Ok(()) }
    /// ```
    #[cfg(feature = "encryption")]
    pub fn encrypt_with<A: AeadMutInPlace + KeyInit>(self, key: Key<A>) -> EnvOpenOptions<A> {
        let EnvOpenOptions { encrypt: _, map_size, max_readers, max_dbs, flags } = self;
        EnvOpenOptions { encrypt: Some(key), map_size, max_readers, max_dbs, flags }
    }

    /// Set one or [more LMDB flags](http://www.lmdb.tech/doc/group__mdb__env.html).
    ///
    /// ```
    /// use std::fs;
    /// use std::path::Path;
    /// use heed::{EnvOpenOptions, Database, EnvFlags};
    /// use heed::types::*;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// fs::create_dir_all(Path::new("target").join("database.mdb"))?;
    /// let mut env_builder = EnvOpenOptions::new();
    /// unsafe { env_builder.flags(EnvFlags::NO_TLS | EnvFlags::NO_META_SYNC); }
    /// let dir = tempfile::tempdir().unwrap();
    /// let env = unsafe { env_builder.open(dir.path())? };
    ///
    /// // we will open the default unamed database
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, U32<byteorder::NativeEndian>> = env.create_database(&mut wtxn, None)?;
    ///
    /// // opening a write transaction
    /// db.put(&mut wtxn, "seven", &7)?;
    /// db.put(&mut wtxn, "zero", &0)?;
    /// db.put(&mut wtxn, "five", &5)?;
    /// db.put(&mut wtxn, "three", &3)?;
    /// wtxn.commit()?;
    ///
    /// // Force the OS to flush the buffers (see Flag::NoSync and Flag::NoMetaSync).
    /// env.force_sync();
    ///
    /// // opening a read transaction
    /// // to check if those values are now available
    /// let mut rtxn = env.read_txn()?;
    ///
    /// let ret = db.get(&rtxn, "zero")?;
    /// assert_eq!(ret, Some(0));
    ///
    /// let ret = db.get(&rtxn, "five")?;
    /// assert_eq!(ret, Some(5));
    /// # Ok(()) }
    /// ```
    ///
    /// # Safety
    ///
    /// It is unsafe to use unsafe LMDB flags such as `NO_SYNC`, `NO_META_SYNC`, or `NO_LOCK`.
    pub unsafe fn flags(&mut self, flags: EnvFlags) -> &mut Self {
        self.flags |= flags;
        self
    }

    /// Open an environment that will be located at the specified path.
    ///
    /// # Safety
    /// LMDB is backed by a memory map [^1] which comes with some safety precautions.
    ///
    /// Memory map constructors are marked `unsafe` because of the potential
    /// for Undefined Behavior (UB) using the map if the underlying file is
    /// subsequently modified, in or out of process.
    ///
    /// LMDB itself has a locking system that solves this problem,
    /// but it will not save you from making mistakes yourself.
    ///
    /// These are some things to take note of:
    ///
    /// - Avoid long-lived transactions, they will cause the database to grow quickly [^2]
    /// - Avoid aborting your process with an active transaction [^3]
    /// - Do not use LMDB on remote filesystems, even between processes on the same host [^4]
    /// - You must manage concurrent accesses yourself if using [`EnvFlags::NO_LOCK`] [^5]
    /// - Anything that causes LMDB's lock file to be broken will cause synchronization issues and may introduce UB [^6]
    ///
    /// `heed` itself upholds some safety invariants, including but not limited to:
    /// - Calling [`EnvOpenOptions::open`] twice in the same process, at the same time is OK [^7]
    ///
    /// For more details, it is highly recommended to read LMDB's official documentation. [^8]
    ///
    /// [^1]: <https://en.wikipedia.org/wiki/Memory_map>
    /// [^2]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L107-L114>
    /// [^3]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L118-L121>
    /// [^4]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    /// [^5]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L129>
    /// [^6]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L49-L52>
    /// [^7]: <https://github.com/LMDB/lmdb/blob/b8e54b4c31378932b69f1298972de54a565185b1/libraries/liblmdb/lmdb.h#L102-L105>
    /// [^8]: <http://www.lmdb.tech/doc/index.html>
    pub unsafe fn open<P: AsRef<Path>>(&self, path: P) -> Result<Env> {
        let mut lock = OPENED_ENV.write().unwrap();

        let path = match canonicalize_path(path.as_ref()) {
            Err(err) => {
                if err.kind() == NotFound && self.flags.contains(EnvFlags::NO_SUB_DIR) {
                    let path = path.as_ref();
                    match path.parent().zip(path.file_name()) {
                        Some((dir, file_name)) => canonicalize_path(dir)?.join(file_name),
                        None => return Err(err.into()),
                    }
                } else {
                    return Err(err.into());
                }
            }
            Ok(path) => path,
        };

        let original_options = SimplifiedOpenOptions::from(self);
        match lock.entry(path) {
            Entry::Occupied(entry) => {
                let env = entry.get().env.clone().ok_or(Error::DatabaseClosing)?;
                let options = entry.get().options.clone();
                if options == original_options {
                    Ok(env)
                } else {
                    Err(Error::BadOpenOptions { env, options: original_options })
                }
            }
            Entry::Vacant(entry) => {
                let path = entry.key();
                let path_str = CString::new(path.as_os_str().as_bytes()).unwrap();

                unsafe {
                    let mut env: *mut ffi::MDB_env = ptr::null_mut();
                    mdb_result(ffi::mdb_env_create(&mut env))?;

                    // This block will only be executed if the "encryption" feature is enabled.
                    if let Some(key) = &self.encrypt {
                        let key = crate::into_val(key);
                        mdb_result(ffi::mdb_env_set_encrypt(
                            env,
                            Some(encrypt_func_wrapper::<E>),
                            &key,
                            <E as AeadCore>::TagSize::U32,
                        ))?;
                    }

                    if let Some(size) = self.map_size {
                        if size % page_size::get() != 0 {
                            let msg = format!(
                                "map size ({}) must be a multiple of the system page size ({})",
                                size,
                                page_size::get()
                            );
                            return Err(Error::Io(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                msg,
                            )));
                        }
                        mdb_result(ffi::mdb_env_set_mapsize(env, size))?;
                    }

                    if let Some(readers) = self.max_readers {
                        mdb_result(ffi::mdb_env_set_maxreaders(env, readers))?;
                    }

                    if let Some(dbs) = self.max_dbs {
                        mdb_result(ffi::mdb_env_set_maxdbs(env, dbs))?;
                    }

                    // When the `read-txn-no-tls` feature is enabled, we must force LMDB
                    // to avoid using the thread local storage, this way we allow users
                    // to use references of RoTxn between threads safely.
                    let flags = if cfg!(feature = "read-txn-no-tls") {
                        self.flags | EnvFlags::NO_TLS
                    } else {
                        self.flags
                    };

                    let result =
                        mdb_result(ffi::mdb_env_open(env, path_str.as_ptr(), flags.bits(), 0o600));

                    match result {
                        Ok(()) => {
                            let signal_event = Arc::new(SignalEvent::manual(false));
                            let inner = EnvInner { env, path: path.clone() };
                            let env = Env(Arc::new(inner));
                            let cache_entry = EnvEntry {
                                env: Some(env.clone()),
                                options: original_options,
                                signal_event,
                            };
                            entry.insert(cache_entry);
                            Ok(env)
                        }
                        Err(e) => {
                            ffi::mdb_env_close(env);
                            Err(e.into())
                        }
                    }
                }
            }
        }
    }
}

impl<E: AeadMutInPlace + KeyInit> fmt::Debug for EnvOpenOptions<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let EnvOpenOptions { encrypt, map_size, max_readers, max_dbs, flags } = self;
        f.debug_struct("EnvOpenOptions")
            .field("encrypted", &encrypt.is_some())
            .field("map_size", &map_size)
            .field("max_readers", &max_readers)
            .field("max_dbs", &max_dbs)
            .field("flags", &flags)
            .finish()
    }
}

fn encrypt<A: AeadMutInPlace + KeyInit>(
    key: &[u8],
    nonce: &[u8],
    aad: &[u8],
    plaintext: &[u8],
    chipertext_out: &mut [u8],
    auth_out: &mut [u8],
) -> aead::Result<()> {
    chipertext_out.copy_from_slice(plaintext);
    let key: &Key<A> = key.try_into().unwrap();
    let nonce: &Nonce<A> = if nonce.len() >= A::NonceSize::USIZE {
        nonce[..A::NonceSize::USIZE].into()
    } else {
        return Err(aead::Error);
    };
    let mut aead = A::new(key);
    let tag = aead.encrypt_in_place_detached(nonce, aad, chipertext_out)?;
    auth_out.copy_from_slice(&tag);
    Ok(())
}

fn decrypt<A: AeadMutInPlace + KeyInit>(
    key: &[u8],
    nonce: &[u8],
    aad: &[u8],
    chipher_text: &[u8],
    output: &mut [u8],
    auth_in: &[u8],
) -> aead::Result<()> {
    output.copy_from_slice(chipher_text);
    let key: &Key<A> = key.try_into().unwrap();
    let nonce: &Nonce<A> = if nonce.len() >= A::NonceSize::USIZE {
        nonce[..A::NonceSize::USIZE].into()
    } else {
        return Err(aead::Error);
    };
    let tag: &Tag<A> = auth_in.try_into().unwrap();
    let mut aead = A::new(key);
    aead.decrypt_in_place_detached(nonce, aad, output, tag)
}

/// The wrapper function that is called by LMDB that directly calls
/// the Rust idiomatic function internally.
unsafe extern "C" fn encrypt_func_wrapper<E: AeadMutInPlace + KeyInit>(
    src: *const ffi::MDB_val,
    dst: *mut ffi::MDB_val,
    key_ptr: *const ffi::MDB_val,
    encdec: i32,
) -> i32 {
    let result = catch_unwind(|| {
        let input = std::slice::from_raw_parts((*src).mv_data as *const u8, (*src).mv_size);
        let output = std::slice::from_raw_parts_mut((*dst).mv_data as *mut u8, (*dst).mv_size);
        let key = std::slice::from_raw_parts((*key_ptr).mv_data as *const u8, (*key_ptr).mv_size);
        let iv = std::slice::from_raw_parts(
            (*key_ptr.offset(1)).mv_data as *const u8,
            (*key_ptr.offset(1)).mv_size,
        );
        let auth = std::slice::from_raw_parts_mut(
            (*key_ptr.offset(2)).mv_data as *mut u8,
            (*key_ptr.offset(2)).mv_size,
        );

        let aad = [];
        let nonce = iv;
        let result = if encdec == 1 {
            encrypt::<E>(&key, nonce, &aad, input, output, auth)
        } else {
            decrypt::<E>(&key, nonce, &aad, input, output, auth)
        };

        result.is_err() as i32
    });

    match result {
        Ok(out) => out,
        Err(_) => 1,
    }
}

/// A dummy encryption/decryption algorithm that must never be used.
/// Only here for Rust API purposes.
pub enum DummyEncrypt {}

impl AeadMutInPlace for DummyEncrypt {
    fn encrypt_in_place_detached(
        &mut self,
        _nonce: &Nonce<Self>,
        _associated_data: &[u8],
        _buffer: &mut [u8],
    ) -> aead::Result<Tag<Self>> {
        Err(aead::Error)
    }

    fn decrypt_in_place_detached(
        &mut self,
        _nonce: &Nonce<Self>,
        _associated_data: &[u8],
        _buffer: &mut [u8],
        _tag: &Tag<Self>,
    ) -> aead::Result<()> {
        Err(aead::Error)
    }
}

impl AeadCore for DummyEncrypt {
    type NonceSize = U0;
    type TagSize = U0;
    type CiphertextOverhead = U0;
}

impl KeySizeUser for DummyEncrypt {
    type KeySize = U0;
}

impl KeyInit for DummyEncrypt {
    fn new(_key: &GenericArray<u8, Self::KeySize>) -> Self {
        panic!("This DummyEncrypt type must not be used")
    }
}
