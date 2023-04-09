use std::any::TypeId;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::CString;
use std::ops::Deref;
use std::{ptr, sync};

use crate::env::DatabaseType::{self, Typed, Untyped};
use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::{Database, Env, Error, PolyDatabase, Result};

/// A read-only transaction.
pub struct RoTxn<'e> {
    pub(crate) txn: *mut ffi::MDB_txn,
    env: &'e Env,
}

impl<'e> RoTxn<'e> {
    pub(crate) fn new(env: &'e Env) -> Result<RoTxn<'e>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn { txn, env })
    }

    pub(crate) fn env_mut_ptr(&self) -> *mut ffi::MDB_env {
        self.env.env_mut_ptr()
    }

    // get the dbi from the env without any call to LMDB
    pub fn open_database<KC, DC>(
        &self,
        name: Option<&str>,
    ) -> Result<Option<Database<'static, KC, DC>>>
    where
        KC: 'static,
        DC: 'static,
    {
        let mut lock = self.env.0.dbi_open_mutex.lock().unwrap();

        match lock.entry(name.map(ToOwned::to_owned)) {
            Entry::Occupied(mut entry) => {
                let (dbi, t) = entry.get_mut();
                let types = DatabaseType::Typed {
                    key_type: TypeId::of::<KC>(),
                    data_type: TypeId::of::<DC>(),
                };

                if t.is_none() {
                    *t = Some(types);
                    Ok(Some(Database::new(self.env_mut_ptr() as _, *dbi)))
                } else if *t != Some(types) {
                    Err(Error::InvalidDatabaseTyping)
                } else {
                    Ok(Some(Database::new(self.env_mut_ptr() as _, *dbi)))
                }
            }
            Entry::Vacant(entry) => Ok(None),
        }
    }

    // get the dbi from the env without any call to LMDB
    pub fn open_poly_database(&self, name: Option<&str>) -> Result<Option<PolyDatabase<'static>>> {
        let mut lock = self.env.0.dbi_open_mutex.lock().unwrap();

        match lock.entry(name.map(ToOwned::to_owned)) {
            Entry::Occupied(mut entry) => {
                let types = DatabaseType::Untyped;

                let (dbi, t) = entry.get_mut();
                if t.is_none() {
                    *t = Some(types);
                    Ok(Some(PolyDatabase::new(self.env_mut_ptr() as _, *dbi)))
                } else if *t != Some(types) {
                    Err(Error::InvalidDatabaseTyping)
                } else {
                    Ok(Some(PolyDatabase::new(self.env_mut_ptr() as _, *dbi)))
                }
            }
            Entry::Vacant(entry) => Ok(None),
        }
    }
}

impl Drop for RoTxn<'_> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            abort_txn(self.txn);
        }
    }
}

#[cfg(feature = "sync-read-txn")]
unsafe impl<T> Sync for RoTxn<'_> {}

fn abort_txn(txn: *mut ffi::MDB_txn) {
    // Asserts that the transaction hasn't been already committed.
    assert!(!txn.is_null());
    unsafe { ffi::mdb_txn_abort(txn) }
}

/// A read-write transaction.
pub struct RwTxn<'p> {
    pub(crate) txn: RoTxn<'p>,
    // The list of dbi only available while the transaction is alive
    // this list will be added to the global list if the transaction is successfully committed.
    local_opened_dbi: sync::Mutex<HashMap<Option<String>, (u32, DatabaseType)>>,
}

impl<'p> RwTxn<'p> {
    pub(crate) fn new(env: &'p Env) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        unsafe { mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr(), ptr::null_mut(), 0, &mut txn))? };
        Ok(RwTxn { txn: RoTxn { txn, env }, local_opened_dbi: Default::default() })
    }

    pub(crate) fn nested(env: &'p Env, parent: &'p mut RwTxn) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_ptr: *mut ffi::MDB_txn = parent.txn.txn;
        unsafe { mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr(), parent_ptr, 0, &mut txn))? };
        Ok(RwTxn { txn: RoTxn { txn, env }, local_opened_dbi: Default::default() })
    }

    pub(crate) fn env_mut_ptr(&self) -> *mut ffi::MDB_env {
        self.txn.env.env_mut_ptr()
    }

    // TODO document me
    pub fn open_database<'t, KC, DC>(
        &'t self,
        name: Option<&str>,
    ) -> Result<Option<Database<'t, KC, DC>>>
    where
        KC: 'static,
        DC: 'static,
    {
        let types = Typed { key_type: TypeId::of::<KC>(), data_type: TypeId::of::<DC>() };
        match self.raw_init_database(name, types, false) {
            Ok(dbi) => Ok(Some(Database::new(self.env_mut_ptr() as _, dbi))),
            Err(Error::Mdb(e)) if e.not_found() => Ok(None),
            Err(e) => Err(e),
        }
    }

    // TODO document me
    pub fn open_poly_database<'t>(
        &'t self,
        name: Option<&str>,
    ) -> Result<Option<PolyDatabase<'t>>> {
        match self.raw_init_database(name, Untyped, false) {
            Ok(dbi) => Ok(Some(PolyDatabase::new(self.env_mut_ptr() as _, dbi))),
            Err(Error::Mdb(e)) if e.not_found() => Ok(None),
            Err(e) => Err(e),
        }
    }

    // TODO document me
    pub fn create_database<'t, KC, DC>(&'t self, name: Option<&str>) -> Result<Database<'t, KC, DC>>
    where
        KC: 'static,
        DC: 'static,
    {
        let types = Typed { key_type: TypeId::of::<KC>(), data_type: TypeId::of::<DC>() };
        match self.raw_init_database(name, types, true) {
            Ok(dbi) => Ok(Database::new(self.env_mut_ptr() as _, dbi)),
            Err(e) => Err(e),
        }
    }

    // TODO document me
    pub fn create_poly_database<'t>(&'t self, name: Option<&str>) -> Result<PolyDatabase<'t>> {
        match self.raw_init_database(name, Untyped, true) {
            Ok(dbi) => Ok(PolyDatabase::new(self.env_mut_ptr() as _, dbi)),
            Err(e) => Err(e),
        }
    }

    fn raw_init_database(
        &self,
        name: Option<&str>,
        types: DatabaseType,
        create: bool,
    ) -> Result<u32> {
        let mut global = self.txn.env.0.dbi_open_mutex.lock().unwrap();
        let mut local = self.local_opened_dbi.lock().unwrap();

        let raw_txn = self.txn.txn;
        let flags = if create { ffi::MDB_CREATE } else { 0 };
        match raw_open_dbi(raw_txn, name, flags) {
            Ok(dbi) => {
                let name = name.map(ToOwned::to_owned);
                if let Some((_, Some(t))) = global.get(&name) {
                    if *t == types {
                        Ok(dbi)
                    } else {
                        Err(Error::InvalidDatabaseTyping)
                    }
                } else {
                    match local.entry(name) {
                        Entry::Occupied(mut entry) => {
                            let (dbi, t) = entry.get_mut();
                            if *t != types {
                                Err(Error::InvalidDatabaseTyping)
                            } else {
                                Ok(*dbi)
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert((dbi, types));
                            Ok(dbi)
                        }
                    }
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(self.txn.txn)) };
        self.txn.txn = ptr::null_mut();
        match result {
            Ok(()) => {
                for (name, (dbi, types)) in self.local_opened_dbi.into_inner().unwrap() {
                    // ...
                }

                // let mut lock = self.txn.env.0.dbi_open_mutex.lock().unwrap();

                // let raw_txn = self.txn.txn;
                // let flags = if create { ffi::MDB_CREATE } else { 0 };
                // match raw_open_dbi(raw_txn, name, flags) {
                //     Ok(dbi) => match lock.entry(name.map(ToOwned::to_owned)) {
                //         Entry::Occupied(mut entry) => {
                //             let (dbi, t) = entry.get_mut();
                //             if t.is_none() {
                //                 *t = Some(types);
                //                 Ok(*dbi)
                //             } else if *t != Some(types) {
                //                 Err(Error::InvalidDatabaseTyping)
                //             } else {
                //                 Ok(*dbi)
                //             }
                //         }
                //         Entry::Vacant(entry) => {
                //             entry.insert((dbi, Some(types)));
                //             Ok(dbi)
                //         }
                //     },
                //     Err(e) => Err(e.into()),
                // }

                todo!("store the opened databases into the env")
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn abort(mut self) {
        abort_txn(self.txn.txn);
        self.txn.txn = ptr::null_mut();
    }
}

impl<'p> Deref for RwTxn<'p> {
    type Target = RoTxn<'p>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

fn raw_open_dbi(
    raw_txn: *mut ffi::MDB_txn,
    name: Option<&str>,
    flags: u32,
) -> std::result::Result<u32, crate::mdb::lmdb_error::Error> {
    let mut dbi = 0;
    let name = name.map(|n| CString::new(n).unwrap());
    let name_ptr = match name {
        Some(ref name) => name.as_bytes_with_nul().as_ptr() as *const _,
        None => ptr::null(),
    };

    // safety: The name cstring is cloned by LMDB, we can drop it after.
    //         If a read-only is used with the MDB_CREATE flag, LMDB will throw an error.
    unsafe { mdb_result(ffi::mdb_dbi_open(raw_txn, name_ptr, flags, &mut dbi))? };

    Ok(dbi)
}
