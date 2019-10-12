use std::ops::{Deref, DerefMut};
use std::{marker, mem, ptr};

use crate::lmdb_error::lmdb_result;
use crate::*;

pub struct RoCursor<'txn> {
    cursor: *mut ffi::MDB_cursor,
    _marker: marker::PhantomData<&'txn ()>,
}

impl<'txn> RoCursor<'txn> {
    pub(crate) fn new<KC, DC>(txn: &'txn RoTxn, db: Database<KC, DC>) -> Result<RoCursor<'txn>> {
        let mut cursor: *mut ffi::MDB_cursor = ptr::null_mut();

        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_open(
                txn.txn,
                db.dbi,
                &mut cursor,
            ))
        };

        Ok(RoCursor { cursor, _marker: marker::PhantomData })
    }

    pub fn move_on_first(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor on the first database key
        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::MDB_FIRST,
            ))
        };

        if let Err(error) = result {
            if error.not_found() {
                return Ok(None)
            } else {
                return Err(Error::Lmdb(error))
            }
        }

        let key = unsafe { crate::from_val(key_val.assume_init()) };
        let data = unsafe { crate::from_val(data_val.assume_init()) };

        Ok(Some((key, data)))
    }

    pub fn move_on_key(&mut self, key: &[u8]) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = unsafe { crate::into_val(&key) };
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor to the specified key
        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_get(
                self.cursor,
                &mut key_val,
                data_val.as_mut_ptr(),
                ffi::MDB_SET_KEY,
            ))
        };

        if let Err(error) = result {
            if error.not_found() {
                return Ok(None)
            } else {
                return Err(Error::Lmdb(error))
            }
        }

        let key = unsafe { crate::from_val(key_val) };
        let data = unsafe { crate::from_val(data_val.assume_init()) };

        Ok(Some((key, data)))
    }

    pub fn move_on_key_greater_than_or_equal_to(&mut self, key: &[u8]) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = unsafe { crate::into_val(&key) };
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor to the specified key
        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_get(
                self.cursor,
                &mut key_val,
                data_val.as_mut_ptr(),
                ffi::MDB_SET_RANGE,
            ))
        };

        if let Err(error) = result {
            if error.not_found() {
                return Ok(None)
            } else {
                return Err(Error::Lmdb(error))
            }
        }

        let key = unsafe { crate::from_val(key_val) };
        let data = unsafe { crate::from_val(data_val.assume_init()) };

        Ok(Some((key, data)))
    }

    pub fn move_on_next(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor to the next non-dup key
        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::MDB_NEXT,
            ))
        };

        if let Err(error) = result {
            if error.not_found() {
                return Ok(None)
            } else {
                return Err(Error::Lmdb(error))
            }
        }

        let key = unsafe { crate::from_val(key_val.assume_init()) };
        let data = unsafe { crate::from_val(data_val.assume_init()) };

        Ok(Some((key, data)))
    }

    pub fn get_current(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Retrieve the key/data at the current cursor position
        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::MDB_GET_CURRENT,
            ))
        };

        if let Err(error) = result {
            if error.not_found() {
                return Ok(None)
            } else {
                return Err(Error::Lmdb(error))
            }
        }

        let key = unsafe { crate::from_val(key_val.assume_init()) };
        let data = unsafe { crate::from_val(data_val.assume_init()) };

        Ok(Some((key, data)))
    }
}

impl Drop for RoCursor<'_> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.cursor) }
    }
}

pub struct RwCursor<'txn> {
    cursor: RoCursor<'txn>,
}

impl<'txn> RwCursor<'txn> {
    pub(crate) fn new<KC, DC>(txn: &'txn RwTxn, db: Database<KC, DC>) -> Result<RwCursor<'txn>> {
        Ok(RwCursor { cursor: RoCursor::new(txn, db)? })
    }

    pub fn put_current(&mut self, data: &[u8]) -> Result<bool> {
        let key = match self.get_current() {
            Ok(Some((key, _))) => key,
            Ok(None) => return Ok(false), // TODO must return an error
            Err(error) => return Err(error),
        };

        let mut key_val = unsafe { crate::into_val(&key) };
        let mut data_val = unsafe { crate::into_val(&data) };

        // Modify the pointed data
        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_put(
                self.cursor.cursor,
                &mut key_val,
                &mut data_val,
                ffi::MDB_CURRENT,
            ))
        };

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    pub fn del_current(&mut self) -> Result<bool> {
        // Delete the current entry
        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_del(self.cursor.cursor, 0))
        };

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(Error::Lmdb(e)),
        }
    }
}

impl<'txn> Deref for RwCursor<'txn> {
    type Target = RoCursor<'txn>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl DerefMut for RwCursor<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}
