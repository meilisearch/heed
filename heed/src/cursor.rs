use std::ops::{Deref, DerefMut};
use std::{marker, mem, ptr};

use crate::*;
use crate::mdb::error::mdb_result;
use crate::mdb::ffi;

pub struct RoCursor<'txn> {
    cursor: *mut ffi::MDB_cursor,
    _marker: marker::PhantomData<&'txn ()>,
}

impl<'txn> RoCursor<'txn> {
    pub(crate) fn new<T>(txn: &'txn RoTxn<T>, dbi: ffi::MDB_dbi) -> Result<RoCursor<'txn>> {
        let mut cursor: *mut ffi::MDB_cursor = ptr::null_mut();

        unsafe { mdb_result(ffi::mdb_cursor_open(txn.txn, dbi, &mut cursor))? }

        Ok(RoCursor {
            cursor,
            _marker: marker::PhantomData,
        })
    }

    pub fn move_on_first(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor on the first database key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_FIRST,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_first_dup_of(&mut self, key: &[u8]) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let key_mdb_val =  unsafe { crate::into_val(&key) };
        let mut key_val = mem::MaybeUninit::new(key_mdb_val);
        let mut data_val = mem::MaybeUninit::zeroed();

        // Move the cursor on the first database key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_SET,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_last(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor on the first database key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_LAST,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_key_greater_than_or_equal_to(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = unsafe { crate::into_val(&key) };
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor to the specified key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                &mut key_val,
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_SET_RANGE,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_next(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor to the next non-dup key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_NEXT,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_next_dup_of(&mut self, key: &[u8]) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let key_mdb_val =  unsafe { crate::into_val(&key) };
        let mut key_val = mem::MaybeUninit::new(key_mdb_val);
        let mut data_val = mem::MaybeUninit::zeroed();

        // Move the cursor to the next non-dup key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_NEXT_DUP,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
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
    pub(crate) fn new<T>(txn: &'txn RwTxn<T>, dbi: ffi::MDB_dbi) -> Result<RwCursor<'txn>> {
        Ok(RwCursor {
            cursor: RoCursor::new(txn, dbi)?,
        })
    }

    pub fn del_current(&mut self) -> Result<bool> {
        // Delete the current entry
        let result = unsafe { mdb_result(ffi::mdb_cursor_del(self.cursor.cursor, 0)) };

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn put_current(&mut self, key: &[u8], data: &[u8]) -> Result<bool> {
        let mut key_val = unsafe { crate::into_val(&key) };
        let mut data_val = unsafe { crate::into_val(&data) };

        // Modify the pointed data
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_put(
                self.cursor.cursor,
                &mut key_val,
                &mut data_val,
                ffi::MDB_CURRENT,
            ))
        };

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn append(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        let mut key_val = unsafe { crate::into_val(&key) };
        let mut data_val = unsafe { crate::into_val(&data) };

        // Modify the pointed data
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_put(
                self.cursor.cursor,
                &mut key_val,
                &mut data_val,
                ffi::MDB_APPEND,
            ))
        };

        result.map_err(Into::into)
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
