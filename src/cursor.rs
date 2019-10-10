use std::borrow::Cow;
use std::ops::Deref;
use std::{marker, mem};

use crate::lmdb_error::lmdb_result;
use crate::*;

pub struct RoCursor<'txn, KC, DC> {
    cursor: *mut ffi::MDB_cursor,
    _marker: marker::PhantomData<&'txn (KC, DC)>,
}

impl<'txn, KC, DC> RoCursor<'txn, KC, DC> {
    pub(crate) fn new(txn: &'txn RoTxn, db: Database<KC, DC>) -> RoCursor<'txn, KC, DC> {
        unimplemented!()
    }

    pub fn move_on_first(&mut self) -> Result<Option<(Cow<'txn, KC::DItem>, Cow<'txn, DC::DItem>)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
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
        let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

        let data = unsafe { crate::from_val(data_val.assume_init()) };
        let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

        Ok(Some((key, data)))
    }

    pub fn move_on_key(
        &mut self,
        key: &KC::EItem,
    ) -> Result<Option<(Cow<'txn, KC::DItem>, Cow<'txn, DC::DItem>)>>
    where
        KC: BytesEncode + BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;

        let mut key_val = unsafe { crate::into_val(&key_bytes) };
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
        let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

        let data = unsafe { crate::from_val(data_val.assume_init()) };
        let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

        Ok(Some((key, data)))
    }

    pub fn move_on_next(&mut self) -> Result<Option<(Cow<'txn, KC::DItem>, Cow<'txn, DC::DItem>)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
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
        let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

        let data = unsafe { crate::from_val(data_val.assume_init()) };
        let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

        Ok(Some((key, data)))
    }

    pub fn get_current(&mut self) -> Result<Option<(Cow<'txn, KC::DItem>, Cow<'txn, DC::DItem>)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
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
        let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

        let data = unsafe { crate::from_val(data_val.assume_init()) };
        let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

        Ok(Some((key, data)))
    }
}

impl<KC, DC> Drop for RoCursor<'_, KC, DC> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.cursor) }
    }
}

pub struct RwCursor<'txn, KC, DC> {
    cursor: RoCursor<'txn, KC, DC>,
}

impl<'txn, KC, DC> RwCursor<'txn, KC, DC> {
    pub(crate) fn new(txn: &'txn RwTxn, db: Database<KC, DC>) -> RoCursor<'txn, KC, DC> {
        unimplemented!()
    }

    pub fn put_current(&mut self, data: &DC::EItem) -> Result<()>
    where DC: BytesEncode
    {
        unimplemented!()
    }

    pub fn del_current(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl<'txn, KC, DC> Deref for RwCursor<'txn, KC, DC> {
    type Target = RoCursor<'txn, KC, DC>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}
