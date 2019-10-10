use std::borrow::Cow;
use std::ops::Deref;
use std::{marker, mem};

use crate::{Result, Error, Database, RoTxn, RwTxn, BytesEncode, BytesDecode};

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
        let ret = unsafe {
            ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::MDB_FIRST,
            )
        };

        match ret {
            0 => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

                let data = unsafe { crate::from_val(data_val.assume_init()) };
                let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

                Ok(Some((key, data)))
            },
            ffi::MDB_NOTFOUND => Ok(None),
            _ => panic!("Found an error {}", ret),
        }
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
        let ret = unsafe {
            ffi::mdb_cursor_get(
                self.cursor,
                &mut key_val,
                data_val.as_mut_ptr(),
                ffi::MDB_SET_KEY,
            )
        };

        match ret {
            0 => {
                let key = unsafe { crate::from_val(key_val) };
                let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

                let data = unsafe { crate::from_val(data_val.assume_init()) };
                let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

                Ok(Some((key, data)))
            },
            ffi::MDB_NOTFOUND => Ok(None),
            _ => panic!("Found an error {}", ret),
        }
    }

    pub fn move_on_next(&mut self) -> Result<Option<(Cow<'txn, KC::DItem>, Cow<'txn, DC::DItem>)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor to the next non-dup key
        let ret = unsafe {
            ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::MDB_NEXT,
            )
        };

        match ret {
            0 => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

                let data = unsafe { crate::from_val(data_val.assume_init()) };
                let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

                Ok(Some((key, data)))
            },
            ffi::MDB_NOTFOUND => Ok(None),
            _ => panic!("Found an error {}", ret),
        }
    }

    pub fn get_current(&mut self) -> Result<Option<(Cow<'txn, KC::DItem>, Cow<'txn, DC::DItem>)>>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Retrieve the key/data at the current cursor position
        let ret = unsafe {
            ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::MDB_GET_CURRENT,
            )
        };

        match ret {
            0 => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let key = KC::bytes_decode(key).ok_or(Error::Decoding)?;

                let data = unsafe { crate::from_val(data_val.assume_init()) };
                let data = DC::bytes_decode(data).ok_or(Error::Decoding)?;

                Ok(Some((key, data)))
            },
            ffi::MDB_NOTFOUND => Ok(None),
            _ => panic!("Found an error {}", ret),
        }
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
