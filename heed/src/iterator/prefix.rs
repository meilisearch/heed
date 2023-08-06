use std::borrow::Cow;
use std::marker;

use types::LazyDecode;

use super::{advance_key, retreat_key};
use crate::cursor::MoveOperation;
use crate::*;

fn move_on_prefix_end<'txn>(
    cursor: &mut RoCursor<'txn>,
    prefix: &mut Vec<u8>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
    advance_key(prefix);
    let result = cursor
        .move_on_key_greater_than_or_equal_to(prefix)
        .and_then(|_| cursor.move_on_prev(MoveOperation::NoDup));
    retreat_key(prefix);
    result
}

/// A read-only prefix iterator structure.
pub struct RoPrefix<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    prefix: Vec<u8>,
    move_operation: MoveOperation,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoPrefix<'txn, KC, DC> {
    pub(crate) fn new(
        cursor: RoCursor<'txn>,
        prefix: Vec<u8>,
        move_operation: MoveOperation,
    ) -> RoPrefix<'txn, KC, DC> {
        RoPrefix {
            cursor,
            prefix,
            move_operation,
            move_on_first: true,
            _phantom: marker::PhantomData,
        }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// For more info, see [`RoIter::move_between_keys`].
    pub fn move_between_keys(&mut self) {
        self.move_operation = MoveOperation::NoDup;
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// For more info, see [`RoIter::move_through_duplicate_values`].
    pub fn move_through_duplicate_values(&mut self) {
        self.move_operation = MoveOperation::Any;
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoPrefix<'txn, KC2, DC2> {
        RoPrefix {
            cursor: self.cursor,
            prefix: self.prefix,
            move_operation: self.move_operation,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoPrefix<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoPrefix<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoPrefix<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RoPrefix<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            self.cursor.move_on_next(self.move_operation)
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            move_on_prefix_end(&mut self.cursor, &mut self.prefix)
        } else {
            match (self.cursor.current(), move_on_prefix_end(&mut self.cursor, &mut self.prefix)) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// A read-write prefix iterator structure.
pub struct RwPrefix<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    prefix: Vec<u8>,
    move_operation: MoveOperation,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwPrefix<'txn, KC, DC> {
    pub(crate) fn new(
        cursor: RwCursor<'txn>,
        prefix: Vec<u8>,
        move_operation: MoveOperation,
    ) -> RwPrefix<'txn, KC, DC> {
        RwPrefix {
            cursor,
            prefix,
            move_operation,
            move_on_first: true,
            _phantom: marker::PhantomData,
        }
    }

    /// Delete the entry the cursor is currently pointing to.
    ///
    /// Returns `true` if the entry was successfully deleted.
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database
    /// while modifying it.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn del_current(&mut self) -> Result<bool> {
        self.cursor.del_current()
    }

    /// Write a new value to the current entry.
    ///
    /// The given key **must** be equal to the one this cursor is pointing otherwise the database
    /// can be put into an inconsistent state.
    ///
    /// Returns `true` if the entry was successfully written.
    ///
    /// > This is intended to be used when the new data is the same size as the old.
    /// > Otherwise it will simply perform a delete of the old record followed by an insert.
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database while
    /// modifying it, so you can't use the key/value that comes from the cursor to feed
    /// this function.
    ///
    /// In other words: Transform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn put_current<'a>(
        &mut self,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(data).map_err(Error::Encoding)?;
        self.cursor.put_current(&key_bytes, &data_bytes)
    }

    /// Write a new value to the current entry.
    ///
    /// The given key **must** be equal to the one this cursor is pointing otherwise the database
    /// can be put into an inconsistent state.
    ///
    /// Returns `true` if the entry was successfully written.
    ///
    /// > This is intended to be used when the new data is the same size as the old.
    /// > Otherwise it will simply perform a delete of the old record followed by an insert.
    ///
    /// # Safety
    ///
    /// Please read the safety notes of the [`RwPrefix::put_current`] method.
    pub unsafe fn put_current_reserved<'a, F>(
        &mut self,
        key: &'a KC::EItem,
        data_size: usize,
        write_func: F,
    ) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        F: FnMut(&mut ReservedSpace) -> io::Result<()>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        self.cursor.put_current_reserved(&key_bytes, data_size, write_func)
    }

    /// Insert a key-value pair in this database. The entry is written with the specified flags.
    ///
    /// For more info, see [`RoIter::put_current_with_flags`].
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database while
    /// modifying it, so you can't use the key/value that comes from the cursor to feed
    /// this function.
    ///
    /// In other words: Transform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn put_current_with_flags<'a>(
        &mut self,
        flags: PutFlags,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(data).map_err(Error::Encoding)?;
        self.cursor.put_current_with_flags(flags, &key_bytes, &data_bytes)
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RwPrefix<'txn, KC2, DC2> {
        RwPrefix {
            cursor: self.cursor,
            prefix: self.prefix,
            move_operation: self.move_operation,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData,
        }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// For more info, see [`RoIter::move_between_keys`].
    pub fn move_between_keys(&mut self) {
        self.move_operation = MoveOperation::NoDup;
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// For more info, see [`RoIter::move_through_duplicate_values`].
    pub fn move_through_duplicate_values(&mut self) {
        self.move_operation = MoveOperation::Any;
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RwPrefix<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RwPrefix<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RwPrefix<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RwPrefix<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            self.cursor.move_on_next(self.move_operation)
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            move_on_prefix_end(&mut self.cursor, &mut self.prefix)
        } else {
            match (self.cursor.current(), move_on_prefix_end(&mut self.cursor, &mut self.prefix)) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// A reverse read-only prefix iterator structure.
pub struct RoRevPrefix<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    prefix: Vec<u8>,
    move_operation: MoveOperation,
    move_on_last: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRevPrefix<'txn, KC, DC> {
    pub(crate) fn new(
        cursor: RoCursor<'txn>,
        prefix: Vec<u8>,
        move_operation: MoveOperation,
    ) -> RoRevPrefix<'txn, KC, DC> {
        RoRevPrefix {
            cursor,
            prefix,
            move_operation,
            move_on_last: true,
            _phantom: marker::PhantomData,
        }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// For more info, see [`RoIter::move_between_keys`].
    pub fn move_between_keys(&mut self) {
        self.move_operation = MoveOperation::NoDup;
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// For more info, see [`RoIter::move_through_duplicate_values`].
    pub fn move_through_duplicate_values(&mut self) {
        self.move_operation = MoveOperation::Any;
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoRevPrefix<'txn, KC2, DC2> {
        RoRevPrefix {
            cursor: self.cursor,
            prefix: self.prefix,
            move_operation: self.move_operation,
            move_on_last: self.move_on_last,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoRevPrefix<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoRevPrefix<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoRevPrefix<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RoRevPrefix<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            move_on_prefix_end(&mut self.cursor, &mut self.prefix)
        } else {
            self.cursor.move_on_prev(self.move_operation)
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            let current = self.cursor.current();
            let start = self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix);
            match (current, start) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// A reverse read-write prefix iterator structure.
pub struct RwRevPrefix<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    prefix: Vec<u8>,
    move_operation: MoveOperation,
    move_on_last: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRevPrefix<'txn, KC, DC> {
    pub(crate) fn new(
        cursor: RwCursor<'txn>,
        prefix: Vec<u8>,
        move_operation: MoveOperation,
    ) -> RwRevPrefix<'txn, KC, DC> {
        RwRevPrefix {
            cursor,
            prefix,
            move_operation,
            move_on_last: true,
            _phantom: marker::PhantomData,
        }
    }

    /// Delete the entry the cursor is currently pointing to.
    ///
    /// Returns `true` if the entry was successfully deleted.
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database
    /// while modifying it.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn del_current(&mut self) -> Result<bool> {
        self.cursor.del_current()
    }

    /// Write a new value to the current entry.
    ///
    /// The given key **must** be equal to the one this cursor is pointing otherwise the database
    /// can be put into an inconsistent state.
    ///
    /// Returns `true` if the entry was successfully written.
    ///
    /// > This is intended to be used when the new data is the same size as the old.
    /// > Otherwise it will simply perform a delete of the old record followed by an insert.
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database while
    /// modifying it, so you can't use the key/value that comes from the cursor to feed
    /// this function.
    ///
    /// In other words: Transform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn put_current<'a>(
        &mut self,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(data).map_err(Error::Encoding)?;
        self.cursor.put_current(&key_bytes, &data_bytes)
    }

    /// Write a new value to the current entry.
    ///
    /// The given key **must** be equal to the one this cursor is pointing otherwise the database
    /// can be put into an inconsistent state.
    ///
    /// Returns `true` if the entry was successfully written.
    ///
    /// > This is intended to be used when the new data is the same size as the old.
    /// > Otherwise it will simply perform a delete of the old record followed by an insert.
    ///
    /// # Safety
    ///
    /// Please read the safety notes of the [`RwRevPrefix::put_current`] method.
    pub unsafe fn put_current_reserved<'a, F>(
        &mut self,
        key: &'a KC::EItem,
        data_size: usize,
        write_func: F,
    ) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        F: FnMut(&mut ReservedSpace) -> io::Result<()>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        self.cursor.put_current_reserved(&key_bytes, data_size, write_func)
    }

    /// Insert a key-value pair in this database. The entry is written with the specified flags.
    ///
    /// For more info, see [`RoIter::put_current_with_flags`].
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database while
    /// modifying it, so you can't use the key/value that comes from the cursor to feed
    /// this function.
    ///
    /// In other words: Transform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn put_current_with_flags<'a>(
        &mut self,
        flags: PutFlags,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(key).map_err(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(data).map_err(Error::Encoding)?;
        self.cursor.put_current_with_flags(flags, &key_bytes, &data_bytes)
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RwRevPrefix<'txn, KC2, DC2> {
        RwRevPrefix {
            cursor: self.cursor,
            prefix: self.prefix,
            move_operation: self.move_operation,
            move_on_last: self.move_on_last,
            _phantom: marker::PhantomData,
        }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// For more info, see [`RoIter::move_between_keys`].
    pub fn move_between_keys(&mut self) {
        self.move_operation = MoveOperation::NoDup;
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// For more info, see [`RoIter::move_through_duplicate_values`].
    pub fn move_through_duplicate_values(&mut self) {
        self.move_operation = MoveOperation::Any;
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RwRevPrefix<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RwRevPrefix<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RwRevPrefix<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RwRevPrefix<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            move_on_prefix_end(&mut self.cursor, &mut self.prefix)
        } else {
            self.cursor.move_on_prev(self.move_operation)
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            let current = self.cursor.current();
            let start = self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix);
            match (current, start) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
