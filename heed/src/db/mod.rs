use crate::*;
use std::borrow::Cow;
use std::marker;
use std::ops::Bound;

mod polymorph;
mod uniform;

pub use self::polymorph::PolyDatabase;
pub use self::uniform::Database;

pub fn advance_key(bytes: &mut Vec<u8>) {
    match bytes.last_mut() {
        Some(&mut 255) | None => bytes.push(0),
        Some(last) => *last += 1,
    }
}

fn retreat_key(bytes: &mut Vec<u8>) {
    match bytes.last_mut() {
        Some(&mut 0) => { bytes.pop(); },
        Some(last) => *last -= 1,
        None => panic!("Vec is empty and must not be"),
    }
}

pub struct RoIter<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoIter<'txn, KC, DC> {
    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoIter<'txn, KC2, DC2> {
        RoIter {
            cursor: self.cursor,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoIter<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoIter<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoIter<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RoIter<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_first()
        } else {
            self.cursor.move_on_next()
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.cursor.move_on_last()
        } else {
            match (self.cursor.current(), self.cursor.move_on_last()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RwIter<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwIter<'txn, KC, DC> {
    /// Delete the entry the cursor is currently pointing to.
    ///
    /// Returns `true` if the entry was successfully deleted.
    pub fn del_current(&mut self) -> Result<bool> {
        self.cursor.del_current()
    }

    /// Write a new value to the current entry.
    /// The given key must be equal to the one this cursor is pointing at.
    ///
    /// Returns `true` if the entry was successfully written.
    ///
    /// This is intended to be used when the new data is the same size as the old.
    /// Otherwise it will simply perform a delete of the old record followed by an insert.
    pub fn put_current<'a>(&mut self, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;
        self.cursor.put_current(&key_bytes, &data_bytes)
    }

    /// Append the given key/value pair to the end of the database.
    ///
    /// If a key is inserted that is less than any previous key a `KeyExist` error
    /// is returned and the key is not inserted into the database.
    pub fn append<'a>(&mut self, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;
        self.cursor.append(&key_bytes, &data_bytes)
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RwIter<'txn, KC2, DC2> {
        RwIter {
            cursor: self.cursor,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RwIter<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RwIter<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RwIter<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RwIter<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_first()
        } else {
            self.cursor.move_on_next()
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.cursor.move_on_last()
        } else {
            match (self.cursor.current(), self.cursor.move_on_last()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RoRevIter<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    move_on_last: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRevIter<'txn, KC, DC> {
    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoRevIter<'txn, KC2, DC2> {
        RoRevIter {
            cursor: self.cursor,
            move_on_last: self.move_on_last,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoRevIter<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoRevIter<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoRevIter<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RoRevIter<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            self.cursor.move_on_last()
        } else {
            self.cursor.move_on_prev()
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor.move_on_first()
        } else {
            match (self.cursor.current(), self.cursor.move_on_first()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RwRevIter<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    move_on_last: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRevIter<'txn, KC, DC> {
    /// Delete the entry the cursor is currently pointing to.
    ///
    /// Returns `true` if the entry was successfully deleted.
    pub fn del_current(&mut self) -> Result<bool> {
        self.cursor.del_current()
    }

    /// Write a new value to the current entry.
    /// The given key must be equal to the one this cursor is pointing at.
    ///
    /// Returns `true` if the entry was successfully written.
    ///
    /// This is intended to be used when the new data is the same size as the old.
    /// Otherwise it will simply perform a delete of the old record followed by an insert.
    pub fn put_current<'a>(&mut self, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;
        self.cursor.put_current(&key_bytes, &data_bytes)
    }

    /// Append the given key/value pair to the end of the database.
    ///
    /// If a key is inserted that is less than any previous key a `KeyExist` error
    /// is returned and the key is not inserted into the database.
    pub fn append<'a>(&mut self, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<()>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;
        self.cursor.append(&key_bytes, &data_bytes)
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RwRevIter<'txn, KC2, DC2> {
        RwRevIter {
            cursor: self.cursor,
            move_on_last: self.move_on_last,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RwRevIter<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RwRevIter<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RwRevIter<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RwRevIter<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            self.cursor.move_on_last()
        } else {
            self.cursor.move_on_prev()
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor.move_on_first()
        } else {
            match (self.cursor.current(), self.cursor.move_on_first()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Some(key), Some(data)) => Some(Ok((key, data))),
                (_, _) => Some(Err(Error::Decoding)),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RoRange<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    move_on_start: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRange<'txn, KC, DC> {
    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoRange<'txn, KC2, DC2> {
        RoRange {
            cursor: self.cursor,
            move_on_start: self.move_on_start,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoRange<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoRange<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoRange<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

fn move_on_range_end<'txn>(
    cursor: &mut RoCursor<'txn>,
    end_bound: &Bound<Vec<u8>>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>>
{
    match end_bound {
        Bound::Included(end) => {
            match cursor.move_on_key_greater_than_or_equal_to(end) {
                Ok(Some((key, data))) if key == &end[..] => Ok(Some((key, data))),
                Ok(_) => cursor.move_on_prev(),
                Err(e) => Err(e),
            }
        },
        Bound::Excluded(end) => {
            cursor
                .move_on_key_greater_than_or_equal_to(end)
                .and_then(|_| cursor.move_on_prev())
        },
        Bound::Unbounded => cursor.move_on_last(),
    }
}

impl<'txn, KC, DC> Iterator for RoRange<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_start {
            self.move_on_start = false;
            match &mut self.start_bound {
                Bound::Included(start) => {
                    self.cursor.move_on_key_greater_than_or_equal_to(start)
                },
                Bound::Excluded(start) => {
                    advance_key(start);
                    let result = self.cursor.move_on_key_greater_than_or_equal_to(start);
                    retreat_key(start);
                    result
                },
                Bound::Unbounded => self.cursor.move_on_first(),
            }
        } else {
            self.cursor.move_on_next()
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.end_bound {
                    Bound::Included(end) => key <= end,
                    Bound::Excluded(end) => key < end,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
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
        let result = if self.move_on_start {
            move_on_range_end(&mut self.cursor, &self.end_bound)
        } else {
            match (self.cursor.current(), move_on_range_end(&mut self.cursor, &self.end_bound)) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.start_bound {
                    Bound::Included(start) => key >= start,
                    Bound::Excluded(start) => key > start,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
                    }
                } else {
                    None
                }
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RwRange<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    move_on_start: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRange<'txn, KC, DC> {
    pub fn del_current(&mut self) -> Result<bool> {
        self.cursor.del_current()
    }

    pub fn put_current<'a>(&mut self, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;
        self.cursor.put_current(&key_bytes, &data_bytes)
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RwRange<'txn, KC2, DC2> {
        RwRange {
            cursor: self.cursor,
            move_on_start: self.move_on_start,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RwRange<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RwRange<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RwRange<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RwRange<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_start {
            self.move_on_start = false;
            match &mut self.start_bound {
                Bound::Included(start) => {
                    self.cursor.move_on_key_greater_than_or_equal_to(start)
                },
                Bound::Excluded(start) => {
                    advance_key(start);
                    let result = self.cursor.move_on_key_greater_than_or_equal_to(start);
                    retreat_key(start);
                    result
                },
                Bound::Unbounded => self.cursor.move_on_first(),
            }
        } else {
            self.cursor.move_on_next()
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match self.end_bound {
                    Bound::Included(ref end) => key <= end,
                    Bound::Excluded(ref end) => key < end,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
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
        let result = if self.move_on_start {
            move_on_range_end(&mut self.cursor, &self.end_bound)
        } else {
            match (self.cursor.current(), move_on_range_end(&mut self.cursor, &self.end_bound)) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.start_bound {
                    Bound::Included(start) => key >= start,
                    Bound::Excluded(start) => key > start,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
                    }
                } else {
                    None
                }
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RoRevRange<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    move_on_end: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoRevRange<'txn, KC, DC> {
    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoRevRange<'txn, KC2, DC2> {
        RoRevRange {
            cursor: self.cursor,
            move_on_end: self.move_on_end,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoRevRange<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoRevRange<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoRevRange<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RoRevRange<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            self.move_on_end = false;
            match &mut self.end_bound {
                Bound::Included(end) => {
                    match self.cursor.move_on_key_greater_than_or_equal_to(end) {
                        Ok(Some((key, _))) if key > end => self.cursor.move_on_prev(),
                        Ok(opt) => Ok(opt),
                        Err(e) => Err(e),
                    }
                },
                Bound::Excluded(end) => {
                    self.cursor
                        .move_on_key_greater_than_or_equal_to(end)
                        .and_then(|_| self.cursor.move_on_prev())
                },
                Bound::Unbounded => self.cursor.move_on_last(),
            }
        } else {
            self.cursor.move_on_prev()
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.start_bound {
                    Bound::Included(start) => key >= start,
                    Bound::Excluded(start) => key > start,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
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

pub struct RwRevRange<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    move_on_end: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwRevRange<'txn, KC, DC> {
    pub fn del_current(&mut self) -> Result<bool> {
        self.cursor.del_current()
    }

    pub fn put_current<'a>(&mut self, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;
        self.cursor.put_current(&key_bytes, &data_bytes)
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RwRevRange<'txn, KC2, DC2> {
        RwRevRange {
            cursor: self.cursor,
            move_on_end: self.move_on_end,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData::default(),
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RwRevRange<'txn, KC2, DC> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RwRevRange<'txn, KC, DC2> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RwRevRange<'txn, KC, LazyDecode<DC>> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC> Iterator for RwRevRange<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            self.move_on_end = false;
            match &mut self.end_bound {
                Bound::Included(end) => {
                    match self.cursor.move_on_key_greater_than_or_equal_to(end) {
                        Ok(Some((key, _))) if key > end => self.cursor.move_on_prev(),
                        Ok(opt) => Ok(opt),
                        Err(e) => Err(e),
                    }
                },
                Bound::Excluded(end) => {
                    self.cursor
                        .move_on_key_greater_than_or_equal_to(end)
                        .and_then(|_| self.cursor.move_on_prev())
                },
                Bound::Unbounded => self.cursor.move_on_last(),
            }
        } else {
            self.cursor.move_on_prev()
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.start_bound {
                    Bound::Included(start) => key >= start,
                    Bound::Excluded(start) => key > start,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
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

fn move_on_prefix_end<'txn>(
    cursor: &mut RoCursor<'txn>,
    prefix: &mut Vec<u8>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>>
{
    advance_key(prefix);
    let result = cursor
        .move_on_key_greater_than_or_equal_to(prefix)
        .and_then(|_| cursor.move_on_prev());
    retreat_key(prefix);
    result
}

pub struct RoPrefix<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    prefix: Vec<u8>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RoPrefix<'txn, KC, DC> {
    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoPrefix<'txn, KC2, DC2> {
        RoPrefix {
            cursor: self.cursor,
            prefix: self.prefix,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData::default(),
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
            self.cursor.move_on_next()
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
                    }
                } else {
                    None
                }
            },
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
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
                    }
                } else {
                    None
                }
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RwPrefix<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    prefix: Vec<u8>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> RwPrefix<'txn, KC, DC> {
    pub fn del_current(&mut self) -> Result<bool> {
        self.cursor.del_current()
    }

    pub fn put_current<'a>(&mut self, key: &'a KC::EItem, data: &'a DC::EItem) -> Result<bool>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let key_bytes: Cow<[u8]> = KC::bytes_encode(&key).ok_or(Error::Encoding)?;
        let data_bytes: Cow<[u8]> = DC::bytes_encode(&data).ok_or(Error::Encoding)?;
        self.cursor.put_current(&key_bytes, &data_bytes)
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RwPrefix<'txn, KC2, DC2> {
        RwPrefix {
            cursor: self.cursor,
            prefix: self.prefix,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData::default(),
        }
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
            self.cursor.move_on_next()
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
                    }
                } else {
                    None
                }
            },
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
                },
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                if key.starts_with(&self.prefix) {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Some(key), Some(data)) => Some(Ok((key, data))),
                        (_, _) => Some(Err(Error::Decoding)),
                    }
                } else {
                    None
                }
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn prefix_iter_with_byte_255() {
        use std::fs;
        use std::path::Path;
        use crate::EnvOpenOptions;
        use crate::types::*;

        fs::create_dir_all(Path::new("target").join("prefix_iter_with_byte_255.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("prefix_iter_with_byte_255.mdb")).unwrap();
        let db = env.create_database::<ByteSlice, Str>(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], "hello").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 1,   0, 119, 111, 114, 108, 100], "world").unwrap();

        // Lets check that we can prefix_iter on that sequence with the key "255".
        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0, 255]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0u8, 0, 0, 255, 104, 101, 108, 108, 111][..], "hello")));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[  0, 0, 0, 255, 119, 111, 114, 108, 100][..], "world")));
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.abort().unwrap();
    }

    #[test]
    fn iter_last() {
        use std::fs;
        use std::path::Path;
        use crate::EnvOpenOptions;
        use crate::types::*;
        use crate::{zerocopy::I32, byteorder::BigEndian};

        fs::create_dir_all(Path::new("target").join("iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("iter_last.mdb")).unwrap();
        let db = env.create_database::<OwnedType<BEI32>, Unit>(None).unwrap();
        type BEI32 = I32<BigEndian>;

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &BEI32::new(1), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(2), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(3), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(4), &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(4), ())));

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(4), ())));

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(4), ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(4), ())));
        assert_eq!(iter.next().transpose().unwrap(), None);
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &BEI32::new(1), &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(1), ())));

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }

    #[test]
    fn range_iter_last() {
        use std::fs;
        use std::path::Path;
        use crate::EnvOpenOptions;
        use crate::{zerocopy::I32, byteorder::BigEndian};
        use crate::types::*;

        fs::create_dir_all(Path::new("target").join("iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("iter_last.mdb")).unwrap();
        let db = env.create_database::<OwnedType<BEI32>, Unit>(None).unwrap();
        type BEI32 = I32<BigEndian>;

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &BEI32::new(1), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(2), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(3), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(4), &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(4), ())));

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(4), ())));

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(4), ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(4), ())));
        assert_eq!(iter.next().transpose().unwrap(), None);
        assert_eq!(iter.last().transpose().unwrap(), None);

        let range = BEI32::new(2)..=BEI32::new(4);
        let mut iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(4), ())));

        let range = BEI32::new(2)..BEI32::new(4);
        let mut iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(3), ())));

        let range = BEI32::new(2)..BEI32::new(4);
        let mut iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let range = BEI32::new(2)..BEI32::new(2);
        let iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), None);

        let range = BEI32::new(2)..=BEI32::new(1);
        let iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &BEI32::new(1), &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(1), ())));

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }

    #[test]
    fn prefix_iter_last() {
        use std::fs;
        use std::path::Path;
        use crate::EnvOpenOptions;
        use crate::types::*;

        fs::create_dir_all(Path::new("target").join("prefix_iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("prefix_iter_last.mdb")).unwrap();
        let db = env.create_database::<ByteSlice, Unit>(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 1,   0, 119, 111, 114, 108, 100], &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ())));

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ())));

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 1,   0, 119, 111, 114, 108, 100][..], ())));

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 1,   0, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }
}
