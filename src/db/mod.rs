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

pub struct RoIter<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
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
}

pub struct RwIter<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> RwIter<'_, KC, DC> {
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
}

pub struct RoRange<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    start_bound: Option<Bound<Vec<u8>>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<'txn, KC, DC> Iterator for RoRange<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.start_bound.take() {
            Some(Bound::Included(start)) => {
                self.cursor.move_on_key_greater_than_or_equal_to(&start)
            }
            Some(Bound::Excluded(mut start)) => {
                advance_key(&mut start);
                self.cursor.move_on_key_greater_than_or_equal_to(&start)
            }
            Some(Bound::Unbounded) => self.cursor.move_on_first(),
            None => self.cursor.move_on_next(),
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
}

pub struct RwRange<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    start_bound: Option<Bound<Vec<u8>>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> RwRange<'_, KC, DC> {
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
}

impl<'txn, KC, DC> Iterator for RwRange<'txn, KC, DC>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.start_bound.take() {
            Some(Bound::Included(start)) => {
                self.cursor.move_on_key_greater_than_or_equal_to(&start)
            }
            Some(Bound::Excluded(mut start)) => {
                advance_key(&mut start);
                self.cursor.move_on_key_greater_than_or_equal_to(&start)
            }
            Some(Bound::Unbounded) => self.cursor.move_on_first(),
            None => self.cursor.move_on_next(),
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
}
