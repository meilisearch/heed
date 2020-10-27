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

pub struct RoPrefix<'txn, KC, DC> {
    cursor: RoCursor<'txn>,
    prefix: Vec<u8>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
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
}

pub struct RwPrefix<'txn, KC, DC> {
    cursor: RwCursor<'txn>,
    prefix: Vec<u8>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> RwPrefix<'_, KC, DC> {
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
            .map_size(10 * 1024 * 1024 * 1024) // 10GB
            .max_dbs(3000)
            .open(Path::new("target").join("prefix_iter_with_byte_255.mdb")).unwrap();
        let db = env.create_database::<ByteSlice, Str>(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], "hello").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], "world").unwrap();

        // Lets check that we can prefix_iter on that sequence with the key "255".
        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0, 255]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0u8, 0, 0, 255, 104, 101, 108, 108, 111][..], "hello")));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], "world")));
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.abort().unwrap();
    }
}
