use std::ops::Bound;

use crate::*;

fn move_on_range_end<'txn>(
    cursor: &mut RoCursor<'txn>,
    end_bound: &Bound<Vec<u8>>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
    match end_bound {
        Bound::Included(end) => match cursor.move_on_key_greater_than_or_equal_to(end) {
            Ok(Some((key, data))) if key == &end[..] => Ok(Some((key, data))),
            Ok(_) => cursor.move_on_prev(),
            Err(e) => Err(e),
        },
        Bound::Excluded(end) => cursor
            .move_on_key_greater_than_or_equal_to(end)
            .and_then(|_| cursor.move_on_prev()),
        Bound::Unbounded => cursor.move_on_last(),
    }
}

fn move_on_range_start<'txn>(
    cursor: &mut RoCursor<'txn>,
    start_bound: &Bound<Vec<u8>>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
    match start_bound {
        Bound::Included(start) => cursor.move_on_key_greater_than_or_equal_to(start),
        Bound::Excluded(start) => match cursor.move_on_key_greater_than_or_equal_to(start)? {
            Some((key, _)) if key == start => cursor.move_on_next(),
            Some((key, val)) => Ok(Some((key, val))),
            None => Ok(None),
        },
        Bound::Unbounded => cursor.move_on_first(),
    }
}

pub struct RoRange<'txn> {
    cursor: RoCursor<'txn>,
    move_on_start: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
}

impl<'txn> RoRange<'txn> {
    pub(crate) fn new(
        cursor: RoCursor<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> RoRange<'txn> {
        RoRange {
            cursor,
            move_on_start: true,
            start_bound,
            end_bound,
        }
    }
}

impl<'txn> Iterator for RoRange<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_start {
            self.move_on_start = false;
            move_on_range_start(&mut self.cursor, &mut self.start_bound)
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
                    Some(Ok((key, data)))
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
            match (
                self.cursor.current(),
                move_on_range_end(&mut self.cursor, &self.end_bound),
            ) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
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
                    Some(Ok((key, data)))
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RwRange<'txn> {
    cursor: RwCursor<'txn>,
    move_on_start: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
}

impl<'txn> RwRange<'txn> {
    pub(crate) fn new(
        cursor: RwCursor<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> RwRange<'txn> {
        RwRange {
            cursor,
            move_on_start: true,
            start_bound,
            end_bound,
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
    /// In other words: Tranform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn put_current<A, B>(&mut self, key: A, data: B) -> Result<bool>
    where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.cursor.put_current(key.as_ref(), data.as_ref())
    }

    /// Append the given key/value pair to the end of the database.
    ///
    /// If a key is inserted that is less than any previous key a `KeyExist` error
    /// is returned and the key is not inserted into the database.
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database while
    /// modifying it, so you can't use the key/value that comes from the cursor to feed
    /// this function.
    ///
    /// In other words: Tranform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn append<A, B>(&mut self, key: A, data: B) -> Result<()>
    where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.cursor.append(key.as_ref(), data.as_ref())
    }
}

impl<'txn> Iterator for RwRange<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_start {
            self.move_on_start = false;
            move_on_range_start(&mut self.cursor, &mut self.start_bound)
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
                    Some(Ok((key, data)))
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
            match (
                self.cursor.current(),
                move_on_range_end(&mut self.cursor, &self.end_bound),
            ) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
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
                    Some(Ok((key, data)))
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RoRevRange<'txn> {
    cursor: RoCursor<'txn>,
    move_on_end: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
}

impl<'txn> RoRevRange<'txn> {
    pub(crate) fn new(
        cursor: RoCursor<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> RoRevRange<'txn> {
        RoRevRange {
            cursor,
            move_on_end: true,
            start_bound,
            end_bound,
        }
    }
}

impl<'txn> Iterator for RoRevRange<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            self.move_on_end = false;
            move_on_range_end(&mut self.cursor, &self.end_bound)
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
                    Some(Ok((key, data)))
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            move_on_range_start(&mut self.cursor, &mut self.start_bound)
        } else {
            let current = self.cursor.current();
            let start = move_on_range_start(&mut self.cursor, &mut self.start_bound);
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
                let must_be_returned = match &self.end_bound {
                    Bound::Included(end) => key <= end,
                    Bound::Excluded(end) => key < end,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    Some(Ok((key, data)))
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct RwRevRange<'txn> {
    cursor: RwCursor<'txn>,
    move_on_end: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
}

impl<'txn> RwRevRange<'txn> {
    pub(crate) fn new(
        cursor: RwCursor<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> RwRevRange<'txn> {
        RwRevRange {
            cursor,
            move_on_end: true,
            start_bound,
            end_bound,
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
    /// In other words: Tranform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn put_current<A, B>(&mut self, key: A, data: B) -> Result<bool>
    where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.cursor.put_current(key.as_ref(), data.as_ref())
    }

    /// Append the given key/value pair to the end of the database.
    ///
    /// If a key is inserted that is less than any previous key a `KeyExist` error
    /// is returned and the key is not inserted into the database.
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database while
    /// modifying it, so you can't use the key/value that comes from the cursor to feed
    /// this function.
    ///
    /// In other words: Tranform the key and value that you borrow from this database into an owned
    /// version of them i.e. `&str` into `String`.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val).
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn append<A, B>(&mut self, key: A, data: B) -> Result<()>
    where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.cursor.append(key.as_ref(), data.as_ref())
    }
}

impl<'txn> Iterator for RwRevRange<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            self.move_on_end = false;
            move_on_range_end(&mut self.cursor, &self.end_bound)
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
                    Some(Ok((key, data)))
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            move_on_range_start(&mut self.cursor, &mut self.start_bound)
        } else {
            let current = self.cursor.current();
            let start = move_on_range_start(&mut self.cursor, &mut self.start_bound);
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
                let must_be_returned = match &self.end_bound {
                    Bound::Included(end) => key <= end,
                    Bound::Excluded(end) => key < end,
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    Some(Ok((key, data)))
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
