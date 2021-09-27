use crate::*;

pub struct RoIter<'txn> {
    cursor: RoCursor<'txn>,
    move_on_first: bool,
}

impl<'txn> RoIter<'txn> {
    pub(crate) fn new(cursor: RoCursor<'txn>) -> RoIter<'txn> {
        RoIter {
            cursor,
            move_on_first: true,
        }
    }
}

impl<'txn> Iterator for RoIter<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_first()
        } else {
            self.cursor.move_on_next()
        };

        result.transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.cursor.move_on_last()
        } else {
            match (self.cursor.current(), self.cursor.move_on_last()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result.transpose()
    }
}

pub struct RwIter<'txn> {
    cursor: RwCursor<'txn>,
    move_on_first: bool,
}

impl<'txn> RwIter<'txn> {
    pub(crate) fn new(cursor: RwCursor<'txn>) -> RwIter<'txn> {
        RwIter {
            cursor,
            move_on_first: true,
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
    pub unsafe fn put_current<'a>(&mut self, key: &'a [u8], data: &'a [u8]) -> Result<bool> {
        self.cursor.put_current(key, data)
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
    pub unsafe fn append<'a>(&mut self, key: &'a [u8], data: &'a [u8]) -> Result<()> {
        self.cursor.append(key, data)
    }
}

impl<'txn> Iterator for RwIter<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_first()
        } else {
            self.cursor.move_on_next()
        };

        result.transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.cursor.move_on_last()
        } else {
            match (self.cursor.current(), self.cursor.move_on_last()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result.transpose()
    }
}

pub struct RoRevIter<'txn> {
    cursor: RoCursor<'txn>,
    move_on_last: bool,
}

impl<'txn> RoRevIter<'txn> {
    pub(crate) fn new(cursor: RoCursor<'txn>) -> RoRevIter<'txn> {
        RoRevIter {
            cursor,
            move_on_last: true,
        }
    }
}

impl<'txn> Iterator for RoRevIter<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            self.cursor.move_on_last()
        } else {
            self.cursor.move_on_prev()
        };

        result.transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor.move_on_first()
        } else {
            match (self.cursor.current(), self.cursor.move_on_first()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result.transpose()
    }
}

pub struct RwRevIter<'txn> {
    cursor: RwCursor<'txn>,
    move_on_last: bool,
}

impl<'txn> RwRevIter<'txn> {
    pub(crate) fn new(cursor: RwCursor<'txn>) -> RwRevIter<'txn> {
        RwRevIter {
            cursor,
            move_on_last: true,
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
    pub unsafe fn put_current<'a>(&mut self, key: &'a [u8], data: &'a [u8]) -> Result<bool> {
        self.cursor.put_current(key, data)
    }

    /// Append the given key/value pair to the end of the database.
    ///
    /// If a key is inserted that is less than any previous key a `KeyExist` error
    /// is returned and the key is not inserted into the database.
    pub unsafe fn append<'a>(&mut self, key: &'a [u8], data: &'a [u8]) -> Result<()> {
        self.cursor.append(key, data)
    }
}

impl<'txn> Iterator for RwRevIter<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            self.cursor.move_on_last()
        } else {
            self.cursor.move_on_prev()
        };

        result.transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor.move_on_first()
        } else {
            match (self.cursor.current(), self.cursor.move_on_first()) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result.transpose()
    }
}
