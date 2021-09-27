use super::advance_key;
use crate::*;

fn move_on_last_prefix<'txn>(
    cursor: &mut RoCursor<'txn>,
    prefix: Vec<u8>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
    match advance_key(prefix) {
        Some(next_prefix) => cursor
            .move_on_key_greater_than_or_equal_to(&next_prefix)
            .and_then(|_| cursor.move_on_prev()),
        None => cursor.move_on_last(),
    }
}

pub struct RoPrefix<'txn> {
    cursor: RoCursor<'txn>,
    prefix: Vec<u8>,
    move_on_first: bool,
}

impl<'txn> RoPrefix<'txn> {
    pub(crate) fn new(cursor: RoCursor<'txn>, prefix: Vec<u8>) -> RoPrefix<'txn> {
        RoPrefix {
            cursor,
            prefix,
            move_on_first: true,
        }
    }
}

impl<'txn> Iterator for RoPrefix<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor
                .move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            self.cursor.move_on_next()
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            move_on_last_prefix(&mut self.cursor, self.prefix.clone())
        } else {
            match (
                self.cursor.current(),
                move_on_last_prefix(&mut self.cursor, self.prefix.clone()),
            ) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }
}

pub struct RwPrefix<'txn> {
    cursor: RwCursor<'txn>,
    prefix: Vec<u8>,
    move_on_first: bool,
}

impl<'txn> RwPrefix<'txn> {
    pub(crate) fn new(cursor: RwCursor<'txn>, prefix: Vec<u8>) -> RwPrefix<'txn> {
        RwPrefix {
            cursor,
            prefix,
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

impl<'txn> Iterator for RwPrefix<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor
                .move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            self.cursor.move_on_next()
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            move_on_last_prefix(&mut self.cursor, self.prefix.clone())
        } else {
            match (
                self.cursor.current(),
                move_on_last_prefix(&mut self.cursor, self.prefix.clone()),
            ) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }
}

pub struct RoRevPrefix<'txn> {
    cursor: RoCursor<'txn>,
    prefix: Vec<u8>,
    move_on_last: bool,
}

impl<'txn> RoRevPrefix<'txn> {
    pub(crate) fn new(cursor: RoCursor<'txn>, prefix: Vec<u8>) -> RoRevPrefix<'txn> {
        RoRevPrefix {
            cursor,
            prefix,
            move_on_last: true,
        }
    }
}

impl<'txn> Iterator for RoRevPrefix<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            move_on_last_prefix(&mut self.cursor, self.prefix.clone())
        } else {
            self.cursor.move_on_prev()
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor
                .move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            let current = self.cursor.current();
            let start = self
                .cursor
                .move_on_key_greater_than_or_equal_to(&self.prefix);
            match (current, start) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }
}

pub struct RwRevPrefix<'txn> {
    cursor: RwCursor<'txn>,
    prefix: Vec<u8>,
    move_on_last: bool,
}

impl<'txn> RwRevPrefix<'txn> {
    pub(crate) fn new(cursor: RwCursor<'txn>, prefix: Vec<u8>) -> RwRevPrefix<'txn> {
        RwRevPrefix {
            cursor,
            prefix,
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

impl<'txn> Iterator for RwRevPrefix<'txn> {
    type Item = Result<(&'txn [u8], &'txn [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            move_on_last_prefix(&mut self.cursor, self.prefix.clone())
        } else {
            self.cursor.move_on_prev()
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor
                .move_on_key_greater_than_or_equal_to(&self.prefix)
        } else {
            let current = self.cursor.current();
            let start = self
                .cursor
                .move_on_key_greater_than_or_equal_to(&self.prefix);
            match (current, start) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        result
            .map(|option| option.filter(|(k, _)| k.starts_with(&self.prefix)))
            .transpose()
    }
}
