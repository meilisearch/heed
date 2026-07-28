mod iter;
mod prefix;
mod range;

pub use self::iter::{RoIter, RoRevIter, RwIter, RwRevIter};
pub use self::prefix::{RoPrefix, RoRevPrefix, RwPrefix, RwRevPrefix};
pub use self::range::{RoRange, RoRevRange, RwRange, RwRevRange};

/// This is just set of tests to check that the Cursors
/// are not Send. We need to use doc test as it is the
/// only way to check for expected compilation failures.
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RoIter;
/// fn is_send<T: Send>() {}
/// is_send::<RoIter<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RoRevIter;
/// fn is_send<T: Send>() {}
/// is_send::<RoRevIter<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RoRange;
/// fn is_send<T: Send>() {}
/// is_send::<RoRange<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RoRevRange;
/// fn is_send<T: Send>() {}
/// is_send::<RoRevRange<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RoPrefix;
/// fn is_send<T: Send>() {}
/// is_send::<RoPrefix<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RoRevPrefix;
/// fn is_send<T: Send>() {}
/// is_send::<RoRevPrefix<Bytes, Bytes>>();
/// ```
///
/// Starting the next section with the Read-write Iterators.
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RwIter;
/// fn is_send<T: Send>() {}
/// is_send::<RwIter<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RwRevIter;
/// fn is_send<T: Send>() {}
/// is_send::<RwRevIter<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RwRange;
/// fn is_send<T: Send>() {}
/// is_send::<RwRange<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RwRevRange;
/// fn is_send<T: Send>() {}
/// is_send::<RwRevRange<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RwPrefix;
/// fn is_send<T: Send>() {}
/// is_send::<RwPrefix<Bytes, Bytes>>();
/// ```
///
/// ```rust,compile_fail
/// use heed::types::*;
/// use heed::RwRevPrefix;
/// fn is_send<T: Send>() {}
/// is_send::<RwRevPrefix<Bytes, Bytes>>();
/// ```
#[doc(hidden)]
#[allow(unused)]
fn test_txns_are_not_send() {}

#[cfg(test)]
mod tests {
    use std::ops;

    #[test]
    fn prefix_iter_last_with_byte_255() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Str>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], "hello").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], "world").unwrap();

        db.put(&mut wtxn, &[255, 255, 0, 254, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[255, 255, 0, 255, 104, 101, 108, 108, 111], "hello").unwrap();
        db.put(&mut wtxn, &[255, 255, 0, 255, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[255, 255, 1, 0, 119, 111, 114, 108, 100], "world").unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.prefix_iter(&wtxn, &[0, 0, 0, 255]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], "world"))
        );

        // Lets check that we can prefix_iter on that sequence with the key "255".
        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0, 255]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0u8, 0, 0, 255, 104, 101, 108, 108, 111][..], "hello"))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], "world"))
        );
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        // Lets check that we properly get the last entry.
        let iter = db.prefix_iter(&wtxn, &[255]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[255, 255, 1, 0, 119, 111, 114, 108, 100][..], "world"))
        );

        // Lets check that we can prefix_iter on that sequence with the key "255".
        let mut iter = db.prefix_iter(&wtxn, &[255]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 0, 254, 119, 111, 114, 108, 100][..], "world"))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 0, 255, 104, 101, 108, 108, 111][..], "hello"))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 0, 255, 119, 111, 114, 108, 100][..], "world"))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 1, 0, 119, 111, 114, 108, 100][..], "world"))
        );
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.abort();
    }

    #[test]
    fn iter_last() {
        use crate::byteorder::BigEndian;
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<BEI32, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        type BEI32 = I32<BigEndian>;

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &1, &()).unwrap();
        db.put(&mut wtxn, &2, &()).unwrap();
        db.put(&mut wtxn, &3, &()).unwrap();
        db.put(&mut wtxn, &4, &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((4, ())));

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((4, ())));

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((4, ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((4, ())));
        assert_eq!(iter.next().transpose().unwrap(), None);
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &1, &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((1, ())));

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();
    }

    #[test]
    fn range_iter_last() {
        use crate::byteorder::BigEndian;
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<BEI32, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        type BEI32 = I32<BigEndian>;

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &1, &()).unwrap();
        db.put(&mut wtxn, &2, &()).unwrap();
        db.put(&mut wtxn, &3, &()).unwrap();
        db.put(&mut wtxn, &4, &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((4, ())));

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((4, ())));

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((4, ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((4, ())));
        assert_eq!(iter.next().transpose().unwrap(), None);
        assert_eq!(iter.last().transpose().unwrap(), None);

        let range = 2..=4;
        let mut iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((4, ())));

        let range = 2..4;
        let mut iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((3, ())));

        let range = 2..4;
        let mut iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let range = 2..2;
        let iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), None);

        #[allow(clippy::reversed_empty_ranges)]
        let range = 2..=1;
        let iter = db.range(&wtxn, &range).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &1, &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((1, ())));

        let mut iter = db.range(&wtxn, &(..)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();
    }

    #[test]
    fn range_iter_last_with_byte_255() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 1], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 2], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0], &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db
            .range(
                &wtxn,
                &(ops::Bound::Excluded(&[0, 0, 0][..]), ops::Bound::Included(&[0, 0, 1, 0][..])),
            )
            .unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 1, 0][..], ())));

        // Lets check that we can range_iter on that sequence with the key "255".
        let mut iter = db
            .range(
                &wtxn,
                &(ops::Bound::Excluded(&[0, 0, 0][..]), ops::Bound::Included(&[0, 0, 1, 0][..])),
            )
            .unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 1][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 2][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 1, 0][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.abort();
    }

    #[test]
    fn prefix_iter_last() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ()))
        );

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ()))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ()))
        );

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], ()))
        );

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();
    }

    #[test]
    fn rev_prefix_iter_last() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ()))
        );

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ()))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ()))
        );

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.rev_prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], ()))
        );

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();
    }

    #[test]
    fn rev_prefix_iter_last_with_byte_255() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], &()).unwrap();

        db.put(&mut wtxn, &[255, 255, 0, 254, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[255, 255, 0, 255, 104, 101, 108, 108, 111], &()).unwrap();
        db.put(&mut wtxn, &[255, 255, 0, 255, 119, 111, 114, 108, 100], &()).unwrap();
        db.put(&mut wtxn, &[255, 255, 1, 0, 119, 111, 114, 108, 100], &()).unwrap();

        // Lets check that we can get last entry on that sequence ending with the key "255".
        let iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0, 255]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ()))
        );

        // Lets check that we can prefix_iter on that sequence ending with the key "255".
        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0, 255]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ()))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db.rev_prefix_iter(&wtxn, &[255, 255]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 1, 0, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 0, 255, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 0, 255, 104, 101, 108, 108, 111][..], ()))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[255, 255, 0, 254, 119, 111, 114, 108, 100][..], ()))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();
    }

    #[test]
    fn rev_range_iter_last() {
        use crate::byteorder::BigEndian;
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<BEI32, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        type BEI32 = I32<BigEndian>;

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &1, &()).unwrap();
        db.put(&mut wtxn, &2, &()).unwrap();
        db.put(&mut wtxn, &3, &()).unwrap();
        db.put(&mut wtxn, &4, &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.rev_range(&wtxn, &(1..=3)).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((1, ())));

        let mut iter = db.rev_range(&wtxn, &(0..4)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((1, ())));

        let mut iter = db.rev_range(&wtxn, &(0..=5)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((4, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((3, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((2, ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((1, ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.rev_range(&wtxn, &(0..=5)).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((1, ())));

        let mut iter = db.rev_range(&wtxn, &(4..=4)).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((4, ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort();
    }

    #[test]
    fn count_duplicates_on_dup_sort() {
        use crate::byteorder::BigEndian;
        use crate::types::*;
        use crate::{DatabaseFlags, EnvOpenOptions};

        type BEI64 = I64<BigEndian>;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env
            .database_options()
            .types::<BEI64, BEI64>()
            .flags(DatabaseFlags::DUP_SORT)
            .name("dup-sort")
            .create(&mut wtxn)
            .unwrap();

        // Insert keys: one with a single value, one with multiple values.
        db.put(&mut wtxn, &42, &100).unwrap();
        db.put(&mut wtxn, &68, &120).unwrap();
        db.put(&mut wtxn, &68, &121).unwrap();
        db.put(&mut wtxn, &68, &122).unwrap();

        // A regular iterator over a DUP_SORT database errors before first next.
        let mut iter = db.iter(&wtxn).unwrap();
        assert!(iter.count_duplicates().is_err());

        // The first next positions on key 42 which has a single value.
        assert_eq!(iter.next().transpose().unwrap(), Some((42, 100)));
        assert_eq!(iter.count_duplicates().unwrap(), 1);

        // The second next advances to key 68 which has three values.
        assert_eq!(iter.next().transpose().unwrap(), Some((68, 120)));
        assert_eq!(iter.count_duplicates().unwrap(), 3);

        // Exhaust the remaining duplicates.
        assert_eq!(iter.next().transpose().unwrap(), Some((68, 121)));
        assert_eq!(iter.next().transpose().unwrap(), Some((68, 122)));
        assert_eq!(iter.next().transpose().unwrap(), None);

        // The count remains available after exhaustion and does not break
        // subsequent iteration.
        assert_eq!(iter.count_duplicates().unwrap(), 3);
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        // An iterator obtained via get_duplicates is pre-positioned.
        let mut iter = db.get_duplicates(&wtxn, &68).unwrap().expect("key exists");
        assert_eq!(iter.count_duplicates().unwrap(), 3);

        // The count stays constant mid-iteration.
        assert_eq!(iter.next().transpose().unwrap(), Some((68, 120)));
        assert_eq!(iter.count_duplicates().unwrap(), 3);

        // Exhaust the remaining duplicates.
        assert_eq!(iter.next().transpose().unwrap(), Some((68, 121)));
        assert_eq!(iter.next().transpose().unwrap(), Some((68, 122)));
        assert_eq!(iter.next().transpose().unwrap(), None);

        // The count remains available after exhaustion.
        assert_eq!(iter.count_duplicates().unwrap(), 3);
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        // A single-value key in a DUP_SORT database has count 1.
        let mut iter = db.get_duplicates(&wtxn, &42).unwrap().expect("key exists");
        assert_eq!(iter.count_duplicates().unwrap(), 1);
        assert_eq!(iter.next().transpose().unwrap(), Some((42, 100)));
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.commit().unwrap();
    }

    #[test]
    fn count_duplicates_on_non_dup_sort() {
        use crate::byteorder::BigEndian;
        use crate::types::*;
        use crate::EnvOpenOptions;

        type BEI64 = I64<BigEndian>;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env
            .database_options()
            .types::<BEI64, BEI64>()
            .name("non-dup-sort")
            .create(&mut wtxn)
            .unwrap();

        db.put(&mut wtxn, &42, &100).unwrap();
        db.put(&mut wtxn, &68, &200).unwrap();

        // count_duplicates is safe to call even before the first next().
        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(iter.count_duplicates().unwrap(), 1);

        // Every key in a non-DUP_SORT database has exactly one value.
        assert_eq!(iter.next().transpose().unwrap(), Some((42, 100)));
        assert_eq!(iter.count_duplicates().unwrap(), 1);
        assert_eq!(iter.next().transpose().unwrap(), Some((68, 200)));
        assert_eq!(iter.count_duplicates().unwrap(), 1);

        // Exhaust the iterator.
        assert_eq!(iter.next().transpose().unwrap(), None);

        // The count remains available after exhaustion.
        assert_eq!(iter.count_duplicates().unwrap(), 1);
        drop(iter);

        wtxn.commit().unwrap();
    }

    #[test]
    fn rev_range_iter_last_with_byte_255() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(3000)
                .open(dir.path())
                .unwrap()
        };

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Unit>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 1], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 2], &()).unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0], &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db
            .rev_range(
                &wtxn,
                &(ops::Bound::Excluded(&[0, 0, 0][..]), ops::Bound::Included(&[0, 0, 1, 0][..])),
            )
            .unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 0, 1][..], ())));

        // Lets check that we can range_iter on that sequence with the key "255".
        let mut iter = db
            .rev_range(
                &wtxn,
                &(ops::Bound::Excluded(&[0, 0, 0][..]), ops::Bound::Included(&[0, 0, 1, 0][..])),
            )
            .unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 1, 0][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 2][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 1][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.abort();
    }
}
