mod iter;
mod prefix;
mod range;

pub use self::iter::{RoIter, RoRevIter, RwIter, RwRevIter};
pub use self::prefix::{RoPrefix, RoRevPrefix, RwPrefix, RwRevPrefix};
pub use self::range::{RoRange, RoRevRange, RwRange, RwRevRange};

fn advance_key(bytes: &mut Vec<u8>) {
    match bytes.last_mut() {
        Some(&mut 255) | None => bytes.push(0),
        Some(last) => *last += 1,
    }
}

fn retreat_key(bytes: &mut Vec<u8>) {
    match bytes.last_mut() {
        Some(&mut 0) => {
            bytes.pop();
        }
        Some(last) => *last -= 1,
        None => panic!("Vec is empty and must not be"),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn prefix_iter_with_byte_255() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(dir.path())
            .unwrap();

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<ByteSlice, Str>(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], "hello").unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], "world").unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], "world").unwrap();

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

        wtxn.abort();
    }

    #[test]
    fn iter_last() {
        use crate::byteorder::BigEndian;
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(dir.path())
            .unwrap();

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
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(dir.path())
            .unwrap();

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
    fn prefix_iter_last() {
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(dir.path())
            .unwrap();

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<ByteSlice, Unit>(&mut wtxn, None).unwrap();
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
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(dir.path())
            .unwrap();

        let mut wtxn = env.write_txn().unwrap();
        let db = env.create_database::<ByteSlice, Unit>(&mut wtxn, None).unwrap();
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
    fn rev_range_iter_last() {
        use crate::byteorder::BigEndian;
        use crate::types::*;
        use crate::EnvOpenOptions;

        let dir = tempfile::tempdir().unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(dir.path())
            .unwrap();

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
}
