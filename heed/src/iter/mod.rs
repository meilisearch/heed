mod iter;
mod range;
mod prefix;

pub use self::iter::{RoIter, RoRevIter, RwIter, RwRevIter};
pub use self::range::{RoRange, RoRevRange, RwRange, RwRevRange};
pub use self::prefix::{RoPrefix, RoRevPrefix, RwPrefix, RwRevPrefix};

fn advance_key(bytes: &mut Vec<u8>) {
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

    #[test]
    fn rev_prefix_iter_last() {
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
        let iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ())));

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ())));

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.rev_prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((&[0, 0, 1,   0, 119, 111, 114, 108, 100][..], ())));

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((&[0, 0, 1,   0, 119, 111, 114, 108, 100][..], ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }

    #[test]
    fn rev_range_iter_last() {
        use std::fs;
        use std::path::Path;
        use crate::EnvOpenOptions;
        use crate::{zerocopy::I32, byteorder::BigEndian};
        use crate::types::*;

        fs::create_dir_all(Path::new("target").join("range_iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("range_iter_last.mdb")).unwrap();
        let db = env.create_database::<OwnedType<BEI32>, Unit>(None).unwrap();
        type BEI32 = I32<BigEndian>;

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &BEI32::new(1), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(2), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(3), &()).unwrap();
        db.put(&mut wtxn, &BEI32::new(4), &()).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.rev_range(&wtxn, &(BEI32::new(1)..=BEI32::new(3))).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(1), ())));

        let mut iter = db.rev_range(&wtxn, &(BEI32::new(0)..BEI32::new(4))).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(1), ())));

        let mut iter = db.rev_range(&wtxn, &(BEI32::new(0)..=BEI32::new(5))).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(4), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(3), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(2), ())));
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(1), ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.rev_range(&wtxn, &(BEI32::new(0)..=BEI32::new(5))).unwrap();
        assert_eq!(iter.last().transpose().unwrap(), Some((BEI32::new(1), ())));

        let mut iter = db.rev_range(&wtxn, &(BEI32::new(4)..=BEI32::new(4))).unwrap();
        assert_eq!(iter.next().transpose().unwrap(), Some((BEI32::new(4), ())));
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }
}
