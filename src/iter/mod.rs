mod iter;
mod prefix;
mod range;

pub use self::iter::{RoIter, RoRevIter, RwIter, RwRevIter};
pub use self::prefix::{RoPrefix, RoRevPrefix, RwPrefix, RwRevPrefix};
pub use self::range::{RoRange, RoRevRange, RwRange, RwRevRange};

/// Returns a vector representing the key that is just **after** the one provided.
fn advance_key(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(x) = bytes.last_mut() {
        if let Some(y) = x.checked_add(1) {
            *x = y;
            return Some(bytes);
        } else {
            bytes.pop();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn prefix_iter_with_byte_255() {
        use crate::EnvOpenOptions;
        use std::fs;
        use std::path::Path;

        fs::create_dir_all(Path::new("target").join("prefix_iter_with_byte_255.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("prefix_iter_with_byte_255.mdb"))
            .unwrap();
        let db = env.create_database(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(
            &mut wtxn,
            &[0, 0, 0, 254, 119, 111, 114, 108, 100],
            b"world",
        )
        .unwrap();
        db.put(
            &mut wtxn,
            &[0, 0, 0, 255, 104, 101, 108, 108, 111],
            b"hello",
        )
        .unwrap();
        db.put(
            &mut wtxn,
            &[0, 0, 0, 255, 119, 111, 114, 108, 100],
            b"world",
        )
        .unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], b"world")
            .unwrap();

        // Lets check that we can prefix_iter on that sequence with the key "255".
        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0, 255]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((
                &[0u8, 0, 0, 255, 104, 101, 108, 108, 111][..],
                &b"hello"[..]
            ))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &b"world"[..]))
        );
        assert_eq!(iter.next().transpose().unwrap(), None);
        drop(iter);

        wtxn.abort().unwrap();
    }

    #[test]
    fn iter_last() {
        use crate::EnvOpenOptions;
        use std::fs;
        use std::path::Path;

        fs::create_dir_all(Path::new("target").join("iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("iter_last.mdb"))
            .unwrap();
        let db = env.create_database(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, 1_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 2_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 3_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 4_i32.to_be_bytes(), []).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.iter(&wtxn).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.next().transpose().unwrap(), None);
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, 1_i32.to_be_bytes(), &[][..]).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.iter(&wtxn).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db.iter(&wtxn).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }

    #[test]
    fn range_iter_last() {
        use crate::EnvOpenOptions;
        use std::fs;
        use std::path::Path;

        fs::create_dir_all(Path::new("target").join("iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("iter_last.mdb"))
            .unwrap();
        let db = env.create_database(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, 1_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 2_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 3_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 4_i32.to_be_bytes(), []).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.range::<_, &[u8]>(&wtxn, ..).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db.range::<_, &[u8]>(&wtxn, ..).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db.range::<_, &[u8]>(&wtxn, ..).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db.range::<_, &[u8]>(&wtxn, ..).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.next().transpose().unwrap(), None);
        assert_eq!(iter.last().transpose().unwrap(), None);

        let mut iter = db
            .range(&wtxn, 2_i32.to_be_bytes()..=4_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db
            .range(&wtxn, 2_i32.to_be_bytes()..4_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db
            .range(&wtxn, 2_i32.to_be_bytes()..4_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db
            .range(&wtxn, 2_i32.to_be_bytes()..2_i32.to_be_bytes())
            .unwrap();
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db
            .range(&wtxn, 2_i32.to_be_bytes()..=1_i32.to_be_bytes())
            .unwrap();
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, 1_i32.to_be_bytes(), []).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.range::<_, &[u8]>(&wtxn, ..).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db.range::<_, &[u8]>(&wtxn, ..).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }

    #[test]
    fn prefix_iter_last() {
        use crate::EnvOpenOptions;
        use std::fs;
        use std::path::Path;

        fs::create_dir_all(Path::new("target").join("prefix_iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("prefix_iter_last.mdb"))
            .unwrap();
        let db = env.create_database(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], [])
            .unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], [])
            .unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], [])
            .unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], [])
            .unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &[][..]))
        );

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], &[][..]))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &[][..]))
        );

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], &[][..]))
        );

        let mut iter = db.prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }

    #[test]
    fn rev_prefix_iter_last() {
        use crate::EnvOpenOptions;
        use std::fs;
        use std::path::Path;

        fs::create_dir_all(Path::new("target").join("prefix_iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("prefix_iter_last.mdb"))
            .unwrap();
        let db = env.create_database(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 254, 119, 111, 114, 108, 100], [])
            .unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 104, 101, 108, 108, 111], [])
            .unwrap();
        db.put(&mut wtxn, &[0, 0, 0, 255, 119, 111, 114, 108, 100], [])
            .unwrap();
        db.put(&mut wtxn, &[0, 0, 1, 0, 119, 111, 114, 108, 100], [])
            .unwrap();

        // Lets check that we properly get the last entry.
        let iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], &[][..]))
        );

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], &[][..]))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], &[][..]))
        );

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 0]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 0, 254, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db.rev_prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], &[][..]))
        );

        let mut iter = db.rev_prefix_iter(&wtxn, &[0, 0, 1]).unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&[0, 0, 1, 0, 119, 111, 114, 108, 100][..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }

    #[test]
    fn rev_range_iter_last() {
        use crate::EnvOpenOptions;
        use std::fs;
        use std::path::Path;

        fs::create_dir_all(Path::new("target").join("range_iter_last.mdb")).unwrap();
        let env = EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(Path::new("target").join("range_iter_last.mdb"))
            .unwrap();
        let db = env.create_database(None).unwrap();

        // Create an ordered list of keys...
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, 1_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 2_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 3_i32.to_be_bytes(), []).unwrap();
        db.put(&mut wtxn, 4_i32.to_be_bytes(), []).unwrap();

        // Lets check that we properly get the last entry.
        let iter = db
            .rev_range(&wtxn, 1_i32.to_be_bytes()..=3_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db
            .rev_range(&wtxn, 0_i32.to_be_bytes()..4_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db
            .rev_range(&wtxn, 0_i32.to_be_bytes()..=5_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&3_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&2_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        let iter = db
            .rev_range(&wtxn, 0_i32.to_be_bytes()..=5_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.last().transpose().unwrap(),
            Some((&1_i32.to_be_bytes()[..], &[][..]))
        );

        let mut iter = db
            .rev_range(&wtxn, 4_i32.to_be_bytes()..=4_i32.to_be_bytes())
            .unwrap();
        assert_eq!(
            iter.next().transpose().unwrap(),
            Some((&4_i32.to_be_bytes()[..], &[][..]))
        );
        assert_eq!(iter.last().transpose().unwrap(), None);

        wtxn.abort().unwrap();
    }
}
