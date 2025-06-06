//! Comprehensive B+Tree tests

#[cfg(test)]
mod tests {
    use crate::btree::BTree;
    use crate::env::EnvBuilder;
    use crate::db::Database;
    use crate::meta::DbInfo;
    use crate::error::PageId;
    use crate::comparator::LexicographicComparator;
    use tempfile::TempDir;
    use std::sync::Arc;

    #[test]
    fn test_btree_empty_operations() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let txn = env.begin_txn().unwrap();
        let root = PageId(3);
        
        // Search in empty tree
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key").unwrap().is_none());
    }

    #[test]
    fn test_btree_single_entry() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Insert single entry
        assert!(BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"key", b"value").unwrap().is_none());
        assert_eq!(db_info.entries, 1);
        
        // Search for it
        let result = BTree::<LexicographicComparator>::search(&txn, root, b"key").unwrap();
        assert_eq!(result.as_deref(), Some(&b"value"[..]));
        
        // Update it
        let old = BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"key", b"new_value").unwrap();
        assert_eq!(old, Some(b"value".to_vec()));
        assert_eq!(db_info.entries, 1);
        
        // Delete it
        let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, b"key").unwrap();
        assert_eq!(deleted, Some(b"new_value".to_vec()));
        assert_eq!(db_info.entries, 0);
        
        // Verify it's gone
        assert!(BTree::<LexicographicComparator>::search(&txn, root, b"key").unwrap().is_none());
    }

    #[test]
    fn test_btree_sequential_insert() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Insert sequential keys
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            assert!(BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes()).unwrap().is_none());
        }
        
        assert_eq!(db_info.entries, 100);
        
        // Verify all keys
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected_value = format!("value_{:03}", i);
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes()).unwrap();
            assert_eq!(result.as_deref(), Some(expected_value.as_bytes()));
        }
    }

    #[test]
    fn test_btree_random_insert() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Insert keys in random order
        let keys: Vec<usize> = vec![50, 25, 75, 10, 30, 60, 80, 5, 15, 20, 35, 55, 65, 70, 85];
        
        for &i in &keys {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            assert!(BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes()).unwrap().is_none());
        }
        
        assert_eq!(db_info.entries, keys.len() as u64);
        
        // Verify all keys
        for &i in &keys {
            let key = format!("key_{:03}", i);
            let expected_value = format!("value_{:03}", i);
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes()).unwrap();
            assert_eq!(result.as_deref(), Some(expected_value.as_bytes()));
        }
    }

    #[test]
    fn test_btree_page_split() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        let initial_depth = db_info.depth;
        
        // Insert enough entries to force splits
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = vec![i as u8; 256]; // Large values to fill pages faster
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), &value).unwrap();
        }
        
        // Verify tree grew
        assert!(db_info.depth > initial_depth, "Tree should have grown in depth");
        assert!(db_info.branch_pages > 0, "Should have branch pages");
        assert_eq!(db_info.entries, 50);
        
        // Verify all entries are still accessible
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist", key);
            assert_eq!(result.unwrap()[0], i as u8);
        }
    }

    #[test]
    fn test_btree_deletion_patterns() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Insert entries
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Delete every third entry
        for i in (0..30).step_by(3) {
            let key = format!("key_{:03}", i);
            let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, key.as_bytes()).unwrap();
            assert!(deleted.is_some());
        }
        
        assert_eq!(db_info.entries, 20);
        
        // Verify correct entries remain
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let result = BTree::<LexicographicComparator>::search(&txn, root, key.as_bytes()).unwrap();
            
            if i % 3 == 0 {
                assert!(result.is_none(), "Key {} should be deleted", key);
            } else {
                assert!(result.is_some(), "Key {} should exist", key);
            }
        }
    }

    #[test]
    fn test_btree_large_values() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Test with increasingly large values
        let sizes = vec![100, 500, 1000, 2000, 5000, 10000];
        
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("key_{}", i);
            let value = vec![i as u8; size];
            
            assert!(BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), &value).unwrap().is_none());
        }
        
        // Update db_info with final root
        db_info.root = root;
        txn.update_db_info(None, db_info).unwrap();
        
        // Verify all values
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("key_{}", i);
            let result = BTree::<LexicographicComparator>::search(&txn, db_info.root, key.as_bytes()).unwrap();
            assert!(result.is_some());
            let value = result.unwrap();
            assert_eq!(value.len(), size);
            assert!(value.iter().all(|&b| b == i as u8));
        }
        
        // Update large values
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("key_{}", i);
            let new_value = vec![(i + 10) as u8; size];
            
            let old = BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, key.as_bytes(), &new_value).unwrap();
            assert!(old.is_some());
            assert_eq!(old.unwrap().len(), size);
        }
        
        // Update db_info after updates
        db_info.root = root;
        txn.update_db_info(None, db_info).unwrap();
        
        // Delete large values
        for (i, _) in sizes.iter().enumerate() {
            let key = format!("key_{}", i);
            let deleted = BTree::<LexicographicComparator>::delete(&mut txn, &mut root, &mut db_info, key.as_bytes()).unwrap();
            assert!(deleted.is_some());
        }
        
        // Update db_info after deletes
        db_info.root = root;
        txn.update_db_info(None, db_info).unwrap();
        
        assert_eq!(db_info.entries, 0);
        
        txn.commit().unwrap();
    }

    #[test]
    fn test_btree_cursor_iteration() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        // Create and populate database
        let db: Database<String, String> = {
            let mut txn = env.begin_write_txn().unwrap();
            let db = env.create_database(&mut txn, Some("test_db")).unwrap();
            
            // Insert entries
            for i in 0..20 {
                let key = format!("key_{:02}", i);
                let value = format!("value_{:02}", i);
                db.put(&mut txn, key, value).unwrap();
            }
            
            txn.commit().unwrap();
            db
        };
        
        // Test forward iteration
        {
            let txn = env.begin_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            let mut count = 0;
            let mut prev_key = None;
            
            while let Some((key, _value)) = cursor.next().unwrap() {
                // Verify ordering
                if let Some(prev) = prev_key {
                    assert!(key > prev, "Keys should be in ascending order");
                }
                prev_key = Some(key.to_vec());
                count += 1;
            }
            
            assert_eq!(count, 20);
        }
        
        // Test seek operation
        {
            let txn = env.begin_txn().unwrap();
            let mut cursor = db.cursor(&txn).unwrap();
            
            // Seek to middle
            let seek_key = "key_10".to_string();
            let result = cursor.seek(&seek_key).unwrap();
            assert!(result.is_some());
            let (key, value) = result.unwrap();
            assert_eq!(key, seek_key.as_bytes());
            assert_eq!(value.as_bytes(), b"value_10");
            
            // Continue iteration from seek point
            let mut count = 1;
            while cursor.next().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, 10); // Should have 10 entries from key_10 onwards
        }
    }

    #[test]
    fn test_btree_edge_cases() {
        let dir = TempDir::new().unwrap();
        let env = Arc::new(
            EnvBuilder::new()
                .map_size(10 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        );
        
        let mut txn = env.begin_write_txn().unwrap();
        let mut root = PageId(3);
        let mut db_info = DbInfo::default();
        db_info.root = root;
        db_info.leaf_pages = 1;
        
        // Empty key
        assert!(BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, b"", b"empty_key_value").unwrap().is_none());
        assert_eq!(BTree::<LexicographicComparator>::search(&txn, root, b"").unwrap().as_deref(), Some(&b"empty_key_value"[..]));
        
        // Very long key (but within limits)
        let long_key = vec![b'a'; 400];
        assert!(BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, &long_key, b"long_key_value").unwrap().is_none());
        assert_eq!(BTree::<LexicographicComparator>::search(&txn, root, &long_key).unwrap().as_deref(), Some(&b"long_key_value"[..]));
        
        // Binary keys
        let binary_key = vec![0x00, 0xFF, 0x7F, 0x80, 0x01];
        assert!(BTree::<LexicographicComparator>::insert(&mut txn, &mut root, &mut db_info, &binary_key, b"binary_value").unwrap().is_none());
        assert_eq!(BTree::<LexicographicComparator>::search(&txn, root, &binary_key).unwrap().as_deref(), Some(&b"binary_value"[..]));
    }
}