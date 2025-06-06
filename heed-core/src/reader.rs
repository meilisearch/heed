//! Reader slot management for MVCC
//!
//! This module manages reader slots to track active read transactions
//! and determine which pages can be safely recycled.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::error::{Error, Result, TransactionId};

/// Maximum number of readers
pub const MAX_READERS: usize = 126;

/// Reader slot information stored in shared memory
#[repr(C)]
#[derive(Debug)]
pub struct ReaderSlot {
    /// Process ID
    pub pid: AtomicU32,
    /// Thread ID  
    pub tid: AtomicU64,
    /// Transaction ID being read
    pub txn_id: AtomicU64,
    /// Timestamp when slot was acquired (for stale reader detection)
    pub timestamp: AtomicU64,
}

impl ReaderSlot {
    /// Create a new empty reader slot
    pub fn new() -> Self {
        Self {
            pid: AtomicU32::new(0),
            tid: AtomicU64::new(0),
            txn_id: AtomicU64::new(0),
            timestamp: AtomicU64::new(0),
        }
    }
    
    /// Check if this slot is free
    pub fn is_free(&self) -> bool {
        self.pid.load(Ordering::Acquire) == 0
    }
    
    /// Try to acquire this slot
    pub fn try_acquire(&self, txn_id: TransactionId) -> bool {
        // Try to atomically set PID from 0 to current process ID
        let pid = std::process::id();
        let old_pid = self.pid.compare_exchange(
            0,
            pid,
            Ordering::AcqRel,
            Ordering::Acquire
        );
        
        if old_pid.is_ok() {
            // Successfully acquired the slot
            self.tid.store(thread_id(), Ordering::Release);
            self.txn_id.store(txn_id.0, Ordering::Release);
            self.timestamp.store(current_timestamp(), Ordering::Release);
            true
        } else {
            false
        }
    }
    
    /// Release this slot
    pub fn release(&self) {
        // Clear transaction ID first to ensure readers see consistent state
        self.txn_id.store(0, Ordering::Release);
        self.tid.store(0, Ordering::Release);
        self.timestamp.store(0, Ordering::Release);
        // Clear PID last to make slot available
        self.pid.store(0, Ordering::Release);
    }
    
    /// Check if this slot is stale (process died without releasing)
    pub fn is_stale(&self) -> bool {
        let pid = self.pid.load(Ordering::Acquire);
        if pid == 0 {
            return false;
        }
        
        // Check if process is still alive
        #[cfg(unix)]
        {
            unsafe {
                // Send signal 0 to check if process exists
                libc::kill(pid as i32, 0) != 0
            }
        }
        
        #[cfg(not(unix))]
        {
            // On non-Unix systems, use a timeout approach
            let timestamp = self.timestamp.load(Ordering::Acquire);
            let now = current_timestamp();
            // Consider stale after 5 minutes of inactivity
            now.saturating_sub(timestamp) > 300_000_000_000 // 5 minutes in nanoseconds
        }
    }
}

/// Reader table for managing all reader slots
pub struct ReaderTable {
    /// Array of reader slots
    slots: Vec<ReaderSlot>,
}

impl ReaderTable {
    /// Create a new reader table
    pub fn new(max_readers: usize) -> Self {
        let mut slots = Vec::with_capacity(max_readers);
        for _ in 0..max_readers {
            slots.push(ReaderSlot::new());
        }
        Self { slots }
    }
    
    /// Try to acquire a reader slot
    pub fn acquire(&self, txn_id: TransactionId) -> Result<usize> {
        // First pass: try to find a free slot
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.try_acquire(txn_id) {
                return Ok(i);
            }
        }
        
        // Second pass: check for stale slots
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.is_stale() {
                // Force release the stale slot
                slot.release();
                if slot.try_acquire(txn_id) {
                    return Ok(i);
                }
            }
        }
        
        Err(Error::ReadersFull)
    }
    
    /// Release a reader slot
    pub fn release(&self, slot_index: usize) {
        if let Some(slot) = self.slots.get(slot_index) {
            slot.release();
        }
    }
    
    /// Get the oldest active reader transaction ID
    pub fn oldest_reader(&self) -> Option<TransactionId> {
        let mut oldest = None;
        
        for slot in &self.slots {
            let txn_id = slot.txn_id.load(Ordering::Acquire);
            if txn_id > 0 {
                match oldest {
                    None => oldest = Some(TransactionId(txn_id)),
                    Some(TransactionId(old)) if txn_id < old => {
                        oldest = Some(TransactionId(txn_id));
                    }
                    _ => {}
                }
            }
        }
        
        oldest
    }
    
    /// Clean up stale reader slots
    pub fn cleanup_stale(&self) -> usize {
        let mut cleaned = 0;
        
        for slot in &self.slots {
            if slot.is_stale() {
                slot.release();
                cleaned += 1;
            }
        }
        
        cleaned
    }
    
    /// Get all active reader transaction IDs
    pub fn active_readers(&self) -> Vec<TransactionId> {
        let mut readers = Vec::new();
        
        for slot in &self.slots {
            let txn_id = slot.txn_id.load(Ordering::Acquire);
            if txn_id > 0 {
                readers.push(TransactionId(txn_id));
            }
        }
        
        readers
    }
    
    /// Get reader count
    pub fn reader_count(&self) -> usize {
        self.slots.iter()
            .filter(|slot| !slot.is_free())
            .count()
    }
    
    /// Enumerate all active readers with detailed information
    pub fn enumerate_readers(&self) -> Vec<ReaderInfo> {
        let mut readers = Vec::new();
        
        for (slot_idx, slot) in self.slots.iter().enumerate() {
            let pid = slot.pid.load(Ordering::Acquire);
            if pid > 0 {
                readers.push(ReaderInfo {
                    slot_index: slot_idx,
                    pid,
                    tid: slot.tid.load(Ordering::Acquire),
                    txn_id: TransactionId(slot.txn_id.load(Ordering::Acquire)),
                    timestamp: slot.timestamp.load(Ordering::Acquire),
                    is_stale: slot.is_stale(),
                });
            }
        }
        
        readers
    }
}

/// Detailed information about an active reader
#[derive(Debug, Clone)]
pub struct ReaderInfo {
    /// Slot index
    pub slot_index: usize,
    /// Process ID
    pub pid: u32,
    /// Thread ID
    pub tid: u64,
    /// Transaction ID
    pub txn_id: TransactionId,
    /// Timestamp when acquired (nanoseconds since epoch)
    pub timestamp: u64,
    /// Whether this reader appears to be stale
    pub is_stale: bool,
}

impl ReaderInfo {
    /// Get age of this reader in seconds
    pub fn age_seconds(&self) -> u64 {
        let now = current_timestamp();
        (now.saturating_sub(self.timestamp)) / 1_000_000_000
    }
}

/// Get current thread ID
fn thread_id() -> u64 {
    #[cfg(unix)]
    {
        unsafe { libc::pthread_self() as u64 }
    }
    
    #[cfg(windows)]
    {
        unsafe { winapi::um::processthreadsapi::GetCurrentThreadId() as u64 }
    }
    
    #[cfg(not(any(unix, windows)))]
    {
        // Fallback: use thread local storage address as ID
        thread_local! {
            static THREAD_ID: u8 = 0;
        }
        THREAD_ID.with(|id| id as *const _ as u64)
    }
}

/// Get current timestamp in nanoseconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_reader_slot_acquire_release() {
        let slot = ReaderSlot::new();
        assert!(slot.is_free());
        
        // Acquire slot
        assert!(slot.try_acquire(TransactionId(100)));
        assert!(!slot.is_free());
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 100);
        
        // Can't acquire again
        assert!(!slot.try_acquire(TransactionId(200)));
        
        // Release slot
        slot.release();
        assert!(slot.is_free());
        assert_eq!(slot.txn_id.load(Ordering::Acquire), 0);
    }
    
    #[test]
    fn test_reader_table() {
        let table = ReaderTable::new(10);
        
        // Acquire some slots
        let slot1 = table.acquire(TransactionId(100)).unwrap();
        let slot2 = table.acquire(TransactionId(200)).unwrap();
        let slot3 = table.acquire(TransactionId(150)).unwrap();
        
        assert_eq!(table.reader_count(), 3);
        
        // Check oldest reader
        assert_eq!(table.oldest_reader(), Some(TransactionId(100)));
        
        // Release a slot
        table.release(slot1);
        assert_eq!(table.reader_count(), 2);
        assert_eq!(table.oldest_reader(), Some(TransactionId(150)));
        
        // Release all
        table.release(slot2);
        table.release(slot3);
        assert_eq!(table.reader_count(), 0);
        assert_eq!(table.oldest_reader(), None);
    }
    
    #[test]
    fn test_reader_table_full() {
        let table = ReaderTable::new(3);
        
        // Fill all slots
        let _slot1 = table.acquire(TransactionId(100)).unwrap();
        let _slot2 = table.acquire(TransactionId(200)).unwrap();
        let _slot3 = table.acquire(TransactionId(300)).unwrap();
        
        // Next acquire should fail
        assert!(table.acquire(TransactionId(400)).is_err());
    }
    
    #[test]
    fn test_reader_management_integration() {
        use crate::env::EnvBuilder;
        use tempfile::TempDir;
        
        let dir = TempDir::new().unwrap();
        let env = EnvBuilder::new()
            .map_size(10 * 1024 * 1024)
            .max_readers(5)
            .open(dir.path())
            .unwrap();
        
        // Start multiple read transactions
        let txn1 = env.begin_txn().unwrap();
        let txn2 = env.begin_txn().unwrap();
        let txn3 = env.begin_txn().unwrap();
        
        // Check reader count
        let inner = env.inner();
        assert_eq!(inner.readers.reader_count(), 3);
        
        // Drop a transaction
        drop(txn2);
        
        // Reader count should decrease
        assert_eq!(inner.readers.reader_count(), 2);
        
        // Should be able to start new transactions
        let txn4 = env.begin_txn().unwrap();
        let txn5 = env.begin_txn().unwrap();
        
        assert_eq!(inner.readers.reader_count(), 4);
        
        // Clean up
        drop(txn1);
        drop(txn3);
        drop(txn4);
        drop(txn5);
        
        assert_eq!(inner.readers.reader_count(), 0);
    }
}