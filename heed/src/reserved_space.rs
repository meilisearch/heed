use std::mem::MaybeUninit;
use std::{fmt, io};

use crate::mdb::ffi;

/// A structure that is used to improve the write speed in LMDB.
///
/// You must write the exact amount of bytes, no less, no more.
pub struct ReservedSpace {
    size: usize,
    start_ptr: *mut u8,
    written: usize,
}

impl ReservedSpace {
    pub(crate) unsafe fn from_val(val: ffi::MDB_val) -> ReservedSpace {
        ReservedSpace { size: val.mv_size, start_ptr: val.mv_data as *mut u8, written: 0 }
    }

    /// The total number of bytes that this memory buffer has.
    pub fn size(&self) -> usize {
        self.size
    }

    /// The remaining number of bytes that this memory buffer has.
    pub fn remaining(&self) -> usize {
        self.size - self.written
    }

    /// Get a slice of all the bytes that have previously been written.
    ///
    /// This can be used to write information which cannot be known until the very end of
    /// serialization. For example, this method can be used to serialize a value, then compute a
    /// checksum over the bytes, and then write that checksum to a header at the start of the
    /// reserved space.
    pub fn written_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.start_ptr, self.written) }
    }

    /// Fills the remaining reserved space with zeroes.
    ///
    /// This can be used together with [`written_mut`](Self::written_mut) to get a mutable view of
    /// the entire reserved space.
    ///
    /// ### Note
    ///
    /// After calling this function, the entire space is considered to be filled and any
    /// further attempt to [`write`](std::io::Write::write) anything else will fail.
    pub fn fill_zeroes(&mut self) {
        for i in self.written..self.size {
            unsafe { self.start_ptr.add(i).write(0) };
        }
        self.written = self.size;
    }

    /// Get a slice of bytes corresponding to the *entire* reserved space.
    ///
    /// It is safe to write to any byte within the slice. However, for a write past the end of the
    /// prevously written bytes to take effect, [`assume_written`](Self::assume_written) has to be
    /// called to mark those bytes as initialized.
    ///
    /// # Safety
    ///
    /// As the memory comes from within the database itself, the bytes may not yet be
    /// initialized. Thus, it is up to the caller to ensure that only initialized memory is read
    /// (ensured by the [`MaybeUninit`] API).
    pub fn as_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe { std::slice::from_raw_parts_mut(self.start_ptr.cast(), self.size) }
    }

    /// Marks the bytes in the range `0..len` as being initialized by advancing the internal write
    /// pointer.
    ///
    /// # Safety
    ///
    /// The caller guarantees that all bytes in the range have been initialized.
    pub unsafe fn assume_written(&mut self, len: usize) {
        debug_assert!(len <= self.size);
        self.written = len;
    }
}

impl io::Write for ReservedSpace {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.remaining() >= buf.len() {
            let dest = unsafe { self.start_ptr.add(self.written) };
            unsafe { buf.as_ptr().copy_to_nonoverlapping(dest, buf.len()) };
            self.written += buf.len();
            Ok(buf.len())
        } else {
            Err(io::Error::from(io::ErrorKind::WriteZero))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Debug for ReservedSpace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ReservedSpace").finish()
    }
}
