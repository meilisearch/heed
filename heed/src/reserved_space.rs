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
