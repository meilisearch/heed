//! I/O operations with io_uring support

use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use memmap2::{MmapMut, MmapOptions};
use crate::error::{Result, Error, PageId};
use crate::page::{Page, PageHeader, PAGE_SIZE};

/// I/O backend trait
pub trait IoBackend: Send + Sync {
    /// Read a page from disk
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>>;
    
    /// Write a page to disk
    fn write_page(&self, page: &Page) -> Result<()>;
    
    /// Sync data to disk
    fn sync(&self) -> Result<()>;
    
    /// Get the current size in pages
    fn size_in_pages(&self) -> u64;
    
    /// Grow the file to accommodate more pages
    fn grow(&self, new_size: u64) -> Result<()>;
}

/// Standard I/O backend using memory mapping
pub struct MmapBackend {
    /// The underlying file
    file: File,
    /// Memory map (protected by mutex for resizing)
    mmap: Arc<Mutex<MmapMut>>,
    /// Current file size in bytes
    file_size: AtomicU64,
    /// Page size (usually 4KB)
    page_size: usize,
    /// File path for reopening on resize
    path: std::path::PathBuf,
}

impl MmapBackend {
    /// Create a new mmap backend
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_options(path, 10 * 1024 * 1024) // Default 10MB
    }
    
    /// Create with initial size
    pub fn with_options(path: impl AsRef<Path>, initial_size: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| Error::Io(e))?;
        
        // Get current file size
        let metadata = file.metadata().map_err(|e| Error::Io(e))?;
        let mut file_size = metadata.len();
        
        // Ensure minimum size
        let min_size = PAGE_SIZE as u64 * 4; // At least 4 pages (2 meta + 2 data)
        if file_size < min_size {
            file_size = initial_size.max(min_size);
            file.set_len(file_size).map_err(|e| Error::Io(e))?;
        }
        
        // Ensure size is page-aligned
        let page_size = PAGE_SIZE;
        file_size = (file_size / page_size as u64) * page_size as u64;
        
        // Create memory map
        let mmap = unsafe {
            MmapOptions::new()
                .len(file_size as usize)
                .map_mut(&file)
                .map_err(|e| Error::Io(e))?
        };
        
        Ok(Self {
            file,
            mmap: Arc::new(Mutex::new(mmap)),
            file_size: AtomicU64::new(file_size),
            page_size,
            path,
        })
    }
    
    /// Get a slice of the memory map for a page
    fn get_page_slice(&self, page_id: PageId) -> Result<&[u8]> {
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;
        
        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }
        
        // This is safe because we're returning a slice that lives as long as the mmap
        let mmap = self.mmap.lock().unwrap();
        let ptr = mmap.as_ptr();
        
        unsafe {
            Ok(std::slice::from_raw_parts(
                ptr.add(offset),
                self.page_size
            ))
        }
    }
    
    /// Get a mutable slice of the memory map for a page
    fn get_page_slice_mut(&self, page_id: PageId) -> Result<&mut [u8]> {
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;
        
        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }
        
        // This is safe because we have exclusive access through the mmap mutex
        let mut mmap = self.mmap.lock().unwrap();
        let ptr = mmap.as_mut_ptr();
        
        unsafe {
            Ok(std::slice::from_raw_parts_mut(
                ptr.add(offset),
                self.page_size
            ))
        }
    }
}

impl IoBackend for MmapBackend {
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>> {
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;
        
        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }
        
        let mmap = self.mmap.lock().unwrap();
        
        // Create a boxed page to hold the data
        let mut page = Page::new(page_id, crate::page::PageFlags::empty());
        
        // Copy the entire page data
        let src = &mmap[offset..offset + self.page_size];
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                page.as_mut() as *mut Page as *mut u8,
                self.page_size
            );
        }
        
        Ok(page)
    }
    
    fn write_page(&self, page: &Page) -> Result<()> {
        let page_id = PageId(page.header.pgno);
        let offset = page_id.0 as usize * self.page_size;
        let size = self.file_size.load(Ordering::Acquire) as usize;
        
        if offset + self.page_size > size {
            return Err(Error::InvalidPageId(page_id));
        }
        
        let mut mmap = self.mmap.lock().unwrap();
        
        // Write the entire page data
        let dst = &mut mmap[offset..offset + self.page_size];
        unsafe {
            std::ptr::copy_nonoverlapping(
                page as *const Page as *const u8,
                dst.as_mut_ptr(),
                self.page_size
            );
        }
        
        Ok(())
    }
    
    fn sync(&self) -> Result<()> {
        let mmap = self.mmap.lock().unwrap();
        mmap.flush().map_err(|e| Error::Io(e))?;
        Ok(())
    }
    
    fn size_in_pages(&self) -> u64 {
        self.file_size.load(Ordering::Acquire) / self.page_size as u64
    }
    
    fn grow(&self, new_size: u64) -> Result<()> {
        let new_size_bytes = new_size * self.page_size as u64;
        let current_size = self.file_size.load(Ordering::Acquire);
        
        if new_size_bytes <= current_size {
            return Ok(());
        }
        
        // Grow the file
        self.file.set_len(new_size_bytes).map_err(|e| Error::Io(e))?;
        
        // Remap
        let mut mmap_guard = self.mmap.lock().unwrap();
        
        // Create new mmap
        let new_mmap = unsafe {
            MmapOptions::new()
                .len(new_size_bytes as usize)
                .map_mut(&self.file)
                .map_err(|e| Error::Io(e))?
        };
        
        // Replace the old mmap
        *mmap_guard = new_mmap;
        
        // Update size
        self.file_size.store(new_size_bytes, Ordering::Release);
        
        Ok(())
    }
}

/// Zero-copy page access for reading
pub struct PageRef<'a> {
    data: &'a [u8],
}

impl<'a> PageRef<'a> {
    /// Create from a memory-mapped region
    pub fn from_mmap(data: &'a [u8]) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Custom("Invalid page size".into()));
        }
        Ok(Self { data })
    }
    
    /// Get the page header
    pub fn header(&self) -> &PageHeader {
        unsafe {
            &*(self.data.as_ptr() as *const PageHeader)
        }
    }
    
    /// Get page data (excluding header)
    pub fn data(&self) -> &[u8] {
        &self.data[std::mem::size_of::<PageHeader>()..]
    }
}

/// Zero-copy page access for writing
pub struct PageRefMut<'a> {
    data: &'a mut [u8],
}

impl<'a> PageRefMut<'a> {
    /// Create from a memory-mapped region
    pub fn from_mmap(data: &'a mut [u8]) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Custom("Invalid page size".into()));
        }
        Ok(Self { data })
    }
    
    /// Get the page header
    pub fn header(&self) -> &PageHeader {
        unsafe {
            &*(self.data.as_ptr() as *const PageHeader)
        }
    }
    
    /// Get mutable page header
    pub fn header_mut(&mut self) -> &mut PageHeader {
        unsafe {
            &mut *(self.data.as_mut_ptr() as *mut PageHeader)
        }
    }
    
    /// Get mutable page data (excluding header)
    pub fn data_mut(&mut self) -> &mut [u8] {
        let header_size = std::mem::size_of::<PageHeader>();
        &mut self.data[header_size..]
    }
}

/// File locking for exclusive access
#[cfg(unix)]
pub fn lock_file(file: &File) -> Result<()> {
    use libc::{flock, LOCK_EX, LOCK_NB};
    use std::os::unix::io::AsRawFd;
    
    let fd = file.as_raw_fd();
    let result = unsafe { flock(fd, LOCK_EX | LOCK_NB) };
    
    if result != 0 {
        return Err(Error::Custom("Failed to acquire file lock".into()));
    }
    
    Ok(())
}

#[cfg(windows)]
pub fn lock_file(file: &File) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};
    use windows_sys::Win32::System::IO::OVERLAPPED;
    
    let handle = file.as_raw_handle() as isize;
    let mut overlapped = OVERLAPPED::default();
    
    let result = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    
    if result == 0 {
        return Err(Error::Custom("Failed to acquire file lock".into()));
    }
    
    Ok(())
}

#[cfg(not(any(unix, windows)))]
pub fn lock_file(_file: &File) -> Result<()> {
    // No file locking on other platforms
    Ok(())
}

/// io_uring backend for Linux
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub struct IoUringBackend {
    mmap: MmapBackend, // Fall back to mmap for now
    ring: io_uring::IoUring,
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl IoUringBackend {
    /// Create a new io_uring backend
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let mmap = MmapBackend::new(path)?;
        let ring = io_uring::IoUring::new(256)
            .map_err(|e| Error::Io(std::io::Error::from(e)))?;
        
        Ok(Self { mmap, ring })
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl IoBackend for IoUringBackend {
    fn read_page(&self, page_id: PageId) -> Result<Box<Page>> {
        // For now, fall back to mmap
        // TODO: Implement async io_uring operations
        self.mmap.read_page(page_id)
    }
    
    fn write_page(&self, page: &Page) -> Result<()> {
        self.mmap.write_page(page)
    }
    
    fn sync(&self) -> Result<()> {
        self.mmap.sync()
    }
    
    fn size_in_pages(&self) -> u64 {
        self.mmap.size_in_pages()
    }
    
    fn grow(&self, new_size: u64) -> Result<()> {
        self.mmap.grow(new_size)
    }
}