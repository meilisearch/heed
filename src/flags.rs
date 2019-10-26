use enumflags2::BitFlags;

// LMDB flags (see http://www.lmdb.tech/doc/group__mdb__env.html for more details).
#[derive(BitFlags, Copy, Clone, Debug, PartialEq)]
#[repr(u32)]
pub enum Flags {
    MdbFixedmap = 0x01,
    MdbNosSubDir = 0x4000,
    MdbNoSync = 0x10000,
    MdbRdOnly = 0x20000,
    MdbNoMetaSync = 0x40000,
    MdbWriteMap = 0x80000,
    MdbMapAsync = 0x100000,
    MdbNoTls = 0x200000,
    MdbNoLock = 0x400000,
    MdbNoRdAhead = 0x800000,
    MdbNoMemInit = 0x1000000,
}
