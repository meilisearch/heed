// MDBX flags (see https://github.com/erthink/libmdbx/blob/master/mdbx.h for more details).
#[repr(u32)]
pub enum Flags {
    MdbNoSubDir = mdbx_sys::MDBX_NOSUBDIR,
    MdbRdOnly = mdbx_sys::MDBX_RDONLY,
    MdbNoMetaSync = mdbx_sys::MDBX_NOMETASYNC,
    MdbWriteMap = mdbx_sys::MDBX_WRITEMAP,
    MdbMapAsync = mdbx_sys::MDBX_MAPASYNC,
    MdbNoTls = mdbx_sys::MDBX_NOTLS,
    MdbNoRdAhead = mdbx_sys::MDBX_NORDAHEAD,
    MdbNoMemInit = mdbx_sys::MDBX_NOMEMINIT,
}
