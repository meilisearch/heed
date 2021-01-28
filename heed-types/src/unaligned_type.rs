use std::borrow::Cow;
use std::error::Error;

use bytemuck::{Pod, bytes_of, try_from_bytes};
use heed_traits::{BytesDecode, BytesEncode};

/// Describes a slice that is totally borrowed and doesn't
/// depends on any [memory alignment].
///
/// If you need to store a slice that does depend on memory alignment
/// and that can be big it is recommended to use the [`CowType`].
///
/// To store slices, you must look at the [`CowSlice`],
/// [`OwnedSlice`] or [`UnalignedSlice`] types.
///
/// [memory alignment]: std::mem::align_of()
/// [`CowType`]: crate::CowType
/// [`UnalignedSlice`]: crate::UnalignedSlice
/// [`OwnedSlice`]: crate::OwnedSlice
/// [`CowSlice`]: crate::CowSlice
pub struct UnalignedType<T>(std::marker::PhantomData<T>);

impl<T: Pod> BytesEncode for UnalignedType<T> {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn Error>> {
        Ok(Cow::Borrowed(bytes_of(item)))
    }
}

impl<'a, T: Pod> BytesDecode<'a> for UnalignedType<T> {
    type DItem = &'a T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        try_from_bytes(bytes).map_err(Into::into)
    }
}

unsafe impl<T> Send for UnalignedType<T> {}

unsafe impl<T> Sync for UnalignedType<T> {}
