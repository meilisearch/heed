use std::borrow::Cow;
use std::convert::Infallible;

use bytemuck::{bytes_of, try_from_bytes, AnyBitPattern, NoUninit, PodCastError};
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

impl<'a, T: NoUninit> BytesEncode<'a> for UnalignedType<T> {
    type EItem = T;
    type Err = Infallible;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        Ok(Cow::Borrowed(bytes_of(item)))
    }
}

impl<'a, T: AnyBitPattern> BytesDecode<'a> for UnalignedType<T> {
    type DItem = &'a T;
    type Err = PodCastError;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err> {
        try_from_bytes(bytes)
    }
}

unsafe impl<T> Send for UnalignedType<T> {}

unsafe impl<T> Sync for UnalignedType<T> {}
