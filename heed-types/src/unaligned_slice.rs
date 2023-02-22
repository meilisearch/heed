use std::borrow::Cow;

use bytemuck::{try_cast_slice, AnyBitPattern, NoUninit, PodCastError};
use heed_traits::{BytesDecode, BytesEncode};

/// Describes a type that is totally borrowed and doesn't
/// depends on any [memory alignment].
///
/// If you need to store a type that does depend on memory alignment
/// and that can be big it is recommended to use the [`CowType`].
///
/// [memory alignment]: std::mem::align_of()
/// [`CowType`]: crate::CowType
pub struct UnalignedSlice<T>(std::marker::PhantomData<T>);

impl<'a, T: NoUninit> BytesEncode<'a> for UnalignedSlice<T> {
    type EItem = [T];
    type Err = PodCastError;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        try_cast_slice(item).map(Cow::Borrowed)
    }
}

impl<'a, T: AnyBitPattern> BytesDecode<'a> for UnalignedSlice<T> {
    type DItem = &'a [T];
    type Err = PodCastError;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err> {
        try_cast_slice(bytes)
    }
}

unsafe impl<T> Send for UnalignedSlice<T> {}

unsafe impl<T> Sync for UnalignedSlice<T> {}
