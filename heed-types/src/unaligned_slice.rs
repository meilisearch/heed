use std::borrow::Cow;

use bytemuck::{try_cast_slice, AnyBitPattern, NoUninit};
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

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<[u8]>> {
        try_cast_slice(item).map(Cow::Borrowed).ok()
    }
}

impl<'a, T: AnyBitPattern> BytesDecode<'a> for UnalignedSlice<T> {
    type DItem = &'a [T];

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        try_cast_slice(bytes).ok()
    }
}

unsafe impl<T> Send for UnalignedSlice<T> {}

unsafe impl<T> Sync for UnalignedSlice<T> {}
