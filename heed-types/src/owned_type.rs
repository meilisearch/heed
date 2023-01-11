use std::borrow::Cow;

use bytemuck::{bytes_of, AnyBitPattern, NoUninit};
use heed_traits::{BoxedError, BytesDecode, BytesEncode};

use crate::CowType;

/// Describes a type that is totally owned (doesn't
/// hold any reference to the original slice).
///
/// If you need to store a type that doesn't depends on any
/// [memory alignment] and that can be big it is recommended
/// to use the [`UnalignedType`].
///
/// The [`CowType`] is recommended for borrowed types (types that holds
/// references to the original slice).
///
/// To store slices, you must look at the [`CowSlice`],
/// [`OwnedSlice`] or [`UnalignedSlice`] types.
///
/// [memory alignment]: std::mem::align_of()
/// [`UnalignedType`]: crate::UnalignedType
/// [`CowType`]: crate::CowType
/// [`UnalignedSlice`]: crate::UnalignedSlice
/// [`OwnedSlice`]: crate::OwnedSlice
/// [`CowSlice`]: crate::CowSlice
pub struct OwnedType<T>(std::marker::PhantomData<T>);

impl<'a, T: NoUninit> BytesEncode<'a> for OwnedType<T> {
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        Ok(Cow::Borrowed(bytes_of(item)))
    }
}

impl<'a, T: AnyBitPattern + NoUninit> BytesDecode<'a> for OwnedType<T> {
    type DItem = T;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        CowType::<T>::bytes_decode(bytes).map(Cow::into_owned)
    }
}

unsafe impl<T> Send for OwnedType<T> {}

unsafe impl<T> Sync for OwnedType<T> {}
