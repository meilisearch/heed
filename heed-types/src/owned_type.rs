use std::borrow::Cow;

use crate::CowType;
use heed_traits::{BytesDecode, BytesEncode};
use zerocopy::{AsBytes, FromBytes};

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

impl<'a, T: 'a> BytesEncode<'a> for OwnedType<T>
where
    T: AsBytes,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for OwnedType<T>
where
    T: FromBytes + Copy,
{
    type DItem = T;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        CowType::<T>::bytes_decode(bytes).map(Cow::into_owned)
    }
}

unsafe impl<T> Send for OwnedType<T> {}

unsafe impl<T> Sync for OwnedType<T> {}
